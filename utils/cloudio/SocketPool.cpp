/* Copyright (C) 2019 MariaDB Corporaton

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#include "SocketPool.h"
#include "configcpp.h"
#include "logger.h"
#include "messageFormat.h"

#include <sys/socket.h>
#include <sys/un.h>

using namespace std;

namespace
{

void log(logging::LOG_TYPE whichLogFile, const string& msg)
{
    logging::Logger logger(12);  //12 = configcpp
    logger.logMessage(whichLogFile, msg, logging::LoggingID(12));
}

}

namespace idbdatafile
{

SocketPool::SocketPool()
{
    config::Config *config = config::Config::makeConfig();  // the most 'config' ever put into a single line of code?
    string stmp;
    int64_t itmp = 0;
    
    try 
    {
        stmp = config->getConfig("StorageManager", "MaxSockets");
        itmp = strtol(stmp.c_str(), NULL, 10);
        if (itmp > 500 || itmp < 1)
        {
            string errmsg = "SocketPool(): Got a bad value '" + stmp + "' for StorageManager/MaxSockets.  Range is 1-500.";
            log(logging::LOG_TYPE_CRITICAL, errmsg);
            throw runtime_error(errmsg);
        }
        maxSockets = itmp;
    }
    catch (exception &e) 
    {
        ostringstream os;
        os << "SocketPool(): Using default of " << defaultSockets << ".";
        log(logging::LOG_TYPE_CRITICAL, os.str());
        maxSockets = defaultSockets;
    }
}

SocketPool::~SocketPool()
{
    boost::mutex::scoped_lock(mutex);

    for (uint i = 0; i < allSockets.size(); i++)
        ::close(allSockets[i]);
}

#define sm_check_error \
    if (err < 0) \
    { \
        cout << "SP: got an error on the socket" << endl; \
        remoteClosed(sock); \
        return -1; \
    }
    
int SocketPool::send_recv(messageqcpp::ByteStream &in, messageqcpp::ByteStream *out)
{
    uint count = 0;
    uint length = in.length();
    int sock = getSocket();
    const uint8_t *inbuf = in.buf();
    int err = 0;
    
    /* TODO: make these writes not send SIGPIPE
    TODO: turn at least the header bits into a single write */ 
    err = ::write(sock, &storagemanager::SM_MSG_START, sizeof(storagemanager::SM_MSG_START));
    sm_check_error;
    err = ::write(sock, &length, sizeof(length));
    sm_check_error;
    while (count < length)
    {
        err = ::write(sock, &inbuf[count], length-count);
        sm_check_error;
        count += err;
        in.advance(err);
    }
    //cout << "SP sent the msg" << endl;
    
    out->restart();
    uint8_t *outbuf;
    uint8_t window[8192];
    length = 0;
    uint remainingBytes = 0;
    uint i;
    
    while (1)
    {
        // here remainingBytes means the # of bytes from the previous message
        err = ::read(sock, &window[remainingBytes], 8192 - remainingBytes);
        sm_check_error;
        if (err == 0)
        {
            remoteClosed(sock);
            // TODO, a retry loop
            return -1;
        }
        uint endOfData = remainingBytes + err;   // for clarity
        
        // scan for the 8-byte header.  If it is fragmented, move the fragment to the front of the buffer
        // for the next iteration to handle.
        
        if (endOfData < 8)
        {
            remainingBytes = endOfData;
            continue;
        }
        
        for (i = 0; i <= endOfData - 8; i++)
        {
            if (*((uint *) &window[i]) == storagemanager::SM_MSG_START)
            {
                length = *((uint *) &window[i+4]);
                break;
            }
        }
        
        assert(i == 0);   // in testing there shouldn't be any garbage in the stream
        
        if (length == 0)    // didn't find the header yet
        {
            // i == endOfData - 7 here
            remainingBytes = endOfData - i;
            memmove(window, &window[i], remainingBytes);
        }
        else
        {
            // i == first byte of the header here
            // copy the payload fragment we got into the output bytestream
            uint startOfPayload = i + 8;   // for clarity
            out->needAtLeast(length);
            outbuf = out->getInputPtr();
            memcpy(outbuf, &window[startOfPayload], endOfData - startOfPayload);
            if (length < endOfData - startOfPayload)
                cout << "SocketPool: warning!  Probably got a bad length field!  payload length = " << length <<
                    " endOfData = " << endOfData << " startOfPayload = " << startOfPayload << endl;
            remainingBytes = length - (endOfData - startOfPayload);    // remainingBytes is now the # of bytes left to read
            out->advanceInputPtr(endOfData - startOfPayload);
            break;   // done looking for the header, can fill the output buffer directly now.
        }
    }
    
    // read the rest of the payload directly into the output bytestream
    while (remainingBytes > 0)
    {
        err = ::read(sock, &outbuf[length - remainingBytes], remainingBytes);
        sm_check_error;
        remainingBytes -= err;
        out->advanceInputPtr(err);
    }
    returnSocket(sock);
    return 0;
}

int SocketPool::getSocket()
{
    boost::mutex::scoped_lock lock(mutex);
    int clientSocket;
    
    if (freeSockets.size() == 0 && allSockets.size() < maxSockets)
    {
        // make a new connection
        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strcpy(&addr.sun_path[1], &storagemanager::socket_name[1]);   // first char is null...
        clientSocket = ::socket(AF_UNIX, SOCK_STREAM, 0);
        int err = ::connect(clientSocket, (const struct sockaddr *) &addr, sizeof(addr));
        if (err >= 0)
            allSockets.push_back(clientSocket);
        else
        {
            int saved_errno = errno;
            ostringstream os;
            char buf[80];
            os << "SocketPool::getSocket() failed to connect; got '" << strerror_r(saved_errno, buf, 80);
            cout << os.str() << endl;
            log(logging::LOG_TYPE_CRITICAL, os.str());
            errno = saved_errno;
        }
        return clientSocket;
    }
    
    // wait for a socket to become free
    while (freeSockets.size() == 0)
        socketAvailable.wait(lock);
    
    clientSocket = freeSockets.front();
    freeSockets.pop_front();
    return clientSocket;
}
    
void SocketPool::returnSocket(const int sock)
{
    boost::mutex::scoped_lock lock(mutex);
    freeSockets.push_back(sock);
    socketAvailable.notify_one();
}

void SocketPool::remoteClosed(const int sock)
{
    boost::mutex::scoped_lock lock(mutex);
    ::close(sock);
    for (vector<int>::iterator i = allSockets.begin(); i != allSockets.end(); ++i)
        if (*i == sock)
        {
            allSockets.erase(i, i+1);
            break;
        }
}


}

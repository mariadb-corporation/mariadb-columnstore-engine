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
    clientSocket = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (clientSocket < 0) 
    {
        char buf[80], *ptr;
        ptr = strerror_r(errno, buf, 80);
        throw runtime_error("SocketPool(): Failed to get clientSocket, got " + string(ptr));
    }
}

SocketPool::~SocketPool()
{
    boost::mutex::scoped_lock(mutex);

    for (uint i = 0; i < allSockets.size(); i++)
        ::close(allSockets[i]);
    ::close(clientSocket);
}

#define sm_check_error \
    if (err < 0) \
    { \
        returnSocket(sock); \
        return -1; \
    }
    
int SocketPool::send_recv(messageqcpp::ByteStream &in, messageqcpp::ByteStream *out)
{
    uint count = 0;
    uint length = in.length();
    int sock = getSocket();
    const uint8_t *inbuf = in.buf();
    int err = 0;
    
    /* TODO: make these writes not send SIGPIPE */
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
    
    out->restart();
    uint8_t *outbuf;
    uint8_t window[8192];
    bool foundHeader = false;
    length = 0;
    uint remainingBytes = 0;
    uint i;
    
    /* TODO: consider adding timeouts on msg recv if we start using tcp sockets */
    while (!foundHeader)
    {
        // here remainingBytes means the # of bytes from the previous message
        err = ::read(sock, &window[remainingBytes], 8192 - remainingBytes);
        sm_check_error;
        uint endOfData = remainingBytes + err;   // for clarity
        
        // scan for the 8-byte header.  If it is fragmented, move the fragment to the front of the buffer
        // for the next iteration to handle.
        for (i = 0; i <= endOfData - 8 && !foundHeader; i++)
        {
            if (*((uint *) &window[i]) == storagemanager::SM_MSG_START)
            {
                length = *((uint *) &window[i+4]);
                foundHeader = true;
            }
        }
        
        if (!foundHeader)
        {
            remainingBytes = endOfData - i;
            if (i != 0)
                memmove(window, &window[i], remainingBytes);
            else
                continue;   // if i == 0, then the read was too short to see the whole header, do another read().
        }
        else
        {
            // copy the payload fragment we got into the output bytestream
            out->needAtLeast(length);
            outbuf = out->getInputPtr();
            memcpy(outbuf, &window[i+8], endOfData - (i+8));
            remainingBytes = length - (endOfData - (i+8));    // remainingBytes is now the # of bytes left to read
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
    int ret;
    
    if (freeSockets.size() == 0 && allSockets.size() < maxSockets)
    {
        // make a new connection
        struct sockaddr_un addr;
        memset(&addr, 0, sizeof(addr));
        addr.sun_family = AF_UNIX;
        strcpy(&addr.sun_path[0], storagemanager::socket_name);
        ret = ::connect(clientSocket, (const struct sockaddr *) &addr, sizeof(addr));
        if (ret >= 0)
            allSockets.push_back(ret);
        else
        {
            int saved_errno = errno;
            ostringstream os;
            char buf[80];
            os << "SocketPool::getSocket() failed to connect; got '" << strerror_r(saved_errno, buf, 80);
            log(logging::LOG_TYPE_CRITICAL, os.str());
            errno = saved_errno;
        }
        return ret;
    }
    
    // wait for a socket to become free
    while (freeSockets.size() == 0)
        socketAvailable.wait(lock);
    
    ret = freeSockets.front();
    freeSockets.pop_front();
    return ret;
}
    
void SocketPool::returnSocket(const int sock)
{
    boost::mutex::scoped_lock lock(mutex);
    freeSockets.push_back(sock);
    socketAvailable.notify_one();
}

}

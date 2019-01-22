// copy licensing stuff here



#include "SocketPool.h"
#include "configcpp.h"
#include "logging.h"
#include "storage-manager/include/messageFormat.h"

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
        if (itmp > 500 || itmp < 1) {
            string errmsg = "SocketPool(): Got a bad value '" + stmp + "' for StorageManager/MaxSockets.";
            log(logging::CRITICAL, errmsg);
            throw runtime_error(errmsg);
        maxSockets = itmp;
    }
    catch (exception &e) 
    {
        ostringstream os;
        os << "SocketPool(): Using default of " << defaultSockets << ".";
        log(logging::CRITICAL, os.str());
        maxSockets = defaultSockets;
    }
    clientSocket = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (clientSocket < 0) {
        char buf[80], *ptr;
        ptr = strerror_r(errno, buf, 80);
        throw runtime_error("SocketPool(): Failed to get clientSocket, got " + string(ptr));
    }
}

SocketPool::~SocketPool()
{
    boost::mutex::scoped_lock(mutex);

    while (!allSockets.empty()) {
        int next = allSockets.front();
        allSockets.pop_front();
        ::close(next);
    }
    ::close(clientSocket)
}

int SocketPool::send_recv(const messageqcpp::ByteStream &in, messageqcpp::ByteStream *out)
{
    uint count = 0;
    uint length = in.length();
    int sock = getSocket();
    uint8_t inbuf* = in.buf();
    int err = 0;
    
    ::write(sock, &storagemanager::SM_MSG_START, sizeof(storagemanager::SM_MSG_START));
    ::write(sock, &length, sizeof(length));
    while (count < length)
    {
        err = ::write(sock, &inbuf[count], length-count);
        if (err < 0)
        {
            returnSocket(sock);
            return -1;
        }
        count += err;
    }
    
    out->restart();
    uint8_t *outbuf;
    uint8_t window[8192];
    bool foundheader = false;
    length = 0;
    uint remainingBytes = 0;
    int i;
    while (!foundheader)
    {
        // here remainingBytes means the # of bytes from the previous message
        err = ::read(sock, &window[remainingBytes], 8192 - remainingBytes);
        if (err < 0)
        {
            returnSocket(sock);
            return -1;
        }
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
            out->needAtLeast(length);
            outbuf = out->buf();
            memcpy(outbuf, &window[i+8], endOfData - (i+8));
            remainingBytes = length - (endOfData - (i+8));    // remainingBytes is now the # of bytes left to read
        }
    }
    
    // read the rest of the payload
    while (remainingBytes > 0)
    {
        err = ::read(sock, &outbuf[length - remainingBytes], remainingBytes);
        if (err < 0)
        {
            returnSocket(sock);
            return -1;
        }
        remainingBytes -= err;
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
        ret = ::connect(tmp, &addr, sizeof(addr));
        if (ret >= 0)
            allSockets.push_back(ret);
        return ret;
    }
    
    // wait for a socket to become free
    while (freeSockets.size() == 0)
        socketAvailable.wait(lock);
    
    ret = freeSockets.front();
    freeSockets.pop_front();
    return ret;
}
    
void SocketPool::returnSocket(int sock)
{
    boost::mutex::scoped_lock lock(mutex);
    freeSockets.push_back(sock);
    socketAvailable.notify_one();
}

}

// copy licensing stuff here

#ifndef _SOCKETPOOL_H_
#define _SOCKETPOOL_H_

#include <boost/utility.hpp>
#include <boost/thread/mutex.hpp>

#include "bytestream.h"

namespace idbdatafile
{

/* This should be renamed; it's more than a pool, it also does the low-level communication.  TBD. */
class SocketPool : public boost::noncopyable
{
    public:
        SocketPool();
        
        // the dtor will immediately close all sockets
        virtual ~SocketPool();
        
        // 0 = success, -1 = failure.  Should this throw instead?
        int send_recv(const ByteStream &to_send, ByteStream *to_recv);
        
    private:
        int getSocket();
        void returnSocket();
        
        std::vector<int> allSockets;
        std::deque<int> freeSockets;
        boost::mutex mutex;
        boost::condition socketAvailable;
        int clientSocket;
        int maxSockets;
        static const int defaultSockets = 20;
};

}



#endif

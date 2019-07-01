
#ifndef CLIENTREQUESTPROCESSOR_H_
#define CLIENTREQUESTPROCESSOR_H_

#include "ThreadPool.h"
#include <sys/types.h>


namespace storagemanager
{

class ClientRequestProcessor : public boost::noncopyable
{
    public:
        static ClientRequestProcessor *get();
        virtual ~ClientRequestProcessor();

        void processRequest(int sock, uint len);
        void shutdown();

    private:
        ClientRequestProcessor();
        ThreadPool threadPool;
};

}

#endif

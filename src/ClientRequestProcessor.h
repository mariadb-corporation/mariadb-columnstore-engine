
#ifndef CLIENTREQUESTPROCESSOR_H_
#define CLIENTREQUESTPROCESSOR_H_

#include "ThreadPool.h"
#include <sys/types.h>


namespace storagemanager
{

class ClientRequestProcessor : public boost::noncopyable
{
    public:
        ClientRequestProcessor();
        virtual ~ClientRequestProcessor();
        
        void processRequest(int sock, uint len);
        
    private:
        ThreadPool threadPool;
};

}

#endif

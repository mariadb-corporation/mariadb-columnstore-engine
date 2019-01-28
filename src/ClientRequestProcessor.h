
#include "ThreadPool.h"


namespace storagemanager
{

class ClientRequestProcessor : public boost::noncopyable
{
    public:
        ClientRequestProcessor();
        virtual ~ClientRequestProcessor();
        
        void processRequest(int sock);
        
    private:
        ThreadPool threadPool;


};

}

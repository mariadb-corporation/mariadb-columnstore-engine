
#include "ClientRequestProcessor.h"
#include "ProcessTask.h"
#include <sys/types.h>

namespace
{
storagemanager::ClientRequestProcessor *crp = NULL;
boost::mutex m;
};

namespace storagemanager
{

ClientRequestProcessor::ClientRequestProcessor()
{
}

ClientRequestProcessor::~ClientRequestProcessor()
{
}

ClientRequestProcessor * ClientRequestProcessor::get()
{
    if (crp)
        return crp;
    boost::mutex::scoped_lock s(m);
    if (crp)
        return crp;
    crp = new ClientRequestProcessor();
    return crp;
}

void ClientRequestProcessor::processRequest(int sock, uint len)
{
    boost::shared_ptr<ThreadPool::Job> t(new ProcessTask(sock, len));
    threadPool.addJob(t);
}

void ClientRequestProcessor::shutdown()
{
    delete crp;
}

}



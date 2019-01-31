

#include "ClientRequestProcessor.h"
#include "ProcessTask.h"
#include <sys/types.h>

namespace storagemanager
{

ClientRequestProcessor::ClientRequestProcessor()
{
}

ClientRequestProcessor::~ClientRequestProcessor()
{
}

void ClientRequestProcessor::processRequest(int sock, uint len)
{
    boost::shared_ptr<ThreadPool::Job> t(new ProcessTask(sock, len));
    threadPool.addJob(t);
}

}



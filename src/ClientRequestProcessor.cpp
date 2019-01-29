

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
    ProcessTask t(sock, len);
    threadPool.addJob(t);
}

}



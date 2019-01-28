

#include "ClientRequestProcessor.h"

namespace storagemanager
{

ClientRequestProcessor::ClientRequestProcessor()
{
}

ClientRequestProcessor::~ClientRequestProcessor()
{
}

ClientRequestProcessor::processRequest(int sock)
{
    ProcessTask t(sock);
    threadPool.addJob(t);
}

}



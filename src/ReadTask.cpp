
#include "ReadTask.h"
#include "messageFormat.h"
#include "SMLogging.h"
#include <errno.h>

using namespace std;

namespace storagemanager
{

ReadTask::ReadTask(int sock, uint len) : PosixTask(sock, len)
{
}

ReadTask::~ReadTask()
{
}

#define check_error(msg, ret) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return ret; \
    }

#define max(x, y) (x > y ? x : y)
    
bool ReadTask::run()
{
    SMLogging* logger = SMLogging::get();
    uint8_t buf[1024] = {0};

    // get the parameters
    if (getLength() > 1023) {
        handleError("ReadTask read", EFAULT);
        return true;
    }
    
    bool success;
    success = read(buf, getLength());
    check_error("ReadTask read cmd", false);
    read_cmd *cmd = (read_cmd *) buf;
    
    #ifdef SM_TRACE
    logger->log(LOG_DEBUG,"read %s count %i offset %i.",cmd->filename,cmd->count,cmd->offset);
    #endif
    
    // read from IOC, write to the socket
    vector<uint8_t> outbuf;
    outbuf.resize(max(cmd->count, 4) + sizeof(sm_response));
    sm_response *resp = (sm_response *) &outbuf[0];
    
    resp->returnCode = 0;
    uint payloadLen = 0;
    
    // todo: do the reading and writing in chunks
    // todo: need to make this use O_DIRECT on the IOC side
    ssize_t err;
    while ((uint) resp->returnCode < cmd->count)
    {
        try
        {
            err = ioc->read(cmd->filename, &resp->payload[resp->returnCode], cmd->offset + resp->returnCode, 
                cmd->count - resp->returnCode);
        }
        catch (exception &e)
        {
            logger->log(LOG_DEBUG, "ReadTask: caught '%s'", e.what());
            errno = EIO;
            err = -1;
        }
        if (err < 0) {
            if (resp->returnCode == 0) {
                resp->returnCode = err;
                payloadLen = 4;
                *((int32_t *) resp->payload) = errno;
            }
            break;
        }
        if (err == 0)
            break;
        resp->returnCode += err;
    }
    if (resp->returnCode >= 0)
        payloadLen = resp->returnCode;
    success = write(*resp, payloadLen);
    return success;
}



}

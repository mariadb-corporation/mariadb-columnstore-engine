
#include "ReadTask.h"
#include "messageFormat.h"
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

#define min(x, y) (x < y ? x : y)
    
bool ReadTask::run()
{
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
    
    // read from IOC, write to the socket
    vector<uint8_t> outbuf;
    outbuf.resize(min(cmd->count, 4) + sizeof(sm_msg_resp));
    sm_msg_resp *resp = (sm_msg_resp *) &outbuf[0];
    
    resp->type = SM_MSG_START;
    resp->returnCode = 0;
    resp->payloadLen = 4;
    
    // todo: do the reading and writing in chunks
    // todo: need to make this use O_DIRECT on the IOC side
    ioc->willRead(cmd->filename, cmd->offset, cmd->count);
    int err;
    while (resp->returnCode < cmd->count)
    {
        err = ioc->read(cmd->filename, &resp->payload[resp->returnCode], cmd->offset + resp->returnCode, 
          cmd->count - resp->returnCode);
        if (err < 0) {
            if (resp->returnCode == 0) {
                resp->payloadLen = 8;
                resp->returnCode = err;
                *((int32_t *) resp->payload) = errno;
            }
            break;
        }
        resp->returnCode += err;
        resp->payloadLen += err;
    }
    
    success = write(&outbuf[0], resp->payloadLen + SM_HEADER_LEN);
    return success;
}



}


#include "PingTask.h"
#include "messageFormat.h"
#include <errno.h>

namespace storagemanager
{

PingTask::PingTask(int sock, uint len) : PosixTask(sock, len)
{
}

PingTask::~PingTask()
{
}

void PingTask::run()
{
    // not much to check on for Milestone 1
    
    uint8_t buf;
    
    if (getLength() > 1)
    {
        handleError("PingTask", E2BIG);
        return;
    }
    // consume the msg
    bool success = read(&buf, getLength());
    if (!success)
    {
        handleError("PingTask", errno);
        return;
    }
    
    // send generic success response
    sm_msg_resp ret;
    ret.type = SM_MSG_START;
    ret.payloadLen = 4;
    ret.returnCode = 0;
    write((uint8_t *) &ret, sizeof(ret));
}

}

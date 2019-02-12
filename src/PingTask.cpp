
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

bool PingTask::run()
{
    // not much to check on for Milestone 1
    
    uint8_t buf;
    
    if (getLength() > 1)
    {
        handleError("PingTask", E2BIG);
        return true;
    }
    // consume the msg
    bool success = read(&buf, getLength());
    if (!success)
    {
        handleError("PingTask", errno);
        return false;
    }
    
    // send generic success response
    sm_response ret;
    ret.returnCode = 0;
    success = write(ret, 0);
    return success;
}

}


#include "TruncateTask.h"
#include <errno.h>
#include "messageFormat.h"

using namespace std;

namespace storagemanager
{

TruncateTask::TruncateTask(int sock, uint len) : PosixTask(sock, len)
{
}

TruncateTask::~TruncateTask()
{
}

#define check_error(msg, ret) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return ret; \
    }

bool TruncateTask::run()
{
    bool success;
    uint8_t buf[1024] = {0};
    
    if (getLength() > 1023) {
        handleError("TruncateTask read", ENAMETOOLONG);
        return false;
    }
    
    success = read(buf, getLength());
    check_error("TruncateTask read", false);
    truncate_cmd *cmd = (truncate_cmd *) buf;
    
    int err = ioc->truncate(cmd->filename, cmd->length);
    if (err)
    {
        handleError("TruncateTask truncate", errno);
        return true;
    }
    
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    resp->type = SM_MSG_START;
    resp->payloadLen = 4;
    resp->returnCode = 0;
    success = write(buf, sizeof(sm_msg_resp));
    return success;
}

}

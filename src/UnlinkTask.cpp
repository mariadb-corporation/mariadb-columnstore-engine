
#include "UnlinkTask.h"
#include "IOCoordinator.h"
#include <errno.h>
#include "messageFormat.h"

using namespace std;

namespace storagemanager
{

UnlinkTask::UnlinkTask(int sock, uint len) : PosixTask(sock, len)
{
}

UnlinkTask::~UnlinkTask()
{
}

#define check_error(msg) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return; \
    }


void UnlinkTask::run()
{
    bool success;
    uint8_t buf[1024] = {0};
    
    if (getLength() > 1023) {
        handleError("UnlinkTask read", ENAMETOOLONG);
        return;
    }
    
    success = read(buf, getLength());
    check_error("UnlinkTask read");
    unlink_cmd *cmd = (unlink_cmd *) buf;
    
    int err = ioc->unlink(cmd->filename);
    if (err)
    {
        handleError("UnlinkTask unlink", errno);
        return;
    }
    
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    resp->type = SM_MSG_START;
    resp->payloadLen = 4;
    resp->returnCode = 0;
    write(buf, sizeof(*resp));
}

}

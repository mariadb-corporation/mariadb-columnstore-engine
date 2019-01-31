
#include "TruncateTask.h"
#include "IOCoordinator.h"
#include <errno.h>
#include "messageFormat.h"

using namespace std;

extern storagemanager::IOCoordinator *ioc;

namespace storagemanager
{

TruncateTask::TruncateTask(int sock, uint len) : PosixTask(sock, len)
{
}

TruncateTask::~TruncateTask()
{
}

#define check_error(msg) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return; \
    }

void TruncateTask::run()
{
    bool success;
    uint8_t buf[1024] = {0};
    
    if (getLength() > 1023) {
        handleError("TruncateTask read", ENAMETOOLONG);
        return;
    }
    
    success = read(buf, getLength());
    check_error("TruncateTask read");
    truncate_cmd *cmd = (truncate_cmd *) buf;
    
    int err = ioc->truncate(cmd->filename, cmd->length);
    if (err)
    {
        handleError("TruncateTask truncate", errno);
        return;
    }
    
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    resp->type = SM_MSG_START;
    resp->payloadLen = 4;
    resp->returnCode = 0;
    write(buf, sizeof(sm_msg_resp));
}

}

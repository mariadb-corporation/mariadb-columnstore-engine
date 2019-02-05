
#include "UnlinkTask.h"
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

#define check_error(msg, ret) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return ret; \
    }


bool UnlinkTask::run()
{
    bool success;
    uint8_t buf[1024] = {0};
    
    if (getLength() > 1023) {
        handleError("UnlinkTask read", ENAMETOOLONG);
        return true;
    }
    
    success = read(buf, getLength());
    check_error("UnlinkTask read", false);
    unlink_cmd *cmd = (unlink_cmd *) buf;
    
    #ifdef SM_TRACE
    cout << "unlink " << cmd->filename << endl;
    #endif
    
    int err = ioc->unlink(cmd->filename);
    if (err)
    {
        handleError("UnlinkTask unlink", errno);
        return true;
    }
    
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    resp->type = SM_MSG_START;
    resp->payloadLen = 4;
    resp->returnCode = 0;
    success = write(buf, sizeof(*resp));
    return success;
}

}

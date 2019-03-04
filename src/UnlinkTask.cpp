
#include "UnlinkTask.h"
#include <errno.h>
#include "messageFormat.h"
#include "SMLogging.h"

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
    SMLogging* logger = SMLogging::get();
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
    logger->log(LOG_DEBUG,"unlink %s.",cmd->filename);
    #endif
    
    int err = ioc->unlink(cmd->filename);
    if (err)
    {
        handleError("UnlinkTask unlink", errno);
        return true;
    }
    
    sm_response *resp = (sm_response *) buf;
    resp->returnCode = 0;
    success = write(*resp, 0);
    return success;
}

}

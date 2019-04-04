
#include "TruncateTask.h"
#include <errno.h>
#include "messageFormat.h"
#include "SMLogging.h"

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
    SMLogging* logger = SMLogging::get();
    bool success;
    uint8_t buf[1024] = {0};
    
    if (getLength() > 1023) {
        handleError("TruncateTask read", ENAMETOOLONG);
        return false;
    }
    
    success = read(buf, getLength());
    check_error("TruncateTask read", false);
    truncate_cmd *cmd = (truncate_cmd *) buf;
    
    #ifdef SM_TRACE
    logger->log(LOG_DEBUG,"truncate %s newlength %i.",cmd->filename,cmd->length);
    #endif
    int err;
    
    try
    {
        err = ioc->truncate(cmd->filename, cmd->length);
    }
    catch (exception &e)
    {
        logger->log(LOG_DEBUG, "TruncateTask: caught '%s'", e.what());
        errno = EIO;
        err = -1;
    }
    if (err)
    {
        handleError("TruncateTask truncate", errno);
        return true;
    }
    
    sm_response *resp = (sm_response *) buf;
    resp->returnCode = 0;
    success = write(*resp, 0);
    return success;
}

}

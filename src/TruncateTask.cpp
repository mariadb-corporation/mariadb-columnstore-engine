
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
    
    #ifdef SM_TRACE
    cout << "truncate " << cmd->filename << " newlength " << cmd->length << endl;
    #endif
    
    int err = ioc->truncate(cmd->filename, cmd->length);
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

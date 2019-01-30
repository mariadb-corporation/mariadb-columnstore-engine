
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
    cmd_overlay *cmd = (cmd_overlay *) buf;
    
    int err = ioc->truncate(cmd->filename, cmd->newSize);
    if (err)
    {
        handleError("TruncateTask truncate", errno);
        return;
    }
    
    uint32_t *buf32 = (uint32_t *) buf;
    buf32[0] = SM_MSG_START;
    buf32[1] = 4;
    buf32[2] = 0;
    write(buf, 12);
}

}


#include "StatTask.h"
#include "messageFormat.h"
#include "IOCoordinator.h"
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>
#include <string.h>

using namespace std;

extern storagemanager::IOCoordinator *ioc;

namespace storagemanager
{

StatTask::StatTask(int sock, uint len) : PosixTask(sock, len)
{
}

StatTask::~StatTask()
{
}

#define check_error(msg) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return; \
    }

void StatTask::run()
{
    bool success;
    uint8_t buf[1024] = {0};
    
    if (getLength() > 1023) {
        handleError("StatTask read", ENAMETOOLONG);
        return;
    }
    
    success = read(buf, getLength());
    check_error("StatTask read");
    cmd_overlay *cmd = (cmd_overlay *) buf;
    
    int err = ioc->stat(cmd->path, (struct stat *) &buf[SM_HEADER_LEN]);
    if (err)
    {
        handleError("StatTask stat", errno);
        return;
    }
    
    uint32_t *buf32 = (uint32_t *) buf;
    buf32[0] = SM_MSG_START;
    buf32[1] = sizeof(struct stat);
    write(buf, SM_HEADER_LEN + sizeof(struct stat));
}

}


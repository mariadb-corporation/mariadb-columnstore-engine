

#include "OpenTask.h"
#include "IOCoordinator.h"
#include "messageFormat.h"
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

using namespace std;

extern storagemanager::IOCoordinator *ioc;

namespace storagemanager
{

OpenTask::OpenTask(int sock, uint len) : PosixTask(sock, len)
{
}

OpenTask::~OpenTask()
{
}

void OpenTask::run()
{
    /*
        get the parameters
        call IOManager to do the work
        return the result
    */
    bool success;
    uint8_t buf[1024] = {0};
    
    if (getLength() > 1023)
    {
        handleError("OpenTask read", ENAMETOOLONG);
        return;
    }
    
    success = read(buf, getLength());
    if (!success)
    {
        handleError("OpenTask read", errno);
        return;
    }
    
    cmd_overlay *cmd = (cmd_overlay *) buf;
    
    int err = ioc->open(cmd->filename, cmd->openmode, (struct stat *) &buf[SM_HEADER_LEN]);
    if (err)
    {
        handleError("OpenTask open", errno);
        return;
    }
    
    uint32_t *buf32 = (uint32_t *) buf;
    buf32[0] = SM_MSG_START;
    buf32[1] = sizeof(struct stat);
    success = write(buf, sizeof(struct stat) + SM_HEADER_LEN);
    if (!success)
        handleError("OpenTask write", errno);
}

}


#include "AppendTask.h"
#include "IOCoordinator.h"
#include <errno.h>

using namespace std;

extern storagemanager::IOCoordinator *ioc;

namespace storagemanager
{

AppendTask::AppendTask(int sock, uint len) : PosixTask(sock, len)
{
}

AppendTask::~AppendTask()
{
}

#define check_error(msg) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return; \
    }
    
#define min(x, y) (x < y ? x : y)

void AppendTask::run()
{
    bool success;
    uint8_t cmdbuf[1024] = {0};
    int err;
    
    success = read(cmdbuf, sizeof(struct cmd_overlay));
    check_error("AppendTask read");
    cmd_overlay *cmd = (cmd_overlay *) cmdbuf;
    
    success = read(&cmdbuf[sizeof(*cmd)], min(cmd->filename_len, 1024 - sizeof(*cmd) - 1));
    check_error("AppendTask read");
    
    size_t count = 0;
    vector<uint8_t> databuf;
    databuf.resize(cmd->count);
    
    // todo: do this in chunks...
    while (count < cmd->count)
    {
        success = read(&databuf[count], cmd->count - count);
        check_error("AppendTask read data");
        
        uint count2 = 0;
        while (count2 < cmd->count - count)
        {
            err = ioc->append(cmd->filename, &databuf[count], cmd->count - count);
            if (err <= 0)
            {
                handleError("AppendTask write data", errno);
                return;
            }
            count2 += err;
        }
        count += cmd->count;
    }
}

}

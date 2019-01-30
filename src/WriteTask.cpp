
#include "WriteTask.h"
#include "IOCoordinator.h"
#include <errno.h>

using namespace std;

extern storagemanager::IOCoordinator *ioc;

namespace storagemanager
{

WriteTask::WriteTask(int sock, uint len) : PosixTask(sock, len)
{
}

WriteTask::~WriteTask()
{
}

#define check_error(msg) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return; \
    }

#define min(x, y) (x < y ? x : y)

void WriteTask::run()
{
    bool success;
    uint8_t cmdbuf[1024] = {0};

    success = read(cmdbuf, sizeof(struct cmd_overlay));
    check_error("WriteTask read");
    cmd_overlay *cmd = (cmd_overlay *) cmdbuf;
    
    success = read(&cmdbuf[sizeof(*cmd)], min(cmd->filename_len, 1024 - sizeof(*cmd) - 1));
    check_error("WriteTask read");
    
    size_t count = 0;
    vector<uint8_t> databuf;
    databuf.resize(cmd->count);
    
    // todo: do this in chunks...
    while (count < cmd->count)
    {
        success = read(&databuf[count], cmd->count - count);
        check_error("WriteTask read data");
        count += cmd->count;
        int count2 = 0;
        while (count2 < count)
        {
            int err = ioc->write(cmd->filename, &databuf[count + count2], cmd->offset + count2, cmd->count - count2);
            if (err <= 0)
            {
                handleError("WriteTask write", errno);
                return;
            }
            count2 += err;
        }
    }
}

}

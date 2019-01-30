
#include "AppendTask.h"
#include "messageFormat.h"
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
    
    size_t readCount = 0, writeCount = 0;
    vector<uint8_t> databuf;
    uint bufsize = 1 << 20;   // 1 MB
    databuf.resize(bufsize);  // 1 MB
    
    while (readCount < cmd->count)
    {
        uint toRead = min(cmd->count - readCount, bufsize);    // 1 MB
        success = read(&databuf[0], toRead);
        check_error("AppendTask read data");
        readCount += toRead;
        while (writeCount < readCount)
        {
            int err = ioc->append(cmd->filename, &databuf[writeCount], readCount - writeCount);
            if (err <= 0)
                break;
            writeCount += err;
        }
        if (readCount != writeCount)
            break;
    }
    uint32_t *buf32 = (uint32_t *) cmdbuf;
    buf32[0] = SM_MSG_START;
    buf32[1] = 4;
    buf32[2] = writeCount;
    write(cmdbuf, 12);
}

}

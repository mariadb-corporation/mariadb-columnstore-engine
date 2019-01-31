
#include "WriteTask.h"
#include "messageFormat.h"
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
            
    size_t readCount = 0, writeCount = 0;
    vector<uint8_t> databuf;
    uint bufsize = 1 << 20;   // 1 MB
    databuf.resize(bufsize);

    while (readCount < cmd->count)
    {
        uint toRead = min(cmd->count - readCount, bufsize);
        success = read(&databuf[0], toRead);
        check_error("WriteTask read data");
        readCount += toRead;
        while (writeCount < readCount)
        {
            int err = ioc->write(cmd->filename, &databuf[writeCount], cmd->offset + writeCount, readCount - writeCount);
            if (err <= 0)
                break;
            writeCount += err;
        }
        if (writeCount != readCount)
            break;
    }
    
    uint32_t response[4];
    response[0] = SM_MSG_START;
    if (cmd->count != 0 && writeCount == 0)
    {
        response[1] = 8;
        response[2] = -1;
        response[3] = errno;
        write((uint8_t *) response, 16);
    }
    else
    {
        response[1] = 4;
        response[2] = writeCount;
        write((uint8_t *) response, 12);
    }
}

}

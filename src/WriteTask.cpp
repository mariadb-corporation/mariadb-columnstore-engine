
#include "WriteTask.h"

using namespace std;

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

void WriteTask::run()
{
    bool success;
    uint8_t cmdbuf[1024] = {0};

    success = read(cmdbuf, sizeof(struct cmd_overlay));
    check_error("WriteTask read");
    cmd_overlay *cmd = cmdbuf;
    
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
        // IOC->write()
    }
}

}

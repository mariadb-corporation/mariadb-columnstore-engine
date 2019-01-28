
#include "AppendTask.h"

using namespace std;

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
        handleError(msg); \
        return; \
    }

void AppendTask::run()
{
    bool success;
    uint8_t cmdbuf[1024] = {0};

    success = read(cmdbuf, sizeof(struct cmd_overlay));
    check_error("AppendTask read");
    cmd_overlay *cmd = cmdbuf;
    
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
        count += cmd->count;
        // IOC->append()
    }
}

}

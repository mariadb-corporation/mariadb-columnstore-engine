
#include "UnlinkTask.h"

using namespace std;

namespace storagemanager
{

UnlinkTask::UnlinkTask(int sock, uint len) : PosixTask(sock, len)
{
}

UnlinkTask::~UnlinkTask()
{
}

void UnlinkTask::run()
{
    bool success;
    uint8_t buf[1024] = {0};
    
    if (getLength() > 1023) {
        handleError("UnlinkTask read", ENAMETOOLONG);
        return;
    }
    
    success = read(buf, getLength());
    check_error("UnlinkTask read");
    cmd_overlay *cmd = (cmd_overlay *) buf;
    
    // IOC->unlink(cmd->filename);
    
    // generic success msg
    uint32_t *buf32 = buf;
    buf32[0] = SM_MSG_START;
    buf32[1] = 4;
    buf32[2] = 0;
    write(buf, 12);
}

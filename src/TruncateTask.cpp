
#include "TruncateTask.h"

using namespace std;

namespace storagemanager
{

TruncateTask::TruncateTask(int sock, uint len) : PosixTask(sock, len)
{
}

TruncateTask::~TruncateTask()
{
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
    
    // IOC->truncate(cmd->filename, cmd->newSize);
    
    // generic success msg
    uint32_t *buf32 = buf;
    buf32[0] = SM_MSG_START;
    buf32[1] = 4;
    buf32[2] = 0;
    write(buf, 12);
}

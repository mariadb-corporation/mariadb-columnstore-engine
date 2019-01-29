
#include "StatTask.h"

using namespace std;

namespace storagemanager
{

StatTask::StatTask(int sock, uint len) : PosixTask(sock, len)
{
}

StatTask::~StatTask()
{
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
    
    struct stat _stat;
    // IOC->stat(cmd->path, &_stat);
    
    // bogus response
    memset(&_stat, 0, sizeof(_stat));
    uint32_t *buf32 = (uint32_t *) buf;
    buf32[0] = SM_MSG_START;
    buf32[1] = sizeof(_stat);
    memcpy(&buf[SM_HEADER_LEN], &_stat, sizeof(_stat));
    write(buf, SM_HEADER_LEN + sizeof(_stat));
}

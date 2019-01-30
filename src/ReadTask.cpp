
#include "ReadTask.h"
#include "messageFormat.h"
#include "IOCoordinator.h"
#include <errno.h>

using namespace std;

extern storagemanager::IOCoordinator *ioc;

namespace storagemanager
{

ReadTask::ReadTask(int sock, uint len) : PosixTask(sock, len)
{
}

ReadTask::~ReadTask()
{
}

#define check_error(msg) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return; \
    }

void ReadTask::run()
{
    uint8_t buf[1024] = {0};

    // get the parameters
    if (getLength() > 1023) {
        handleError("ReadTask read", EFAULT);
        return;
    }
    
    bool success;
    success = read(buf, getLength());
    check_error("ReadTask read cmd");
    cmd_overlay *cmd = (cmd_overlay *) buf;
    
    // read from IOC, write to the socket
    vector<uint8_t> outbuf;
    outbuf.resize(cmd->count + SM_HEADER_LEN);
    uint32_t *outbuf32 = (uint32_t *) &outbuf[0];
    outbuf32[0] = SM_MSG_START;
    outbuf32[1] = cmd->count;
    
    // do the reading and writing in chunks
    ioc->willRead(cmd->filename, cmd->offset, cmd->count);
    int count = 0, err;
    while (count < cmd->count)
    {
        err = ioc->read(cmd->filename, &outbuf[SM_HEADER_LEN + count], cmd->offset + count, cmd->count - count);
        if (err < 0)
        {
            handleError("ReadTask read data", errno);
            return;
        }
        else if (err == 0)
        {
            handleError("ReadTask EOF", errno);
            return;
        }
        count += err;
    }
    
    write(outbuf);
}



}

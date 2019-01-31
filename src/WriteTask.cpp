
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
    
    success = read(cmdbuf, sizeof(write_cmd));
    check_error("WriteTask read");
    write_cmd *cmd = (write_cmd *) cmdbuf;
    
    if (cmd->flen > 1023 - sizeof(*cmd))
    {
        handleError("WriteTask", ENAMETOOLONG);
        return;
    }
    success = read(&cmdbuf[sizeof(*cmd)], cmd->flen);
    check_error("WriteTask read");
            
    size_t readCount = 0, writeCount = 0;
    vector<uint8_t> databuf;
    uint bufsize = min(1 << 20, cmd->count);   // 1 MB
    databuf.resize(bufsize);

    while (readCount < cmd->count)
    {
        uint toRead = min(cmd->count - readCount, bufsize);
        success = read(&databuf[0], toRead);
        check_error("WriteTask read data");
        readCount += toRead;
        uint writePos = 0;
        while (writeCount < readCount)
        {
            int err = ioc->append(cmd->filename, &databuf[writePos], toRead - writePos);
            if (err <= 0)
                break;
            writeCount += err;
            writePos += err;
        }
        if (writeCount != readCount)
            break;
    }
    
    uint8_t respbuf[sizeof(sm_msg_resp) + 4];
    sm_msg_resp *resp = (sm_msg_resp *) respbuf;
    resp->type = SM_MSG_START;
    if (cmd->count != 0 && writeCount == 0)
    {
        resp->payloadLen = 8;
        resp->returnCode = -1;
        *((int *) &resp[1]) = errno;
        write((uint8_t *) respbuf, sizeof(sm_msg_resp) + 4);
    }
    else
    {
        resp->payloadLen = 4;
        resp->returnCode = writeCount;
        write((uint8_t *) respbuf, sizeof(sm_msg_resp));
    }
}

}

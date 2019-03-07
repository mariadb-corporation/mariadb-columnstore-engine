
#include "WriteTask.h"
#include "messageFormat.h"
#include "IOCoordinator.h"
#include "SMLogging.h"
#include <errno.h>

using namespace std;

namespace storagemanager
{

WriteTask::WriteTask(int sock, uint len) : PosixTask(sock, len)
{
}

WriteTask::~WriteTask()
{
}

#define check_error(msg, ret) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return ret; \
    }

#define min(x, y) (x < y ? x : y)

bool WriteTask::run()
{
    SMLogging* logger = SMLogging::get();
    bool success;
    uint8_t cmdbuf[1024] = {0};
    
    success = read(cmdbuf, sizeof(write_cmd));
    check_error("WriteTask read", false);
    write_cmd *cmd = (write_cmd *) cmdbuf;
    
    if (cmd->flen > 1023 - sizeof(*cmd))
    {
        handleError("WriteTask", ENAMETOOLONG);
        return true;
    }
    success = read(&cmdbuf[sizeof(*cmd)], cmd->flen);
    check_error("WriteTask read", false);
            
    #ifdef SM_TRACE
    logger->log(LOG_DEBUG,"write filename %s offset %i count %i.",cmd->filename,cmd->offset,cmd->count);
    #endif
            
    size_t readCount = 0, writeCount = 0;
    vector<uint8_t> databuf;
    uint bufsize = min(1 << 20, cmd->count);   // 1 MB
    databuf.resize(bufsize);

    while (readCount < cmd->count)
    {
        uint toRead = min(cmd->count - readCount, bufsize);
        success = read(&databuf[0], toRead);
        check_error("WriteTask read data", false);
        readCount += toRead;
        uint writePos = 0;
        while (writeCount < readCount)
        {
            int err = ioc->write(cmd->filename, &databuf[writePos], cmd->offset + writeCount, toRead - writePos);
            if (err <= 0)
                break;
            writeCount += err;
            writePos += err;
        }
        if (writeCount != readCount)
            break;
    }
    
    uint8_t respbuf[sizeof(sm_response) + 4];
    sm_response *resp = (sm_response *) respbuf;
    uint payloadLen = 0;
    if (cmd->count != 0 && writeCount == 0)
    {
        payloadLen = 4;
        resp->returnCode = -1;
        *((int *) &resp[1]) = errno;
        success = write(*resp, payloadLen);
    }
    else
        resp->returnCode = writeCount;
    success = write(*resp, payloadLen);
    return success;
}

}


#include "StatTask.h"
#include "messageFormat.h"
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>
#include <string.h>

using namespace std;

namespace storagemanager
{

StatTask::StatTask(int sock, uint len) : PosixTask(sock, len)
{
}

StatTask::~StatTask()
{
}

#define check_error(msg, ret) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return ret; \
    }

bool StatTask::run()
{
    bool success;
    uint8_t buf[1024] = {0};
    
    if (getLength() > 1023) {
        handleError("StatTask read", ENAMETOOLONG);
        return true;
    }
    
    success = read(buf, getLength());
    check_error("StatTask read", false);
    stat_cmd *cmd = (stat_cmd *) buf;
    sm_response *resp = (sm_response *) buf;
    
    #ifdef SM_TRACE
    cout << "stat " << cmd->filename << endl;
    #endif
    
    int err = ioc->stat(cmd->filename, (struct stat *) resp->payload);

    resp->returnCode = err;
    uint payloadLen;
    if (!err)
        payloadLen = sizeof(struct stat);
    else {
        payloadLen = 4;
        *((int32_t *) resp->payload) = errno;
    }
    success = write(*resp, payloadLen);
    return success;
}

}


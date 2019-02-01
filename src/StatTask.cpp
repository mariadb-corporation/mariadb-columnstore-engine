
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

#define check_error(msg) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return; \
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
    stat_cmd *cmd = (stat_cmd *) buf;
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    
    int err = ioc->stat(cmd->filename, (struct stat *) resp->payload);
    if (err)
    {
        handleError("StatTask stat", errno);
        return;
    }

    resp->type = SM_MSG_START;
    resp->payloadLen = sizeof(struct stat) + 4;
    resp->returnCode = 0;
    write(buf, sizeof(*resp) + sizeof(struct stat));
}

}


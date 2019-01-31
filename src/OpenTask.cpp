

#include "OpenTask.h"
#include "IOCoordinator.h"
#include "messageFormat.h"
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

using namespace std;

extern storagemanager::IOCoordinator *ioc;

namespace storagemanager
{

OpenTask::OpenTask(int sock, uint len) : PosixTask(sock, len)
{
}

OpenTask::~OpenTask()
{
}

void OpenTask::run()
{
    /*
        get the parameters
        call IOManager to do the work
        return the result
    */
    bool success;
    uint8_t buf[1024] = {0};
    
    if (getLength() > 1023)
    {
        handleError("OpenTask read1", ENAMETOOLONG);
        return;
    }
    
    success = read(buf, getLength());
    if (!success)
    {
        handleError("OpenTask read2", errno);
        return;
    }
    
    open_cmd *cmd = (open_cmd *) buf;

    int err = ioc->open(cmd->filename, cmd->openmode, (struct stat *) &buf[sizeof(sm_msg_resp)]);
    if (err)
    {
        handleError("OpenTask open", errno);
        return;
    }
    
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    resp->type = SM_MSG_START;
    resp->payloadLen = sizeof(struct stat) + 4;
    resp->returnCode = 0;
    success = write(buf, sizeof(struct stat) + sizeof(sm_msg_resp));
    if (!success)
        handleError("OpenTask write", errno);
}

}

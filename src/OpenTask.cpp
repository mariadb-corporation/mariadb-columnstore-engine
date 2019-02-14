

#include "OpenTask.h"
#include "messageFormat.h"
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

using namespace std;

namespace storagemanager
{

OpenTask::OpenTask(int sock, uint len) : PosixTask(sock, len)
{
}

OpenTask::~OpenTask()
{
}

bool OpenTask::run()
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
        return true;
    }
    
    success = read(buf, getLength());
    if (!success)
    {
        handleError("OpenTask read2", errno);
        return false;
    }
    
    open_cmd *cmd = (open_cmd *) buf;

    #ifdef SM_TRACE
    syslog(LOG_DEBUG, "open filename %s mode %o.",cmd->filename,cmd->openmode);
    #endif
    sm_response *resp = (sm_response *) buf;
    int err = ioc->open(cmd->filename, cmd->openmode, (struct stat *) &resp->payload);
    if (err)
    {
        handleError("OpenTask open", errno);
        return true;
    }
    
    resp->returnCode = 0;
    success = write(*resp, sizeof(struct stat));
    if (!success)
        handleError("OpenTask write", errno);
    return success;
}

}

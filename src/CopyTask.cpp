
#include "CopyTask.h"
#include <errno.h>
#include "SMLogging.h"
#include "messageFormat.h"

using namespace std;

namespace storagemanager
{

CopyTask::CopyTask(int sock, uint len) : PosixTask(sock, len)
{
}

CopyTask::~CopyTask()
{
}

#define check_error(msg, ret) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return ret; \
    }

bool CopyTask::run()
{
    bool success;
    SMLogging* logger = SMLogging::get();

    uint8_t buf[2048] = {0};
    
    if (getLength() > 2047) 
    {
        handleError("CopyTask read", ENAMETOOLONG);
        return true;
    }
    
    success = read(buf, getLength());
    check_error("CopyTask read", false);
    copy_cmd *cmd = (copy_cmd *) buf;
    string filename1(cmd->file1.filename, cmd->file1.flen);   // need to copy this in case it's not null terminated
    f_name *filename2 = (f_name *) &buf[sizeof(copy_cmd) + cmd->file1.flen];
    
    #ifdef SM_TRACE
    logger->log(LOG_DEBUG,"copy %s to %s.",filename1.c_str(),filename2->filename);
    #endif
    int err;
    try
    {
        err = ioc->copyFile(filename1.c_str(), filename2->filename);
    }
    catch (exception &e)
    {
        logger->log(LOG_DEBUG, "CopyTask: caught %s", e.what());
        err = -1;
        errno = EIO;
    }
        
    if (err)
    {
        handleError("CopyTask copy", errno);
        return true;
    }
    
    sm_response *resp = (sm_response *) buf;
    uint payloadLen = 0;
    resp->returnCode = 0;
    success = write(*resp, 0);
    return success;
}

}

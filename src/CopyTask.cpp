
#include "CopyTask.h"
#include <errno.h>
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

#define check_error(msg) \
    if (!success) \
    { \
        handleError(msg, errno); \
        return; \
    }

void CopyTask::run()
{
    bool success;
    uint8_t buf[2048] = {0};
    
    if (getLength() > 2047) 
    {
        handleError("CopyTask read", ENAMETOOLONG);
        return;
    }
    
    success = read(buf, getLength());
    check_error("CopyTask read");
    copy_cmd *cmd = (copy_cmd *) buf;
    string filename1(cmd->file1.filename, cmd->file1.flen);   // need to copy this in case it's not null terminated
    f_name *filename2 = (f_name *) &buf[sizeof(copy_cmd) + cmd->file1.flen];
    
    int err = ioc->copyFile(filename1.c_str(), filename2->filename);
    if (err)
    {
        handleError("CopyTask copy", errno);
        return;
    }
    
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    resp->type = SM_MSG_START;
    resp->payloadLen = 4;
    resp->returnCode = 0;
    write(buf, sizeof(sm_msg_resp));
}

}



#include "OpenTask.h"
#include <sys/stat.h>

using namespace std;

namespace storagemanager
{

OpenTask::OpenTask(int sock, uint len) : PosixTask(sock, len)
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
    uint8_t *buf;
    
    if (getLength() > 1024)
    {
        handleError("OpenTask read", ENAMETOOLONG);
        return;
    }
    
    buf = alloca(getLength());
    success = read(&buf, getLength());
    if (!success)
    {
        handleError("OpenTask read", errno);
        return;
    }
    
    cmd_overlay *cmd = (cmd_overlay *) buf;
    
    // IOC->open(filename, openmode, &buf[SM_HEADER_LEN])
    
    // stand-in dummy response
    uint32_t *buf32 = (uint32_t *) buf;
    buf32[0] = SM_MSG_START;
    buf32[1] = sizeof(struct stat);
    success = write(buf, sizeof(struct stat) + SM_HEADER_LEN);
    if (!success)
        handleError("OpenTask write", errno);
}

}

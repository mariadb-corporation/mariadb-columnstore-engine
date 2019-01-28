

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
    uint8_t *buf = alloca(max(getLength(), sizeof(struct stat) + SM_HEADER_LEN));
    
    success = read(&buf, getLength());
    if (!success)
    {
        handleError("OpenTask read", errno);
        return;
    }
    
    uint32_t flen = *((uint32_t *) &buf[0]);
    string filename((char *) &buf[4], flen);   // might want to require filenames to be NULL-terminated for efficiency
    int openmode = *((int *) &buf[4+flen]);
    
    // IOC->open(filename, openmode, &buf[SM_HEADER_LEN])
    
    // stand-in dummy response
    uint32_t *buf32 = (uint32_t *) &buf[0];
    buf32[0] = SM_MSG_START;
    buf32[1] = sizeof(struct stat);
    success = write(buf, sizeof(struct stat) + SM_HEADER_LEN);
    if (!success)
        handleError("OpenTask write", errno);
}

}

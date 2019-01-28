
#include "ReadTask.h"

using namespace std;

namespace storagemanager
{

ReadTask::ReadTask(int sock, uint len) : PosixTask(sock, len)
{
}

ReadTask::~ReadTask()
{
}

void ReadTask::run()
{

    // get the parameters
    if (getLength() > 1024) {
        handleError("ReadTask read", EFAULT);
        return;
    }
    
    bool success;
    uint8_t *buf = alloca(getLength());
    success = read(&buf, getLength());
    cmd_overlay *cmd = (cmd_overlay *) buf;
    
    // read from IOC, write to the socket
    vector<uint8_t> outbuf;
    outbuf.resize(count + SM_HEADER_LEN);
    uint32_t *outbuf32 = (uint32_t *) &outbuf[0];
    outbuf32[0] = SM_MSG_START;
    outbuf32[1] = length;
    
    // do the reading and writing in chunks
    // IOC->willRead(filename, offset, length);
    // IOC->read(filename, offset, length, &outbuf[SM_HEADER_LEN]);
    
    write(outbuf);
}



}

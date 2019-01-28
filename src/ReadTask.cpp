
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
    bool success;
    uint8_t *buf = alloca(getLength());
    int boff = 0;
    success = read(&buf, getLength());
    uint flen = *((uint *) &buf[0])
    boff += 4;
    string filename(&buf[boff], flen);
    boff += flen;
    size_t count = *((size_t *) &buf[boff]);
    boff += sizeof(size_t);
    off_t offset = *((off_t *) &buf[boff]);
    
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


#include "PosixTask.h"


namespace storagemanager
{

PosixTask::PosixTask(int _sock, uint _length) : sock(_sock), remainingLength(_length)
{
}

PosixTask::~PosixTask()
{
    // return the socket
}

void PosixTask::handleError(int errCode)
{
    char buf[80];
    // TODO: construct and log a message
    cout << "PosixTask caught an error reading from a socket: " << strerror_r(errCode, buf, 80) << endl;
}

/* Optimization.  Make this read larger chunks into a buffer & supply data from that when possible. */
int PosixTask::read(vector<uint8_t> *buf, uint offset, uint length)
{
    if (length > remainingLength)
        length = remainingLength;
    
    uint originalSize = buf->size();
    buf->resize(originalSize + length);
    
    uint count = 0;
    int err;
    while (count < length)
    {
        /* TODO: need a timeout here? */
        err = ::read(sock, &(*buf)[count + offset], length - count);
        if (err < 0) {
            buf->resize(originalSize);
            handleError(errno);
            return -1;
        }
        else if (err == 0) {
            buf->resize(originalSize);
            handleError(0);
            return -1;
        }
        count += err;
        remainingLength -= err;
    }
    return count;
}


}

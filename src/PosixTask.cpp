
#include "PosixTask.h"
#include "messageFormat.h"
#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <string.h>

#define min(x, y) (x < y ? x : y)

using namespace std;

namespace storagemanager
{

PosixTask::PosixTask(int _sock, uint _length) : 
    sock(_sock),
    totalLength(_length),
    remainingLengthInStream(_length),
    remainingLengthForCaller(_length),
    bufferPos(0),
    bufferLen(0),
    socketReturned(false)
{
}

PosixTask::~PosixTask()
{
    consumeMsg();
    if (!socketReturned)
        returnSocket();
}

void PosixTask::handleError(const char *name, int errCode)
{
    char buf[80];
    
    // send an error response if possible
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    resp->type = SM_MSG_START;
    resp->payloadLen = 8;
    resp->returnCode = -1;
    *((int *) resp->payload) = errCode;
    write((uint8_t *) buf, sizeof(*resp) + 4);
    
    // TODO: construct and log a message
    cout << name << " caught an error: " << strerror_r(errCode, buf, 80) << endl;
    socketError();
}

void PosixTask::returnSocket()
{
    socketReturned = true;
}

void PosixTask::socketError()
{
    socketReturned = true;
}

uint PosixTask::getRemainingLength()
{
    return remainingLengthForCaller;
}

uint PosixTask::getLength()
{
    return totalLength;
}

// todo, need this to return an int instead of a bool b/c it modifies the length of the read
bool PosixTask::read(uint8_t *buf, uint length)
{
    if (length > remainingLengthForCaller)
        length = remainingLengthForCaller;
    if (length == 0)
        return false;
    
    uint count = 0;
    int err;
    
    // copy data from the local buffer first.
    uint dataInBuffer = bufferLen - bufferPos;
    if (length <= dataInBuffer)
    {
        memcpy(buf, &localBuffer[bufferPos], length);
        count = length;
        bufferPos += length;
        remainingLengthForCaller -= length;
    }
    else if (dataInBuffer > 0)
    {
        memcpy(buf, &localBuffer[bufferPos], dataInBuffer);
        count = dataInBuffer;
        bufferPos += dataInBuffer;
        remainingLengthForCaller -= dataInBuffer;
    }
    
    // read the remaining requested amount from the stream.
    // ideally, combine the recv here with the recv below that refills the local
    // buffer.
    while (count < length)
    {
        err = ::recv(sock, &buf[count], length - count, 0);
        if (err <= 0)
            return false;

        count += err;
        remainingLengthInStream -= err;
        remainingLengthForCaller -= err;
    }
    
    /* The caller's request has been satisfied here.  If there is remaining data in the stream
    get what's available. */
    primeBuffer();
    return true;
}

void PosixTask::primeBuffer()
{
    if (remainingLengthInStream > 0)
    {
        // Reset the buffer to allow a larger read.
        if (bufferLen == bufferPos)
        {
            bufferLen = 0;
            bufferPos = 0;
        }
        else if (bufferLen - bufferPos < 1024)  // if < 1024 in the buffer, move data to the front
        {
            // debating whether it is more efficient to use a circular buffer + more
            // recv's, or to move data to reduce the # of recv's.  WAG: moving data.
            memmove(localBuffer, &localBuffer[bufferPos], bufferLen - bufferPos);
            bufferLen -= bufferPos;
            bufferPos = 0;
        }
        
        uint toRead = min(remainingLengthInStream, bufferSize - bufferLen);
        int err = ::recv(sock, &localBuffer[bufferLen], toRead, MSG_DONTWAIT);
        // ignoring errors here since this is supposed to be silent.
        // errors will be caught by the next read
        if (err > 0) 
        {
            bufferLen += err;
            remainingLengthInStream -= err;
        }
    }
}

bool PosixTask::write(const uint8_t *buf, uint len)
{
    int err;
    uint count = 0;
    
    while (count < len)
    {
        err = ::send(sock, &buf[count], len - count, 0);
        if (err < 0)
            return false;
        count += err;
    }
    return true;
}

bool PosixTask::write(const vector<uint8_t> &buf)
{
    return write(&buf[0], buf.size());
}

void PosixTask::consumeMsg()
{
    uint8_t buf[1024];
    int err;
    
    bufferLen = 0;
    bufferPos = 0;
    remainingLengthForCaller = 0;
    
    while (remainingLengthInStream > 0)
    {
        err = ::recv(sock, buf, min(remainingLengthInStream, 1024), 0);
        if (err <= 0) {
            remainingLengthInStream = 0;
            break;
        }
        remainingLengthInStream -= err;
    }
}

}

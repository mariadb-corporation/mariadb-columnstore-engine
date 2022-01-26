/* Copyright (C) 2019 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#include "PosixTask.h"
#include "messageFormat.h"
#include "SMLogging.h"
#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <string.h>

#define min(x, y) (x < y ? x : y)

using namespace std;

namespace storagemanager
{
PosixTask::PosixTask(int _sock, uint _length)
 : sock(_sock)
 , totalLength(_length)
 , remainingLengthInStream(_length)
 , remainingLengthForCaller(_length)
 , bufferPos(0)
 , bufferLen(0)
{
  ioc = IOCoordinator::get();
}

PosixTask::~PosixTask()
{
  assert(remainingLengthForCaller == 0);
  assert(remainingLengthInStream == 0);
  consumeMsg();
}

void PosixTask::handleError(const char* name, int errCode)
{
  SMLogging* logger = SMLogging::get();
  char buf[80];

  // send an error response if possible
  sm_response* resp = (sm_response*)buf;
  resp->returnCode = -1;
  *((int*)resp->payload) = errCode;
  bool success = write(*resp, 4);

  if (!success)
    logger->log(LOG_ERR, "%s caught an error: %s.", name, strerror_r(errCode, buf, 80));
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
int PosixTask::read(uint8_t* buf, uint length)
{
  if (length > remainingLengthForCaller)
    length = remainingLengthForCaller;
  if (length == 0)
    return 0;

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
    if (err < 0)
      return err;

    count += err;
    remainingLengthInStream -= err;
    remainingLengthForCaller -= err;
  }

  /* The caller's request has been satisfied here.  If there is remaining data in the stream
  get what's available. */
  primeBuffer();
  return count;
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

bool PosixTask::write(const uint8_t* buf, uint len)
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

bool PosixTask::write(sm_response& resp, uint payloadLength)
{
  int err;
  uint count = 0;
  uint8_t* buf = (uint8_t*)&resp;

  resp.header.type = SM_MSG_START;
  resp.header.flags = 0;
  resp.header.payloadLen = payloadLength + sizeof(sm_response) - sizeof(sm_msg_header);
  uint toSend = payloadLength + sizeof(sm_response);

  while (count < toSend)
  {
    err = ::send(sock, &buf[count], toSend - count, 0);
    if (err < 0)
      return false;
    count += err;
  }
  return true;
}

bool PosixTask::write(const vector<uint8_t>& buf)
{
  return write(&buf[0], buf.size());
}

void PosixTask::consumeMsg()
{
  SMLogging* logger = SMLogging::get();

  uint8_t buf[1024];
  int err;

  bufferLen = 0;
  bufferPos = 0;
  remainingLengthForCaller = 0;

  while (remainingLengthInStream > 0)
  {
    logger->log(LOG_WARNING, "PosixTask::consumeMsg(): Discarding the tail end of a partial msg.");
    err = ::recv(sock, buf, min(remainingLengthInStream, 1024), 0);
    if (err <= 0)
    {
      remainingLengthInStream = 0;
      break;
    }
    remainingLengthInStream -= err;
  }
}

}  // namespace storagemanager

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

#ifndef POSIX_TASK_H_
#define POSIX_TASK_H_

#include <vector>
#include <sys/types.h>
#include <stdint.h>
#include <iostream>
#include "IOCoordinator.h"
#include "messageFormat.h"

namespace storagemanager
{
class PosixTask
{
 public:
  PosixTask(int sock, uint length);
  virtual ~PosixTask();

  // this should return false if there was a network error, true otherwise including for other errors
  virtual bool run() = 0;
  void primeBuffer();

 protected:
  int read(uint8_t* buf, uint length);
  bool write(const std::vector<uint8_t>& buf);
  bool write(sm_response& resp, uint payloadLength);
  bool write(const uint8_t* buf, uint length);
  void consumeMsg();          // drains the remaining portion of the message
  uint getLength();           // returns the total length of the msg
  uint getRemainingLength();  // returns the remaining length from the caller's perspective
  void handleError(const char* name, int errCode);

  IOCoordinator* ioc;

 private:
  PosixTask();

  int sock;
  int totalLength;
  uint remainingLengthInStream;
  uint remainingLengthForCaller;
  static const uint bufferSize = 4096;
  uint8_t localBuffer[bufferSize];
  uint bufferPos;
  uint bufferLen;
};

}  // namespace storagemanager
#endif

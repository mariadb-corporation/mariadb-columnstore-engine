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

#include "PingTask.h"
#include "messageFormat.h"
#include <errno.h>

namespace storagemanager
{
PingTask::PingTask(int sock, uint len) : PosixTask(sock, len)
{
}

PingTask::~PingTask()
{
}

bool PingTask::run()
{
  // not much to check on for Milestone 1

  uint8_t buf;

  if (getLength() > 1)
  {
    handleError("PingTask", E2BIG);
    return true;
  }
  // consume the msg
  int success = read(&buf, getLength());
  if (success < 0)
  {
    handleError("PingTask", errno);
    return false;
  }

  // send generic success response
  sm_response ret;
  ret.returnCode = 0;
  success = write(ret, 0);
  return success;
}

}  // namespace storagemanager

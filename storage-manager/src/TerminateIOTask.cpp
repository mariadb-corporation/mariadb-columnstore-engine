/* Copyright (C) 2024 MariaDB Corporation

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

#include "TerminateIOTask.h"
#include "messageFormat.h"
#include "src/CloudStorage.h"
#include <errno.h>

namespace storagemanager
{

TerminateIOTask::TerminateIOTask(int sock, uint len) : PosixTask(sock, len)
{
}

bool TerminateIOTask::run()
{
  bool success;
  uint8_t buf[sizeof(terminate_iotask_cmd)];
  int err;

  if (getLength() != sizeof(terminate_iotask_cmd))
  {
    handleError("TerminateIOTask read", EBADMSG);
    return true;
  }

  err = read(buf, getLength());
  if (err < 0)
  {
    handleError("TerminateIOTask read", errno);
    return false;
  }

  auto* cmd = reinterpret_cast<terminate_iotask_cmd*>(buf);

  auto* cs = CloudStorage::get();
  success = cs->killTask(cmd->id);

  if (success)
  {
    sm_response resp;
    resp.returnCode = 0;
    success = write(resp, 0);
    if (!success)
    {
      handleError("TerminateIOTask write", errno);
      return false;
    }
  }
  else
  {
    handleError("TerminateIOTask", ENOENT);
  }
  return true;
}

}  // namespace storagemanager

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

#include "ListIOTask.h"
#include "SMLogging.h"
#include "messageFormat.h"
#include "src/CloudStorage.h"
#include <errno.h>
#include <string.h>

namespace storagemanager
{

ListIOTask::ListIOTask(int sock, uint len) : PosixTask(sock, len)
{
}

bool ListIOTask::run()
{
  bool success;
  uint8_t buf[1];
  int err;

  if (getLength() > 1)
  {
    handleError("ListIOTask read", E2BIG);
    return true;
  }

  // consume the msg
  err = read(buf, getLength());
  if (err < 0)
  {
    handleError("ListIOTask read", errno);
    return false;
  }

  auto* cs = CloudStorage::get();
  auto taskList = cs->taskList();
  size_t payloadLen = sizeof(list_iotask_resp) + sizeof(list_iotask_resp_entry) * taskList.size();
  size_t headerLen = sizeof(sm_response);
  std::vector<uint8_t> payloadBuf(payloadLen + headerLen);

  auto* resp = reinterpret_cast<sm_response*>(payloadBuf.data());
  resp->header.type = SM_MSG_START;
  resp->header.payloadLen = payloadLen + headerLen - sizeof(sm_msg_header);
  resp->header.flags = 0;
  resp->returnCode = 0;
  auto* r = reinterpret_cast<list_iotask_resp*>(resp->payload);
  r->elements = taskList.size();

  for (size_t i = 0; i < taskList.size(); ++i)
  {
    r->entries[i].id = taskList[i].id;
    r->entries[i].runningTime = taskList[i].runningTime;
  }
  success = write(payloadBuf.data(), payloadBuf.size());
  if (!success)
  {
    handleError("ListIOTask read", errno);
    return false;
  }
  return true;
}

}  // namespace storagemanager

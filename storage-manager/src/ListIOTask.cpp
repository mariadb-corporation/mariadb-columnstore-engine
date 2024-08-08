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

#include "ListIOTask.h"
#include "SMLogging.h"
#include "messageFormat.h"
#include "src/CloudStorage.h"
#include <errno.h>
#include <string.h>
#include <iostream>

using namespace std;

namespace storagemanager
{
ListIOTask::ListIOTask(int sock, uint len) : PosixTask(sock, len)
{
}

ListIOTask::~ListIOTask()
{
}

#define check_error(msg, ret) \
  if (!success)               \
  {                           \
    handleError(msg, errno);  \
    return ret;               \
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
  auto task_list = cs->taskList();
  size_t payload_len = sizeof(list_iotask_resp) + sizeof(list_iotask_resp_entry) * task_list.size();
  size_t header_len = sizeof(sm_response);
  vector<uint8_t> payload_buf(payload_len + header_len);

  sm_response* resp = reinterpret_cast<sm_response*>(payload_buf.data());
  resp->header.type = SM_MSG_START;
  resp->header.payloadLen = payload_len + header_len - sizeof(sm_msg_header);
  resp->header.flags = 0;
  resp->returnCode = 0;
  list_iotask_resp* r = reinterpret_cast<list_iotask_resp*>(resp->payload);
  r->elements = task_list.size();

  for (size_t i = 0; i < task_list.size(); ++i)
  {
    r->entries[i].id = task_list[i].id;
    r->entries[i].runningTime = task_list[i].runningTime;
  }
  success = write(payload_buf.data(), payload_buf.size());
  check_error("ListIOTask write", false);
  return true;
}

}  // namespace storagemanager

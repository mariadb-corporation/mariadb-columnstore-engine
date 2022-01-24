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

#include "ListDirectoryTask.h"
#include "SMLogging.h"
#include "messageFormat.h"
#include <errno.h>
#include <string.h>
#include <iostream>

using namespace std;

namespace storagemanager
{
ListDirectoryTask::ListDirectoryTask(int sock, uint len) : PosixTask(sock, len)
{
}

ListDirectoryTask::~ListDirectoryTask()
{
}

#define check_error(msg, ret) \
  if (!success)               \
  {                           \
    handleError(msg, errno);  \
    return ret;               \
  }

#define min(x, y) (x < y ? x : y)

bool ListDirectoryTask::writeString(uint8_t* buf, int* offset, int size, const string& str)
{
  bool success;
  if (size - *offset < 4)  // eh, let's not frag 4 bytes.
  {
    success = write(buf, *offset);
    check_error("ListDirectoryTask::writeString()", false);
    *offset = 0;
  }
  int count = 0, len = str.length();
  *((uint32_t*)&buf[*offset]) = len;
  *offset += 4;
  while (count < len)
  {
    int toWrite = min(size - *offset, len);
    memcpy(&buf[*offset], &str.data()[count], toWrite);
    count += toWrite;
    *offset += toWrite;
    if (*offset == size)
    {
      success = write(buf, *offset);
      check_error("ListDirectoryTask::writeString()", false);
      *offset = 0;
    }
  }
  return true;
}

bool ListDirectoryTask::run()
{
  SMLogging* logger = SMLogging::get();
  bool success;
  uint8_t buf[1024] = {0};
  int err;

  if (getLength() > 1023)
  {
    handleError("ListDirectoryTask read", ENAMETOOLONG);
    return true;
  }

  err = read(buf, getLength());
  if (err < 0)
  {
    handleError("ListDirectoryTask read", errno);
    return false;
  }
  listdir_cmd* cmd = (listdir_cmd*)buf;

#ifdef SM_TRACE
  logger->log(LOG_DEBUG, "list_directory %s.", cmd->path);
#endif

  vector<string> listing;
  try
  {
    err = ioc->listDirectory(cmd->path, &listing);
  }
  catch (exception& e)
  {
    logger->log(LOG_ERR, "ListDirectoryTask: caught '%s'", e.what());
    errno = EIO;
    err = -1;
  }
  if (err)
  {
    handleError("ListDirectory", errno);
    return true;
  }

  // be careful modifying the listdir return types...
  uint payloadLen = sizeof(listdir_resp) + (sizeof(listdir_resp_entry) * listing.size());
  for (uint i = 0; i < listing.size(); i++)
    payloadLen += listing[i].size();

  sm_response* resp = (sm_response*)buf;
  resp->header.type = SM_MSG_START;
  resp->header.payloadLen = payloadLen + sizeof(sm_response) - sizeof(sm_msg_header);
  resp->header.flags = 0;
  resp->returnCode = 0;
  listdir_resp* r = (listdir_resp*)resp->payload;
  r->elements = listing.size();

  int offset = (uint64_t)r->entries - (uint64_t)buf;
  for (uint i = 0; i < listing.size(); i++)
  {
    success = writeString(buf, &offset, 1024, listing[i]);
    check_error("ListDirectoryTask write", false);
  }

  if (offset != 0)
  {
    success = write(buf, offset);
    check_error("ListDirectoryTask write", false);
  }
  return true;
}

}  // namespace storagemanager

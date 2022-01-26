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

#include "AppendTask.h"
#include "messageFormat.h"
#include "SMLogging.h"
#include <errno.h>

using namespace std;

namespace storagemanager
{
AppendTask::AppendTask(int sock, uint len) : PosixTask(sock, len)
{
}

AppendTask::~AppendTask()
{
}

#define check_error(msg, ret) \
  if (success < 0)            \
  {                           \
    handleError(msg, errno);  \
    return ret;               \
  }

#define min(x, y) (x < y ? x : y)

bool AppendTask::run()
{
  SMLogging* logger = SMLogging::get();
  int success;
  uint8_t cmdbuf[1024] = {0};
  ssize_t err;

  success = read(cmdbuf, sizeof(append_cmd));
  check_error("AppendTask read", false);
  append_cmd* cmd = (append_cmd*)cmdbuf;

  if (cmd->flen > 1023 - sizeof(*cmd))
  {
    handleError("AppendTask", ENAMETOOLONG);
    return true;
  }
  success = read(&cmdbuf[sizeof(*cmd)], cmd->flen);
  check_error("AppendTask read", false);

#ifdef SM_TRACE
  logger->log(LOG_DEBUG, "append %d bytes to %s.", cmd->count, cmd->filename);
#endif

  ssize_t readCount = 0, writeCount = 0;
  vector<uint8_t> databuf;
  uint bufsize = min(100 << 20, cmd->count);  // 100 MB
  // uint bufsize = cmd->count;
  databuf.resize(bufsize);

  while (readCount < cmd->count)
  {
    uint toRead = min(static_cast<uint>(cmd->count - readCount), bufsize);
    success = read(&databuf[0], toRead);
    check_error("AppendTask read data", false);
    if (success == 0)
      break;
    readCount += success;
    uint writePos = 0;

    while (writeCount < readCount)
    {
      try
      {
        err = ioc->append(cmd->filename, &databuf[writePos], success - writePos);
      }
      catch (exception& e)
      {
        logger->log(LOG_ERR, "AppendTask: caught '%s'", e.what());
        errno = EIO;
        err = -1;
      }
      if (err <= 0)
        break;
      writeCount += err;
      writePos += err;
    }
    if (readCount != writeCount)
      break;
  }

  uint8_t respbuf[sizeof(sm_response) + 4];
  sm_response* resp = (sm_response*)respbuf;
  uint payloadLen = 0;
  if (cmd->count != 0 && writeCount == 0)
  {
    payloadLen = 4;
    resp->returnCode = -1;
    *((int*)&resp[1]) = errno;
  }
  else
    resp->returnCode = writeCount;
  return write(*resp, payloadLen);
}

}  // namespace storagemanager

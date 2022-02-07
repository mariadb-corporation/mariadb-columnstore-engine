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

#include "StatTask.h"
#include "messageFormat.h"
#include "SMLogging.h"
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>
#include <string.h>

using namespace std;

namespace storagemanager
{
StatTask::StatTask(int sock, uint len) : PosixTask(sock, len)
{
}

StatTask::~StatTask()
{
}

#define check_error(msg, ret) \
  if (success < 0)            \
  {                           \
    handleError(msg, errno);  \
    return ret;               \
  }

bool StatTask::run()
{
  SMLogging* logger = SMLogging::get();
  int success;
  uint8_t buf[1024] = {0};

  if (getLength() > 1023)
  {
    handleError("StatTask read", ENAMETOOLONG);
    return true;
  }

  success = read(buf, getLength());
  check_error("StatTask read", false);
  stat_cmd* cmd = (stat_cmd*)buf;
  sm_response* resp = (sm_response*)buf;

#ifdef SM_TRACE
  logger->log(LOG_DEBUG, "stat %s.", cmd->filename);
#endif
  int err;
  try
  {
    err = ioc->stat(cmd->filename, (struct stat*)resp->payload);
  }
  catch (exception& e)
  {
    logger->log(LOG_ERR, "StatTask: caught '%s'", e.what());
    errno = EIO;
    err = -1;
  }

  resp->returnCode = err;
  uint payloadLen;
  if (!err)
    payloadLen = sizeof(struct stat);
  else
  {
    payloadLen = 4;
    *((int32_t*)resp->payload) = errno;
  }
  return write(*resp, payloadLen);
}

}  // namespace storagemanager

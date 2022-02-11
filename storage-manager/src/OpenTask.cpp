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

#include "OpenTask.h"
#include "messageFormat.h"
#include "SMLogging.h"
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

using namespace std;

namespace storagemanager
{
OpenTask::OpenTask(int sock, uint len) : PosixTask(sock, len)
{
}

OpenTask::~OpenTask()
{
}

bool OpenTask::run()
{
  /*
      get the parameters
      call IOManager to do the work
      return the result
  */
  SMLogging* logger = SMLogging::get();
  int success;
  uint8_t buf[1024] = {0};

  if (getLength() > 1023)
  {
    handleError("OpenTask read1", ENAMETOOLONG);
    return true;
  }

  success = read(buf, getLength());
  if (success < 0)
  {
    handleError("OpenTask read2", errno);
    return false;
  }

  open_cmd* cmd = (open_cmd*)buf;

#ifdef SM_TRACE
  logger->log(LOG_DEBUG, "open filename %s mode %o.", cmd->filename, cmd->openmode);
#endif
  sm_response* resp = (sm_response*)buf;
  int err;

  try
  {
    err = ioc->open(cmd->filename, cmd->openmode, (struct stat*)&resp->payload);
  }
  catch (exception& e)
  {
    logger->log(LOG_ERR, "OpenTask: caught '%s'", e.what());
    errno = EIO;
    err = -1;
  }
  if (err)
  {
    handleError("OpenTask open", errno);
    return true;
  }

  resp->returnCode = 0;
  success = write(*resp, sizeof(struct stat));
  if (!success)
    handleError("OpenTask write", errno);
  return success;
}

}  // namespace storagemanager

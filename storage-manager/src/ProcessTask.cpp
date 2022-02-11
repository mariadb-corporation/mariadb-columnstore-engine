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

#include "ProcessTask.h"
#include <vector>
#include <iostream>
#include "messageFormat.h"
#include <sys/socket.h>
#include <boost/scoped_ptr.hpp>

#include "AppendTask.h"
#include "CopyTask.h"
#include "ListDirectoryTask.h"
#include "OpenTask.h"
#include "PingTask.h"
#include "ReadTask.h"
#include "StatTask.h"
#include "TruncateTask.h"
#include "UnlinkTask.h"
#include "WriteTask.h"
#include "SyncTask.h"
#include "SessionManager.h"
#include "SMLogging.h"

using namespace std;

namespace storagemanager
{
ProcessTask::ProcessTask(int _sock, uint _length) : sock(_sock), length(_length), returnedSock(false)
{
  assert(length > 0);
}

ProcessTask::~ProcessTask()
{
  if (!returnedSock)
    (SessionManager::get())->returnSocket(sock);
}

void ProcessTask::handleError(int saved_errno)
{
  SMLogging* logger = SMLogging::get();
  SessionManager::get()->socketError(sock);
  returnedSock = true;
  char buf[80];
  logger->log(LOG_ERR, "ProcessTask: got an error during a socket read: %s.",
              strerror_r(saved_errno, buf, 80));
}

void ProcessTask::operator()()
{
  /*
      Read the command from the socket
      Create the appropriate PosixTask
      Run it
  */
  vector<uint8_t> msg;
  int err;
  uint8_t opcode;

  err = ::recv(sock, &opcode, 1, MSG_PEEK);
  if (err <= 0)
  {
    handleError(errno);
    return;
  }

  boost::scoped_ptr<PosixTask> task;
  switch (opcode)
  {
    case OPEN: task.reset(new OpenTask(sock, length)); break;
    case READ: task.reset(new ReadTask(sock, length)); break;
    case WRITE: task.reset(new WriteTask(sock, length)); break;
    case STAT: task.reset(new StatTask(sock, length)); break;
    case UNLINK: task.reset(new UnlinkTask(sock, length)); break;
    case APPEND: task.reset(new AppendTask(sock, length)); break;
    case TRUNCATE: task.reset(new TruncateTask(sock, length)); break;
    case LIST_DIRECTORY: task.reset(new ListDirectoryTask(sock, length)); break;
    case PING: task.reset(new PingTask(sock, length)); break;
    case SYNC: task.reset(new SyncTask(sock, length)); break;
    case COPY: task.reset(new CopyTask(sock, length)); break;
    default: throw runtime_error("ProcessTask: got an unknown opcode");
  }
  task->primeBuffer();
  bool success = task->run();
  if (!success)
  {
    SessionManager::get()->socketError(sock);
    returnedSock = true;
  }
  else
  {
    SessionManager::get()->returnSocket(sock);
    returnedSock = true;
  }
}

}  // namespace storagemanager

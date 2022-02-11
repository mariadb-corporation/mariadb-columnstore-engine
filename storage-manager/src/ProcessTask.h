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

#ifndef PROCESS_TASK_H_
#define PROCESS_TASK_H_

#include "ThreadPool.h"

namespace storagemanager
{
class ProcessTask : public ThreadPool::Job
{
 public:
  ProcessTask(int sock, uint length);  // _sock is the socket to read from
  virtual ~ProcessTask();

  void operator()();

 private:
  ProcessTask();

  void handleError(int errCode);
  int sock;
  uint length;
  bool returnedSock;
};

}  // namespace storagemanager

#endif

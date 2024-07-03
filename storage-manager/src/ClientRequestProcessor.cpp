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

#include "ClientRequestProcessor.h"
#include "ProcessTask.h"
#include <sys/types.h>

namespace
{
storagemanager::ClientRequestProcessor* crp = NULL;
boost::mutex m;
};  // namespace

namespace storagemanager
{
ClientRequestProcessor::ClientRequestProcessor()
{
}

ClientRequestProcessor::~ClientRequestProcessor()
{
}

ClientRequestProcessor* ClientRequestProcessor::get()
{
  if (crp)
    return crp;
  boost::mutex::scoped_lock s(m);
  if (crp)
    return crp;
  crp = new ClientRequestProcessor();
  return crp;
}

void ClientRequestProcessor::processRequest(int sock, uint len)
{
  boost::shared_ptr<ThreadPool::Job> t(new ProcessTask(sock, len));
  threadPool.addJob(t);
}

void ClientRequestProcessor::shutdown()
{
  delete crp;
}

}  // namespace storagemanager

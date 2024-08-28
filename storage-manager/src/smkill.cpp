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

#include <cstdlib>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <time.h>

#include "IOCoordinator.h"
#include "messageFormat.h"
#include "SMComm.h"

using namespace std;
using namespace storagemanager;

void usage(const char* progname)
{
  cerr << progname << " kills S3 network operations by StorageManager" << endl;
  cerr << "Usage: " << progname << " <Id>|all" << endl;
}

bool SMOnline()
{
  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strcpy(&addr.sun_path[1], &socket_name[1]);  // first char is null...
  int clientSocket = ::socket(AF_UNIX, SOCK_STREAM, 0);
  int err = ::connect(clientSocket, (const struct sockaddr*)&addr, sizeof(addr));
  if (err >= 0)
  {
    ::close(clientSocket);
    return true;
  }
  return false;
}

int main(int argc, char** argv)
{
  if (argc != 2)
  {
    usage(argv[0]);
    return EXIT_FAILURE;
  }

  string arg{argv[1]};
  uint64_t id = 0;
  bool all = false;
  if (arg == "all")
  {
    all = true;
  }
  else
  {
    id = stoll(arg);
  }

  if (!SMOnline())
  {
    cerr << "StorageManager is offline" << endl;
    return EXIT_FAILURE;
  }

  auto* sm = idbdatafile::SMComm::get();
  if (!all)
  {
    auto ret = sm->killIOTask(id);
    if (ret < 0)
    {
      cerr << strerror(errno) << endl;
    }
    return ret < 0 ? EXIT_FAILURE : EXIT_SUCCESS;
  }

  // iterate over ids
  vector<list_iotask_resp_entry> entries;
  if (sm->listIOTasks(&entries) != 0)
  {
    cerr << "Couldn't get list of IO tasks" << endl;
    return EXIT_FAILURE;
  }

  for (const auto& entry: entries)
  {
    sm->killIOTask(entry.id);
  }

  return EXIT_SUCCESS;
}

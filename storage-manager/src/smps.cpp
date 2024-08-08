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

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <time.h>
#include <getopt.h>

#include "IOCoordinator.h"
#include "messageFormat.h"
#include "SMComm.h"

using namespace std;
using namespace storagemanager;

void usage(const char* progname)
{
  cerr << progname << " shows S3 network operations by StorageManager" << endl;
  cerr << "Usage: " << progname << " [options]" << endl;
  cerr << "Options:" << endl;
  cerr << "  -n - sort by id" << endl;
  cerr << "  -t - sort by running time" << endl;
  cerr << "  -r - reverse order" << endl;
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

bool byId(const list_iotask_resp_entry& lhs, const list_iotask_resp_entry& rhs)
{
  return lhs.id < rhs.id;
}

bool byTime(const list_iotask_resp_entry& lhs, const list_iotask_resp_entry& rhs)
{
  return lhs.runningTime < rhs.runningTime;
}

int main(int argc, char** argv)
{
  bool reverseSort = false;
  function<bool(const list_iotask_resp_entry&, const list_iotask_resp_entry&)> sortPred;
  int c;
  while ((c = getopt(argc, argv, "nrth")) != -1)
  {
    switch (c)
    {
      case 'n':
        sortPred = byId;
        break;
      case 't':
        sortPred = byTime;
        break;
      case 'r':
        reverseSort = true;
        break;
      default:
        usage(argv[0]);
        return c == 'h' ? EXIT_SUCCESS : EXIT_FAILURE;
    }
  }

  if (!SMOnline())
  {
    cerr << "StorageManager is offline" << endl;
    return EXIT_FAILURE;
  }

  auto* sm = idbdatafile::SMComm::get();
  vector<list_iotask_resp_entry> entries;
  if (sm->listIOTasks(&entries) != 0)
  {
    cerr << "Couldn't get list of IO tasks" << endl;
    return EXIT_FAILURE;
  }

  if (sortPred)
  {
    sort(entries.begin(), entries.end(), sortPred);
  }
  if (reverseSort)
  {
    reverse(entries.begin(), entries.end());
  }
  cout << setw(24) << left << "Id" << "Time" << endl;
  for (const auto& entry: entries)
  {
    cout << setw(24) << left << entry.id << setw(12) << right << fixed << entry.runningTime << endl;
  }

  return EXIT_SUCCESS;
}

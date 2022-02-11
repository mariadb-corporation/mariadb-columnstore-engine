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

#include <iostream>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <boost/filesystem.hpp>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <time.h>

#include "IOCoordinator.h"
#include "SMFileSystem.h"
#include "SMDataFile.h"
#include "messageFormat.h"

using namespace std;
using namespace storagemanager;

void usage(const char* progname)
{
  cerr << progname << " is like 'ls -l' for files managed by StorageManager" << endl;
  cerr << "Usage: " << progname << " directory" << endl;
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

void lsOffline(const char* path)
{
  try
  {
    boost::scoped_ptr<IOCoordinator> ioc(IOCoordinator::get());
    vector<string> listing;

    int err = ioc->listDirectory(path, &listing);
    if (err)
      exit(1);

    struct stat _stat;
    boost::filesystem::path base(path);
    boost::filesystem::path p;
    cout.fill(' ');
    for (auto& entry : listing)
    {
      p = base / entry;
      err = ioc->stat(p.string().c_str(), &_stat);
      if (!err)
      {
        if (_stat.st_mode & S_IFDIR)
        {
          cout << "d";
          cout.width(14);
        }
        else
          cout.width(15);

        struct tm* my_tm = localtime(&_stat.st_mtim.tv_sec);
        char date[100];
        strftime(date, 100, "%b %e %H:%M", my_tm);
        cout << right << _stat.st_size << left << " " << date << left << " " << entry << endl;
      }
      else
      {
        cout.width(15);
        cout << right << "error" << left << " " << entry << endl;
      }
    }
  }
  catch (exception& e)
  {
    cerr << "smls lsOffline FAIL: " << e.what() << endl;
  }
}

void lsOnline(const char* path)
{
  idbdatafile::SMFileSystem fs;
  list<string> listing;

  int err = fs.listDirectory(path, listing);
  if (err)
    exit(1);

  boost::filesystem::path base(path);
  boost::filesystem::path p;
  cout.fill(' ');
  for (auto& entry : listing)
  {
    p = base / entry;
    bool isDir = fs.isDir(p.string().c_str());
    ssize_t size = fs.size(p.string().c_str());
    idbdatafile::SMDataFile df(p.string().c_str(), O_RDONLY, 1);
    time_t mtime = df.mtime();
    if (size >= 0)
    {
      if (isDir)
      {
        cout << "d";
        cout.width(14);
      }
      else
        cout.width(15);

      struct tm* my_tm = localtime(&mtime);
      char date[100];
      strftime(date, 100, "%b %e %H:%M", my_tm);
      cout << right << size << left << " " << date << left << " " << entry << endl;
    }
    else
    {
      cout.width(15);
      cout << right << "error" << left << " " << entry << endl;
    }
  }
}

int makePathPrefix(char* target, int targetlen)
{
  // MCOL-3438 -> add bogus directories to the front of each param
  Config* config = Config::get();
  int prefixDepth = stoi(config->getValue("ObjectStorage", "common_prefix_depth"));
  target[0] = '/';
  target[1] = 0;
  int bufpos = 1;

  for (int i = 0; i < prefixDepth; i++)
  {
    if (bufpos + 3 >= targetlen)
    {
      cerr << "invalid prefix depth in ObjectStorage/common_prefix_depth";
      exit(1);
    }
    memcpy(&target[bufpos], "x/\0", 3);
    bufpos += 2;
  }
  return bufpos;
}

int main(int argc, char** argv)
{
  if (argc != 2)
  {
    usage(argv[0]);
    return 1;
  }

  char prefix[8192];
  int prefixlen = makePathPrefix(prefix, 8192);

  if (SMOnline())
    lsOnline(strncat(prefix, argv[1], 8192 - prefixlen));
  else
    lsOffline(strncat(prefix, argv[1], 8192 - prefixlen));

  return 0;
}

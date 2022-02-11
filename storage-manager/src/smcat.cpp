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
#include "IOCoordinator.h"
#include "messageFormat.h"
#include "SMDataFile.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>

using namespace std;
using namespace storagemanager;

void usage(const char* progname)
{
  cerr << progname << " is 'cat' for files managed by StorageManager." << endl;
  cerr << "Usage: " << progname << " file1 file2 ... fileN" << endl;
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

void catFileOffline(const char* filename, int prefixlen)
{
  uint8_t data[8192];
  off_t offset = 0;
  int read_err, write_err, count;
  try
  {
    boost::scoped_ptr<IOCoordinator> ioc(IOCoordinator::get());

    do
    {
      count = 0;
      read_err = ioc->read(filename, data, offset, 8192);
      if (read_err < 0)
      {
        int l_errno = errno;
        cerr << "Error reading " << &filename[prefixlen] << ": " << strerror_r(l_errno, (char*)data, 8192)
             << endl;
      }

      while (count < read_err)
      {
        write_err = write(STDOUT_FILENO, &data[count], read_err - count);
        if (write_err < 0)
        {
          int l_errno = errno;
          cerr << "Error writing to stdout: " << strerror_r(l_errno, (char*)data, 8192) << endl;
          exit(1);
        }
        count += write_err;
      }
      offset += read_err;
    } while (read_err > 0);
  }
  catch (exception& e)
  {
    cerr << "smcat catFileOffline FAIL: " << e.what() << endl;
  }
}

void catFileOnline(const char* filename, int prefixlen)
{
  uint8_t data[8192];
  off_t offset = 0;
  int read_err, write_err, count;
  idbdatafile::SMDataFile df(filename, O_RDONLY, 0);

  do
  {
    count = 0;
    read_err = df.read(data, 8192);
    if (read_err < 0)
    {
      int l_errno = errno;
      cerr << "Error reading " << &filename[prefixlen] << ": " << strerror_r(l_errno, (char*)data, 8192)
           << endl;
    }

    while (count < read_err)
    {
      write_err = write(STDOUT_FILENO, &data[count], read_err - count);
      if (write_err < 0)
      {
        int l_errno = errno;
        cerr << "Error writing to stdout: " << strerror_r(l_errno, (char*)data, 8192) << endl;
        exit(1);
      }
      count += write_err;
    }
    offset += read_err;
  } while (read_err > 0);
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
  if (argc < 2)
  {
    usage(argv[0]);
    return 1;
  }

  char prefix[8192];
  int prefixlen = makePathPrefix(prefix, 8192);

  for (int i = 1; i < argc; i++)
  {
    strncat(&prefix[prefixlen], argv[i], 8192 - prefixlen);

    if (SMOnline())
      catFileOnline(prefix, prefixlen);
    else
      catFileOffline(prefix, prefixlen);
  }

  return 0;
}

/* Copyright (C) 2014 InfiniDB, Inc.

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

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <boost/thread.hpp>
#include <unistd.h>
#include <list>
#include <string>

#include "SMFileSystem.h"
#include "SMDataFile.h"
#include "SMFileFactory.h"
#include "sm_exceptions.h"
#include "messageFormat.h"

/* The purpose of this is to test the idbdatafile cloud classes using a dummy
SessionManager defined in this file. */

#undef NDEBUG
#include <assert.h>

using namespace idbdatafile;
using namespace std;

volatile bool die = false;
int errCode = 0;

/* Right now this will just accept one connection & send a fixed error response */
void error_server_thread()
{
  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strcpy(&addr.sun_path[1], &storagemanager::socket_name[1]);

  int server_socket = ::socket(AF_UNIX, SOCK_STREAM, 0);
  assert(server_socket >= 0);

  int err = ::bind(server_socket, (struct sockaddr*)&addr, sizeof(addr));
  assert(err == 0);

  err = ::listen(server_socket, 1);
  assert(err == 0);

  socklen_t addrlen;
  struct sockaddr_un client_addr;
  memset(&client_addr, 0, sizeof(client_addr));
  int client_socket = ::accept(server_socket, (struct sockaddr*)&client_addr, &addrlen);
  assert(client_socket >= 0);

  // cout << "server thread got a connection" << endl;

  uint8_t buf[4096];
  uint32_t response[4] = {storagemanager::SM_MSG_START, 8, (uint32_t)-1, EINVAL};
  uint remainingBytes = 0;

  while (!die)
  {
    /* This just scans for SM_MSG_START, and as it finds them it sends a generic error
       response. */

    err = ::recv(client_socket, &buf[remainingBytes], 4096 - remainingBytes, MSG_DONTWAIT);
    if (err < 0)
    {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
      {
        // cout << "server looping..." << endl;
        sleep(1);  // who cares
        continue;
      }
      else
      {
        // char errbuf[80];
        // cout << "server thread got an error: " << strerror_r(errno, errbuf, 80) << endl;
        close(client_socket);
        errCode = -1;
        die = true;
        return;
      }
    }

    // cout << "server thread got some data" << endl;
    uint endOfData = remainingBytes + err;
    uint i;
    if (endOfData < 4)
    {
      remainingBytes = endOfData;
      continue;
    }
    for (i = 0; i <= endOfData - 4; i++)
    {
      if (*((uint32_t*)&buf[i]) == storagemanager::SM_MSG_START)
      {
        // cout << "server thread found a msg start magic" << endl;
        err = ::send(client_socket, response, 16, 0);
        assert(err > 0);
      }
    }
    memmove(buf, &buf[i], endOfData - i);  // should be the trailing 3 bytes of the data
    remainingBytes = endOfData - i;
  }
}

int test1()
{
  /* Start a server thread that returns generic errors */
  boost::thread server_thread(error_server_thread);

  /* Instantiate each of SM subclasses, call each function, and verify the expected error response */
  int err;
  SMFileFactory factory;

  cout << "open" << endl;
  IDBDataFile* file = factory.open("dummy", "r", 0, 0);
  assert(file == NULL && errno == EINVAL && !die);

  SMFileSystem filesystem;

  cout << "compressedSize" << endl;
  bool gotException = false;
  try
  {
    filesystem.compressedSize("dummy");
  }
  catch (NotImplementedYet&)
  {
    gotException = true;
  }
  assert(gotException && !die);

  cout << "copyFile" << endl;
  try
  {
    filesystem.copyFile("dummy1", "dummy2");
  }
  catch (NotImplementedYet&)
  {
    gotException = true;
  }
  assert(gotException && !die);

  cout << "rename" << endl;
  try
  {
    filesystem.rename("dummy1", "dummy2");
  }
  catch (NotImplementedYet&)
  {
    gotException = true;
  }
  assert(gotException && !die);

  cout << "exists" << endl;
  err = filesystem.exists("dummy");
  assert(!err && errno == EINVAL);

  cout << "filesystemisup" << endl;
  err = filesystem.filesystemIsUp();
  assert(!err && errno == EINVAL && !die);

  cout << "isdir" << endl;
  err = filesystem.isDir("dummy");
  assert(!err && errno == EINVAL && !die);

  cout << "listdirectory" << endl;
  list<string> filenames;
  err = filesystem.listDirectory("dummy", filenames);
  assert(err == -1 && errno == EINVAL && filenames.empty() && !die);

  cout << "remove" << endl;
  err = filesystem.remove("dummy");
  assert(err == -1 && errno == EINVAL && !die);

  cout << "size" << endl;
  err = filesystem.size("dummy");
  assert(err == -1 && errno == EINVAL && !die);

  cout << "mkdir" << endl;
  err = filesystem.mkdir("dummy");
  assert(err == 0 && !die);

  cout << "datafile constructor" << endl;
  SMDataFile f("dummy", O_RDONLY, 12345);
  f = SMDataFile("dummy2", O_WRONLY, 123456);
  f = SMDataFile("dummy2", O_RDWR | O_APPEND, 1234567);

  cout << "pread" << endl;
  uint8_t buf[1024];
  err = f.pread(buf, 0, 1024);
  assert(err == -1 && errno == EINVAL && !die);

  cout << "read" << endl;
  err = f.read(buf, 1024);
  assert(err == -1 && errno == EINVAL && !die);

  cout << "write" << endl;
  err = f.write(buf, 1024);
  assert(err == -1 && errno == EINVAL && !die);

  cout << "seek" << endl;
  err = f.seek(1234, SEEK_SET);
  assert(err == 0 && !die);

  err = f.seek(1234, SEEK_CUR);
  assert(err == 0 && !die);

  err = f.seek(1234, SEEK_END);
  assert(err == -1 && errno == EINVAL && !die);

  cout << "truncate" << endl;
  err = f.truncate(1234);
  assert(err == -1 && errno == EINVAL && !die);

  cout << "size" << endl;
  err = f.size();
  assert(err == -1 && errno == EINVAL && !die);

  cout << "tell" << endl;
  err = f.tell();
  assert(err == 2468);

  cout << "flush" << endl;
  err = f.flush();
  assert(err == 0);

  cout << "mtime" << endl;
  err = f.mtime();
  assert(err == -1 && errno == EINVAL && !die);

  cout << "close" << endl;
  err = f.close();
  assert(err == 0);

  // done, return errCode
  die = true;
  cout << "done, waiting for server thread to stop" << endl;
  server_thread.join();
  return errCode;
}

int main()
{
  int ret;

  ret = test1();
  return ret;
}

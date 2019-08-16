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
#include "SMFileSystem.h"
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>


using namespace std;
using namespace storagemanager;

void usage(const char *progname)
{
    cerr << progname << " is like 'rm -rf' for files managed by StorageManager, but with no options or globbing" << endl;
    cerr << "Usage: " << progname << " file-or-dir1 file-or-dir2 .. file-or-dirN" << endl;
}

bool SMOnline()
{
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strcpy(&addr.sun_path[1], &socket_name[1]);   // first char is null...
    int clientSocket = ::socket(AF_UNIX, SOCK_STREAM, 0);
    int err = ::connect(clientSocket, (const struct sockaddr *) &addr, sizeof(addr));
    if (err >= 0)
    {
        ::close(clientSocket);
        return true;
    }
    return false;
}

void rmOffline(int argCount, const char **args)
{
    boost::scoped_ptr<IOCoordinator> ioc(IOCoordinator::get());
    for (int i = 1; i < argCount; i++)
        ioc->unlink(args[i]);
}

void rmOnline(int argCount, const char **args)
{
    idbdatafile::SMFileSystem fs;
    
    for (int i = 1; i < argCount; i++)
        fs.remove(args[i]);
}

int main(int argc, const char **argv)
{
    if (argc < 2)
    {
        usage(argv[0]);
        return 1;
    }
    
    if (SMOnline())
        rmOnline(argc, argv);
    else
        rmOffline(argc, argv);
    return 0;
}

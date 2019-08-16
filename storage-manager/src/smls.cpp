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

#include "IOCoordinator.h"
#include "SMFileSystem.h"
#include "messageFormat.h"

using namespace std;
using namespace storagemanager;

void usage(const char *progname)
{
    cerr << progname << " is like 'ls -l' for files managed by StorageManager" << endl;
    cerr << "Usage: " << progname << " directory" << endl;
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
        ::close(err);
        return true;
    }
    return false;
}

void lsOffline(const char *path)
{
    IOCoordinator *ioc = IOCoordinator::get();
    vector<string> listing;
    char buf[80];
    int err = ioc->listDirectory(path, &listing);
    if (err)
    {
        int l_errno = errno;
        cerr << strerror_r(l_errno, buf, 80) << endl;
        exit(1);
    }
    
    struct stat _stat;
    boost::filesystem::path base(path);
    boost::filesystem::path p;
    cout.fill(' ');
    for (auto &entry : listing)
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
            cout << right << _stat.st_size << left << " " << entry << endl;
        }
        else
        {
            cout.width(15);
            cout << right << "error" << left <<  " " << entry << endl;
        }
    }
    delete ioc;
}

void lsOnline(const char *path)
{
    idbdatafile::SMFileSystem fs;
    list<string> listing;
    char buf[80];
    
    int err = fs.listDirectory(path, listing);
    if (err)
    {
        int l_errno = errno;
        cerr << strerror_r(l_errno, buf, 80) << endl;
        exit(1);
    }
    
    boost::filesystem::path base(path);
    boost::filesystem::path p;
    cout.fill(' ');
    for (auto &entry : listing)
    {
        p = base / entry;
        bool isDir = fs.isDir(p.string().c_str());
        ssize_t size = fs.size(p.string().c_str());
        if (size >= 0)
        {
            if (isDir)
            {
                cout << "d";
                cout.width(14);
            }
            else
                cout.width(15);
            cout << right << size << left << " " << entry << endl;
        }
        else
        {
            cout << strerror_r(errno, buf, 80) << endl;
            cout.width(15);
            cout << right << "error" << left <<  " " << entry << endl;
        }
    }
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        usage(argv[0]);
        return 1;
    }
    
    // todo.  need to sanitize the input.  Ownership will remove X directories from the front
    // of whatever is passed to it, possibly giving a nonsensical answer.
    
    if (SMOnline())
        lsOnline(argv[1]);
    else
        lsOffline(argv[1]);
    
    return 0;
}


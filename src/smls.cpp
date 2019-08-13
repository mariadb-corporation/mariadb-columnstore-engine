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

#include "IOCoordinator.h"

using namespace std;
using namespace storagemanager;

void usage(const char *progname)
{
    cerr << progname << " is like 'ls -l' for files managed by StorageManager" << endl;
    cerr << "Usage: " << progname << " directory" << endl;
}

int main(int argc, char **argv)
{
    if (argc != 2)
    {
        usage(argv[0]);
        return 1;
    }
    
    IOCoordinator *ioc = IOCoordinator::get();
    vector<string> listing;
    char buf[80];
    int err = ioc->listDirectory(argv[1], &listing);
    if (err)
    {
        int l_errno = errno;
        cerr << strerror_r(l_errno, buf, 80) << endl;
        return 1;
    }
    
    struct stat _stat;
    boost::filesystem::path base(argv[1]);
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
    
    return 0;
}


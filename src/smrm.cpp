/* Copyright (C) 2019 MariaDB Corporaton

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

using namespace std;
using namespace storagemanager;

void usage(const char *progname)
{
    cerr << progname << " is like 'rm -rf' for files managed by StorageManager, but with no options or globbing" << endl;
    cerr << "Usage: " << progname << " file-or-dir1 file-or-dir2 .. file-or-dirN" << endl;
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        usage(argv[0]);
        return 1;
    }
    
    IOCoordinator *ioc = IOCoordinator::get();
    for (int i = 1; i < argc; i++)
        ioc->unlink(argv[i]);
        
    return 0;
}

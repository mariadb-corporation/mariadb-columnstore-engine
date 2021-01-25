/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2017 MariaDB
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

/******************************************************************************************
* $Id: getMySQLpw.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
*
******************************************************************************************/
/**
 * @file
 */

#include <my_global.h>
#include <unistd.h>
#include <iterator>
#include <numeric>
#include <deque>
#include <iostream>
#include <ostream>
#include <fstream>
#include <cstdlib>
#include <string>
#include <limits.h>
#include <sstream>
#include <exception>
#include <stdexcept>
#include <vector>
#include <stdio.h>
#include <ctype.h>
#include <netdb.h>
#include <sys/sysinfo.h>
#include <climits>
#include <cstring>
#include <glob.h>

#include "liboamcpp.h"
#include "installdir.h"

using namespace std;
using namespace oam;

int main(int argc, char* argv[])
{
    Oam oam;

    cout << oam::UnassignedName << endl;

    exit (0);

    string USER = "root";
    char* p = getenv("USER");

    if (p && *p)
        USER = p;

    string HOME = "/root";
    p = getenv("HOME");

    if (p && *p)
        HOME = p;

    string fileName = HOME + "/.my.cnf";

    ifstream file (fileName.c_str());

    if (!file)
    {
        cout << oam::UnassignedName << endl;
        exit (1);
    }

    char line[400];
    string buf;

    while (file.getline(line, 400))
    {
        buf = line;

        string::size_type pos = buf.find("root", 0);

        if (pos != string::npos)
        {
            file.getline(line, 400);
            buf = line;

            pos = buf.find("password", 0);

            if (pos != string::npos)
            {
                string::size_type pos1 = buf.find("=", pos);

                if (pos1 != string::npos)
                {
                    //password found

                    string password = buf.substr(pos1 + 2, 80);

                    cout << password << endl;
                    exit (0);
                }
            }
        }
    }

    file.close();

    cout << oam::UnassignedName << endl;

    exit (1);

}
// vim:ts=4 sw=4:


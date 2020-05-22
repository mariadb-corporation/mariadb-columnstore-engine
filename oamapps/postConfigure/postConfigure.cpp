/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation

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
* $Id: postConfigure.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
* List of files being updated by post-install configure:
*		mariadb/columnstore/etc/Columnstore.xml
*		mariadb/columnstore/etc/ProcessConfig.xml
*		/etc/rc.d/rc.local
*
******************************************************************************************/
/**
 * @file
 */

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

#include <arpa/inet.h>
#include <sys/socket.h>
#include <ifaddrs.h>

#include <string.h> /* for strncpy */
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>

#include <readline/history.h>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/tokenizer.hpp>

#include "mcsconfig.h"
#include "columnstoreversion.h"
#include "liboamcpp.h"
#include "configcpp.h"

using namespace std;
using namespace config;

#include "helpers.h"
using namespace installer;

#include "installdir.h"

bool updateBash(string homeDir);

int main(int argc, char* argv[])
{
    string cmd;
    string numBlocksPctParam = "";
    string totalUmMemoryParam = "";
    string homeDir = "/root";
    bool rootUser = true;

    //check if root-user
    int user;
    user = getuid();

    if (user != 0)
    {
        rootUser = false;
	}

    if (!rootUser)
    {
        char* p = getenv("HOME");

        if (p && *p)
            homeDir = p;
    }

	//get current time and date
	time_t now;
	now = time(NULL);
	struct tm tm;
	localtime_r(&now, &tm);
	char timestamp[200];
	strftime (timestamp, 200, "%m:%d:%y-%H:%M:%S", &tm);
	string currentDate = timestamp;
	string postConfigureLog = "/var/log/columnstore-postconfigure-" + currentDate;

	// perform single server install
	cout << endl << "Performing the Single Server Install." << endl << endl;

	if (!rootUser)
    {
		if (!updateBash(homeDir))
			cout << "updateBash error" << endl;
    }

	//check if dbrm data resides in older directory path and inform user if it does
	dbrmDirCheck();

    if (numBlocksPctParam.empty()) {
        numBlocksPctParam = "-";
    }
    if (totalUmMemoryParam.empty()) {
        totalUmMemoryParam = "-";
    }

	cmd = "columnstore_installer dummy.rpm dummy.rpm dummy.rpm dummy.rpm dummy.rpm initial dummy n --nodeps ' ' 1 " + numBlocksPctParam + " " + totalUmMemoryParam;
	system(cmd.c_str());
	exit(0);
}

bool updateBash(string homeDir)
{
    string fileName = homeDir + "/.bashrc";
    ifstream newFile (fileName.c_str());
    newFile.close();

    return true;
}
// vim:ts=4 sw=4:

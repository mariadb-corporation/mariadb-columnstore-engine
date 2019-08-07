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
* $Id$
*
******************************************************************************************/
#include <unistd.h>
#include <string>
using namespace std;

#include <boost/thread/mutex.hpp>
using namespace boost;

#ifdef _MSC_VER
#include "idbregistry.h"
#endif
#include "installdir.h"
#include "configcpp.h"

using namespace config;

#include <iostream> 
   
using namespace std;

namespace startup
{
/* static */
mutex StartUp::fInstallDirLock;
/* static */
string* StartUp::fInstallDirp = 0;

/* static */
const string StartUp::installDir()
{
    mutex::scoped_lock lk(fInstallDirLock);

    if (fInstallDirp)
        return *fInstallDirp;

#ifdef _MSC_VER
    fInstallDirp = new string("C:\\Calpont");
    string cfStr = IDBreadRegistry("");

    if (!cfStr.empty())
        *fInstallDirp = cfStr;

#else
    fInstallDirp = new string("/usr/local/mariadb/columnstore");
    //See if we can figure out the install dir in Linux...
    //1. env var COLUMNSTORE_INSTALL_DIR
    const char* p = 0;
    p = getenv("COLUMNSTORE_INSTALL_DIR");

    if (p && *p)
        *fInstallDirp = p;

    //2. up one level from current binary location?
    //3. fall back to /usr/local/mariadb/columnstore
#endif

    return *fInstallDirp;
}

/* static */
mutex StartUp::fTmpDirLock;
/* static */
string* StartUp::fTmpDirp = 0;

/* static */
const string StartUp::tmpDir()
{
    mutex::scoped_lock lk(fTmpDirLock);

    if (fTmpDirp)
        return *fTmpDirp;

#ifdef _MSC_VER
    fTmpDirp = new string("C:\\Calpont\Tmp");
    string cfStr = IDBreadRegistry("");

    if (!cfStr.empty())
        *fTmpDirp = cfStr;
    
    return *fTmpDirp;

#else

	//check for non-root user 
/*    const char* p = getenv("HOME");
	string homedir = p;

    if (homedir == "/root")
	{
		Config* sysConfig = Config::makeConfig();

		string TempFileDir;

		try
		{
			TempFileDir = sysConfig->getConfig("SystemConfig", "TempFileDir");
		}
		catch (...)
		{}

		fTmpDirp = new string("/tmp");
		
		*fTmpDirp = *fTmpDirp + TempFileDir;
	}
	else
	{
		// non-root user
		fTmpDirp = new string(homedir);

		*fTmpDirp = *fTmpDirp + "/.tmp";
	}
*/
	Config* sysConfig = Config::makeConfig();

	string TempFileDir;

	try
	{
		TempFileDir = sysConfig->getConfig("SystemConfig", "SystemTempFileDir");
	}
	catch (...)
	{}
	
	return TempFileDir;
	
#endif


}

}
// vim:ts=4 sw=4:


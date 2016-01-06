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

/******************************************************************************************
* $Id: getMySQLpw.cpp 64 2006-10-12 22:21:51Z dhill $
*
*
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

#include "liboamcpp.h"
#include "installdir.h"

using namespace std;
using namespace oam;

int main(int argc, char *argv[])
{
	Oam oam;

	//get mysql user password
	string mysqlpw = oam::UnassignedName;
	try {
		mysqlpw = oam.getMySQLPassword();
	}
	catch (...)
	{
		cout << oam::UnassignedName << endl;
		exit (1);
	}

	if ( mysqlpw == oam::UnassignedName ) {
		cout << oam::UnassignedName << endl;
		exit (1);
	}

	cout << mysqlpw << endl;

	try {
		oam.setSystemConfig("MySQLPasswordConfig", "y");
	}
	catch(...) {}

	exit (0);

}
// vim:ts=4 sw=4:


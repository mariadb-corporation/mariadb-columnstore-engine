/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporaton

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

#ifndef HELPERS_H__
#define HELPERS_H__

#include "liboamcpp.h"

using namespace messageqcpp;
namespace installer
{

typedef struct Child_Module_struct
{
	std::string     moduleName;
	std::string     moduleIP;
	std::string     hostName;
} ChildModule;

typedef std::vector<ChildModule> ChildModuleList;

extern bool waitForActive();
extern void dbrmDirCheck();
extern void mysqlSetup();
extern int sendMsgProcMon( std::string module, ByteStream msg, int requestID, int timeout );
extern int sendUpgradeRequest(int IserverTypeInstall, bool pmwithum);
extern int sendReplicationRequest(int IserverTypeInstall, std::string password, std::string mysqlPort, bool pmwithum);
extern void checkFilesPerPartion(int DBRootCount, Config* sysConfig);
extern void checkMysqlPort( string& mysqlPort, Config* sysConfig);
extern bool writeConfig(Config* sysConfig);
extern void checkSystemMySQLPort(std::string& mysqlPort, Config* sysConfig, std::string USER, std::string password, ChildModuleList childmodulelist, int IserverTypeInstall, bool pmwithum);
extern const char* callReadline(string prompt);
extern void callFree(const char* );

}

#endif


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
 * $Id: columnstoreDB.cpp 419 2007-07-22 17:18:00Z dhill $
 *
 ******************************************************************************************/
/**
 * @file
 */

#include <iostream>
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
#include <sys/signal.h>
#include <sys/types.h>

#include "liboamcpp.h"
#include "configcpp.h"
#include "installdir.h"
#include "mcsconfig.h"

using namespace std;
using namespace oam;
using namespace config;
#include "calpontsystemcatalog.h"
using namespace execplan;

namespace
{
void usage(char* prog)
{
  cout << endl;
  cout << "Usage: " << prog << " [options]" << endl;

  cout << endl;
  cout << "This utility is used to suspend and resume Columnstore Database Writes." << endl;
  cout << "Normally this would be done while performing Database Backups and" << endl;
  cout << "Restores " << endl;
  cout << endl;

  cout << "Options:" << endl;
  cout << "-c <command>   Command: suspend or resume" << endl << endl;
  cout << "-h             Display this help." << endl << endl;
}
}  // namespace

/******************************************************************************************
 * @brief   DisplayLockedTables
 *
 * purpose: Show the details of all the locks in tableLocks
 *          Used when attempting to suspend or stop the
 *          database, but there are table locks.
 *
 ******************************************************************************************/
void DisplayLockedTables(std::vector<BRM::TableLockInfo>& tableLocks, BRM::DBRM* pDBRM)
{
  cout << "The following tables are locked:" << endl;

  // Initial widths of columns to display. We pass thru the table
  // and see if we need to grow any of these.
  unsigned int lockIDColumnWidth = 6;       // "LockID"
  unsigned int tableNameColumnWidth = 12;   // "Name"
  unsigned int ownerColumnWidth = 7;        // "Process"
  unsigned int pidColumnWidth = 3;          // "PID"
  unsigned int sessionIDColumnWidth = 7;    // "Session"
  unsigned int createTimeColumnWidth = 12;  // "CreationTime"
  unsigned int dbrootColumnWidth = 7;       // "DBRoots"
  unsigned int stateColumnWidth = 9;        // "State"

  // Initialize System Catalog object used to get table name
  boost::shared_ptr<execplan::CalpontSystemCatalog> systemCatalogPtr =
      execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(0);

  std::string fullTblName;
  const char* tableState;

  // Make preliminary pass through the table locks in order to determine our
  // output column widths based on the data.  Min column widths are based on
  // the width of the column heading (except for the 'state' column).
  uint64_t maxLockID = 0;
  uint32_t maxPID = 0;
  int32_t maxSessionID = 0;
  int32_t minSessionID = 0;
  std::vector<std::string> createTimes;
  std::vector<std::string> pms;
  char cTimeBuffer[64];

  execplan::CalpontSystemCatalog::TableName tblName;

  for (unsigned idx = 0; idx < tableLocks.size(); idx++)
  {
    if (tableLocks[idx].id > maxLockID)
    {
      maxLockID = tableLocks[idx].id;
    }

    try
    {
      tblName = systemCatalogPtr->tableName(tableLocks[idx].tableOID);
    }
    catch (...)
    {
      tblName.schema.clear();
      tblName.table.clear();
    }

    fullTblName = tblName.toString();

    if (fullTblName.size() > tableNameColumnWidth)
    {
      tableNameColumnWidth = fullTblName.size();
    }

    if (tableLocks[idx].ownerName.length() > ownerColumnWidth)
    {
      ownerColumnWidth = tableLocks[idx].ownerName.length();
    }

    if (tableLocks[idx].ownerPID > maxPID)
    {
      maxPID = tableLocks[idx].ownerPID;
    }

    if (tableLocks[idx].ownerSessionID > maxSessionID)
    {
      maxSessionID = tableLocks[idx].ownerSessionID;
    }

    if (tableLocks[idx].ownerSessionID < minSessionID)
    {
      minSessionID = tableLocks[idx].ownerSessionID;
    }

    // Creation Time.
    // While we're at it, we save the time string off into a vector
    // so we can display it later without recalcing it.
    struct tm timeTM;
    localtime_r(&tableLocks[idx].creationTime, &timeTM);
    ctime_r(&tableLocks[idx].creationTime, cTimeBuffer);
    strftime(cTimeBuffer, 64, "%F %r:", &timeTM);
    cTimeBuffer[strlen(cTimeBuffer) - 1] = '\0';  // strip trailing '\n'
    std::string cTimeStr(cTimeBuffer);

    if (cTimeStr.length() > createTimeColumnWidth)
    {
      createTimeColumnWidth = cTimeStr.length();
    }

    createTimes.push_back(cTimeStr);
  }

  tableNameColumnWidth += 1;
  ownerColumnWidth += 1;
  createTimeColumnWidth += 1;

  std::ostringstream idString;
  idString << maxLockID;

  if (idString.str().length() > lockIDColumnWidth)
    lockIDColumnWidth = idString.str().length();

  lockIDColumnWidth += 1;

  std::ostringstream pidString;
  pidString << maxPID;

  if (pidString.str().length() > pidColumnWidth)
    pidColumnWidth = pidString.str().length();

  pidColumnWidth += 1;

  const std::string sessionNoneStr("BulkLoad");
  std::ostringstream sessionString;
  sessionString << maxSessionID;

  if (sessionString.str().length() > sessionIDColumnWidth)
    sessionIDColumnWidth = sessionString.str().length();

  if ((minSessionID < 0) && (sessionNoneStr.length() > sessionIDColumnWidth))
    sessionIDColumnWidth = sessionNoneStr.length();

  sessionIDColumnWidth += 1;

  // write the column headers before the first entry
  cout.setf(ios::left, ios::adjustfield);
  cout << setw(lockIDColumnWidth) << "LockID" << setw(tableNameColumnWidth) << "Name"
       << setw(ownerColumnWidth) << "Process" << setw(pidColumnWidth) << "PID" << setw(sessionIDColumnWidth)
       << "Session" << setw(createTimeColumnWidth) << "CreationTime" << setw(stateColumnWidth) << "State"
       << setw(dbrootColumnWidth) << "DBRoots" << endl;

  for (unsigned idx = 0; idx < tableLocks.size(); idx++)
  {
    try
    {
      tblName = systemCatalogPtr->tableName(tableLocks[idx].tableOID);
    }
    catch (...)
    {
      tblName.schema.clear();
      tblName.table.clear();
    }

    fullTblName = tblName.toString();
    cout << setw(lockIDColumnWidth) << tableLocks[idx].id << setw(tableNameColumnWidth) << fullTblName
         << setw(ownerColumnWidth) << tableLocks[idx].ownerName << setw(pidColumnWidth)
         << tableLocks[idx].ownerPID;

    // Log session ID, or "BulkLoad" if session is -1
    if (tableLocks[idx].ownerSessionID < 0)
      cout << setw(sessionIDColumnWidth) << sessionNoneStr;
    else
      cout << setw(sessionIDColumnWidth) << tableLocks[idx].ownerSessionID;

    // Creation Time
    cout << setw(createTimeColumnWidth) << createTimes[idx];

    // Processor State
    if (pDBRM && !pDBRM->checkOwner(tableLocks[idx].id))
    {
      tableState = "Abandoned";
    }
    else
    {
      tableState = ((tableLocks[idx].state == BRM::LOADING) ? "LOADING" : "CLEANUP");
    }

    cout << setw(stateColumnWidth) << tableState;

    // PM List
    cout << setw(dbrootColumnWidth);

    for (unsigned k = 0; k < tableLocks[idx].dbrootList.size(); k++)
    {
      if (k > 0)
        cout << ',';

      cout << tableLocks[idx].dbrootList[k];
    }

    cout << endl;
  }  // end of loop through table locks
}

int main(int argc, char** argv)
{
  string command;
  Oam oam;
  BRM::DBRM dbrm;

  char c;

  // Invokes member function `int operator ()(void);'
  while ((c = getopt(argc, argv, "c:h")) != -1)
  {
    switch (c)
    {
      case 'c': command = optarg; break;

      case 'h':
        usage(argv[0]);
        exit(-1);
        break;

      default:
        usage(argv[0]);
        exit(1);
        break;
    }
  }

  if (command == "suspend")
  {
    try
    {
      std::vector<BRM::TableLockInfo> tableLocks = dbrm.getAllTableLocks();

      if (!tableLocks.empty())
      {
        DisplayLockedTables(tableLocks, &dbrm);
      }
      else
      {
        dbrm.setSystemSuspended(true);
        sleep(5);
        string cmd = "save_brm  > " + string(MCSLOGDIR) + "/save_brm.log1 2>&1";
        int rtnCode = system(cmd.c_str());

        if (rtnCode == 0)
        {
          cout << endl << "Suspend Columnstore Database Writes Request successfully completed" << endl;
        }
        else
        {
          cout << endl << "Suspend Columnstore Database Writes Failed: save_brm Failed" << endl;
          dbrm.setSystemSuspended(false);
        }
      }
    }
    catch (exception& e)
    {
      cout << endl << "**** Suspend Columnstore Database Writes Failed: " << e.what() << endl;
    }
    catch (...)
    {
      cout << endl << "**** Suspend Columnstore Database Writes Failed" << endl;
    }
  }
  else
  {
    if (command == "resume")
    {
      try
      {
        dbrm.setSystemSuspended(false);
        cout << endl << "Resume Columnstore Database Writes Request successfully completed" << endl;
      }
      catch (exception& e)
      {
        cout << endl << "**** Resume Columnstore Database Writes Failed: " << e.what() << endl;
      }
      catch (...)
      {
        cout << endl << "**** Resume Columnstore Database Writes Failed" << endl;
      }
    }
    else
    {
      cout << "Invalid Command Entered, please try again" << endl;
      exit(-1);
    }
  }

  exit(0);
}

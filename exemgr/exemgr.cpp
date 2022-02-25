/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporation

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

/**********************************************************************
 *   $Id: main.cpp 1000 2013-07-24 21:05:51Z pleblanc $
 *
 *
 ***********************************************************************/
/**
 * @brief execution plan manager main program
 *
 * This is the ?main? program for dealing with execution plans and
 * result sets. It sits in a loop waiting for CalpontExecutionPlan
 * from the FEP. It then passes the CalpontExecutionPlan to the
 * JobListFactory from which a JobList is obtained. This is passed
 * to to the Query Manager running in the real-time portion of the
 * EC. The ExecutionPlanManager waits until the Query Manager
 * returns a result set for the job list. These results are passed
 * into the CalpontResultFactory, which outputs a CalpontResultSet.
 * The ExecutionPlanManager passes the CalpontResultSet into the
 * VendorResultFactory which produces a result set tailored to the
 * specific DBMS front end in use. The ExecutionPlanManager then
 * sends the VendorResultSet back to the Calpont Database Connector
 * on the Front-End Processor where it is returned to the DBMS
 * front-end.
 */
#include <iostream>
#include <cstdint>
#include <csignal>
#include <sys/resource.h>

#undef root_name
#include <boost/filesystem.hpp>

#include "calpontselectexecutionplan.h"
#include "mcsanalyzetableexecutionplan.h"
#include "activestatementcounter.h"
#include "distributedenginecomm.h"
#include "resourcemanager.h"
#include "configcpp.h"
#include "queryteleserverparms.h"
#include "iosocket.h"
#include "joblist.h"
#include "joblistfactory.h"
#include "oamcache.h"
#include "simplecolumn.h"
#include "bytestream.h"
#include "telestats.h"
#include "messageobj.h"
#include "messagelog.h"
#include "sqllogger.h"
#include "femsghandler.h"
#include "idberrorinfo.h"
#include "MonitorProcMem.h"
#include "liboamcpp.h"
#include "crashtrace.h"
#include "service.h"

#include <mutex>
#include <thread>
#include <condition_variable>

#include "dbrm.h"

#include "mariadb_my_sys.h"
#include "statistics.h"
#include "serviceexemgr.h"

int main(int argc, char* argv[])
{
  opterr = 0;
  exemgr::Opt opt(argc, argv);

  // Set locale language
  setlocale(LC_ALL, "");
  setlocale(LC_NUMERIC, "C");

  // This is unset due to the way we start it
  // program_invocation_short_name = const_cast<char*>("ExeMgr");

  // Initialize the charset library
  MY_INIT(argv[0]);
  // global ptr defined in serviceexemgr.cpp
  exemgr::globServiceExeMgr = new exemgr::ServiceExeMgr(opt);
  return exemgr::globServiceExeMgr->Run();
}


/* Copyright (C) 2022 Mariadb Corporation.

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

#pragma once

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

namespace exemgr
{
  class SQLFrontSessionThread
  {
  public:
    SQLFrontSessionThread(const messageqcpp::IOSocket& ios, joblist::DistributedEngineComm* ec,
                  joblist::ResourceManager* rm)
    : fIos(ios)
    , fEc(ec)
    , fRm(rm)
    , fStatsRetrieved(false)
    , fTeleClient(globServiceExeMgr->getTeleServerParms())
    , fOamCachePtr(oam::OamCache::makeOamCache())
    {
    }

  private:
    messageqcpp::IOSocket fIos;
    joblist::DistributedEngineComm* fEc;
    joblist::ResourceManager* fRm;
    querystats::QueryStats fStats;

    // Variables used to store return stats
    bool fStatsRetrieved;

    querytele::QueryTeleClient fTeleClient;

    oam::OamCache* fOamCachePtr;  // this ptr is copyable...

    //...Reinitialize stats for start of a new query
    void initStats(uint32_t sessionId, std::string& sqlText)
    {
      initMaxMemPct(sessionId);

      fStats.reset();
      fStats.setStartTime();
      fStats.fSessionID = sessionId;
      fStats.fQuery = sqlText;
      fStatsRetrieved = false;
    }
    //...Get % memory usage during latest query for sesssionId.
    //...SessionId >= 0x80000000 is system catalog query we can ignore.
    static uint64_t getMaxMemPct(uint32_t sessionId);
    //...Delete sessionMemMap entry for the specified session's memory % use.
    //...SessionId >= 0x80000000 is system catalog query we can ignore.
    static void deleteMaxMemPct(uint32_t sessionId);
    //...Get and log query stats to specified output stream
    const std::string formatQueryStats(
        joblist::SJLP& jl,         // joblist associated with query
        const std::string& label,  // header label to print in front of log output
        bool includeNewLine,       // include line breaks in query stats std::string
        bool vtableModeOn, bool wantExtendedStats, uint64_t rowsReturned);
    static void incThreadCntPerSession(uint32_t sessionId);
    static void decThreadCntPerSession(uint32_t sessionId);
    //...Init sessionMemMap entry for specified session to 0 memory %.
    //...SessionId >= 0x80000000 is system catalog query we can ignore.
    static void initMaxMemPct(uint32_t sessionId);
    //... Round off to human readable format (KB, MB, or GB).
    const std::string roundBytes(uint64_t value) const;
    void setRMParms(const execplan::CalpontSelectExecutionPlan::RMParmVec& parms);
    void buildSysCache(const execplan::CalpontSelectExecutionPlan& csep,
                      boost::shared_ptr<execplan::CalpontSystemCatalog> csc);
    void writeCodeAndError(messageqcpp::ByteStream::quadbyte code, const std::string emsg);
    void analyzeTableExecute(messageqcpp::ByteStream& bs, joblist::SJLP& jl, bool& stmtCounted);
    void analyzeTableHandleStats(messageqcpp::ByteStream& bs);
    uint64_t roundMB(uint64_t value) const;
  public:
    void operator()();
  };
}

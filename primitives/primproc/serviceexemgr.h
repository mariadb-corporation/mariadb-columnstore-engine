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

namespace exemgr
{
  class Opt
  {
  public:
    int m_debug;
    bool m_e;
    bool m_fg;
    Opt() : m_debug(0), m_e(false), m_fg(false) {};
    Opt(int argc, char* argv[]) : m_debug(0), m_e(false), m_fg(false)
    {
      int c;
      while ((c = getopt(argc, argv, "edf")) != EOF)
      {
        switch (c)
        {
          case 'd': m_debug++; break;

          case 'e': m_e = true; break;

          case 'f': m_fg = true; break;

          case '?':
          default: break;
        }
      }
    }
    int getDebugLevel() const
    {
      return m_debug;
    }
  };

  class ServiceExeMgr : public Service, public Opt
  {
    using SessionMemMap_t = std::map<uint32_t, size_t>;
    using ThreadCntPerSessionMap_t = std::map<uint32_t, uint32_t>;
  protected:
    void log(logging::LOG_TYPE type, const std::string& str)
    {
      logging::LoggingID logid(16);
      logging::Message::Args args;
      logging::Message message(8);
      args.add(strerror(errno));
      message.format(args);
      logging::Logger logger(logid.fSubsysID);
      logger.logMessage(type, message, logid);
    }

  public:
    ServiceExeMgr(const Opt& opt) : Service("ExeMgr"), Opt(opt), msgLog_(logging::Logger(16))
    {
      bool runningWithExeMgr = true;
      rm_ = joblist::ResourceManager::instance(runningWithExeMgr);
    }
    ServiceExeMgr(const Opt& opt, config::Config* aConfig) : Service("ExeMgr"), Opt(opt), msgLog_(logging::Logger(16))
    {
      bool runningWithExeMgr = true;
      rm_ = joblist::ResourceManager::instance(runningWithExeMgr, aConfig);
    }
    void LogErrno() override
    {
      log(logging::LOG_TYPE_CRITICAL, std::string(strerror(errno)));
    }
    void ParentLogChildMessage(const std::string& str) override
    {
      log(logging::LOG_TYPE_INFO, str);
    }
    int Child() override;
    int Run()
    {
      return m_fg ? Child() : RunForking();
    }
    static const constexpr unsigned logDefaultMsg = logging::M0000;
    static const constexpr unsigned logDbProfStartStatement = logging::M0028;
    static const constexpr unsigned logDbProfEndStatement = logging::M0029;
    static const constexpr unsigned logStartSql = logging::M0041;
    static const constexpr unsigned logEndSql = logging::M0042;
    static const constexpr unsigned logRssTooBig = logging::M0044;
    static const constexpr unsigned logDbProfQueryStats = logging::M0047;
    static const constexpr unsigned logExeMgrExcpt = logging::M0055;
    // If any flags other than the table mode flags are set, produce output to screeen
    static const constexpr uint32_t flagsWantOutput = (0xffffffff & ~execplan::CalpontSelectExecutionPlan::TRACE_TUPLE_AUTOSWITCH &
      ~execplan::CalpontSelectExecutionPlan::TRACE_TUPLE_OFF);
    logging::Logger& getLogger()
    {
      return msgLog_;
    }
    void updateSessionMap(const size_t pct)
    {
      std::lock_guard<std::mutex> lk(sessionMemMapMutex_);

      for (auto mapIter = sessionMemMap_.begin(); mapIter != sessionMemMap_.end(); ++mapIter)
      {
        if (pct > mapIter->second)
        {
          mapIter->second = pct;
        }
      }
    }
    ThreadCntPerSessionMap_t& getThreadCntPerSessionMap()
    {
      return threadCntPerSessionMap_;
    }
    std::mutex& getThreadCntPerSessionMapMutex()
    {
      return threadCntPerSessionMapMutex_;
    }
    void initMaxMemPct(uint32_t sessionId)
    {
      // WIP
      if (sessionId < 0x80000000)
      {
        std::lock_guard<std::mutex> lk(sessionMemMapMutex_);
        auto mapIter = sessionMemMap_.find(sessionId);

        if (mapIter == sessionMemMap_.end())
        {
          sessionMemMap_[sessionId] = 0;
        }
        else
        {
          mapIter->second = 0;
        }
      }
    }
    uint64_t getMaxMemPct(const uint32_t sessionId)
    {
      uint64_t maxMemoryPct = 0;
      // WIP
      if (sessionId < 0x80000000)
      {
        std::lock_guard<std::mutex> lk(sessionMemMapMutex_);
        auto mapIter = sessionMemMap_.find(sessionId);

        if (mapIter != sessionMemMap_.end())
        {
          maxMemoryPct = (uint64_t)mapIter->second;
        }
      }

      return maxMemoryPct;
    }
    void deleteMaxMemPct(uint32_t sessionId)
    {
      if (sessionId < 0x80000000)
      {
        std::lock_guard<std::mutex> lk(sessionMemMapMutex_);
        auto mapIter = sessionMemMap_.find(sessionId);

        if (mapIter != sessionMemMap_.end())
        {
          sessionMemMap_.erase(sessionId);
        }
      }
    }
    //...Increment the number of threads using the specified sessionId
    void incThreadCntPerSession(uint32_t sessionId)
    {
      std::lock_guard<std::mutex> lk(threadCntPerSessionMapMutex_);
      auto mapIter = threadCntPerSessionMap_.find(sessionId);

      if (mapIter == threadCntPerSessionMap_.end())
        threadCntPerSessionMap_.insert(ThreadCntPerSessionMap_t::value_type(sessionId, 1));
      else
        mapIter->second++;
    }
    //...Decrement the number of threads using the specified sessionId.
    //...When the thread count for a sessionId reaches 0, the corresponding
    //...CalpontSystemCatalog objects are deleted.
    //...The user query and its associated catalog query have a different
    //...session Id where the highest bit is flipped.
    //...The object with id(sessionId | 0x80000000) cannot be removed before
    //...user query session completes because the sysdata may be used for
    //...debugging/stats purpose, such as result graph, etc.
    void decThreadCntPerSession(uint32_t sessionId)
    {
      std::lock_guard<std::mutex> lk(threadCntPerSessionMapMutex_);
      auto mapIter = threadCntPerSessionMap_.find(sessionId);

      if (mapIter != threadCntPerSessionMap_.end())
      {
        if (--mapIter->second == 0)
        {
          threadCntPerSessionMap_.erase(mapIter);
          execplan::CalpontSystemCatalog::removeCalpontSystemCatalog(sessionId);
          execplan::CalpontSystemCatalog::removeCalpontSystemCatalog((sessionId ^ 0x80000000));
        }
      }
    }
    ActiveStatementCounter* getStatementsRunningCount()
    {
      return statementsRunningCount_;
    }
    joblist::DistributedEngineComm* getDec()
    {
      return dec_;
    }
    int toInt(const std::string& val)
    {
      if (val.length() == 0)
        return -1;

      return static_cast<int>(config::Config::fromText(val));
    }
    const std::string prettyPrintMiniInfo(const std::string& in);

    const std::string timeNow()
    {
      time_t outputTime = time(0);
      struct tm ltm;
      char buf[32];  // ctime(3) says at least 26
      size_t len = 0;
      asctime_r(localtime_r(&outputTime, &ltm), buf);
      len = strlen(buf);

      if (len > 0)
        --len;

      if (buf[len] == '\n')
        buf[len] = 0;

      return buf;
    }
    querytele::QueryTeleServerParms& getTeleServerParms()
    {
      return teleServerParms_;
    }
    joblist::ResourceManager& getRm()
    {
      return *rm_;
    }
  private:
    void setupSignalHandlers();
    int8_t setupCwd()
    {
      std::string workdir = rm_->getScWorkingDir();
      int8_t rc = chdir(workdir.c_str());

      if (rc < 0 || access(".", W_OK) != 0)
        rc = chdir("/tmp");

      return (rc < 0) ? -5 : rc;
    }
    int setupResources()
    {
      struct rlimit rlim;

      if (getrlimit(RLIMIT_NOFILE, &rlim) != 0)
      {
        return -1;
      }
      rlim.rlim_cur = rlim.rlim_max = 65536;

      if (setrlimit(RLIMIT_NOFILE, &rlim) != 0)
      {
        return -2;
      }
      if (getrlimit(RLIMIT_NOFILE, &rlim) != 0)
      {
        return -3;
      }
      if (rlim.rlim_cur != 65536)
      {
        return -4;
      }
      return 0;
    }

    logging::Logger msgLog_;
    SessionMemMap_t sessionMemMap_; // track memory% usage during a query
    std::mutex sessionMemMapMutex_;
    //...The FrontEnd may establish more than 1 connection (which results in
    //   more than 1 ExeMgr thread) per session.  These threads will share
    //   the same CalpontSystemCatalog object for that session.  Here, we
    //   define a std::map to track how many threads are sharing each session, so
    //   that we know when we can safely delete a CalpontSystemCatalog object
    //   shared by multiple threads per session.
    ThreadCntPerSessionMap_t threadCntPerSessionMap_;
    std::mutex threadCntPerSessionMapMutex_;
    ActiveStatementCounter* statementsRunningCount_;
    joblist::DistributedEngineComm* dec_;
    joblist::ResourceManager* rm_;
    // Its attributes are set in Child()
    querytele::QueryTeleServerParms teleServerParms_;
  };
  extern ServiceExeMgr* globServiceExeMgr;
}
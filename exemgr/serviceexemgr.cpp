/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019-22 MariaDB Corporation

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
#include "sqlfrontsessionthread.h"


namespace exemgr
{
  ServiceExeMgr* globServiceExeMgr = nullptr;
  void startRssMon(size_t maxPct, int pauseSeconds);

  void added_a_pm(int)
  {
    logging::LoggingID logid(21, 0, 0);
    logging::Message::Args args1;
    logging::Message msg(1);
    args1.add("exeMgr caught SIGHUP. Resetting connections");
    msg.format(args1);
    std::cout << msg.msg().c_str() << std::endl;
    logging::Logger logger(logid.fSubsysID);
    logger.logMessage(logging::LOG_TYPE_DEBUG, msg, logid);

    auto* dec = exemgr::globServiceExeMgr->getDec();
    if (dec)
    {
      oam::OamCache* oamCache = oam::OamCache::makeOamCache();
      oamCache->forceReload();
      dec->Setup();
    }
  }

  void printTotalUmMemory(int sig)
  {
    int64_t num = globServiceExeMgr->getRm().availableMemory();
    std::cout << "Total UM memory available: " << num << std::endl;
  }

  void ServiceExeMgr::setupSignalHandlers()
  {
    struct sigaction ign;

    memset(&ign, 0, sizeof(ign));
    ign.sa_handler = SIG_IGN;

    sigaction(SIGPIPE, &ign, 0);

    memset(&ign, 0, sizeof(ign));
    ign.sa_handler = exemgr::added_a_pm;
    sigaction(SIGHUP, &ign, 0);
    ign.sa_handler = exemgr::printTotalUmMemory;
    sigaction(SIGUSR1, &ign, 0);
    memset(&ign, 0, sizeof(ign));
    ign.sa_handler = fatalHandler;
    sigaction(SIGSEGV, &ign, 0);
    sigaction(SIGABRT, &ign, 0);
    sigaction(SIGFPE, &ign, 0);
  }

  void cleanTempDir()
  {
    using TempDirPurpose = config::Config::TempDirPurpose;
    struct Dirs
    {
      std::string section;
      std::string allowed;
      TempDirPurpose purpose;
    };
    std::vector<Dirs> dirs{{"HashJoin", "AllowDiskBasedJoin", TempDirPurpose::Joins},
                          {"RowAggregation", "AllowDiskBasedAggregation", TempDirPurpose::Aggregates}};
    const auto config = config::Config::makeConfig();

    for (const auto& dir : dirs)
    {
      std::string allowStr = config->getConfig(dir.section, dir.allowed);
      bool allow = (allowStr == "Y" || allowStr == "y");

      std::string tmpPrefix = config->getTempFileDir(dir.purpose);

      if (allow && tmpPrefix.empty())
      {
        std::cerr << "Empty tmp directory name for " << dir.section << std::endl;
        logging::LoggingID logid(16, 0, 0);
        logging::Message::Args args;
        logging::Message message(8);
        args.add("Empty tmp directory name for:");
        args.add(dir.section);
        message.format(args);
        logging::Logger logger(logid.fSubsysID);
        logger.logMessage(logging::LOG_TYPE_CRITICAL, message, logid);
      }

      tmpPrefix += "/";

      idbassert(tmpPrefix != "/");

      /* This is quite scary as ExeMgr usually runs as root */
      try
      {
        if (allow)
        {
          boost::filesystem::remove_all(tmpPrefix);
        }
        boost::filesystem::create_directories(tmpPrefix);
      }
      catch (const std::exception& ex)
      {
        std::cerr << ex.what() << std::endl;
        logging::LoggingID logid(16, 0, 0);
        logging::Message::Args args;
        logging::Message message(8);
        args.add("Exception whilst cleaning tmpdir: ");
        args.add(ex.what());
        message.format(args);
        logging::Logger logger(logid.fSubsysID);
        logger.logMessage(logging::LOG_TYPE_WARNING, message, logid);
      }
      catch (...)
      {
        std::cerr << "Caught unknown exception during tmpdir cleanup" << std::endl;
        logging::LoggingID logid(16, 0, 0);
        logging::Message::Args args;
        logging::Message message(8);
        args.add("Unknown exception whilst cleaning tmpdir");
        message.format(args);
        logging::Logger logger(logid.fSubsysID);
        logger.logMessage(logging::LOG_TYPE_WARNING, message, logid);
      }
    }
  }

  const std::string ServiceExeMgr::prettyPrintMiniInfo(const std::string& in)
  {
    // 1. take the std::string and tok it by '\n'
    // 2. for each part in each line calc the longest part
    // 3. padding to each longest value, output a header and the lines
    typedef boost::tokenizer<boost::char_separator<char> > my_tokenizer;
    boost::char_separator<char> sep1("\n");
    my_tokenizer tok1(in, sep1);
    std::vector<std::string> lines;
    std::string header = "Desc Mode Table TableOID ReferencedColumns PIO LIO PBE Elapsed Rows";
    const int header_parts = 10;
    lines.push_back(header);

    for (my_tokenizer::const_iterator iter1 = tok1.begin(); iter1 != tok1.end(); ++iter1)
    {
      if (!iter1->empty())
        lines.push_back(*iter1);
    }

    std::vector<unsigned> lens;

    for (int i = 0; i < header_parts; i++)
      lens.push_back(0);

    std::vector<std::vector<std::string> > lineparts;
    std::vector<std::string>::iterator iter2;
    int j;

    for (iter2 = lines.begin(), j = 0; iter2 != lines.end(); ++iter2, j++)
    {
      boost::char_separator<char> sep2(" ");
      my_tokenizer tok2(*iter2, sep2);
      int i;
      std::vector<std::string> parts;
      my_tokenizer::iterator iter3;

      for (iter3 = tok2.begin(), i = 0; iter3 != tok2.end(); ++iter3, i++)
      {
        if (i >= header_parts)
          break;

        std::string part(*iter3);

        if (j != 0 && i == 8)
          part.resize(part.size() - 3);

        assert(i < header_parts);

        if (part.size() > lens[i])
          lens[i] = part.size();

        parts.push_back(part);
      }

      assert(i == header_parts);
      lineparts.push_back(parts);
    }

    std::ostringstream oss;

    std::vector<std::vector<std::string> >::iterator iter1 = lineparts.begin();
    std::vector<std::vector<std::string> >::iterator end1 = lineparts.end();

    oss << "\n";

    while (iter1 != end1)
    {
      std::vector<std::string>::iterator iter2 = iter1->begin();
      std::vector<std::string>::iterator end2 = iter1->end();
      assert(distance(iter2, end2) == header_parts);
      int i = 0;

      while (iter2 != end2)
      {
        assert(i < header_parts);
        oss << std::setw(lens[i]) << std::left << *iter2 << " ";
        ++iter2;
        i++;
      }

      oss << "\n";
      ++iter1;
    }

    return oss.str();
  }

  int ServiceExeMgr::Child()
  {
    // Make sure CSC thinks it's on a UM or else bucket reuse stuff below will stall
    if (!m_e)
      setenv("CALPONT_CSC_IDENT", "um", 1);

    setupSignalHandlers();
    int err = 0;
    if (!m_debug)
      err = setupResources();
    std::string errMsg;

    switch (err)
    {
      case -1:
      case -3: errMsg = "Error getting file limits, please see non-root install documentation"; break;

      case -2: errMsg = "Error setting file limits, please see non-root install documentation"; break;

      case -4:
        errMsg = "Could not install file limits to required value, please see non-root install documentation";
        break;

      default: errMsg = "Couldn't change working directory or unknown error"; break;
    }

    err = setupCwd();

    if (err < 0)
    {
      oam::Oam oam;
      logging::Message::Args args;
      logging::Message message;
      args.add(errMsg);
      message.format(args);
      logging::LoggingID lid(16);
      logging::MessageLog ml(lid);
      ml.logCriticalMessage(message);
      std::cerr << errMsg << std::endl;

      NotifyServiceInitializationFailed();
      return 2;
    }

    cleanTempDir();

    logging::MsgMap msgMap;
    msgMap[logDefaultMsg] = logging::Message(logDefaultMsg);
    msgMap[logDbProfStartStatement] = logging::Message(logDbProfStartStatement);
    msgMap[logDbProfEndStatement] = logging::Message(logDbProfEndStatement);
    msgMap[logStartSql] = logging::Message(logStartSql);
    msgMap[logEndSql] = logging::Message(logEndSql);
    msgMap[logRssTooBig] = logging::Message(logRssTooBig);
    msgMap[logDbProfQueryStats] = logging::Message(logDbProfQueryStats);
    msgMap[logExeMgrExcpt] = logging::Message(logExeMgrExcpt);
    msgLog_.msgMap(msgMap);

    dec_ = joblist::DistributedEngineComm::instance(rm_, true);
    dec_->Open();

    bool tellUser = true;

    messageqcpp::MessageQueueServer* mqs;

    statementsRunningCount_ = new ActiveStatementCounter(rm_->getEmExecQueueSize());
    const std::string ExeMgr = "ExeMgr1";
    for (;;)
    {
      try
      {
        mqs = new messageqcpp::MessageQueueServer(ExeMgr, rm_->getConfig(), messageqcpp::ByteStream::BlockSize, 64);
        break;
      }
      catch (std::runtime_error& re)
      {
        std::string what = re.what();

        if (what.find("Address already in use") != std::string::npos)
        {
          if (tellUser)
          {
            std::cerr << "Address already in use, retrying..." << std::endl;
            tellUser = false;
          }

          sleep(5);
        }
        else
        {
          throw;
        }
      }
    }

    // class jobstepThreadPool is used by other processes. We can't call
    // resourcemanaager (rm) functions during the static creation of threadpool
    // because rm has a "isExeMgr" flag that is set upon creation (rm is a singleton).
    // From  the pools perspective, it has no idea if it is ExeMgr doing the
    // creation, so it has no idea which way to set the flag. So we set the max here.
    joblist::JobStep::jobstepThreadPool.setMaxThreads(rm_->getJLThreadPoolSize());
    joblist::JobStep::jobstepThreadPool.setName("ExeMgrJobList");

    if (rm_->getJlThreadPoolDebug() == "Y" || rm_->getJlThreadPoolDebug() == "y")
    {
      joblist::JobStep::jobstepThreadPool.setDebug(true);
      joblist::JobStep::jobstepThreadPool.invoke(
          threadpool::ThreadPoolMonitor(&joblist::JobStep::jobstepThreadPool));
    }

    int serverThreads = rm_->getEmServerThreads();
    int maxPct = rm_->getEmMaxPct();
    int pauseSeconds = rm_->getEmSecondsBetweenMemChecks();
    int priority = rm_->getEmPriority();

    FEMsgHandler::threadPool.setMaxThreads(serverThreads);
    FEMsgHandler::threadPool.setName("FEMsgHandler");

    if (maxPct > 0)
    {
      // Defined in rssmonfcn.cpp
      exemgr::startRssMon(maxPct, pauseSeconds);
    }

    setpriority(PRIO_PROCESS, 0, priority);

    std::string teleServerHost(rm_->getConfig()->getConfig("QueryTele", "Host"));

    if (!teleServerHost.empty())
    {
      int teleServerPort = toInt(rm_->getConfig()->getConfig("QueryTele", "Port"));

      if (teleServerPort > 0)
      {
        teleServerParms_ = querytele::QueryTeleServerParms(teleServerHost, teleServerPort);
      }
    }

    NotifyServiceStarted();

    std::cout << "Starting ExeMgr: st = " << serverThreads << ", qs = " << rm_->getEmExecQueueSize()
              << ", mx = " << maxPct << ", cf = " << rm_->getConfig()->configFile() << std::endl;

    {
      BRM::DBRM* dbrm = new BRM::DBRM();
      dbrm->setSystemQueryReady(true);
      delete dbrm;
    }

    threadpool::ThreadPool exeMgrThreadPool(serverThreads, 0);
    exeMgrThreadPool.setName("ExeMgrServer");

    if (rm_->getExeMgrThreadPoolDebug() == "Y" || rm_->getExeMgrThreadPoolDebug() == "y")
    {
      exeMgrThreadPool.setDebug(true);
      exeMgrThreadPool.invoke(threadpool::ThreadPoolMonitor(&exeMgrThreadPool));
    }

    // Load statistics.
    try
    {
      statistics::StatisticsManager::instance()->loadFromFile();
    }
    catch (...)
    {
      std::cerr << "Cannot load statistics from file " << std::endl;
    }

    for (;;)
    {
      messageqcpp::IOSocket ios;
      ios = mqs->accept();
      exeMgrThreadPool.invoke(exemgr::SQLFrontSessionThread(ios, dec_, rm_));
    }

    exeMgrThreadPool.wait();

    return 0;
  }
} // namespace

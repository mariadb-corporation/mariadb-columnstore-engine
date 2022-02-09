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
#include "threadnaming.h"

class Opt
{
 public:
  int m_debug;
  bool m_e;
  bool m_fg;
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
};

class ServiceExeMgr : public Service, public Opt
{
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
  ServiceExeMgr(const Opt& opt) : Service("ExeMgr"), Opt(opt)
  {
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
};

namespace
{
// If any flags other than the table mode flags are set, produce output to screeen
const uint32_t flagsWantOutput = (0xffffffff & ~execplan::CalpontSelectExecutionPlan::TRACE_TUPLE_AUTOSWITCH &
                                  ~execplan::CalpontSelectExecutionPlan::TRACE_TUPLE_OFF);

int gDebug;

const unsigned logDefaultMsg = logging::M0000;
const unsigned logDbProfStartStatement = logging::M0028;
const unsigned logDbProfEndStatement = logging::M0029;
const unsigned logStartSql = logging::M0041;
const unsigned logEndSql = logging::M0042;
const unsigned logRssTooBig = logging::M0044;
const unsigned logDbProfQueryStats = logging::M0047;
const unsigned logExeMgrExcpt = logging::M0055;

logging::Logger msgLog(16);

typedef std::map<uint32_t, size_t> SessionMemMap_t;
SessionMemMap_t sessionMemMap;  // track memory% usage during a query
std::mutex sessionMemMapMutex;

//...The FrontEnd may establish more than 1 connection (which results in
//   more than 1 ExeMgr thread) per session.  These threads will share
//   the same CalpontSystemCatalog object for that session.  Here, we
//   define a std::map to track how many threads are sharing each session, so
//   that we know when we can safely delete a CalpontSystemCatalog object
//   shared by multiple threads per session.
typedef std::map<uint32_t, uint32_t> ThreadCntPerSessionMap_t;
ThreadCntPerSessionMap_t threadCntPerSessionMap;
std::mutex threadCntPerSessionMapMutex;

// This var is only accessed using thread-safe inc/dec calls
ActiveStatementCounter* statementsRunningCount;

joblist::DistributedEngineComm* ec;

auto rm = joblist::ResourceManager::instance(true);

int toInt(const std::string& val)
{
  if (val.length() == 0)
    return -1;

  return static_cast<int>(config::Config::fromText(val));
}

const std::string ExeMgr("ExeMgr1");

const std::string prettyPrintMiniInfo(const std::string& in)
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

const std::string timeNow()
{
  time_t outputTime = time(0);
  struct tm ltm;
  char buf[32];  // ctime(3) says at least 26
  size_t len = 0;
#ifdef _MSC_VER
  asctime_s(buf, 32, localtime_r(&outputTime, &ltm));
#else
  asctime_r(localtime_r(&outputTime, &ltm), buf);
#endif
  len = strlen(buf);

  if (len > 0)
    --len;

  if (buf[len] == '\n')
    buf[len] = 0;

  return buf;
}

querytele::QueryTeleServerParms gTeleServerParms;

class SessionThread
{
 public:
  SessionThread(const messageqcpp::IOSocket& ios, joblist::DistributedEngineComm* ec,
                joblist::ResourceManager* rm)
   : fIos(ios)
   , fEc(ec)
   , fRm(rm)
   , fStatsRetrieved(false)
   , fTeleClient(gTeleServerParms)
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
  static uint64_t getMaxMemPct(uint32_t sessionId)
  {
    uint64_t maxMemoryPct = 0;

    if (sessionId < 0x80000000)
    {
      std::lock_guard<std::mutex> lk(sessionMemMapMutex);
      SessionMemMap_t::iterator mapIter = sessionMemMap.find(sessionId);

      if (mapIter != sessionMemMap.end())
      {
        maxMemoryPct = (uint64_t)mapIter->second;
      }
    }

    return maxMemoryPct;
  }

  //...Delete sessionMemMap entry for the specified session's memory % use.
  //...SessionId >= 0x80000000 is system catalog query we can ignore.
  static void deleteMaxMemPct(uint32_t sessionId)
  {
    if (sessionId < 0x80000000)
    {
      std::lock_guard<std::mutex> lk(sessionMemMapMutex);
      SessionMemMap_t::iterator mapIter = sessionMemMap.find(sessionId);

      if (mapIter != sessionMemMap.end())
      {
        sessionMemMap.erase(sessionId);
      }
    }
  }

  //...Get and log query stats to specified output stream
  const std::string formatQueryStats(
      joblist::SJLP& jl,         // joblist associated with query
      const std::string& label,  // header label to print in front of log output
      bool includeNewLine,       // include line breaks in query stats std::string
      bool vtableModeOn, bool wantExtendedStats, uint64_t rowsReturned)
  {
    std::ostringstream os;

    // Get stats if not already acquired for current query
    if (!fStatsRetrieved)
    {
      if (wantExtendedStats)
      {
        // wait for the ei data to be written by another thread (brain-dead)
        struct timespec req = {0, 250000};  // 250 usec
#ifdef _MSC_VER
        Sleep(20);  // 20ms on Windows
#else
        nanosleep(&req, 0);
#endif
      }

      // Get % memory usage during current query for sessionId
      jl->querySummary(wantExtendedStats);
      fStats = jl->queryStats();
      fStats.fMaxMemPct = getMaxMemPct(fStats.fSessionID);
      fStats.fRows = rowsReturned;
      fStatsRetrieved = true;
    }

    std::string queryMode;
    queryMode = (vtableModeOn ? "Distributed" : "Standard");

    // Log stats to specified output stream
    os << label << ": MaxMemPct-" << fStats.fMaxMemPct << "; NumTempFiles-" << fStats.fNumFiles
       << "; TempFileSpace-" << roundBytes(fStats.fFileBytes) << "; ApproxPhyI/O-" << fStats.fPhyIO
       << "; CacheI/O-" << fStats.fCacheIO << "; BlocksTouched-" << fStats.fMsgRcvCnt;

    if (includeNewLine)
      os << std::endl << "      ";  // insert line break
    else
      os << "; ";  // continue without line break

    os << "PartitionBlocksEliminated-" << fStats.fCPBlocksSkipped << "; MsgBytesIn-"
       << roundBytes(fStats.fMsgBytesIn) << "; MsgBytesOut-" << roundBytes(fStats.fMsgBytesOut) << "; Mode-"
       << queryMode;

    return os.str();
  }

  //...Increment the number of threads using the specified sessionId
  static void incThreadCntPerSession(uint32_t sessionId)
  {
    std::lock_guard<std::mutex> lk(threadCntPerSessionMapMutex);
    ThreadCntPerSessionMap_t::iterator mapIter = threadCntPerSessionMap.find(sessionId);

    if (mapIter == threadCntPerSessionMap.end())
      threadCntPerSessionMap.insert(ThreadCntPerSessionMap_t::value_type(sessionId, 1));
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
  static void decThreadCntPerSession(uint32_t sessionId)
  {
    std::lock_guard<std::mutex> lk(threadCntPerSessionMapMutex);
    ThreadCntPerSessionMap_t::iterator mapIter = threadCntPerSessionMap.find(sessionId);

    if (mapIter != threadCntPerSessionMap.end())
    {
      if (--mapIter->second == 0)
      {
        threadCntPerSessionMap.erase(mapIter);
        execplan::CalpontSystemCatalog::removeCalpontSystemCatalog(sessionId);
        execplan::CalpontSystemCatalog::removeCalpontSystemCatalog((sessionId ^ 0x80000000));
      }
    }
  }

  //...Init sessionMemMap entry for specified session to 0 memory %.
  //...SessionId >= 0x80000000 is system catalog query we can ignore.
  static void initMaxMemPct(uint32_t sessionId)
  {
    if (sessionId < 0x80000000)
    {
      // std::cout << "Setting pct to 0 for session " << sessionId << std::endl;
      std::lock_guard<std::mutex> lk(sessionMemMapMutex);
      SessionMemMap_t::iterator mapIter = sessionMemMap.find(sessionId);

      if (mapIter == sessionMemMap.end())
      {
        sessionMemMap[sessionId] = 0;
      }
      else
      {
        mapIter->second = 0;
      }
    }
  }

  //... Round off to human readable format (KB, MB, or GB).
  const std::string roundBytes(uint64_t value) const
  {
    const char* units[] = {"B", "KB", "MB", "GB", "TB"};
    uint64_t i = 0, up = 0;
    uint64_t roundedValue = value;

    while (roundedValue > 1024 && i < 4)
    {
      up = (roundedValue & 512);
      roundedValue /= 1024;
      i++;
    }

    if (up)
      roundedValue++;

    std::ostringstream oss;
    oss << roundedValue << units[i];
    return oss.str();
  }

  //...Round off to nearest (1024*1024) MB
  uint64_t roundMB(uint64_t value) const
  {
    uint64_t roundedValue = value >> 20;

    if (value & 524288)
      roundedValue++;

    return roundedValue;
  }

  void setRMParms(const execplan::CalpontSelectExecutionPlan::RMParmVec& parms)
  {
    for (execplan::CalpontSelectExecutionPlan::RMParmVec::const_iterator it = parms.begin();
         it != parms.end(); ++it)
    {
      switch (it->id)
      {
        case execplan::PMSMALLSIDEMEMORY:
        {
          fRm->addHJPmMaxSmallSideMap(it->sessionId, it->value);
          break;
        }

        case execplan::UMSMALLSIDEMEMORY:
        {
          fRm->addHJUmMaxSmallSideMap(it->sessionId, it->value);
          break;
        }

        default:;
      }
    }
  }

  void buildSysCache(const execplan::CalpontSelectExecutionPlan& csep,
                     boost::shared_ptr<execplan::CalpontSystemCatalog> csc)
  {
    const execplan::CalpontSelectExecutionPlan::ColumnMap& colMap = csep.columnMap();
    execplan::CalpontSelectExecutionPlan::ColumnMap::const_iterator it;
    std::string schemaName;

    for (it = colMap.begin(); it != colMap.end(); ++it)
    {
      const auto sc = dynamic_cast<execplan::SimpleColumn*>((it->second).get());

      if (sc)
      {
        schemaName = sc->schemaName();

        // only the first time a schema is got will actually query
        // system catalog. System catalog keeps a schema name std::map.
        // if a schema exists, the call getSchemaInfo returns without
        // doing anything.
        if (!schemaName.empty())
          csc->getSchemaInfo(schemaName);
      }
    }

    execplan::CalpontSelectExecutionPlan::SelectList::const_iterator subIt;

    for (subIt = csep.derivedTableList().begin(); subIt != csep.derivedTableList().end(); ++subIt)
    {
      buildSysCache(*(dynamic_cast<execplan::CalpontSelectExecutionPlan*>(subIt->get())), csc);
    }
  }

  void writeCodeAndError(messageqcpp::ByteStream::quadbyte code, const std::string emsg)
  {
    messageqcpp::ByteStream emsgBs;
    messageqcpp::ByteStream tbs;
    tbs << code;
    fIos.write(tbs);
    emsgBs << emsg;
    fIos.write(emsgBs);
  }

  void analyzeTableExecute(messageqcpp::ByteStream& bs, joblist::SJLP& jl, bool& stmtCounted)
  {
    messageqcpp::ByteStream::quadbyte qb;
    execplan::MCSAnalyzeTableExecutionPlan caep;

    bs = fIos.read();
    caep.unserialize(bs);

    statementsRunningCount->incr(stmtCounted);
    jl = joblist::JobListFactory::makeJobList(&caep, fRm, false, true);

    // Joblist is empty.
    if (jl->status() == logging::statisticsJobListEmpty)
    {
      if (caep.traceOn())
        std::cout << "JobList is empty " << std::endl;

      jl.reset();
      bs.restart();
      qb = ANALYZE_TABLE_SUCCESS;
      bs << qb;
      fIos.write(bs);
      bs.reset();
      statementsRunningCount->decr(stmtCounted);
      return;
    }

    if (UNLIKELY(fEc->getNumConnections() != fEc->connectedPmServers()))
    {
      std::cout << "fEc setup " << std::endl;
      fEc->Setup();
    }

    if (jl->status() == 0)
    {
      if (jl->putEngineComm(fEc) != 0)
        throw std::runtime_error(jl->errMsg());
    }
    else
    {
      throw std::runtime_error("ExeMgr: could not build a JobList!");
    }

    // Execute a joblist.
    jl->doQuery();

    FEMsgHandler msgHandler(jl, &fIos);

    msgHandler.start();
    auto rowCount = jl->projectTable(100, bs);
    msgHandler.stop();

    auto outRG = (static_cast<joblist::TupleJobList*>(jl.get()))->getOutputRowGroup();

    if (caep.traceOn())
      std::cout << "Row count " << rowCount << std::endl;

    // Process `RowGroup`, increase an epoch and save statistics to the file.
    auto* statisticsManager = statistics::StatisticsManager::instance();
    statisticsManager->analyzeColumnKeyTypes(outRG, caep.traceOn());
    statisticsManager->incEpoch();
    statisticsManager->saveToFile();

    // Distribute statistics across all ExeMgr clients if possible.
    statistics::StatisticsDistributor::instance()->distributeStatistics();

    // Send the signal back to front-end.
    bs.restart();
    qb = ANALYZE_TABLE_SUCCESS;
    bs << qb;
    fIos.write(bs);
    bs.reset();
    statementsRunningCount->decr(stmtCounted);
  }

  void analyzeTableHandleStats(messageqcpp::ByteStream& bs)
  {
    messageqcpp::ByteStream::quadbyte qb;
#ifdef DEBUG_STATISTICS
    std::cout << "Get distributed statistics on ExeMgr(Client) from ExeMgr(Server) " << std::endl;
#endif
    bs = fIos.read();
#ifdef DEBUG_STATISTICS
    std::cout << "Read the hash from statistics on ExeMgr(Client) from ExeMgr(Server) " << std::endl;
#endif
    uint64_t dataHashRec;
    bs >> dataHashRec;

    uint64_t dataHash = statistics::StatisticsManager::instance()->computeHashFromStats();
    // The stats are the same.
    if (dataHash == dataHashRec)
    {
#ifdef DEBUG_STATISTICS
      std::cout << "The hash is the same as rec hash on ExeMgr(Client) from ExeMgr(Server) " << std::endl;
#endif
      qb = ANALYZE_TABLE_SUCCESS;
      bs << qb;
      fIos.write(bs);
      bs.reset();
      return;
    }

    bs.restart();
    qb = ANALYZE_TABLE_NEED_STATS;
    bs << qb;
    fIos.write(bs);

    bs.restart();
    bs = fIos.read();
#ifdef DEBUG_STATISTICS
    std::cout << "Read statistics on ExeMgr(Client) from ExeMgr(Server) " << std::endl;
#endif
    statistics::StatisticsManager::instance()->unserialize(bs);
    statistics::StatisticsManager::instance()->saveToFile();

#ifdef DEBUG_STATISTICS
    std::cout << "Write flag on ExeMgr(Client) to ExeMgr(Server)" << std::endl;
#endif
    qb = ANALYZE_TABLE_SUCCESS;
    bs << qb;
    fIos.write(bs);
    bs.reset();
  }

 public:
  void operator()()
  {
    utils::setThreadName("SessionThread");
    messageqcpp::ByteStream bs, inbs;
    execplan::CalpontSelectExecutionPlan csep;
    csep.sessionID(0);
    joblist::SJLP jl;
    bool incSessionThreadCnt = true;
    std::mutex jlMutex;
    std::condition_variable jlCleanupDone;
    int destructing = 0;

    bool selfJoin = false;
    bool tryTuples = false;
    bool usingTuples = false;
    bool stmtCounted = false;

    try
    {
      for (;;)
      {
        selfJoin = false;
        tryTuples = false;
        usingTuples = false;

        if (jl)
        {
          // puts the real destruction in another thread to avoid
          // making the whole session wait.  It can take several seconds.
          std::unique_lock<std::mutex> scoped(jlMutex);
          destructing++;
          std::thread bgdtor(
              [jl, &jlMutex, &jlCleanupDone, &destructing]
              {
                std::unique_lock<std::mutex> scoped(jlMutex);
                const_cast<joblist::SJLP&>(jl).reset();  // this happens second; does real destruction
                if (--destructing == 0)
                  jlCleanupDone.notify_one();
              });
          jl.reset();  // this runs first
          bgdtor.detach();
        }

        bs = fIos.read();

        if (bs.length() == 0)
        {
          if (gDebug > 1 || (gDebug && !csep.isInternal()))
            std::cout << "### Got a close(1) for session id " << csep.sessionID() << std::endl;

          // connection closed by client
          fIos.close();
          break;
        }
        else if (bs.length() < 4)  // Not a CalpontSelectExecutionPlan
        {
          if (gDebug)
            std::cout << "### Got a not-a-plan for session id " << csep.sessionID() << " with length "
                      << bs.length() << std::endl;

          fIos.close();
          break;
        }
        else if (bs.length() == 4)  // possible tuple flag
        {
          messageqcpp::ByteStream::quadbyte qb;
          bs >> qb;

          if (qb == 4)  // UM wants new tuple i/f
          {
            if (gDebug)
              std::cout << "### UM wants tuples" << std::endl;

            tryTuples = true;
            // now wait for the CSEP...
            bs = fIos.read();
          }
          else if (qb == 5)  // somebody wants stats
          {
            bs.restart();
            qb = statementsRunningCount->cur();
            bs << qb;
            qb = statementsRunningCount->waiting();
            bs << qb;
            fIos.write(bs);
            fIos.close();
            break;
          }
          else if (qb == ANALYZE_TABLE_EXECUTE)
          {
            analyzeTableExecute(bs, jl, stmtCounted);
            continue;
          }
          else if (qb == ANALYZE_TABLE_REC_STATS)
          {
            analyzeTableHandleStats(bs);
            continue;
          }
          else
          {
            if (gDebug)
              std::cout << "### Got a not-a-plan value " << qb << std::endl;

            fIos.close();
            break;
          }
        }

      new_plan:
        try
        {
          csep.unserialize(bs);
        }
        catch (logging::IDBExcept& ex)
        {
          // We can get here on illegal function parameter data type, e.g.
          //   SELECT blob_column|1 FROM t1;
          statementsRunningCount->decr(stmtCounted);
          writeCodeAndError(ex.errorCode(), std::string(ex.what()));
          continue;
        }

        querytele::QueryTeleStats qts;

        if (!csep.isInternal() && (csep.queryType() == "SELECT" || csep.queryType() == "INSERT_SELECT"))
        {
          qts.query_uuid = csep.uuid();
          qts.msg_type = querytele::QueryTeleStats::QT_START;
          qts.start_time = querytele::QueryTeleClient::timeNowms();
          qts.query = csep.data();
          qts.session_id = csep.sessionID();
          qts.query_type = csep.queryType();
          qts.system_name = fOamCachePtr->getSystemName();
          qts.module_name = fOamCachePtr->getModuleName();
          qts.local_query = csep.localQuery();
          qts.schema_name = csep.schemaName();
          fTeleClient.postQueryTele(qts);
        }

        if (gDebug > 1 || (gDebug && !csep.isInternal()))
          std::cout << "### For session id " << csep.sessionID() << ", got a CSEP" << std::endl;

        setRMParms(csep.rmParms());
        // Re-establish lost PP connections.
        if (UNLIKELY(fEc->getNumConnections() != fEc->connectedPmServers()))
        {
          fEc->Setup();
        }
        // @bug 1021. try to get schema cache for a come in query.
        // skip system catalog queries.
        if (!csep.isInternal())
        {
          boost::shared_ptr<execplan::CalpontSystemCatalog> csc =
              execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(csep.sessionID());
          buildSysCache(csep, csc);
        }

        // As soon as we have a session id for this thread, update the
        // thread count per session; only do this once per thread.
        // Mask 0x80000000 is for associate user query and csc query
        if (incSessionThreadCnt)
        {
          incThreadCntPerSession(csep.sessionID() | 0x80000000);
          incSessionThreadCnt = false;
        }

        bool needDbProfEndStatementMsg = false;
        logging::Message::Args args;
        std::string sqlText = csep.data();
        logging::LoggingID li(16, csep.sessionID(), csep.txnID());

        // Initialize stats for this query, including
        // init sessionMemMap entry for this session to 0 memory %.
        // We will need this later for traceOn() or if we receive a
        // table request with qb=3 (see below). This is also recorded
        // as query start time.
        initStats(csep.sessionID(), sqlText);
        fStats.fQueryType = csep.queryType();

        // Log start and end statement if tracing is enabled.  Keep in
        // mind the trace flag won't be set for system catalog queries.
        if (csep.traceOn())
        {
          args.reset();
          args.add((int)csep.statementID());
          args.add((int)csep.verID().currentScn);
          args.add(sqlText);
          msgLog.logMessage(logging::LOG_TYPE_DEBUG, logDbProfStartStatement, args, li);
          needDbProfEndStatementMsg = true;
        }

        // Don't log subsequent self joins after first.
        if (selfJoin)
          sqlText = "";

        std::ostringstream oss;
        oss << sqlText << "; |" << csep.schemaName() << "|";
        logging::SQLLogger sqlLog(oss.str(), li);

        statementsRunningCount->incr(stmtCounted);

        if (tryTuples)
        {
          try  // @bug2244: try/catch around fIos.write() calls responding to makeTupleList
          {
            jl = joblist::JobListFactory::makeJobList(&csep, fRm, true, true);
            // assign query stats
            jl->queryStats(fStats);

            if ((jl->status()) == 0 && (jl->putEngineComm(fEc) == 0))
            {
              usingTuples = true;

              // Tell the FE that we're sending tuples back, not TableBands
              writeCodeAndError(0, "NOERROR");
              auto tjlp = dynamic_cast<joblist::TupleJobList*>(jl.get());
              assert(tjlp);
              messageqcpp::ByteStream tbs;
              tbs << tjlp->getOutputRowGroup();
              fIos.write(tbs);
            }
            else
            {
              const std::string emsg = jl->errMsg();
              statementsRunningCount->decr(stmtCounted);
              writeCodeAndError(jl->status(), emsg);
              std::cerr << "ExeMgr: could not build a tuple joblist: " << emsg << std::endl;
              continue;
            }
          }
          catch (std::exception& ex)
          {
            std::ostringstream errMsg;
            errMsg << "ExeMgr: error writing makeJoblist "
                      "response; "
                   << ex.what();
            throw std::runtime_error(errMsg.str());
          }
          catch (...)
          {
            std::ostringstream errMsg;
            errMsg << "ExeMgr: unknown error writing makeJoblist "
                      "response; ";
            throw std::runtime_error(errMsg.str());
          }

          if (!usingTuples)
          {
            if (gDebug)
              std::cout << "### UM wanted tuples but it didn't work out :-(" << std::endl;
          }
          else
          {
            if (gDebug)
              std::cout << "### UM wanted tuples and we'll do our best;-)" << std::endl;
          }
        }
        else
        {
          usingTuples = false;
          jl = joblist::JobListFactory::makeJobList(&csep, fRm, false, true);

          if (jl->status() == 0)
          {
            std::string emsg;

            if (jl->putEngineComm(fEc) != 0)
              throw std::runtime_error(jl->errMsg());
          }
          else
          {
            throw std::runtime_error("ExeMgr: could not build a JobList!");
          }
        }

        jl->doQuery();

        execplan::CalpontSystemCatalog::OID tableOID;
        bool swallowRows = false;
        joblist::DeliveredTableMap tm;
        uint64_t totalBytesSent = 0;
        uint64_t totalRowCount = 0;

        // Project each table as the FE asks for it
        for (;;)
        {
          bs = fIos.read();

          if (bs.length() == 0)
          {
            if (gDebug > 1 || (gDebug && !csep.isInternal()))
              std::cout << "### Got a close(2) for session id " << csep.sessionID() << std::endl;

            break;
          }

          if (gDebug && bs.length() > 4)
            std::cout << "### For session id " << csep.sessionID() << ", got too many bytes = " << bs.length()
                      << std::endl;

          // TODO: Holy crud! Can this be right?
          //@bug 1444 Yes, if there is a self-join
          if (bs.length() > 4)
          {
            selfJoin = true;
            statementsRunningCount->decr(stmtCounted);
            goto new_plan;
          }

          assert(bs.length() == 4);

          messageqcpp::ByteStream::quadbyte qb;

          try  // @bug2244: try/catch around fIos.write() calls responding to qb command
          {
            bs >> qb;

            if (gDebug > 1 || (gDebug && !csep.isInternal()))
              std::cout << "### For session id " << csep.sessionID() << ", got a command = " << qb
                        << std::endl;

            if (qb == 0)
            {
              // No more tables, query is done
              break;
            }
            else if (qb == 1)
            {
              // super-secret flag indicating that the UM is going to scarf down all the rows in the
              //   query.
              swallowRows = true;
              tm = jl->deliveredTables();
              continue;
            }
            else if (qb == 2)
            {
              // UM just wants any table
              assert(swallowRows);
              auto iter = tm.begin();

              if (iter == tm.end())
              {
                if (gDebug > 1 || (gDebug && !csep.isInternal()))
                  std::cout << "### For session id " << csep.sessionID() << ", returning end flag"
                            << std::endl;

                bs.restart();
                bs << (messageqcpp::ByteStream::byte)1;
                fIos.write(bs);
                continue;
              }

              tableOID = iter->first;
            }
            else if (qb == 3)  // special option-UM wants job stats std::string
            {
              std::string statsString;

              // Log stats std::string to be sent back to front end
              statsString = formatQueryStats(
                  jl, "Query Stats", false,
                  !(csep.traceFlags() & execplan::CalpontSelectExecutionPlan::TRACE_TUPLE_OFF),
                  (csep.traceFlags() & execplan::CalpontSelectExecutionPlan::TRACE_LOG), totalRowCount);

              bs.restart();
              bs << statsString;

              if ((csep.traceFlags() & execplan::CalpontSelectExecutionPlan::TRACE_LOG) != 0)
              {
                bs << jl->extendedInfo();
                bs << prettyPrintMiniInfo(jl->miniInfo());
              }
              else
              {
                std::string empty;
                bs << empty;
                bs << empty;
              }

              // send stats to connector for inserting to the querystats table
              fStats.serialize(bs);
              fIos.write(bs);
              continue;
            }
            // for table mode handling
            else if (qb == 4)
            {
              statementsRunningCount->decr(stmtCounted);
              bs = fIos.read();
              goto new_plan;
            }
            else  // (qb > 3)
            {
              // Return table bands for the requested tableOID
              tableOID = static_cast<execplan::CalpontSystemCatalog::OID>(qb);
            }
          }
          catch (std::exception& ex)
          {
            std::ostringstream errMsg;
            errMsg << "ExeMgr: error writing qb response "
                      "for qb cmd "
                   << qb << "; " << ex.what();
            throw std::runtime_error(errMsg.str());
          }
          catch (...)
          {
            std::ostringstream errMsg;
            errMsg << "ExeMgr: unknown error writing qb response "
                      "for qb cmd "
                   << qb;
            throw std::runtime_error(errMsg.str());
          }

          if (swallowRows)
            tm.erase(tableOID);

          FEMsgHandler msgHandler(jl, &fIos);

          if (tableOID == 100)
            msgHandler.start();

          //...Loop serializing table bands projected for the tableOID
          for (;;)
          {
            uint32_t rowCount;

            rowCount = jl->projectTable(tableOID, bs);

            msgHandler.stop();

            if (jl->status())
            {
              const auto errInfo = logging::IDBErrorInfo::instance();

              if (jl->errMsg().length() != 0)
                bs << jl->errMsg();
              else
                bs << errInfo->errorMsg(jl->status());
            }

            try  // @bug2244: try/catch around fIos.write() calls projecting rows
            {
              if (csep.traceFlags() & execplan::CalpontSelectExecutionPlan::TRACE_NO_ROWS3)
              {
                // Skip the write to the front end until the last empty band.  Used to time queries
                // through without any front end waiting.
                if (tableOID < 3000 || rowCount == 0)
                  fIos.write(bs);
              }
              else
              {
                fIos.write(bs);
              }
            }
            catch (std::exception& ex)
            {
              msgHandler.stop();
              std::ostringstream errMsg;
              errMsg << "ExeMgr: error projecting rows "
                        "for tableOID: "
                     << tableOID << "; rowCnt: " << rowCount << "; prevTotRowCnt: " << totalRowCount << "; "
                     << ex.what();
              jl->abort();

              while (rowCount)
                rowCount = jl->projectTable(tableOID, bs);

              if (tableOID == 100 && msgHandler.aborted())
              {
                /* TODO: modularize the cleanup code, as well as
                 * the rest of this fcn */

                decThreadCntPerSession(csep.sessionID() | 0x80000000);
                statementsRunningCount->decr(stmtCounted);
                fIos.close();
                return;
              }

              // std::cout << "connection drop\n";
              throw std::runtime_error(errMsg.str());
            }
            catch (...)
            {
              std::ostringstream errMsg;
              msgHandler.stop();
              errMsg << "ExeMgr: unknown error projecting rows "
                        "for tableOID: "
                     << tableOID << "; rowCnt: " << rowCount << "; prevTotRowCnt: " << totalRowCount;
              jl->abort();

              while (rowCount)
                rowCount = jl->projectTable(tableOID, bs);

              throw std::runtime_error(errMsg.str());
            }

            totalRowCount += rowCount;
            totalBytesSent += bs.length();

            if (rowCount == 0)
            {
              msgHandler.stop();
              // No more bands, table is done
              bs.reset();

              // @bug 2083 decr active statement count here for table mode.
              if (!usingTuples)
                statementsRunningCount->decr(stmtCounted);

              break;
            }
            else
            {
              bs.restart();
            }
          }  // End of loop to project and serialize table bands for a table
        }    // End of loop to process tables

        // @bug 828
        if (csep.traceOn())
          jl->graph(csep.sessionID());

        if (needDbProfEndStatementMsg)
        {
          std::string ss;
          std::ostringstream prefix;
          prefix << "ses:" << csep.sessionID() << " Query Totals";

          // Log stats std::string to standard out
          ss = formatQueryStats(jl, prefix.str(), true,
                                !(csep.traceFlags() & execplan::CalpontSelectExecutionPlan::TRACE_TUPLE_OFF),
                                (csep.traceFlags() & execplan::CalpontSelectExecutionPlan::TRACE_LOG),
                                totalRowCount);
          //@Bug 1306. Added timing info for real time tracking.
          std::cout << ss << " at " << timeNow() << std::endl;

          // log query stats to debug log file
          args.reset();
          args.add((int)csep.statementID());
          args.add(fStats.fMaxMemPct);
          args.add(fStats.fNumFiles);
          args.add(fStats.fFileBytes);  // log raw byte count instead of MB
          args.add(fStats.fPhyIO);
          args.add(fStats.fCacheIO);
          args.add(fStats.fMsgRcvCnt);
          args.add(fStats.fMsgBytesIn);
          args.add(fStats.fMsgBytesOut);
          args.add(fStats.fCPBlocksSkipped);
          msgLog.logMessage(logging::LOG_TYPE_DEBUG, logDbProfQueryStats, args, li);
          //@bug 1327
          deleteMaxMemPct(csep.sessionID());
          // Calling reset here, will cause joblist destructor to be
          // called, which "joins" the threads.  We need to do that
          // here to make sure all syslogging from all the threads
          // are complete; and that our logDbProfEndStatement will
          // appear "last" in the syslog for this SQL statement.
          // puts the real destruction in another thread to avoid
          // making the whole session wait.  It can take several seconds.
          int stmtID = csep.statementID();
          std::unique_lock<std::mutex> scoped(jlMutex);
          // C7's compiler complains about the msgLog capture here
          // msgLog is global scope, and passed by copy, so, unclear
          // what the warning is about.
          destructing++;
          std::thread bgdtor(
              [jl, &jlMutex, &jlCleanupDone, stmtID, &li, &destructing]
              {
                std::unique_lock<std::mutex> scoped(jlMutex);
                const_cast<joblist::SJLP&>(jl).reset();  // this happens second; does real destruction
                logging::Message::Args args;
                args.add(stmtID);
                msgLog.logMessage(logging::LOG_TYPE_DEBUG, logDbProfEndStatement, args, li);
                if (--destructing == 0)
                  jlCleanupDone.notify_one();
              });
          jl.reset();  // this happens first
          bgdtor.detach();
        }
        else
          // delete sessionMemMap entry for this session's memory % use
          deleteMaxMemPct(csep.sessionID());

        std::string endtime(timeNow());

        if ((csep.traceFlags() & flagsWantOutput) && (csep.sessionID() < 0x80000000))
        {
          std::cout << "For session " << csep.sessionID() << ": " << totalBytesSent << " bytes sent back at "
                    << endtime << std::endl;

          // @bug 663 - Implemented caltraceon(16) to replace the
          // $FIFO_SINK compiler definition in pColStep.
          // This option consumes rows in the project steps.
          if (csep.traceFlags() & execplan::CalpontSelectExecutionPlan::TRACE_NO_ROWS4)
          {
            std::cout << std::endl;
            std::cout << "**** No data returned to DM.  Rows consumed "
                         "in ProjectSteps - caltrace(16) is on (FIFO_SINK)."
                         " ****"
                      << std::endl;
            std::cout << std::endl;
          }
          else if (csep.traceFlags() & execplan::CalpontSelectExecutionPlan::TRACE_NO_ROWS3)
          {
            std::cout << std::endl;
            std::cout << "**** No data returned to DM - caltrace(8) is "
                         "on (SWALLOW_ROWS_EXEMGR). ****"
                      << std::endl;
            std::cout << std::endl;
          }
        }

        statementsRunningCount->decr(stmtCounted);

        if (!csep.isInternal() && (csep.queryType() == "SELECT" || csep.queryType() == "INSERT_SELECT"))
        {
          qts.msg_type = querytele::QueryTeleStats::QT_SUMMARY;
          qts.max_mem_pct = fStats.fMaxMemPct;
          qts.num_files = fStats.fNumFiles;
          qts.phy_io = fStats.fPhyIO;
          qts.cache_io = fStats.fCacheIO;
          qts.msg_rcv_cnt = fStats.fMsgRcvCnt;
          qts.cp_blocks_skipped = fStats.fCPBlocksSkipped;
          qts.msg_bytes_in = fStats.fMsgBytesIn;
          qts.msg_bytes_out = fStats.fMsgBytesOut;
          qts.rows = totalRowCount;
          qts.end_time = querytele::QueryTeleClient::timeNowms();
          qts.session_id = csep.sessionID();
          qts.query_type = csep.queryType();
          qts.query = csep.data();
          qts.system_name = fOamCachePtr->getSystemName();
          qts.module_name = fOamCachePtr->getModuleName();
          qts.local_query = csep.localQuery();
          fTeleClient.postQueryTele(qts);
        }
      }

      // Release CSC object (for sessionID) that was added by makeJobList()
      // Mask 0x80000000 is for associate user query and csc query.
      // (actual joblist destruction happens at the top of this loop)
      decThreadCntPerSession(csep.sessionID() | 0x80000000);
    }
    catch (std::exception& ex)
    {
      decThreadCntPerSession(csep.sessionID() | 0x80000000);
      statementsRunningCount->decr(stmtCounted);
      std::cerr << "### ExeMgr ses:" << csep.sessionID() << " caught: " << ex.what() << std::endl;
      logging::Message::Args args;
      logging::LoggingID li(16, csep.sessionID(), csep.txnID());
      args.add(ex.what());
      msgLog.logMessage(logging::LOG_TYPE_CRITICAL, logExeMgrExcpt, args, li);
      fIos.close();
    }
    catch (...)
    {
      decThreadCntPerSession(csep.sessionID() | 0x80000000);
      statementsRunningCount->decr(stmtCounted);
      std::cerr << "### Exception caught!" << std::endl;
      logging::Message::Args args;
      logging::LoggingID li(16, csep.sessionID(), csep.txnID());
      args.add("ExeMgr caught unknown exception");
      msgLog.logMessage(logging::LOG_TYPE_CRITICAL, logExeMgrExcpt, args, li);
      fIos.close();
    }

    // make sure we don't leave scope while joblists are being destroyed
    std::unique_lock<std::mutex> scoped(jlMutex);
    while (destructing > 0)
      jlCleanupDone.wait(scoped);
  }
};

class RssMonFcn : public utils::MonitorProcMem
{
 public:
  RssMonFcn(size_t maxPct, int pauseSeconds) : MonitorProcMem(maxPct, 0, 21, pauseSeconds)
  {
  }

  /*virtual*/
  void operator()() const
  {
    for (;;)
    {
      size_t rssMb = rss();
      size_t pct = rssMb * 100 / fMemTotal;

      if (pct > fMaxPct)
      {
        if (fMaxPct >= 95)
        {
          std::cerr << "Too much memory allocated!" << std::endl;
          logging::Message::Args args;
          args.add((int)pct);
          args.add((int)fMaxPct);
          msgLog.logMessage(logging::LOG_TYPE_CRITICAL, logRssTooBig, args, logging::LoggingID(16));
          exit(1);
        }

        if (statementsRunningCount->cur() == 0)
        {
          std::cerr << "Too much memory allocated!" << std::endl;
          logging::Message::Args args;
          args.add((int)pct);
          args.add((int)fMaxPct);
          msgLog.logMessage(logging::LOG_TYPE_WARNING, logRssTooBig, args, logging::LoggingID(16));
          exit(1);
        }

        std::cerr << "Too much memory allocated, but stmts running" << std::endl;
      }

      // Update sessionMemMap entries lower than current mem % use
      {
        std::lock_guard<std::mutex> lk(sessionMemMapMutex);

        for (SessionMemMap_t::iterator mapIter = sessionMemMap.begin(); mapIter != sessionMemMap.end();
             ++mapIter)
        {
          if (pct > mapIter->second)
          {
            mapIter->second = pct;
          }
        }
      }

      pause_();
    }
  }
};

#ifdef _MSC_VER
void exit_(int)
{
  exit(0);
}
#endif

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

  if (ec)
  {
    oam::OamCache* oamCache = oam::OamCache::makeOamCache();
    oamCache->forceReload();
    ec->Setup();
  }
}

void printTotalUmMemory(int sig)
{
  int64_t num = rm->availableMemory();
  std::cout << "Total UM memory available: " << num << std::endl;
}

void setupSignalHandlers()
{
#ifdef _MSC_VER
  signal(SIGINT, exit_);
  signal(SIGTERM, exit_);
#else
  struct sigaction ign;

  memset(&ign, 0, sizeof(ign));
  ign.sa_handler = SIG_IGN;

  sigaction(SIGPIPE, &ign, 0);

  memset(&ign, 0, sizeof(ign));
  ign.sa_handler = added_a_pm;
  sigaction(SIGHUP, &ign, 0);
  ign.sa_handler = printTotalUmMemory;
  sigaction(SIGUSR1, &ign, 0);
  memset(&ign, 0, sizeof(ign));
  ign.sa_handler = fatalHandler;
  sigaction(SIGSEGV, &ign, 0);
  sigaction(SIGABRT, &ign, 0);
  sigaction(SIGFPE, &ign, 0);
#endif
}

int8_t setupCwd(joblist::ResourceManager* rm)
{
  std::string workdir = rm->getScWorkingDir();
  int8_t rc = chdir(workdir.c_str());

  if (rc < 0 || access(".", W_OK) != 0)
    rc = chdir("/tmp");

  return (rc < 0) ? -5 : rc;
}

void startRssMon(size_t maxPct, int pauseSeconds)
{
  new boost::thread(RssMonFcn(maxPct, pauseSeconds));
}

int setupResources()
{
#ifdef _MSC_VER
  // FIXME:
#else
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

#endif
  return 0;
}

}  // namespace

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

int ServiceExeMgr::Child()
{
  gDebug = m_debug;

#ifdef _MSC_VER
  // FIXME:
#else

  // Make sure CSC thinks it's on a UM or else bucket reuse stuff below will stall
  if (!m_e)
    setenv("CALPONT_CSC_IDENT", "um", 1);

#endif
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

  err = setupCwd(rm);

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
  msgLog.msgMap(msgMap);

  ec = joblist::DistributedEngineComm::instance(rm, true);
  ec->Open();

  bool tellUser = true;

  messageqcpp::MessageQueueServer* mqs;

  statementsRunningCount = new ActiveStatementCounter(rm->getEmExecQueueSize());

  for (;;)
  {
    try
    {
      mqs = new messageqcpp::MessageQueueServer(ExeMgr, rm->getConfig(), messageqcpp::ByteStream::BlockSize,
                                                64);
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
  joblist::JobStep::jobstepThreadPool.setMaxThreads(rm->getJLThreadPoolSize());
  joblist::JobStep::jobstepThreadPool.setName("ExeMgrJobList");

  if (rm->getJlThreadPoolDebug() == "Y" || rm->getJlThreadPoolDebug() == "y")
  {
    joblist::JobStep::jobstepThreadPool.setDebug(true);
    joblist::JobStep::jobstepThreadPool.invoke(
        threadpool::ThreadPoolMonitor(&joblist::JobStep::jobstepThreadPool));
  }

  int serverThreads = rm->getEmServerThreads();
  int maxPct = rm->getEmMaxPct();
  int pauseSeconds = rm->getEmSecondsBetweenMemChecks();
  int priority = rm->getEmPriority();

  FEMsgHandler::threadPool.setMaxThreads(serverThreads);
  FEMsgHandler::threadPool.setName("FEMsgHandler");

  if (maxPct > 0)
    startRssMon(maxPct, pauseSeconds);

#ifndef _MSC_VER
  setpriority(PRIO_PROCESS, 0, priority);
#endif

  std::string teleServerHost(rm->getConfig()->getConfig("QueryTele", "Host"));

  if (!teleServerHost.empty())
  {
    int teleServerPort = toInt(rm->getConfig()->getConfig("QueryTele", "Port"));

    if (teleServerPort > 0)
    {
      gTeleServerParms.host = teleServerHost;
      gTeleServerParms.port = teleServerPort;
    }
  }

  NotifyServiceStarted();

  std::cout << "Starting ExeMgr: st = " << serverThreads << ", qs = " << rm->getEmExecQueueSize()
            << ", mx = " << maxPct << ", cf = " << rm->getConfig()->configFile() << std::endl;

  {
    BRM::DBRM* dbrm = new BRM::DBRM();
    dbrm->setSystemQueryReady(true);
    delete dbrm;
  }

  threadpool::ThreadPool exeMgrThreadPool(serverThreads, 0);
  exeMgrThreadPool.setName("ExeMgrServer");

  if (rm->getExeMgrThreadPoolDebug() == "Y" || rm->getExeMgrThreadPoolDebug() == "y")
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
    exeMgrThreadPool.invoke(SessionThread(ios, ec, rm));
  }

  exeMgrThreadPool.wait();

  return 0;
}

int main(int argc, char* argv[])
{
  opterr = 0;
  Opt opt(argc, argv);

  // Set locale language
  setlocale(LC_ALL, "");
  setlocale(LC_NUMERIC, "C");

  // This is unset due to the way we start it
  program_invocation_short_name = const_cast<char*>("ExeMgr");

  // Initialize the charset library
  MY_INIT(argv[0]);

  return ServiceExeMgr(opt).Run();
}

// vim:ts=4 sw=4:

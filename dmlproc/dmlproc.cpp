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

/***********************************************************************
 *   $Id: dmlproc.cpp 1029 2013-07-31 19:17:42Z dhall $
 *
 *
 ***********************************************************************/
#include <unistd.h>
#include <signal.h>
#include <string>
#include <set>
#include <clocale>
//#include "boost/filesystem/operations.hpp"
//#include "boost/filesystem/path.hpp"
using namespace std;

#include "liboamcpp.h"

#include <boost/scoped_ptr.hpp>
using namespace boost;

#include "dmlproc.h"
#include "dmlprocessor.h"
using namespace dmlprocessor;

#include "configcpp.h"
using namespace config;

#include "sessionmanager.h"
using namespace execplan;

#include "vendordmlstatement.h"
#include "calpontdmlpackage.h"
#include "calpontdmlfactory.h"
using namespace dmlpackage;

#include "dmlpackageprocessor.h"
using namespace dmlpackageprocessor;

#include "idberrorinfo.h"
using namespace logging;

#include "liboamcpp.h"
#include "messagelog.h"
using namespace oam;

#include "writeengine.h"
#include "IDBPolicy.h"

#undef READ  // someone has polluted the namespace!
#include "vss.h"
#include "brmtypes.h"
using namespace BRM;

#include "bytestream.h"
using namespace messageqcpp;

#include "cacheutils.h"
#include "ddlcleanuputil.h"

#include "distributedenginecomm.h"
using namespace joblist;

#include "crashtrace.h"
#include "installdir.h"

#include "mariadb_my_sys.h"

#include "service.h"

threadpool::ThreadPool DMLServer::fDmlPackagepool(10, 0);

namespace
{
class Opt
{
 public:
  int m_debug;
  bool m_fg;
  Opt(int argc, char* argv[]) : m_debug(0), m_fg(false)
  {
    int c;
    while ((c = getopt(argc, argv, "df")) != EOF)
    {
      switch (c)
      {
        case 'd':
          m_debug++;  // TODO: not really used yes
          break;
        case 'f': m_fg = true; break;
        case '?':
        default: break;
      }
    }
  }
};

class ServiceDMLProc : public Service, public Opt
{
 protected:
  int Parent() override
  {
    /*
      Need to shutdown TheadPool,
      otherwise it would get stuck when trying to join fPruneThread.
    */
    joblist::JobStep::jobstepThreadPool.stop();
    return Service::Parent();
  }

  void log(logging::LOG_TYPE type, const std::string& str)
  {
    LoggingID logid(23, 0, 0);
    Message::Args args;
    Message message(8);
    args.add(str);
    message.format(args);
    logging::Logger logger(logid.fSubsysID);
    logger.logMessage(LOG_TYPE_CRITICAL, message, logid);
  }

  void setupChildSignalHandlers();

 public:
  ServiceDMLProc(const Opt& opt) : Service("DMLProc"), Opt(opt)
  {
  }
  void LogErrno() override
  {
    log(LOG_TYPE_CRITICAL, std::string(strerror(errno)));
  }
  void ParentLogChildMessage(const std::string& str) override
  {
    log(LOG_TYPE_INFO, str);
  }
  int Child() override;
  int Run()
  {
    return m_fg ? Child() : RunForking();
  }
};

DistributedEngineComm* Dec;

void added_a_pm(int)
{
  LoggingID logid(21, 0, 0);
  logging::Message::Args args1;
  logging::Message msg(1);
  args1.add("DMLProc caught SIGHUP. Resetting connections");
  msg.format(args1);
  logging::Logger logger(logid.fSubsysID);
  logger.logMessage(LOG_TYPE_DEBUG, msg, logid);

  Dec->Setup();
  // MCOL-140 clear the waiting queue as all transactions are probably going to fail
  PackageHandler::clearTableAccess();

  logger.logMessage(LOG_TYPE_DEBUG, msg, logid);

  // WriteEngine::WEClients::instance(WriteEngine::WEClients::DMLPROC)->Setup();
  logger.logMessage(LOG_TYPE_DEBUG, msg, logid);
}

int toInt(const string& val)
{
  if (val.length() == 0)
    return -1;

  return static_cast<int>(Config::fromText(val));
}

class CleanUpThread
{
 public:
  CleanUpThread()
  {
  }
  ~CleanUpThread()
  {
  }
  void operator()()
  {
    ddlcleanuputil::ddl_cleanup();
    return;
  }
};

// This function rolls back any active transactions in case of an abnormal shutdown.
void rollbackAll(DBRM* dbrm)
{
  Oam oam;

  // Log a message in info.log
  logging::Message::Args args;
  logging::Message message(2);
  args.add("DMLProc starts rollbackAll.");
  message.format(args);
  logging::LoggingID lid(20);
  logging::MessageLog ml(lid);
  ml.logInfoMessage(message);

  boost::shared_array<BRM::SIDTIDEntry> activeTxns;
  BRM::TxnID txnID;
  SessionManager sessionManager;
  int rc = 0;
  rc = dbrm->isReadWrite();

  if (rc != 0)
    throw std::runtime_error("Rollback will be deferred due to DBRM is in read only state.");

  dbrm->setSystemReady(false);
  // DDL clean up thread
  thread_group tg;
  tg.create_thread(CleanUpThread());

  std::vector<TableLockInfo> tableLocks;

  try
  {
    tableLocks = dbrm->getAllTableLocks();
  }
  catch (std::exception&)
  {
    throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
  }

  uint64_t uniqueId = dbrm->getUnique64();
  RollbackTransactionProcessor rollbackProcessor(dbrm);
  std::string errorMsg;
  unsigned int i = 0;
  BRM::TxnID txnId;
  ostringstream oss;

  oss << "DMLProc will rollback " << tableLocks.size() << " tables.";
  // cout << oss.str() << endl;
  logging::Message::Args args5;
  logging::Message message5(2);
  args5.add(oss.str());
  message5.format(args5);
  ml.logInfoMessage(message5);
  OamCache* oamcache = OamCache::makeOamCache();
  OamCache::dbRootPMMap_t dbRootPMMap = oamcache->getDBRootToPMMap();
  int errorTxn = 0;

  for (i = 0; i < tableLocks.size(); i++)
  {
    if (tableLocks[i].ownerTxnID > 0)  // transaction rollback
    {
      ostringstream oss;
      oss << "DMLProc is rolling back transaction " << tableLocks[i].ownerTxnID;
      // cout << oss.str() << endl;
      logging::Message::Args args1;
      logging::Message message2(2);
      args1.add(oss.str());
      message2.format(args1);
      ml.logInfoMessage(message2);
      dbrm->invalidateUncommittedExtentLBIDs(tableLocks[i].ownerTxnID, false);
      uint32_t sessionid = 0;
      txnId.id = tableLocks[i].ownerTxnID;
      txnId.valid = true;
      rc = rollbackProcessor.rollBackTransaction(uniqueId, txnId, sessionid, errorMsg);

      if (rc == 0)
      {
        ostringstream oss;
        oss << "DMLProc rolled back transaction " << tableLocks[i].ownerTxnID
            << " and is releasing table lock id " << tableLocks[i].id;
        logging::Message::Args args3;
        logging::Message message3(2);
        args3.add(oss.str());
        message3.format(args3);
        ml.logInfoMessage(message3);

        //@Bug 5285 get the process name to see whether calling bulkrollback is necessary.
        string::size_type namelen = tableLocks[i].ownerName.find_first_of(" ");

        if (namelen != string::npos)
        {
          rollbackProcessor.rollBackBatchAutoOnTransaction(uniqueId, txnId, sessionid, tableLocks[i].tableOID,
                                                           errorMsg);
        }

        logging::logCommand(0, tableLocks[i].ownerTxnID, "ROLLBACK;");

        // release table lock if not DDLProc
        if (tableLocks[i].ownerName == "DDLProc")
          continue;

        bool lockReleased = true;

        try
        {
          lockReleased = dbrm->releaseTableLock(tableLocks[i].id);
        }
        catch (std::exception&)
        {
          throw std::runtime_error(IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE));
        }

        if (lockReleased)
        {
          sessionManager.rolledback(txnId);
          ostringstream oss;
          oss << "DMLProc rolled back transaction " << tableLocks[i].ownerTxnID << " and table lock id "
              << tableLocks[i].id << " is released.";
          // cout << oss.str() << endl;
          logging::Message::Args args2;
          logging::Message message2(2);
          args2.add(oss.str());
          message2.format(args2);
          ml.logInfoMessage(message2);
        }
        else
        {
          ostringstream oss;
          oss << "DMLProc rolled back transaction " << tableLocks[i].ownerTxnID << " and table lock id "
              << tableLocks[i].id << " is not released.";
          // cout << oss.str() << endl;
          logging::Message::Args args2;
          logging::Message message2(2);
          args2.add(oss.str());
          message2.format(args2);
          ml.logInfoMessage(message2);
        }
      }
      else
      {
        //@Bug 4524 still need to set readonly as transaction information is lost during system restart.
        ostringstream oss;
        oss << " problem with rollback transaction " << tableLocks[i].ownerTxnID
            << "and DBRM is setting to readonly and table lock is not released: " << errorMsg;
        rc = dbrm->setReadOnly(true);

        // Log to critical log
        logging::Message::Args args6;
        logging::Message message6(2);
        args6.add(oss.str());
        message6.format(args6);
        ml.logCriticalMessage(message6);
        errorTxn = tableLocks[i].ownerTxnID;
      }
    }
    else if (tableLocks[i].ownerName == "WriteEngineServer")  // redistribution
    {
      // Just release the table lock
      bool lockReleased = true;

      try
      {
        lockReleased = dbrm->releaseTableLock(tableLocks[i].id);
      }
      catch (std::exception& ex)
      {
        ostringstream oss;
        oss << "DMLProc didn't release table lock id " << tableLocks[i].id << " for redistribution due to "
            << ex.what();
        logging::Message::Args args2;
        logging::Message message2(2);
        args2.add(oss.str());
        message2.format(args2);
        ml.logInfoMessage(message2);
      }

      if (lockReleased)
      {
        ostringstream oss;
        oss << "DMLProc released table lock id " << tableLocks[i].id << " for redistribution.";
        logging::Message::Args args2;
        logging::Message message2(2);
        args2.add(oss.str());
        message2.format(args2);
        ml.logInfoMessage(message2);
      }
    }
    else  // bulkrollback
    {
      // change owner, still keep transaction id to -1, and session id to -1.
      bool lockReleased = true;

      try
      {
        rollbackProcessor.processBulkRollback(tableLocks[i], dbrm, uniqueId, dbRootPMMap, lockReleased);
        ostringstream oss;
        oss << "DMLProc started bulkrollback on table OID " << tableLocks[i].tableOID << " and table lock id "
            << tableLocks[i].id << " finished and tablelock is released.";
        // cout << oss.str() << endl;
        logging::Message::Args args4;
        logging::Message message4(2);
        args4.add(oss.str());
        message4.format(args4);
        ml.logInfoMessage(message4);
      }
      catch (std::exception& ex)
      {
        ostringstream oss;
        oss << "DMLProc started bulkrollback on table OID " << tableLocks[i].tableOID << " and table lock id "
            << tableLocks[i].id << " failed:" << ex.what();

        if (lockReleased)
          oss << " but the tablelock is released due to it is in CLEANUP state";
        else
          oss << " and tablelock is not released.";

        // cout << oss.str() << endl;
        logging::Message::Args args3;
        logging::Message message3(2);
        args3.add(oss.str());
        message3.format(args3);
        ml.logWarningMessage(message3);
      }
    }
  }

  // Clear out the session manager session list of sessions / transactions.
  // Take care of any transaction left from create table as there is no table lock information
  set<VER_t> txnList;
  rc = dbrm->getCurrentTxnIDs(txnList);

  if (txnList.size() > 0)
  {
    ostringstream oss;
    oss << "DMLProc will rollback " << txnList.size() << " transactions.";
    logging::Message::Args args2;
    logging::Message message2(2);
    args2.add(oss.str());
    message2.format(args2);
    ml.logInfoMessage(message2);
    set<VER_t>::const_iterator curTxnID;

    for (curTxnID = txnList.begin(); curTxnID != txnList.end(); ++curTxnID)
    {
      dbrm->invalidateUncommittedExtentLBIDs(*curTxnID, false);
      txnId.id = *curTxnID;
      txnId.valid = true;
      uint32_t sessionid = 0;
      ostringstream oss;
      oss << "DMLProc will roll back transaction " << *curTxnID;
      logging::Message::Args args2;
      logging::Message message2(2);
      args2.add(oss.str());
      message2.format(args2);
      ml.logInfoMessage(message2);
      rc = rollbackProcessor.rollBackTransaction(uniqueId, txnId, sessionid, errorMsg);

      if (rc == 0)
      {
        logging::logCommand(0, *curTxnID, "ROLLBACK;");
        ostringstream oss;

        oss << " DMLProc rolled back transaction " << *curTxnID;
        // Log to warning log
        logging::Message::Args args6;
        logging::Message message6(2);
        args6.add(oss.str());
        message6.format(args6);
        ml.logInfoMessage(message6);
      }
      else
      {
        //@Bug 4524 still need to set readonly as transaction information is lost during system restart.
        ostringstream oss;
        oss << " problem with rollback transaction " << txnId.id
            << "and DBRM is setting to readonly and table lock is not released: " << errorMsg;
        rc = dbrm->setReadOnly(true);

        // Log to critical log
        logging::Message::Args args6;
        logging::Message message6(2);
        args6.add(oss.str());
        message6.format(args6);
        ml.logCriticalMessage(message6);
        errorTxn = *curTxnID;
      }
    }
  }

  int len;
  activeTxns = sessionManager.SIDTIDMap(len);

  //@Bug 1627 Don't start DMLProc if either controller or worker node is not up
  if (activeTxns == NULL)
  {
    std::string err = "DBRM problem is encountered.";
    throw std::runtime_error(err);
  }

  for (int i = 0; i < len; i++)
  {
    txnID = activeTxns[i].txnid;

    if (txnID.id != errorTxn)
    {
      sessionManager.rolledback(txnID);
      oss << " DMLProc released transaction " << txnID.id;
      logging::Message::Args args6;
      logging::Message message6(2);
      args6.add(oss.str());
      message6.format(args6);
      ml.logInfoMessage(message6);
    }
  }

  // Clear out the DBRM.

  dbrm->clear();

  // Flush the cache
  cacheutils::flushPrimProcCache();

  // Log a message in info.log
  logging::Message::Args args1;
  logging::Message message1(2);
  args1.add("DMLProc finished rollbackAll.");
  message1.format(args1);
  ml.logInfoMessage(message1);

  dbrm->setSystemReady(true);
}

int8_t setupCwd()
{
  string workdir = startup::StartUp::tmpDir();

  if (workdir.length() == 0)
    workdir = ".";

  int8_t rc = chdir(workdir.c_str());

  if (rc < 0 || access(".", W_OK) != 0)
    rc = chdir("/tmp");

  return rc;
}
}  // namespace

void ServiceDMLProc::setupChildSignalHandlers()
{
#ifndef _MSC_VER
  /* set up some signal handlers */
  struct sigaction ign;
  memset(&ign, 0, sizeof(ign));
  ign.sa_handler = added_a_pm;
  sigaction(SIGHUP, &ign, 0);
  ign.sa_handler = SIG_IGN;
  sigaction(SIGPIPE, &ign, 0);

  memset(&ign, 0, sizeof(ign));
  ign.sa_handler = fatalHandler;
  sigaction(SIGSEGV, &ign, 0);
  sigaction(SIGABRT, &ign, 0);
  sigaction(SIGFPE, &ign, 0);
#endif
}

int ServiceDMLProc::Child()
{
  BRM::DBRM dbrm;
  Oam oam;

  Config* cf = Config::makeConfig();

  if (setupCwd())
  {
    LoggingID logid(21, 0, 0);
    logging::Message::Args args1;
    logging::Message msg(1);
    args1.add("DMLProc couldn't cwd.");
    msg.format(args1);
    logging::Logger logger(logid.fSubsysID);
    logger.logMessage(LOG_TYPE_CRITICAL, msg, logid);
    NotifyServiceInitializationFailed();
    return 1;
  }

  WriteEngine::WriteEngineWrapper::init(WriteEngine::SUBSYSTEM_ID_DMLPROC);
#ifdef _MSC_VER
  // In windows, initializing the wrapper (A dll) does not set the static variables
  // in the main program
  idbdatafile::IDBPolicy::configIDBPolicy();
#endif

  //@Bug 1627
  try
  {
    rollbackAll(&dbrm);  // Rollback any
  }
  catch (std::exception& e)
  {
    logging::Message::Args args;
    logging::Message message(2);
    args.add("DMLProc failed to start due to :");
    args.add(e.what());
    message.format(args);
    logging::LoggingID lid(20);
    logging::MessageLog ml(lid);

    ml.logCriticalMessage(message);

    cerr << "DMLProc failed to start due to : " << e.what() << endl;
    NotifyServiceInitializationFailed();
    return 1;
  }

  int temp;
  int serverThreads = 10;
  int serverQueueSize = 0;
  const string DMLProc("DMLProc");

  temp = toInt(cf->getConfig(DMLProc, "ServerThreads"));

  if (temp > 0)
    serverThreads = temp;

  //	temp = toInt(cf->getConfig(DMLProc, "ServerQueueSize"));
  //	if (temp > 0)
  //			serverQueueSize = temp;

  // read and cleanup port before trying to use
  try
  {
    string port = cf->getConfig(DMLProc, "Port");
    string cmd = "fuser -k " + port + "/tcp >/dev/null 2>&1";

    // Couldn't check the return code b/c
    // fuser returns 1 for unused port.
#if defined(__GNUC__) && __GNUC__ >= 5
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
    (void)::system(cmd.c_str());
#pragma GCC diagnostic pop
#else
    (void)::system(cmd.c_str());
#endif
  }
  catch (...)
  {
  }

  DMLServer dmlserver(serverThreads, serverQueueSize, &dbrm);

  NotifyServiceStarted();

  ResourceManager* rm = ResourceManager::instance();

  // jobstepThreadPool is used by other processes. We can't call
  // resourcemanaager (rm) functions during the static creation of threadpool
  // because rm has a "isExeMgr" flag that is set upon creation (rm is a singleton).
  // From  the pools perspective, it has no idea if it is ExeMgr doing the
  // creation, so it has no idea which way to set the flag. So we set the max here.
  JobStep::jobstepThreadPool.setMaxThreads(rm->getJLThreadPoolSize());
  JobStep::jobstepThreadPool.setName("DMLProcJobList");

  if (rm->getDMLJlThreadPoolDebug() == "Y" || rm->getDMLJlThreadPoolDebug() == "y")
  {
    JobStep::jobstepThreadPool.setDebug(true);
    JobStep::jobstepThreadPool.invoke(threadpool::ThreadPoolMonitor(&JobStep::jobstepThreadPool));
    DMLServer::fDmlPackagepool.setDebug(true);
    DMLServer::fDmlPackagepool.invoke(threadpool::ThreadPoolMonitor(&DMLServer::fDmlPackagepool));
  }

  Dec = DistributedEngineComm::instance(rm);

  setupChildSignalHandlers();

  _exit(dmlserver.start());

  return 1;
}

int main(int argc, char** argv)
{
  Opt opt(argc, argv);

  // Set locale language
  setlocale(LC_ALL, "");
  setlocale(LC_NUMERIC, "C");
  // This is unset due to the way we start it
  program_invocation_short_name = const_cast<char*>("DMLProc");
  // Initialize the charset library
  MY_INIT(argv[0]);

  return ServiceDMLProc(opt).Run();
}

// vim:ts=4 sw=4:

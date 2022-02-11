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
 *   $Id: dmlprocessor.h 955 2013-03-20 19:37:27Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */
#ifndef DMLPROCESSOR_H
#define DMLPROCESSOR_H

#include <boost/scoped_ptr.hpp>
#include "threadpool.h"
#include "messagequeue.h"
#include "bytestream.h"
#include "insertdmlpackage.h"
#include "deletedmlpackage.h"
#include "updatedmlpackage.h"
#include "commanddmlpackage.h"
#include "insertpackageprocessor.h"
#include "deletepackageprocessor.h"
#include "updatepackageprocessor.h"
#include "commandpackageprocessor.h"
#include "messagelog.h"
#include "distributedenginecomm.h"
#include "batchinsertprocessor.h"
#include "dmlpackageprocessor.h"
#include "calpontsystemcatalog.h"
#include "dmlpackageprocessor.h"
#include "batchinsertprocessor.h"
#include "querytele.h"

template <typename T, typename Container = std::deque<T> >
class iterable_queue : public std::queue<T, Container>
{
 public:
  typedef typename Container::iterator iterator;
  typedef typename Container::const_iterator const_iterator;

  iterator begin()
  {
    return this->c.begin();
  }
  iterator end()
  {
    return this->c.end();
  }
  const_iterator begin() const
  {
    return this->c.begin();
  }
  const_iterator end() const
  {
    return this->c.end();
  }
  iterator find(T t)
  {
    iterator it;

    for (it = begin(); it != end(); ++it)
    {
      if (*it == t)
      {
        break;
      }
    }

    return it;
  }
  iterator erase(typename Container::iterator it)
  {
    return this->c.erase(it);
  }
  iterator erase(T t)
  {
    iterator it = find(t);

    if (it != end())
    {
      erase(it);
    }

    return it;
  }
};

namespace dmlprocessor
{
/** @brief main server thread
 */
class DMLServer
{
 public:
  DMLServer(int packageMaxThreads, int packageWorkQueueSize, BRM::DBRM* aDbrm);

  ~DMLServer()
  {
  }

  int start();

  /** @brief get the dml package thread pool size
   */
  inline int getPackageThreadPoolSize() const
  {
    return fPackageMaxThreads;
  }

  /** @brief set the dml package thread pool size
   *
   * @param threadPoolSize the maximum number of threads to process dml packages
   */
  inline void setPackageThreadPoolSize(int threadPoolSize)
  {
    fPackageMaxThreads = threadPoolSize;
  }

  /** @brief get the maximum number of dml packages allowed in the work queue
   */
  inline int getPackageWorkQueueSize() const
  {
    return fPackageWorkQueueSize;
  }

  /** @brief set the dml package work queue size
   *
   * @param workQueueSize the maximum number of dml packages in the work queue
   */
  inline void setPackageWorkQueueSize(int workQueueSize)
  {
    fPackageWorkQueueSize = workQueueSize;
  }

  inline bool pendingShutdown() const
  {
    return fShutdownFlag;
  }

  inline void startShutdown()
  {
    fShutdownFlag = true;
  }

 private:
  // not copyable
  DMLServer(const DMLServer& rhs);
  DMLServer& operator=(const DMLServer& rhs);

  int fPackageMaxThreads;    /** @brief max number of threads to process dml packages */
  int fPackageWorkQueueSize; /** @brief max number of packages waiting in the work queue */

  boost::scoped_ptr<messageqcpp::MessageQueueServer> fMqServer;
  BRM::DBRM* fDbrm;
  bool fShutdownFlag;

 public:
  /** @brief the thread pool for processing dml packages
   */
  static threadpool::ThreadPool fDmlPackagepool;
};

/** @brief Thread to process a single dml package.
 *  Created and run from DMLProcessor
 */
class PackageHandler
{
 public:
  PackageHandler(const messageqcpp::IOSocket& ios, boost::shared_ptr<messageqcpp::ByteStream> bs,
                 uint8_t packageType, joblist::DistributedEngineComm* ec, bool concurrentSuport,
                 uint64_t maxDeleteRows, uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnId,
                 BRM::DBRM* aDbrm, const querytele::QueryTeleClient& qtc,
                 boost::shared_ptr<execplan::CalpontSystemCatalog> csc);
  ~PackageHandler();

  void run();
  void rollbackPending();

  execplan::CalpontSystemCatalog::SCN getTxnid()
  {
    return fTxnid;
  }
  uint32_t getSessionID()
  {
    return fSessionID;
  }

 private:
  messageqcpp::IOSocket fIos;
  boost::shared_ptr<messageqcpp::ByteStream> fByteStream;
  boost::scoped_ptr<dmlpackageprocessor::DMLPackageProcessor> fProcessor;
  messageqcpp::ByteStream::quadbyte fPackageType;
  joblist::DistributedEngineComm* fEC;
  bool fConcurrentSupport;
  uint64_t fMaxDeleteRows;
  uint32_t fSessionID;
  uint32_t fTableOid;
  execplan::CalpontSystemCatalog::SCN fTxnid;
  execplan::SessionManager sessionManager;
  BRM::DBRM* fDbrm;
  querytele::QueryTeleClient fQtc;
  boost::shared_ptr<execplan::CalpontSystemCatalog> fcsc;

  // MCOL-140 A map to hold table oids so that we can wait for DML on a table.
  // This effectively creates a table lock for transactions.
  // Used to serialize operations because the VSS can't handle inserts
  // or updates on the same block.
  // When an Insert, Update or Delete command arrives, we look here
  // for the table oid. If found, wait until it is no longer here.
  // If this transactionID (SCN) is < the transactionID in the table, don't delay
  // and hope for the best, as we're already out of order.
  // When the VSS is engineered to handle transactions out of order, all MCOL-140
  // code is to be removed.
  int synchTableAccess(dmlpackage::CalpontDMLPackage* dmlPackage);
  int releaseTableAccess();
  int forceReleaseTableAccess();
  typedef iterable_queue<execplan::CalpontSystemCatalog::SCN> tableAccessQueue_t;
  static std::map<uint32_t, tableAccessQueue_t> tableOidMap;
  static boost::condition_variable tableOidCond;
  static boost::mutex tableOidMutex;

 public:
  static int clearTableAccess();

  // MCOL-3296 Add a class to call synchTableAccess on creation and
  // releaseTableAccess on destuction for exception safeness.
  class SynchTable
  {
   public:
    SynchTable() : fphp(NULL){};
    SynchTable(PackageHandler* php, dmlpackage::CalpontDMLPackage* dmlPackage)
    {
      setPackage(php, dmlPackage);
    }
    ~SynchTable()
    {
      if (fphp)
        fphp->releaseTableAccess();
    }
    bool setPackage(PackageHandler* php, dmlpackage::CalpontDMLPackage* dmlPackage)
    {
      if (fphp)
        fphp->releaseTableAccess();
      fphp = php;
      if (fphp)
        fphp->synchTableAccess(dmlPackage);
      return true;
    }

   private:
    PackageHandler* fphp;
  };
};

/** @brief processes dml packages as they arrive
 */
class DMLProcessor
{
 public:
  /** @brief ctor
   *
   * @param packageMaxThreads the maximum number of threads to process dml  packages
   * @param packageWorkQueueSize the maximum number of dml packages in the work queue
   */
  DMLProcessor(messageqcpp::IOSocket ios, BRM::DBRM* aDbrm);

  /** @brief entry point for the DMLProcessor
   */
  void operator()();

  static void log(const std::string& msg, logging::LOG_TYPE level);

 private:
  messageqcpp::IOSocket fIos;

  execplan::SessionManager sessionManager;
  boost::shared_ptr<execplan::CalpontSystemCatalog> csc;
  BRM::DBRM* fDbrm;
  querytele::QueryTeleClient fQtc;
  bool fConcurrentSupport;

  // A map to hold pointers to all active PackageProcessors
  typedef std::map<uint32_t, boost::shared_ptr<PackageHandler> > PackageHandlerMap_t;
  static PackageHandlerMap_t packageHandlerMap;
  static boost::mutex packageHandlerMapLock;

  // A map to hold pointers to all BatchInsertProc object
  static std::map<uint32_t, BatchInsertProc*> batchinsertProcessorMap;
  static boost::mutex batchinsertProcessorMapLock;

  friend struct CancellationThread;
  friend class PackageHandler;
};

class RollbackTransactionProcessor : public dmlpackageprocessor::DMLPackageProcessor
{
 public:
  RollbackTransactionProcessor(BRM::DBRM* aDbrm) : DMLPackageProcessor(aDbrm, 1)
  {
  }
  /** @brief process an Rollback transactions
   *
   * @param cpackage the UpdateDMLPackage to process
   */
  inline DMLResult processPackage(dmlpackage::CalpontDMLPackage& cpackage)
  {
    DMLResult result;
    result.result = NO_ERROR;
    return result;
  }

  void processBulkRollback(BRM::TableLockInfo lockInfo, BRM::DBRM* dbrm, uint64_t uniqueId,
                           oam::OamCache::dbRootPMMap_t& dbRootPMMap, bool& lockReleased);

 protected:
};

}  // namespace dmlprocessor

#endif  // DMLPROCESSOR_H
// vim:ts=4 sw=4:

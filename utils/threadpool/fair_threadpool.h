/* Copyright (c) 2022 MariaDB Corporation

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

#include <condition_variable>
#include <string>
#include <iostream>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include <atomic>
#include <queue>
#include <unordered_map>
#include <list>
#include <functional>

#include "primitives/primproc/umsocketselector.h"
#include "prioritythreadpool.h"

namespace threadpool
{
class FairThreadPool
{
 public:
  using Functor = PriorityThreadPool::Functor;

  using TransactionIdxT = uint32_t;
  struct Job
  {
    Job() : weight_(1), priority_(0), id_(0)
    {
    }
    Job(const uint32_t uniqueID, const uint32_t stepID, const TransactionIdxT txnIdx,
        const boost::shared_ptr<Functor>& functor, const primitiveprocessor::SP_UM_IOSOCK& sock,
        const uint32_t weight = 1, const uint32_t priority = 0, const uint32_t id = 0)
     : uniqueID_(uniqueID)
     , stepID_(stepID)
     , txnIdx_(txnIdx)
     , functor_(functor)
     , sock_(sock)
     , weight_(weight)
     , priority_(priority)
     , id_(id)
    {
    }
    uint32_t uniqueID_;
    uint32_t stepID_;
    TransactionIdxT txnIdx_;
    boost::shared_ptr<Functor> functor_;
    primitiveprocessor::SP_UM_IOSOCK sock_;
    uint32_t weight_;
    uint32_t priority_;
    uint32_t id_;
  };

  /*********************************************
   *  ctor/dtor
   *
   *********************************************/

  /** @brief ctor
   */

  FairThreadPool(uint targetWeightPerRun, uint highThreads, uint midThreads, uint lowThreads, uint id = 0);
  virtual ~FairThreadPool();

  void removeJobs(uint32_t id);
  void addJob(const Job& job, bool useLock = true);
  void stop();

  /** @brief for use in debugging
   */
  void dump();

  // If a job is blocked, we want to temporarily increase the number of threads managed by the pool
  // A problem can occur if all threads are running long or blocked for a single query. Other
  // queries won't get serviced, even though there are cpu cycles available.
  // These calls are currently protected by respondLock in sendThread(). If you call from other
  // places, you need to consider atomicity.
  void incBlockedThreads()
  {
    blockedThreads++;
  }
  void decBlockedThreads()
  {
    blockedThreads--;
  }
  uint32_t blockedThreadCount() const
  {
    return blockedThreads;
  }
  size_t queueSize() const
  {
    return weightedTxnsQueue_.size();
  }

 protected:
 private:
  struct ThreadHelper
  {
    ThreadHelper(FairThreadPool* impl, PriorityThreadPool::Priority queue) : ptp(impl), preferredQueue(queue)
    {
    }
    void operator()()
    {
      ptp->threadFcn(preferredQueue);
    }
    FairThreadPool* ptp;
    PriorityThreadPool::Priority preferredQueue;
  };

  explicit FairThreadPool();
  explicit FairThreadPool(const FairThreadPool&);
  FairThreadPool& operator=(const FairThreadPool&);

  PriorityThreadPool::Priority pickAQueue(PriorityThreadPool::Priority preference);
  void threadFcn(const PriorityThreadPool::Priority preferredQueue);
  void sendErrorMsg(uint32_t id, uint32_t step, primitiveprocessor::SP_UM_IOSOCK sock);

  uint32_t threadCounts;
  uint32_t defaultThreadCounts;
  std::mutex mutex;
  std::condition_variable newJob;
  boost::thread_group threads;
  bool _stop;
  uint32_t weightPerRun;
  volatile uint id;  // prevent it from being optimized out

  using WeightT = uint32_t;
  using WeightedTxnT = std::pair<WeightT, TransactionIdxT>;
  using WeightedTxnVec = std::vector<WeightedTxnT>;
  struct PrioQueueCmp
  {
    bool operator()(WeightedTxnT lhs, WeightedTxnT rhs)
    {
      if (lhs.first == rhs.first)
        return lhs.second > rhs.second;
      return lhs.first > rhs.first;
    }
  };
  using RunListT = std::vector<Job>;
  using RescheduleVecType = std::vector<bool>;
  using WeightedTxnPrioQueue = std::priority_queue<WeightedTxnT, WeightedTxnVec, PrioQueueCmp>;
  using ThreadPoolJobsList = std::list<Job>;
  using Txn2ThreadPoolJobsListMap = std::unordered_map<TransactionIdxT, ThreadPoolJobsList*>;
  Txn2ThreadPoolJobsListMap txn2JobsListMap_;
  WeightedTxnPrioQueue weightedTxnsQueue_;
  std::atomic<uint32_t> blockedThreads;
  std::atomic<uint32_t> extraThreads;
  bool stopExtra;
};

}  // namespace threadpool
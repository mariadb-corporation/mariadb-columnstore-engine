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

// The idea of this thread pool is to run morsel jobs(primitive job) is to equaly distribute CPU time
// b/w multiple parallel queries(thread maps morsel to query using txnId). Query(txnId) has its weight
// stored in PriorityQueue that thread increases before run another morsel for the query. When query is
// done(ThreadPoolJobsList is empty) it is removed from PQ and the Map(txn to ThreadPoolJobsList).
// I tested multiple morsels per one loop iteration in ::threadFcn. This approach reduces CPU consumption
// and increases query timings.
class FairThreadPool
{
 public:
  using Functor = PriorityThreadPool::Functor;

  using TransactionIdxT = uint32_t;
  struct Job
  {
    Job(const uint32_t uniqueID, const uint32_t stepID, const TransactionIdxT txnIdx,
        const boost::shared_ptr<Functor>& functor, const primitiveprocessor::SP_UM_IOSOCK& sock,
        const uint32_t weight, const uint32_t priority = 0, const uint32_t id = 0)
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
    // sock_ is nullptr here. This is kinda dangerous.
    Job(const uint32_t uniqueID, const uint32_t stepID, const TransactionIdxT txnIdx,
        const boost::shared_ptr<Functor>& functor, const uint32_t weight, const uint32_t priority = 0,
        const uint32_t id = 0)
     : uniqueID_(uniqueID)
     , stepID_(stepID)
     , txnIdx_(txnIdx)
     , functor_(functor)
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
  void addJob(const Job& job);
  void stop();

  /** @brief for use in debugging
   */
  void dump();

  size_t queueSize() const
  {
    return weightedTxnsQueue_.size();
  }
  // This method enables a pool current workload estimate.
  size_t jobsRunning() const
  {
    return jobsRunning_.load(std::memory_order_relaxed);
  }
  // If a job is blocked, we want to temporarily increase the number of threads managed by the pool
  // A problem can occur if all threads are running long or blocked for a single query. Other
  // queries won't get serviced, even though there are cpu cycles available.
  // These calls are currently protected by respondLock in sendThread(). If you call from other
  // places, you need to consider atomicity.
  void incBlockedThreads()
  {
    ++blockedThreads_;
  }
  void decBlockedThreads()
  {
    --blockedThreads_;
  }
  uint32_t blockedThreadCount()
  {
    return blockedThreads_;
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

  void threadFcn(const PriorityThreadPool::Priority preferredQueue);
  void sendErrorMsg(uint32_t id, uint32_t step, primitiveprocessor::SP_UM_IOSOCK sock);

  uint32_t defaultThreadCounts;
  std::mutex mutex;
  std::condition_variable newJob;
  boost::thread_group threads;
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
  std::atomic<size_t> jobsRunning_{0};
  std::atomic<size_t> threadCounts_{0};
  std::atomic<bool> stop_{false};

  std::atomic<uint32_t> blockedThreads_{0};
  std::atomic<uint32_t> extraThreads_{0};
  bool stopExtra_;
};

}  // namespace threadpool
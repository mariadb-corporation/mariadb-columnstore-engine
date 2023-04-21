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
 *   $Id: $
 *
 *
 ***********************************************************************/
/** @file */

#pragma once

#include <string>
#include <iostream>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <boost/thread/thread.hpp>
#include <map>
#include <mutex>
#include <condition_variable>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include <atomic>
#include "primitives/primproc/umsocketselector.h"
#include "atomicops.h"

namespace threadpool
{
class PriorityThreadPool
{
 public:
  class Functor
  {
   public:
    virtual ~Functor(){};
    // as of 12/3/13, all implementors return 0 and -1.  -1 will cause
    // this thread pool to reschedule the job, 0 will throw it away on return.
    virtual int operator()() = 0;
  };

  struct Job
  {
    Job() : weight(1), priority(0), id(0)
    {
    }
    boost::shared_ptr<Functor> functor;
    uint32_t weight;
    uint32_t priority;
    uint32_t id;
    uint32_t uniqueID;
    uint32_t stepID;
    primitiveprocessor::SP_UM_IOSOCK sock;
  };

  enum Priority
  {
    LOW,
    MEDIUM,
    HIGH,
    _COUNT,
    EXTRA  // After _COUNT because _COUNT is for jobQueue size and EXTRA isn't a jobQueue. But we need EXTRA
           // in places where Priority is used.
  };

  /*********************************************
   *  ctor/dtor
   *
   *********************************************/

  /** @brief ctor
   */

  PriorityThreadPool(uint targetWeightPerRun, uint highThreads, uint midThreads, uint lowThreads,
                     uint id = 0);
  virtual ~PriorityThreadPool();

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
  uint32_t blockedThreadCount()
  {
    return blockedThreads;
  }

 protected:
 private:
  struct ThreadHelper
  {
    ThreadHelper(PriorityThreadPool* impl, Priority queue) : ptp(impl), preferredQueue(queue)
    {
    }
    void operator()()
    {
      ptp->threadFcn(preferredQueue);
    }
    PriorityThreadPool* ptp;
    Priority preferredQueue;
  };

  explicit PriorityThreadPool();
  explicit PriorityThreadPool(const PriorityThreadPool&);
  PriorityThreadPool& operator=(const PriorityThreadPool&);

  Priority pickAQueue(Priority preference);
  void threadFcn(const Priority preferredQueue) throw();
  void sendErrorMsg(uint32_t id, uint32_t step, primitiveprocessor::SP_UM_IOSOCK sock);

  std::list<Job> jobQueues[3];  // higher indexes = higher priority
  uint32_t threadCounts[3];
  uint32_t defaultThreadCounts[3];
  std::mutex mutex;
  std::condition_variable newJob;
  boost::thread_group threads;
  bool _stop;
  uint32_t weightPerRun;
  volatile uint id;  // prevent it from being optimized out

  std::atomic<uint32_t> blockedThreads;
  std::atomic<uint32_t> extraThreads;
  bool stopExtra;
};

}  // namespace threadpool

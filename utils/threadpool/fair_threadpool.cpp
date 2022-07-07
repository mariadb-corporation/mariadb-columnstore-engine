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

#include <atomic>
#include <stdexcept>
#include <unistd.h>
#include <exception>
using namespace std;

#include "messageobj.h"
#include "messagelog.h"
#include "threadnaming.h"
using namespace logging;

#include "fair_threadpool.h"
using namespace boost;

#include "dbcon/joblist/primitivemsg.h"

namespace threadpool
{
FairThreadPool::FairThreadPool(uint targetWeightPerRun, uint highThreads, uint midThreads, uint lowThreads,
                               uint ID)
 : weightPerRun(targetWeightPerRun), id(ID), stopExtra_(false)
{
  boost::thread* newThread;
  size_t numberOfThreads = highThreads + midThreads + lowThreads;
  for (uint32_t i = 0; i < numberOfThreads; ++i)
  {
    newThread = threads.create_thread(ThreadHelper(this, PriorityThreadPool::Priority::HIGH));
    newThread->detach();
  }
  cout << "FairThreadPool started " << numberOfThreads << " thread/-s.\n";
  threadCounts_.store(numberOfThreads, std::memory_order_relaxed);
  defaultThreadCounts = numberOfThreads;
}

FairThreadPool::~FairThreadPool()
{
  stop();
}

void FairThreadPool::addJob(const Job& job)
{
  boost::thread* newThread;
  std::unique_lock<std::mutex> lk(mutex, std::defer_lock_t());

  // Create any missing threads
  if (defaultThreadCounts != threadCounts_.load(std::memory_order_relaxed))
  {
    newThread = threads.create_thread(ThreadHelper(this, PriorityThreadPool::Priority::HIGH));
    newThread->detach();
    threadCounts_.fetch_add(1, std::memory_order_relaxed);
  }

  lk.lock();
  // If some threads have blocked (because of output queue full)
  // Temporarily add some extra worker threads to make up for the blocked threads.
  if (blockedThreads_ > extraThreads_)
  {
    stopExtra_ = false;
    newThread = threads.create_thread(ThreadHelper(this, PriorityThreadPool::Priority::EXTRA));
    newThread->detach();
    ++extraThreads_;
  }
  else if (blockedThreads_ == 0)
  {
    // Release the temporary threads -- some threads have become unblocked.
    stopExtra_ = true;
  }

  auto jobsListMapIter = txn2JobsListMap_.find(job.txnIdx_);
  if (jobsListMapIter == txn2JobsListMap_.end())  // there is no txn in the map
  {
    ThreadPoolJobsList* jobsList = new ThreadPoolJobsList;
    jobsList->push_back(job);
    txn2JobsListMap_[job.txnIdx_] = jobsList;
    weightedTxnsQueue_.push({job.weight_, job.txnIdx_});
  }
  else  // txn is in the map
  {
    if (jobsListMapIter->second->empty())  // there are no jobs for the txn
    {
      weightedTxnsQueue_.push({job.weight_, job.txnIdx_});
    }
    jobsListMapIter->second->push_back(job);
  }

  newJob.notify_one();
}

void FairThreadPool::removeJobs(uint32_t id)
{
  std::unique_lock<std::mutex> lk(mutex);

  for (auto& txnJobsMapPair : txn2JobsListMap_)
  {
    ThreadPoolJobsList* txnJobsList = txnJobsMapPair.second;
    // txnJobsList must not be nullptr
    if (txnJobsList->empty())
    {
      // The next branching is for delayed clean-up that is
      // necessary to avoid  BATCH_PRIMITIVE_DESTROY job self-removal
      // running in the same ThreadPool.
      if (txnJobsList->emptyFlag)
      {
        txn2JobsListMap_.erase(txnJobsMapPair.first);
        delete txnJobsList;
      }
      else
      {
        txnJobsList->emptyFlag = true;
      }
      continue;
      // There is no clean-up for PQ. It will happen later in threadFcn
    }
    auto job = txnJobsList->begin();
    while (job != txnJobsList->end())
    {
      if (job->id_ == id)
      {
        job = txnJobsList->erase(job);  // update the job iter
        if (txnJobsList->empty())
        {
          txn2JobsListMap_.erase(txnJobsMapPair.first);
          delete txnJobsList;
          break;
          // There is no clean-up for PQ. It will happen later in threadFcn
        }
        continue;  // go-on skiping job iter increment
      }
      ++job;
    }
  }
}

void FairThreadPool::threadFcn(const PriorityThreadPool::Priority preferredQueue)
{
  utils::setThreadName("Idle");
  RunListT runList(1);  // This is a vector to allow to grab multiple jobs
  RescheduleVecType reschedule;
  bool running = false;
  bool rescheduleJob = false;

  try
  {
    while (!stop_.load(std::memory_order_relaxed))
    {
      runList.clear();  // remove the job
      std::unique_lock<std::mutex> lk(mutex);

      if (weightedTxnsQueue_.empty())
      {
        // If this is an EXTRA thread due toother threads blocking, and all blockers are unblocked,
        // we don't want this one any more.
        if (preferredQueue == PriorityThreadPool::Priority::EXTRA && stopExtra_)
        {
          --extraThreads_;
          return;
        }
        newJob.wait(lk);
        continue;  // just go on w/o re-taking the lock
      }

      WeightedTxnT weightedTxn = weightedTxnsQueue_.top();
      auto txnAndJobListPair = txn2JobsListMap_.find(weightedTxn.second);
      // Looking for non-empty jobsList in a loop
      // The loop waits on newJob cond_var if PQ is empty(no jobs in this thread pool)
      while (txnAndJobListPair == txn2JobsListMap_.end() || txnAndJobListPair->second->empty())
      {
        // JobList is empty. This can happen when this method pops the last Job.
        if (txnAndJobListPair != txn2JobsListMap_.end())
        {
          ThreadPoolJobsList* txnJobsList = txnAndJobListPair->second;
          delete txnJobsList;
          txn2JobsListMap_.erase(txnAndJobListPair->first);
        }
        weightedTxnsQueue_.pop();
        if (weightedTxnsQueue_.empty())  // remove the empty
        {
          break;
        }
        weightedTxn = weightedTxnsQueue_.top();
        txnAndJobListPair = txn2JobsListMap_.find(weightedTxn.second);
      }

      if (weightedTxnsQueue_.empty())
      {
        newJob.wait(lk);  // might need a lock here
        continue;
      }

      // We have non-empty jobsList at this point.
      // Remove the txn from a queue first to add it later
      weightedTxnsQueue_.pop();
      TransactionIdxT txnIdx = txnAndJobListPair->first;
      ThreadPoolJobsList* jobsList = txnAndJobListPair->second;
      runList.push_back(jobsList->front());

      jobsList->pop_front();
      // Add the jobList back into the PQ adding some weight to it
      // Current algo doesn't reduce total txn weight if the job is rescheduled.
      if (!jobsList->empty())
      {
        weightedTxnsQueue_.push({weightedTxn.first + runList[0].weight_, txnIdx});
      }

      lk.unlock();

      running = true;
      jobsRunning_.fetch_add(1, std::memory_order_relaxed);
      rescheduleJob = (*(runList[0].functor_))();  // run the functor
      jobsRunning_.fetch_sub(1, std::memory_order_relaxed);
      running = false;

      utils::setThreadName("Idle");

      if (rescheduleJob)
      {
        // to avoid excessive CPU usage waiting for data from storage
        usleep(500);
        addJob(runList[0]);
      }
    }
  }
  catch (std::exception& ex)
  {
    // std::cout << "FairThreadPool::threadFcn(): std::exception - no reschedule but send an error" <<
    // std::endl;
    if (running)
    {
      jobsRunning_.fetch_sub(1, std::memory_order_relaxed);
    }
    // Log the exception and exit this thread
    try
    {
      threadCounts_.fetch_sub(1, std::memory_order_relaxed);
#ifndef NOLOGGING
      logging::Message::Args args;
      logging::Message message(5);
      args.add("threadFcn: Caught exception: ");
      args.add(ex.what());

      message.format(args);

      logging::LoggingID lid(22);
      logging::MessageLog ml(lid);

      ml.logErrorMessage(message);
#endif

      if (running)
      {
        sendErrorMsg(runList[0].uniqueID_, runList[0].stepID_, runList[0].sock_);
      }
    }
    catch (...)
    {
      std::cout << "FairThreadPool::threadFcn(): std::exception - double exception: failed to send an error"
                << std::endl;
    }
  }
  catch (...)
  {
    // std::cout << "FairThreadPool::threadFcn(): ... exception - no reschedule but send an error" <<
    // std::endl; Log the exception and exit this thread
    try
    {
      if (running)
      {
        jobsRunning_.fetch_sub(1, std::memory_order_relaxed);
      }
      threadCounts_.fetch_sub(1, std::memory_order_relaxed);
#ifndef NOLOGGING
      logging::Message::Args args;
      logging::Message message(6);
      args.add("threadFcn: Caught unknown exception!");

      message.format(args);

      logging::LoggingID lid(22);
      logging::MessageLog ml(lid);

      ml.logErrorMessage(message);
#endif

      if (running)
        sendErrorMsg(runList[0].uniqueID_, runList[0].stepID_, runList[0].sock_);
    }
    catch (...)
    {
      std::cout << "FairThreadPool::threadFcn(): ... exception - double exception: failed to send an error"
                << std::endl;
    }
  }
}

void FairThreadPool::sendErrorMsg(uint32_t id, uint32_t step, primitiveprocessor::SP_UM_IOSOCK sock)
{
  ISMPacketHeader ism;
  PrimitiveHeader ph = {0, 0, 0, 0, 0, 0};

  ism.Status = logging::primitiveServerErr;
  ph.UniqueID = id;
  ph.StepID = step;
  messageqcpp::ByteStream msg(sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
  msg.append((uint8_t*)&ism, sizeof(ism));
  msg.append((uint8_t*)&ph, sizeof(ph));

  sock->write(msg);
}

void FairThreadPool::stop()
{
  stop_.store(true, std::memory_order_relaxed);
}

}  // namespace threadpool
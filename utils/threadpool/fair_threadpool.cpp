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
 : _stop(false), weightPerRun(targetWeightPerRun), id(ID), blockedThreads(0), extraThreads(0), stopExtra(true)
{
  boost::thread* newThread;
  size_t numberOfThreads = highThreads + midThreads + lowThreads;
  for (uint32_t i = 0; i < numberOfThreads; ++i)
  {
    newThread = threads.create_thread(ThreadHelper(this, PriorityThreadPool::Priority::HIGH));
    newThread->detach();
  }
  cout << "FairThreadPool started " << numberOfThreads << " thread/-s.\n";
  defaultThreadCounts = threadCounts = numberOfThreads;
}

FairThreadPool::~FairThreadPool()
{
  stop();
}

void FairThreadPool::addJob(const Job& job)
{
  addJob_(job);
}

void FairThreadPool::addJob_(const Job& job, bool useLock)
{
  boost::thread* newThread;
  std::unique_lock<std::mutex> lk(mutex, std::defer_lock_t());

  if (useLock)
    lk.lock();

  // Create any missing threads
  if (defaultThreadCounts != threadCounts)
  {
    newThread = threads.create_thread(ThreadHelper(this, PriorityThreadPool::Priority::HIGH));
    newThread->detach();
    ++threadCounts;
  }

  // If some threads have blocked (because of output queue full)
  // Temporarily add some extra worker threads to make up for the blocked threads.
  if (blockedThreads > extraThreads)
  {
    stopExtra = false;
    newThread = threads.create_thread(ThreadHelper(this, PriorityThreadPool::Priority::EXTRA));
    newThread->detach();
    extraThreads++;
  }
  else if (blockedThreads == 0)
  {
    // Release the temporary threads -- some threads have become unblocked.
    stopExtra = true;
  }

  auto jobsListMapIter = txn2JobsListMap_.find(job.txnIdx_);
  if (jobsListMapIter == txn2JobsListMap_.end())
  {
    ThreadPoolJobsList* jobsList = new ThreadPoolJobsList;
    jobsList->push_back(job);
    txn2JobsListMap_[job.txnIdx_] = jobsList;
    WeightT currentTopWeight = weightedTxnsQueue_.empty() ? 0 : weightedTxnsQueue_.top().first;
    weightedTxnsQueue_.push({currentTopWeight, job.txnIdx_});
  }
  else
  {
    jobsListMapIter->second->push_back(job);
  }

  if (useLock)
    newJob.notify_one();
}

void FairThreadPool::removeJobs(uint32_t id)
{
  std::unique_lock<std::mutex> lk(mutex);

  for (auto& txnJobsMapPair : txn2JobsListMap_)
  {
    ThreadPoolJobsList* txnJobsList = txnJobsMapPair.second;
    auto job = txnJobsList->begin();
    while (job != txnJobsList->end())
    {
      if (job->id_ == id)
      {
        job = txnJobsList->erase(job); // update the job iter
        if (txnJobsList->empty())
        {
          txn2JobsListMap_.erase(txnJobsMapPair.first);
          delete txnJobsList;
          break;
          // There is no clean-up for PQ. It will happen later in threadFcn
        }
        continue; // go-on skiping job iter increment
      }
      ++job;
    }
  }
}

void FairThreadPool::threadFcn(const PriorityThreadPool::Priority preferredQueue)
{
  if (preferredQueue == PriorityThreadPool::Priority::EXTRA)
    utils::setThreadName("Extra");
  else
    utils::setThreadName("Idle");
  RunListT runList; // This is a vector to allow to grab multiple jobs
  RescheduleVecType reschedule;
  bool running = false;
  bool rescheduleJob = false;

  try
  {
    while (!_stop)
    {
      runList.clear(); // remove the job
      std::unique_lock<std::mutex> lk(mutex);

      if (preferredQueue == PriorityThreadPool::Priority::EXTRA && stopExtra)
      {
        --extraThreads;
        return;
      }

      if (weightedTxnsQueue_.empty())
      {
        newJob.wait(lk);
        continue; // just go on w/o re-taking the lock
      }

      WeightedTxnT weightedTxn = weightedTxnsQueue_.top();
      auto txnAndJobListPair = txn2JobsListMap_.find(weightedTxn.second);
      // Looking for non-empty jobsList in a loop
      // Waiting on cond_var if PQ is empty(no jobs in this thread pool)
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
        if (weightedTxnsQueue_.empty()) // remove the empty
        {
          break;
        }
        weightedTxn = weightedTxnsQueue_.top();
        txnAndJobListPair = txn2JobsListMap_.find(weightedTxn.second);
      }

      if (weightedTxnsQueue_.empty())
      {
        newJob.wait(lk); // might need a lock here
        continue;
      }

      // We have non-empty jobsList at this point.
      // Remove the txn from a queue first to add it later
      weightedTxnsQueue_.pop();
      TransactionIdxT txnIdx = txnAndJobListPair->first;
      ThreadPoolJobsList* jobsList = txnAndJobListPair->second;
      // Job& job = jobsList->front();
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
      rescheduleJob = (*(runList[0].functor_))();
      running = false;

      utils::setThreadName("Idle");

      if (rescheduleJob)
      {
        lk.lock();
        addJob_(runList[0], false);
        newJob.notify_one();
        lk.unlock();
      }
    }
  }
  catch (std::exception& ex)
  {
    // Log the exception and exit this thread
    try
    {
      --threadCounts;
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
        sendErrorMsg(runList[0].uniqueID_, runList[0].stepID_, runList[0].sock_);
    }
    catch (...)
    {
    }
  }
  catch (...)
  {
    // Log the exception and exit this thread
    try
    {
      --threadCounts;
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
  _stop = true;
}

}  // namespace threadpool
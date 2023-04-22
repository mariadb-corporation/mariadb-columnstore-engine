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
 *   $Id: threadpool.cpp 3495 2013-01-21 14:09:51Z rdempsey $
 *
 *
 ***********************************************************************/
#include <stdexcept>
#include <iostream>
using namespace std;

#include "messageobj.h"
#include "messagelog.h"
using namespace logging;

#include "threadpool.h"
#include "threadnaming.h"
#include <iomanip>
#include <sstream>
#include "boost/date_time/posix_time/posix_time_types.hpp"
#include "mcsconfig.h"

namespace threadpool
{
ThreadPool::ThreadPool() : fMaxThreads(0), fQueueSize(0)
{
  init();
}

ThreadPool::ThreadPool(size_t maxThreads, size_t queueSize)
 : fMaxThreads(maxThreads), fQueueSize(queueSize), fPruneThread(NULL)
{
  init();
}

ThreadPool::~ThreadPool() throw()
{
  try
  {
    boost::mutex::scoped_lock initLock(fInitMutex);
    stop();
  }
  catch (...)
  {
  }
}

void ThreadPool::init()
{
  boost::mutex::scoped_lock initLock(fInitMutex);
  fThreadCount = 0;
  fGeneralErrors = 0;
  fFunctorErrors = 0;
  waitingFunctorsSize = 0;
  fIssued = 0;
  fDebug = false;
  fStop = false;
  fNextFunctor = fWaitingFunctors.end();
  fNextHandle = 1;
  fPruneThread = new boost::thread(boost::bind(&ThreadPool::pruneThread, this));
}

void ThreadPool::setQueueSize(size_t queueSize)
{
  boost::mutex::scoped_lock lock1(fMutex);
  fQueueSize = queueSize;
}

void ThreadPool::pruneThread()
{
  utils::setThreadName("pruneThread");
  boost::unique_lock<boost::mutex> lock2(fPruneMutex);

  while (true)
  {
    if (fStop)
      return;
    if (fPruneThreadEnd.wait_for(lock2, boost::chrono::minutes{1}) == boost::cv_status::timeout)
    {
      while (!fPruneThreads.empty())
      {
        if (fDebug)
        {
          ostringstream oss;
          oss << "pruning thread " << fPruneThreads.top();
          logging::Message::Args args;
          logging::Message message(0);
          args.add(oss.str());
          message.format(args);
          logging::LoggingID lid(22);
          logging::MessageLog ml(lid);
          ml.logWarningMessage(message);
        }
        fThreads.join_one(fPruneThreads.top());
        fPruneThreads.pop();
      }
    }
    else
    {
      break;
    }
  }
}

void ThreadPool::setMaxThreads(size_t maxThreads)
{
  boost::mutex::scoped_lock lock1(fMutex);
  fMaxThreads = maxThreads;
}

void ThreadPool::stop()
{
  boost::mutex::scoped_lock lock1(fMutex);
  if (fStop)
    return;  // Was stopped earlier
  fStop = true;
  lock1.unlock();

  fPruneThreadEnd.notify_all();
  fPruneThread->join();
  delete fPruneThread;
  fNeedThread.notify_all();
  fThreads.join_all();
}

void ThreadPool::wait()
{
  boost::mutex::scoped_lock lock1(fMutex);

  while (waitingFunctorsSize > 0)
  {
    fThreadAvailable.wait(lock1);
    // cerr << "woke!" << endl;
  }
}

void ThreadPool::join(uint64_t thrHandle)
{
  boost::mutex::scoped_lock lock1(fMutex);

  while (waitingFunctorsSize > 0)
  {
    Container_T::iterator iter;
    Container_T::iterator end = fWaitingFunctors.end();
    bool foundit = false;

    for (iter = fWaitingFunctors.begin(); iter != end; ++iter)
    {
      foundit = false;

      if (iter->hndl == thrHandle)
      {
        foundit = true;
        break;
      }
    }

    if (!foundit)
    {
      break;
    }

    fThreadAvailable.wait(lock1);
  }
}

void ThreadPool::join(std::vector<uint64_t>& thrHandle)
{
  boost::mutex::scoped_lock lock1(fMutex);

  while (waitingFunctorsSize > 0)
  {
    Container_T::iterator iter;
    Container_T::iterator end = fWaitingFunctors.end();
    bool foundit = false;

    for (iter = fWaitingFunctors.begin(); iter != end; ++iter)
    {
      foundit = false;
      std::vector<uint64_t>::iterator thrIter;
      std::vector<uint64_t>::iterator thrEnd = thrHandle.end();

      for (thrIter = thrHandle.begin(); thrIter != thrEnd; ++thrIter)
      {
        if (iter->hndl == *thrIter)
        {
          foundit = true;
          break;
        }
      }

      if (foundit == true)
      {
        break;
      }
    }

    // If we didn't find any of the handles, then all are complete
    if (!foundit)
    {
      break;
    }

    fThreadAvailable.wait(lock1);
  }
}

uint64_t ThreadPool::invoke(const Functor_T& threadfunc)
{
  boost::mutex::scoped_lock lock1(fMutex);
  uint64_t thrHandle = 0;

  for (;;)
  {
    try
    {
      if (waitingFunctorsSize < fThreadCount)
      {
        // Don't create a thread unless it's needed.  There
        // is a thread available to service this request.
        thrHandle = addFunctor(threadfunc);
        lock1.unlock();
        break;
      }

      bool bAdded = false;

      if (waitingFunctorsSize < fQueueSize || fQueueSize == 0)
      {
        // Don't create a thread unless you have to
        thrHandle = addFunctor(threadfunc);
        bAdded = true;
      }

      if (fDebug)
      {
        ostringstream oss;
        oss << "invoke thread "
            << " on " << fName << " max " << fMaxThreads << " queue " << fQueueSize << " threads "
            << fThreadCount << " running " << fIssued << " waiting " << (waitingFunctorsSize - fIssued)
            << " total " << waitingFunctorsSize;
        logging::Message::Args args;
        logging::Message message(0);
        args.add(oss.str());
        message.format(args);
        logging::LoggingID lid(22);
        logging::MessageLog ml(lid);
        ml.logWarningMessage(message);
      }

      // fQueueSize = 0 disables the queue and is an indicator to allow any number of threads to actually run.
      if (fThreadCount < fMaxThreads || fQueueSize == 0)
      {
        ++fThreadCount;

        lock1.unlock();
        fThreads.create_thread(beginThreadFunc(*this));

        if (bAdded)
          break;

        // If the mutex is unlocked before creating the thread
        // this allows fThreadAvailable to be triggered
        // before the wait below runs.  So run the loop again.
        lock1.lock();
        continue;
      }

      if (bAdded)
      {
        lock1.unlock();
        break;
      }

      if (fDebug)
      {
        logging::Message::Args args;
        logging::Message message(5);
        args.add("invoke: Blocked waiting for thread. Count ");
        args.add(static_cast<uint64_t>(fThreadCount));
        args.add("max ");
        args.add(static_cast<uint64_t>(fMaxThreads));
        message.format(args);
        logging::LoggingID lid(22);
        logging::MessageLog ml(lid);
        ml.logWarningMessage(message);
      }

      fThreadAvailable.wait(lock1);
    }
    catch (...)
    {
      ++fGeneralErrors;
      throw;
    }
  }

  fNeedThread.notify_one();
  return thrHandle;
}

void ThreadPool::beginThread() throw()
{
  utils::setThreadName("Idle");
  try
  {
    boost::unique_lock<boost::mutex> lock1(fMutex);

    for (;;)
    {
      if (fStop)
        break;

      if (fNextFunctor == fWaitingFunctors.end())
      {
        // Wait until someone needs a thread
        // Add the timed wait for queueSize == 0 so we can idle away threads
        // over fMaxThreads
        if (fQueueSize > 0)
        {
          fNeedThread.wait(lock1);
        }
        else
        {
          // Wait no more than 10 minutes
          if (fNeedThread.wait_for(lock1, boost::chrono::minutes{10}) == boost::cv_status::timeout)
          {
            if (fThreadCount > fMaxThreads)
            {
              boost::mutex::scoped_lock lock2(fPruneMutex);
              fPruneThreads.push(boost::this_thread::get_id());
              --fThreadCount;
              return;
            }
          }
        }
      }
      else
      {
        // If there's anything waiting, run it
        if (waitingFunctorsSize - fIssued > 0)
        {
          Container_T::iterator todo = fNextFunctor++;
          ++fIssued;

          if (fDebug)
          {
            ostringstream oss;
            oss << "starting thread "
                << " on " << fName << " max " << fMaxThreads << " queue " << fQueueSize << " threads "
                << fThreadCount << " running " << fIssued << " waiting " << (waitingFunctorsSize - fIssued)
                << " total " << waitingFunctorsSize;
            logging::Message::Args args;
            logging::Message message(0);
            args.add(oss.str());
            message.format(args);
            logging::LoggingID lid(22);
            logging::MessageLog ml(lid);
            ml.logWarningMessage(message);
          }

          lock1.unlock();

          utils::setThreadName("Unspecified");
          try
          {
            todo->functor();
          }
          catch (exception& e)
          {
            ++fFunctorErrors;
#ifndef NOLOGGING
            logging::Message::Args args;
            logging::Message message(5);
            args.add("ThreadPool: Caught exception during execution: ");
            args.add(e.what());
            message.format(args);
            logging::LoggingID lid(22);
            logging::MessageLog ml(lid);
            ml.logErrorMessage(message);
#endif
          }

          utils::setThreadName("Idle");
          lock1.lock();
          --fIssued;
          --waitingFunctorsSize;
          fWaitingFunctors.erase(todo);
          if (fDebug)
          {
            ostringstream oss;
            oss << "Ending thread "
                << " on " << fName << " max " << fMaxThreads << " queue " << fQueueSize << " threads "
                << fThreadCount << " running " << fIssued << " waiting " << (waitingFunctorsSize - fIssued)
                << " total " << waitingFunctorsSize;
            logging::Message::Args args;
            logging::Message message(0);
            args.add(oss.str());
            message.format(args);
            logging::LoggingID lid(22);
            logging::MessageLog ml(lid);
            ml.logWarningMessage(message);
          }
        }
        fThreadAvailable.notify_all();
      }
    }
  }
  catch (exception& ex)
  {
    ++fGeneralErrors;

    // Log the exception and exit this thread
    try
    {
#ifndef NOLOGGING
      logging::Message::Args args;
      logging::Message message(5);
      args.add("beginThread: Caught exception: ");
      args.add(ex.what());

      message.format(args);

      logging::LoggingID lid(22);
      logging::MessageLog ml(lid);

      ml.logErrorMessage(message);
#endif
    }
    catch (...)
    {
    }
  }
  catch (...)
  {
    ++fGeneralErrors;

    // Log the exception and exit this thread
    try
    {
#ifndef NOLOGGING
      logging::Message::Args args;
      logging::Message message(6);
      args.add("beginThread: Caught unknown exception!");

      message.format(args);

      logging::LoggingID lid(22);
      logging::MessageLog ml(lid);

      ml.logErrorMessage(message);
#endif
    }
    catch (...)
    {
    }
  }
}

uint64_t ThreadPool::addFunctor(const Functor_T& func)
{
  bool bAtEnd = false;

  if (fNextFunctor == fWaitingFunctors.end())
    bAtEnd = true;

  PoolFunction_T poolFunction;
  poolFunction.hndl = fNextHandle;
  poolFunction.functor = func;
  fWaitingFunctors.push_back(poolFunction);
  waitingFunctorsSize++;

  if (bAtEnd)
  {
    --fNextFunctor;
  }

  return fNextHandle++;
}

void ThreadPool::dump()
{
  std::cout << "General Errors: " << fGeneralErrors << std::endl;
  std::cout << "Functor Errors: " << fFunctorErrors << std::endl;
  std::cout << "Waiting functors: " << fWaitingFunctors.size() << std::endl;
}

void ThreadPoolMonitor::operator()()
{
  ostringstream filename;
  filename << MCSLOGDIR << "/trace/ThreadPool_" << fPool->name() << ".log";
  fLog = new ofstream(filename.str().c_str());

  for (;;)
  {
    if (!fLog || !fLog->is_open())
    {
      ostringstream oss;
      oss << "ThreadPoolMonitor " << fPool->name() << " has no file ";
      logging::Message::Args args;
      logging::Message message(0);
      args.add(oss.str());
      message.format(args);
      logging::LoggingID lid(22);
      logging::MessageLog ml(lid);
      ml.logWarningMessage(message);
      return;
    }

    // Get a timestamp for output.
    struct tm tm;
    struct timeval tv;

    gettimeofday(&tv, 0);
    localtime_r(&tv.tv_sec, &tm);

    (*fLog) << setfill('0') << setw(2) << tm.tm_hour << ':' << setw(2) << tm.tm_min << ':' << setw(2)
            << tm.tm_sec << '.' << setw(4) << tv.tv_usec / 100 << " Name " << fPool->fName << " Active "
            << fPool->waitingFunctorsSize << " running " << fPool->fIssued << " waiting "
            << (fPool->waitingFunctorsSize - fPool->fIssued) << " ThdCnt " << fPool->fThreadCount << " Max "
            << fPool->fMaxThreads << " Q " << fPool->fQueueSize << endl;

    //		struct timespec req = { 0, 1000 * 100 }; //100 usec
    //		nanosleep(&req, 0);
    sleep(2);
  }
}

}  // namespace threadpool

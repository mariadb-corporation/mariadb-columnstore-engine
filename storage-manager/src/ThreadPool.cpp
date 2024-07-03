/* Copyright (C) 2019 MariaDB Corporation

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

#include "ThreadPool.h"
#include <iostream>

using namespace std;

namespace storagemanager
{
ThreadPool::ThreadPool() : maxThreads(1000), die(false), processQueueOnExit(true), threadsWaiting(0)
{
  // Using this ctor effectively limits the # of threads here to the natural limit of the
  // context it's used in.  In the CRP class for example, the # of active threads would be
  // naturally limited by the # of concurrent operations.
  logger = SMLogging::get();
  pruner = boost::thread([this] { this->prune(); });
}

ThreadPool::ThreadPool(uint num_threads, bool _processQueueOnExit)
 : maxThreads(num_threads), die(false), processQueueOnExit(_processQueueOnExit), threadsWaiting(0)
{
  logger = SMLogging::get();
  pruner = boost::thread([this] { this->prune(); });
}

ThreadPool::~ThreadPool()
{
  boost::unique_lock<boost::mutex> s(mutex);
  die = true;
  if (!processQueueOnExit)
    jobs.clear();

  jobAvailable.notify_all();
  s.unlock();

  threads.join_all();
  pruner.interrupt();
  pruner.join();
}

void ThreadPool::setName(const string& _name)
{
  name = _name;
}

void ThreadPool::addJob(const boost::shared_ptr<Job>& j)
{
  boost::unique_lock<boost::mutex> s(mutex);
  if (die)
    return;

  jobs.push_back(j);
  // Start another thread if necessary
  if (threadsWaiting == 0 && (threads.size() - pruneable.size()) < maxThreads)
  {
    boost::thread* thread = threads.create_thread([this] { this->processingLoop(); });
    s_threads.insert(thread);
    // if (name == "Downloader")
    //    cout << "Threadpool (" << name << "): created a new thread.  live threads = " <<
    //        threads.size() - pruneable.size() << " job count = " << jobs.size() << endl;
  }
  else
    jobAvailable.notify_one();
}

void ThreadPool::prune()
{
  set<ID_Thread>::iterator it;
  boost::unique_lock<boost::mutex> s(mutex);

  while (1)
  {
    while (pruneable.empty() && !die)
      somethingToPrune.wait(s);

    if (die)
      break;

    for (auto& id : pruneable)
    {
      it = s_threads.find(id);
      assert(it != s_threads.end());
      it->thrd->join();
      threads.remove_thread(it->thrd);
      s_threads.erase(it);
    }
    pruneable.clear();
  }
}

void ThreadPool::setMaxThreads(uint newMax)
{
  boost::unique_lock<boost::mutex> s(mutex);
  maxThreads = newMax;
}

int ThreadPool::currentQueueSize() const
{
  boost::unique_lock<boost::mutex> s(mutex);
  return jobs.size();
}

void ThreadPool::processingLoop()
{
  boost::unique_lock<boost::mutex> s(mutex);
  try
  {
    _processingLoop(s);
  }
  catch (...)
  {
  }
  //  _processingLoop returns with the lock held

  pruneable.push_back(boost::this_thread::get_id());
  somethingToPrune.notify_one();
  // if (name == "Downloader")
  //    cout << "threadpool " << name << " thread exiting.  jobs = " << jobs.size() <<
  //      " live thread count = " << threads.size() - pruneable.size() << endl;
}

void ThreadPool::_processingLoop(boost::unique_lock<boost::mutex>& s)
{
  // cout << "Thread started" << endl;
  while (threads.size() - pruneable.size() <=
         maxThreads)  // this is the scale-down mechanism when maxThreads is changed
  {
    while (jobs.empty() && !die)
    {
      threadsWaiting++;
      bool timedout = !jobAvailable.timed_wait<>(s, idleThreadTimeout);
      threadsWaiting--;
      if (timedout && jobs.empty())
      {
        // cout << "Thread exiting b/c of timeout" << endl;
        return;
      }
    }
    if (jobs.empty())
    {  // die was set, and there are no jobs left to process
      // cout << "Thread exiting b/c of die (?)" << endl;
      return;
    }
    boost::shared_ptr<Job> job = jobs.front();
    jobs.pop_front();

    s.unlock();
    try
    {
      (*job)();
    }
    catch (exception& e)
    {
      logger->log(LOG_CRIT, "ThreadPool (%s): caught '%s' from a job", name.c_str(), e.what());
    }
    s.lock();
  }
  // cout << "Thread exiting b/c of pruning logic" << endl;
}

inline bool ThreadPool::id_compare::operator()(const ID_Thread& t1, const ID_Thread& t2) const
{
  return t1.id < t2.id;
}

ThreadPool::ID_Thread::ID_Thread(boost::thread::id& i) : id(i)
{
}

ThreadPool::ID_Thread::ID_Thread(boost::thread* t) : id(t->get_id()), thrd(t)
{
}

}  // namespace storagemanager

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
 *
 *   Work dervied from Devguy.com's Open Source C++ thread pool implementation
 *	released under public domain:
 *	http://web.archive.org/liveweb/http://dgpctk.cvs.sourceforge.net/viewvc/dgpctk/dgc%2B%2B/include/dg/thread/threadpool.h?revision=1.22&content-type=text%2Fplain
 *
 *	http://web.archive.org/web/20100104101109/http://devguy.com/bb/viewtopic.php?t=460
 *
 ***********************************************************************/

/** @file */

#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <string>
#include <fstream>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <stack>
#include <stdint.h>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/locks.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include <boost/chrono/chrono.hpp>

#include <memory>

#if defined(_MSC_VER) && defined(xxxTHREADPOOL_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace threadpool
{
// Taken from boost::thread_group and adapted
class ThreadPoolGroup
{
 private:
  ThreadPoolGroup(ThreadPoolGroup const&);
  ThreadPoolGroup& operator=(ThreadPoolGroup const&);

 public:
  ThreadPoolGroup()
  {
  }
  ~ThreadPoolGroup()
  {
    for (std::list<boost::thread*>::iterator it = threads.begin(), end = threads.end(); it != end; ++it)
    {
      delete *it;
    }
  }

  template <typename F>
  boost::thread* create_thread(F threadfunc)
  {
    boost::lock_guard<boost::shared_mutex> guard(m);
#if __cplusplus >= 201103L
    std::unique_ptr<boost::thread> new_thread(new boost::thread(threadfunc));
#else
    std::auto_ptr<boost::thread> new_thread(new boost::thread(threadfunc));
#endif
    threads.push_back(new_thread.get());
    return new_thread.release();
  }

  void add_thread(boost::thread* thrd)
  {
    if (thrd)
    {
      boost::lock_guard<boost::shared_mutex> guard(m);
      threads.push_back(thrd);
    }
  }

  void remove_thread(boost::thread* thrd)
  {
    boost::lock_guard<boost::shared_mutex> guard(m);
    std::list<boost::thread*>::iterator const it = std::find(threads.begin(), threads.end(), thrd);
    if (it != threads.end())
    {
      threads.erase(it);
    }
  }

  void join_all()
  {
    boost::shared_lock<boost::shared_mutex> guard(m);

    for (std::list<boost::thread*>::iterator it = threads.begin(), end = threads.end(); it != end; ++it)
    {
      (*it)->join();
    }
  }

  void interrupt_all()
  {
    boost::shared_lock<boost::shared_mutex> guard(m);

    for (std::list<boost::thread*>::iterator it = threads.begin(), end = threads.end(); it != end; ++it)
    {
      (*it)->interrupt();
    }
  }

  size_t size() const
  {
    boost::shared_lock<boost::shared_mutex> guard(m);
    return threads.size();
  }

  void join_one(boost::thread::id id)
  {
    boost::shared_lock<boost::shared_mutex> guard(m);
    for (std::list<boost::thread*>::iterator it = threads.begin(), end = threads.end(); it != end; ++it)
    {
      if ((*it)->get_id() == id)
      {
        (*it)->join();
        threads.erase(it);
        return;
      }
    }
  }

 private:
  std::list<boost::thread*> threads;
  mutable boost::shared_mutex m;
};

/** @brief ThreadPool is a component for working with pools of threads and asynchronously
 * executing tasks. It is responsible for creating threads and tracking which threads are "busy"
 * and which are idle. Idle threads are utilized as "work" is added to the system.
 */
class ThreadPool
{
 public:
  typedef boost::function0<void> Functor_T;

  /*********************************************
   *  ctor/dtor
   *
   *********************************************/

  /** @brief ctor
   */
  EXPORT ThreadPool();

  /** @brief ctor
   *
   * @param maxThreads the maximum number of threads in this pool. This is the maximum number
   *        of simultaneuous operations that can go on.
   * @param queueSize  the maximum number of work tasks in the queue. This is the maximum
   *        number of jobs that can queue up in the work list before invoke() blocks.
   *        If 0, then threads never block and total threads may
   *        exceed maxThreads. Nothing waits. Thread count will
   *        idle down to maxThreads when less work is required.
   */
  EXPORT explicit ThreadPool(size_t maxThreads, size_t queueSize);

  /** @brief dtor
   */
  EXPORT ~ThreadPool() throw();

  /*********************************************
   *  accessors/mutators
   *
   *********************************************/
  /** @brief set the work queue size
   *
   * @param queueSize the size of the work queue
   */
  EXPORT void setQueueSize(size_t queueSize);

  /** @brief fet the work queue size
   */
  inline size_t getQueueSize() const
  {
    return fQueueSize;
  }

  /** @brief set the maximum number of threads to be used to process
   * the work queue
   *
   * @param maxThreads the maximum number of threads
   */
  EXPORT void setMaxThreads(size_t maxThreads);

  /** @brief get the maximum number of threads
   */
  inline size_t getMaxThreads() const
  {
    return fMaxThreads;
  }

  /** @brief get the issued number of threads
   */
  inline size_t getIssuedThreads()
  {
    return fIssued;
  }

  /** @brief queue size accessor
   *
   */
  inline uint32_t getWaiting() const
  {
    return waitingFunctorsSize;
  }

  /*********************************************
   *  operations
   *
   *********************************************/

  /** @brief invoke a functor in a separate thread managed by the pool
   *
   * If all maxThreads are busy, threadfunc will be added to a work list and
   * will run when a thread comes free. If all threads are busy and there are
   * queueSize tasks already waiting, invoke() will block until a slot in the
   * queue comes free.
   */
  EXPORT uint64_t invoke(const Functor_T& threadfunc);

  /** @brief stop the threads
   */
  EXPORT void stop();

  /** @brief wait on all the threads to complete
   */
  EXPORT void wait();

  /** @brief Wait for a specific thread
   */
  EXPORT void join(uint64_t thrHandle);

  /** @brief Wait for a specific thread
   */
  EXPORT void join(std::vector<uint64_t>& thrHandle);

  /** @brief for use in debugging
   */
  EXPORT void dump();

  EXPORT std::string& name()
  {
    return fName;
  }

  EXPORT void setName(std::string name)
  {
    fName = name;
  }
  EXPORT void setName(const char* name)
  {
    fName = name;
  }

  EXPORT bool debug()
  {
    return fDebug;
  }

  EXPORT void setDebug(bool d)
  {
    fDebug = d;
  }

  friend class ThreadPoolMonitor;

 protected:
 private:
  // Used internally to keep a handle associated with each functor for join()
  struct PoolFunction_T
  {
    uint64_t hndl;
    Functor_T functor;
  };

  /** @brief initialize data memebers
   */
  void init();

  /** @brief add a functor to the list
   */
  uint64_t addFunctor(const Functor_T& func);

  /** @brief thread entry point
   */
  void beginThread() throw();

  void pruneThread();

  ThreadPool(const ThreadPool&);
  ThreadPool& operator=(const ThreadPool&);

  friend struct beginThreadFunc;

  struct beginThreadFunc
  {
    beginThreadFunc(ThreadPool& impl) : fImpl(impl)
    {
    }

    void operator()()
    {
      fImpl.beginThread();
    }

    ThreadPool& fImpl;
  };

  struct NoOp
  {
    void operator()() const
    {
    }
  };

  size_t fThreadCount;
  size_t fMaxThreads;
  size_t fQueueSize;

  typedef std::list<PoolFunction_T> Container_T;
  Container_T fWaitingFunctors;
  Container_T::iterator fNextFunctor;

  uint32_t fIssued;
  boost::mutex fMutex;
  boost::condition_variable fThreadAvailable;  // triggered when a thread is available
  boost::condition_variable fNeedThread;       // triggered when a thread is needed
  ThreadPoolGroup fThreads;

  bool fStop;
  long fGeneralErrors;
  long fFunctorErrors;
  uint32_t waitingFunctorsSize;
  uint64_t fNextHandle;

  std::string fName;  // Optional to add a name to the pool for debugging.
  bool fDebug;
  boost::mutex fInitMutex;
  boost::mutex fPruneMutex;
  boost::condition_variable fPruneThreadEnd;
  boost::thread* fPruneThread;
  std::stack<boost::thread::id> fPruneThreads;  // A list of stale thread IDs to be joined
};

// This class, if instantiated, will continuously log details about the indicated threadpool
// The log will end up in /var/log/mariadb/columnstore/trace/threadpool_<name>.log
class ThreadPoolMonitor
{
 public:
  ThreadPoolMonitor(ThreadPool* pool) : fPool(pool), fLog(NULL)
  {
  }

  ~ThreadPoolMonitor()
  {
    if (fLog)
    {
      delete fLog;
    }
  }

  void operator()();

 private:
  // defaults okay
  // ThreadPoolMonitor(const ThreadPoolMonitor& rhs);
  // ThreadPoolMonitor& operator=(const ThreadPoolMonitor& rhs);
  ThreadPool* fPool;
  std::ofstream* fLog;
};

}  // namespace threadpool

#undef EXPORT

#endif  // THREADPOOL_H

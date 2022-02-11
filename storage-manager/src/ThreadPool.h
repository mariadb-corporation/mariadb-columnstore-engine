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

#ifndef SM_THREADPOOL_H_
#define SM_THREADPOOL_H_

#include <deque>
#include <set>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include <boost/noncopyable.hpp>
#include "SMLogging.h"

namespace storagemanager
{
class ThreadPool : public boost::noncopyable
{
 public:
  ThreadPool();  // this ctor uses an 'unbounded' # of threads; relies on the context for a natural max,
  ThreadPool(uint num_threads, bool processQueueOnExit = false);  // this ctor lets caller specify a limit.
  virtual ~ThreadPool();

  // addJob doesn't block
  class Job
  {
   public:
    virtual ~Job(){};
    virtual void operator()() = 0;
  };

  void addJob(const boost::shared_ptr<Job>& j);
  void setMaxThreads(uint newMax);
  int currentQueueSize() const;
  void setName(const std::string&);  // set the name of this threadpool (for debugging)

 private:
  void processingLoop();                                    // the fcn run by each thread
  void _processingLoop(boost::unique_lock<boost::mutex>&);  // processingLoop() wraps _processingLoop() with
                                                            // thread management stuff.

  SMLogging* logger;
  uint maxThreads;
  bool die;
  bool processQueueOnExit;
  int threadsWaiting;
  std::string name;
  boost::thread_group threads;

  // the set s_threads below is intended to make pruning idle threads efficient.
  // there should be a cleaner way to do it.
  struct ID_Thread
  {
    ID_Thread(boost::thread::id&);
    ID_Thread(boost::thread*);
    boost::thread::id id;
    boost::thread* thrd;
  };

  struct id_compare
  {
    bool operator()(const ID_Thread&, const ID_Thread&) const;
  };
  std::set<ID_Thread, id_compare> s_threads;

  boost::condition jobAvailable;
  std::deque<boost::shared_ptr<Job> > jobs;
  mutable boost::mutex mutex;

  const boost::posix_time::time_duration idleThreadTimeout = boost::posix_time::seconds(60);
  boost::thread pruner;
  boost::condition somethingToPrune;
  std::vector<boost::thread::id> pruneable;  // when a thread is about to return it puts its id here
  void prune();
};

}  // namespace storagemanager

#endif

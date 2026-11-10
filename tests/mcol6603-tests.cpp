/* Copyright (C) 2026 MariaDB Corporation

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

/* MCOL-6603: PrimProc forks before it starts its services, so nothing built
   before the fork may start a thread. A ThreadPool used to start its prune
   thread in the constructor; it now starts it on the first invoke().

   The thread count comes from /proc/self/task. The lazy Stats monitor thread
   is not tested here: Stats has a DBRM member, which needs the BRM shm.
 */

#include <dirent.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>

#include "threadpool.h"

using threadpool::ThreadPool;

namespace
{
int threadCount()
{
  DIR* dir = opendir("/proc/self/task");
  if (dir == nullptr)
    return -1;
  int n = 0;
  while (dirent* e = readdir(dir))
  {
    if (e->d_name[0] != '.')
      ++n;
  }
  closedir(dir);
  return n;
}

// Runs fn on its own thread. If it hasn't returned within the limit, the test
// can't finish (the thread is stuck), so fail and exit rather than hang ctest.
template <typename Fn>
void returnsWithin(std::chrono::seconds limit, const char* what, Fn fn)
{
  std::promise<void> done;
  std::future<void> f = done.get_future();
  std::thread t(
      [&done, fn]() mutable
      {
        fn();
        done.set_value();
      });
  if (f.wait_for(limit) != std::future_status::ready)
  {
    fprintf(stderr, "%s did not return within %lld s\n", what, static_cast<long long>(limit.count()));
    fflush(stderr);
    _exit(1);
  }
  t.join();
}
}  // namespace

TEST(ThreadPoolLazyStartTest, ThreadPoolSpawnsNoThreadsUntilInvoke)
{
  const int before = threadCount();
  ASSERT_GT(before, 0);

  auto pool = std::make_unique<ThreadPool>(4, 4);
  EXPECT_EQ(threadCount(), before) << "the constructor started a thread";

  // The task holds its worker until released, so the count below is stable.
  std::mutex m;
  std::condition_variable cv;
  bool started = false, release = false;
  pool->invoke(
      [&]()
      {
        std::unique_lock<std::mutex> lk(m);
        started = true;
        cv.notify_all();
        cv.wait(lk, [&] { return release; });
      });
  {
    std::unique_lock<std::mutex> lk(m);
    ASSERT_TRUE(cv.wait_for(lk, std::chrono::seconds(10), [&] { return started; }));
  }
  // One worker plus the prune thread.
  EXPECT_GE(threadCount(), before + 2);

  {
    std::lock_guard<std::mutex> lk(m);
    release = true;
  }
  cv.notify_all();
  returnsWithin(std::chrono::seconds(10), "ThreadPool::stop() after invoke", [&] { pool->stop(); });
  returnsWithin(std::chrono::seconds(10), "~ThreadPool() after stop", [&] { pool.reset(); });
  EXPECT_EQ(threadCount(), before);
}

TEST(ThreadPoolLazyStartTest, StopWithoutTasksReturns)
{
  const int before = threadCount();
  auto pool = std::make_unique<ThreadPool>(4, 4);

  returnsWithin(std::chrono::seconds(10), "ThreadPool::stop() with no task", [&] { pool->stop(); });
  returnsWithin(std::chrono::seconds(10), "~ThreadPool() with no task", [&] { pool.reset(); });

  // A pool destroyed without stop() must not hang either.
  auto unstopped = std::make_unique<ThreadPool>(4, 4);
  returnsWithin(std::chrono::seconds(10), "~ThreadPool() without stop()", [&] { unstopped.reset(); });
  EXPECT_EQ(threadCount(), before);
}

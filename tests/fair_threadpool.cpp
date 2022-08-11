/* Copyright (C) 2022 MariaDB Corporation

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

#include <gtest/gtest.h>
#include <iostream>
#include <mutex>
#include <vector>

#include "utils/threadpool/fair_threadpool.h"

using namespace primitiveprocessor;
using namespace std;
using namespace threadpool;

using ResultsType = std::vector<int>;
static ResultsType results;
static std::mutex globMutex;

class FairThreadPoolTest : public testing::Test
{
 public:
  void SetUp() override
  {
    results.clear();
    threadPool = new FairThreadPool(1, 1, 0, 0);
  }

  FairThreadPool* threadPool;
};

class TestFunctor : public FairThreadPool::Functor
{
 public:
  TestFunctor(const size_t id, const size_t delay) : id_(id), delay_(delay)
  {
  }
  ~TestFunctor(){};
  int operator()() override
  {
    std::lock_guard<std::mutex> gl(globMutex);
    usleep(delay_);
    results.push_back(id_);
    return 0;
  }

 private:
  size_t id_;
  size_t delay_;
};

class TestRescheduleFunctor : public FairThreadPool::Functor
{
 public:
  TestRescheduleFunctor(const size_t id, const size_t delay) : id_(id), delay_(delay)
  {
  }
  ~TestRescheduleFunctor(){};
  int operator()() override
  {
    if (firstRun)
    {
      firstRun = false;
      return 1;  // re-schedule the Job
    }
    usleep(delay_);
    std::lock_guard<std::mutex> gl(globMutex);
    results.push_back(id_);
    return 0;
  }

 private:
  size_t id_;
  size_t delay_;
  bool firstRun = true;
};

testing::AssertionResult isThisOrThat(const ResultsType& arr, const size_t idxA, const int a,
                                      const size_t idxB, const int b)
{
  if (arr.empty() || arr.size() <= max(idxA, idxB))
    return testing::AssertionFailure() << "The supplied vector is either empty or not big enough.";
  if (arr[idxA] == a && arr[idxB] == b)
    return testing::AssertionSuccess();
  if (arr[idxA] == b && arr[idxB] == a)
    return testing::AssertionSuccess();
  return testing::AssertionFailure() << "The values at positions " << idxA << " " << idxB << " are not " << a
                                     << " and " << b << std::endl;
}

TEST_F(FairThreadPoolTest, FairThreadPoolAdd)
{
  SP_UM_IOSOCK sock(new messageqcpp::IOSocket);
  auto functor1 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(1, 150000));
  FairThreadPool::Job job1(1, 1, 1, functor1, sock, 1);
  auto functor2 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(2, 150000));
  FairThreadPool::Job job2(2, 1, 1, functor2, sock, 2);
  auto functor3 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(3, 5000));
  FairThreadPool::Job job3(3, 1, 1, functor3, sock, 1);
  auto functor4 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(4, 5000));
  FairThreadPool::Job job4(4, 1, 2, functor4, sock, 1);

  threadPool->addJob(job1);
  threadPool->addJob(job2);
  threadPool->addJob(job3);
  threadPool->addJob(job4);

  while (threadPool->queueSize())
  {
    usleep(350000);
  }

  EXPECT_EQ(threadPool->queueSize(), 0ULL);
  EXPECT_EQ(results.size(), 4ULL);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 4);
  EXPECT_TRUE(isThisOrThat(results, 2, 2, 3, 3));
}

TEST_F(FairThreadPoolTest, FairThreadPoolRemove)
{
  SP_UM_IOSOCK sock(new messageqcpp::IOSocket);
  auto functor1 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(1, 100000));
  FairThreadPool::Job job1(1, 1, 1, functor1, sock, 1, 0, 1);
  auto functor2 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(2, 50000));
  FairThreadPool::Job job2(2, 1, 1, functor2, sock, 1, 0, 2);
  auto functor3 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(3, 50000));
  FairThreadPool::Job job3(3, 1, 2, functor3, sock, 1, 0, 3);

  threadPool->addJob(job1);
  threadPool->addJob(job2);
  threadPool->addJob(job3);
  threadPool->removeJobs(job2.id_);

  while (threadPool->queueSize())
  {
    usleep(250000);
  }

  EXPECT_EQ(threadPool->queueSize(), 0ULL);
  EXPECT_EQ(results.size(), 2ULL);
  EXPECT_EQ(results[0], 1);
  EXPECT_EQ(results[1], 3);
}

TEST_F(FairThreadPoolTest, FairThreadPoolCleanUp)
{
  SP_UM_IOSOCK sock(new messageqcpp::IOSocket);
  auto functor1 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(1, 100000));
  FairThreadPool::Job job1(1, 1, 1, functor1, sock, 1, 0, 1);
  auto functor2 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(2, 50000));
  FairThreadPool::Job job2(2, 1, 1, functor2, sock, 1, 0, 2);
  auto functor3 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(3, 50000));
  FairThreadPool::Job job3(3, 1, 2, functor3, sock, 1, 0, 3);

  threadPool->addJob(job1);
  threadPool->removeJobs(job1.id_);
  threadPool->addJob(job2);
  threadPool->removeJobs(job2.id_);
  threadPool->addJob(job3);
  threadPool->removeJobs(job3.id_);
  threadPool->removeJobs(job1.id_);
  threadPool->removeJobs(job1.id_);

  while (threadPool->queueSize())
  {
    usleep(250000);
  }

  EXPECT_EQ(threadPool->queueSize(), 0ULL);
}

TEST_F(FairThreadPoolTest, FairThreadPoolReschedule)
{
  SP_UM_IOSOCK sock(new messageqcpp::IOSocket);
  auto functor1 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(1, 100000));
  FairThreadPool::Job job1(1, 1, 1, functor1, sock, 1, 0, 1);
  auto functor2 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(2, 50000));
  FairThreadPool::Job job2(2, 1, 2, functor2, sock, 1, 0, 2);
  auto functor3 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(3, 50000));
  FairThreadPool::Job job3(3, 1, 3, functor3, sock, 1, 0, 3);

  threadPool->addJob(job1);
  threadPool->addJob(job2);
  threadPool->addJob(job3);

  while (threadPool->queueSize())
  {
    usleep(250000);
  }

  EXPECT_EQ(threadPool->queueSize(), 0ULL);
  EXPECT_EQ(results.size(), 3ULL);
  EXPECT_EQ(results[0], 1);
  EXPECT_TRUE(isThisOrThat(results, 1, 2, 2, 3));
}
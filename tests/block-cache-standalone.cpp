/* Copyright (C) 2016-2021 MariaDB Corporation

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

#include <cstddef>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <barrier>
#include <stop_token>

#include "blocksize.h"
#include "filebuffermgr.h"
#include "testing_utils.h"

using namespace testing_utils;

void taskDispatcher(const size_t randNum, ThreadDB& tdb, dbbc::FileBufferMgr& fbm)
{
  if (randNum < tdb.constants.bulkInsert || (tdb.insertedBlocks.size() < tdb.constants.minTDBSize &&
                                             tdb.insertedBlocks.size() < tdb.constants.maxTDBSize))
  {
    bulkInsert(randNum, tdb, fbm);
  }
  else if (randNum < tdb.constants.find)
  {
    find(randNum, tdb, fbm);
  }
  else if (randNum < tdb.constants.flushOne)
  {
    flushOne(randNum, tdb, fbm);
  }
  else if (randNum < tdb.constants.flushMany)
  {
    flushMany(randNum, tdb, fbm);
  }
}

std::mt19937& getThreadLocalGen()
{
  thread_local std::mt19937 rng(std::random_device{}());  // Each thread gets its own generator
  return rng;
}

// Function to generate random numbers for the specified duration
void runRoutine(const size_t threadId, dbbc::FileBufferMgr& fbm, std::stop_token stop_token,
                std::barrier<>& sync_point, std::barrier<>& checking_sync_point,
                std::barrier<>& cleanup_sync_point)
{
  std::uniform_int_distribution<size_t> dist(1, 100);
  auto tdb = ThreadDB(threadId);

  sync_point.arrive_and_wait();

  while (!stop_token.stop_requested())
  {
    auto random_number = dist(getThreadLocalGen());
    taskDispatcher(random_number, tdb, fbm);
  }

  checking_sync_point.arrive_and_wait();

  std::cout << "Thread " << threadId << " checking." << std::endl;

  tdb.checkConsistency(fbm);

  cleanup_sync_point.arrive_and_wait();

  tdb.removeAllEntriesFromCache(fbm);

  std::cout << "Thread " << threadId << " stopping." << std::endl;
}

int main(int argc, char* argv[])
{
  if (argc < 2)
  {
    std::cerr << "Usage: " << " <minutes> <threads> <cache size in MB>" << std::endl;
    return 1;
  }

  auto minsNum = std::stoul(argv[1]);
  auto thredsNum = std::stoul(argv[2]);
  auto cacheSizeinMB = std::stoul(argv[3]);
  size_t numBlocks = cacheSizeinMB * 1024 * 1024 / BLOCK_SIZE;

  auto fbm = dbbc::FileBufferMgr(numBlocks);

  {
    std::vector<std::jthread> threads;
    std::stop_source ss;

    std::barrier sync_point(thredsNum);
    std::barrier checking_sync_point(thredsNum);
    std::barrier cleanup_sync_point(thredsNum);

    for (size_t i = 0; i < thredsNum; ++i)
    {
      threads.emplace_back(runRoutine, i + 1, std::ref(fbm), ss.get_token(), std::ref(sync_point),
                           std::ref(checking_sync_point), std::ref(cleanup_sync_point));
    }

    std::this_thread::sleep_for(std::chrono::minutes(minsNum));
    ss.request_stop();
  }

  if (fbm.cacheSize() > 0)
  {
    std::cerr << "Inconsistent state: cache is not empty!!" << std::endl;
  }

  return 0;
}
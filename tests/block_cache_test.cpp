/* Copyright (C) 2024 MariaDB Corporation

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
// #include <mutex>
#include <vector>

#include "filebuffermgr.h"
#include "testing_utils.h"

using namespace std;

using namespace dbbc;
using namespace testing_utils;

class FileBufferMgrTest : public testing::Test
{
 public:
  void SetUp() override
  {
    const size_t numBlocks = 16 * 1024 * 1024 / BLOCK_SIZE;  // 16MB
    fbm = unique_ptr<FileBufferMgr>(new FileBufferMgr(numBlocks));
    tdb = unique_ptr<ThreadDB>(new ThreadDB(1));
  }

  unique_ptr<FileBufferMgr> fbm;
  unique_ptr<ThreadDB> tdb;
};

// testing::AssertionResult isThisOrThat(const ResultsType& arr, const size_t idxA, const int a,
//                                       const size_t idxB, const int b)
// {
//   if (arr.empty() || arr.size() <= max(idxA, idxB))
//     return testing::AssertionFailure() << "The supplied vector is either empty or not big enough.";
//   if (arr[idxA] == a && arr[idxB] == b)
//     return testing::AssertionSuccess();
//   if (arr[idxA] == b && arr[idxB] == a)
//     return testing::AssertionSuccess();
//   return testing::AssertionFailure() << "The values at positions " << idxA << " " << idxB << " are not " <<
//   a
//                                      << " and " << b << std::endl;
// }

TEST_F(FileBufferMgrTest, FileBufferMgrBulkInsert)
{
  auto& fbm = *this->fbm;
  auto& tdb = *this->tdb;

  auto ops = tdb.generateAndStoreOps(42);
  fbm.bulkInsert(ops);
  EXPECT_EQ(fbm.cacheSize(), ops.size());

  for (auto& op : ops)
  {
    EXPECT_TRUE(fbm.exists(op.lbid, op.ver));
    delete[] op.data;
    op.data = nullptr;
  }
}

TEST_F(FileBufferMgrTest, FileBufferMgrBulkFlushOne)
{
  auto& fbm = *this->fbm;
  auto& tdb = *this->tdb;

  auto ops = tdb.generateAndStoreOps(42);
  fbm.bulkInsert(ops);
  EXPECT_EQ(fbm.cacheSize(), ops.size());

  for (auto& op : ops)
  {
    EXPECT_TRUE(fbm.exists(op.lbid, op.ver));
    delete[] op.data;
    op.data = nullptr;
  }

  for (auto& op : ops)
  {
    fbm.flushOne(op.lbid, op.ver);
    EXPECT_FALSE(fbm.exists(op.lbid, op.ver));
  }

  EXPECT_EQ(fbm.cacheSize(), 0ULL);
}
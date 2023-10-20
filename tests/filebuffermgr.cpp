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
#include <memory>
#include "brmtypes.h"
// #include <iostream>
// #include <mutex>
// #include <vector>

using namespace std;

#include "filebuffermgr.h"

using namespace dbbc;
namespace dbbc
{

class FileBufferMgrTest : public testing::Test
{
 public:
  static constexpr int64_t OpsNumber = 5;
  void SetUp() override
  {
    bufferMgr_ = unique_ptr<FileBufferMgr>(new FileBufferMgr(100));
    buff_ = make_unique<uint8_t[]>(BLOCK_SIZE * OpsNumber);
  }

  bool insertCorrectlyUpdatesSets(const CacheInsert_t& op)
  {
    size_t partition = bufferMgr_->partition(op.lbid);
    auto setsIt = bufferMgr_->fbSets[partition].find(HashObject_t{op.lbid, op.ver, 0});
    if (setsIt == bufferMgr_->fbSets[partition].end())
      return false;

    auto [lbid, ver, poolIdx] = *setsIt;
    if (lbid != op.lbid || ver != op.ver)
      return false;

    return true;
  }

  // bool insertCorrectlyUpdatesLists(const CacheInsert_t& op, const size_t num)
  // {
  //     const size_t partition = bufferMgr_->partition(op.lbid);
  //     if (bufferMgr_->fbLists[partition].size() < num + 1)
  //     {
  //     return false;
  //     }

  //     auto listIt = bufferMgr_->fbLists[partition].begin();
  //     advance(listIt, num);
  //     auto [lbid, ver, poolIdx] = *listIt;
  //     if (lbid != op.lbid || ver != op.ver)
  //     {
  //     return false;
  //     }

  //     return true;
  // }

  // TBD
  bool insertCorrectlyCopiesBlock(const CacheInsert_t& op)
  {
    return false;
  }

  bool insertCorrectlyUpdatesPool(const CacheInsert_t& op)
  {
    return false;
  }

  bool insertUpdatesCacheSizes(const CacheInsert_t& op)
  {
    return false;
  }

  unique_ptr<FileBufferMgr> bufferMgr_;
  unique_ptr<uint8_t[]> buff_;
};

// TEST_F(FairThreadPoolTesta, FairThreadPoolAdd)
// {
//   SP_UM_IOSOCK sock(new messageqcpp::IOSocket);
//   auto functor1 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(1, 150000));
//   FairThreadPool::Job job1(1, 1, 1, functor1, sock, 1);
//   auto functor2 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(2, 150000));
//   FairThreadPool::Job job2(2, 1, 1, functor2, sock, 2);
//   auto functor3 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(3, 5000));
//   FairThreadPool::Job job3(3, 1, 1, functor3, sock, 1);
//   auto functor4 = boost::shared_ptr<FairThreadPool::Functor>(new TestFunctor(4, 5000));
//   FairThreadPool::Job job4(4, 1, 2, functor4, sock, 1);

//   threadPool->addJob(job1);
//   threadPool->addJob(job2);
//   threadPool->addJob(job3);
//   threadPool->addJob(job4);

//   while (threadPool->queueSize())
//   {
//     usleep(350000);
//   }

//   EXPECT_EQ(threadPool->queueSize(), 0ULL);
//   EXPECT_EQ(results.size(), 4ULL);
//   EXPECT_EQ(results[0], 1);
//   EXPECT_EQ(results[1], 4);
//   EXPECT_TRUE(isThisOrThat(results, 2, 2, 3, 3));
// }

template <typename L, typename R>
bool CompareLbidAndVer(const L& x, const R& y)
{
  return x.Lbid() == y.Lbid() && x.Verid() == y.Verid();
}

template <typename L, typename R>
::testing::AssertionResult AreFieldsEqual(const char* a_expr, const char* b_expr, const L& x, const R& y)
{
  if (CompareLbidAndVer(x, y))
  {
    return ::testing::AssertionSuccess();
  }
  return ::testing::AssertionFailure() << a_expr << " and " << b_expr
                                       << " have different values ("
                                          "{lbid="
                                       << x.Lbid() << ",ver=" << x.Verid() << "}"
                                       << " vs {lbid=" << y.Lbid() << ",ver=" << y.Verid() << "})";
}

TEST_F(FileBufferMgrTest, bulkInsert)
{
  using ListOffsets = vector<size_t>;
  const uint8_t* data = buff_.get();
  CacheInsertVec ops = {
      CacheInsert_t{250144LL, 10, data},
      CacheInsert_t{0LL, 0, data + BLOCK_SIZE},
      CacheInsert_t{9223372036854775807LL, 42, data + 2 * BLOCK_SIZE},
      CacheInsert_t{100000LL, 5, data + 3 * BLOCK_SIZE},
      CacheInsert_t{25LL, 5, data + 4 * BLOCK_SIZE},
  };
  bufferMgr_->bulkInsert(ops);

  ListOffsets offsets(bufferMgr_->MagicNumber, 0);
  for (auto& op : ops)
  {
    const size_t partition = bufferMgr_->partition(op.lbid);
    // TBD This doesn't check the buffer contents yet.
    // check set
    EXPECT_TRUE(insertCorrectlyUpdatesSets(op));

    // check list
    EXPECT_TRUE(bufferMgr_->fbLists[partition].size() > offsets[partition]);
    auto listIt = bufferMgr_->fbLists[partition].rbegin();
    advance(listIt, offsets[partition]);
    ++offsets[partition];

    EXPECT_PRED_FORMAT2(AreFieldsEqual, *listIt, op);

    // check pool
    for (auto& buffer : bufferMgr_->fFBPool)
    {
      auto& listLoc = *buffer.listLoc();
      EXPECT_PRED_FORMAT2(AreFieldsEqual, buffer, listLoc);
    }
  }
}

TEST_F(FileBufferMgrTest, flushOIDs)
{
  EXPECT_EQ(true, true);
}

TEST_F(FileBufferMgrTest, insertAndFlush)
{
  EXPECT_EQ(true, true);
}

}  // namespace dbbc
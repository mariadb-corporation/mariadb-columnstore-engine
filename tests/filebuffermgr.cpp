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

using namespace std;

#include "brmtypes.h"
#include "extentmap.h"
#include "filebuffermgr.h"

using namespace dbbc;
namespace dbbc
{

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

template <typename L, typename R>
::testing::AssertionResult AreFieldsDiff(const char* a_expr, const char* b_expr, const L& x, const R& y)
{
  if (!CompareLbidAndVer(x, y))
  {
    return ::testing::AssertionSuccess();
  }
  return ::testing::AssertionFailure() << a_expr << " and " << b_expr
                                       << " have same values ("
                                          "{lbid="
                                       << x.Lbid() << ",ver=" << x.Verid() << "}"
                                       << " vs {lbid=" << y.Lbid() << ",ver=" << y.Verid() << "})";
}

class FileBufferMgrTest : public testing::Test
{
 public:
  using ListOffsets = vector<size_t>;
  static constexpr int64_t OpsNumber = 7;
  void SetUp() override
  {
    bufferMgr_ = unique_ptr<FileBufferMgr>(new FileBufferMgr(100));
    buff_ = make_unique<uint8_t[]>(BLOCK_SIZE * OpsNumber);
  }

  bool operationCorrectlyUpdatesSets(const CacheInsert_t& op, const bool setMustHaveOp)
  {
    size_t partition = bufferMgr_->partition(op.lbid);
    auto setsIt = bufferMgr_->fbSets[partition].find(HashObject_t{op.lbid, op.ver, 0});
    if (setsIt == bufferMgr_->fbSets[partition].end())
      return !setMustHaveOp;

    auto [lbid, ver, poolIdx] = *setsIt;
    if (lbid != op.lbid || ver != op.ver)
      return false;

    return setMustHaveOp;
  }

  // TBD
  void insertCorrectlyUpdatesList(const CacheInsert_t& op, ListOffsets& offsets)
  {
    const size_t partition = bufferMgr_->partition(op.lbid);
    EXPECT_TRUE(bufferMgr_->fbLists[partition].size() > offsets[partition]);
    auto listIt = bufferMgr_->fbLists[partition].rbegin();
    advance(listIt, offsets[partition]);
    ++offsets[partition];
    EXPECT_PRED_FORMAT2(AreFieldsEqual, *listIt, op);
  }

  // TBD
  bool insertCorrectlyCopiesBlock(const CacheInsert_t& op)
  {
    return false;
  }

  void insertCorrectlyUpdatesPool()
  {
    for (auto& buffer : bufferMgr_->fFBPool)
    {
      auto& listLoc = *buffer.listLoc();
      EXPECT_PRED_FORMAT2(AreFieldsEqual, buffer, listLoc);
    }
  }

  bool insertUpdatesCacheSizes(const CacheInsert_t& op)
  {
    return false;
  }

  void flushOIDsCorrectlyUpdatesStructs(CacheInsertVec& opsToFlush)
  {
    const bool setMustHaveOp = false;

    for (auto op : opsToFlush)
    {
      EXPECT_TRUE(operationCorrectlyUpdatesSets(op, setMustHaveOp));

      const size_t partition = bufferMgr_->partition(op.lbid);
      auto listIt = bufferMgr_->fbLists[partition].begin();
      for (; listIt != bufferMgr_->fbLists[partition].begin(); ++listIt)
      {
        EXPECT_PRED_FORMAT2(AreFieldsDiff, *listIt, op);
      }
    }
  }

  void runBulkInsertTest()
  {
  }

  unique_ptr<FileBufferMgr> bufferMgr_;
  unique_ptr<uint8_t[]> buff_;
};

TEST_F(FileBufferMgrTest, bulkInsert)
{
  runBulkInsertTest();

  const uint8_t* data = buff_.get();
  CacheInsertVec ops = {
      CacheInsert_t{250144LL, 10, data},
      CacheInsert_t{0LL, 0, data + BLOCK_SIZE},
      CacheInsert_t{9223372036854775807LL, 42, data + 2 * BLOCK_SIZE},
      CacheInsert_t{100000LL, 5, data + 3 * BLOCK_SIZE},
      CacheInsert_t{25LL, 5, data + 4 * BLOCK_SIZE},
  };
  bufferMgr_->bulkInsert(ops);

  ListOffsets offsets(bufferMgr_->PartitionsNumber, 0);
  for (auto& op : ops)
  {
    const bool setMustHaveOp = true;
    EXPECT_TRUE(operationCorrectlyUpdatesSets(op, setMustHaveOp));
    insertCorrectlyUpdatesList(op, offsets);
    insertCorrectlyUpdatesPool();
  }
}

TEST_F(FileBufferMgrTest, flushOIDs)
{
  const uint8_t* data = buff_.get();
  CacheInsertVec opsToFlush = {
      CacheInsert_t{201728LL, 5, data + 3 * BLOCK_SIZE},
      CacheInsert_t{201729LL, 5, data + 4 * BLOCK_SIZE},
      CacheInsert_t{201730LL, 5, data + 5 * BLOCK_SIZE},
  };
  CacheInsertVec ops = {
      CacheInsert_t{250144LL, 10, data},
      CacheInsert_t{0LL, 0, data + BLOCK_SIZE},
      CacheInsert_t{9223372036854775807LL, 42, data + 2 * BLOCK_SIZE},
      CacheInsert_t{25LL, 5, data + 6 * BLOCK_SIZE},
  };
  ops.insert(ops.end(), opsToFlush.begin(), opsToFlush.end());

  bufferMgr_->bulkInsert(ops);

  std::vector<BRM::EMEntry> extents{
      BRM::EMEntry(),
  };
  extents[0].range.start = 201728LL;
  extents[0].range.size = 8;

  auto notInPartitions = [](const BRM::EMEntry& range) { return false; };
  bufferMgr_->flushExtents(extents, notInPartitions);
  flushOIDsCorrectlyUpdatesStructs(opsToFlush);
}

}  // namespace dbbc
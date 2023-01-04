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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>
#include <numeric>
#include <type_traits>
#include <random>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "conststring.h"
#include "dbcon/joblist/heaporderby.h"
#include "jlf_common.h"
#include "pdqorderby.h"
#include "resourcemanager.h"
#include "rowgroup.h"
#include "m_ctype.h"

using namespace std;
using namespace testing;
using namespace sorting;

using RGFieldsType = std::vector<uint32_t>;

rowgroup::RowGroup setupRG(const std::vector<execplan::CalpontSystemCatalog::ColDataType>& cts,
                           const RGFieldsType& widths, const RGFieldsType& charsets)
{
  std::vector<execplan::CalpontSystemCatalog::ColDataType> types = cts;
  RGFieldsType offsets{2};
  for (auto w : widths)
  {
    offsets.push_back(offsets.back() + w);
  }
  RGFieldsType roids(widths.size());
  std::iota(roids.begin(), roids.end(), 3000);
  RGFieldsType tkeys(widths.size());
  std::fill(tkeys.begin(), tkeys.end(), 1);
  RGFieldsType cscale(widths.size());
  std::fill(cscale.begin(), cscale.end(), 0);
  RGFieldsType precision(widths.size());
  std::fill(precision.begin(), precision.end(), 20);
  return rowgroup::RowGroup(roids.size(),  // column count
                            offsets,       // oldOffset
                            roids,         // column oids
                            tkeys,         // keys
                            types,         // types
                            charsets,      // charset numbers
                            cscale,        // scale
                            precision,     // precision
                            20,            // sTableThreshold
                            false          // useStringTable
  );
}

// integral type, col type, NULL value, socket numbers
class KeyTypeTestInt64 : public testing::Test
{
 public:
  void SetUp() override
  {
    keysCols_ = {{0, false}};
    rg_ = setupRG({execplan::CalpontSystemCatalog::BIGINT}, {8}, {8});
    rgData_ = rowgroup::RGData(rg_);
    rg_.setData(&rgData_);
    rowgroup::Row r;
    rg_.initRow(&r);
    uint32_t rowSize = r.getSize();
    rg_.getRow(0, &r);
    for (int64_t i = 0; i < rowgroup::rgCommonSize; ++i)  // Worst case scenario for PQ
    {
      if (i == 42)
      {
        r.setIntField<8>(joblist::BIGINTNULL, 0);
        r.nextRow(rowSize);
      }
      else if (i == 8191)
      {
        r.setIntField<8>(-3891607892, 0);
        r.nextRow(rowSize);
      }
      else if (i == 8190)
      {
        r.setIntField<8>(-3004747259, 0);
        r.nextRow(rowSize);
      }
      else
      {
        if (i > 4000)
        {
          r.setIntField<8>(-i, 0);
          r.nextRow(rowSize);
        }
        else
        {
          r.setIntField<8>(i, 0);
          r.nextRow(rowSize);
        }
      }
    }
  }

  joblist::OrderByKeysType keysCols_;
  rowgroup::RowGroup rg_;
  rowgroup::RGData rgData_;
};

TEST_F(KeyTypeTestInt64, KeyTypeCtorInt64)
{
  size_t bufUnitSize = rg_.getOffsets().back() - 2 + 1;
  uint8_t* expected = new uint8_t[bufUnitSize];
  uint8_t* buf = new uint8_t[bufUnitSize * rowgroup::rgCommonSize + 1];
  for (size_t i = 0; i < rowgroup::rgCommonSize; ++i)
  {
    // std::cout << "value " << i << std::endl;
    auto key = KeyType(rg_, keysCols_, {0, i, 0}, &buf[i * bufUnitSize]);
    if (i != 42)
    {
      expected[0] = 1;
      int64_t* v = reinterpret_cast<int64_t*>(&expected[1]);
      if (i == 8190)
      {
        *v = -3004747259;
      }
      else if (i == 8191)
      {
        *v = -3891607892;
      }
      else if (i > 4000)
      {
        *v = -i;
      }
      else
      {
        *v = i;
      }
      // TBD add DSC encoding swaping the bits
      *v ^= 0x8000000000000000;
      *v = htonll(*v);
      ASSERT_FALSE(key.key() == nullptr);
      ASSERT_EQ(memcmp(expected, key.key(), 9), 0);
    }
    else
    {
      expected[0] = 0;
      ASSERT_EQ(memcmp(expected, key.key(), 1), 0);
    }
  }
}

TEST_F(KeyTypeTestInt64, KeyTypeLessInt64)
{
  size_t bufUnitSize = rg_.getOffsets().back() - 2 + 1;
  uint8_t* key1Buf = new uint8_t[bufUnitSize];
  uint8_t* key2Buf = new uint8_t[bufUnitSize];
  std::memset(key1Buf, 0, bufUnitSize);
  std::memset(key2Buf, 0, bufUnitSize);
  auto key1 = KeyType(rg_, keysCols_, {0, 42, 0}, key1Buf);
  auto key2 = KeyType(rg_, keysCols_, {0, 42, 0}, key2Buf);
  ASSERT_FALSE(key1.less(key2, rg_, keysCols_));
  ASSERT_FALSE(key2.less(key1, rg_, keysCols_));

  uint8_t* key3Buf = new uint8_t[bufUnitSize];
  uint8_t* key4Buf = new uint8_t[bufUnitSize];
  std::memset(key3Buf, 0, bufUnitSize);
  std::memset(key4Buf, 0, bufUnitSize);
  auto key3 = KeyType(rg_, keysCols_, {0, 4, 0}, key3Buf);
  auto key4 = KeyType(rg_, keysCols_, {0, 5, 0}, key4Buf);
  ASSERT_TRUE(key3.less(key4, rg_, keysCols_));
  ASSERT_FALSE(key4.less(key3, rg_, keysCols_));

  uint8_t* key5Buf = new uint8_t[bufUnitSize];
  uint8_t* key6Buf = new uint8_t[bufUnitSize];
  std::memset(key5Buf, 0, bufUnitSize);
  std::memset(key6Buf, 0, bufUnitSize);
  auto key5 = KeyType(rg_, keysCols_, {0, 4001, 0}, key5Buf);
  auto key6 = KeyType(rg_, keysCols_, {0, 4002, 0}, key6Buf);
  ASSERT_FALSE(key5.less(key6, rg_, keysCols_));
  ASSERT_TRUE(key6.less(key5, rg_, keysCols_));

  std::memset(key5Buf, 0, bufUnitSize);
  std::memset(key6Buf, 0, bufUnitSize);
  auto key5_ = KeyType(rg_, keysCols_, {0, 8190, 0}, key5Buf);
  auto key6_ = KeyType(rg_, keysCols_, {0, 8191, 0}, key6Buf);
  ASSERT_FALSE(key5.less(key6_, rg_, keysCols_));
  ASSERT_TRUE(key6.less(key5_, rg_, keysCols_));

  ASSERT_TRUE(key1.less(key3, rg_, keysCols_));
  ASSERT_TRUE(key1.less(key5, rg_, keysCols_));
  ASSERT_FALSE(key4.less(key1, rg_, keysCols_));
  ASSERT_FALSE(key5.less(key1, rg_, keysCols_));
}

// intXX_t key
// uintXX_t key
// no temporal b/c they are int equivalents
// float, double key
// decimal
// wide decimal
// ConstString: < 8 byte, 8 < len < StringThreshold, len < StringThreshold
// Mixed

class KeyTypeTestVarchar : public testing::Test
{
 public:
  void SetUp() override
  {
    keysCols_ = {{0, false}};
    uint32_t stringMaxSize = 16;
    rg_ = setupRG({execplan::CalpontSystemCatalog::VARCHAR}, {stringMaxSize}, {33});
    rgData_ = rowgroup::RGData(rg_);
    rg_.setData(&rgData_);
    rowgroup::Row r;
    rg_.initRow(&r);
    uint32_t rowSize = r.getSize();
    rg_.getRow(0, &r);
    static constexpr const char str1[]{'a', '\t'};  // 'a\t'
    utils::ConstString cs1(str1, 2);
    strings_.push_back(cs1);
    static constexpr const char str2[]{'a'};  // 'a'
    utils::ConstString cs2(str2, 1);
    strings_.push_back(cs2);
    static constexpr const char str3[]{'a', ' '};  // 'a '
    utils::ConstString cs3(str3, 2);
    strings_.push_back(cs3);
    static constexpr const char str4[]{'a', 'z', 'b', 'c', 'd', '\t', 'q', 'q', 'q'};  // 'az'
    utils::ConstString cs4(str4, sizeof(str4));
    strings_.push_back(cs4);
    r.setStringField(cs1, 0);
    r.nextRow(rowSize);
    r.setStringField(cs2, 0);
    r.nextRow(rowSize);
    r.setStringField(cs3, 0);
    r.nextRow(rowSize);
    r.setStringField(cs4, 0);
    rg_.setRowCount(4);
  }

  joblist::OrderByKeysType keysCols_;
  rowgroup::RowGroup rg_;
  rowgroup::RGData rgData_;
  std::vector<utils::ConstString> strings_;
};

TEST_F(KeyTypeTestVarchar, KeyTypeCtorVarcharPad)
{
  // utf8_general_ci = 33, 'a' == 'a '
  size_t bufUnitSize = rg_.getOffsets().back() - 2 + 1;  // NULL byte
  uint8_t* expected = new uint8_t[bufUnitSize];
  [[maybe_unused]] uint8_t* buf = new uint8_t[bufUnitSize * strings_.size()];
  rg_.setCharset(0, &my_charset_utf8mb3_general_ci);
  for ([[maybe_unused]] size_t i = 0; auto& s : strings_)
  {
    std::cout << "s " << s.str() << std::endl;
    uint8_t* pos = expected;
    uint nweights = s.length();
    auto key = KeyType(rg_, keysCols_, {0, i, 0}, &buf[i * bufUnitSize]);
    *pos++ = 1;
    datatypes::Charset cs(&my_charset_utf8mb3_general_ci);
    cs.strnxfrm(pos, bufUnitSize - 1, nweights, reinterpret_cast<const uchar*>(s.str()), s.length(),
                cs.getDefaultFlags());
    // TBD add DSC encoding swaping the bits
    ASSERT_FALSE(key.key() == nullptr);
    ASSERT_EQ(memcmp(expected, key.key(), bufUnitSize), 0);
    ++i;
  }
}

TEST_F(KeyTypeTestVarchar, KeyTypeCtorVarcharNoPad)
{
  // utf8_general_ci = 1057, 'a' != 'a '
  size_t bufUnitSize = rg_.getOffsets().back() - 2 + 1;  // NULL byte
  uint8_t* expected = new uint8_t[bufUnitSize];
  [[maybe_unused]] uint8_t* buf = new uint8_t[bufUnitSize * strings_.size()];
  rg_.setCharset(0, &my_charset_utf8mb3_general_nopad_ci);
  for ([[maybe_unused]] size_t i = 0; auto& s : strings_)
  {
    std::cout << "s " << s.str() << std::endl;
    auto key = KeyType(rg_, keysCols_, {0, i, 0}, &buf[i * bufUnitSize]);
    uint8_t* pos = expected;
    uint nweights = s.length();
    *pos++ = 1;
    memset(pos, 0, bufUnitSize - 1);
    datatypes::Charset cs(&my_charset_utf8mb3_general_nopad_ci);
    cs.strnxfrm(pos, bufUnitSize - 1, nweights, reinterpret_cast<const uchar*>(s.str()), s.length(), 0);
    // TBD add DSC encoding swaping the bits
    ASSERT_FALSE(key.key() == nullptr);
    ASSERT_EQ(memcmp(expected, key.key(), bufUnitSize), 0);
    ++i;
  }
}

TEST_F(KeyTypeTestVarchar, KeyTypeLessVarcharPad)
{
  rg_.setCharset(0, &my_charset_utf8mb3_general_ci);
  size_t bufUnitSize = rg_.getOffsets().back() - 2 + 1;
  // 1 and 2 are reserved for NULL comparison

  uint8_t* key3Buf = new uint8_t[bufUnitSize];
  uint8_t* key4Buf = new uint8_t[bufUnitSize];
  auto key3 = KeyType(rg_, keysCols_, {0, 0, 0}, key3Buf);
  auto key4 = KeyType(rg_, keysCols_, {0, 1, 0}, key4Buf);
  ASSERT_TRUE(key3.less(key4, rg_, keysCols_));
  ASSERT_FALSE(key4.less(key3, rg_, keysCols_));

  uint8_t* key5Buf = new uint8_t[bufUnitSize];
  auto key5 = KeyType(rg_, keysCols_, {0, 2, 0}, key5Buf);
  ASSERT_FALSE(key4.less(key5, rg_, keysCols_));
  ASSERT_FALSE(key5.less(key4, rg_, keysCols_));

  uint8_t* key6Buf = new uint8_t[bufUnitSize];
  auto key6 = KeyType(rg_, keysCols_, {0, 3, 0}, key6Buf);
  ASSERT_TRUE(key3.less(key6, rg_, keysCols_));
  ASSERT_TRUE(key4.less(key6, rg_, keysCols_));
  ASSERT_TRUE(key5.less(key6, rg_, keysCols_));
  ASSERT_FALSE(key6.less(key3, rg_, keysCols_));
  ASSERT_FALSE(key6.less(key4, rg_, keysCols_));
  ASSERT_FALSE(key6.less(key5, rg_, keysCols_));
}

TEST_F(KeyTypeTestVarchar, KeyTypeLessVarcharNoPad)
{
  rg_.setCharset(0, &my_charset_utf8mb3_general_nopad_ci);
  size_t bufUnitSize = rg_.getOffsets().back() - 2 + 1;
  // 1 and 2 are reserved for NULL comparison

  uint8_t* key3Buf = new uint8_t[bufUnitSize];
  uint8_t* key4Buf = new uint8_t[bufUnitSize];
  auto key3 = KeyType(rg_, keysCols_, {0, 0, 0}, key3Buf);
  auto key4 = KeyType(rg_, keysCols_, {0, 1, 0}, key4Buf);
  ASSERT_FALSE(key3.less(key4, rg_, keysCols_));
  ASSERT_TRUE(key4.less(key3, rg_, keysCols_));

  uint8_t* key5Buf = new uint8_t[bufUnitSize];
  auto key5 = KeyType(rg_, keysCols_, {0, 2, 0}, key5Buf);
  ASSERT_TRUE(key4.less(key5, rg_, keysCols_));
  ASSERT_FALSE(key5.less(key4, rg_, keysCols_));

  uint8_t* key6Buf = new uint8_t[bufUnitSize];
  auto key6 = KeyType(rg_, keysCols_, {0, 3, 0}, key6Buf);
  ASSERT_TRUE(key3.less(key6, rg_, keysCols_));
  ASSERT_TRUE(key4.less(key6, rg_, keysCols_));
  ASSERT_TRUE(key5.less(key6, rg_, keysCols_));
  ASSERT_FALSE(key6.less(key3, rg_, keysCols_));
  ASSERT_FALSE(key6.less(key4, rg_, keysCols_));
  ASSERT_FALSE(key6.less(key5, rg_, keysCols_));
}

class HeapOrderByTest : public testing::Test
{
 public:
  void SetUp() override
  {
    keysCols_ = {{0, false}};
    RGFieldsType offsets{2, 10};
    RGFieldsType roids{3000};
    RGFieldsType tkeys{1};
    RGFieldsType cscale{0};
    RGFieldsType precision{20};
    RGFieldsType charSetNumVec{8};
    std::vector<execplan::CalpontSystemCatalog::ColDataType> types{execplan::CalpontSystemCatalog::BIGINT};
    rg_ = rowgroup::RowGroup(roids.size(),   // column count
                             offsets,        // oldOffset
                             roids,          // column oids
                             tkeys,          // keys
                             types,          // types
                             charSetNumVec,  // charset numbers
                             cscale,         // scale
                             precision,      // precision
                             20,             // sTableThreshold
                             false           // useStringTable
    );
  }

  joblist::OrderByKeysType keysCols_;
  rowgroup::RowGroup rg_;
  std::vector<rowgroup::RGData> rgDatas_;
};

TEST_F(HeapOrderByTest, HeapOrderByCtor)
{
  size_t heapSize = 4;
  rowgroup::Row r;
  sorting::SortingThreads prevPhasThreads;
  joblist::OrderByKeysType keysAndDirections = {{0, false}};
  joblist::MemManager* mm = new joblist::MemManager;  //(&rm, sl, false, false);
  // no NULLs yet
  // отсортировать
  std::vector<std::vector<int64_t>> data{{3660195432, 3377000516, 3369182711, 2874400139, 2517866456,
                                          -517915385, -1950920917, -2522630870, -3733817126, -3891607892},
                                         {3396035276, 2989829828, 2938792700, 2907046279, 2508452465,
                                          873216056, 220139688, -1886091209, -2996493537, -3004747259},
                                         {3340465022, 2029570516, 1999115580, 630267809, 149731580,
                                          -816942484, -1665500714, -2753689374, -3087922913, -3250034565},
                                         {4144560611, 1759584866, 1642547418, 517102532, 344540230,
                                          -525087651, -976832186, -1379630329, -2362115756, -3558545988}};
  sorting::ValueRangesVector ranges(heapSize, {0, data.front().size()});
  for (size_t i = 0; i < heapSize; ++i)
  {
    rgDatas_.emplace_back(rowgroup::RGData(rg_));
    auto& rgData = rgDatas_.back();
    auto& rg = rg_;
    rg.setData(&rgData);
    rg.initRow(&r);
    rg.getRow(0, &r);
    uint32_t rowSize = r.getSize();
    sorting::PermutationVec perm(data[i].size());
    size_t it = 0;
    std::generate(perm.begin(), perm.end(), [i, it]() mutable { return PermutationType{0, it++, i}; });
    std::for_each(data[i].begin(), data[i].end(),
                  [&, rg, r, rowSize, perm](const int64_t x) mutable
                  {
                    r.setIntField<8>(x, 0);
                    r.nextRow(rowSize);
                  });
    rg.setRowCount(data[i].size());
    // std::cout << " i " << i << " " << rg.toString() << std::endl;
    prevPhasThreads.emplace_back(new PDQOrderBy());
    // добавить permutations вида {0,1,2...}
    prevPhasThreads.back()->getRGDatas().push_back(rgData);
    prevPhasThreads.back()->getMutPermutation().swap(perm);
  }
  HeapOrderBy h(rg_, keysAndDirections, 0, std::numeric_limits<size_t>::max(), mm, 1, prevPhasThreads,
                heapSize, ranges);
  // [[maybe_unused]] auto& keys = h.heap();
  // for (auto k : keys)
  // {
  //   std::cout << " perm {" << k.second.rgdataID << "," << k.second.rowID << "," << k.second.threadID <<
  //   "}"
  //             << std::endl;
  // }
  PermutationVec right = {PermutationType{0, 0, 0}, PermutationType{0, 9, 0}, PermutationType{0, 8, 0},
                          PermutationType{0, 9, 3}, PermutationType{0, 7, 0}, PermutationType{0, 9, 1},
                          PermutationType{0, 9, 2}, PermutationType{0, 8, 3}};
  for (auto r = right.begin(); auto k : h.heap())
  {
    ASSERT_EQ(k.second, *r++);
  }
}
TEST_F(HeapOrderByTest, HeapOrderByCtorOddSourceThreadsNumber)
{
}

TEST_F(HeapOrderByTest, HeapOrderBy_getTopPermuteFromHeap)
{
  size_t heapSize = 4;
  rowgroup::Row r;
  sorting::SortingThreads prevPhasThreads;
  joblist::OrderByKeysType keysAndDirections = {{0, false}};
  joblist::MemManager* mm = new joblist::MemManager;  //(&rm, sl, false, false);
  // no NULLs yet
  // отсортировать
  std::vector<std::vector<int64_t>> data{{3660195432, 3377000516, 3369182711, 2874400139, 2517866456,
                                          -517915385, -1950920917, -2522630870, -3733817126, -3891607892},
                                         {3396035276, 2989829828, 2938792700, 2907046279, 2508452465,
                                          873216056, 220139688, -1886091209, -2996493537, -3004747259},
                                         {3340465022, 2029570516, 1999115580, 630267809, 149731580,
                                          -816942484, -1665500714, -2753689374, -3087922913, -3250034565},
                                         {4144560611, 1759584866, 1642547418, 517102532, 344540230,
                                          -525087651, -976832186, -1379630329, -2362115756, -3558545988}};
  sorting::ValueRangesVector ranges(heapSize, {0, data.front().size()});
  for (size_t i = 0; i < heapSize; ++i)
  {
    rgDatas_.emplace_back(rowgroup::RGData(rg_));
    auto& rgData = rgDatas_.back();
    auto& rg = rg_;
    rg.setData(&rgData);
    rg.initRow(&r);
    rg.getRow(0, &r);
    uint32_t rowSize = r.getSize();
    sorting::PermutationVec perm(data[i].size());
    size_t it = 0;
    std::generate(perm.begin(), perm.end(), [i, it]() mutable { return PermutationType{0, it++, i}; });
    std::for_each(data[i].begin(), data[i].end(),
                  [&, rg, r, rowSize, perm](const int64_t x) mutable
                  {
                    r.setIntField<8>(x, 0);
                    r.nextRow(rowSize);
                  });
    rg.setRowCount(data[i].size());
    // std::cout << " i " << i << " " << rg.toString() << std::endl;
    prevPhasThreads.emplace_back(new PDQOrderBy());
    // добавить permutations вида {0,1,2...}
    prevPhasThreads.back()->getRGDatas().push_back(rgData);
    prevPhasThreads.back()->getMutPermutation().swap(perm);
  }
  HeapOrderBy h(rg_, keysAndDirections, 0, std::numeric_limits<size_t>::max(), mm, 1, prevPhasThreads,
                heapSize, ranges);
  [[maybe_unused]] auto& keys = h.heap();
  PermutationVec right = {PermutationType{0, 0, 0}, PermutationType{0, 9, 0}, PermutationType{0, 8, 0},
                          PermutationType{0, 9, 3}, PermutationType{0, 7, 0}, PermutationType{0, 9, 1},
                          PermutationType{0, 9, 2}, PermutationType{0, 8, 3}};
  for (auto r = right.begin(); auto k : keys)
  {
    ASSERT_EQ(k.second, *r++);
  }
  // for (auto k : h.heap())
  // {
  //   std::cout << " perm {" << k.second.rgdataID << "," << k.second.rowID << "," << k.second.threadID <<
  //   "}"
  //             << std::endl;
  // }
  PermutationType p;
  std::vector<int64_t> values;
  while ((p = h.getTopPermuteFromHeap(h.heapMut(), prevPhasThreads)) !=
         sorting::HeapOrderBy::ImpossiblePermute)
  {
    ASSERT_EQ(0UL, p.rgdataID);
    rg_.setData(&rgDatas_[p.threadID]);
    int64_t v = rg_.getColumnValue<execplan::CalpontSystemCatalog::BIGINT, int64_t, int64_t>(0, p.rowID);

    // std::cout << "p {" << p.rgdataID << "," << p.rowID << "," << p.threadID << "}"
    //           << " v " << v << std::endl;
    // for (auto k : h.heap())
    // {
    //   std::cout << " perm {" << k.second.rgdataID << "," << k.second.rowID << "," << k.second.threadID <<
    //   "}"
    //             << std::endl;
    // }

    if (!values.empty())
    {
      int64_t b = values.back();
      ASSERT_GE(v, b);
    }
    values.push_back(v);
  }
  ASSERT_TRUE(is_sorted(values.begin(), values.end()));
}
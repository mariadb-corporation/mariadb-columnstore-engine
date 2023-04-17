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
#include <bitset>
#include <cmath>
#include <cstdint>
#include <functional>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <iostream>
#include <iterator>
#include <limits>
#include <numeric>
#include <tuple>
#include <type_traits>
#include <random>

#include "conststring.h"
#include "dbcon/joblist/heaporderby.h"
#include "jlf_common.h"
#include "joblisttypes.h"
#include "m_ctype.h"
#include "mcs_basic_types.h"
#include "mcs_int128.h"
#include "pdqorderby.h"
#include "resourcemanager.h"
#include "rowgroup.h"

using namespace std;
using namespace testing;
using namespace sorting;

void printHexArray(const uint8_t* left, const uint8_t* right, const size_t len)
{
  std::cout << "len " << len << std::endl;
  std::cout << std::hex;
  for (size_t i = 0; i < len; ++i)
  {
    if (left[i] == right[i])
    {
      uint16_t r = right[i];
      std::cout << r << " ";
    }
    else
    {
      uint16_t l = left[i];
      uint16_t r = right[i];
      std::cout << std::endl
                << " idx " << std::dec << i << std::hex << " left " << l << " right " << r << std::endl;
    }
  }
  std::cout << std::dec << std::endl;
}
enum OutcomesT
{
  TR,
  FAL,
  SKIP
};

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

class KeyTypeWideDecimalTest : public testing::Test
{
 public:
  void SetUp() override
  {
    keysCols_ = {{0, true}};
    rg_ = setupRG({execplan::CalpontSystemCatalog::DECIMAL}, {16}, {8});
    rgData_ = rowgroup::RGData(rg_);
    rg_.setData(&rgData_);
    rowgroup::Row r;
    rg_.initRow(&r);
    uint32_t rowSize = r.getSize();
    rg_.getRow(0, &r);
    int128_t val = -1;
    val <<= 65;
    val -= 1;
    // 0th is NULL
    std::vector<int128_t> data = {
        100000000000000000000000000_xxl,

        100000000000000000000000000000000000001_xxl,

        1,

        42,

        val,

        -1,
    };
    r.setInt128Field(datatypes::TSInt128::NullValue, 0);
    r.nextRow(rowSize);
    for_each(data.begin(), data.end(),
             [&r, rowSize](const double arg)
             {
               r.setInt128Field(arg, 0);
               r.nextRow(rowSize);
             });
    rg_.setRowCount(data.size() + 1);
  }

  joblist::OrderByKeysType keysCols_;
  rowgroup::RowGroup rg_;
  rowgroup::RGData rgData_;
};

TEST_F(KeyTypeWideDecimalTest, KeyTypeCtorWDecimalAsc)
{
  size_t bufUnitSize = rg_.getColumnWidth(0) + 1;
  uint8_t* expected = new uint8_t[bufUnitSize];
  uint8_t* buf = new uint8_t[bufUnitSize];

  for (size_t i = 0; i < rg_.getRowCount(); ++i)
  {
    const bool isAsc = true;
    uint8_t* pos = &expected[0];
    memset(pos, 0, bufUnitSize);
    memset(buf, 0, bufUnitSize);
    auto key = KeyType(rg_, keysCols_, {0, i, 0}, buf);
    auto v =
        rg_.getColumnValue<execplan::CalpontSystemCatalog::DECIMAL, datatypes::TSInt128, datatypes::TSInt128>(
            0, i);
    if (v == datatypes::TSInt128::NullValue)
    {
      *pos++ = (!isAsc) ? 1 : 0;
    }
    else
    {
      *pos++ = (!isAsc) ? 0 : 1;
    }
    memcpy(pos, &v.getValue(), sizeof(int128_t));
    int64_t* wDecimaAsInt64 = (int64_t*)(pos);
    // Switch the sign bit and turn the parts into big-endian form.
    int64_t lower = htonll(wDecimaAsInt64[0]);
    wDecimaAsInt64[0] = htonll(wDecimaAsInt64[1] ^ 0x8000000000000000);
    wDecimaAsInt64[1] = lower;
    ASSERT_FALSE(key.key() == nullptr);
    ASSERT_EQ(memcmp(expected, key.key(), bufUnitSize), 0);
  }
}

TEST_F(KeyTypeWideDecimalTest, KeyTypeCtorWDecimalDsc)
{
  const bool isAsc = false;
  this->keysCols_ = {{0, isAsc}};
  size_t bufUnitSize = rg_.getColumnWidth(0) + 1;
  uint8_t* expected = new uint8_t[bufUnitSize];
  uint8_t* buf = new uint8_t[bufUnitSize];

  for (size_t i = 0; i < rg_.getRowCount(); ++i)
  {
    uint8_t* pos = &expected[0];
    memset(pos, 0, bufUnitSize);
    memset(buf, 0, bufUnitSize);
    auto key = KeyType(rg_, keysCols_, {0, i, 0}, buf);
    auto v =
        rg_.getColumnValue<execplan::CalpontSystemCatalog::DECIMAL, datatypes::TSInt128, datatypes::TSInt128>(
            0, i);
    if (v == datatypes::TSInt128::NullValue)
    {
      *pos++ = (!isAsc) ? 1 : 0;
    }
    else
    {
      *pos++ = (!isAsc) ? 0 : 1;
    }
    memcpy(pos, &v.getValue(), sizeof(int128_t));
    int64_t* wDecimaAsInt64 = (int64_t*)(pos);
    // Switch the sign bit and turn the parts into big-endian form.
    int64_t lower = ~htonll(wDecimaAsInt64[0]);
    wDecimaAsInt64[0] = ~htonll(wDecimaAsInt64[1] ^ 0x8000000000000000);
    wDecimaAsInt64[1] = lower;
    ASSERT_FALSE(key.key() == nullptr);
    ASSERT_EQ(memcmp(expected, key.key(), bufUnitSize), 0);
  }
}

TEST_F(KeyTypeWideDecimalTest, KeyTypeCtorWDecimalLessAsc)
{
  sorting::SortingThreads prevPhaseSorting;
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  size_t keysNumber = this->rg_.getRowCount();
  std::vector<uint8_t*> keyBufsVec(keysNumber);
  std::vector<KeyType> keys;
  size_t i = 0;
  for_each(keyBufsVec.begin(), keyBufsVec.end(),
           [this, &keys, &i, bufUnitSize](uint8_t* buf)
           {
             buf = new uint8_t[bufUnitSize];
             std::memset(buf, 0, bufUnitSize);
             keys.emplace_back(KeyType(this->rg_, this->keysCols_, {0, i++, 0}, buf));
           });
  // keys[0] == NULL
  std::vector<std::vector<OutcomesT>> expectedResultsMatrix = {
      {FAL, TR, TR, TR, TR, TR, TR},       {FAL, FAL, TR, FAL, FAL, FAL, FAL},
      {FAL, FAL, FAL, FAL, FAL, FAL, FAL}, {FAL, TR, TR, FAL, TR, FAL, FAL},
      {FAL, TR, TR, FAL, FAL, FAL, FAL},   {FAL, TR, TR, TR, TR, FAL, TR},
      {FAL, TR, TR, TR, TR, FAL, FAL}};
  [[maybe_unused]] size_t x = 0;
  [[maybe_unused]] size_t y = 0;
  bool testHadFailed = false;
  for_each(keys.begin(), keys.end(),
           [this, &prevPhaseSorting, &expectedResultsMatrix, &keys, &x, &y, &testHadFailed](auto& key1)
           {
             y = 0;
             for_each(
                 keys.begin(), keys.end(),
                 [this, &prevPhaseSorting, &expectedResultsMatrix, &key1, &x, &y, &testHadFailed](auto& key2)
                 {
                   OutcomesT result =
                       (key1.less(key2, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting))
                           ? TR
                           : FAL;
                   if (expectedResultsMatrix[x][y] != SKIP && expectedResultsMatrix[x][y] != result)
                   {
                     std::cout << "Results mismatch with: left row number = " << x
                               << " and right row number = " << y << std::endl;
                     testHadFailed = true;
                   }
                   ++y;
                 });
             ++x;
           });
  if (testHadFailed)
  {
    ASSERT_TRUE(false);
  }
  i = 0;
}

TEST_F(KeyTypeWideDecimalTest, KeyTypeCtorWDecimalLessDsc)
{
  const bool isAsc = false;
  this->keysCols_ = {{0, isAsc}};
  sorting::SortingThreads prevPhaseSorting;
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  size_t keysNumber = this->rg_.getRowCount();
  std::vector<uint8_t*> keyBufsVec(keysNumber);
  std::vector<KeyType> keys;
  size_t i = 0;
  for_each(keyBufsVec.begin(), keyBufsVec.end(),
           [this, &keys, &i, bufUnitSize](uint8_t* buf)
           {
             buf = new uint8_t[bufUnitSize];
             std::memset(buf, 0, bufUnitSize);
             keys.emplace_back(KeyType(this->rg_, this->keysCols_, {0, i++, 0}, buf));
           });
  std::vector<std::vector<OutcomesT>> expectedResultsMatrix = {{FAL, FAL, FAL, FAL, FAL, FAL, FAL},

                                                               {TR, FAL, FAL, TR, TR, TR, TR},

                                                               {TR, TR, FAL, TR, TR, TR, TR},

                                                               {TR, FAL, FAL, FAL, FAL, TR, TR},

                                                               {TR, FAL, FAL, TR, FAL, TR, TR},

                                                               {TR, FAL, FAL, FAL, FAL, FAL, FAL},

                                                               {TR, FAL, FAL, FAL, FAL, TR, FAL}};

  size_t x = 0;
  size_t y = 0;
  bool testHadFailed = false;
  for_each(keys.begin(), keys.end(),
           [this, &prevPhaseSorting, &expectedResultsMatrix, &keys, &x, &y, &testHadFailed](auto& key1)
           {
             y = 0;
             for_each(
                 keys.begin(), keys.end(),
                 [this, &prevPhaseSorting, &expectedResultsMatrix, &key1, &x, &y, &testHadFailed](auto& key2)
                 {
                   OutcomesT result =
                       (key1.less(key2, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting))
                           ? TR
                           : FAL;
                   if (expectedResultsMatrix[x][y] != SKIP && expectedResultsMatrix[x][y] != result)
                   {
                     std::cout << "Results mismatch with: left row number = " << x
                               << " and right row number = " << y << std::endl;
                     testHadFailed = true;
                   }
                   ++y;
                 });
             ++x;
           });

  if (testHadFailed)
  {
    ASSERT_TRUE(false);
  }
  i = 0;
}

template <typename T>
class KeyTypeFloatTest : public testing::Test
{
 public:
  using IntegralType = T;

  void SetUp() override
  {
    keysCols_ = {{0, true}};
    rg_ = setupRG({(std::is_same<float, T>::value) ? execplan::CalpontSystemCatalog::FLOAT
                                                   : execplan::CalpontSystemCatalog::DOUBLE},
                  {sizeof(T)}, {8});
    rgData_ = rowgroup::RGData(rg_);
    rg_.setData(&rgData_);
    rowgroup::Row r;
    rg_.initRow(&r);
    uint32_t rowSize = r.getSize();
    rg_.getRow(0, &r);
    // 0th is NULL
    std::vector<T> data = {0.0,
                           123123123.123123,
                           321321321.321321,
                           std::numeric_limits<T>::quiet_NaN(),
                           std::numeric_limits<T>::infinity(),
                           -123123123.123123,
                           -123123223.123123,
                           0.9999999999999999,
                           -0.9999999999999999};
    if constexpr (std::is_same<float, T>::value)
    {
      r.setIntField<4>(joblist::FLOATNULL, 0);
    }
    else
    {
      r.setIntField<8>(joblist::DOUBLENULL, 0);
    }
    r.nextRow(rowSize);
    if constexpr (std::is_same<T, float>::value)
    {
      for_each(data.begin(), data.end(),
               [&r, rowSize](const float arg)
               {
                 r.setFloatField(arg, 0);
                 r.nextRow(rowSize);
               });
    }
    else
    {
      for_each(data.begin(), data.end(),
               [&r, rowSize](const double arg)
               {
                 r.setDoubleField(arg, 0);
                 r.nextRow(rowSize);
               });
    }
    rg_.setRowCount(data.size() + 1);
  }
  static constexpr auto makeKeyF = [](const float a)
  {
    int floatAsInt = 0;
    memcpy(&floatAsInt, &a, sizeof(float));
    int s = (floatAsInt & 0x80000000) ^ 0x80000000;
    int32_t e = (floatAsInt >> 23) & 0xFF;
    int m = (s) ? floatAsInt & 0x7FFFFF : (~floatAsInt) & 0x7FFFFF;
    m = (e) ? m | 0x800000 : m << 1;
    m = (s) ? m : m & 0xFF7FFFFF;
    e = ((s) ? e : ~e & 0xFF) << 23;
    int key = m;
    key ^= s;
    key |= e;
    return htonl(key);
  };

  static constexpr auto makeKeyD = [](const double a)
  {
    int64_t doubleAsInt = 0;
    memcpy(&doubleAsInt, &a, sizeof(double));
    int64_t s = (doubleAsInt & 0x8000000000000000) ^ 0x8000000000000000;  // sign
    int64_t e = (doubleAsInt >> 52) & 0x07FF;                             // get 12 exponent bits
    int64_t m = (s) ? doubleAsInt & 0x07FFFFFFFFFFFF
                    : (~doubleAsInt) & 0x07FFFFFFFFFFFF;  // get 52 bits of a fraction
    m = (e) ? m | 0x8000000000000
            : m << 1;  // set an additional 52th bit if exp != 0 or move the value left 1 bit
    m = (s) ? m : m & 0xFFF7FFFFFFFFFFFF;  // if negative take the faction or take all but extra bit instead
    e = ((s) ? e : ~e & 0x07FF) << 51;
    int64_t key = m;
    key ^= s;
    key |= e;
    return htonll(key);
  };
  joblist::OrderByKeysType keysCols_;
  rowgroup::RowGroup rg_;
  rowgroup::RGData rgData_;
};

using KeyTypeFloatTypes = ::testing::Types<double, float>;
TYPED_TEST_SUITE(KeyTypeFloatTest, KeyTypeFloatTypes);

TYPED_TEST(KeyTypeFloatTest, KeyTypeCtorAsc)
{
  using T = typename KeyTypeFloatTest<TypeParam>::IntegralType;
  using IntT = typename datatypes::WidthToSIntegralType<sizeof(T)>::type;
  uint32_t columnWidth = this->rg_.getColumnWidth(0);
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  uint8_t* expected = new uint8_t[bufUnitSize];
  uint8_t* buf = new uint8_t[bufUnitSize * this->rg_.getRowCount() + 1];

  const uint8_t* nullValuePtr = nullptr;
  if constexpr (std::is_same<float, T>::value)
  {
    nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::FLOATNULL);
  }
  else
  {
    nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::DOUBLENULL);
  }

  for (size_t i = 0; i < this->rg_.getRowCount(); ++i)
  {
    memset(expected, 0, sizeof(T));
    uint8_t* pos = expected;
    rowgroup::RowGroup& rg = this->rg_;
    IntT k = 0;
    bool isNeitherNullOrSpecial = false;
    if constexpr (std::is_same<float, T>::value)
    {
      T v = rg.getColumnValue<execplan::CalpontSystemCatalog::FLOAT, T, T>(0, i);
      isNeitherNullOrSpecial = (memcmp(nullValuePtr, &v, columnWidth) != 0) && !isnan(v) && !isinf(v);
      k = this->makeKeyF(v);
    }
    else
    {
      T v = rg.getColumnValue<execplan::CalpontSystemCatalog::DOUBLE, T, T>(0, i);
      isNeitherNullOrSpecial = (memcmp(nullValuePtr, &v, columnWidth) != 0) && !isnan(v) && !isinf(v);
      k = this->makeKeyD(v);
    }

    *pos++ = static_cast<uint8_t>(isNeitherNullOrSpecial);
    memcpy(pos, &k, sizeof(T));

    auto key = KeyType(this->rg_, this->keysCols_, {0, i, 0}, &buf[i * bufUnitSize]);
    ASSERT_FALSE(key.key() == nullptr);
    ASSERT_EQ(memcmp(key.key(), expected, bufUnitSize), 0);
  }
}

TYPED_TEST(KeyTypeFloatTest, KeyTypeCtorDsc)
{
  using T = typename KeyTypeFloatTest<TypeParam>::IntegralType;
  using IntT = typename datatypes::WidthToSIntegralType<sizeof(T)>::type;
  const bool isAsc = false;
  this->keysCols_ = {{0, isAsc}};
  uint32_t columnWidth = this->rg_.getColumnWidth(0);
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  uint8_t* expected = new uint8_t[bufUnitSize];
  uint8_t* buf = new uint8_t[bufUnitSize * this->rg_.getRowCount() + 1];

  const uint8_t* nullValuePtr = nullptr;
  if constexpr (std::is_same<float, T>::value)
  {
    nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::FLOATNULL);
  }
  else
  {
    nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::DOUBLENULL);
  }

  for (size_t i = 0; i < this->rg_.getRowCount(); ++i)
  {
    memset(expected, 0, sizeof(T));
    uint8_t* pos = expected;
    rowgroup::RowGroup& rg = this->rg_;
    IntT k = 0;
    bool isNeitherNullOrSpecial = false;
    if constexpr (std::is_same<float, T>::value)
    {
      T v = rg.getColumnValue<execplan::CalpontSystemCatalog::FLOAT, T, T>(0, i);
      isNeitherNullOrSpecial = (memcmp(nullValuePtr, &v, columnWidth) != 0) && !isnan(v) && !isinf(v);
      k = ~this->makeKeyF(v);
    }
    else
    {
      T v = rg.getColumnValue<execplan::CalpontSystemCatalog::DOUBLE, T, T>(0, i);
      isNeitherNullOrSpecial = (memcmp(nullValuePtr, &v, columnWidth) != 0) && !isnan(v) && !isinf(v);
      k = ~this->makeKeyD(v);
    }

    *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNeitherNullOrSpecial)
                      : static_cast<uint8_t>(isNeitherNullOrSpecial);
    memcpy(pos, &k, sizeof(T));

    auto key = KeyType(this->rg_, this->keysCols_, {0, i, 0}, &buf[i * bufUnitSize]);
    ASSERT_FALSE(key.key() == nullptr);
    ASSERT_EQ(memcmp(expected, key.key(), bufUnitSize), 0);
  }
}

TYPED_TEST(KeyTypeFloatTest, KeyTypeLessAsc)
{
  sorting::SortingThreads prevPhaseSorting;
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  size_t keysNumber = this->rg_.getRowCount();
  std::vector<std::unique_ptr<uint8_t>> keyBufsVec;
  std::vector<KeyType> keys;
  for (size_t i = 0; i < keysNumber; ++i)
  {
    keyBufsVec.emplace_back(std::unique_ptr<uint8_t>(new uint8_t[bufUnitSize]));
    std::memset(keyBufsVec.back().get(), 0, bufUnitSize);
    keys.emplace_back(KeyType(this->rg_, this->keysCols_, {0, i, 0}, keyBufsVec.back().get()));
  }
  // keys[0] == NULL
  std::vector<std::vector<OutcomesT>> expectedResultsMatrix = {
      {FAL, TR, TR, TR, SKIP, TR, TR, TR, TR, TR},

      {FAL, FAL, TR, TR, FAL, FAL, FAL, FAL, TR, FAL},

      {FAL, FAL, FAL, TR, FAL, FAL, FAL, FAL, FAL, FAL},

      {FAL, FAL, FAL, FAL, FAL, FAL, FAL, FAL, FAL, FAL},

      {FAL, TR, TR, TR, FAL, SKIP, TR, TR, TR, TR},

      {FAL, TR, TR, TR, FAL, FAL, TR, TR, TR, TR},

      {FAL, TR, TR, TR, FAL, FAL, FAL, FAL, TR, TR},

      {FAL, TR, TR, TR, FAL, FAL, TR, FAL, TR, TR},

      {FAL, FAL, TR, TR, FAL, FAL, FAL, FAL, FAL, FAL},

      {FAL, TR, TR, TR, FAL, FAL, FAL, FAL, TR, FAL}};
  [[maybe_unused]] size_t x = 0;
  [[maybe_unused]] size_t y = 0;
  bool testHadFailed = false;
  for_each(keys.begin(), keys.end(),
           [this, &prevPhaseSorting, &expectedResultsMatrix, &keys, &x, &y, &testHadFailed](auto& key1)
           {
             y = 0;
             for_each(
                 keys.begin(), keys.end(),
                 [this, &prevPhaseSorting, &expectedResultsMatrix, &key1, &x, &y, &testHadFailed](auto& key2)
                 {
                   OutcomesT result =
                       (key1.less(key2, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting))
                           ? TR
                           : FAL;
                   if (expectedResultsMatrix[x][y] != SKIP && expectedResultsMatrix[x][y] != result)
                   {
                     std::cout << "Results mismatch with: left row number = " << x
                               << " and right row number = " << y << std::endl;
                     testHadFailed = true;
                   }
                   ++y;
                 });
             ++x;
           });
  if (testHadFailed)
  {
    ASSERT_TRUE(false);
  }
}

// TYPED_TEST(KeyTypeFloatTest1, KeyTypeLessAsc1)
// {
//   sorting::SortingThreads prevPhaseSorting;
//   size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
//   size_t keysNumber = this->rg_.getRowCount();
//   std::vector<std::unique_ptr<uint8_t>> keyBufsVec;  // (keysNumber);
//   std::vector<KeyType> keys;
//   for (size_t i = 0; i < keysNumber; ++i)
//   //  [this, &keys, &i, &keyBufsVec, bufUnitSize](uint8_t* buf)
//   {
//     keyBufsVec.emplace_back(std::unique_ptr<uint8_t>(new uint8_t[bufUnitSize]));
//     // auto buf = std::unique_ptr<uint8_t>(new uint8_t[bufUnitSize]);
//     std::memset(keyBufsVec.back().get(), 0, bufUnitSize);
//     keys.emplace_back(KeyType(this->rg_, this->keysCols_, {0, i++, 0}, keyBufsVec.back().get()));
//   }
//   //  });
//   // keys[0] == NULL
//   std::vector<std::vector<OutcomesT>> expectedResultsMatrix = {
//       {FAL, TR, TR, TR, SKIP, TR, TR, TR, TR, TR},

//       {FAL, FAL, TR, TR, FAL, FAL, FAL, FAL, TR, FAL},

//       {FAL, FAL, FAL, TR, FAL, FAL, FAL, FAL, FAL, FAL},

//       {FAL, FAL, FAL, FAL, FAL, FAL, FAL, FAL, FAL, FAL},

//       {FAL, TR, TR, TR, FAL, SKIP, TR, TR, TR, TR},

//       {FAL, TR, TR, TR, FAL, FAL, TR, TR, TR, TR},

//       {FAL, TR, TR, TR, FAL, FAL, FAL, FAL, TR, TR},

//       {FAL, TR, TR, TR, FAL, FAL, TR, FAL, TR, TR},

//       {FAL, FAL, TR, TR, FAL, FAL, FAL, FAL, FAL, FAL},

//       {FAL, TR, TR, TR, FAL, FAL, FAL, FAL, TR, FAL}};
//   [[maybe_unused]] size_t x = 0;
//   [[maybe_unused]] size_t y = 0;
//   bool testHadFailed = false;
//   for_each(keys.begin(), keys.end(),
//            [this, &prevPhaseSorting, &expectedResultsMatrix, &keys, &x, &y, &testHadFailed](auto& key1)
//            {
//              y = 0;
//              for_each(
//                  keys.begin(), keys.end(),
//                  [this, &prevPhaseSorting, &expectedResultsMatrix, &key1, &x, &y, &testHadFailed](auto&
//                  key2)
//                  {
//                    OutcomesT result =
//                        (key1.less(key2, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0},
//                        prevPhaseSorting))
//                            ? TR
//                            : FAL;
//                    if (expectedResultsMatrix[x][y] != SKIP && expectedResultsMatrix[x][y] != result)
//                    {
//                      std::cout << "Results mismatch with: left row number = " << x
//                                << " and right row number = " << y << std::endl;
//                      testHadFailed = true;
//                    }
//                    ++y;
//                  });
//              ++x;
//            });
//   if (testHadFailed)
//   {
//     ASSERT_TRUE(false);
//   }
// }

TYPED_TEST(KeyTypeFloatTest, KeyTypeLessDsc)
{
  const bool isAsc = false;
  this->keysCols_ = {{0, isAsc}};
  sorting::SortingThreads prevPhaseSorting;
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  size_t keysNumber = this->rg_.getRowCount();
  std::vector<uint8_t*> keyBufsVec(keysNumber);
  std::vector<KeyType> keys;
  size_t i = 0;
  for_each(keyBufsVec.begin(), keyBufsVec.end(),
           [this, &keys, &i, bufUnitSize](uint8_t* buf)
           {
             buf = new uint8_t[bufUnitSize];
             std::memset(buf, 0, bufUnitSize);
             keys.emplace_back(KeyType(this->rg_, this->keysCols_, {0, i++, 0}, buf));
           });
  std::vector<std::vector<OutcomesT>> expectedResultsMatrix = {
      {FAL, FAL, FAL, FAL, FAL, FAL, FAL, FAL, FAL, FAL},

      {TR, FAL, FAL, FAL, TR, TR, TR, TR, FAL, TR},

      {TR, TR, FAL, FAL, TR, TR, TR, TR, TR, TR},

      {TR, TR, TR, FAL, TR, TR, TR, TR, TR, TR},

      {SKIP, FAL, FAL, FAL, FAL, FAL, FAL, FAL, FAL, FAL},

      {TR, FAL, FAL, FAL, SKIP, FAL, FAL, FAL, FAL, FAL},

      {TR, FAL, FAL, FAL, TR, TR, FAL, TR, FAL, FAL},

      {TR, FAL, FAL, FAL, TR, TR, FAL, FAL, FAL, FAL},

      {TR, TR, FAL, FAL, TR, TR, TR, TR, FAL, TR},

      {TR, FAL, FAL, FAL, TR, TR, TR, TR, FAL, FAL}};
  size_t x = 0;
  size_t y = 0;
  bool testHadFailed = false;
  for_each(keys.begin(), keys.end(),
           [this, &prevPhaseSorting, &expectedResultsMatrix, &keys, &x, &y, &testHadFailed](auto& key1)
           {
             y = 0;
             for_each(
                 keys.begin(), keys.end(),
                 [this, &prevPhaseSorting, &expectedResultsMatrix, &key1, &x, &y, &testHadFailed](auto& key2)
                 {
                   OutcomesT result =
                       (key1.less(key2, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting))
                           ? TR
                           : FAL;
                   if (expectedResultsMatrix[x][y] != SKIP && expectedResultsMatrix[x][y] != result)
                   {
                     std::cout << "Results mismatch with: left row number = " << x
                               << " and right row number = " << y << std::endl;
                     testHadFailed = true;
                   }
                   ++y;
                 });
             ++x;
           });

  if (testHadFailed)
  {
    ASSERT_TRUE(false);
  }
}

const constexpr execplan::CalpontSystemCatalog::ColDataType SignedCTs[] = {
    execplan::CalpontSystemCatalog::TINYINT,  execplan::CalpontSystemCatalog::TINYINT,
    execplan::CalpontSystemCatalog::SMALLINT, execplan::CalpontSystemCatalog::SMALLINT,
    execplan::CalpontSystemCatalog::INT,      execplan::CalpontSystemCatalog::INT,
    execplan::CalpontSystemCatalog::INT,      execplan::CalpontSystemCatalog::INT,
    execplan::CalpontSystemCatalog::BIGINT};

const constexpr execplan::CalpontSystemCatalog::ColDataType USignedCTs[] = {
    execplan::CalpontSystemCatalog::UTINYINT,  execplan::CalpontSystemCatalog::UTINYINT,
    execplan::CalpontSystemCatalog::USMALLINT, execplan::CalpontSystemCatalog::USMALLINT,
    execplan::CalpontSystemCatalog::UINT,      execplan::CalpontSystemCatalog::UINT,
    execplan::CalpontSystemCatalog::UINT,      execplan::CalpontSystemCatalog::UINT,
    execplan::CalpontSystemCatalog::UBIGINT};

template <typename T>
class KeyTypeTestIntT : public testing::Test
{
 public:
  using IntegralType = T;

  joblist::OrderByKeysType keysCols_;
  rowgroup::RowGroup rg_;
  rowgroup::RGData rgData_;

  void SetUp() override
  {
    keysCols_ = {{0, true}};
    rg_ = setupRG({SignedCTs[sizeof(T)]}, {sizeof(T)}, {8});
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
        r.setIntField<sizeof(T)>(sorting::getNullValue<T, T>(SignedCTs[sizeof(T)]), 0);
        r.nextRow(rowSize);
      }
      else if (sizeof(T) == 8 && i == 8191)
      {
        r.setIntField<sizeof(T)>(-3891607892, 0);
        r.nextRow(rowSize);
      }
      else if (sizeof(T) == 8 && i == 8190)
      {
        r.setIntField<sizeof(T)>(-3004747259, 0);
        r.nextRow(rowSize);
      }
      else
      {
        if (i > 4000)
        {
          r.setIntField<sizeof(T)>(-i, 0);
          r.nextRow(rowSize);
        }
        else
        {
          r.setIntField<sizeof(T)>(i, 0);
          r.nextRow(rowSize);
        }
      }
    }
  }
};

using KeyTypeIntTypes = ::testing::Types<int64_t, int32_t, int16_t, int8_t>;
TYPED_TEST_SUITE(KeyTypeTestIntT, KeyTypeIntTypes);

TYPED_TEST(KeyTypeTestIntT, KeyTypeCtorAsc)
{
  using T = typename KeyTypeTestIntT<TypeParam>::IntegralType;
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  uint8_t* expected = new uint8_t[bufUnitSize];
  uint8_t* buf = new uint8_t[bufUnitSize * rowgroup::rgCommonSize + 1];
  size_t upperBound = (sizeof(T) == 1) ? 127 : rowgroup::rgCommonSize;
  for (size_t i = 0; i < upperBound; ++i)
  {
    auto key = KeyType(this->rg_, this->keysCols_, {0, i, 0}, &buf[i * bufUnitSize]);
    if (i != 42)
    {
      expected[0] = 1;
      T* v = reinterpret_cast<T*>(&expected[1]);
      if (sizeof(T) == 8 && i == 8190)
      {
        *v = -3004747259;
      }
      else if (sizeof(T) == 8 && i == 8191)
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
      *v ^= 1ULL << (sizeof(T) * 8 - 1);
      switch (sizeof(T))
      {
        case (8): *v = htonll(*v); break;
        case (4): *v = htonl(*v); break;
        case (2): *v = htons(*v); break;
        case (1): *v = *v; break;
        default: break;
      }
      // *v = htonll(*v);
      ASSERT_FALSE(key.key() == nullptr);
      ASSERT_EQ(memcmp(expected, key.key(), bufUnitSize), 0);
    }
    else
    {
      expected[0] = 0;
      ASSERT_EQ(memcmp(expected, key.key(), 1), 0);
    }
  }
}

TYPED_TEST(KeyTypeTestIntT, KeyTypeCtorDsc)
{
  const bool isAsc = false;
  this->keysCols_ = {{0, isAsc}};
  using T = typename KeyTypeTestIntT<TypeParam>::IntegralType;
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  uint8_t* expected = new uint8_t[bufUnitSize];
  uint8_t* buf = new uint8_t[bufUnitSize * rowgroup::rgCommonSize + 1];
  size_t upperBound = (sizeof(T) == 1) ? 127 : rowgroup::rgCommonSize;
  for (size_t i = 0; i < upperBound; ++i)
  {
    auto key = KeyType(this->rg_, this->keysCols_, {0, i, 0}, &buf[i * bufUnitSize]);
    if (i != 42)
    {
      expected[0] = (!isAsc) ? 0 : 1;
      ;
      T* v = reinterpret_cast<T*>(&expected[1]);
      if (sizeof(T) == 8 && i == 8190)
      {
        *v = -3004747259;
      }
      else if (sizeof(T) == 8 && i == 8191)
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
      *v ^= 1ULL << (sizeof(T) * 8 - 1);
      switch (sizeof(T))
      {
        case (8): *v = ~htonll(*v); break;
        case (4): *v = ~htonl(*v); break;
        case (2): *v = ~htons(*v); break;
        case (1): *v = ~*v; break;
        default: break;
      }
      // *v = ~htonll(*v);
      ASSERT_FALSE(key.key() == nullptr);
      ASSERT_EQ(memcmp(expected, key.key(), bufUnitSize), 0);
    }
    else
    {
      expected[0] = (!isAsc) ? 1 : 0;
      ASSERT_EQ(memcmp(expected, key.key(), 1), 0);
    }
  }
}

TYPED_TEST(KeyTypeTestIntT, KeyTypeLessAsc)
{
  sorting::SortingThreads prevPhaseSorting;
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  uint8_t* key1Buf = new uint8_t[bufUnitSize];
  uint8_t* key2Buf = new uint8_t[bufUnitSize];
  std::memset(key1Buf, 0, bufUnitSize);
  std::memset(key2Buf, 0, bufUnitSize);
  auto key1 = KeyType(this->rg_, this->keysCols_, {0, 42, 0}, key1Buf);
  auto key2 = KeyType(this->rg_, this->keysCols_, {0, 42, 0}, key2Buf);
  ASSERT_FALSE(key1.less(key2, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key2.less(key1, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));

  uint8_t* key3Buf = new uint8_t[bufUnitSize];
  uint8_t* key4Buf = new uint8_t[bufUnitSize];
  std::memset(key3Buf, 0, bufUnitSize);
  std::memset(key4Buf, 0, bufUnitSize);
  auto key3 = KeyType(this->rg_, this->keysCols_, {0, 4, 0}, key3Buf);
  auto key4 = KeyType(this->rg_, this->keysCols_, {0, 5, 0}, key4Buf);
  ASSERT_TRUE(key3.less(key4, this->rg_, this->keysCols_, {0, 4, 0}, {0, 5, 0}, prevPhaseSorting));
  ASSERT_FALSE(key4.less(key3, this->rg_, this->keysCols_, {0, 5, 0}, {0, 4, 0}, prevPhaseSorting));

  uint8_t* key5Buf = new uint8_t[bufUnitSize];
  uint8_t* key6Buf = new uint8_t[bufUnitSize];
  std::memset(key5Buf, 0, bufUnitSize);
  std::memset(key6Buf, 0, bufUnitSize);
  auto key5 = KeyType(this->rg_, this->keysCols_, {0, 4001, 0}, key5Buf);
  auto key6 = KeyType(this->rg_, this->keysCols_, {0, 4002, 0}, key6Buf);
  ASSERT_FALSE(key5.less(key6, this->rg_, this->keysCols_, {0, 4001, 0}, {0, 4002, 0}, prevPhaseSorting));
  ASSERT_TRUE(key6.less(key5, this->rg_, this->keysCols_, {0, 4002, 0}, {0, 4001, 0}, prevPhaseSorting));

  std::memset(key5Buf, 0, bufUnitSize);
  std::memset(key6Buf, 0, bufUnitSize);
  auto key5_ = KeyType(this->rg_, this->keysCols_, {0, 8190, 0}, key5Buf);
  auto key6_ = KeyType(this->rg_, this->keysCols_, {0, 8191, 0}, key6Buf);
  ASSERT_FALSE(key5.less(key6_, this->rg_, this->keysCols_, {0, 8190, 0}, {0, 8191, 0}, prevPhaseSorting));
  ASSERT_TRUE(key6.less(key5_, this->rg_, this->keysCols_, {0, 8191, 0}, {0, 8190, 0}, prevPhaseSorting));

  ASSERT_TRUE(key1.less(key3, this->rg_, this->keysCols_, {0, 42, 0}, {0, 4, 0}, prevPhaseSorting));
  ASSERT_TRUE(key1.less(key5, this->rg_, this->keysCols_, {0, 42, 0}, {0, 4001, 0}, prevPhaseSorting));
  ASSERT_FALSE(key4.less(key1, this->rg_, this->keysCols_, {0, 4001, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key5.less(key1, this->rg_, this->keysCols_, {0, 4002, 0}, {0, 42, 0}, prevPhaseSorting));
}

template <typename T>
class KeyTypeTestUIntT : public testing::Test
{
 public:
  using IntegralType = T;

  joblist::OrderByKeysType keysCols_;
  rowgroup::RowGroup rg_;
  rowgroup::RGData rgData_;

  void SetUp() override
  {
    keysCols_ = {{0, true}};
    rg_ = setupRG({USignedCTs[sizeof(T)]}, {sizeof(T)}, {8});
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
        r.setIntField<sizeof(T)>(sorting::getNullValue<T, T>(USignedCTs[sizeof(T)]), 0);
        r.nextRow(rowSize);
      }
      else if (sizeof(T) == 8 && i == 8191)
      {
        r.setIntField<sizeof(T)>(3004747259, 0);
        r.nextRow(rowSize);
      }
      else if (sizeof(T) == 8 && i == 8190)
      {
        r.setIntField<sizeof(T)>(3891607892, 0);
        r.nextRow(rowSize);
      }
      else
      {
        r.setIntField<sizeof(T)>(i, 0);
        r.nextRow(rowSize);
      }
    }
  }
};

using KeyTypeUIntTypes = ::testing::Types<uint64_t, uint32_t, uint16_t, uint8_t>;
TYPED_TEST_SUITE(KeyTypeTestUIntT, KeyTypeUIntTypes);

TYPED_TEST(KeyTypeTestUIntT, KeyTypeCtorAsc)
{
  using T = typename KeyTypeTestIntT<TypeParam>::IntegralType;
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  uint8_t* expected = new uint8_t[bufUnitSize];
  uint8_t* buf = new uint8_t[bufUnitSize * rowgroup::rgCommonSize + 1];
  size_t upperBound = (sizeof(T) == 1) ? 127 : rowgroup::rgCommonSize;
  for (size_t i = 0; i < upperBound; ++i)
  {
    auto key = KeyType(this->rg_, this->keysCols_, {0, i, 0}, &buf[i * bufUnitSize]);
    if (i != 42)
    {
      expected[0] = 1;
      T* v = reinterpret_cast<T*>(&expected[1]);
      if (sizeof(T) == 8 && i == 8190)
      {
        *v = 3891607892;
      }
      else if (sizeof(T) == 8 && i == 8191)
      {
        *v = 3004747259;
      }
      else
      {
        *v = i;
      }
      *v ^= 1ULL << (sizeof(T) * 8 - 1);
      switch (sizeof(T))
      {
        case (8): *v = htonll(*v); break;
        case (4): *v = htonl(*v); break;
        case (2): *v = htons(*v); break;
        case (1): *v = *v; break;
        default: break;
      }
      // *v = htonll(*v);
      ASSERT_FALSE(key.key() == nullptr);
      ASSERT_EQ(memcmp(expected, key.key(), bufUnitSize), 0);
    }
    else
    {
      expected[0] = 0;
      ASSERT_EQ(memcmp(expected, key.key(), 1), 0);
    }
  }
}

TYPED_TEST(KeyTypeTestUIntT, KeyTypeCtorDsc)
{
  const bool isAsc = false;
  this->keysCols_ = {{0, isAsc}};
  using T = typename KeyTypeTestIntT<TypeParam>::IntegralType;
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  uint8_t* expected = new uint8_t[bufUnitSize];
  uint8_t* buf = new uint8_t[bufUnitSize * rowgroup::rgCommonSize + 1];
  size_t upperBound = (sizeof(T) == 1) ? 127 : rowgroup::rgCommonSize;
  for (size_t i = 0; i < upperBound; ++i)
  {
    auto key = KeyType(this->rg_, this->keysCols_, {0, i, 0}, &buf[i * bufUnitSize]);
    if (i != 42)
    {
      expected[0] = (!isAsc) ? 0 : 1;
      T* v = reinterpret_cast<T*>(&expected[1]);
      if (sizeof(T) == 8 && i == 8190)
      {
        *v = 3891607892;
      }
      else if (sizeof(T) == 8 && i == 8191)
      {
        *v = 3004747259;
      }
      else
      {
        *v = i;
      }
      *v ^= 1ULL << (sizeof(T) * 8 - 1);
      switch (sizeof(T))
      {
        case (8): *v = ~htonll(*v); break;
        case (4): *v = ~htonl(*v); break;
        case (2): *v = ~htons(*v); break;
        case (1): *v = ~*v; break;
        default: break;
      }
      ASSERT_FALSE(key.key() == nullptr);
      ASSERT_EQ(memcmp(expected, key.key(), bufUnitSize), 0);
    }
    else
    {
      expected[0] = (!isAsc) ? 1 : 0;
      ASSERT_EQ(memcmp(expected, key.key(), 1), 0);
    }
  }
}

TYPED_TEST(KeyTypeTestUIntT, KeyTypeLessAsc)
{
  using T = typename KeyTypeTestIntT<TypeParam>::IntegralType;
  sorting::SortingThreads prevPhaseSorting;
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  uint8_t* key1Buf = new uint8_t[bufUnitSize];
  uint8_t* key2Buf = new uint8_t[bufUnitSize];
  std::memset(key1Buf, 0, bufUnitSize);
  std::memset(key2Buf, 0, bufUnitSize);
  auto key1 = KeyType(this->rg_, this->keysCols_, {0, 42, 0}, key1Buf);
  auto key2 = KeyType(this->rg_, this->keysCols_, {0, 42, 0}, key2Buf);
  ASSERT_FALSE(key1.less(key2, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key2.less(key1, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));

  uint8_t* key3Buf = new uint8_t[bufUnitSize];
  uint8_t* key4Buf = new uint8_t[bufUnitSize];
  std::memset(key3Buf, 0, bufUnitSize);
  std::memset(key4Buf, 0, bufUnitSize);
  auto key3 = KeyType(this->rg_, this->keysCols_, {0, 4, 0}, key3Buf);
  auto key4 = KeyType(this->rg_, this->keysCols_, {0, 5, 0}, key4Buf);
  ASSERT_TRUE(key3.less(key4, this->rg_, this->keysCols_, {0, 4, 0}, {0, 5, 0}, prevPhaseSorting));
  ASSERT_FALSE(key4.less(key3, this->rg_, this->keysCols_, {0, 5, 0}, {0, 4, 0}, prevPhaseSorting));

  uint8_t* key5Buf = new uint8_t[bufUnitSize];
  uint8_t* key6Buf = new uint8_t[bufUnitSize];
  std::memset(key5Buf, 0, bufUnitSize);
  std::memset(key6Buf, 0, bufUnitSize);
  auto key5 = KeyType(this->rg_, this->keysCols_, {0, 4001, 0}, key5Buf);
  auto key6 = KeyType(this->rg_, this->keysCols_, {0, 4002, 0}, key6Buf);
  ASSERT_TRUE(key5.less(key6, this->rg_, this->keysCols_, {0, 4001, 0}, {0, 4002, 0}, prevPhaseSorting));
  ASSERT_FALSE(key6.less(key5, this->rg_, this->keysCols_, {0, 4002, 0}, {0, 4001, 0}, prevPhaseSorting));

  uint8_t* key7Buf = new uint8_t[bufUnitSize];
  uint8_t* key8Buf = new uint8_t[bufUnitSize];
  std::memset(key7Buf, 0, bufUnitSize);
  std::memset(key8Buf, 0, bufUnitSize);
  auto key7 = KeyType(this->rg_, this->keysCols_, {0, 8190, 0}, key7Buf);
  auto key8 = KeyType(this->rg_, this->keysCols_, {0, 8191, 0}, key8Buf);
  if (sizeof(T) == 1)
  {
    ASSERT_TRUE(key5.less(key8, this->rg_, this->keysCols_, {0, 8190, 0}, {0, 8191, 0}, prevPhaseSorting));
    ASSERT_FALSE(key6.less(key7, this->rg_, this->keysCols_, {0, 8191, 0}, {0, 8190, 0}, prevPhaseSorting));
  }
  else if (sizeof(T) == 8)
  {
    ASSERT_FALSE(key7.less(key8, this->rg_, this->keysCols_, {0, 8190, 0}, {0, 8191, 0}, prevPhaseSorting));
    ASSERT_TRUE(key8.less(key7, this->rg_, this->keysCols_, {0, 8190, 0}, {0, 8191, 0}, prevPhaseSorting));
    ASSERT_TRUE(key5.less(key8, this->rg_, this->keysCols_, {0, 8190, 0}, {0, 8191, 0}, prevPhaseSorting));
    ASSERT_TRUE(key6.less(key7, this->rg_, this->keysCols_, {0, 8191, 0}, {0, 8190, 0}, prevPhaseSorting));
  }
  else
  {
    ASSERT_TRUE(key5.less(key8, this->rg_, this->keysCols_, {0, 8190, 0}, {0, 8191, 0}, prevPhaseSorting));
    ASSERT_TRUE(key6.less(key7, this->rg_, this->keysCols_, {0, 8191, 0}, {0, 8190, 0}, prevPhaseSorting));
  }

  ASSERT_TRUE(key1.less(key3, this->rg_, this->keysCols_, {0, 42, 0}, {0, 4, 0}, prevPhaseSorting));
  ASSERT_TRUE(key1.less(key5, this->rg_, this->keysCols_, {0, 42, 0}, {0, 4001, 0}, prevPhaseSorting));
  ASSERT_FALSE(key4.less(key1, this->rg_, this->keysCols_, {0, 4001, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key5.less(key1, this->rg_, this->keysCols_, {0, 4002, 0}, {0, 42, 0}, prevPhaseSorting));

  // Permuts are not important
  ASSERT_FALSE(key1.less(key1, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key2.less(key2, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key3.less(key3, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key4.less(key4, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key5.less(key5, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key6.less(key6, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
}

TYPED_TEST(KeyTypeTestUIntT, KeyTypeLessDsc)
{
  const bool isAsc = false;
  this->keysCols_ = {{0, isAsc}};
  using T = typename KeyTypeTestIntT<TypeParam>::IntegralType;
  sorting::SortingThreads prevPhaseSorting;
  size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  uint8_t* key1Buf = new uint8_t[bufUnitSize];
  uint8_t* key2Buf = new uint8_t[bufUnitSize];
  std::memset(key1Buf, 0, bufUnitSize);
  std::memset(key2Buf, 0, bufUnitSize);
  auto key1 = KeyType(this->rg_, this->keysCols_, {0, 42, 0}, key1Buf);
  auto key2 = KeyType(this->rg_, this->keysCols_, {0, 42, 0}, key2Buf);
  ASSERT_FALSE(key1.less(key2, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key2.less(key1, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  uint8_t* key3Buf = new uint8_t[bufUnitSize];
  uint8_t* key4Buf = new uint8_t[bufUnitSize];
  std::memset(key3Buf, 0, bufUnitSize);
  std::memset(key4Buf, 0, bufUnitSize);
  auto key3 = KeyType(this->rg_, this->keysCols_, {0, 4, 0}, key3Buf);
  auto key4 = KeyType(this->rg_, this->keysCols_, {0, 5, 0}, key4Buf);
  ASSERT_FALSE(key3.less(key4, this->rg_, this->keysCols_, {0, 4, 0}, {0, 5, 0}, prevPhaseSorting));
  ASSERT_TRUE(key4.less(key3, this->rg_, this->keysCols_, {0, 5, 0}, {0, 4, 0}, prevPhaseSorting));

  uint8_t* key5Buf = new uint8_t[bufUnitSize];
  uint8_t* key6Buf = new uint8_t[bufUnitSize];
  std::memset(key5Buf, 0, bufUnitSize);
  std::memset(key6Buf, 0, bufUnitSize);
  auto key5 = KeyType(this->rg_, this->keysCols_, {0, 4001, 0}, key5Buf);
  auto key6 = KeyType(this->rg_, this->keysCols_, {0, 4002, 0}, key6Buf);
  ASSERT_FALSE(key5.less(key6, this->rg_, this->keysCols_, {0, 4001, 0}, {0, 4002, 0}, prevPhaseSorting));
  ASSERT_TRUE(key6.less(key5, this->rg_, this->keysCols_, {0, 4002, 0}, {0, 4001, 0}, prevPhaseSorting));

  uint8_t* key7Buf = new uint8_t[bufUnitSize];
  uint8_t* key8Buf = new uint8_t[bufUnitSize];
  std::memset(key7Buf, 0, bufUnitSize);
  std::memset(key8Buf, 0, bufUnitSize);
  auto key7 = KeyType(this->rg_, this->keysCols_, {0, 8190, 0}, key7Buf);
  auto key8 = KeyType(this->rg_, this->keysCols_, {0, 8191, 0}, key8Buf);
  if (sizeof(T) == 1)
  {
    ASSERT_FALSE(key5.less(key8, this->rg_, this->keysCols_, {0, 8190, 0}, {0, 8191, 0}, prevPhaseSorting));
    ASSERT_TRUE(key6.less(key7, this->rg_, this->keysCols_, {0, 8191, 0}, {0, 8190, 0}, prevPhaseSorting));
  }
  else if (sizeof(T) == 8)
  {
    ASSERT_TRUE(key7.less(key8, this->rg_, this->keysCols_, {0, 8190, 0}, {0, 8191, 0}, prevPhaseSorting));
    ASSERT_FALSE(key8.less(key7, this->rg_, this->keysCols_, {0, 8190, 0}, {0, 8191, 0}, prevPhaseSorting));
    ASSERT_FALSE(key5.less(key8, this->rg_, this->keysCols_, {0, 8190, 0}, {0, 8191, 0}, prevPhaseSorting));
    ASSERT_FALSE(key6.less(key7, this->rg_, this->keysCols_, {0, 8191, 0}, {0, 8190, 0}, prevPhaseSorting));
  }
  else
  {
    ASSERT_FALSE(key5.less(key8, this->rg_, this->keysCols_, {0, 8190, 0}, {0, 8191, 0}, prevPhaseSorting));
    ASSERT_FALSE(key6.less(key7, this->rg_, this->keysCols_, {0, 8191, 0}, {0, 8190, 0}, prevPhaseSorting));
  }

  ASSERT_FALSE(key1.less(key3, this->rg_, this->keysCols_, {0, 42, 0}, {0, 4, 0}, prevPhaseSorting));

  ASSERT_FALSE(key1.less(key5, this->rg_, this->keysCols_, {0, 42, 0}, {0, 4001, 0}, prevPhaseSorting));
  ASSERT_TRUE(key4.less(key1, this->rg_, this->keysCols_, {0, 4001, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_TRUE(key5.less(key1, this->rg_, this->keysCols_, {0, 4002, 0}, {0, 42, 0}, prevPhaseSorting));

  // Permuts are not important
  ASSERT_FALSE(key1.less(key3, this->rg_, this->keysCols_, {0, 42, 0}, {0, 4, 0}, prevPhaseSorting));
  ASSERT_FALSE(key1.less(key4, this->rg_, this->keysCols_, {0, 42, 0}, {0, 4001, 0}, prevPhaseSorting));
  ASSERT_FALSE(key1.less(key5, this->rg_, this->keysCols_, {0, 42, 0}, {0, 4001, 0}, prevPhaseSorting));
  ASSERT_FALSE(key1.less(key6, this->rg_, this->keysCols_, {0, 42, 0}, {0, 4001, 0}, prevPhaseSorting));

  ASSERT_FALSE(key1.less(key1, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key2.less(key2, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key3.less(key3, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key4.less(key4, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key5.less(key5, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
  ASSERT_FALSE(key6.less(key6, this->rg_, this->keysCols_, {0, 42, 0}, {0, 42, 0}, prevPhaseSorting));
}

class KeyTypeTestVarcharP : public testing::TestWithParam<std::tuple<uint32_t, CHARSET_INFO*>>
{
 public:
  void SetUp() override
  {
    keysCols_ = {{0, true}};
    uint32_t stringMaxSize = std::get<0>(GetParam());
    rg_ = setupRG({execplan::CalpontSystemCatalog::VARCHAR}, {stringMaxSize}, {33});
    // Turn a StringStore on
    if (stringMaxSize > rg_.getStringTableThreshold())
    {
      rg_.setUseStringTable(true);
    }
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
    static constexpr const char str4[]{'a', 'z'};  // 'az'
    utils::ConstString cs4(str4, sizeof(str4));
    strings_.push_back(cs4);
    r.setStringField(cs1, 0);
    r.nextRow(rowSize);
    r.setStringField(cs2, 0);
    r.nextRow(rowSize);
    r.setStringField(cs3, 0);
    r.nextRow(rowSize);
    r.setStringField(cs4, 0);
    rg_.setRowCount(strings_.size());
    sorting::PermutationVec perm(rg_.getRowCount());
    size_t it = 0;
    std::generate(perm.begin(), perm.end(), [it]() mutable { return PermutationType{0, it++, 0}; });
    prevPhasThreads_.emplace_back(new PDQOrderBy());
    prevPhasThreads_.back()->getRGDatas().push_back(rgData_);
    prevPhasThreads_.back()->getMutPermutation().swap(perm);
  }

  joblist::OrderByKeysType keysCols_;
  rowgroup::RowGroup rg_;
  rowgroup::RGData rgData_;
  std::vector<utils::ConstString> strings_;
  sorting::SortingThreads prevPhasThreads_;
};

TEST_P(KeyTypeTestVarcharP, KeyTypeCtorVarchar)
{
  // utf8_general_ci = 33, 'a' == 'a '
  uint32_t columnIdx = 0;
  uint32_t w = rg_.getColumnWidth(columnIdx);
  size_t bufUnitSizeNoNull =
      (rg_.isLongString(columnIdx) && w >= rg_.getStringTableThreshold() && !rg_.getForceInline()[columnIdx])
          ? rg_.getStringTableThreshold()
          : w;
  size_t bufUnitSize = bufUnitSizeNoNull + 1;
  uint8_t* expected = new uint8_t[bufUnitSize];
  [[maybe_unused]] uint8_t* buf = new uint8_t[bufUnitSize * strings_.size()];
  // rg_.setCharset(0, &my_charset_utf8mb3_general_ci);
  CHARSET_INFO* charsetInfo = std::get<1>(GetParam());
  rg_.setCharset(0, charsetInfo);
  datatypes::Charset cs(charsetInfo);
  uint flags = (charsetInfo->state & MY_CS_NOPAD) ? 0 : cs.getDefaultFlags();
  for ([[maybe_unused]] size_t i = 0; auto& s : strings_)
  {
    uint8_t* pos = expected;
    uint nweights = s.length();
    auto key = KeyType(rg_, keysCols_, {0, i, 0}, &buf[i * bufUnitSize]);
    memset(pos, 0, bufUnitSize);
    *pos++ = 1;
    cs.strnxfrm(pos, bufUnitSizeNoNull, nweights, reinterpret_cast<const uchar*>(s.str()), s.length(), flags);
    // TBD add DSC encoding swaping the bits
    ASSERT_FALSE(key.key() == nullptr);
    ASSERT_EQ(memcmp(expected, key.key(), bufUnitSizeNoNull), 0);
    ++i;
  }
}

TEST_P(KeyTypeTestVarcharP, KeyTypeLessVarcharPad1)
{
  rg_.setCharset(0, &my_charset_utf8mb3_general_ci);
  uint32_t columnIdx = 0;
  uint32_t w = rg_.getColumnWidth(columnIdx);
  size_t bufUnitSize =
      ((rg_.isLongString(columnIdx) && w >= rg_.getStringTableThreshold() && !rg_.getForceInline()[columnIdx])
           ? rg_.getStringTableThreshold()
           : w) +
      1;  // NULL byte

  // size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  size_t keysNumber = this->rg_.getRowCount();
  std::vector<uint8_t*> keyBufsVec(keysNumber);
  std::vector<KeyType> keys;
  size_t i = 0;
  for_each(keyBufsVec.begin(), keyBufsVec.end(),
           [this, &keys, &i, bufUnitSize](uint8_t* buf)
           {
             buf = new uint8_t[bufUnitSize];
             std::memset(buf, 0, bufUnitSize);
             keys.emplace_back(KeyType(this->rg_, this->keysCols_, {0, i++, 0}, buf));
           });
  // keys[0] == NULL
  std::vector<std::vector<OutcomesT>> expectedResultsMatrix = {
      {FAL, TR, TR, TR},

      {FAL, FAL, FAL, TR},

      {FAL, FAL, FAL, TR},

      {FAL, FAL, FAL, FAL},

  };
  [[maybe_unused]] size_t x = 0;
  [[maybe_unused]] size_t y = 0;
  bool testHadFailed = false;
  for_each(keys.begin(), keys.end(),
           [this, &expectedResultsMatrix, &keys, &x, &y, &testHadFailed](auto& key1)
           {
             y = 0;
             for_each(keys.begin(), keys.end(),
                      [this, &expectedResultsMatrix, &key1, &x, &y, &testHadFailed](auto& key2)
                      {
                        OutcomesT result = (key1.less(key2, this->rg_, this->keysCols_, {0, x, 0}, {0, y, 0},
                                                      prevPhasThreads_))
                                               ? TR
                                               : FAL;
                        if (expectedResultsMatrix[x][y] != SKIP && expectedResultsMatrix[x][y] != result)
                        {
                          std::cout << "Results mismatch with: left row number = " << x
                                    << " and right row number = " << y << std::endl;
                          testHadFailed = true;
                        }
                        ++y;
                      });
             ++x;
           });
  if (testHadFailed)
  {
    ASSERT_TRUE(false);
  }
}

TEST_P(KeyTypeTestVarcharP, KeyTypeLessVarcharNoPad)
{
  rg_.setCharset(0, &my_charset_utf8mb3_general_nopad_ci);
  uint32_t columnIdx = 0;
  uint32_t w = rg_.getColumnWidth(columnIdx);
  size_t bufUnitSize =
      ((rg_.isLongString(columnIdx) && w >= rg_.getStringTableThreshold() && !rg_.getForceInline()[columnIdx])
           ? rg_.getStringTableThreshold()
           : w) +
      1;  // NULL byte

  // size_t bufUnitSize = this->rg_.getColumnWidth(0) + 1;
  size_t keysNumber = this->rg_.getRowCount();
  std::vector<uint8_t*> keyBufsVec(keysNumber);
  std::vector<KeyType> keys;
  size_t i = 0;
  for_each(keyBufsVec.begin(), keyBufsVec.end(),
           [this, &keys, &i, bufUnitSize](uint8_t* buf)
           {
             buf = new uint8_t[bufUnitSize];
             std::memset(buf, 0, bufUnitSize);
             keys.emplace_back(KeyType(this->rg_, this->keysCols_, {0, i++, 0}, buf));
           });
  // keys[0] == NULL
  std::vector<std::vector<OutcomesT>> expectedResultsMatrix = {
      {FAL, FAL, TR, TR},

      {TR, FAL, TR, TR},

      {FAL, FAL, FAL, TR},

      {FAL, FAL, FAL, FAL},

  };
  [[maybe_unused]] size_t x = 0;
  [[maybe_unused]] size_t y = 0;
  bool testHadFailed = false;
  for_each(keys.begin(), keys.end(),
           [this, &expectedResultsMatrix, &keys, &x, &y, &testHadFailed](auto& key1)
           {
             y = 0;
             for_each(keys.begin(), keys.end(),
                      [this, &expectedResultsMatrix, &key1, &x, &y, &testHadFailed](auto& key2)
                      {
                        OutcomesT result = (key1.less(key2, this->rg_, this->keysCols_, {0, x, 0}, {0, y, 0},
                                                      prevPhasThreads_))
                                               ? TR
                                               : FAL;
                        if (expectedResultsMatrix[x][y] != SKIP && expectedResultsMatrix[x][y] != result)
                        {
                          std::cout << "Results mismatch with: left row number = " << x
                                    << " and right row number = " << y << std::endl;
                          testHadFailed = true;
                        }
                        ++y;
                      });
             ++x;
           });
  if (testHadFailed)
  {
    ASSERT_TRUE(false);
  }
}

INSTANTIATE_TEST_SUITE_P(KeyTypeTestVarchar, KeyTypeTestVarcharP,
                         testing::Values(std::make_tuple(4, &my_charset_utf8mb3_general_ci),
                                         std::make_tuple(8, &my_charset_utf8mb3_general_ci),
                                         std::make_tuple(16, &my_charset_utf8mb3_general_ci),
                                         std::make_tuple(35, &my_charset_utf8mb3_general_ci),
                                         std::make_tuple(4, &my_charset_utf8mb3_general_nopad_ci),
                                         std::make_tuple(8, &my_charset_utf8mb3_general_nopad_ci),
                                         std::make_tuple(16, &my_charset_utf8mb3_general_nopad_ci),
                                         std::make_tuple(35, &my_charset_utf8mb3_general_nopad_ci)),
                         [](const ::testing::TestParamInfo<KeyTypeTestVarcharP::ParamType>& info)
                         {
                           std::string name = "_width_" + std::to_string(std::get<0>(info.param));
                           auto* cs = std::get<1>(info.param);
                           if (cs == &my_charset_utf8mb3_general_ci)
                           {
                             name += "_utf8mb3_general_ci";
                           }
                           else
                           {
                             name += "_utf8mb3_general_nopad_ci";
                           }
                           return name;
                         });

class KeyTypeTestLongVarchar : public testing::Test
{
 public:
  void SetUp() override
  {
    keysCols_ = {{0, true}};
    uint32_t stringMaxSize = 50;
    rg_ = setupRG({execplan::CalpontSystemCatalog::VARCHAR}, {stringMaxSize}, {33});
    rg_.setUseStringTable(true);
    rgData_ = rowgroup::RGData(rg_);
    rg_.setData(&rgData_);
    rowgroup::Row r;
    rg_.initRow(&r);
    uint32_t rowSize = r.getSize();
    rg_.getRow(0, &r);
    static constexpr const char str0[]{'a',  'b', 'a', 't', 'o', 'h', 'z', 'o', 'p', '1', 'c', 'a',
                                       'u',  'C', 'a', 'e', 'z', '5', 'a', 'e', 'B', '5', 'P', 'a',
                                       '\t', 'i', 'P', 'h', 'o', 'h', '6', 't', 'h', 'a', 'e'};
    static constexpr const char str1[]{'G', 'o', 'a', 't', 'o', 'h', 'z', 'o', 'p', '1', 'c', 'a', 'u',
                                       'C', 'a', 'e', 'z', '5', 'a', 'e', 'B', '5', 'P', 'h', 'a', 'i',
                                       'P', 'h', 'o', 'h', '6', 'f', 'a', 'h', 'T', '7', 'h', 'o'};
    static constexpr const char str2[]{'G', 'o', 'a', 't', 'o', 'h', 'z', 'o', 'p', '1', 'c', 'a', 'u', 'C',
                                       'a', 'e', 'z', '5', 'a', 'e', 'B', '5', 'P', 'h', 'a', 'i', 'P', 'h',
                                       'o', 'h', '6', 'c', 'h', 'e', 'e', 'k', 'a', 'l', 'e', 'i', 'r'};
    static constexpr const char str3[]{'u', 'u', 'd', 'e', 'e', 'f', '7', 'T', 'e', 'i', 'p', 'h',
                                       'e', 'e', 't', '8', 'a', 'w', '8', 's', 'h', 'u', 'a', 'f',
                                       'a', 'e', 'w', 'e', 'e', 't', 'h', '3', 's', 'i', 'e'};
    static constexpr const char str4[]{'G', 'o', 'a', 't', 'o', 'h', 'z', 'o', 'p', '1', 'c', 'a', 'u', 'C',
                                       'a', 'e', 'z', '5', 'a', 'e', 'B', '5', 'P', 'h', 'a', 'i', 'P', 'h',
                                       'o', 'h', '6', 'Z', '2', 'd', 'o', 'o', 'c', 'h', 'o', 'b'};
    static constexpr const char str5[]{'h', 'a', 'e', '5', 'o', 'u', 'x', 'e', 'e', 'T', '9', 'i',
                                       'B', 'i', 'e', '7', 'e', 'i', 'n', 'g', 'i', 'e', 'd', 'a',
                                       'e', '9', 'o', 'h', 'm', 'u', 'g', 'h', 'u', 'o', 'n'};
    static constexpr const char str6[]{'G', 'o', 'a', 't', 'o', 'h', 'z', 'o', 'p', '1', 'c', 'a', 'u',
                                       'C', 'a', 'e', 'z', '5', 'a', 'e', 'B', '5', 'P', 'h', 'a', 'i',
                                       'P', 'h', 'o', 'h', '6', 'f', 'a', 'h', 'T', '7', 'h', 'o'};

    utils::ConstString cs0(str0, sizeof(str0));
    strings_.push_back(cs0);
    utils::ConstString cs1(str1, sizeof(str1));
    strings_.push_back(cs1);
    utils::ConstString cs2(str2, sizeof(str2));
    strings_.push_back(cs2);
    utils::ConstString cs3(str3, sizeof(str3));
    strings_.push_back(cs3);
    utils::ConstString cs4(str4, sizeof(str4));
    strings_.push_back(cs4);
    utils::ConstString cs5(str5, sizeof(str5));
    strings_.push_back(cs5);
    utils::ConstString cs6(str6, sizeof(str6));
    strings_.push_back(cs6);
    r.setStringField(cs0, 0);
    r.nextRow(rowSize);
    r.setStringField(cs1, 0);
    r.nextRow(rowSize);
    r.setStringField(cs2, 0);
    r.nextRow(rowSize);
    r.setStringField(cs3, 0);
    r.nextRow(rowSize);
    r.setStringField(cs4, 0);
    r.nextRow(rowSize);
    r.setStringField(cs5, 0);
    r.nextRow(rowSize);
    r.setStringField(cs6, 0);
    rg_.setRowCount(7);
    sorting::PermutationVec perm(strings_.size());
    size_t it = 0;
    std::generate(perm.begin(), perm.end(), [it]() mutable { return PermutationType{0, it++, 0}; });
    prevPhasThreads_.emplace_back(new PDQOrderBy());
    prevPhasThreads_.back()->getRGDatas().push_back(rgData_);
    prevPhasThreads_.back()->getMutPermutation().swap(perm);
  }

  joblist::OrderByKeysType keysCols_;
  rowgroup::RowGroup rg_;
  rowgroup::RGData rgData_;
  std::vector<utils::ConstString> strings_;
  sorting::SortingThreads prevPhasThreads_;
};

TEST_F(KeyTypeTestLongVarchar, KeyTypeLessVarcharPadAsc)
{
  rg_.setCharset(0, &my_charset_utf8mb3_general_ci);
  size_t bufUnitSize = rg_.getColumnWidth(0) + 1;

  uint8_t* key0Buf = new uint8_t[bufUnitSize];
  uint8_t* key1Buf = new uint8_t[bufUnitSize];
  uint8_t* key2Buf = new uint8_t[bufUnitSize];
  uint8_t* key3Buf = new uint8_t[bufUnitSize];
  uint8_t* key4Buf = new uint8_t[bufUnitSize];
  uint8_t* key5Buf = new uint8_t[bufUnitSize];
  uint8_t* key6Buf = new uint8_t[bufUnitSize];

  auto key0 = KeyType(rg_, keysCols_, {0, 0, 0}, key0Buf);
  auto key1 = KeyType(rg_, keysCols_, {0, 1, 0}, key1Buf);
  auto key2 = KeyType(rg_, keysCols_, {0, 2, 0}, key2Buf);
  auto key3 = KeyType(rg_, keysCols_, {0, 3, 0}, key3Buf);
  auto key4 = KeyType(rg_, keysCols_, {0, 4, 0}, key4Buf);
  auto key5 = KeyType(rg_, keysCols_, {0, 5, 0}, key5Buf);
  auto key6 = KeyType(rg_, keysCols_, {0, 6, 0}, key6Buf);

  ASSERT_FALSE(key1.less(key1, rg_, keysCols_, {0, 1, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key2, rg_, keysCols_, {0, 2, 0}, {0, 2, 0}, prevPhasThreads_));
  ASSERT_FALSE(key3.less(key3, rg_, keysCols_, {0, 3, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key4, rg_, keysCols_, {0, 4, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key5, rg_, keysCols_, {0, 5, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key6, rg_, keysCols_, {0, 6, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_TRUE(key0.less(key1, rg_, keysCols_, {0, 0, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key2, rg_, keysCols_, {0, 0, 0}, {0, 2, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key3, rg_, keysCols_, {0, 0, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key4, rg_, keysCols_, {0, 0, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key5, rg_, keysCols_, {0, 0, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key6, rg_, keysCols_, {0, 0, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key0, rg_, keysCols_, {0, 1, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key0, rg_, keysCols_, {0, 2, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key3.less(key0, rg_, keysCols_, {0, 3, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key0, rg_, keysCols_, {0, 4, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key0, rg_, keysCols_, {0, 5, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key0, rg_, keysCols_, {0, 6, 0}, {0, 0, 0}, prevPhasThreads_));

  ASSERT_TRUE(key2.less(key1, rg_, keysCols_, {0, 2, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key3, rg_, keysCols_, {0, 2, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key4, rg_, keysCols_, {0, 2, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key5, rg_, keysCols_, {0, 2, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key6, rg_, keysCols_, {0, 2, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key6, rg_, keysCols_, {0, 1, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key3.less(key6, rg_, keysCols_, {0, 3, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key6, rg_, keysCols_, {0, 4, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key6, rg_, keysCols_, {0, 5, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key6, rg_, keysCols_, {0, 5, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_TRUE(key6.less(key3, rg_, keysCols_, {0, 6, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key6.less(key4, rg_, keysCols_, {0, 6, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key6.less(key5, rg_, keysCols_, {0, 6, 0}, {0, 5, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key6, rg_, keysCols_, {0, 1, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key1, rg_, keysCols_, {0, 6, 0}, {0, 1, 0}, prevPhasThreads_));

  ASSERT_TRUE(key1.less(key3, rg_, keysCols_, {0, 1, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key1.less(key4, rg_, keysCols_, {0, 1, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key1.less(key5, rg_, keysCols_, {0, 1, 0}, {0, 5, 0}, prevPhasThreads_));

  ASSERT_FALSE(key3.less(key1, rg_, keysCols_, {0, 3, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key1, rg_, keysCols_, {0, 4, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key1, rg_, keysCols_, {0, 5, 0}, {0, 1, 0}, prevPhasThreads_));

  ASSERT_TRUE(key4.less(key5, rg_, keysCols_, {0, 4, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_TRUE(key4.less(key3, rg_, keysCols_, {0, 4, 0}, {0, 3, 0}, prevPhasThreads_));

  ASSERT_FALSE(key3.less(key4, rg_, keysCols_, {0, 3, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key4, rg_, keysCols_, {0, 5, 0}, {0, 4, 0}, prevPhasThreads_));

  ASSERT_TRUE(key5.less(key3, rg_, keysCols_, {0, 5, 0}, {0, 3, 0}, prevPhasThreads_));

  ASSERT_FALSE(key3.less(key5, rg_, keysCols_, {0, 3, 0}, {0, 5, 0}, prevPhasThreads_));
}

TEST_F(KeyTypeTestLongVarchar, KeyTypeLessVarcharPadDsc)
{
  const bool isAsc = false;
  keysCols_ = {{0, isAsc}};
  rg_.setCharset(0, &my_charset_utf8mb3_general_ci);
  size_t bufUnitSize = rg_.getStringTableThreshold() + 1;

  uint8_t* key0Buf = new uint8_t[bufUnitSize];
  uint8_t* key1Buf = new uint8_t[bufUnitSize];
  uint8_t* key2Buf = new uint8_t[bufUnitSize];
  uint8_t* key3Buf = new uint8_t[bufUnitSize];
  uint8_t* key4Buf = new uint8_t[bufUnitSize];
  uint8_t* key5Buf = new uint8_t[bufUnitSize];
  uint8_t* key6Buf = new uint8_t[bufUnitSize];

  auto key0 = KeyType(rg_, keysCols_, {0, 0, 0}, key0Buf);
  auto key1 = KeyType(rg_, keysCols_, {0, 1, 0}, key1Buf);
  auto key2 = KeyType(rg_, keysCols_, {0, 2, 0}, key2Buf);
  auto key3 = KeyType(rg_, keysCols_, {0, 3, 0}, key3Buf);
  auto key4 = KeyType(rg_, keysCols_, {0, 4, 0}, key4Buf);
  auto key5 = KeyType(rg_, keysCols_, {0, 5, 0}, key5Buf);
  auto key6 = KeyType(rg_, keysCols_, {0, 6, 0}, key6Buf);

  ASSERT_FALSE(key1.less(key1, rg_, keysCols_, {0, 1, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key2, rg_, keysCols_, {0, 2, 0}, {0, 2, 0}, prevPhasThreads_));
  ASSERT_FALSE(key3.less(key3, rg_, keysCols_, {0, 3, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key4, rg_, keysCols_, {0, 4, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key5, rg_, keysCols_, {0, 5, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key6, rg_, keysCols_, {0, 6, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key0.less(key1, rg_, keysCols_, {0, 0, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key0.less(key2, rg_, keysCols_, {0, 0, 0}, {0, 2, 0}, prevPhasThreads_));
  ASSERT_FALSE(key0.less(key3, rg_, keysCols_, {0, 0, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key0.less(key4, rg_, keysCols_, {0, 0, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key0.less(key5, rg_, keysCols_, {0, 0, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key0.less(key6, rg_, keysCols_, {0, 0, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_TRUE(key1.less(key0, rg_, keysCols_, {0, 1, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key0, rg_, keysCols_, {0, 2, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_TRUE(key3.less(key0, rg_, keysCols_, {0, 3, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_TRUE(key4.less(key0, rg_, keysCols_, {0, 4, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_TRUE(key5.less(key0, rg_, keysCols_, {0, 5, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_TRUE(key6.less(key0, rg_, keysCols_, {0, 6, 0}, {0, 0, 0}, prevPhasThreads_));

  ASSERT_FALSE(key2.less(key1, rg_, keysCols_, {0, 2, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key3, rg_, keysCols_, {0, 2, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key4, rg_, keysCols_, {0, 2, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key5, rg_, keysCols_, {0, 2, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key6, rg_, keysCols_, {0, 2, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key6, rg_, keysCols_, {0, 1, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_TRUE(key3.less(key6, rg_, keysCols_, {0, 3, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_TRUE(key4.less(key6, rg_, keysCols_, {0, 4, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_TRUE(key5.less(key6, rg_, keysCols_, {0, 5, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_TRUE(key6.less(key6, rg_, keysCols_, {0, 5, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key6.less(key3, rg_, keysCols_, {0, 6, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key4, rg_, keysCols_, {0, 6, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key5, rg_, keysCols_, {0, 6, 0}, {0, 5, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key6, rg_, keysCols_, {0, 1, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key1, rg_, keysCols_, {0, 6, 0}, {0, 1, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key3, rg_, keysCols_, {0, 1, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key1.less(key4, rg_, keysCols_, {0, 1, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key1.less(key5, rg_, keysCols_, {0, 1, 0}, {0, 5, 0}, prevPhasThreads_));

  ASSERT_TRUE(key3.less(key1, rg_, keysCols_, {0, 3, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_TRUE(key4.less(key1, rg_, keysCols_, {0, 4, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_TRUE(key5.less(key1, rg_, keysCols_, {0, 5, 0}, {0, 1, 0}, prevPhasThreads_));

  ASSERT_FALSE(key4.less(key5, rg_, keysCols_, {0, 4, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key3, rg_, keysCols_, {0, 4, 0}, {0, 3, 0}, prevPhasThreads_));

  ASSERT_TRUE(key3.less(key4, rg_, keysCols_, {0, 3, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key5.less(key4, rg_, keysCols_, {0, 5, 0}, {0, 4, 0}, prevPhasThreads_));

  ASSERT_FALSE(key5.less(key3, rg_, keysCols_, {0, 5, 0}, {0, 3, 0}, prevPhasThreads_));

  ASSERT_TRUE(key3.less(key5, rg_, keysCols_, {0, 3, 0}, {0, 5, 0}, prevPhasThreads_));
}

TEST_F(KeyTypeTestLongVarchar, KeyTypeLessVarcharNoPadAsc)
{
  rg_.setCharset(0, &my_charset_utf8mb3_general_nopad_ci);
  size_t bufUnitSize = rg_.getColumnWidth(0) + 1;

  uint8_t* key0Buf = new uint8_t[bufUnitSize];
  uint8_t* key1Buf = new uint8_t[bufUnitSize];
  uint8_t* key2Buf = new uint8_t[bufUnitSize];
  uint8_t* key3Buf = new uint8_t[bufUnitSize];
  uint8_t* key4Buf = new uint8_t[bufUnitSize];
  uint8_t* key5Buf = new uint8_t[bufUnitSize];
  uint8_t* key6Buf = new uint8_t[bufUnitSize];

  auto key0 = KeyType(rg_, keysCols_, {0, 0, 0}, key0Buf);
  auto key1 = KeyType(rg_, keysCols_, {0, 1, 0}, key1Buf);
  auto key2 = KeyType(rg_, keysCols_, {0, 2, 0}, key2Buf);
  auto key3 = KeyType(rg_, keysCols_, {0, 3, 0}, key3Buf);
  auto key4 = KeyType(rg_, keysCols_, {0, 4, 0}, key4Buf);
  auto key5 = KeyType(rg_, keysCols_, {0, 5, 0}, key5Buf);
  auto key6 = KeyType(rg_, keysCols_, {0, 6, 0}, key6Buf);

  ASSERT_FALSE(key1.less(key1, rg_, keysCols_, {0, 1, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key2, rg_, keysCols_, {0, 2, 0}, {0, 2, 0}, prevPhasThreads_));
  ASSERT_FALSE(key3.less(key3, rg_, keysCols_, {0, 3, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key4, rg_, keysCols_, {0, 4, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key5, rg_, keysCols_, {0, 5, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key6, rg_, keysCols_, {0, 6, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_TRUE(key0.less(key1, rg_, keysCols_, {0, 0, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key2, rg_, keysCols_, {0, 0, 0}, {0, 2, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key3, rg_, keysCols_, {0, 0, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key4, rg_, keysCols_, {0, 0, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key5, rg_, keysCols_, {0, 0, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key6, rg_, keysCols_, {0, 0, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key0, rg_, keysCols_, {0, 1, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key0, rg_, keysCols_, {0, 2, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key3.less(key0, rg_, keysCols_, {0, 3, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key0, rg_, keysCols_, {0, 4, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key0, rg_, keysCols_, {0, 5, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key0, rg_, keysCols_, {0, 6, 0}, {0, 0, 0}, prevPhasThreads_));

  ASSERT_TRUE(key2.less(key1, rg_, keysCols_, {0, 2, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key3, rg_, keysCols_, {0, 2, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key4, rg_, keysCols_, {0, 2, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key5, rg_, keysCols_, {0, 2, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key6, rg_, keysCols_, {0, 2, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key6, rg_, keysCols_, {0, 1, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key3.less(key6, rg_, keysCols_, {0, 3, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key6, rg_, keysCols_, {0, 4, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key6, rg_, keysCols_, {0, 5, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key6, rg_, keysCols_, {0, 5, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_TRUE(key6.less(key3, rg_, keysCols_, {0, 6, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key6.less(key4, rg_, keysCols_, {0, 6, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key6.less(key5, rg_, keysCols_, {0, 6, 0}, {0, 5, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key6, rg_, keysCols_, {0, 1, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key1, rg_, keysCols_, {0, 6, 0}, {0, 1, 0}, prevPhasThreads_));

  ASSERT_TRUE(key1.less(key3, rg_, keysCols_, {0, 1, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key1.less(key4, rg_, keysCols_, {0, 1, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key1.less(key5, rg_, keysCols_, {0, 1, 0}, {0, 5, 0}, prevPhasThreads_));

  ASSERT_FALSE(key3.less(key1, rg_, keysCols_, {0, 3, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key1, rg_, keysCols_, {0, 4, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key1, rg_, keysCols_, {0, 5, 0}, {0, 1, 0}, prevPhasThreads_));

  ASSERT_TRUE(key4.less(key5, rg_, keysCols_, {0, 4, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_TRUE(key4.less(key3, rg_, keysCols_, {0, 4, 0}, {0, 3, 0}, prevPhasThreads_));

  ASSERT_FALSE(key3.less(key4, rg_, keysCols_, {0, 3, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key4, rg_, keysCols_, {0, 5, 0}, {0, 4, 0}, prevPhasThreads_));

  ASSERT_TRUE(key5.less(key3, rg_, keysCols_, {0, 5, 0}, {0, 3, 0}, prevPhasThreads_));

  ASSERT_FALSE(key3.less(key5, rg_, keysCols_, {0, 3, 0}, {0, 5, 0}, prevPhasThreads_));
}

TEST_F(KeyTypeTestLongVarchar, KeyTypeLessVarcharNoPadDsc)
{
  const bool isAsc = false;
  keysCols_ = {{0, isAsc}};
  rg_.setCharset(0, &my_charset_utf8mb3_general_nopad_ci);
  size_t bufUnitSize = rg_.getColumnWidth(0) + 1;

  uint8_t* key0Buf = new uint8_t[bufUnitSize];
  uint8_t* key1Buf = new uint8_t[bufUnitSize];
  uint8_t* key2Buf = new uint8_t[bufUnitSize];
  uint8_t* key3Buf = new uint8_t[bufUnitSize];
  uint8_t* key4Buf = new uint8_t[bufUnitSize];
  uint8_t* key5Buf = new uint8_t[bufUnitSize];
  uint8_t* key6Buf = new uint8_t[bufUnitSize];

  auto key0 = KeyType(rg_, keysCols_, {0, 0, 0}, key0Buf);
  auto key1 = KeyType(rg_, keysCols_, {0, 1, 0}, key1Buf);
  auto key2 = KeyType(rg_, keysCols_, {0, 2, 0}, key2Buf);
  auto key3 = KeyType(rg_, keysCols_, {0, 3, 0}, key3Buf);
  auto key4 = KeyType(rg_, keysCols_, {0, 4, 0}, key4Buf);
  auto key5 = KeyType(rg_, keysCols_, {0, 5, 0}, key5Buf);
  auto key6 = KeyType(rg_, keysCols_, {0, 6, 0}, key6Buf);

  ASSERT_FALSE(key1.less(key1, rg_, keysCols_, {0, 1, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key2, rg_, keysCols_, {0, 2, 0}, {0, 2, 0}, prevPhasThreads_));
  ASSERT_FALSE(key3.less(key3, rg_, keysCols_, {0, 3, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key4, rg_, keysCols_, {0, 4, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key5, rg_, keysCols_, {0, 5, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key6, rg_, keysCols_, {0, 6, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key0.less(key1, rg_, keysCols_, {0, 0, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key0.less(key2, rg_, keysCols_, {0, 0, 0}, {0, 2, 0}, prevPhasThreads_));
  ASSERT_FALSE(key0.less(key3, rg_, keysCols_, {0, 0, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key0.less(key4, rg_, keysCols_, {0, 0, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key0.less(key5, rg_, keysCols_, {0, 0, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key0.less(key6, rg_, keysCols_, {0, 0, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_TRUE(key1.less(key0, rg_, keysCols_, {0, 1, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key0, rg_, keysCols_, {0, 2, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_TRUE(key3.less(key0, rg_, keysCols_, {0, 3, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_TRUE(key4.less(key0, rg_, keysCols_, {0, 4, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_TRUE(key5.less(key0, rg_, keysCols_, {0, 5, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_TRUE(key6.less(key0, rg_, keysCols_, {0, 6, 0}, {0, 0, 0}, prevPhasThreads_));

  ASSERT_FALSE(key2.less(key1, rg_, keysCols_, {0, 2, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key3, rg_, keysCols_, {0, 2, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key4, rg_, keysCols_, {0, 2, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key5, rg_, keysCols_, {0, 2, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key6, rg_, keysCols_, {0, 2, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key6, rg_, keysCols_, {0, 1, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_TRUE(key3.less(key6, rg_, keysCols_, {0, 3, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_TRUE(key4.less(key6, rg_, keysCols_, {0, 4, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_TRUE(key5.less(key6, rg_, keysCols_, {0, 5, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_TRUE(key6.less(key6, rg_, keysCols_, {0, 5, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key6.less(key3, rg_, keysCols_, {0, 6, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key4, rg_, keysCols_, {0, 6, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key5, rg_, keysCols_, {0, 6, 0}, {0, 5, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key6, rg_, keysCols_, {0, 1, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key1, rg_, keysCols_, {0, 6, 0}, {0, 1, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key3, rg_, keysCols_, {0, 1, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key1.less(key4, rg_, keysCols_, {0, 1, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key1.less(key5, rg_, keysCols_, {0, 1, 0}, {0, 5, 0}, prevPhasThreads_));

  ASSERT_TRUE(key3.less(key1, rg_, keysCols_, {0, 3, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_TRUE(key4.less(key1, rg_, keysCols_, {0, 4, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_TRUE(key5.less(key1, rg_, keysCols_, {0, 5, 0}, {0, 1, 0}, prevPhasThreads_));

  ASSERT_FALSE(key4.less(key5, rg_, keysCols_, {0, 4, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key3, rg_, keysCols_, {0, 4, 0}, {0, 3, 0}, prevPhasThreads_));

  ASSERT_TRUE(key3.less(key4, rg_, keysCols_, {0, 3, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key5.less(key4, rg_, keysCols_, {0, 5, 0}, {0, 4, 0}, prevPhasThreads_));

  ASSERT_FALSE(key5.less(key3, rg_, keysCols_, {0, 5, 0}, {0, 3, 0}, prevPhasThreads_));

  ASSERT_TRUE(key3.less(key5, rg_, keysCols_, {0, 3, 0}, {0, 5, 0}, prevPhasThreads_));
}

class KeyTypeCompositeKeyTest : public testing::Test
{
 public:
  void SetUp() override
  {
    keysCols_ = {{0, true}, {1, true}};
    // uint32_t stringMaxSize = 50;
    rg_ = setupRG({execplan::CalpontSystemCatalog::VARCHAR, execplan::CalpontSystemCatalog::BIGINT}, {121, 8},
                  {33, 33});
    rg_.setUseStringTable(true);
    rgData_ = rowgroup::RGData(rg_);
    rg_.setData(&rgData_);
    rowgroup::Row r;
    rg_.initRow(&r);
    uint32_t rowSize = r.getSize();
    rg_.getRow(0, &r);
    static constexpr const char str0[]{'a',  'b', 'a', 't', 'o', 'h', 'z', 'o', 'p', '1', 'c', 'a',
                                       'u',  'C', 'a', 'e', 'z', '5', 'a', 'e', 'B', '5', 'P', 'a',
                                       '\t', 'i', 'P', 'h', 'o', 'h', '6', 't', 'h', 'a', 'e'};
    static constexpr const char str1[]{'G', 'o', 'a', 't', 'o', 'h', 'z', 'o', 'p', '1', 'c', 'a', 'u',
                                       'C', 'a', 'e', 'z', '5', 'a', 'e', 'B', '5', 'P', 'h', 'a', 'i',
                                       'P', 'h', 'o', 'h', '6', 'f', 'a', 'h', 'T', '7', 'h', 'o'};
    static constexpr const char str2[]{'G', 'o', 'a', 't', 'o', 'h', 'z', 'o', 'p', '1', 'c', 'a', 'u',
                                       'C', 'a', 'e', 'z', '5', 'a', 'e', 'B', '5', 'P', 'h', 'a', 'i',
                                       'P', 'h', 'o', 'h', '6', 'f', 'a', 'h', 'T', '7', 'h', 'o'};
    static constexpr const char str3[]{'u', 'u', 'd', 'e', 'e', 'f', '7', 'T', 'e', 'i', 'p', 'h',
                                       'e', 'e', 't', '8', 'a', 'w', '8', 's', 'h', 'u', 'a', 'f',
                                       'a', 'e', 'w', 'e', 'e', 't', 'h', '3', 's', 'i', 'e'};
    static constexpr const char str4[]{'G', 'o', 'a', 't', 'o', 'h', 'z', 'o', 'p', '1', 'c', 'a', 'u', 'C',
                                       'a', 'e', 'z', '5', 'a', 'e', 'B', '5', 'P', 'h', 'a', 'i', 'P', 'h',
                                       'o', 'h', '6', 'Z', '2', 'd', 'o', 'o', 'c', 'h', 'o', 'b'};
    static constexpr const char str5[]{'h', 'a', 'e', '5', 'o', 'u', 'x', 'e', 'e', 'T', '9', 'i',
                                       'B', 'i', 'e', '7', 'e', 'i', 'n', 'g', 'i', 'e', 'd', 'a',
                                       'e', '9', 'o', 'h', 'm', 'u', 'g', 'h', 'u', 'o', 'n'};
    static constexpr const char str6[]{'G', 'o', 'a', 't', 'o', 'h', 'z', 'o', 'p', '1', 'c', 'a', 'u',
                                       'C', 'a', 'e', 'z', '5', 'a', 'e', 'B', '5', 'P', 'h', 'a', 'i',
                                       'P', 'h', 'o', 'h', '6', 'f', 'a', 'h', 'T', '7', 'h', 'o'};

    utils::ConstString cs0(str0, sizeof(str0));
    strings_.push_back(cs0);
    utils::ConstString cs1(str1, sizeof(str1));
    strings_.push_back(cs1);
    utils::ConstString cs2(str2, sizeof(str2));
    strings_.push_back(cs2);
    utils::ConstString cs3(str3, sizeof(str3));
    strings_.push_back(cs3);
    utils::ConstString cs4(str4, sizeof(str4));
    strings_.push_back(cs4);
    utils::ConstString cs5(str5, sizeof(str5));
    strings_.push_back(cs5);
    utils::ConstString cs6(str6, sizeof(str6));
    strings_.push_back(cs6);
    r.setStringField(cs0, 0);
    r.setIntField<8>(42, 1);
    r.nextRow(rowSize);
    r.setStringField(cs1, 0);
    r.setIntField<8>(-300888445, 1);
    r.nextRow(rowSize);
    r.setStringField(cs2, 0);
    r.setIntField<8>(-309888445, 1);
    r.nextRow(rowSize);
    r.setStringField(cs3, 0);
    r.setIntField<8>(-9999999999999, 1);
    r.nextRow(rowSize);
    r.setStringField(cs4, 0);
    r.setIntField<8>(40, 1);
    r.nextRow(rowSize);
    r.setStringField(cs5, 0);
    r.setIntField<8>(-1, 1);
    r.nextRow(rowSize);
    r.setStringField(cs6, 0);
    r.setIntField<8>(-309888445, 1);
    rg_.setRowCount(7);
    sorting::PermutationVec perm(strings_.size());
    size_t it = 0;
    std::generate(perm.begin(), perm.end(), [it]() mutable { return PermutationType{0, it++, 0}; });
    prevPhasThreads_.emplace_back(new PDQOrderBy());
    prevPhasThreads_.back()->getRGDatas().push_back(rgData_);
    prevPhasThreads_.back()->getMutPermutation().swap(perm);
  }
  joblist::OrderByKeysType keysCols_;
  rowgroup::RowGroup rg_;
  rowgroup::RGData rgData_;
  std::vector<utils::ConstString> strings_;
  sorting::SortingThreads prevPhasThreads_;
};

TEST_F(KeyTypeCompositeKeyTest, KeyTypeCtorVarcharPadAsc)
{
  // utf8_general_ci = 33, 'a' == 'a '
  uint32_t bufUnitSizeNoNull = 0;  // not NULL byte
  for_each(rg_.getColWidths().begin(), rg_.getColWidths().end(),
           [&bufUnitSizeNoNull, this](const uint32_t width) {
             bufUnitSizeNoNull +=
                 (width <= rg_.getStringTableThreshold()) ? width : rg_.getStringTableThreshold();
           });
  uint32_t nullBytesCount = rg_.getColWidths().size();
  uint32_t bufUnitSize = nullBytesCount + bufUnitSizeNoNull;
  [[maybe_unused]] uint8_t* expected = new uint8_t[bufUnitSize];
  [[maybe_unused]] uint8_t* buf = new uint8_t[bufUnitSize * strings_.size()];
  auto* charsetInfo = &my_charset_utf8mb3_general_ci;
  rg_.setCharset(0, charsetInfo);
  datatypes::Charset cs(charsetInfo);
  rowgroup::Row r;
  rg_.initRow(&r);
  rg_.getRow(0, &r);
  uint32_t rowSize = r.getSize();
  [[maybe_unused]] uint flags = cs.getDefaultFlags();
  size_t varcharKeyLength = (rg_.getColumnWidth(0) <= rg_.getStringTableThreshold())
                                ? rg_.getColumnWidth(0)
                                : rg_.getStringTableThreshold();
  for ([[maybe_unused]] size_t i = 0; auto& s : strings_)
  {
    // std::cout << "s " << s.str() << std::endl;
    uint8_t* pos = expected;
    uint nweights = s.length();
    auto key = KeyType(rg_, keysCols_, {0, i, 0}, &buf[i * bufUnitSize]);
    memset(pos, 0, bufUnitSize);
    *pos++ = 1;
    size_t len = cs.strnxfrm(pos, varcharKeyLength, nweights, reinterpret_cast<const uchar*>(s.str()),
                             s.length(), flags);
    int64_t value = r.getIntField<8>(1);
    r.nextRow(rowSize);
    pos += len;
    *pos++ = 1;
    memcpy(pos, &value, sizeof(int64_t));
    int64_t* v = reinterpret_cast<int64_t*>(pos);
    *v ^= 0x8000000000000000;
    *v = htonll(*v);
    ASSERT_FALSE(key.key() == nullptr);
    ASSERT_EQ(memcmp(expected, key.key(), bufUnitSize), 0);
    ++i;
  }
}

TEST_F(KeyTypeCompositeKeyTest, KeyTypeCtorVarcharPadDsc)
{
  // utf8_general_ci = 33, 'a' == 'a '
  const bool isAsc = false;
  keysCols_ = {{0, isAsc}, {1, isAsc}};
  uint32_t bufUnitSizeNoNull = 0;  // not NULL byte
  for_each(rg_.getColWidths().begin(), rg_.getColWidths().end(),
           [&bufUnitSizeNoNull, this](const uint32_t width) {
             bufUnitSizeNoNull +=
                 (width <= rg_.getStringTableThreshold()) ? width : rg_.getStringTableThreshold();
           });
  uint32_t nullBytesCount = rg_.getColWidths().size();
  uint32_t bufUnitSize = nullBytesCount + bufUnitSizeNoNull;
  [[maybe_unused]] uint8_t* expected = new uint8_t[bufUnitSize];
  [[maybe_unused]] uint8_t* buf = new uint8_t[bufUnitSize * strings_.size()];
  auto* charsetInfo = &my_charset_utf8mb3_general_ci;
  rg_.setCharset(0, charsetInfo);
  datatypes::Charset cs(charsetInfo);
  rowgroup::Row r;
  rg_.initRow(&r);
  rg_.getRow(0, &r);
  uint32_t rowSize = r.getSize();
  [[maybe_unused]] uint flags = cs.getDefaultFlags();
  size_t varcharKeyLength = (rg_.getColumnWidth(0) <= rg_.getStringTableThreshold())
                                ? rg_.getColumnWidth(0)
                                : rg_.getStringTableThreshold();
  for ([[maybe_unused]] size_t i = 0; auto& s : strings_)
  {
    // std::cout << "s " << s.str() << std::endl;
    uint8_t* pos = expected;
    uint nweights = s.length();
    auto key = KeyType(rg_, keysCols_, {0, i, 0}, &buf[i * bufUnitSize]);
    memset(pos, 0, bufUnitSize);
    *pos++ = (!isAsc) ? 0 : 1;  // 1st column NULL byte
    size_t len = cs.strnxfrm(pos, varcharKeyLength, nweights, reinterpret_cast<const uchar*>(s.str()),
                             s.length(), flags);
    uint8_t* end = pos + len;
    for (; pos < end; ++pos)
    {
      *pos = ~*pos;
    }
    *pos++ = (!isAsc) ? 0 : 1;  // 2nd column NULL byte
    int64_t value = r.getIntField<8>(1);
    r.nextRow(rowSize);
    memcpy(pos, &value, sizeof(int64_t));
    int64_t* v = reinterpret_cast<int64_t*>(pos);
    *v ^= 0x8000000000000000;
    *v = ~htonll(*v);
    ASSERT_FALSE(key.key() == nullptr);
    ASSERT_EQ(memcmp(expected, key.key(), bufUnitSize), 0);
    ++i;
  }
}

TEST_F(KeyTypeCompositeKeyTest, KeyTypeCtorVarcharPadAscLess)
{
  // utf8_general_ci = 33, 'a' == 'a '
  uint32_t bufUnitSizeNoNull = 0;  // not NULL byte
  for_each(rg_.getColWidths().begin(), rg_.getColWidths().end(),
           [&bufUnitSizeNoNull, this](const uint32_t width) {
             bufUnitSizeNoNull +=
                 (width <= rg_.getStringTableThreshold()) ? width : rg_.getStringTableThreshold();
           });
  uint32_t nullBytesCount = rg_.getColWidths().size();
  uint32_t bufUnitSize = nullBytesCount + bufUnitSizeNoNull;
  [[maybe_unused]] uint8_t* expected = new uint8_t[bufUnitSize];
  [[maybe_unused]] uint8_t* buf = new uint8_t[bufUnitSize * strings_.size()];
  rg_.setCharset(0, &my_charset_utf8mb3_general_ci);
  uint8_t* key0Buf = new uint8_t[bufUnitSize];
  uint8_t* key1Buf = new uint8_t[bufUnitSize];
  uint8_t* key2Buf = new uint8_t[bufUnitSize];
  uint8_t* key3Buf = new uint8_t[bufUnitSize];
  uint8_t* key4Buf = new uint8_t[bufUnitSize];
  uint8_t* key5Buf = new uint8_t[bufUnitSize];
  uint8_t* key6Buf = new uint8_t[bufUnitSize];

  auto key0 = KeyType(rg_, keysCols_, {0, 0, 0}, key0Buf);
  auto key1 = KeyType(rg_, keysCols_, {0, 1, 0}, key1Buf);
  auto key2 = KeyType(rg_, keysCols_, {0, 2, 0}, key2Buf);
  auto key3 = KeyType(rg_, keysCols_, {0, 3, 0}, key3Buf);
  auto key4 = KeyType(rg_, keysCols_, {0, 4, 0}, key4Buf);
  auto key5 = KeyType(rg_, keysCols_, {0, 5, 0}, key5Buf);
  auto key6 = KeyType(rg_, keysCols_, {0, 6, 0}, key6Buf);

  ASSERT_FALSE(key1.less(key1, rg_, keysCols_, {0, 1, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key2, rg_, keysCols_, {0, 2, 0}, {0, 2, 0}, prevPhasThreads_));
  ASSERT_FALSE(key3.less(key3, rg_, keysCols_, {0, 3, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key4, rg_, keysCols_, {0, 4, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key5, rg_, keysCols_, {0, 5, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key6, rg_, keysCols_, {0, 6, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_TRUE(key0.less(key1, rg_, keysCols_, {0, 0, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key2, rg_, keysCols_, {0, 0, 0}, {0, 2, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key3, rg_, keysCols_, {0, 0, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key4, rg_, keysCols_, {0, 0, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key5, rg_, keysCols_, {0, 0, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_TRUE(key0.less(key6, rg_, keysCols_, {0, 0, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key0, rg_, keysCols_, {0, 1, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key0, rg_, keysCols_, {0, 2, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key3.less(key0, rg_, keysCols_, {0, 3, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key0, rg_, keysCols_, {0, 4, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key0, rg_, keysCols_, {0, 5, 0}, {0, 0, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key0, rg_, keysCols_, {0, 6, 0}, {0, 0, 0}, prevPhasThreads_));

  ASSERT_TRUE(key2.less(key1, rg_, keysCols_, {0, 2, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key3, rg_, keysCols_, {0, 2, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key4, rg_, keysCols_, {0, 2, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key2.less(key5, rg_, keysCols_, {0, 2, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key2.less(key6, rg_, keysCols_, {0, 2, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key6, rg_, keysCols_, {0, 1, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key3.less(key6, rg_, keysCols_, {0, 3, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key6, rg_, keysCols_, {0, 4, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key6, rg_, keysCols_, {0, 5, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key6, rg_, keysCols_, {0, 5, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_TRUE(key6.less(key3, rg_, keysCols_, {0, 6, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key6.less(key4, rg_, keysCols_, {0, 6, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key6.less(key5, rg_, keysCols_, {0, 6, 0}, {0, 5, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key6, rg_, keysCols_, {0, 1, 0}, {0, 6, 0}, prevPhasThreads_));
  ASSERT_TRUE(key6.less(key1, rg_, keysCols_, {0, 6, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key6.less(key2, rg_, keysCols_, {0, 6, 0}, {0, 2, 0}, prevPhasThreads_));

  ASSERT_FALSE(key1.less(key2, rg_, keysCols_, {0, 1, 0}, {0, 2, 0}, prevPhasThreads_));
  ASSERT_TRUE(key1.less(key3, rg_, keysCols_, {0, 1, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key1.less(key4, rg_, keysCols_, {0, 1, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_TRUE(key1.less(key5, rg_, keysCols_, {0, 1, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_FALSE(key1.less(key6, rg_, keysCols_, {0, 1, 0}, {0, 6, 0}, prevPhasThreads_));

  ASSERT_FALSE(key3.less(key1, rg_, keysCols_, {0, 3, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key4.less(key1, rg_, keysCols_, {0, 4, 0}, {0, 1, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key1, rg_, keysCols_, {0, 5, 0}, {0, 1, 0}, prevPhasThreads_));

  ASSERT_TRUE(key4.less(key5, rg_, keysCols_, {0, 4, 0}, {0, 5, 0}, prevPhasThreads_));
  ASSERT_TRUE(key4.less(key3, rg_, keysCols_, {0, 4, 0}, {0, 3, 0}, prevPhasThreads_));

  ASSERT_FALSE(key3.less(key4, rg_, keysCols_, {0, 3, 0}, {0, 4, 0}, prevPhasThreads_));
  ASSERT_FALSE(key5.less(key4, rg_, keysCols_, {0, 5, 0}, {0, 4, 0}, prevPhasThreads_));

  ASSERT_TRUE(key5.less(key3, rg_, keysCols_, {0, 5, 0}, {0, 3, 0}, prevPhasThreads_));
  ASSERT_TRUE(key5.less(key3, rg_, keysCols_, {0, 5, 0}, {0, 3, 0}, prevPhasThreads_));

  ASSERT_FALSE(key3.less(key5, rg_, keysCols_, {0, 3, 0}, {0, 5, 0}, prevPhasThreads_));
}

class HeapOrderByTest : public testing::Test
{
 public:
  void SetUp() override
  {
    keysCols_ = {{0, true}};
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
  size_t heapSizeHalf = 4;
  rowgroup::Row r;
  sorting::SortingThreads prevPhasThreads;
  joblist::OrderByKeysType keysAndDirections = {{0, true}};
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
  sorting::ValueRangesVector ranges(heapSizeHalf, {0, data.front().size()});
  for (size_t i = 0; i < heapSizeHalf; ++i)
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
                heapSizeHalf, ranges);
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
  size_t heapSizeHalf = 3;
  rowgroup::Row r;
  sorting::SortingThreads prevPhasThreads;
  joblist::OrderByKeysType keysAndDirections = {{0, true}};
  joblist::MemManager* mm = new joblist::MemManager;  //(&rm, sl, false, false);
  // no NULLs yet
  std::vector<std::vector<int64_t>> data{
      {3660195432, 3377000516, 3369182711, 2874400139, 2517866456, -517915385, -1950920917, -2522630870,
       -3733817126, -3891607892},
      {3396035276, 2989829828, 2938792700, 2907046279, 2508452465, 873216056, 220139688, -1886091209,
       -2996493537, -3004747259},
      {3340465022, 2029570516, 1999115580, 630267809, 149731580, -816942484, -1665500714, -2753689374,
       -3087922913, -3250034565},
  };
  sorting::ValueRangesVector ranges(heapSizeHalf, {0, data.front().size()});
  for (size_t i = 0; i < heapSizeHalf; ++i)
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
                heapSizeHalf, ranges);
  PermutationVec right = {
      PermutationType{0, 0, 0},

      PermutationType{0, 9, 0},

      PermutationType{0, 8, 0},

      PermutationType{0, 9, 2},

      PermutationType{0, 7, 0},

      PermutationType{0, 9, 1},

      PermutationType{0, 8, 2},

      sorting::HeapOrderBy::ImpossiblePermute,
  };
  for (auto r = right.begin(); auto k : h.heap())
  {
    ASSERT_EQ(k.second, *r++);
  }
}

TEST_F(HeapOrderByTest, HeapOrderBy_getTopPermuteFromHeapLarge)
{
  size_t heapSizeHalf = 13;
  rowgroup::Row r;
  sorting::SortingThreads prevPhasThreads;
  const bool isAsc = true;
  joblist::OrderByKeysType keysAndDirections = {{0, isAsc}};
  joblist::MemManager* mm = new joblist::MemManager;
  // no NULLs yet
  const size_t perThreadNumbers = 5000;
  std::mt19937 mt;
  std::vector<std::vector<int64_t>> data(heapSizeHalf, std::vector<int64_t>(perThreadNumbers, 0));
  std::vector<int64_t> examplar_data;
  ranges::for_each(data,
                   [&mt, &examplar_data](auto& v)
                   {
                     ranges::generate(v, [&mt]() { return mt() % 50000; });
                     ranges::sort(v, std::greater<int64_t>());
                     //  ranges::copy(v, std::ostream_iterator<int64_t>(std::cout, " "));
                     //  std::cout << std::endl;
                     ranges::transform(v, std::back_inserter(examplar_data), [](auto el) { return el; });
                   });
  sorting::ValueRangesVector ranges(heapSizeHalf, {0, data.front().size()});
  for (size_t i = 0; i < heapSizeHalf; ++i)
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
    ranges::generate(perm, [i, it]() mutable { return PermutationType{0, it++, i}; });
    ranges::for_each(data[i],
                     [&, rg, r, rowSize, perm](const int64_t x) mutable
                     {
                       r.setIntField<8>(x, 0);
                       r.nextRow(rowSize);
                     });
    rg.setRowCount(data[i].size());
    prevPhasThreads.emplace_back(new PDQOrderBy());
    // добавить permutations вида {0,1,2...}
    prevPhasThreads.back()->getRGDatas().push_back(rgData);
    prevPhasThreads.back()->getMutPermutation().swap(perm);
  }
  HeapOrderBy h(rg_, keysAndDirections, 0, std::numeric_limits<size_t>::max(), mm, 1, prevPhasThreads,
                heapSizeHalf, ranges);

  PermutationType p;
  std::vector<int64_t> values;
  [[maybe_unused]] size_t i = 0;
  while ((p = h.getTopPermuteFromHeap(h.heapMut(), prevPhasThreads)) !=
         sorting::HeapOrderBy::ImpossiblePermute)
  {
    // std::cout << " perm threadID " << p.threadID << " rowId " << p.rowID;
    ASSERT_EQ(0UL, p.rgdataID);
    rg_.setData(&rgDatas_[p.threadID]);
    int64_t v = rg_.getColumnValue<execplan::CalpontSystemCatalog::BIGINT, int64_t, int64_t>(0, p.rowID);
    // std::cout << " i " << i++ << " v " << v << std::endl;
    // std::cout << " heapToString " << h.heapToString(prevPhasThreads) << std::endl;
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
  ASSERT_EQ(values.size(), perThreadNumbers * heapSizeHalf);
  ranges::sort(examplar_data);
  ASSERT_THAT(values, Eq(examplar_data));
}

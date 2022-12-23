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

#include "dbcon/joblist/heaporderby.h"
#include "rowgroup.h"

using namespace std;
using namespace testing;
using namespace sorting;

// using KeyType = KeyType;
using RGFieldsType = std::vector<uint32_t>;

// integral type, col type, NULL value, socket numbers
class KeyTypeTest : public testing::Test
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

TEST_F(KeyTypeTest, KeyTypeCtor)
{
  uint8_t* expected = new uint8_t[rg_.getOffsets().back() - 2 + 1];
  for (size_t i = 0; i < rowgroup::rgCommonSize; ++i)
  {
    // std::cout << "value " << i << std::endl;
    auto key = KeyType(rg_, keysCols_, {0, i, 0});
    if (i != 42)
    {
      expected[0] = 1;
      int64_t* v = reinterpret_cast<int64_t*>(&expected[1]);
      *v = (i > 4000) ? -i : i;
      ASSERT_FALSE(key.key() == nullptr);
      ASSERT_EQ(memcmp(expected, key.key(), 9), 0);
    }
    else
    {
      expected[0] = 0;
      ASSERT_EQ(memcmp(expected, key.key(), 1), 0);
    }
  }

  auto key1 = KeyType(rg_, keysCols_, {0, 42, 0});
  auto key2 = KeyType(rg_, keysCols_, {0, 42, 0});
  ASSERT_FALSE(key1.less(key2, rg_, keysCols_));
  ASSERT_FALSE(key2.less(key1, rg_, keysCols_));

  auto key3 = KeyType(rg_, keysCols_, {0, 4, 0});
  auto key4 = KeyType(rg_, keysCols_, {0, 5, 0});
  ASSERT_TRUE(key3.less(key4, rg_, keysCols_));
  ASSERT_FALSE(key4.less(key3, rg_, keysCols_));

  auto key5 = KeyType(rg_, keysCols_, {0, 4001, 0});
  auto key6 = KeyType(rg_, keysCols_, {0, 4002, 0});
  ASSERT_FALSE(key5.less(key6, rg_, keysCols_));
  ASSERT_TRUE(key6.less(key5, rg_, keysCols_));

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
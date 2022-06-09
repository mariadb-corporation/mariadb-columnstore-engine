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

#if defined(__x86_64__)
#include <iostream>
#include <gtest/gtest.h>

#include "simd_sse.h"
#include "datatypes/mcs_datatype.h"
#include "datatypes/mcs_int128.h"

using namespace std;

template <typename T>
class SimdProcessorTypedTest : public testing::Test {
  using IntegralType = T;
  public:


  void SetUp() override
  {
  }
};

using SimdProcessor128TypedTestTypes = ::testing::Types<uint64_t, uint32_t, uint16_t, uint8_t, int64_t, int32_t, int16_t, int8_t>;
TYPED_TEST_SUITE(SimdProcessorTypedTest, SimdProcessor128TypedTestTypes);

TYPED_TEST(SimdProcessorTypedTest, SimdFilterProcessor_simd128)
{
  using Proc = typename simd::SimdFilterProcessor<simd::vi128_wr, TypeParam>;
  using SimdType = typename Proc::SimdType;
  constexpr static simd::MT allTrue = 0xFFFF;
  constexpr static simd::MT allFalse = 0x0;
  Proc proc;
  SimdType lhs = proc.loadValue((TypeParam)-2);
  SimdType rhs = proc.loadValue((TypeParam)-3);
  EXPECT_GT((uint64_t)-2LL, (uint64_t)-3LL);
  EXPECT_EQ(proc.cmpGe(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpGt(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpGe(rhs, lhs), allFalse);
  EXPECT_EQ(proc.cmpGt(rhs, lhs), allFalse);
  EXPECT_EQ(proc.cmpLe(rhs, lhs), allTrue);
  EXPECT_EQ(proc.cmpLt(rhs, lhs), allTrue);
  EXPECT_EQ(proc.cmpLe(lhs, rhs), allFalse);
  EXPECT_EQ(proc.cmpLt(lhs, rhs), allFalse);
  EXPECT_EQ(proc.cmpEq(rhs, lhs), allFalse);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), allTrue);
  lhs = proc.loadValue((TypeParam)-3);
  EXPECT_EQ(proc.cmpEq(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), allFalse);
}
#endif
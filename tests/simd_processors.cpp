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

class SimdProcessorTest : public testing::Test {
  public:

  void SetUp() override
  {
  }
};

TEST_F(SimdProcessorTest, SimdFilterProcessor_simd128_U64)
{
  constexpr const uint8_t W = 8;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  using UT = datatypes::make_unsigned<IntegralType>::type;
  using Proc = typename simd::SimdFilterProcessor<simd::vi128_wr, UT>;
  using SimdType = typename Proc::SimdType;
  constexpr static simd::MT allTrue = 0xFFFF;
  constexpr static simd::MT allFalse = 0x0;
  Proc proc;
  SimdType lhs = proc.loadValue((UT)-2);
  SimdType rhs = proc.loadValue((UT)-3);
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
  lhs = proc.loadValue((UT)-3);
  EXPECT_EQ(proc.cmpEq(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), allFalse);
}

TEST_F(SimdProcessorTest, SimdFilterProcessor_simd128_U32)
{
  constexpr const uint8_t W = 4;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  using UT = datatypes::make_unsigned<IntegralType>::type;
  using Proc = typename simd::SimdFilterProcessor<simd::vi128_wr, UT>;
  using SimdType = typename Proc::SimdType;
  constexpr static simd::MT allTrue = 0xFFFF;
  constexpr static simd::MT allFalse = 0x0;
  Proc proc;
  SimdType lhs = proc.loadValue((UT)-2);
  SimdType rhs = proc.loadValue((UT)-3);
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
  lhs = proc.loadValue((UT)-3);
  EXPECT_EQ(proc.cmpEq(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), allFalse);
}

TEST_F(SimdProcessorTest, SimdFilterProcessor_simd128_U16)
{
  constexpr const uint8_t W = 2;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  using UT = datatypes::make_unsigned<IntegralType>::type;
  using Proc = typename simd::SimdFilterProcessor<simd::vi128_wr, UT>;
  using SimdType = typename Proc::SimdType;
  constexpr static simd::MT allTrue = 0xFFFF;
  constexpr static simd::MT allFalse = 0x0;
  Proc proc;
  SimdType lhs = proc.loadValue((UT)-2);
  SimdType rhs = proc.loadValue((UT)-3);
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
  lhs = proc.loadValue((UT)-3);
  EXPECT_EQ(proc.cmpEq(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), allFalse);
}

TEST_F(SimdProcessorTest, SimdFilterProcessor_simd128_U8)
{
  constexpr const uint8_t W = 1;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  using UT = datatypes::make_unsigned<IntegralType>::type;
  using Proc = typename simd::SimdFilterProcessor<simd::vi128_wr, UT>;
  using SimdType = typename Proc::SimdType;
  constexpr static simd::MT allTrue = 0xFFFF;
  constexpr static simd::MT allFalse = 0x0;
  Proc proc;
  SimdType lhs = proc.loadValue((UT)-2);
  SimdType rhs = proc.loadValue((UT)-3);
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
  lhs = proc.loadValue((UT)-3);
  EXPECT_EQ(proc.cmpEq(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), allFalse);
}

TEST_F(SimdProcessorTest, SimdFilterProcessor_simd128_S64)
{
  constexpr const uint8_t W = 8;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  using Proc = typename simd::SimdFilterProcessor<simd::vi128_wr, IntegralType>;
  using SimdType = typename Proc::SimdType;
  constexpr static simd::MT allTrue = 0xFFFF;
  constexpr static simd::MT allFalse = 0x0;
  Proc proc;
  SimdType lhs = proc.loadValue((IntegralType)-2);
  SimdType rhs = proc.loadValue((IntegralType)-3);
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
  lhs = proc.loadValue((IntegralType)-3);
  EXPECT_EQ(proc.cmpEq(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), allFalse);
}

TEST_F(SimdProcessorTest, SimdFilterProcessor_simd128_S32)
{
  constexpr const uint8_t W = 4;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  using Proc = typename simd::SimdFilterProcessor<simd::vi128_wr, IntegralType>;
  using SimdType = typename Proc::SimdType;
  constexpr static simd::MT allTrue = 0xFFFF;
  constexpr static simd::MT allFalse = 0x0;
  Proc proc;
  SimdType lhs = proc.loadValue((IntegralType)-2);
  SimdType rhs = proc.loadValue((IntegralType)-3);
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
  lhs = proc.loadValue((IntegralType)-3);
  EXPECT_EQ(proc.cmpEq(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), allFalse);
}

TEST_F(SimdProcessorTest, SimdFilterProcessor_simd128_S16)
{
  constexpr const uint8_t W = 2;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  using Proc = typename simd::SimdFilterProcessor<simd::vi128_wr, IntegralType>;
  using SimdType = typename Proc::SimdType;
  constexpr static simd::MT allTrue = 0xFFFF;
  constexpr static simd::MT allFalse = 0x0;
  Proc proc;
  SimdType lhs = proc.loadValue((IntegralType)-2);
  SimdType rhs = proc.loadValue((IntegralType)-3);
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
  lhs = proc.loadValue((IntegralType)-3);
  EXPECT_EQ(proc.cmpEq(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), allFalse);
}

TEST_F(SimdProcessorTest, SimdFilterProcessor_simd128_S8)
{
  constexpr const uint8_t W = 1;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  using Proc = typename simd::SimdFilterProcessor<simd::vi128_wr, IntegralType>;
  using SimdType = typename Proc::SimdType;
  constexpr static simd::MT allTrue = 0xFFFF;
  constexpr static simd::MT allFalse = 0x0;
  Proc proc;
  SimdType lhs = proc.loadValue((IntegralType)-2);
  SimdType rhs = proc.loadValue((IntegralType)-3);
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
  lhs = proc.loadValue((IntegralType)-3);
  EXPECT_EQ(proc.cmpEq(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), allFalse);
}
#endif
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

#include <iostream>
#include <gtest/gtest.h>

#include "simd_sse.h"
#include "utils/common/columnwidth.h"
#include "datatypes/mcs_datatype.h"
#include "datatypes/mcs_int128.h"
#include "stats.h"
#include "primitives/linux-port/primitiveprocessor.h"
#include "col1block.h"
#include "col2block.h"
#include "col4block.h"
#include "col8block.h"
#include "col16block.h"
#include "col_float_block.h"
#include "col_double_block.h"
#include "col_neg_float.h"
#include "col_neg_double.h"

using namespace primitives;
using namespace datatypes;
using namespace std;

// If a test crashes check if there is a corresponding literal binary array in
// readBlockFromLiteralArray.

class SimdProcessorTest : public ::testing::Test
{
 protected:

  void SetUp() override
  {
  }
};

TEST_F(SimdProcessorTest, SimdFilterProcessor_simd128_U64)
{
  constexpr const uint8_t W = 8;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  using UT = datatypes::make_unsigned<IntegralType>::type;
  using Proc = simd::SimdFilterProcessor<simd::vi128_wr, UT>;
  using SimdType = Proc::SimdType;
  Proc proc;
  SimdType lhs = proc.loadValue(-2LL);
  SimdType rhs = proc.loadValue(-3LL);
  EXPECT_GT((uint64_t)-2LL, (uint64_t)-3LL);
  EXPECT_EQ(proc.cmpGe(lhs, rhs), 0xFFFF);
  EXPECT_EQ(proc.cmpGt(lhs, rhs), 0xFFFF);
  EXPECT_EQ(proc.cmpGe(rhs, lhs), 0x0);
  EXPECT_EQ(proc.cmpGt(rhs, lhs), 0x0);
  EXPECT_EQ(proc.cmpLe(rhs, lhs), 0xFFFF);
  EXPECT_EQ(proc.cmpLt(rhs, lhs), 0xFFFF);
  EXPECT_EQ(proc.cmpLe(lhs, rhs), 0x0);
  EXPECT_EQ(proc.cmpLt(lhs, rhs), 0x0);
  EXPECT_EQ(proc.cmpEq(rhs, lhs), 0x0);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), 0xFFFF);
  lhs = proc.loadValue(-3LL);
  EXPECT_EQ(proc.cmpEq(lhs, rhs), 0xFFFF);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), 0x0);
}

TEST_F(SimdProcessorTest, SimdFilterProcessor_simd128_U32)
{
  constexpr const uint8_t W = 4;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  using UT = datatypes::make_unsigned<IntegralType>::type;
  using Proc = simd::SimdFilterProcessor<simd::vi128_wr, UT>;
  using SimdType = Proc::SimdType;
  Proc proc;
  SimdType lhs = proc.loadValue(-2LL);
  SimdType rhs = proc.loadValue(-3LL);
  EXPECT_GT((uint64_t)-2LL, (uint64_t)-3LL);
  EXPECT_EQ(proc.cmpGe(lhs, rhs), 0xFFFF);
  EXPECT_EQ(proc.cmpGt(lhs, rhs), 0xFFFF);
  EXPECT_EQ(proc.cmpGe(rhs, lhs), 0x0);
  EXPECT_EQ(proc.cmpGt(rhs, lhs), 0x0);
  EXPECT_EQ(proc.cmpLe(rhs, lhs), 0xFFFF);
  EXPECT_EQ(proc.cmpLt(rhs, lhs), 0xFFFF);
  EXPECT_EQ(proc.cmpLe(lhs, rhs), 0x0);
  EXPECT_EQ(proc.cmpLt(lhs, rhs), 0x0);
  EXPECT_EQ(proc.cmpEq(rhs, lhs), 0x0);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), 0xFFFF);
  lhs = proc.loadValue(-3LL);
  EXPECT_EQ(proc.cmpEq(lhs, rhs), 0xFFFF);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), 0x0);
}
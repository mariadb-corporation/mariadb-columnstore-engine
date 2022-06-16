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
#include "datatypes/mcs_datatype.h"
#include "datatypes/mcs_int128.h"

#if defined(__x86_64__)
#include "simd_sse.h"

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
#ifdef __aarch64__
#include "simd_arm.h"


using namespace std;

template <typename T>
class SimdProcessorTypedTest : public testing::Test
{
  using IntegralType = T;

 public:
  void SetUp() override
  {
  }
};

using SimdProcessor128TypedTestTypes =
    ::testing::Types<uint64_t, uint32_t, uint16_t, uint8_t, int64_t, int32_t, int16_t, int8_t>;
TYPED_TEST_SUITE(SimdProcessorTypedTest, SimdProcessor128TypedTestTypes);

TYPED_TEST(SimdProcessorTypedTest, SimdFilterProcessor_simd128)
{
  using Proc = typename simd::SimdFilterProcessor<typename simd::TypeToVecWrapperType<TypeParam>::WrapperType, TypeParam>;
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

  lhs = rhs = proc.loadValue((TypeParam)0);
  EXPECT_EQ(proc.cmpGe(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpGt(lhs, rhs), allFalse);
  EXPECT_EQ(proc.cmpGe(rhs, lhs), allTrue);
  EXPECT_EQ(proc.cmpGt(rhs, lhs), allFalse);
  EXPECT_EQ(proc.cmpLe(rhs, lhs), allTrue);
  EXPECT_EQ(proc.cmpLt(rhs, lhs), allFalse);
  EXPECT_EQ(proc.cmpLe(lhs, rhs), allTrue);
  EXPECT_EQ(proc.cmpLt(lhs, rhs), allFalse);
  EXPECT_EQ(proc.cmpEq(rhs, lhs), allTrue);
  EXPECT_EQ(proc.cmpNe(rhs, lhs), allFalse);
}
TEST(SimdProcessorTest,Int8)
{
    using Proc = typename simd::SimdFilterProcessor<typename simd::TypeToVecWrapperType<int8_t>::WrapperType, int8_t>;
    using SimdType = typename Proc::SimdType;
    Proc proc;
    simd::MT expect = 0x0;
    int8_t l[16]{0, 1, 2, 5, 4, 3, 8, 5, 6, 10, 58, 2, 32, 41, 2, 5};
    int8_t r[16]{0, 1, 8, 35, 24, 13, 8, 25, 16, 10, 58, 2, 32, 41, 2, 5};
    SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
    SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
    for (int i = 0; i < 16; i++)
      if (l[i] > r[i])
        expect |= 1 << i;
    EXPECT_EQ(proc.cmpGt(lhs, rhs), expect);
    EXPECT_EQ(proc.cmpLe(lhs, rhs),(simd::MT) ~expect);

    expect = 0x0;
    for (int i = 0; i < 16; i++)
      if (l[i] == r[i])
        expect |= 1 << i;
    EXPECT_EQ(proc.cmpEq(lhs, rhs), expect);
    EXPECT_EQ(proc.cmpNe(lhs, rhs), (simd::MT)~expect);

    expect = 0x0;
    for (int i = 0; i < 16; i++)
      if (l[i] < r[i])
        expect |= 1 << i;
    EXPECT_EQ(proc.cmpLt(lhs, rhs), expect);
    EXPECT_EQ(proc.cmpGe(lhs, rhs), (simd::MT)~expect);
}
TEST(SimdProcessorTest, Uint8)
{
  using Proc =
      typename simd::SimdFilterProcessor<typename simd::TypeToVecWrapperType<uint8_t>::WrapperType, uint8_t>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  simd::MT expect = 0x0;
  uint8_t l[16]{0, 1, 2, 5, 4, 3, 8, 5, 6, 10, 5, 2, 32, 41, 2, 5};
  uint8_t r[16]{0, 1, 8, 35, 24, 13, 8, 25, 16, 10, 58, 2, 32, 41, 2, 5};
  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  for (int i = 0; i < 16; i++)
    if (l[i] > r[i])
      expect |= 1 << i;
  EXPECT_EQ(proc.cmpGt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpLe(lhs, rhs),(simd::MT) ~expect);

  expect = 0x0;
  for (int i = 0; i < 16; i++)
    if (l[i] == r[i])
      expect |= 1 << i;
  EXPECT_EQ(proc.cmpEq(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpNe(lhs, rhs),(simd::MT) ~expect);

  expect = 0x0;
  for (int i = 0; i < 16; i++)
    if (l[i] < r[i])
      expect |= 1 << i;
  EXPECT_EQ(proc.cmpLt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpGe(lhs, rhs),(simd::MT) ~expect);
}
TEST(SimdProcessorTest, Int16)
{
  using Proc =
      typename simd::SimdFilterProcessor<typename simd::TypeToVecWrapperType<int16_t>::WrapperType, int16_t>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  simd::MT expect = 0x0;
  int16_t l[8]{0, 1, 2, -5, 4, 3, -8, 200};
  int16_t r[8]{0, 105, -8, 35, 24, 13, 8};
  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  for (int i = 0; i < 8; i++)
    if (l[i] > r[i])
      expect |= 3 << i * 2;
  EXPECT_EQ(proc.cmpGt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpLe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 8; i++)
    if (l[i] == r[i])
      expect |= 3 << i * 2;
  EXPECT_EQ(proc.cmpEq(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpNe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 8; i++)
    if (l[i] < r[i])
      expect |= 3 << i * 2;
  EXPECT_EQ(proc.cmpLt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpGe(lhs, rhs), (simd::MT)~expect);
}
TEST(SimdProcessorTest, Uint16)
{
  using Proc = typename simd::SimdFilterProcessor<typename simd::TypeToVecWrapperType<uint16_t>::WrapperType,
                                                  uint16_t>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  simd::MT expect = 0x0;
  uint16_t l[8]{0, 1, 2, 5, 4, 3, 8, 5};
  uint16_t r[8]{0, 1, 8, 35, 24, 13, 8};
  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  for (int i = 0; i < 8; i++)
    if (l[i] > r[i])
      expect |= 3 << i*2;
  EXPECT_EQ(proc.cmpGt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpLe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 8; i++)
    if (l[i] == r[i])
      expect |= 3 << i * 2;
  EXPECT_EQ(proc.cmpEq(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpNe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 8; i++)
    if (l[i] < r[i])
      expect |= 3 << i * 2;
  EXPECT_EQ(proc.cmpLt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpGe(lhs, rhs), (simd::MT)~expect);
}

TEST(SimdProcessorTest, Int32)
{
  using Proc =
      typename simd::SimdFilterProcessor<typename simd::TypeToVecWrapperType<int32_t>::WrapperType, int32_t>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  simd::MT expect = 0x0;
  int32_t l[8]{0, 1, 2, -5};
  int32_t r[8]{0, 105, -8,54333};
  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  for (int i = 0; i < 4; i++)
    if (l[i] > r[i])
      expect |= 15 << i * 4;
  EXPECT_EQ(proc.cmpGt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpLe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 4; i++)
    if (l[i] == r[i])
      expect |= 15 << i * 4;
  EXPECT_EQ(proc.cmpEq(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpNe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 4; i++)
    if (l[i] < r[i])
      expect |= 15 << i * 4;
  EXPECT_EQ(proc.cmpLt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpGe(lhs, rhs), (simd::MT)~expect);
}
TEST(SimdProcessorTest, Uint32)
{
  using Proc = typename simd::SimdFilterProcessor<typename simd::TypeToVecWrapperType<uint32_t>::WrapperType,
                                                  uint32_t>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  simd::MT expect = 0x0;
  uint32_t l[4]{0, 1002, 2, 514};
  uint32_t r[4]{2, 1, 80555, 35};
  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  for (int i = 0; i < 4; i++)
    if (l[i] > r[i])
      expect |= 15 << i * 4;
  EXPECT_EQ(proc.cmpGt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpLe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 4; i++)
    if (l[i] == r[i])
      expect |= 15 << i * 4;
  EXPECT_EQ(proc.cmpEq(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpNe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 4; i++)
    if (l[i] < r[i])
      expect |= 15 << i * 4;
  EXPECT_EQ(proc.cmpLt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpGe(lhs, rhs), (simd::MT)~expect);
}
TEST(SimdProcessorTest, Int64)
{
  using Proc =
      typename simd::SimdFilterProcessor<typename simd::TypeToVecWrapperType<int64_t>::WrapperType, int64_t>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  simd::MT expect = 0x0;
  int64_t l[2]{-5, 122020};
  int64_t r[2]{0, 105};
  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  for (int i = 0; i < 2; i++)
    if (l[i] > r[i])
      expect |= 0xFF << i * 8;
  EXPECT_EQ(proc.cmpGt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpLe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 2; i++)
    if (l[i] == r[i])
      expect |= 0xFF << i * 8;
  EXPECT_EQ(proc.cmpEq(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpNe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 2; i++)
    if (l[i] < r[i])
      expect |= 0xFF << i * 8;
  EXPECT_EQ(proc.cmpLt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpGe(lhs, rhs), (simd::MT)~expect);
}
TEST(SimdProcessorTest, Uint64)
{
  using Proc = typename simd::SimdFilterProcessor<typename simd::TypeToVecWrapperType<uint64_t>::WrapperType,
                                                  uint64_t>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  simd::MT expect = 0x0;
  uint64_t l[2]{822, 1002};
  uint64_t r[2]{2, 1};
  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  for (int i = 0; i < 2; i++)
    if (l[i] > r[i])
      expect |= 0xFF << i * 8;
  EXPECT_EQ(proc.cmpGt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpLe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 2; i++)
    if (l[i] == r[i])
      expect |= 0xFF << i * 8;
  EXPECT_EQ(proc.cmpEq(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpNe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 2; i++)
    if (l[i] < r[i])
      expect |= 0xFF << i * 8;
  EXPECT_EQ(proc.cmpLt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpGe(lhs, rhs), (simd::MT)~expect);
}
TEST(SimdProcessorTest, Float64)
{
  using Proc = typename simd::SimdFilterProcessor<simd::vi128d_wr, double>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  simd::MT expect = 0x0;
  float64_t l[2]{-5.0, 12.5620};
  float64_t r[2]{2.9, 1};
  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  for (int i = 0; i < 2; i++)
    if (l[i] > r[i])
      expect |= 0xFF << i * 8;
  EXPECT_EQ(proc.cmpGt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpLe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 2; i++)
    if (l[i] == r[i])
      expect |= 0xFF << i * 8;
  EXPECT_EQ(proc.cmpEq(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpNe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 2; i++)
    if (l[i] < r[i])
      expect |= 0xFF << i * 8;
  EXPECT_EQ(proc.cmpLt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpGe(lhs, rhs), (simd::MT)~expect);
}
TEST(SimdProcessorTest, Float32)
{
  using Proc = typename simd::SimdFilterProcessor<simd::vi128f_wr, float>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  simd::MT expect = 0x0;
  float32_t l[4]{82, 102,-5.6,9.5};
  float32_t r[4]{2.0, 1,-5.7,6};
  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  for (int i = 0; i < 4; i++)
    if (l[i] > r[i])
      expect |= 15 << i * 4;
  EXPECT_EQ(proc.cmpGt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpLe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 4; i++)
    if (l[i] == r[i])
      expect |= 15 << i * 4;
  EXPECT_EQ(proc.cmpEq(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpNe(lhs, rhs), (simd::MT)~expect);

  expect = 0x0;
  for (int i = 0; i < 4; i++)
    if (l[i] < r[i])
      expect |= 15 << i * 4;
  EXPECT_EQ(proc.cmpLt(lhs, rhs), expect);
  EXPECT_EQ(proc.cmpGe(lhs, rhs), (simd::MT)~expect);
}
#endif

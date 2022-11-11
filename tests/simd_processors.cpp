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

#include <cstdint>
#include <functional>
#include <iostream>
#include <type_traits>
#include <gtest/gtest.h>
#include "datatypes/mcs_datatype.h"
#include "datatypes/mcs_int128.h"
#include "simd_sse.h"
#include "simd_arm.h"
#if defined(__x86_64__)
#define TESTS_USING_SSE 1
using float64_t = double;
using float32_t = float;
#endif
#ifdef __aarch64__
#define TESTS_USING_ARM 1
#endif

using namespace std;
#if defined(__x86_64__) || __aarch64__
template <typename T>
class SimdProcessorTypedTest : public testing::Test
{
 public:
  using IntegralType = T;
#if TESTS_USING_SSE
  using SimdType =
      std::conditional_t<std::is_same<T, float>::value, simd::vi128f_wr,
                         std::conditional_t<std::is_same<T, double>::value, simd::vi128d_wr, simd::vi128_wr>>;
  using Proc = typename simd::SimdFilterProcessor<SimdType, T>;
#else
  using Proc = typename simd::SimdFilterProcessor<typename simd::TypeToVecWrapperType<T>::WrapperType, T>;
#endif
  void SetUp() override
  {
  }
};

using SimdProcessor128TypedTestTypes =
    ::testing::Types<uint64_t, uint32_t, uint16_t, uint8_t, int64_t, int32_t, int16_t, int8_t>;
TYPED_TEST_SUITE(SimdProcessorTypedTest, SimdProcessor128TypedTestTypes);

TYPED_TEST(SimdProcessorTypedTest, SimdFilterProcessor_simd128)
{
  using Proc = typename SimdProcessorTypedTest<TypeParam>::Proc;

  auto cmpEqFunctor = [](typename Proc::MaskType left, typename Proc::MaskType right)
  { return !memcmp((void*)(&left), (void*)(&right), sizeof(typename Proc::MaskType)); };

  using SimdType = typename Proc::SimdType;
  Proc proc;
  const typename Proc::MaskType allTrue = proc.trueMask();
  const typename Proc::MaskType allFalse = proc.falseMask();

  SimdType lhs = proc.loadValue((TypeParam)-2);
  SimdType rhs = proc.loadValue((TypeParam)-3);
  EXPECT_GT((uint64_t)-2LL, (uint64_t)-3LL);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(lhs, rhs), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(lhs, rhs), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(rhs, lhs), allFalse));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(rhs, lhs), allFalse));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(rhs, lhs), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(rhs, lhs), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(lhs, rhs), allFalse));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(lhs, rhs), allFalse));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(rhs, lhs), allFalse));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(rhs, lhs), allTrue));
  lhs = proc.loadValue((TypeParam)-3);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(lhs, rhs), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(rhs, lhs), allFalse));

  lhs = rhs = proc.loadValue((TypeParam)0);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(lhs, rhs), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(lhs, rhs), allFalse));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(rhs, lhs), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(rhs, lhs), allFalse));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(rhs, lhs), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(rhs, lhs), allFalse));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(lhs, rhs), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(lhs, rhs), allFalse));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(rhs, lhs), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(rhs, lhs), allFalse));
}

template <typename IntegralType, typename ResultType, int VecSize>
ResultType bitMaskProducerT(const IntegralType* l, const IntegralType* r,
                            std::function<bool(IntegralType, IntegralType)> cmp, const bool printOut = false)
{
  uint64_t allOnes = 0xFFULL;
  for (size_t i = 1; i < sizeof(IntegralType); ++i)
  {
    allOnes |= 0xFFULL << (i * 8);
  }
  ResultType result = {0x0, 0x0};

  uint64_t* resultPtr = reinterpret_cast<uint64_t*>(&result);
  for (size_t i = 0; i < VecSize >> 1; ++i)
  {
    if (cmp(l[i], r[i]))
    {
      if (printOut)
      {
        uint64_t pLeft = l[i];
        uint pRight = r[i];
        std::cout << "i " << i << " l " << cmp.target_type().name() << " r " << pLeft << " " << pRight
                  << std::endl;
      }
      resultPtr[0] |= allOnes << i * sizeof(IntegralType) * 8;
    }
  }
  for (size_t i = VecSize >> 1; i < VecSize; ++i)
  {
    if (cmp(l[i], r[i]))
    {
      if (printOut)
      {
        uint64_t pLeft = l[i];
        uint pRight = r[i];
        std::cout << "i " << i << " l " << cmp.target_type().name() << " r " << pLeft << " " << pRight
                  << std::endl;
      }
      resultPtr[1] |= allOnes << (i - (VecSize >> 1)) * sizeof(IntegralType) * 8;
    }
  }
  return result;
};

TEST(SimdProcessorTest, Int8)
{
  using IntegralType = int8_t;
  IntegralType l[16]{0, 1, 2, 5, 4, 3, 8, 5, 6, 10, 58, 2, 32, 41, 2, 5};
  IntegralType r[16]{0, 1, 8, 35, 24, 13, 8, 25, 16, 10, 58, 2, 32, 41, 2, 5};
  IntegralType minlr[16]{0, 1, 2, 5, 4, 3, 8, 5, 6, 10, 58, 2, 32, 41, 2, 5};
  IntegralType maxlr[16]{0, 1, 8, 35, 24, 13, 8, 25, 16, 10, 58, 2, 32, 41, 2, 5};
  using IntegralType = int8_t;
  using Proc = typename SimdProcessorTypedTest<IntegralType>::Proc;
  constexpr const size_t VecSize = Proc::vecByteSize;
  using SimdType = typename Proc::SimdType;
  auto cmpEqFunctor = [](typename Proc::MaskType left, typename Proc::MaskType right)
  { return !memcmp((void*)(&left), (void*)(&right), sizeof(typename Proc::MaskType)); };
  auto bitMaskProducer = bitMaskProducerT<IntegralType, Proc::MaskType, VecSize>;
  Proc proc;
  const typename Proc::MaskType allTrue = proc.trueMask();
  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  SimdType min = proc.loadFrom(reinterpret_cast<char*>(minlr));
  SimdType max = proc.loadFrom(reinterpret_cast<char*>(maxlr));
  Proc::MaskType expectGt = bitMaskProducer(l, r, std::greater<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(lhs, rhs), expectGt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(lhs, rhs), ~expectGt));
  SimdType testmax = proc.max(lhs, rhs);
  SimdType testmin = proc.min(lhs, rhs);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmax, max), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmin, min), allTrue));

  Proc::MaskType expectEq = bitMaskProducer(l, r, std::equal_to<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(lhs, rhs), expectEq));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(lhs, rhs), ~expectEq));
  Proc::MaskType expectLt = bitMaskProducer(l, r, std::less<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(lhs, rhs), expectLt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(lhs, rhs), ~expectLt));
}

TEST(SimdProcessorTest, Uint8)
{
  using IntegralType = uint8_t;
  IntegralType l[16]{0, 1, 2, 5, 4, 3, 8, 5, 6, 10, 5, 2, 32, 41, 2, 5};
  IntegralType r[16]{0, 1, 8, 35, 24, 13, 8, 25, 16, 10, 58, 2, 32, 41, 2, 5};
  IntegralType minlr[16]{0, 1, 2, 5, 4, 3, 8, 5, 6, 10, 5, 2, 32, 41, 2, 5};
  IntegralType maxlr[16]{0, 1, 8, 35, 24, 13, 8, 25, 16, 10, 58, 2, 32, 41, 2, 5};
  using Proc = typename SimdProcessorTypedTest<IntegralType>::Proc;
  constexpr const size_t VecSize = Proc::vecByteSize / sizeof(IntegralType);
  using SimdType = typename Proc::SimdType;
  auto cmpEqFunctor = [](typename Proc::MaskType left, typename Proc::MaskType right)
  { return !memcmp((void*)(&left), (void*)(&right), sizeof(typename Proc::MaskType)); };
  auto bitMaskProducer = bitMaskProducerT<IntegralType, Proc::MaskType, VecSize>;

  using Proc = typename SimdProcessorTypedTest<uint8_t>::Proc;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  const Proc::MaskType allTrue = proc.trueMask();

  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  SimdType min = proc.loadFrom(reinterpret_cast<char*>(minlr));
  SimdType max = proc.loadFrom(reinterpret_cast<char*>(maxlr));

  Proc::MaskType expectGt = bitMaskProducer(l, r, std::greater<IntegralType>(), true);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(lhs, rhs), expectGt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(lhs, rhs), ~expectGt));
  SimdType testmax = proc.max(lhs, rhs);
  SimdType testmin = proc.min(lhs, rhs);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmax, max), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmin, min), allTrue));

  Proc::MaskType expectEq = bitMaskProducer(l, r, std::equal_to<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(lhs, rhs), expectEq));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(lhs, rhs), ~expectEq));

  Proc::MaskType expectLt = bitMaskProducer(l, r, std::less<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(lhs, rhs), expectLt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(lhs, rhs), ~expectLt));
}

TEST(SimdProcessorTest, Int16)
{
  using IntegralType = int16_t;
  IntegralType l[8]{0, 1, 2, -5, 4, 3, -8, 200};
  IntegralType r[8]{0, 105, -8, 35, 24, 13, 8, 100};
  IntegralType minlr[8]{0, 1, -8, -5, 4, 3, -8, 100};
  IntegralType maxlr[8]{0, 105, 2, 35, 24, 13, 8, 200};
  using Proc = typename SimdProcessorTypedTest<int16_t>::Proc;
  constexpr const size_t VecSize = Proc::vecByteSize / sizeof(IntegralType);
  auto cmpEqFunctor = [](typename Proc::MaskType left, typename Proc::MaskType right)
  { return !memcmp((void*)(&left), (void*)(&right), sizeof(typename Proc::MaskType)); };
  auto bitMaskProducer = bitMaskProducerT<IntegralType, Proc::MaskType, VecSize>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  const Proc::MaskType allTrue = proc.trueMask();

  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  SimdType min = proc.loadFrom(reinterpret_cast<char*>(minlr));
  SimdType max = proc.loadFrom(reinterpret_cast<char*>(maxlr));

  Proc::MaskType expectGt = bitMaskProducer(l, r, std::greater<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(lhs, rhs), expectGt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(lhs, rhs), ~expectGt));
  SimdType testmax = proc.max(lhs, rhs);
  SimdType testmin = proc.min(lhs, rhs);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmax, max), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmin, min), allTrue));

  Proc::MaskType expectEq = bitMaskProducer(l, r, std::equal_to<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(lhs, rhs), expectEq));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(lhs, rhs), ~expectEq));

  Proc::MaskType expectLt = bitMaskProducer(l, r, std::less<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(lhs, rhs), expectLt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(lhs, rhs), ~expectLt));
}
TEST(SimdProcessorTest, Uint16)
{
  using IntegralType = uint16_t;
  IntegralType l[8]{0, 1, 2, 5, 4, 3, 8, 5};
  IntegralType r[8]{0, 1, 8, 35, 24, 13, 8, 17};
  IntegralType minlr[8]{0, 1, 2, 5, 4, 3, 8, 5};
  IntegralType maxlr[8]{0, 1, 8, 35, 24, 13, 8, 17};

  using Proc = typename SimdProcessorTypedTest<int16_t>::Proc;
  constexpr const size_t VecSize = Proc::vecByteSize / sizeof(IntegralType);
  auto cmpEqFunctor = [](typename Proc::MaskType left, typename Proc::MaskType right)
  { return !memcmp((void*)(&left), (void*)(&right), sizeof(typename Proc::MaskType)); };
  auto bitMaskProducer = bitMaskProducerT<IntegralType, Proc::MaskType, VecSize>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  const Proc::MaskType allTrue = proc.trueMask();

  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  SimdType min = proc.loadFrom(reinterpret_cast<char*>(minlr));
  SimdType max = proc.loadFrom(reinterpret_cast<char*>(maxlr));

  Proc::MaskType expectGt = bitMaskProducer(l, r, std::greater<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(lhs, rhs), expectGt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(lhs, rhs), ~expectGt));
  SimdType testmax = proc.max(lhs, rhs);
  SimdType testmin = proc.min(lhs, rhs);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmax, max), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmin, min), allTrue));

  Proc::MaskType expectEq = bitMaskProducer(l, r, std::equal_to<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(lhs, rhs), expectEq));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(lhs, rhs), ~expectEq));

  Proc::MaskType expectLt = bitMaskProducer(l, r, std::less<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(lhs, rhs), expectLt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(lhs, rhs), ~expectLt));
}

TEST(SimdProcessorTest, Int32)
{
  using IntegralType = int32_t;
  IntegralType l[8]{0, 1, 2, -5};
  IntegralType r[8]{0, 105, -8, 54333};
  IntegralType minlr[8]{0, 1, -8, -5};
  IntegralType maxlr[8]{0, 105, 2, 54333};
  using Proc = typename SimdProcessorTypedTest<int32_t>::Proc;
  constexpr const size_t VecSize = Proc::vecByteSize / sizeof(IntegralType);
  auto cmpEqFunctor = [](Proc::MaskType left, Proc::MaskType right)
  { return !memcmp((void*)(&left), (void*)(&right), sizeof(Proc::MaskType)); };
  auto bitMaskProducer = bitMaskProducerT<IntegralType, Proc::MaskType, VecSize>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  const Proc::MaskType allTrue = proc.trueMask();

  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  SimdType min = proc.loadFrom(reinterpret_cast<char*>(minlr));
  SimdType max = proc.loadFrom(reinterpret_cast<char*>(maxlr));

  Proc::MaskType expectGt = bitMaskProducer(l, r, std::greater<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(lhs, rhs), expectGt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(lhs, rhs), ~expectGt));
  SimdType testmax = proc.max(lhs, rhs);
  SimdType testmin = proc.min(lhs, rhs);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmax, max), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmin, min), allTrue));

  Proc::MaskType expectEq = bitMaskProducer(l, r, std::equal_to<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(lhs, rhs), expectEq));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(lhs, rhs), ~expectEq));

  Proc::MaskType expectLt = bitMaskProducer(l, r, std::less<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(lhs, rhs), expectLt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(lhs, rhs), ~expectLt));
}
TEST(SimdProcessorTest, Uint32)
{
  using IntegralType = uint32_t;
  IntegralType l[4]{0, 1002, 2, 514};
  IntegralType r[4]{2, 1, 80555, 35};
  IntegralType minlr[8]{0, 1, 2, 35};
  IntegralType maxlr[8]{2, 1002, 80555, 514};
  using Proc = typename SimdProcessorTypedTest<uint32_t>::Proc;
  constexpr const size_t VecSize = Proc::vecByteSize / sizeof(IntegralType);
  auto cmpEqFunctor = [](typename Proc::MaskType left, typename Proc::MaskType right)
  { return !memcmp((void*)(&left), (void*)(&right), sizeof(typename Proc::MaskType)); };
  auto bitMaskProducer = bitMaskProducerT<IntegralType, Proc::MaskType, VecSize>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  const Proc::MaskType allTrue = proc.trueMask();

  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  SimdType min = proc.loadFrom(reinterpret_cast<char*>(minlr));
  SimdType max = proc.loadFrom(reinterpret_cast<char*>(maxlr));

  Proc::MaskType expectGt = bitMaskProducer(l, r, std::greater<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(lhs, rhs), expectGt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(lhs, rhs), ~expectGt));
  SimdType testmax = proc.max(lhs, rhs);
  SimdType testmin = proc.min(lhs, rhs);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmax, max), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmin, min), allTrue));

  Proc::MaskType expectEq = bitMaskProducer(l, r, std::equal_to<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(lhs, rhs), expectEq));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(lhs, rhs), ~expectEq));

  Proc::MaskType expectLt = bitMaskProducer(l, r, std::less<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(lhs, rhs), expectLt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(lhs, rhs), ~expectLt));
}
TEST(SimdProcessorTest, Int64)
{
  using IntegralType = int64_t;
  IntegralType l[2]{-5, 122020};
  IntegralType r[2]{0, 105};
  IntegralType minlr[8]{-5, 105};
  IntegralType maxlr[8]{0, 122020};
  using Proc = typename SimdProcessorTypedTest<int64_t>::Proc;
  constexpr const size_t VecSize = Proc::vecByteSize / sizeof(IntegralType);
  auto cmpEqFunctor = [](typename Proc::MaskType left, typename Proc::MaskType right)
  { return !memcmp((void*)(&left), (void*)(&right), sizeof(typename Proc::MaskType)); };
  auto bitMaskProducer = bitMaskProducerT<IntegralType, Proc::MaskType, VecSize>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  const Proc::MaskType allTrue = proc.trueMask();

  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  SimdType min = proc.loadFrom(reinterpret_cast<char*>(minlr));
  SimdType max = proc.loadFrom(reinterpret_cast<char*>(maxlr));

  Proc::MaskType expectGt = bitMaskProducer(l, r, std::greater<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(lhs, rhs), expectGt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(lhs, rhs), ~expectGt));
  SimdType testmax = proc.max(lhs, rhs);
  SimdType testmin = proc.min(lhs, rhs);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmax, max), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmin, min), allTrue));

  Proc::MaskType expectEq = bitMaskProducer(l, r, std::equal_to<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(lhs, rhs), expectEq));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(lhs, rhs), ~expectEq));

  Proc::MaskType expectLt = bitMaskProducer(l, r, std::less<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(lhs, rhs), expectLt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(lhs, rhs), ~expectLt));
}
TEST(SimdProcessorTest, Uint64)
{
  using IntegralType = uint64_t;
  IntegralType l[2]{822, 1002};
  IntegralType r[2]{2, 1};
  IntegralType minlr[8]{2, 1};
  IntegralType maxlr[8]{822, 1002};
  using Proc = typename SimdProcessorTypedTest<uint64_t>::Proc;
  constexpr const size_t VecSize = Proc::vecByteSize / sizeof(IntegralType);
  auto cmpEqFunctor = [](typename Proc::MaskType left, typename Proc::MaskType right)
  { return !memcmp((void*)(&left), (void*)(&right), sizeof(typename Proc::MaskType)); };
  auto bitMaskProducer = bitMaskProducerT<IntegralType, Proc::MaskType, VecSize>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  const Proc::MaskType allTrue = proc.trueMask();

  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  SimdType min = proc.loadFrom(reinterpret_cast<char*>(minlr));
  SimdType max = proc.loadFrom(reinterpret_cast<char*>(maxlr));

  Proc::MaskType expectGt = bitMaskProducer(l, r, std::greater<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(lhs, rhs), expectGt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(lhs, rhs), ~expectGt));
  SimdType testmax = proc.max(lhs, rhs);
  SimdType testmin = proc.min(lhs, rhs);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmax, max), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmin, min), allTrue));

  Proc::MaskType expectEq = bitMaskProducer(l, r, std::equal_to<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(lhs, rhs), expectEq));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(lhs, rhs), ~expectEq));

  Proc::MaskType expectLt = bitMaskProducer(l, r, std::less<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(lhs, rhs), expectLt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(lhs, rhs), ~expectLt));
}
TEST(SimdProcessorTest, Float64)
{
  using IntegralType = float64_t;
  IntegralType l[2]{-5.0, 12.5620};
  IntegralType r[2]{2.9, 1};
  IntegralType minlr[8]{-5.0, 1};
  IntegralType maxlr[8]{2.9, 12.5620};
  using Proc = typename SimdProcessorTypedTest<IntegralType>::Proc;
  constexpr const size_t VecSize = Proc::vecByteSize / sizeof(IntegralType);
  auto cmpEqFunctor = [](typename Proc::MaskType left, typename Proc::MaskType right)
  { return !memcmp((void*)(&left), (void*)(&right), sizeof(typename Proc::MaskType)); };
  auto bitMaskProducer = bitMaskProducerT<IntegralType, Proc::MaskType, VecSize>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  const Proc::MaskType allTrue = proc.trueMask();

  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  SimdType min = proc.loadFrom(reinterpret_cast<char*>(minlr));
  SimdType max = proc.loadFrom(reinterpret_cast<char*>(maxlr));

  Proc::MaskType expectGt = bitMaskProducer(l, r, std::greater<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(lhs, rhs), expectGt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(lhs, rhs), ~expectGt));
  SimdType testmax = proc.max(lhs, rhs);
  SimdType testmin = proc.min(lhs, rhs);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmax, max), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmin, min), allTrue));

  Proc::MaskType expectEq = bitMaskProducer(l, r, std::equal_to<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(lhs, rhs), expectEq));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(lhs, rhs), ~expectEq));

  Proc::MaskType expectLt = bitMaskProducer(l, r, std::less<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(lhs, rhs), expectLt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(lhs, rhs), ~expectLt));
}
TEST(SimdProcessorTest, Float32)
{
  using IntegralType = float32_t;
  IntegralType l[4]{82, 102, -5.6, 9.5};
  IntegralType r[4]{2.0, 1, -5.7, 6};
  IntegralType minlr[8]{2.0, 1, -5.7, 6};
  IntegralType maxlr[8]{82, 102, -5.6, 9.5};
  using Proc = typename SimdProcessorTypedTest<IntegralType>::Proc;
  constexpr const size_t VecSize = Proc::vecByteSize / sizeof(IntegralType);
  auto cmpEqFunctor = [](typename Proc::MaskType left, typename Proc::MaskType right)
  { return !memcmp((void*)(&left), (void*)(&right), sizeof(typename Proc::MaskType)); };
  auto bitMaskProducer = bitMaskProducerT<IntegralType, Proc::MaskType, VecSize>;
  using SimdType = typename Proc::SimdType;
  Proc proc;
  const Proc::MaskType allTrue = proc.trueMask();

  SimdType lhs = proc.loadFrom(reinterpret_cast<char*>(l));
  SimdType rhs = proc.loadFrom(reinterpret_cast<char*>(r));
  SimdType min = proc.loadFrom(reinterpret_cast<char*>(minlr));
  SimdType max = proc.loadFrom(reinterpret_cast<char*>(maxlr));

  Proc::MaskType expectGt = bitMaskProducer(l, r, std::greater<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGt(lhs, rhs), expectGt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLe(lhs, rhs), ~expectGt));
  SimdType testmax = proc.max(lhs, rhs);
  SimdType testmin = proc.min(lhs, rhs);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmax, max), allTrue));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(testmin, min), allTrue));

  Proc::MaskType expectEq = bitMaskProducer(l, r, std::equal_to<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpEq(lhs, rhs), expectEq));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpNe(lhs, rhs), ~expectEq));

  Proc::MaskType expectLt = bitMaskProducer(l, r, std::less<IntegralType>(), false);
  EXPECT_TRUE(cmpEqFunctor(proc.cmpLt(lhs, rhs), expectLt));
  EXPECT_TRUE(cmpEqFunctor(proc.cmpGe(lhs, rhs), ~expectLt));
}
#endif
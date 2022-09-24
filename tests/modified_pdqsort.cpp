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
#include <iostream>
#include <limits>
#include <type_traits>
#include <gtest/gtest.h>
#include "gmock/gmock.h"
#include "utils/common/modified_pdqsort.h"

using namespace std;
using namespace testing;

template <typename T>
class PDQSortingTypedTest : public testing::Test
{
 public:
  using IntegralType = T;
  void SetUp() override
  {
  }
};

template <typename T>
class PDQSortingUTypedTest : public testing::Test
{
 public:
  using IntegralType = T;
  void SetUp() override
  {
  }
};

using PDQSortingTypedTestUTypes = ::testing::Types<uint64_t, uint32_t, uint16_t, uint8_t>;

TYPED_TEST_SUITE(PDQSortingUTypedTest, PDQSortingTypedTestUTypes);
using PDQSortingTypedTestTypes = ::testing::Types<int64_t, int32_t, int16_t, int8_t>;
TYPED_TEST_SUITE(PDQSortingTypedTest, PDQSortingTypedTestTypes);

TYPED_TEST(PDQSortingUTypedTest, PDQSortingUnsignedTest)
{
  using T = typename PDQSortingTypedTest<TypeParam>::IntegralType;
  std::vector<T> data = {0, 0, std::numeric_limits<T>::max(), 3, 3, std::numeric_limits<T>::min()};
  sorting::pdqsort(data.begin(), data.end());
  ASSERT_THAT(data, ElementsAre(std::numeric_limits<T>::min(), 0, 0, 3, 3, std::numeric_limits<T>::max()));
  std::vector<T> data2 = {2, 1, std::numeric_limits<T>::max(), 4, 3, std::numeric_limits<T>::min()};
  std::vector<uint64_t> perm2 = {0, 1, 2, 3, 4, 5};
  sorting::mod_pdqsort(data2.begin(), data2.end(), perm2.begin(), perm2.end(), std::less<T>());
  ASSERT_THAT(data2, ElementsAre(0, 1, 2, 3, 4, std::numeric_limits<T>::max()));
  ASSERT_THAT(perm2, ElementsAre(5, 1, 0, 4, 3, 2));
}

TYPED_TEST(PDQSortingTypedTest, PDQSortingSignedTest)
{
  using T = typename PDQSortingTypedTest<TypeParam>::IntegralType;
  std::vector<T> data = {-1, -1, std::numeric_limits<T>::max(), 3, 3, std::numeric_limits<T>::min()};
  sorting::pdqsort(data.begin(), data.end());
  ASSERT_THAT(data, ElementsAre(std::numeric_limits<T>::min(), -1, -1, 3, 3, std::numeric_limits<T>::max()));
  std::vector<T> data2 = {-22, -1, std::numeric_limits<T>::max(), 4, 3, std::numeric_limits<T>::min()};
  std::vector<uint64_t> perm2 = {0, 1, 2, 3, 4, 5};
  sorting::mod_pdqsort(data2.begin(), data2.end(), perm2.begin(), perm2.end(), std::less<T>());
  ASSERT_THAT(data2,
              ElementsAre(std::numeric_limits<T>::min(), -22, -1, 3, 4, std::numeric_limits<T>::max()));
  ASSERT_THAT(perm2, ElementsAre(5, 0, 1, 4, 3, 2));
}
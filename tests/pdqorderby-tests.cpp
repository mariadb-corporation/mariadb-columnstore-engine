/* Copyright (C) 2023 MariaDB Corporation

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

#include "conststring.h"
#include "pdqorderby.h"

using namespace std;
using namespace testing;
using namespace sorting;

class PDQOrderByTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
  }
};

TEST_F(PDQOrderByTest, getNullValue)
{
  auto null = sorting::getNullValue<ConstString, ConstString>(execplan::CalpontSystemCatalog::VARCHAR);
  EXPECT_EQ(null.str(), nullptr);
  EXPECT_EQ(null.length(), 0ULL);
}

// TEST_F(PDQOrderByTest, isNull)
// {
//   auto null = sorting::getNullValue<ConstString, ConstString>(execplan::CalpontSystemCatalog::VARCHAR);
//   auto storageNull = sorting::getNullValue<ConstString, ConstString>(execplan::CalpontSystemCatalog::VARCHAR);

//   EXPECT_EQ(sorting::isNull<ConstString, ConstString>(null, null, storageNull), true);
//   EXPECT_EQ(null.length(), 0ULL);
// }
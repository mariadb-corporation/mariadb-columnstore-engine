/* Copyright (C) 2020 MariaDB Corporation

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

#include "gtest/gtest.h"

#include "treenode.h"
#include "mcs_decimal.h"
#include "dataconvert.h"

TEST(Decimal, compareCheckInt64)
{
  // remainder-based checks
  // Equality checks
  // l value = r value, L scale = R scale
  {
    datatypes::Decimal l(420, 10, 18);
    datatypes::Decimal r(420, 10, 18);
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // l value > r value, L scale < R scale
  {
    datatypes::Decimal l(420, 11, 18);
    datatypes::Decimal r(42, 10, 18);
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // l value < r value, L scale > R scale
  {
    datatypes::Decimal l(42, 10, 18);
    datatypes::Decimal r(420, 11, 18);
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // Inequality checks
  // l value = r value, L scale < R scale
  {
    datatypes::Decimal l(42, 10, 18);
    datatypes::Decimal r(42, 13, 18);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value = r value, L scale < R scale
  {
    datatypes::Decimal l(42, 13, 18);
    datatypes::Decimal r(42, 10, 18);
    EXPECT_FALSE(l > r);
    EXPECT_FALSE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value > r value, L scale > R scale
  {
    datatypes::Decimal l(420, 13, 18);
    datatypes::Decimal r(42, 10, 18);
    EXPECT_FALSE(l > r);
    EXPECT_FALSE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value > r value, L scale < R scale
  {
    datatypes::Decimal l(4200, 11, 18);
    datatypes::Decimal r(42, 12, 18);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value < r value, L scale < R scale
  {
    datatypes::Decimal l(99, 10, 18);
    datatypes::Decimal r(420, 11, 18);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value < r value, L scale < R scale
  {
    datatypes::Decimal l(42, 10, 18);
    datatypes::Decimal r(420, 13, 18);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // quotinent-based checks
  // l value = r value, L scale = R scale
  {
    datatypes::Decimal l(420, 1, 18);
    datatypes::Decimal r(420, 1, 18);
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // l value > r value, L scale < R scale
  {
    datatypes::Decimal l(4200, 2, 18);
    datatypes::Decimal r(420, 1, 18);
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // l value < r value, L scale > R scale
  {
    datatypes::Decimal l(42, 0, 18);
    datatypes::Decimal r(420, 1, 18);
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // Inequality checks
  // l value = r value, L scale < R scale
  {
    datatypes::Decimal l(42000, 0, 18);
    datatypes::Decimal r(42000, 3, 18);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value = r value, L scale < R scale
  {
    datatypes::Decimal l(420000, 3, 18);
    datatypes::Decimal r(420000, 0, 18);
    EXPECT_FALSE(l > r);
    EXPECT_FALSE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value > r value, L scale > R scale
  {
    datatypes::Decimal l(420000, 3, 18);
    datatypes::Decimal r(42000, 0, 18);
    EXPECT_FALSE(l > r);
    EXPECT_FALSE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value > r value, L scale < R scale
  {
    datatypes::Decimal l(420000, 1, 18);
    datatypes::Decimal r(4200, 2, 18);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value < r value, L scale < R scale
  {
    datatypes::Decimal l(990, 0, 18);
    datatypes::Decimal r(4200, 1, 18);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value < r value, L scale < R scale
  {
    datatypes::Decimal l(420, 0, 18);
    datatypes::Decimal r(4200, 3, 18);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }
}

TEST(Decimal, compareCheckInt128)
{
  // remainer-based checks
  // Equality checks
  // l value = r value, L scale = R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(420), 20, 38);
    datatypes::Decimal r(datatypes::TSInt128(420), 20, 38);
    EXPECT_EQ(0, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // l value > r value, L scale < R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(420), 21, 38);
    datatypes::Decimal r(datatypes::TSInt128(42), 20, 38);
    EXPECT_EQ(0, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // l value < r value, L scale > R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(42), 20, 38);
    datatypes::Decimal r(datatypes::TSInt128(420), 21, 38);
    EXPECT_EQ(0, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // Inequality checks
  // l value = r value, L scale < R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(42), 20, 38);
    datatypes::Decimal r(datatypes::TSInt128(42), 23, 38);
    EXPECT_EQ(1, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value = r value, L scale < R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(42), 23, 38);
    datatypes::Decimal r(datatypes::TSInt128(42), 20, 38);
    EXPECT_EQ(-1, datatypes::Decimal::compare(l, r));
    EXPECT_FALSE(l > r);
    EXPECT_FALSE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value > r value, L scale > R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(420), 23, 38);
    datatypes::Decimal r(datatypes::TSInt128(42), 20, 38);
    EXPECT_EQ(-1, datatypes::Decimal::compare(l, r));
    EXPECT_FALSE(l > r);
    EXPECT_FALSE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value > r value, L scale < R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(4200), 21, 38);
    datatypes::Decimal r(datatypes::TSInt128(42), 22, 38);
    EXPECT_EQ(1, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value < r value, L scale < R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(99), 20, 38);
    datatypes::Decimal r(datatypes::TSInt128(420), 21, 38);
    EXPECT_EQ(1, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value < r value, L scale < R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(42), 20, 38);
    datatypes::Decimal r(datatypes::TSInt128(420), 23, 38);
    EXPECT_EQ(1, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // quotinent-based checks
  // l value = r value, L scale = R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(420), 1, 38);
    datatypes::Decimal r(datatypes::TSInt128(420), 1, 38);
    EXPECT_EQ(0, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // l value > r value, L scale < R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(4200), 2, 38);
    datatypes::Decimal r(datatypes::TSInt128(420), 1, 38);
    EXPECT_EQ(0, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // l value < r value, L scale > R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(42), 0, 38);
    datatypes::Decimal r(datatypes::TSInt128(420), 1, 38);
    EXPECT_EQ(0, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // Inequality checks
  // l value = r value, L scale < R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(42000), 0, 38);
    datatypes::Decimal r(datatypes::TSInt128(42000), 3, 38);
    EXPECT_EQ(1, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value = r value, L scale < R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(420000), 3, 38);
    datatypes::Decimal r(datatypes::TSInt128(420000), 0, 38);
    EXPECT_EQ(-1, datatypes::Decimal::compare(l, r));
    EXPECT_FALSE(l > r);
    EXPECT_FALSE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value > r value, L scale > R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(420000), 3, 38);
    datatypes::Decimal r(datatypes::TSInt128(42000), 0, 38);
    EXPECT_EQ(-1, datatypes::Decimal::compare(l, r));
    EXPECT_FALSE(l > r);
    EXPECT_FALSE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value > r value, L scale < R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(420000), 1, 38);
    datatypes::Decimal r(datatypes::TSInt128(4200), 2, 38);
    EXPECT_EQ(1, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value < r value, L scale < R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(990), 0, 38);
    datatypes::Decimal r(datatypes::TSInt128(4200), 1, 38);
    EXPECT_EQ(1, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value < r value, L scale < R scale
  {
    datatypes::Decimal l(datatypes::TSInt128(420), 0, 38);
    datatypes::Decimal r(datatypes::TSInt128(4200), 3, 38);
    EXPECT_EQ(1, datatypes::Decimal::compare(l, r));
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }
}

TEST(Decimal, compareCheckInt64_Int128)
{
  // remainer-based checks
  // Equality checks
  // l value = r value, L scale = R scale
  {
    datatypes::Decimal l(420, 10, 18);
    datatypes::Decimal r(datatypes::TSInt128(420), 10, 38);
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // l value > r value, L scale < R scale
  {
    datatypes::Decimal l(420, 11, 18);
    datatypes::Decimal r(datatypes::TSInt128(42), 10, 38);
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // l value < r value, L scale > R scale
  {
    datatypes::Decimal l(42, 10, 18);
    datatypes::Decimal r(datatypes::TSInt128(420), 11, 38);
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // Inequality checks
  // l value = r value, L scale < R scale
  {
    datatypes::Decimal l(42, 10, 18);
    datatypes::Decimal r(datatypes::TSInt128(42), 13, 38);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value = r value, L scale < R scale
  {
    datatypes::Decimal l(42, 13, 18);
    datatypes::Decimal r(datatypes::TSInt128(42), 10, 38);
    EXPECT_FALSE(l > r);
    EXPECT_FALSE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value > r value, L scale > R scale
  {
    datatypes::Decimal l(420, 13, 18);
    datatypes::Decimal r(datatypes::TSInt128(42), 10, 38);
    EXPECT_FALSE(l > r);
    EXPECT_FALSE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value > r value, L scale < R scale
  {
    datatypes::Decimal l(4200, 11, 18);
    datatypes::Decimal r(datatypes::TSInt128(42), 12, 38);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value < r value, L scale < R scale
  {
    datatypes::Decimal l(99, 10, 18);
    datatypes::Decimal r(datatypes::TSInt128(420), 11, 38);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value < r value, L scale < R scale
  {
    datatypes::Decimal l(42, 10, 18);
    datatypes::Decimal r(datatypes::TSInt128(420), 13, 38);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // quotinent-based checks
  // l value = r value, L scale = R scale
  {
    datatypes::Decimal l(420, 1, 18);
    datatypes::Decimal r(datatypes::TSInt128(420), 1, 38);
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // l value > r value, L scale < R scale
  {
    datatypes::Decimal l(4200, 2, 18);
    datatypes::Decimal r(datatypes::TSInt128(420), 1, 38);
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // l value < r value, L scale > R scale
  {
    datatypes::Decimal l(42, 0, 18);
    datatypes::Decimal r(datatypes::TSInt128(420), 1, 38);
    EXPECT_TRUE(l == r);
    EXPECT_TRUE(l >= r);
  }

  // Inequality checks
  // l value = r value, L scale < R scale
  {
    datatypes::Decimal l(42000, 0, 18);
    datatypes::Decimal r(datatypes::TSInt128(42000), 3, 38);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value = r value, L scale < R scale
  {
    datatypes::Decimal l(420000, 3, 18);
    datatypes::Decimal r(datatypes::TSInt128(420000), 0, 38);
    EXPECT_FALSE(l > r);
    EXPECT_FALSE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value > r value, L scale > R scale
  {
    datatypes::Decimal l(420000, 3, 18);
    datatypes::Decimal r(datatypes::TSInt128(42000), 0, 38);
    EXPECT_FALSE(l > r);
    EXPECT_FALSE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value > r value, L scale < R scale
  {
    datatypes::Decimal l(420000, 1, 18);
    datatypes::Decimal r(datatypes::TSInt128(4200), 2, 38);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value < r value, L scale < R scale
  {
    datatypes::Decimal l(990, 0, 18);
    datatypes::Decimal r(datatypes::TSInt128(4200), 1, 38);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }

  // l value < r value, L scale < R scale
  {
    datatypes::Decimal l(420, 0, 18);
    datatypes::Decimal r(datatypes::TSInt128(4200), 3, 38);
    EXPECT_TRUE(l > r);
    EXPECT_TRUE(l >= r);
    EXPECT_FALSE(l == r);
  }
}

TEST(Decimal, additionNoOverflowCheck)
{
  // Addition w/o overflow check
  execplan::IDB_Decimal l, r, result;
  // same precision, same scale, both positive values
  l.scale = 38;
  l.precision = 38;
  l.s128Value = 42;

  r.scale = 38;
  r.precision = 38;
  r.s128Value = 420;

  result.scale = 38;
  result.precision = 38;
  result.s128Value = 0;

  datatypes::Decimal::addition<int128_t, false>(l, r, result);
  EXPECT_EQ(462, result.s128Value);

  // same precision, same scale, both negative values
  l.s128Value = -42;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::addition<int128_t, false>(l, r, result);
  EXPECT_EQ(-462, result.s128Value);

  // same precision, same scale, +- values
  l.s128Value = 42;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::addition<int128_t, false>(l, r, result);
  EXPECT_EQ(-378, result.s128Value);

  // same precision, same scale, both 0
  l.s128Value = 0;
  r.s128Value = 0;
  result.s128Value = 0;

  datatypes::Decimal::addition<int128_t, false>(l, r, result);
  EXPECT_EQ(0, result.s128Value);

  // different scale
  // same precision, L scale > R scale, both positive values
  l.scale = 38;
  l.precision = 38;
  l.s128Value = 42;

  r.scale = 15;
  r.precision = 38;
  r.s128Value = 420;

  result.scale = 38;
  result.precision = 38;
  result.s128Value = 0;

  datatypes::Decimal::addition<int128_t, false>(l, r, result);
  EXPECT_EQ("0.00000000000042000000000000000000000042", result.toString());

  // same precision, L scale > R scale, both negative values
  l.s128Value = -42;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::addition<int128_t, false>(l, r, result);
  EXPECT_EQ("-0.00000000000042000000000000000000000042", result.toString());

  // same precision, L scale > R scale, +- values
  l.s128Value = 42;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::addition<int128_t, false>(l, r, result);
  EXPECT_EQ("-0.00000000000041999999999999999999999958", result.toString());

  // same precision, L scale > R scale, both 0
  l.s128Value = 0;
  r.s128Value = 0;
  result.s128Value = 0;

  datatypes::Decimal::addition<int128_t, false>(l, r, result);
  EXPECT_EQ(0, result.s128Value);

  // same precision, L scale < R scale, both positive values
  l.scale = 15;
  l.precision = 38;
  l.s128Value = 42;

  r.scale = 38;
  r.precision = 38;
  r.s128Value = 420;

  result.scale = 38;
  result.precision = 38;
  result.s128Value = 0;

  datatypes::Decimal::addition<int128_t, false>(l, r, result);
  EXPECT_EQ("0.00000000000004200000000000000000000420", result.toString());

  // same precision, L scale < R scale, both negative values
  l.s128Value = -42;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::addition<int128_t, false>(l, r, result);
  EXPECT_EQ("-0.00000000000004200000000000000000000420", result.toString());

  // same precision, L scale < R scale, +- values
  l.s128Value = 42;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::addition<int128_t, false>(l, r, result);
  EXPECT_EQ("0.00000000000004199999999999999999999580", result.toString());

  // same precision, L scale < R scale, both 0
  l.s128Value = 0;
  r.s128Value = 0;
  result.s128Value = 0;

  datatypes::Decimal::addition<int128_t, false>(l, r, result);
  EXPECT_EQ(0, result.s128Value);
}

TEST(Decimal, divisionNoOverflowCheck)
{
  // DIVISION
  // same precision, same scale, both positive values
  execplan::IDB_Decimal l, r, result;

  l.scale = 38;
  l.precision = 38;
  l.s128Value = 43;

  r.scale = 38;
  r.precision = 38;
  r.s128Value = 420;

  result.scale = 10;
  result.precision = 38;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("9.7674418605", result.toString());

  // same precision, same scale, both negative values
  l.s128Value = -43;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("9.7674418605", result.toString());

  // same precision, same scale, +- values
  l.s128Value = 2200000;
  r.s128Value = -1900;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(l, r, result);
  EXPECT_EQ("-1157.8947368421", result.toString());

  // same precision, same scale, l = 0
  l.s128Value = 0;
  r.s128Value = 42424242;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(l, r, result);
  EXPECT_EQ(0, result.s128Value);

  // diff scale
  // same precision, L scale > R scale, both positive values
  l.scale = 38;
  l.s128Value = 19;

  r.scale = 15;
  r.s128Value = 22;

  result.scale = 10;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("115789473684210526315789.4736842105", result.toString());

  // same precision, L scale > R scale, both negative values
  l.s128Value = -22;
  r.s128Value = -19;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("86363636363636363636363.6363636364", result.toString());

  // same precision, L scale > R scale, +- values
  l.s128Value = 19;
  r.s128Value = -22;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("-115789473684210526315789.4736842105", result.toString());

  // same precision, L scale > R scale, R = 0
  l.s128Value = 424242;
  r.s128Value = 0;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ(0, result.s128Value);

  // same precision, L scale > R scale, both MAX positive values
  utils::int128Max(l.s128Value);
  utils::int128Max(r.s128Value);
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("100000000000000000000000.0000000000", result.toString());

  // same precision, L scale > R scale, both MIN negative values
  utils::int128Min(l.s128Value);
  utils::int128Min(r.s128Value);
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("100000000000000000000000.0000000000", result.toString());

  // same precision, L scale < R scale, both positive values
  l.scale = 15;
  l.precision = 38;
  l.s128Value = 42;

  r.scale = 38;
  r.precision = 38;
  r.s128Value = 42;

  result.scale = 38;
  result.precision = 38;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("0.00000000000000000000001000000000000000", result.toString());

  // same precision, L scale < R scale, both negative values
  l.s128Value = -22;
  r.s128Value = -19;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("0.00000000000000000000000863636363636364", result.toString());

  // same precision, L scale < R scale, +- values
  l.s128Value = 22;
  r.s128Value = -19;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("-0.00000000000000000000000863636363636364", result.toString());

  // same precision, L scale < R scale, R = 0
  l.s128Value = 42;
  r.s128Value = 0;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ(0, result.s128Value);

  // same precision, L scale < R scale, both MAX positive values
  // WIP Investigate the next two
  utils::int128Max(l.s128Value);
  utils::int128Max(r.s128Value);
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("0.00000000000000000000001000000000000000", result.toString());

  // same precision, L scale < R scale, both MIN negative values
  utils::int128Min(l.s128Value);
  utils::int128Min(r.s128Value);
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("0.00000000000000000000001000000000000000", result.toString());

  // same precision, L scale < R scale, result.scale < (r.scale-l.scale)
  // both positive values
  l.scale = 37;
  l.precision = 38;
  l.s128Value = 43;

  r.scale = 38;
  r.precision = 38;
  r.s128Value = 420;

  result.scale = 0;
  result.precision = 38;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("1", result.toString());

  // same precision, L scale < R scale, result.scale < (r.scale-l.scale)
  // both negative values
  l.s128Value = -22;
  r.s128Value = -1900;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("9", result.toString());

  // same precision, L scale < R scale, result.scale < (r.scale-l.scale)
  // +- values
  l.s128Value = 22;
  r.s128Value = -1900;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("-9", result.toString());

  // same precision, L scale < R scale, result.scale < (r.scale-l.scale)
  // R = 0
  l.s128Value = 42;
  r.s128Value = 0;
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ(0, result.s128Value);

  // same precision, L scale < R scale, result.scale < (r.scale-l.scale)
  // both MAX positive values
  utils::int128Max(l.s128Value);
  utils::int128Max(r.s128Value);
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("0", result.toString());

  // same precision, L scale < R scale, result.scale < (r.scale-l.scale)
  // both MIN negative values
  utils::int128Min(l.s128Value);
  utils::int128Min(r.s128Value);
  result.s128Value = 0;

  datatypes::Decimal::division<int128_t, false>(r, l, result);
  EXPECT_EQ("0", result.toString());
}

void doDiv(const execplan::IDB_Decimal& l, const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result)
{
  datatypes::Decimal::division<int128_t, true>(l, r, result);
}

TEST(Decimal, divisionWithOverflowCheck)
{
  // Divide min int128 by -1
  execplan::IDB_Decimal l, r, result;
  l.scale = 0;
  l.precision = 38;
  l.s128Value = datatypes::Decimal::minInt128;

  r.scale = 0;
  r.precision = 38;
  r.s128Value = -1;

  result.scale = 0;
  result.precision = 38;
  result.s128Value = 42;
  EXPECT_THROW(doDiv(l, r, result), logging::OperationOverflowExcept);

  // Divide two ints one of which overflows after the scaling.
  // TODO We currently do not test overflow due to scaling in
  // case of division. Re-enable this test when we check for overflow
  /*l.s128Value = datatypes::Decimal::maxInt128;
  r.scale = 1;
  r.s128Value = 42;

  result.scale = 1;
  result.s128Value = 42;
  EXPECT_THROW(doDiv(l, r, result), logging::OperationOverflowExcept);*/

  // Normal execution w/o overflow
  l.scale = 0;
  l.s128Value = datatypes::Decimal::maxInt128 - 1;

  r.scale = 0;
  r.s128Value = 0xFFFFFFFFFFFFFFFF;

  result.scale = 0;
  result.s128Value = 0;

  EXPECT_NO_THROW(doDiv(l, r, result));
  EXPECT_EQ("9223372036854775809", result.toString());
}

void doAdd(const execplan::IDB_Decimal& l, const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result)
{
  datatypes::Decimal::addition<int128_t, true>(l, r, result);
}

TEST(Decimal, additionWithOverflowCheck)
{
  // Add two max ints
  execplan::IDB_Decimal l, r, result;
  l.scale = 0;
  l.precision = 38;
  l.s128Value = datatypes::Decimal::maxInt128 - 1;

  r.scale = 0;
  r.precision = 38;
  r.s128Value = datatypes::Decimal::maxInt128 - 1;

  result.scale = 0;
  result.precision = 38;
  result.s128Value = 42;
  EXPECT_THROW(doAdd(l, r, result), logging::OperationOverflowExcept);

  // Add two ints one of which overflows after the scaling.
  l.s128Value = datatypes::Decimal::maxInt128 - 1;
  r.scale = 1;
  r.s128Value = 0xFFFFFFFFFFFFFFFF;

  result.scale = 1;
  result.precision = 38;
  result.s128Value = 0;
  EXPECT_THROW(doAdd(l, r, result), logging::OperationOverflowExcept);

  // Normal execution w/o overflow
  l.scale = 0;
  l.s128Value = datatypes::Decimal::minInt128;

  r.scale = 0;
  r.s128Value = 0xFFFFFFFFFFFFFFFF;

  result.scale = 0;
  result.s128Value = 0;

  EXPECT_NO_THROW(doAdd(l, r, result));
  EXPECT_EQ("-170141183460469231713240559642174554113", result.toString());
}

TEST(Decimal, subtractionNoOverflowCheck)
{
  // Subtractio w/o overflow check
  execplan::IDB_Decimal l, r, result;
  // same precision, same scale, both positive values
  l.scale = 38;
  l.precision = 38;
  l.s128Value = 42;

  r.scale = 38;
  r.precision = 38;
  r.s128Value = 420;

  result.scale = 38;
  result.precision = 38;
  result.s128Value = 0;

  datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
  EXPECT_EQ("-0.00000000000000000000000000000000000378", result.toString());

  // same precision, same scale, both negative values
  l.s128Value = -42;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
  EXPECT_EQ("0.00000000000000000000000000000000000378", result.toString());

  // same precision, same scale, +- values
  l.s128Value = 42;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
  EXPECT_EQ("0.00000000000000000000000000000000000462", result.toString());

  // same precision, same scale, both 0
  l.s128Value = 0;
  r.s128Value = 0;
  result.s128Value = 0;

  datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
  EXPECT_EQ(0, result.s128Value);

  // different scale
  // same precision, L scale > R scale, both positive values
  l.scale = 38;
  l.precision = 38;
  l.s128Value = 42;

  r.scale = 15;
  r.precision = 38;
  r.s128Value = 420;

  result.scale = 38;
  result.precision = 38;
  result.s128Value = 0;

  datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
  EXPECT_EQ("-0.00000000000041999999999999999999999958", result.toString());

  // same precision, L scale > R scale, both negative values
  l.s128Value = -42;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
  EXPECT_EQ("0.00000000000041999999999999999999999958", result.toString());

  // same precision, L scale > R scale, +- values
  l.s128Value = 42;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
  EXPECT_EQ("0.00000000000042000000000000000000000042", result.toString());

  // same precision, L scale > R scale, both 0
  l.s128Value = 0;
  r.s128Value = 0;
  result.s128Value = 0;

  datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
  EXPECT_EQ(0, result.s128Value);

  // same precision, L scale < R scale, both positive values
  l.scale = 15;
  l.precision = 38;
  l.s128Value = 42;

  r.scale = 38;
  r.precision = 38;
  r.s128Value = 420;

  result.scale = 38;
  result.precision = 38;
  result.s128Value = 0;

  datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
  EXPECT_EQ("0.00000000000004199999999999999999999580", result.toString());

  // same precision, L scale < R scale, both negative values
  l.s128Value = -42;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
  EXPECT_EQ("-0.00000000000004199999999999999999999580", result.toString());

  // same precision, L scale < R scale, +- values
  l.s128Value = 42;
  r.s128Value = -420;
  result.s128Value = 0;

  datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
  EXPECT_EQ("0.00000000000004200000000000000000000420", result.toString());

  // same precision, L scale < R scale, both 0
  l.s128Value = 0;
  r.s128Value = 0;
  result.s128Value = 0;

  datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
  EXPECT_EQ(0, result.s128Value);
}

void doSubtract(const execplan::IDB_Decimal& l, const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result)
{
  datatypes::Decimal::subtraction<int128_t, true>(l, r, result);
}

TEST(Decimal, subtractionWithOverflowCheck)
{
  // Subtract a max int from a min int
  execplan::IDB_Decimal l, r, result;
  l.scale = 0;
  l.precision = 38;
  l.s128Value = datatypes::Decimal::minInt128 + 1;

  r.scale = 0;
  r.precision = 38;
  r.s128Value = datatypes::Decimal::maxInt128 - 1;

  result.scale = 0;
  result.precision = 38;
  result.s128Value = 42;
  EXPECT_THROW(doSubtract(l, r, result), logging::OperationOverflowExcept);

  // Subtract two ints one of which overflows after the scaling.
  l.s128Value = datatypes::Decimal::minInt128 + 1;
  r.scale = 1;
  r.s128Value = 0xFFFFFFFFFFFFFFFF;

  result.scale = 1;
  result.precision = 38;
  result.s128Value = 0;

  EXPECT_THROW(doSubtract(l, r, result), logging::OperationOverflowExcept);

  // Normal execution w/o overflow
  l.scale = 0;
  l.s128Value = datatypes::Decimal::maxInt128;

  r.scale = 0;
  r.s128Value = 0xFFFFFFFFFFFFFFFF;

  result.scale = 0;
  result.s128Value = 0;

  EXPECT_NO_THROW(doSubtract(l, r, result));
  EXPECT_EQ("170141183460469231713240559642174554112", result.toString());
}

TEST(Decimal, multiplicationNoOverflowCheck)
{
  // Multiplication
  // same precision, l.scale + r.scale = result.scale, both positive values
  execplan::IDB_Decimal l, r, result;

  l.scale = 19;
  l.precision = 38;
  l.s128Value = 4611686018427387904;

  r.scale = 19;
  r.precision = 38;
  r.s128Value = UINT64_MAX;

  result.scale = 38;
  result.precision = 38;
  result.s128Value = 0;

  datatypes::Decimal::multiplication<int128_t, false>(r, l, result);
  EXPECT_EQ("0.85070591730234615861231965839514664960", result.toString());

  // same precision, l.scale + r.scale = result.scale, both negative values
  l.s128Value = -4611686018427387904;
  r.s128Value = UINT64_MAX;
  r.s128Value = -r.s128Value;
  result.s128Value = 0;

  datatypes::Decimal::multiplication<int128_t, false>(r, l, result);
  EXPECT_EQ("0.85070591730234615861231965839514664960", result.toString());

  // same precision, l.scale + r.scale = result.scale, +- values
  l.s128Value = -4611686018427387904;
  r.s128Value = UINT64_MAX;
  result.s128Value = 0;

  datatypes::Decimal::multiplication<int128_t, false>(l, r, result);
  EXPECT_EQ("-0.85070591730234615861231965839514664960", result.toString());

  // same precision, l.scale + r.scale = result.scale, l = 0
  l.s128Value = 0;
  r.s128Value = UINT64_MAX;
  result.s128Value = 0;

  datatypes::Decimal::multiplication<int128_t, false>(l, r, result);
  EXPECT_EQ(0, result.s128Value);

  // same precision, l.scale + r.scale < result.scale, both positive values
  l.scale = 18;
  l.precision = 38;
  l.s128Value = 72057594037927936;

  r.scale = 18;
  r.precision = 38;
  r.s128Value = 9223372036854775808ULL;

  result.scale = 38;
  result.precision = 38;
  result.s128Value = 0;

  datatypes::Decimal::multiplication<int128_t, false>(r, l, result);
  EXPECT_EQ("0.66461399789245793645190353014017228800", result.toString());

  // same precision, l.scale + r.scale < result.scale, both negative values
  l.s128Value = -72057594037927936;
  r.s128Value = 9223372036854775808ULL;
  r.s128Value = -r.s128Value;
  result.s128Value = 0;

  datatypes::Decimal::multiplication<int128_t, false>(r, l, result);
  EXPECT_EQ("0.66461399789245793645190353014017228800", result.toString());

  // same precision, l.scale + r.scale < result.scale, +- values
  l.s128Value = -72057594037927936;
  r.s128Value = 9223372036854775808ULL;
  result.s128Value = 0;

  datatypes::Decimal::multiplication<int128_t, false>(l, r, result);
  EXPECT_EQ("-0.66461399789245793645190353014017228800", result.toString());

  // same precision, l.scale + r.scale < result.scale, l = 0
  l.s128Value = 0;
  r.s128Value = 9223372036854775808ULL;
  result.s128Value = 0;

  datatypes::Decimal::multiplication<int128_t, false>(l, r, result);
  EXPECT_EQ(0, result.s128Value);

  // same precision, l.scale + r.scale > result.scale, both positive values
  l.scale = 38;
  l.precision = 38;
  l.s128Value = (((int128_t)1234567890123456789ULL * 10000000000000000000ULL) + 1234567890123456789);

  r.scale = 38;
  r.precision = 38;
  r.s128Value = (((int128_t)1234567890123456789ULL * 10000000000000000000ULL) + 1234567890123456789ULL);

  result.scale = 38;
  result.precision = 38;
  result.s128Value = 0;

  datatypes::Decimal::multiplication<int128_t, false>(r, l, result);
  EXPECT_EQ("0.01524157875323883675019051998750190521", result.toString());

  // same precision, l.scale + r.scale > result.scale, both negative values
  l.s128Value = -l.s128Value;
  r.s128Value = -r.s128Value;
  result.s128Value = 0;

  datatypes::Decimal::multiplication<int128_t, false>(r, l, result);
  EXPECT_EQ("0.01524157875323883675019051998750190521", result.toString());

  // same precision, l.scale + r.scale > result.scale, +- values
  r.s128Value = -r.s128Value;
  result.s128Value = 0;

  datatypes::Decimal::multiplication<int128_t, false>(l, r, result);
  EXPECT_EQ("-0.01524157875323883675019051998750190521", result.toString());

  // same precision, l.scale + r.scale > result.scale, l = 0
  r.s128Value = 0;
  result.s128Value = 0;

  datatypes::Decimal::multiplication<int128_t, false>(l, r, result);
  EXPECT_EQ(0, result.s128Value);
}

void doMultiply(const execplan::IDB_Decimal& l, const execplan::IDB_Decimal& r, execplan::IDB_Decimal& result)
{
  datatypes::Decimal::multiplication<int128_t, true>(l, r, result);
}

TEST(Decimal, multiplicationWithOverflowCheck)
{
  execplan::IDB_Decimal l, r, result;
  // result.scale >= l.scale + r.scale
  l.scale = 0;
  l.precision = 38;
  l.s128Value = UINT64_MAX;

  r.scale = 0;
  r.precision = 38;
  r.s128Value = UINT64_MAX;

  result.scale = 0;
  result.precision = 38;
  result.s128Value = 42;
  EXPECT_THROW(doMultiply(l, r, result), logging::OperationOverflowExcept);

  // result.scale < l.scale + r.scale
  l.scale = 36;
  l.precision = 38;
  l.s128Value = (((int128_t)1234567890123456789ULL * 10000000000000000000ULL) + 1234567890123456789);

  r.scale = 36;
  r.precision = 38;
  r.s128Value = (((int128_t)1234567890123456789ULL * 10000000000000000000ULL) + 1234567890123456789);

  result.scale = 38;
  result.precision = 38;
  result.s128Value = 42;
  EXPECT_THROW(doMultiply(l, r, result), logging::OperationOverflowExcept);

  // Normal execution w/o overflow
  l.scale = 0;
  l.s128Value = 4611686018427387904;

  r.scale = 0;
  r.s128Value = 4611686018427387904;

  result.scale = 0;
  result.s128Value = 0;

  EXPECT_NO_THROW(doMultiply(l, r, result));
  EXPECT_EQ("21267647932558653966460912964485513216", result.toString());

  l.setTSInt128Value((int128_t)1 << 122);
  l.setScale(0);
  r.setTSInt128Value(100);
  r.setScale(0);
  EXPECT_THROW(doMultiply(l, r, result), logging::OperationOverflowExcept);

  l.setTSInt128Value((int128_t)1 << 65);
  l.setScale(0);
  r.setTSInt128Value((int128_t)1 << 64);
  r.setScale(0);
  EXPECT_THROW(doMultiply(l, r, result), logging::OperationOverflowExcept);

  l.setTSInt128Value((int128_t)1 << 122);
  l.setScale(0);
  r.setTSInt128Value(2);
  r.setScale(0);
  EXPECT_NO_THROW(doMultiply(l, r, result));
  EXPECT_EQ("10633823966279326983230456482242756608", result.toString());
}

TEST(Decimal, DecimalToStringCheckScale0)
{
  string input, expected;
  int128_t res;
  int precision = 38;
  int scale = 0;
  res = 0;
  datatypes::Decimal dec(0, scale, precision, res);

  // test simple values
  expected = "0";
  EXPECT_EQ(dec.toString(), expected);
  res = 2;
  expected = "2";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -2;
  expected = "-2";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = 123;
  expected = "123";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -123;
  expected = "-123";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);

  // test max/min decimal (i.e. 38 9's)
  res = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
          999999999) *
         100) +
        99;
  expected = "99999999999999999999999999999999999999";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -res;
  expected = "-99999999999999999999999999999999999999";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);

  // test trailing zeros
  res = 123000;
  expected = "123000";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -res;
  expected = "-123000";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
}
TEST(Decimal, DecimalToStringCheckScale10)
{
  string input, expected;
  int128_t res;
  int precision = 38;
  int scale = 10;
  res = 0;
  datatypes::Decimal dec(0, scale, precision, res);

  // test simple values
  expected = "0.0000000000";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);

  res = 2;
  expected = "0.0000000002";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);

  res = -2;
  expected = "-0.0000000002";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);

  res = 123;
  expected = "0.0000000123";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -123;
  expected = "-0.0000000123";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = 12345678901;
  expected = "1.2345678901";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -12345678901;
  expected = "-1.2345678901";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);

  // test max/min decimal (i.e. 38 9's)
  res = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
          999999999) *
         100) +
        99;
  expected = "9999999999999999999999999999.9999999999";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -res;
  expected = "-9999999999999999999999999999.9999999999";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);

  // test trailing zeros
  res = 123000;
  expected = "0.0000123000";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -res;
  expected = "-0.0000123000";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);

  // test leading zeros
  res = 10000000009;
  expected = "1.0000000009";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -res;
  expected = "-1.0000000009";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
}

TEST(Decimal, DecimalToStringCheckScale38)
{
  string input, expected;
  int128_t res;
  int precision = 38;
  int scale = 38;
  res = 0;
  datatypes::Decimal dec(0, scale, precision, res);

  // test simple values
  res = 0;
  expected = "0.00000000000000000000000000000000000000";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = 2;
  expected = "0.00000000000000000000000000000000000002";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -2;
  expected = "-0.00000000000000000000000000000000000002";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = 123;
  expected = "0.00000000000000000000000000000000000123";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -123;
  expected = "-0.00000000000000000000000000000000000123";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = ((((((int128_t)1234567890 * 10000000000) + 1234567890) * 10000000000) + 1234567890) * 100000000) +
        12345678;
  expected = "0.12345678901234567890123456789012345678";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -res;
  expected = "-0.12345678901234567890123456789012345678";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);

  // test max/min decimal (i.e. 38 9's)
  res = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
          999999999) *
         100) +
        99;
  expected = "0.99999999999999999999999999999999999999";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -res;
  expected = "-0.99999999999999999999999999999999999999";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);

  // test trailing zeros
  res = 123000;
  expected = "0.00000000000000000000000000000000123000";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -res;
  expected = "-0.00000000000000000000000000000000123000";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
}

TEST(Decimal, DecimalToStringCheckScale37)
{
  string input, expected;
  int128_t res;
  int precision = 38;
  int scale = 37;
  res = 0;
  datatypes::Decimal dec(0, scale, precision, res);

  // test simple values
  res = 0;
  expected = "0.0000000000000000000000000000000000000";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = 2;
  expected = "0.0000000000000000000000000000000000002";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -2;
  expected = "-0.0000000000000000000000000000000000002";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = 123;
  expected = "0.0000000000000000000000000000000000123";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -123;
  expected = "-0.0000000000000000000000000000000000123";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = ((((((int128_t)1234567890 * 10000000000) + 1234567890) * 10000000000) + 1234567890) * 100000000) +
        12345678;
  expected = "1.2345678901234567890123456789012345678";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -res;
  expected = "-1.2345678901234567890123456789012345678";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);

  // test max/min decimal (i.e. 38 9's)
  res = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
          999999999) *
         100) +
        99;
  expected = "9.9999999999999999999999999999999999999";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -res;
  expected = "-9.9999999999999999999999999999999999999";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);

  // test trailing zeros
  res = 123000;
  expected = "0.0000000000000000000000000000000123000";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
  res = -res;
  expected = "-0.0000000000000000000000000000000123000";
  dec.setTSInt128Value(res);
  EXPECT_EQ(dec.toString(), expected);
}

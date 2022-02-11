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

#include <bitset>
using namespace std;

#include "gtest/gtest.h"

#include "dataconvert.h"
using namespace dataconvert;
#include "joblisttypes.h"
#include "columnwidth.h"

TEST(DataConvertTest, Strtoll128)
{
  char* ep = NULL;
  bool saturate = false;
  string str;
  int128_t val, valMax;
  bitset<64> b1, b2, b3, b4;

  // test empty
  str = "";
  val = strtoll128(str.c_str(), saturate, &ep);
  EXPECT_EQ(val, 0);
  EXPECT_EQ(*ep, '\0');
  EXPECT_FALSE(saturate);

  // test simple values
  str = "123";
  saturate = false;
  val = strtoll128(str.c_str(), saturate, &ep);
  EXPECT_EQ(val, 123);
  EXPECT_EQ(*ep, '\0');
  EXPECT_FALSE(saturate);
  str = "  123";
  saturate = false;
  val = strtoll128(str.c_str(), saturate, &ep);
  EXPECT_EQ(val, 123);
  EXPECT_EQ(*ep, '\0');
  EXPECT_FALSE(saturate);
  str = "    123.45";
  saturate = false;
  val = strtoll128(str.c_str(), saturate, &ep);
  EXPECT_EQ(val, 123);
  EXPECT_NE(*ep, '\0');
  EXPECT_FALSE(saturate);
  str = "-123";
  saturate = false;
  val = strtoll128(str.c_str(), saturate, &ep);
  EXPECT_EQ(val, -123);
  EXPECT_EQ(*ep, '\0');
  EXPECT_FALSE(saturate);
  str = "  -123";
  saturate = false;
  val = strtoll128(str.c_str(), saturate, &ep);
  EXPECT_EQ(val, -123);
  EXPECT_EQ(*ep, '\0');
  EXPECT_FALSE(saturate);
  str = "    -123.45";
  saturate = false;
  val = strtoll128(str.c_str(), saturate, &ep);
  EXPECT_EQ(val, -123);
  EXPECT_NE(*ep, '\0');
  EXPECT_FALSE(saturate);

  // test max/min values
  // test max
  str = "170141183460469231731687303715884105727";
  saturate = false;
  val = strtoll128(str.c_str(), saturate, &ep);
  b1 = *(reinterpret_cast<const uint64_t*>(&val));
  b2 = *(reinterpret_cast<const uint64_t*>(&val) + 1);
  valMax = ((((((((int128_t)170141183 * 1000000000) + 460469231) * 1000000000) + 731687303) * 1000000000) +
             715884105) *
            1000) +
           727;
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_EQ(*ep, '\0');
  EXPECT_FALSE(saturate);
  // test min
  str = "-170141183460469231731687303715884105728";
  saturate = false;
  val = strtoll128(str.c_str(), saturate, &ep);
  b1 = *(reinterpret_cast<const uint64_t*>(&val));
  b2 = *(reinterpret_cast<const uint64_t*>(&val) + 1);
  valMax = ((((((((int128_t)170141183 * 1000000000) + 460469231) * 1000000000) + 731687303) * 1000000000) +
             715884105) *
            1000) +
           727;
  valMax = -valMax - 1;
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_EQ(*ep, '\0');
  EXPECT_FALSE(saturate);

  // test saturation
  // test saturation to max
  str = "170141183460469231731687303715884105728";
  saturate = false;
  val = strtoll128(str.c_str(), saturate, &ep);
  b1 = *(reinterpret_cast<const uint64_t*>(&val));
  b2 = *(reinterpret_cast<const uint64_t*>(&val) + 1);
  valMax = ((((((((int128_t)170141183 * 1000000000) + 460469231) * 1000000000) + 731687303) * 1000000000) +
             715884105) *
            1000) +
           727;
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_EQ(*ep, '\0');
  EXPECT_TRUE(saturate);
  // test saturation to min
  str = "-170141183460469231731687303715884105729";
  saturate = false;
  val = strtoll128(str.c_str(), saturate, &ep);
  b1 = *(reinterpret_cast<const uint64_t*>(&val));
  b2 = *(reinterpret_cast<const uint64_t*>(&val) + 1);
  valMax = ((((((((int128_t)170141183 * 1000000000) + 460469231) * 1000000000) + 731687303) * 1000000000) +
             715884105) *
            1000) +
           727;
  valMax = -valMax - 1;
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_EQ(*ep, '\0');
  EXPECT_TRUE(saturate);
}

TEST(DataConvertTest, NumberIntValue)
{
  datatypes::SystemCatalog::TypeAttributesStd ct;

  int128_t res, valMax;
  string data;
  bool noRoundup = false;
  bool pushWarning;
  bitset<64> b1, b2, b3, b4;

  // tests for signed decimal
  // behaviour of number_int_value for unsigned decimal
  // is similar to the signed case.
  datatypes::SystemCatalog::ColDataType typecode = datatypes::SystemCatalog::DECIMAL;
  // test with decimal(38,0)
  ct.precision = 38;
  ct.scale = 0;
  // test simple values
  // data = "";
  data = "0";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, 0);
  EXPECT_FALSE(pushWarning);
  data = "1234";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, 1234);
  EXPECT_FALSE(pushWarning);
  data = "12.0";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, 12);
  EXPECT_FALSE(pushWarning);
  data = "12.34";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, 12);
  EXPECT_TRUE(pushWarning);
  data = "-1234";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, -1234);
  EXPECT_FALSE(pushWarning);
  data = "-12.34";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, -12);
  EXPECT_TRUE(pushWarning);
  // test max
  data = "99999999999999999999999999999999999999";
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_FALSE(pushWarning);
  // test min
  data = "-99999999999999999999999999999999999999";
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  valMax = -valMax;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_FALSE(pushWarning);
  // test rounding
  data = "12.56";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, 13);
  EXPECT_TRUE(pushWarning);
  data = "-12.56";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, -13);
  EXPECT_TRUE(pushWarning);
  // test saturation
  data = "999999999999999999999999999999999999999";  // data has 39 9's
  // valMax has 38 9's
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);
  data = "-999999999999999999999999999999999999999";  // data has 39 9's
  // valMax has 38 9's
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  valMax = -valMax;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);
  // test scientific notation
  data = "1.23e37";
  valMax = ((((((((int128_t)123000000 * 1000000000) + 0) * 1000000000) + 0) * 1000000000) + 0) * 100) + 0;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_FALSE(pushWarning);
  data = "1.23e38";
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);

  // test with decimal(38,10)
  ct.scale = 10;
  data = "0";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, 0);
  EXPECT_FALSE(pushWarning);
  data = "1234";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, 12340000000000);
  EXPECT_FALSE(pushWarning);
  data = "12.0";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, 120000000000);
  EXPECT_FALSE(pushWarning);
  data = "12.34";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, 123400000000);
  EXPECT_FALSE(pushWarning);
  data = "-1234";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, -12340000000000);
  EXPECT_FALSE(pushWarning);
  data = "-12.34";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, -123400000000);
  EXPECT_FALSE(pushWarning);
  // test max
  data = "9999999999999999999999999999.9999999999";
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_FALSE(pushWarning);
  // test min
  data = "-9999999999999999999999999999.9999999999";
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  valMax = -valMax;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_FALSE(pushWarning);
  // test rounding
  data = "12.11111111119";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, 121111111112);
  EXPECT_TRUE(pushWarning);
  data = "-12.11111111119";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, -121111111112);
  EXPECT_TRUE(pushWarning);
  // test saturation
  data = "99999999999999999999999999999";  // data has 29 9's
  // valMax has 38 9's
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);
  data = "-99999999999999999999999999999";  // data has 29 9's
  // valMax has 38 9's
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  valMax = -valMax;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);
  // test scientific notation
  data = "1.23e9";
  valMax = ((((int128_t)123000000 * 1000000000) + 0) * 100);
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_FALSE(pushWarning);
  data = "1.23e28";
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);
  // test with decimal(38,38)
  ct.scale = 38;
  data = "0";
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  EXPECT_EQ(res, 0);
  EXPECT_FALSE(pushWarning);
  data = "1.234";
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);
  data = "0.123";
  valMax = ((((((((int128_t)123000000 * 1000000000) + 0) * 1000000000) + 0) * 1000000000) + 0) * 100) + 0;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_FALSE(pushWarning);
  data = "-1.234";
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  valMax = -valMax;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);
  data = "-0.123";
  valMax = ((((((((int128_t)123000000 * 1000000000) + 0) * 1000000000) + 0) * 1000000000) + 0) * 100) + 0;
  valMax = -valMax;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_FALSE(pushWarning);
  // test max
  data = "0.99999999999999999999999999999999999999";
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_FALSE(pushWarning);
  // test min
  data = "-0.99999999999999999999999999999999999999";
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  valMax = -valMax;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_FALSE(pushWarning);
  // test rounding
  data = "0.199999999999999999999999999999999999999";
  valMax = ((((((((int128_t)200000000 * 1000000000) + 0) * 1000000000) + 0) * 1000000000) + 0) * 100) + 0;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);
  data = "-0.199999999999999999999999999999999999999";
  valMax = ((((((((int128_t)200000000 * 1000000000) + 0) * 1000000000) + 0) * 1000000000) + 0) * 100) + 0;
  valMax = -valMax;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);
  // test saturation
  data = "99999999999999999999999999999";  // data has 29 9's
  // valMax has 38 9's
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);
  data = "-99999999999999999999999999999";  // data has 29 9's
  // valMax has 38 9's
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  valMax = -valMax;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);
  // test scientific notation
  data = "123e-4";
  valMax = ((((((((int128_t)123000000 * 1000000000) + 0) * 1000000000) + 0) * 1000000000) + 0) * 10) + 0;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_FALSE(pushWarning);
  data = "123e-2";
  valMax = ((((((((int128_t)999999999 * 1000000000) + 999999999) * 1000000000) + 999999999) * 1000000000) +
             999999999) *
            100) +
           99;
  pushWarning = false;
  number_int_value(data, typecode, ct, pushWarning, noRoundup, res);
  b1 = *(reinterpret_cast<const uint64_t*>(&res));
  b2 = *(reinterpret_cast<const uint64_t*>(&res) + 1);
  b3 = *(reinterpret_cast<const uint64_t*>(&valMax));
  b4 = *(reinterpret_cast<const uint64_t*>(&valMax) + 1);
  EXPECT_EQ(b1, b3);
  EXPECT_EQ(b2, b4);
  EXPECT_TRUE(pushWarning);
}
TEST(DataConvertTest, ConvertColumnData)
{
}

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

#include "gtest/gtest.h"

#include "treenode.h"
#include "mcs_decimal.h"
#include "widedecimalutils.h"

TEST(Decimal, compareCheck)
{
    // L values = R value, L scale < R scale
    execplan::IDB_Decimal l, r;
    l.scale = 20;
    l.precision = 38;
    l.s128Value = 42;

    r.scale = 21;
    l.precision = 38;
    r.s128Value = 420;
    EXPECT_EQ(0, datatypes::Decimal::compare(l, r));
    // L values = R value, L scale > R scale
    l.scale = 21;
    l.precision = 38;
    l.s128Value = 420;

    r.scale = 20;
    l.precision = 38;
    r.s128Value = 42;
    EXPECT_EQ(0, datatypes::Decimal::compare(l, r));
    // L values > R value, L scale < R scale
    l.scale = 20;
    l.precision = 38;
    l.s128Value = 999999;

    r.scale = 21;
    l.precision = 38;
    r.s128Value = 420;
    EXPECT_EQ(1, datatypes::Decimal::compare(l, r));
    // L values > R value, L scale > R scale
    l.scale = 21;
    l.precision = 38;
    l.s128Value = 99999999;

    r.scale = 20;
    l.precision = 38;
    r.s128Value = 420;
    EXPECT_EQ(1, datatypes::Decimal::compare(l, r));
    // L values < R value, L scale < R scale
    l.scale = 20;
    l.precision = 38;
    l.s128Value = 99;

    r.scale = 21;
    l.precision = 38;
    r.s128Value = 42000;
    EXPECT_EQ(-1, datatypes::Decimal::compare(l, r));
    // L values < R value, L scale > R scale
    l.scale = 21;
    l.precision = 38;
    l.s128Value = 99;

    r.scale = 20;
    l.precision = 38;
    r.s128Value = 420;
    EXPECT_EQ(-1, datatypes::Decimal::compare(l, r));
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
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    EXPECT_EQ(462, result.s128Value);
    // same precision, same scale, both negative values
    l.scale = 38;
    l.precision = 38;
    l.s128Value = -42;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = -420;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    EXPECT_EQ(-462, result.s128Value);
    // same precision, same scale, +- values
    l.scale = 38;
    l.precision = 38;
    l.s128Value = 42;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = -420;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    EXPECT_EQ(-378, result.s128Value);
    // same precision, same scale, both 0
    l.scale = 38;
    l.precision = 38;
    l.s128Value = 0;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = 0;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    EXPECT_EQ(0, result.s128Value);
    // diff scale
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
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    int128_t s128ScaleMultiplier1 =
        static_cast<int128_t>(10000000000000)*10000000000;
    int128_t s128Result = r.s128Value*s128ScaleMultiplier1+l.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale > R scale, both negative values
    l.scale = 38;
    l.precision = 38;
    l.s128Value = -42;

    r.scale = 15;
    r.precision = 38;
    r.s128Value = -420;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = r.s128Value*s128ScaleMultiplier1+l.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale > R scale, +- values
    l.scale = 38;
    l.precision = 38;
    l.s128Value = 42;

    r.scale = 15;
    r.precision = 38;
    r.s128Value = -420;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = r.s128Value*s128ScaleMultiplier1+l.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale > R scale, both 0
    l.scale = 38;
    l.precision = 38;
    l.s128Value = 0;

    r.scale = 15;
    r.precision = 38;
    r.s128Value = 0;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
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
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = l.s128Value*s128ScaleMultiplier1+r.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale < R scale, both negative values
    l.scale = 15;
    l.precision = 38;
    l.s128Value = -42;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = -420;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = l.s128Value*s128ScaleMultiplier1+r.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale < R scale, +- values
    l.scale = 15;
    l.precision = 38;
    l.s128Value = 42;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = -420;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = l.s128Value*s128ScaleMultiplier1+r.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale < R scale, both 0
    l.scale = 15;
    l.precision = 38;
    l.s128Value = 0;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = 0;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = l.s128Value*s128ScaleMultiplier1+r.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
}

TEST(Decimal, divisionNoOverflowCheck)
{
    // DIVISION
    // same precision, same scale, both positive values
    std::string decimalStr;
    execplan::IDB_Decimal l, r, result;
    l.scale = 38;
    l.precision = 38;
    l.s128Value = 43;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = 420;
    
    result.scale = r.scale;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    
    EXPECT_EQ(r.scale, result.scale);
    EXPECT_EQ(result.precision, result.precision);
    EXPECT_EQ(r.s128Value/l.s128Value, result.s128Value);
    // same precision, same scale, both negative values
    l.scale = 38;
    l.precision = 38;
    l.s128Value = -42;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = -420;
    
    result.scale = r.scale;
    result.precision = r.precision;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    EXPECT_EQ(r.scale, result.scale);
    EXPECT_EQ(r.precision, result.precision);
    EXPECT_EQ(r.s128Value/l.s128Value, result.s128Value);
    // same precision, same scale, +- values
    l.scale = 38;
    l.precision = 38;
    l.s128Value = 42;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = -420;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    EXPECT_EQ(l.s128Value/r.s128Value, result.s128Value);
    // same precision, same scale, l = 0
    l.scale = 38;
    l.precision = 38;
    l.s128Value = 0;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = 42424242;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    EXPECT_EQ(0, result.s128Value);
    // diff scale
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

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    int128_t s128ScaleMultiplier1 =
        static_cast<int128_t>(10000000000000)*10000000000;
    int128_t s128Result = r.s128Value*s128ScaleMultiplier1/l.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale > R scale, both negative values
    l.scale = 38;
    l.precision = 38;
    l.s128Value = -42;

    r.scale = 15;
    r.precision = 38;
    r.s128Value = -420;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = r.s128Value*s128ScaleMultiplier1/l.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale > R scale, +- values
    l.scale = 38;
    l.precision = 38;
    l.s128Value = 42;

    r.scale = 15;
    r.precision = 38;
    r.s128Value = -420;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = r.s128Value*s128ScaleMultiplier1/l.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale > R scale, L = 0
    l.scale = 38;
    l.precision = 38;
    l.s128Value = 0;

    r.scale = 15;
    r.precision = 38;
    r.s128Value = 424242;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    EXPECT_EQ(0, result.s128Value);
    // same precision, L scale > R scale, both MAX positive values
    // WIP Investigate the next two
    l.scale = 38;
    l.precision = 38;
    l.s128Value = 0; utils::int128Max(l.s128Value);

    r.scale = 15;
    r.precision = 38;
    r.s128Value = 0; utils::int128Max(r.s128Value);
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(l, r, result);
    // Use as an examplar
    utils::int128Max(r.s128Value);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = r.s128Value*s128ScaleMultiplier1/l.s128Value;
    // WIP
    //EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale > R scale, both MIN negative values
    l.scale = 38;
    l.precision = 38;
    l.s128Value = 0; utils::int128Min(l.s128Value);

    r.scale = 15;
    r.precision = 38;
    r.s128Value = 0; utils::int128Min(l.s128Value);
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    //datatypes::Decimal::division<int128_t, false>(l, r, result);
    // Use as an examplar
    utils::int128Min(r.s128Value);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = r.s128Value*s128ScaleMultiplier1/l.s128Value;
    //EXPECT_EQ(s128Result, result.s128Value);
    // WIP
    //EXPECT_EQ(r.s128Value, result.s128Value);

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

    datatypes::Decimal::division<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = l.s128Value*s128ScaleMultiplier1/r.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale < R scale, both negative values
    l.scale = 15;
    l.precision = 38;
    l.s128Value = -42;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = -420;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = l.s128Value*s128ScaleMultiplier1/r.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale < R scale, +- values
    l.scale = 15;
    l.precision = 38;
    l.s128Value = 42;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = -420;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = l.s128Value*s128ScaleMultiplier1/r.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale < R scale, L = 0
    l.scale = 15;
    l.precision = 38;
    l.s128Value = 0;

    r.scale = 38;
    r.precision = 38;
    r.s128Value = 42;
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(l, r, result);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = l.s128Value*s128ScaleMultiplier1/r.s128Value;
    EXPECT_EQ(s128Result, result.s128Value);
    // same precision, L scale < R scale, both MAX positive values
    // WIP Investigate the next two
    l.scale = 15;
    l.precision = 38;
    l.s128Value = 0; utils::int128Max(l.s128Value);

    r.scale = 38;
    r.precision = 38;
    r.s128Value = 0; utils::int128Max(r.s128Value);
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(l, r, result);
    // Use as an examplar
    utils::int128Max(r.s128Value);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = l.s128Value*s128ScaleMultiplier1/r.s128Value;
    // WIP
    //EXPECT_EQ(s128Result, result.s128Value);
    //EXPECT_EQ(r.s128Value, result.s128Value);
    // same precision, L scale < R scale, both MIN negative values
    l.scale = 15;
    l.precision = 38;
    l.s128Value = 0; utils::int128Min(l.s128Value);

    r.scale = 38;
    r.precision = 38;
    r.s128Value = 0; utils::int128Min(l.s128Value);
    
    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    //datatypes::Decimal::division<int128_t, false>(l, r, result);
    // Use as an examplar
    utils::int128Min(r.s128Value);
    EXPECT_EQ(38, result.scale);
    EXPECT_EQ(38, result.precision);
    s128Result = l.s128Value*s128ScaleMultiplier1/r.s128Value;
    // WIP
    // EXPECT_EQ(s128Result, result.s128Value);
    //EXPECT_EQ(r.s128Value, result.s128Value);
}

void doDiv(execplan::IDB_Decimal& l,
            execplan::IDB_Decimal& r,
            execplan::IDB_Decimal& result)
{
    datatypes::Decimal::division<int128_t, true>(l, r, result);
}

TEST(Decimal, divisionWithOverflowCheck)
{

    // Divide max int128 by -1
    execplan::IDB_Decimal l, r, result;
    l.scale = 0;
    l.precision = 38;
    l.s128Value = datatypes::Decimal::maxInt128; 

    r.scale = 0;
    r.precision = 38;
    r.s128Value = -1;
    
    result.scale = 0;
    result.precision = 38;
    result.s128Value = 42; 
    EXPECT_THROW(doDiv(l, r, result), logging::OperationOverflowExcept);
    // Divide two ints one of which overflows after the scaling.
    l.scale = 0;
    l.precision = 38;
    l.s128Value = datatypes::Decimal::maxInt128; 

    r.scale = 1;
    r.precision = 38;
    r.s128Value = 42;
    
    result.scale = 1;
    result.precision = 38;
    result.s128Value = 42; 
    EXPECT_THROW(doDiv(l, r, result), logging::OperationOverflowExcept);
    // Normal execution w/o overflow
    l.scale = 0;
    l.precision = 38;
    l.s128Value = datatypes::Decimal::maxInt128-1;

    r.scale = 0;
    r.precision = 38;
    r.s128Value = 0xFFFFFFFFFFFFFFFF;
    
    result.scale = 0;
    result.precision = 38;
    result.s128Value = 0;

    EXPECT_NO_THROW(doDiv(l, r, result));

    l.s128Value /= r.s128Value;

    EXPECT_EQ(0, result.scale);
    EXPECT_EQ(38, result.precision);
    EXPECT_EQ(l.s128Value, result.s128Value);
}

void doAdd(execplan::IDB_Decimal& l,
            execplan::IDB_Decimal& r,
            execplan::IDB_Decimal& result)
{
    datatypes::Decimal::addition<int128_t, true>(l, r, result);
}

TEST(Decimal, additionWithOverflowCheck)
{
    // Add two max ints
    execplan::IDB_Decimal l, r, result;
    l.scale = 0;
    l.precision = 38;
    l.s128Value = datatypes::Decimal::maxInt128-1;

    r.scale = 0;
    r.precision = 38;
    r.s128Value = datatypes::Decimal::maxInt128-1;
    
    result.scale = 0;
    result.precision = 38;
    result.s128Value = 42; 
    EXPECT_THROW(doAdd(l, r, result), logging::OperationOverflowExcept);
    // Add two ints one of which overflows after the scaling.
    l.scale = 0;
    l.precision = 38;
    l.s128Value = datatypes::Decimal::maxInt128-1;

    r.scale = 1;
    r.precision = 38;
    r.s128Value = 0xFFFFFFFFFFFFFFFF;
    
    result.scale = 1;
    result.precision = 38;
    result.s128Value = 0;

    EXPECT_THROW(doAdd(l, r, result), logging::OperationOverflowExcept);
    // Normal execution w/o overflow
    l.scale = 0;
    l.precision = 38;
    l.s128Value = datatypes::Decimal::maxInt128-1;

    r.scale = 0;
    r.precision = 38;
    r.s128Value = 0xFFFFFFFFFFFFFFFF;
    
    result.scale = 0;
    result.precision = 38;
    result.s128Value = 0;

    EXPECT_NO_THROW(doDiv(l, r, result));

    l.s128Value /= r.s128Value;

    EXPECT_EQ(0, result.scale);
    EXPECT_EQ(38, result.precision);
    EXPECT_EQ(l.s128Value, result.s128Value);
}

TEST(Decimal, multiplicationWithOverflowCheck)
{
    datatypes::MultiplicationOverflowCheck mul;
    int128_t x = 42, y = 42, r = 0;
    execplan::IDB_Decimal d;
    EXPECT_NO_THROW(mul(x, y, r));
    EXPECT_EQ(x*y, r);

    x = datatypes::Decimal::maxInt128, y = 42, r = 0;
    EXPECT_THROW(mul(x, y, r), logging::OperationOverflowExcept);
}

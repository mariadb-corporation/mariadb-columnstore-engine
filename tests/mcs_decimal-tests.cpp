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
    datatypes::SystemCatalog::ColDataType colDataType = datatypes::SystemCatalog::DECIMAL;
    char buf[42];
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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000042000000000000000000000042", std::string(buf));

    // same precision, L scale > R scale, both negative values
    l.s128Value = -42;
    r.s128Value = -420;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-0.00000000000042000000000000000000000042", std::string(buf));

    // same precision, L scale > R scale, +- values
    l.s128Value = 42;
    r.s128Value = -420;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-0.00000000000041999999999999999999999958", std::string(buf));

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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000004200000000000000000000420", std::string(buf));

    // same precision, L scale < R scale, both negative values
    l.s128Value = -42;
    r.s128Value = -420;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-0.00000000000004200000000000000000000420", std::string(buf));

    // same precision, L scale < R scale, +- values
    l.s128Value = 42;
    r.s128Value = -420;
    result.s128Value = 0;

    datatypes::Decimal::addition<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000004199999999999999999999580", std::string(buf));

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
    datatypes::SystemCatalog::ColDataType colDataType = datatypes::SystemCatalog::DECIMAL;
    char buf[42];
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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("9.7674418605", std::string(buf));

    // same precision, same scale, both negative values
    l.s128Value = -43;
    r.s128Value = -420;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("9.7674418605", std::string(buf));

    // same precision, same scale, +- values
    l.s128Value = 2200000;
    r.s128Value = -1900;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-1157.8947368421", std::string(buf));

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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("115789473684210526315789.4736842105", std::string(buf));

    // same precision, L scale > R scale, both negative values
    l.s128Value = -22;
    r.s128Value = -19;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("86363636363636363636363.6363636364", std::string(buf));

    // same precision, L scale > R scale, +- values
    l.s128Value = 19;
    r.s128Value = -22;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-115789473684210526315789.4736842105", std::string(buf));

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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("100000000000000000000000.0000000000", std::string(buf));

    // same precision, L scale > R scale, both MIN negative values
    utils::int128Min(l.s128Value);
    utils::int128Min(r.s128Value);
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("100000000000000000000000.0000000000", std::string(buf));

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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000000000000001000000000000000", std::string(buf));

    // same precision, L scale < R scale, both negative values
    l.s128Value = -22;
    r.s128Value = -19;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000000000000000863636363636364", std::string(buf));

    // same precision, L scale < R scale, +- values
    l.s128Value = 22;
    r.s128Value = -19;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-0.00000000000000000000000863636363636364", std::string(buf));

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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000000000000001000000000000000", std::string(buf));

    // same precision, L scale < R scale, both MIN negative values
    utils::int128Min(l.s128Value);
    utils::int128Min(r.s128Value);
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000000000000001000000000000000", std::string(buf));

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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("1", std::string(buf));

    // same precision, L scale < R scale, result.scale < (r.scale-l.scale)
    // both negative values
    l.s128Value = -22;
    r.s128Value = -1900;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("9", std::string(buf));

    // same precision, L scale < R scale, result.scale < (r.scale-l.scale)
    // +- values
    l.s128Value = 22;
    r.s128Value = -1900;
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-9", std::string(buf));

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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0", std::string(buf));

    // same precision, L scale < R scale, result.scale < (r.scale-l.scale)
    // both MIN negative values
    utils::int128Min(l.s128Value);
    utils::int128Min(r.s128Value);
    result.s128Value = 0;

    datatypes::Decimal::division<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0", std::string(buf));
}

void doDiv(const execplan::IDB_Decimal& l,
           const execplan::IDB_Decimal& r,
           execplan::IDB_Decimal& result)
{
    datatypes::Decimal::division<int128_t, true>(l, r, result);
}

TEST(Decimal, divisionWithOverflowCheck)
{
    datatypes::SystemCatalog::ColDataType colDataType = datatypes::SystemCatalog::DECIMAL;
    char buf[42];
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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("9223372036854775809", std::string(buf));
}

void doAdd(const execplan::IDB_Decimal& l,
           const execplan::IDB_Decimal& r,
           execplan::IDB_Decimal& result)
{
    datatypes::Decimal::addition<int128_t, true>(l, r, result);
}

TEST(Decimal, additionWithOverflowCheck)
{
    datatypes::SystemCatalog::ColDataType colDataType = datatypes::SystemCatalog::DECIMAL;
    char buf[42];
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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-170141183460469231713240559642174554113", std::string(buf));
}

TEST(Decimal, subtractionNoOverflowCheck)
{
    datatypes::SystemCatalog::ColDataType colDataType = datatypes::SystemCatalog::DECIMAL;
    char buf[42];
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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-0.00000000000000000000000000000000000378", std::string(buf));

    // same precision, same scale, both negative values
    l.s128Value = -42;
    r.s128Value = -420;
    result.s128Value = 0;

    datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000000000000000000000000000378", std::string(buf));

    // same precision, same scale, +- values
    l.s128Value = 42;
    r.s128Value = -420;
    result.s128Value = 0;

    datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000000000000000000000000000462", std::string(buf));

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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-0.00000000000041999999999999999999999958", std::string(buf));

    // same precision, L scale > R scale, both negative values
    l.s128Value = -42;
    r.s128Value = -420;
    result.s128Value = 0;

    datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000041999999999999999999999958", std::string(buf));

    // same precision, L scale > R scale, +- values
    l.s128Value = 42;
    r.s128Value = -420;
    result.s128Value = 0;

    datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000042000000000000000000000042", std::string(buf));

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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000004199999999999999999999580", std::string(buf));

    // same precision, L scale < R scale, both negative values
    l.s128Value = -42;
    r.s128Value = -420;
    result.s128Value = 0;

    datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-0.00000000000004199999999999999999999580", std::string(buf));

    // same precision, L scale < R scale, +- values
    l.s128Value = 42;
    r.s128Value = -420;
    result.s128Value = 0;

    datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.00000000000004200000000000000000000420", std::string(buf));

    // same precision, L scale < R scale, both 0
    l.s128Value = 0;
    r.s128Value = 0;
    result.s128Value = 0;

    datatypes::Decimal::subtraction<int128_t, false>(l, r, result);
    EXPECT_EQ(0, result.s128Value);
}

void doSubtract(const execplan::IDB_Decimal& l,
                const execplan::IDB_Decimal& r,
                execplan::IDB_Decimal& result)
{
    datatypes::Decimal::subtraction<int128_t, true>(l, r, result);
}

TEST(Decimal, subtractionWithOverflowCheck)
{
    datatypes::SystemCatalog::ColDataType colDataType = datatypes::SystemCatalog::DECIMAL;
    char buf[42];
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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("170141183460469231713240559642174554112", std::string(buf));
}

TEST(Decimal, multiplicationNoOverflowCheck)
{
    // Multiplication
    // same precision, l.scale + r.scale = result.scale, both positive values
    datatypes::SystemCatalog::ColDataType colDataType = datatypes::SystemCatalog::DECIMAL;
    char buf[42];
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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.85070591730234615861231965839514664960", std::string(buf));

    // same precision, l.scale + r.scale = result.scale, both negative values
    l.s128Value = -4611686018427387904;
    r.s128Value = UINT64_MAX;
    r.s128Value = -r.s128Value;
    result.s128Value = 0;

    datatypes::Decimal::multiplication<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.85070591730234615861231965839514664960", std::string(buf));

    // same precision, l.scale + r.scale = result.scale, +- values
    l.s128Value = -4611686018427387904;
    r.s128Value = UINT64_MAX;
    result.s128Value = 0;

    datatypes::Decimal::multiplication<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-0.85070591730234615861231965839514664960", std::string(buf));

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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.66461399789245793645190353014017228800", std::string(buf));

    // same precision, l.scale + r.scale < result.scale, both negative values
    l.s128Value = -72057594037927936;
    r.s128Value = 9223372036854775808ULL;
    r.s128Value = -r.s128Value;
    result.s128Value = 0;

    datatypes::Decimal::multiplication<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.66461399789245793645190353014017228800", std::string(buf));

    // same precision, l.scale + r.scale < result.scale, +- values
    l.s128Value = -72057594037927936;
    r.s128Value = 9223372036854775808ULL;
    result.s128Value = 0;

    datatypes::Decimal::multiplication<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-0.66461399789245793645190353014017228800", std::string(buf));

    // same precision, l.scale + r.scale < result.scale, l = 0
    l.s128Value = 0;
    r.s128Value = 9223372036854775808ULL;
    result.s128Value = 0;

    datatypes::Decimal::multiplication<int128_t, false>(l, r, result);
    EXPECT_EQ(0, result.s128Value);

    // same precision, l.scale + r.scale > result.scale, both positive values
    l.scale = 38;
    l.precision = 38;
    l.s128Value = (((int128_t)1234567890123456789ULL * 10000000000000000000ULL) +
                     1234567890123456789);

    r.scale = 38;
    r.precision = 38;
    r.s128Value = (((int128_t)1234567890123456789ULL * 10000000000000000000ULL) +
                     1234567890123456789ULL);

    result.scale = 38;
    result.precision = 38;
    result.s128Value = 0;

    datatypes::Decimal::multiplication<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.01524157875323883675019051998750190521", std::string(buf));

    // same precision, l.scale + r.scale > result.scale, both negative values
    l.s128Value = -l.s128Value;
    r.s128Value = -r.s128Value;
    result.s128Value = 0;

    datatypes::Decimal::multiplication<int128_t, false>(r, l, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("0.01524157875323883675019051998750190521", std::string(buf));

    // same precision, l.scale + r.scale > result.scale, +- values
    r.s128Value = -r.s128Value;
    result.s128Value = 0;

    datatypes::Decimal::multiplication<int128_t, false>(l, r, result);
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("-0.01524157875323883675019051998750190521", std::string(buf));

    // same precision, l.scale + r.scale > result.scale, l = 0
    r.s128Value = 0;
    result.s128Value = 0;

    datatypes::Decimal::multiplication<int128_t, false>(l, r, result);
    EXPECT_EQ(0, result.s128Value);
}

void doMultiply(const execplan::IDB_Decimal& l,
                const execplan::IDB_Decimal& r,
                execplan::IDB_Decimal& result)
{
    datatypes::Decimal::multiplication<int128_t, true>(l, r, result);
}

TEST(Decimal, multiplicationWithOverflowCheck)
{
    datatypes::SystemCatalog::ColDataType colDataType = datatypes::SystemCatalog::DECIMAL;
    char buf[42];
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
    l.s128Value = (((int128_t)1234567890123456789ULL * 10000000000000000000ULL) +
                     1234567890123456789);

    r.scale = 36;
    r.precision = 38;
    r.s128Value = (((int128_t)1234567890123456789ULL * 10000000000000000000ULL) +
                     1234567890123456789);

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
    dataconvert::DataConvert::decimalToString(&result.s128Value, result.scale, buf, 42, colDataType);
    EXPECT_EQ("21267647932558653966460912964485513216", std::string(buf));
}

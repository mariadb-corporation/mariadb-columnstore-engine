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

#ifndef H_DECIMALDATATYPE
#define H_DECIMALDATATYPE

#include <cstdint>

#include "calpontsystemcatalog.h"

using int128_t = __int128;

namespace execplan
{
    struct IDB_Decimal;
}

namespace datatypes
{

constexpr uint32_t MAXDECIMALWIDTH = 16U;
constexpr uint8_t INT64MAXPRECISION = 18U;
constexpr uint8_t INT128MAXPRECISION = 38U;

const uint64_t mcs_pow_10[20] =
{
    1ULL,
    10ULL,
    100ULL,
    1000ULL,
    10000ULL,
    100000ULL,
    1000000ULL,
    10000000ULL,
    100000000ULL,
    1000000000ULL,
    10000000000ULL,
    100000000000ULL,
    1000000000000ULL,
    10000000000000ULL,
    100000000000000ULL,
    1000000000000000ULL,
    10000000000000000ULL,
    100000000000000000ULL,
    1000000000000000000ULL,
    10000000000000000000ULL
};

constexpr uint32_t maxPowOf10 = sizeof(mcs_pow_10)/sizeof(mcs_pow_10[0])-1;
constexpr int128_t Decimal128Null = int128_t(0x8000000000000000LL) << 64;

/**
    @brief The function to produce scale multiplier/divisor for
    wide decimals.
*/
inline void getScaleDivisor(int128_t& divisor, const int8_t scale)
{
    divisor = 1;
    switch (scale/maxPowOf10)
    {
        case 2:
            divisor *= mcs_pow_10[maxPowOf10];
            divisor *= mcs_pow_10[maxPowOf10];
            break;
        case 1:
            divisor *= mcs_pow_10[maxPowOf10];
        case 0:
            divisor *= mcs_pow_10[scale%maxPowOf10];
        default:
            break;
    }
}

/**
    @brief The template to generalise common math operation
    execution path using struct from <functional>.
*/
template<typename BinaryOperation, typename OverflowCheck>
void execute(const execplan::IDB_Decimal& l,
    const execplan::IDB_Decimal& r,
    execplan::IDB_Decimal& result,
    BinaryOperation op,
    OverflowCheck overflowCheck);

/**
    @brief Contains subset of decimal related operations.

    Most of the methods are static to allow to call them from
    the existing code.
    The main purpose of the class is to collect methods for a
    base Datatype class.
*/
class Decimal
{
    public:
        Decimal() { };
        ~Decimal() { };

        static constexpr int128_t minInt128 = int128_t(0x8000000000000000LL) << 64;
        static constexpr int128_t maxInt128 = (int128_t(0x7FFFFFFFFFFFFFFFLL) << 64) + 0xFFFFFFFFFFFFFFFFLL;

        /**
            @brief Compares two IDB_Decimal taking scale into account. 
        */
        static int compare(const execplan::IDB_Decimal& l, const execplan::IDB_Decimal& r); 
        /**
            @brief Addition template that supports overflow check and
            two internal representations of decimal.
        */
        template<typename T, bool overflow>
        static void addition(const execplan::IDB_Decimal& l,
            const execplan::IDB_Decimal& r,
            execplan::IDB_Decimal& result);

        /**
            @brief Division template that supports overflow check and
            two internal representations of decimal.
        */
        template<typename T, bool overflow>
        static void division(const execplan::IDB_Decimal& l,
            const execplan::IDB_Decimal& r,
            execplan::IDB_Decimal& result);

        /**
            @brief Convinience method to put decimal into a std:;string.
        */
        static std::string toString(execplan::IDB_Decimal& value);

        /**
            @brief The method detects whether decimal type is wide
            using csc data type.
        */
        static constexpr inline bool isWideDecimalType(const execplan::CalpontSystemCatalog::ColType& ct)
        {
            return ((ct.colDataType == execplan::CalpontSystemCatalog::DECIMAL ||
                ct.colDataType == execplan::CalpontSystemCatalog::UDECIMAL) &&
                ct.colWidth == MAXDECIMALWIDTH);
        }

        /**
            @brief The method detects whether decimal type is wide
            using precision.
        */
        static constexpr inline bool isWideDecimalType(const int32_t precision)
        {
            return precision > INT64MAXPRECISION
                && precision <= INT128MAXPRECISION;
        }

};

/**
    @brief The structure contains an overflow check for int128
    division.
*/
struct DivisionOverflowCheck {
    void operator()(const int128_t& x, const int128_t& y)
    {
        if (x == Decimal::maxInt128 && y == -1)
        {
            throw logging::OperationOverflowExcept(
                "Decimal::division<int128_t> produces an overflow.");
        }
    }
};

/**
    @brief The structure contains an overflow check for int128
    addition.
*/
struct MultiplicationOverflowCheck {
    void operator()(const int128_t& x, const int128_t& y)
    {
        if (x * y / y != x)
        {
            throw logging::OperationOverflowExcept(
                "Decimal::multiplication<int128_t> or scale multiplication \
 produces an overflow.");
        }
    }
    bool operator()(const int128_t& x, const int128_t& y, int128_t& r)
    {
        if ((r = x * y) / y != x)
        {
            throw logging::OperationOverflowExcept(
                "Decimal::multiplication<int128_t> or scale multiplication \
 produces an overflow.");
        }
        return true;
    }

};

/**
    @brief The structure contains an overflow check for int128
    addition.
*/
struct AdditionOverflowCheck {
    void operator()(const int128_t& x, const int128_t& y)
    {
        if ((y > 0 && x > Decimal::maxInt128 - y)
            || (y < 0 && x < Decimal::minInt128 - y))
        {
            throw logging::OperationOverflowExcept(
                "Decimal::addition<int128_t> produces an overflow.");
        }
    }
};

/**
    @brief The strucuture runs an empty overflow check for int128
    operation.
*/
struct NoOverflowCheck {
    void operator()(const int128_t& x, const int128_t& y)
    {
        return;
    }
};

} //end of namespace
#endif

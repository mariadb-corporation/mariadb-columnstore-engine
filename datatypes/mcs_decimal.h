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
#include <cfloat>
#include <limits>
#include "exceptclasses.h"
#include "widedecimalutils.h"

using int128_t = __int128;
using uint128_t = unsigned __int128;

namespace datatypes
{
    struct VDecimal;
}

// A class by Fabio Fernandes pulled off of stackoverflow
// Creates a type _xxl that can be used to create 128bit constant values
// Ex: int128_t i128 = 12345678901234567890123456789_xxl
namespace detail_xxl
{
    constexpr uint8_t hexval(char c) 
    { return c>='a' ? (10+c-'a') : c>='A' ? (10+c-'A') : c-'0'; }

    template <int BASE, uint128_t V>
    constexpr uint128_t lit_eval() { return V; }

    template <int BASE, uint128_t V, char C, char... Cs>
    constexpr uint128_t lit_eval() {
        static_assert( BASE!=16 || sizeof...(Cs) <=  32-1, "Literal too large for BASE=16");
        static_assert( BASE!=10 || sizeof...(Cs) <=  39-1, "Literal too large for BASE=10");
        static_assert( BASE!=8  || sizeof...(Cs) <=  44-1, "Literal too large for BASE=8");
        static_assert( BASE!=2  || sizeof...(Cs) <= 128-1, "Literal too large for BASE=2");
        return lit_eval<BASE, BASE*V + hexval(C), Cs...>();
    }

    template<char... Cs > struct LitEval 
    {static constexpr uint128_t eval() {return lit_eval<10,0,Cs...>();} };

    template<char... Cs> struct LitEval<'0','x',Cs...> 
    {static constexpr uint128_t eval() {return lit_eval<16,0,Cs...>();} };

    template<char... Cs> struct LitEval<'0','b',Cs...> 
    {static constexpr uint128_t eval() {return lit_eval<2,0,Cs...>();} };

    template<char... Cs> struct LitEval<'0',Cs...> 
    {static constexpr uint128_t eval() {return lit_eval<8,0,Cs...>();} };

    template<char... Cs> 
    constexpr uint128_t operator "" _xxl() {return LitEval<Cs...>::eval();}
}

template<char... Cs> 
constexpr uint128_t operator "" _xxl() {return ::detail_xxl::operator "" _xxl<Cs...>();}

namespace datatypes
{

constexpr uint32_t MAXDECIMALWIDTH = 16U;
constexpr uint8_t INT64MAXPRECISION = 18U;
constexpr uint8_t INT128MAXPRECISION = 38U;
constexpr uint8_t MAXLEGACYWIDTH = 8U;
constexpr uint8_t MAXSCALEINC4AVG = 4U;
constexpr int8_t IGNOREPRECISION = -1;



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
    10000000000000000000ULL,
};
const int128_t mcs_pow_10_128[20] =
{
    10000000000000000000_xxl,
    100000000000000000000_xxl,
    1000000000000000000000_xxl,
    10000000000000000000000_xxl,
    100000000000000000000000_xxl,
    1000000000000000000000000_xxl,
    10000000000000000000000000_xxl,
    100000000000000000000000000_xxl,
    1000000000000000000000000000_xxl,
    10000000000000000000000000000_xxl,
    100000000000000000000000000000_xxl,
    1000000000000000000000000000000_xxl,
    10000000000000000000000000000000_xxl,
    100000000000000000000000000000000_xxl,
    1000000000000000000000000000000000_xxl,
    10000000000000000000000000000000000_xxl,
    100000000000000000000000000000000000_xxl,
    1000000000000000000000000000000000000_xxl,
    10000000000000000000000000000000000000_xxl,
    100000000000000000000000000000000000000_xxl,
};

constexpr uint32_t maxPowOf10 = sizeof(mcs_pow_10)/sizeof(mcs_pow_10[0])-1;
constexpr int128_t Decimal128Null = int128_t(0x8000000000000000LL) << 64;
constexpr int128_t Decimal128Empty = (int128_t(0x8000000000000000LL) << 64) + 1;


/**
    @brief The function to produce scale multiplier/divisor for
    wide decimals.
*/
template<typename T>
inline void getScaleDivisor(T& divisor, const int8_t scale)
{
    if (scale < 0)
    {
        std::string msg = "getScaleDivisor called with negative scale: " + std::to_string(scale);
        throw std::invalid_argument(msg);
    }
    else if (scale < 19)
    {
        divisor = mcs_pow_10[scale];
    }
    else
    {
        divisor = mcs_pow_10_128[scale-19];
    }
}


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

        static constexpr uint8_t MAXLENGTH16BYTES = 42;
        static constexpr uint8_t MAXLENGTH8BYTES = 23;

        static inline bool isWideDecimalNullValue(const int128_t& val)
        {
            const uint64_t* ptr = reinterpret_cast<const uint64_t*>(&val);
            return (ptr[0] == utils::BINARYNULLVALUELOW && ptr[1] == utils::BINARYNULLVALUEHIGH);
        }

        static inline bool isWideDecimalEmptyValue(const int128_t& val)
        {
            const uint64_t* ptr = reinterpret_cast<const uint64_t*>(&val);
            return (ptr[0] == utils::BINARYEMPTYVALUELOW && ptr[1] == utils::BINARYEMPTYVALUEHIGH);
        }

        static inline void setWideDecimalNullValue(int128_t& val)
        {
            uint64_t* ptr = reinterpret_cast<uint64_t*>(&val);
            ptr[0] = utils::BINARYNULLVALUELOW;
            ptr[1] = utils::BINARYNULLVALUEHIGH;
        }

        static inline void setWideDecimalEmptyValue(int128_t& val)
        {
            uint64_t* ptr = reinterpret_cast<uint64_t*>(&val);
            ptr[0] = utils::BINARYEMPTYVALUELOW;
            ptr[1] = utils::BINARYEMPTYVALUEHIGH;
        }

        static inline void setWideDecimalNullValue(int128_t* val)
        {
            uint64_t* ptr = reinterpret_cast<uint64_t*>(val);
            ptr[0] = utils::BINARYNULLVALUELOW;
            ptr[1] = utils::BINARYNULLVALUEHIGH;
        }

        static inline void setWideDecimalEmptyValue(int128_t* val)
        {
            uint64_t* ptr = reinterpret_cast<uint64_t*>(val);
            ptr[0] = utils::BINARYEMPTYVALUELOW;
            ptr[1] = utils::BINARYEMPTYVALUEHIGH;
        }


        static constexpr int128_t minInt128 = int128_t(0x8000000000000000LL) << 64;
        static constexpr int128_t maxInt128 = (int128_t(0x7FFFFFFFFFFFFFFFLL) << 64) + 0xFFFFFFFFFFFFFFFFLL;

        /**
            @brief Compares two VDecimal taking scale into account. 
        */
        static int compare(const VDecimal& l, const VDecimal& r); 
        /**
            @brief Addition template that supports overflow check and
            two internal representations of decimal.
        */
        template<typename T, bool overflow>
        static void addition(const VDecimal& l,
            const VDecimal& r,
            VDecimal& result);

        /**
            @brief Subtraction template that supports overflow check and
            two internal representations of decimal.
        */
        template<typename T, bool overflow>
        static void subtraction(const VDecimal& l,
            const VDecimal& r,
            VDecimal& result);

        /**
            @brief Division template that supports overflow check and
            two internal representations of decimal.
        */
        template<typename T, bool overflow>
        static void division(const VDecimal& l,
            const VDecimal& r,
            VDecimal& result);

        /**
            @brief Multiplication template that supports overflow check and
            two internal representations of decimal.
        */
        template<typename T, bool overflow>
        static void multiplication(const VDecimal& l,
            const VDecimal& r,
            VDecimal& result);

        /**
            @brief Convenience methods to put decimal into a std::string.
        */
        static std::string toString(VDecimal& value);
        static std::string toString(const VDecimal& value);
        static std::string toString(const int128_t& value);

        /**
            @brief The method detects whether decimal type is wide
            using precision.
        */
        static constexpr inline bool isWideDecimalTypeByPrecision(const int32_t precision)
        {
            return precision > INT64MAXPRECISION
                && precision <= INT128MAXPRECISION;
        }

        /**
            @brief The method converts a __float128 value to an int64_t.
        */
        static inline int64_t getInt64FromFloat128(const __float128& value)
        {
            if (value > static_cast<__float128>(INT64_MAX))
                return INT64_MAX;
            else if (value < static_cast<__float128>(INT64_MIN))
                return INT64_MIN;

            return static_cast<int64_t>(value);
        }

        /**
            @brief The method converts a __float128 value to an uint64_t.
        */
        static inline uint64_t getUInt64FromFloat128(const __float128& value)
        {
            if (value > static_cast<__float128>(UINT64_MAX))
                return UINT64_MAX;
            else if (value < 0)
                return 0;

            return static_cast<uint64_t>(value);
        }

        /**
            @brief The method converts a wide decimal value to an uint32_t.
        */
        static inline uint32_t getUInt32FromWideDecimal(const int128_t& value)
        {
            if (value > static_cast<int128_t>(UINT32_MAX))
                return UINT32_MAX;
            else if (value < 0)
                return 0;

            return static_cast<uint32_t>(value);
        }

        /**
            @brief The method converts a wide decimal value to an int32_t.
        */
        static inline int32_t getInt32FromWideDecimal(const int128_t& value)
        {
            if (value > static_cast<int128_t>(INT32_MAX))
                return INT32_MAX;
            else if (value < static_cast<int128_t>(INT32_MIN))
                return INT32_MIN;

            return static_cast<int32_t>(value);
        }

        /**
            @brief The method converts a wide decimal value to an uint64_t.
        */
        static inline uint64_t getUInt64FromWideDecimal(const int128_t& value)
        {
            if (value > static_cast<int128_t>(UINT64_MAX))
                return UINT64_MAX;
            else if (value < 0)
                return 0;

            return static_cast<uint64_t>(value);
        }

        /**
            @brief The method converts a __float128 value to a double.
        */
        static inline double getDoubleFromFloat128(const __float128& value)
        {
            if (value > static_cast<__float128>(DBL_MAX))
                return DBL_MAX;
            else if (value < -static_cast<__float128>(DBL_MAX))
                return -DBL_MAX;

            return static_cast<double>(value);
        }

        /**
            @brief The method converts a wide decimal value to a double.
        */
        static inline double getDoubleFromWideDecimal(const int128_t& value, int8_t scale)
        {
            int128_t scaleDivisor;

            getScaleDivisor(scaleDivisor, scale);

            __float128 tmpval = (__float128) value / scaleDivisor;

            return getDoubleFromFloat128(tmpval);
        }

        /**
            @brief The method converts a wide decimal value to a double.
        */
        static inline double getDoubleFromWideDecimal(const int128_t& value)
        {
            return getDoubleFromFloat128(static_cast<__float128>(value));
        }

        /**
            @brief The method converts a __float128 value to a long double.
        */
        static inline long double getLongDoubleFromFloat128(const __float128& value)
        {
            if (value > static_cast<__float128>(LDBL_MAX))
                return LDBL_MAX;
            else if (value < -static_cast<__float128>(LDBL_MAX))
                return -LDBL_MAX;

            return static_cast<long double>(value);
        }

        /**
            @brief The method converts a wide decimal value to a long double.
        */
        static inline long double getLongDoubleFromWideDecimal(const int128_t& value)
        {
            return getLongDoubleFromFloat128(static_cast<__float128>(value));
        }

        /**
            @brief The method converts a wide decimal value to an int64_t,
            saturating the value if necessary.
        */
        static inline int64_t getInt64FromWideDecimal(const int128_t& value)
        {
            if (value > static_cast<int128_t>(INT64_MAX))
                return INT64_MAX;
            else if (value < static_cast<int128_t>(INT64_MIN))
                return INT64_MIN;

            return static_cast<int64_t>(value);
        }

        /**
            @brief MDB increases scale by up to 4 digits calculating avg()
        */
        static inline void setScalePrecision4Avg(
            unsigned int& precision,
            unsigned int& scale)
        {
            uint32_t scaleAvailable = INT128MAXPRECISION - scale;
            uint32_t precisionAvailable = INT128MAXPRECISION - precision;
            scale += (scaleAvailable >= MAXSCALEINC4AVG) ? MAXSCALEINC4AVG : scaleAvailable;
            precision += (precisionAvailable >= MAXSCALEINC4AVG) ? MAXSCALEINC4AVG : precisionAvailable;
        }
};

/**
    @brief The structure contains an overflow check for int128
    division.
*/
struct DivisionOverflowCheck {
    void operator()(const int128_t& x, const int128_t& y)
    {
        if (x == Decimal::minInt128 && y == -1)
        {
            throw logging::OperationOverflowExcept(
                "Decimal::division<int128_t> produces an overflow.");
        }
    }
    void operator()(const int64_t x, const int64_t y)
    {
        if (x == std::numeric_limits<int64_t>::min() && y == -1)
        {
            throw logging::OperationOverflowExcept(
                "Decimal::division<int64_t> produces an overflow.");
        }
    }
};

/**
    @brief The structure contains an overflow check for int128
    and int64_t multiplication.
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
    void operator()(const int64_t x, const int64_t y)
    {
        if (x * y / y != x)
        {
            throw logging::OperationOverflowExcept(
                "Decimal::multiplication<int64_t> or scale multiplication \
produces an overflow.");
        }
    }
    bool operator()(const int64_t x, const int64_t y, int64_t& r)
    {
        if ((r = x * y) / y != x)
        {
            throw logging::OperationOverflowExcept(
                "Decimal::multiplication<int64_t> or scale multiplication \
produces an overflow.");
        }
        return true;
    }
};

/**
    @brief The strucuture runs an empty overflow check for int128
    multiplication operation.
*/
struct MultiplicationNoOverflowCheck {
    void operator()(const int128_t& x, const int128_t& y, int128_t& r)
    {
        r = x * y;
    }
};

/**
    @brief The structure contains an overflow check for int128
    and int64 addition.
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
    void operator()(const int64_t x, const int64_t y)
    {
        if ((y > 0 && x > std::numeric_limits<int64_t>::max() - y)
            || (y < 0 && x < std::numeric_limits<int64_t>::min() - y))
        {
            throw logging::OperationOverflowExcept(
                "Decimal::addition<int64_t> produces an overflow.");
        }
    }
};

/**
    @brief The structure contains an overflow check for int128
    subtraction.
*/
struct SubtractionOverflowCheck {
    void operator()(const int128_t& x, const int128_t& y)
    {
        if ((y > 0 && x < Decimal::minInt128 + y)
            || (y < 0 && x > Decimal::maxInt128 + y))
        {
            throw logging::OperationOverflowExcept(
                "Decimal::subtraction<int128_t> produces an overflow.");
        }
    }
    void operator()(const int64_t x, const int64_t y)
    {
        if ((y > 0 && x < std::numeric_limits<int64_t>::min() + y)
            || (y < 0 && x > std::numeric_limits<int64_t>::max() + y))
        {
            throw logging::OperationOverflowExcept(
                "Decimal::subtraction<int64_t> produces an overflow.");
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


/**
 * @brief VDecimal type
 *
 */
struct VDecimal
{
    VDecimal(): s128Value(0), value(0), scale(0), precision(0)
    {
    }

    VDecimal(int64_t val, int8_t s, uint8_t p, const int128_t &val128 = 0) :
        s128Value(val128),
        value(val),
        scale(s),
        precision(p)
    {
    }

    int decimalComp(const VDecimal& d) const
    {
        lldiv_t d1 = lldiv(value, static_cast<int64_t>(mcs_pow_10[scale]));
        lldiv_t d2 = lldiv(d.value, static_cast<int64_t>(mcs_pow_10[d.scale]));

        int ret = 0;

        if (d1.quot > d2.quot)
        {
            ret = 1;
        }
        else if (d1.quot < d2.quot)
        {
            ret = -1;
        }
        else
        {
            // rem carries the value's sign, but needs to be normalized.
            int64_t s = scale - d.scale;

            if (s < 0)
            {
                if ((d1.rem * static_cast<int64_t>(mcs_pow_10[-s])) > d2.rem)
                    ret = 1;
                else if ((d1.rem * static_cast<int64_t>(mcs_pow_10[-s])) < d2.rem)
                    ret = -1;
            }
            else
            {
                if (d1.rem > (d2.rem * static_cast<int64_t>(mcs_pow_10[s])))
                    ret = 1;
                else if (d1.rem < (d2.rem * static_cast<int64_t>(mcs_pow_10[s])))
                    ret = -1;
            }
        }

        return ret;
    }

    bool operator==(const VDecimal& rhs) const
    {
        if (precision > datatypes::INT64MAXPRECISION &&
            rhs.precision > datatypes::INT64MAXPRECISION)
        {
            if (scale == rhs.scale)
                return s128Value == rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) == 0);
        }
        else if (precision > datatypes::INT64MAXPRECISION &&
                 rhs.precision <= datatypes::INT64MAXPRECISION)
        {
            const_cast<VDecimal&>(rhs).s128Value = rhs.value;

            if (scale == rhs.scale)
                return s128Value == rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) == 0);
        }
        else if (precision <= datatypes::INT64MAXPRECISION &&
                 rhs.precision > datatypes::INT64MAXPRECISION)
        {
            if (scale == rhs.scale)
                return (int128_t) value == rhs.s128Value;
            else
                return (datatypes::Decimal::compare(VDecimal(0, scale, precision, (int128_t) value), rhs) == 0);
        }
        else
        {
            if (scale == rhs.scale)
                return value == rhs.value;
            else
                return (decimalComp(rhs) == 0);
        }
    }

    bool operator>(const VDecimal& rhs) const
    {
        if (precision > datatypes::INT64MAXPRECISION &&
            rhs.precision > datatypes::INT64MAXPRECISION)
        {
            if (scale == rhs.scale)
                return s128Value > rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) > 0);
        }
        else if (precision > datatypes::INT64MAXPRECISION &&
                 rhs.precision <= datatypes::INT64MAXPRECISION)
        {
            VDecimal rhstmp(0, rhs.scale, rhs.precision, (int128_t) rhs.value);

            if (scale == rhstmp.scale)
                return s128Value > rhstmp.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhstmp) > 0);
        }
        else if (precision <= datatypes::INT64MAXPRECISION &&
                 rhs.precision > datatypes::INT64MAXPRECISION)
        {
            if (scale == rhs.scale)
                return (int128_t) value > rhs.s128Value;
            else
                return (datatypes::Decimal::compare(VDecimal(0, scale, precision, (int128_t) value), rhs) > 0);
        }
        else
        {
            if (scale == rhs.scale)
                return value > rhs.value;
            else
                return (decimalComp(rhs) > 0);
        }
    }

    bool operator<(const VDecimal& rhs) const
    {
        if (precision > datatypes::INT64MAXPRECISION &&
            rhs.precision > datatypes::INT64MAXPRECISION)
        {
            if (scale == rhs.scale)
                return s128Value < rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) < 0);
        }
        else if (precision > datatypes::INT64MAXPRECISION &&
                 rhs.precision <= datatypes::INT64MAXPRECISION)
        {
            VDecimal rhstmp(0, rhs.scale, rhs.precision, (int128_t) rhs.value);

            if (scale == rhstmp.scale)
                return s128Value < rhstmp.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhstmp) < 0);
        }
        else if (precision <= datatypes::INT64MAXPRECISION &&
                 rhs.precision > datatypes::INT64MAXPRECISION)
        {
            if (scale == rhs.scale)
                return (int128_t) value < rhs.s128Value;
            else
                return (datatypes::Decimal::compare(VDecimal(0, scale, precision, (int128_t) value), rhs) < 0);
        }
        else
        {
            if (scale == rhs.scale)
                return value < rhs.value;
            else
                return (decimalComp(rhs) < 0);
        }
    }

    bool operator>=(const VDecimal& rhs) const
    {
        if (precision > datatypes::INT64MAXPRECISION &&
            rhs.precision > datatypes::INT64MAXPRECISION)
        {
            if (scale == rhs.scale)
                return s128Value >= rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) >= 0);
        }
        else if (precision > datatypes::INT64MAXPRECISION &&
                 rhs.precision <= datatypes::INT64MAXPRECISION)
        {
            VDecimal rhstmp(0, rhs.scale, rhs.precision, (int128_t) rhs.value);

            if (scale == rhstmp.scale)
                return s128Value >= rhstmp.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhstmp) >= 0);
        }
        else if (precision <= datatypes::INT64MAXPRECISION &&
                 rhs.precision > datatypes::INT64MAXPRECISION)
        {
            if (scale == rhs.scale)
                return (int128_t) value >= rhs.s128Value;
            else
                return (datatypes::Decimal::compare(VDecimal(0, scale, precision, (int128_t) value), rhs) >= 0);
        }
        else
        {
            if (scale == rhs.scale)
                return value >= rhs.value;
            else
                return (decimalComp(rhs) >= 0);
        }
    }

    bool operator<=(const VDecimal& rhs) const
    {
        if (precision > datatypes::INT64MAXPRECISION &&
            rhs.precision > datatypes::INT64MAXPRECISION)
        {
            if (scale == rhs.scale)
                return s128Value <= rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) <= 0);
        }
        else if (precision > datatypes::INT64MAXPRECISION &&
                 rhs.precision <= datatypes::INT64MAXPRECISION)
        {
            VDecimal rhstmp(0, rhs.scale, rhs.precision, (int128_t) rhs.value);

            if (scale == rhstmp.scale)
                return s128Value <= rhstmp.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhstmp) <= 0);
        }
        else if (precision <= datatypes::INT64MAXPRECISION &&
                 rhs.precision > datatypes::INT64MAXPRECISION)
        {
            if (scale == rhs.scale)
                return (int128_t) value <= rhs.s128Value;
            else
                return (datatypes::Decimal::compare(VDecimal(0, scale, precision, (int128_t) value), rhs) <= 0);
        }
        else
        {
            if (scale == rhs.scale)
                return value <= rhs.value;
            else
                return (decimalComp(rhs) <= 0);
        }
    }

    bool operator!=(const VDecimal& rhs) const
    {
        if (precision > datatypes::INT64MAXPRECISION &&
            rhs.precision > datatypes::INT64MAXPRECISION)
        {
            if (scale == rhs.scale)
                return s128Value != rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) != 0);
        }
        else if (precision > datatypes::INT64MAXPRECISION &&
                 rhs.precision <= datatypes::INT64MAXPRECISION)
        {
            VDecimal rhstmp(0, rhs.scale, rhs.precision, (int128_t) rhs.value);

            if (scale == rhstmp.scale)
                return s128Value != rhstmp.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhstmp) != 0);
        }
        else if (precision <= datatypes::INT64MAXPRECISION &&
                 rhs.precision > datatypes::INT64MAXPRECISION)
        {
            if (scale == rhs.scale)
                return (int128_t) value != rhs.s128Value;
            else
                return (datatypes::Decimal::compare(VDecimal(0, scale, precision, (int128_t) value), rhs) != 0);
        }
        else
        {
            if (scale == rhs.scale)
                return value != rhs.value;
            else
                return (decimalComp(rhs) != 0);
        }
    }

    int128_t s128Value;
    int64_t value;
    int8_t  scale;	  // 0~38
    uint8_t precision;  // 1~38
};

} //end of namespace
#endif

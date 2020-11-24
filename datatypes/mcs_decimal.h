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
#include "mcs_basic_types.h"
#include "exceptclasses.h"
#include "widedecimalutils.h"
#include "mcs_int128.h"
#include "mcs_float128.h"
#include "checks.h"
#include "branchpred.h"


namespace datatypes
{
    class VDecimal;
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
constexpr int128_t Decimal128Null = TSInt128::NullValue;
constexpr int128_t Decimal128Empty = TSInt128::EmptyValue;


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

        static constexpr uint8_t MAXLENGTH16BYTES = TSInt128::maxLength();
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
            @brief The method detects whether decimal type is wide
            using precision.
        */
        static inline bool isWideDecimalTypeByPrecision(const int32_t precision)
        {
            return precision > INT64MAXPRECISION
                && precision <= INT128MAXPRECISION;
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

//
//  @brief The structure contains an overflow check for int128
//  and int64_t multiplication.
//
struct MultiplicationOverflowCheck {
    void operator()(const int128_t& x, const int128_t& y)
    {
        int128_t tempR = 0;
        this->operator()(x, y, tempR);
    }
    bool operator()(const int128_t& x, const int128_t& y, int128_t& r)
    {
        volatile int128_t z = x * y;
        if (z / y != x)
        {
            throw logging::OperationOverflowExcept(
                "Decimal::multiplication<int128_t> or scale multiplication \
produces an overflow.");
        }
        r = z;
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
class VDecimal: public TSInt128
{
    public:
    VDecimal(): value(0), scale(0), precision(0)
    {
    }

    VDecimal(int64_t val, int8_t s, uint8_t p, const int128_t &val128 = 0) :
        TSInt128(val128),
        value(val),
        scale(s),
        precision(p)
    { }

    VDecimal(int64_t unused, int8_t s, uint8_t p, const int128_t* val128Ptr) :
        TSInt128(val128Ptr),
        value(unused),
        scale(s),
        precision(p)
    { }

    VDecimal(const TSInt128& val128, int8_t s, uint8_t p) :
        TSInt128(val128),
        value(0),
        scale(s),
        precision(p)
    { }


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

    inline TSInt128 toTSInt128() const
    {
        return TSInt128(s128Value);
    }

    inline TFloat128 toTFloat128() const
    {
        return TFloat128(s128Value);
    }

    inline double toDouble() const
    {
        int128_t scaleDivisor;
        getScaleDivisor(scaleDivisor, scale);
        datatypes::TFloat128 tmpval((__float128) s128Value / scaleDivisor);
        return static_cast<double>(tmpval);
    }

    inline operator double() const
    {
        return toDouble();
    }

    inline long double toLongDouble() const
    {
        datatypes::TFloat128 y(s128Value);
        return static_cast<long double>(y);
    }

   
    // The template returns integral as TSInt128 and
    // fractional as D parts. We use template-type-defined types
    // to return a fractional part
    template<typename D>
    inline std::pair<TSInt128, D> getIntegralAndFractional() const
    {
        int128_t scaleDivisor;
        getScaleDivisor(scaleDivisor, scale);
        return std::make_pair(TSInt128(s128Value / scaleDivisor),
                              D((typename get_integral_type<D>::type)(s128Value % scaleDivisor) / scaleDivisor));
    }

    inline TSInt128 getIntegralPart() const
    {
        int128_t scaleDivisor = 0;
        int128_t result = 0;
        if(LIKELY(utils::is_nonnegative(scale)))
        {
            getIntegralPart(scaleDivisor, result);
        }
        else
        {
            getIntegralPartNegativeScale(scaleDivisor, result);
        }
        return TSInt128(result);
    }

    // !!! This is a very hevyweight rouding style
    // Argument determines if it is a ceil
    // rounding or not. 0 - ceil rounding
    inline TSInt128 getRoundedIntegralPart(const uint8_t roundingFactor = 0) const
    {
        int128_t flooredScaleDivisor = 0;
        int128_t roundedValue = 0;
        getIntegralPart(flooredScaleDivisor, roundedValue);
        int128_t ceiledScaleDivisor = (flooredScaleDivisor <= 10) ? 1 : (flooredScaleDivisor / 10);
        int128_t leftO = (s128Value - roundedValue * flooredScaleDivisor) / ceiledScaleDivisor;
        if (leftO > roundingFactor)
        {
            roundedValue++;
        }

        return TSInt128(roundedValue);
    }

    inline TSInt128 getPosNegRoundedIntegralPart(const uint8_t roundingFactor = 0) const
    {
        int128_t flooredScaleDivisor = 0;
        int128_t roundedValue = 0;
        getIntegralPart(flooredScaleDivisor, roundedValue);
        int128_t ceiledScaleDivisor = (flooredScaleDivisor <= 10) ? 1 : (flooredScaleDivisor / 10);
        int128_t leftO = (s128Value - roundedValue * flooredScaleDivisor) / ceiledScaleDivisor;
        if (utils::is_nonnegative(roundedValue) && leftO > roundingFactor)
        {
            roundedValue++;
        }
        if (utils::is_negative(roundedValue) && leftO < -roundingFactor)
        {
            roundedValue--;
        }

        return TSInt128(roundedValue);
    }

    bool operator==(const VDecimal& rhs) const
    {
        if (isTSInt128ByPrecision() && rhs.isTSInt128ByPrecision())
        {
            if (scale == rhs.scale)
                return s128Value == rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) == 0);
        }
        else if (isTSInt128ByPrecision() && !rhs.isTSInt128ByPrecision())
        {
            const_cast<VDecimal&>(rhs).s128Value = rhs.value;

            if (scale == rhs.scale)
                return s128Value == rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) == 0);
        }
        else if (!isTSInt128ByPrecision() && rhs.isTSInt128ByPrecision())
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
        if (isTSInt128ByPrecision() && rhs.isTSInt128ByPrecision())
        {
            if (scale == rhs.scale)
                return s128Value > rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) > 0);
        }
        else if (isTSInt128ByPrecision() && !rhs.isTSInt128ByPrecision())
        {
            VDecimal rhstmp(0, rhs.scale, rhs.precision, (int128_t) rhs.value);

            if (scale == rhstmp.scale)
                return s128Value > rhstmp.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhstmp) > 0);
        }
        else if (!isTSInt128ByPrecision() && rhs.isTSInt128ByPrecision())
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
        if (isTSInt128ByPrecision() && rhs.isTSInt128ByPrecision())
        {
            if (scale == rhs.scale)
                return s128Value < rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) < 0);
        }
        else if (isTSInt128ByPrecision() && !rhs.isTSInt128ByPrecision())
        {
            VDecimal rhstmp(0, rhs.scale, rhs.precision, (int128_t) rhs.value);

            if (scale == rhstmp.scale)
                return s128Value < rhstmp.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhstmp) < 0);
        }
        else if (!isTSInt128ByPrecision() && rhs.isTSInt128ByPrecision())
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
        if (isTSInt128ByPrecision() && rhs.isTSInt128ByPrecision())
        {
            if (scale == rhs.scale)
                return s128Value >= rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) >= 0);
        }
        else if (isTSInt128ByPrecision() && !rhs.isTSInt128ByPrecision())
        {
            VDecimal rhstmp(0, rhs.scale, rhs.precision, (int128_t) rhs.value);

            if (scale == rhstmp.scale)
                return s128Value >= rhstmp.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhstmp) >= 0);
        }
        else if (!isTSInt128ByPrecision() && rhs.isTSInt128ByPrecision())
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
        if (isTSInt128ByPrecision() && rhs.isTSInt128ByPrecision())
        {
            if (scale == rhs.scale)
                return s128Value <= rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) <= 0);
        }
        else if (isTSInt128ByPrecision() && !rhs.isTSInt128ByPrecision())
        {
            VDecimal rhstmp(0, rhs.scale, rhs.precision, (int128_t) rhs.value);

            if (scale == rhstmp.scale)
                return s128Value <= rhstmp.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhstmp) <= 0);
        }
        else if (!isTSInt128ByPrecision() && rhs.isTSInt128ByPrecision())
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
        if (isTSInt128ByPrecision() && rhs.isTSInt128ByPrecision())
        {
            if (scale == rhs.scale)
                return s128Value != rhs.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhs) != 0);
        }
        else if (isTSInt128ByPrecision() && !rhs.isTSInt128ByPrecision())
        {
            VDecimal rhstmp(0, rhs.scale, rhs.precision, (int128_t) rhs.value);

            if (scale == rhstmp.scale)
                return s128Value != rhstmp.s128Value;
            else
                return (datatypes::Decimal::compare(*this, rhstmp) != 0);
        }
        else if (!isTSInt128ByPrecision() && rhs.isTSInt128ByPrecision())
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

    inline bool isTSInt128ByPrecision() const
    {
        return precision > INT64MAXPRECISION
            && precision <= INT128MAXPRECISION;
    }
    // hasTSInt128 explicitly tells to print int128 out in cases
    // where precision can't detect decimal type properly, e.g.
    // DECIMAL(10)/DECIMAL(38)
    std::string toString(bool hasTSInt128 = false) const;
    friend std::ostream& operator<<(std::ostream& os, const VDecimal& dec);

    int64_t value;
    int8_t  scale;	  // 0~38
    uint8_t precision;  // 1~38

    // STRICTLY for unit tests!!!
    void setTSInt64Value(const int64_t x) { value = x; }
    void setTSInt128Value(const int128_t& x) { s128Value = x; }
    void setScale(const uint8_t x) { scale = x; }

private:
    uint8_t writeIntPart(const int128_t& x,
                         char* buf,
                         const uint8_t buflen) const;
    uint8_t writeFractionalPart(const int128_t& x,
                                char* buf,
                                const uint8_t buflen) const;
    std::string toStringTSInt128WithScale() const;
    std::string toStringTSInt64() const; 

    inline void getIntegralPart(int128_t& scaleDivisor, int128_t& result) const
    {
        getScaleDivisor(scaleDivisor, scale);
        result = s128Value / scaleDivisor;
    }

    inline void getIntegralPartNegativeScale(int128_t& scaleDivisor, int128_t& result) const
    {
        getScaleDivisor(scaleDivisor, -scale);
        // Calls for overflow check
        result = s128Value * scaleDivisor;
    }


};

} //end of namespace
#endif

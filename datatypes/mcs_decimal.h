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
#include <functional>
#include "mcs_basic_types.h"
#include "exceptclasses.h"
#include "widedecimalutils.h"
#include "mcs_int128.h"
#include "mcs_float128.h"
#include "checks.h"
#include "branchpred.h"


namespace datatypes
{
    class Decimal;
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

struct lldiv_t_128
{
    int128_t quot;
    int128_t rem;
    lldiv_t_128() : quot(0), rem(0) {}
    lldiv_t_128(const int128_t& a_quot, const int128_t& a_rem)
        : quot(a_quot), rem(a_rem) {}
};

inline lldiv_t_128 lldiv128(const int128_t& dividend, const int128_t& divisor)
{
    if (UNLIKELY(divisor == 0) || UNLIKELY(dividend == 0))
        return lldiv_t_128();

    return lldiv_t_128(dividend / divisor, dividend % divisor);
}

// @brief The class for Decimal related operations
// The class contains Decimal related operations are scale and
// precision aware.
// This class will inherit from:
//      DecimalMeta class that stores scale and precision
//      Storage classes TSInt128 and int64
// !!! There are some static classes that will exists during transition period. 
class Decimal: public TSInt128
{
    public:
        static constexpr uint8_t MAXLENGTH16BYTES = TSInt128::maxLength();
        static constexpr uint8_t MAXLENGTH8BYTES = 23;
        static constexpr int128_t minInt128 = TFloat128::minInt128;
        static constexpr int128_t maxInt128 = TFloat128::maxInt128;

        static inline bool isWideDecimalNullValue(const int128_t& val)
        {
            return (val == TSInt128::NullValue);
        }

        static inline bool isWideDecimalEmptyValue(const int128_t& val)
        {
            return (val == TSInt128::EmptyValue);
        }

        static inline void setWideDecimalNullValue(int128_t& val)
        {
            val = TSInt128::NullValue;
        }

        static inline void setWideDecimalEmptyValue(int128_t& val)
        {
            val = TSInt128::EmptyValue;
        }

        /**
            @brief Compares two Decimal taking scale into account. 
        */
        static int compare(const Decimal& l, const Decimal& r); 
        /**
            @brief Addition template that supports overflow check and
            two internal representations of decimal.
        */
        template<typename T, bool overflow>
        static void addition(const Decimal& l,
            const Decimal& r,
            Decimal& result);

        /**
            @brief Subtraction template that supports overflow check and
            two internal representations of decimal.
        */
        template<typename T, bool overflow>
        static void subtraction(const Decimal& l,
            const Decimal& r,
            Decimal& result);

        /**
            @brief Division template that supports overflow check and
            two internal representations of decimal.
        */
        template<typename T, bool overflow>
        static void division(const Decimal& l,
            const Decimal& r,
            Decimal& result);

        /**
            @brief Multiplication template that supports overflow check and
            two internal representations of decimal.
        */
        template<typename T, bool overflow>
        static void multiplication(const Decimal& l,
            const Decimal& r,
            Decimal& result);

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

        Decimal(): value(0), scale(0), precision(0)
        {
        }

        Decimal(int64_t val, int8_t s, uint8_t p, const int128_t &val128 = 0) :
            TSInt128(val128),
            value(val),
            scale(s),
            precision(p)
        { }

        Decimal(int64_t unused, int8_t s, uint8_t p, const int128_t* val128Ptr) :
            TSInt128(val128Ptr),
            value(unused),
            scale(s),
            precision(p)
        { }

        Decimal(const TSInt128& val128, int8_t s, uint8_t p) :
            TSInt128(val128),
            value(0),
            scale(s),
            precision(p)
        { }


        int decimalComp(const Decimal& d) const
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

        inline float toFloat() const
        {
            int128_t scaleDivisor;
            getScaleDivisor(scaleDivisor, scale);
            datatypes::TFloat128 tmpval((__float128) s128Value / scaleDivisor);
            return static_cast<float>(tmpval);
        }

        inline operator float() const
        {
            return toFloat();
        }

        inline long double toLongDouble() const
        {
            int128_t scaleDivisor;
            getScaleDivisor(scaleDivisor, scale);
            datatypes::TFloat128 tmpval((__float128) s128Value / scaleDivisor);
            return static_cast<long double>(tmpval);
        }

        inline operator long double() const
        {
            return toLongDouble();
        }
       
        // This method returns integral part as a TSInt128 and
        // fractional part as a TFloat128
        inline std::pair<TSInt128, TFloat128> getIntegralAndDividedFractional() const
        {
            int128_t scaleDivisor;
            getScaleDivisor(scaleDivisor, scale);
            return std::make_pair(TSInt128(s128Value / scaleDivisor),
                                  TFloat128((__float128)(s128Value % scaleDivisor) / scaleDivisor));
        }

        // This method returns integral part as a TSInt128 and
        // fractional part as a TFloat128
        inline std::pair<TSInt128, TSInt128> getIntegralAndFractional() const
        {
            int128_t scaleDivisor;
            getScaleDivisor(scaleDivisor, scale);
            return std::make_pair(TSInt128(s128Value / scaleDivisor),
                                  TSInt128(s128Value % scaleDivisor));
        }

        inline std::tuple<TSInt128, TSInt128, TSInt128> getIntegralFractionalAndDivisor() const
        {
            int128_t scaleDivisor;
            getScaleDivisor(scaleDivisor, scale);
            return std::make_tuple(TSInt128(s128Value / scaleDivisor),
                                   TSInt128(s128Value % scaleDivisor),
                                   TSInt128(scaleDivisor));
        }

        inline TSInt128 getIntegralPart() const
        {
            int128_t scaleDivisor = 0;
            if(LIKELY(utils::is_nonnegative(scale)))
            {
                return TSInt128(getIntegralPartNonNegativeScale(scaleDivisor));
            }
            return TSInt128(getIntegralPartNegativeScale(scaleDivisor));
        }

        // !!! This is a very hevyweight rouding style
        // Argument determines if it is a ceil
        // rounding or not. 0 - ceil rounding
        inline TSInt128 getRoundedIntegralPart(const uint8_t roundingFactor = 0) const
        {
            int128_t flooredScaleDivisor = 0;
            int128_t roundedValue = getIntegralPartNonNegativeScale(flooredScaleDivisor);
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
            int128_t roundedValue = getIntegralPartNonNegativeScale(flooredScaleDivisor);
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

        int64_t narrowRound() const
        {
            int64_t scaleDivisor;
            getScaleDivisor(scaleDivisor, scale);
            int64_t intg = value / scaleDivisor;
            int64_t frac2 = 2 * (value % scaleDivisor);
            if (frac2 >= scaleDivisor)
              return intg + 1;
            if (frac2 <= -scaleDivisor)
              return intg - 1;
            return intg;
        }

        // MOD operator for an integer divisor to be used
        // for integer rhs
        inline TSInt128 operator%(const TSInt128& div) const
        {
            if (!isScaled())
            {
                return s128Value % div.getValue();
            }
            // Scale the value and calculate
            // (LHS.value % RHS.value) * LHS.scaleMultiplier + LHS.scale_div_remainder
            auto integralFractionalDivisor = getIntegralFractionalAndDivisor();
            return (std::get<0>(integralFractionalDivisor) % div.getValue()) * std::get<2>(integralFractionalDivisor) + std::get<1>(integralFractionalDivisor);
        }

        template<typename Op128, typename Op64>
        bool cmpOperatorTemplate(const Decimal& rhs) const
        {
            Op128 op128;
            Op64 op64;
            if (precision > datatypes::INT64MAXPRECISION &&
                rhs.precision > datatypes::INT64MAXPRECISION)
            {
                if (scale == rhs.scale)
                    return op128(s128Value, rhs.s128Value);
                else
                    return op64(datatypes::Decimal::compare(*this, rhs), 0);
            }
            else if (precision > datatypes::INT64MAXPRECISION &&
                     rhs.precision <= datatypes::INT64MAXPRECISION)
            {

                if (scale == rhs.scale)
                {
                    return op128(s128Value, (int128_t) rhs.value);
                }
                else
                {
                    // comp_op<int64>(compare(l,r),0)
                    return op64(datatypes::Decimal::compare(
                                    *this, 
                                    Decimal(TSInt128(rhs.value),
                                            rhs.scale,
                                            rhs.precision)),
                           0);
                }
            }
            else if (precision <= datatypes::INT64MAXPRECISION &&
                     rhs.precision > datatypes::INT64MAXPRECISION)
            {
                if (scale == rhs.scale)
                {
                    return op128((int128_t) value, rhs.s128Value);
                }
                else
                {
                    // comp_op<int64>(compare(l,r),0)
                    return op64(datatypes::Decimal::compare(
                                    Decimal(
                                        TSInt128(value),
                                        scale,
                                        precision),
                                    rhs),
                           0);
                }
            }
            else
            {
                if (scale == rhs.scale)
                    return op64(value, rhs.value);
                else
                    return op64(decimalComp(rhs), 0);
            }
        }

        bool operator==(const Decimal& rhs) const
        {
            return cmpOperatorTemplate<std::equal_to<int128_t>,
                                       std::equal_to<int64_t>>(rhs);
        }

        bool operator>(const Decimal& rhs) const
        {
            return cmpOperatorTemplate<std::greater<int128_t>,
                                       std::greater<int64_t>>(rhs);
        }

        bool operator>=(const Decimal& rhs) const
        {
            return cmpOperatorTemplate<std::greater_equal<int128_t>,
                                       std::greater_equal<int64_t>>(rhs);
        }

        bool operator<(const Decimal& rhs) const
        {
            return !this->operator>=(rhs);
        }

        bool operator<=(const Decimal& rhs) const
        {
            return !this->operator>(rhs);
        }

        bool operator!=(const Decimal& rhs) const
        {
            return !this->operator==(rhs);
        }

        inline bool isTSInt128ByPrecision() const
        {
            return precision > INT64MAXPRECISION
                && precision <= INT128MAXPRECISION;
        }
        
        inline bool isScaled() const
        {
            return scale != 0;
        }

        // hasTSInt128 explicitly tells to print int128 out in cases
        // where precision can't detect decimal type properly, e.g.
        // DECIMAL(10)/DECIMAL(38)
        std::string toString(bool hasTSInt128 = false) const;
        friend std::ostream& operator<<(std::ostream& os, const Decimal& dec);

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

        inline int128_t getIntegralPartNonNegativeScale(int128_t& scaleDivisor) const
        {
            getScaleDivisor(scaleDivisor, scale);
            return s128Value / scaleDivisor;
        }

        inline int128_t getIntegralPartNegativeScale(int128_t& scaleDivisor) const
        {
            getScaleDivisor(scaleDivisor, -scale);
            // Calls for overflow check
            return s128Value * scaleDivisor;
        }
}; //end of Decimal

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

} //end of namespace
#endif

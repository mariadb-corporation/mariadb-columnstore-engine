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
#include "mcs_int64.h"
#include "mcs_float128.h"
#include "checks.h"
#include "branchpred.h"
#include "mcs_data_condition.h"

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
{
  return c >= 'a' ? (10 + c - 'a') : c >= 'A' ? (10 + c - 'A') : c - '0';
}

template <int BASE, uint128_t V>
constexpr uint128_t lit_eval()
{
  return V;
}

template <int BASE, uint128_t V, char C, char... Cs>
constexpr uint128_t lit_eval()
{
  static_assert(BASE != 16 || sizeof...(Cs) <= 32 - 1, "Literal too large for BASE=16");
  static_assert(BASE != 10 || sizeof...(Cs) <= 39 - 1, "Literal too large for BASE=10");
  static_assert(BASE != 8 || sizeof...(Cs) <= 44 - 1, "Literal too large for BASE=8");
  static_assert(BASE != 2 || sizeof...(Cs) <= 128 - 1, "Literal too large for BASE=2");
  return lit_eval<BASE, BASE * V + hexval(C), Cs...>();
}

template <char... Cs>
struct LitEval
{
  static constexpr uint128_t eval()
  {
    return lit_eval<10, 0, Cs...>();
  }
};

template <char... Cs>
struct LitEval<'0', 'x', Cs...>
{
  static constexpr uint128_t eval()
  {
    return lit_eval<16, 0, Cs...>();
  }
};

template <char... Cs>
struct LitEval<'0', 'b', Cs...>
{
  static constexpr uint128_t eval()
  {
    return lit_eval<2, 0, Cs...>();
  }
};

template <char... Cs>
struct LitEval<'0', Cs...>
{
  static constexpr uint128_t eval()
  {
    return lit_eval<8, 0, Cs...>();
  }
};

template <char... Cs>
constexpr uint128_t operator"" _xxl()
{
  return LitEval<Cs...>::eval();
}
}  // namespace detail_xxl

template <char... Cs>
constexpr uint128_t operator"" _xxl()
{
  return ::detail_xxl::operator"" _xxl<Cs...>();
}

namespace datatypes
{
constexpr uint32_t MAXDECIMALWIDTH = 16U;
constexpr uint8_t INT64MAXPRECISION = 18U;
constexpr uint8_t INT128MAXPRECISION = 38U;
constexpr uint32_t MAXLEGACYWIDTH = 8U;
constexpr uint8_t MAXSCALEINC4AVG = 4U;

const uint64_t mcs_pow_10[20] = {
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
const int128_t mcs_pow_10_128[20] = {
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

constexpr uint32_t maxPowOf10 = sizeof(mcs_pow_10) / sizeof(mcs_pow_10[0]) - 1;
constexpr int128_t Decimal128Null = TSInt128::NullValue;
constexpr int128_t Decimal128Empty = TSInt128::EmptyValue;

template <typename T>
T scaleDivisor(const uint32_t scale)
{
  if (scale < 19)
    return (T)mcs_pow_10[scale];
  if (scale > 39)
  {
    std::string msg = "scaleDivisor called with a wrong scale: " + std::to_string(scale);
    throw std::invalid_argument(msg);
  }
  return (T)mcs_pow_10_128[scale - 19];
}

// Decomposed Decimal representation
// T - storage data type (int64_t, int128_t)
template <typename T>
class DecomposedDecimal
{
  T mDivisor;
  T mIntegral;
  T mFractional;

 public:
  DecomposedDecimal(T value, uint32_t scale)
   : mDivisor(scaleDivisor<T>(scale)), mIntegral(value / mDivisor), mFractional(value % mDivisor)
  {
  }
  T toSIntRound() const
  {
    T frac2 = 2 * mFractional;
    if (frac2 >= mDivisor)
      return mIntegral + 1;
    if (frac2 <= -mDivisor)
      return mIntegral - 1;
    return mIntegral;
  }
  T toSIntRoundPositive() const
  {
    T frac2 = 2 * mFractional;
    if (frac2 >= mDivisor)
      return mIntegral + 1;
    return mIntegral;
  }
  T toSIntFloor() const
  {
    return mFractional < 0 ? mIntegral - 1 : mIntegral;
  }
  T toSIntCeil() const
  {
    return mFractional > 0 ? mIntegral + 1 : mIntegral;
  }
};

template <typename T>
T applySignedScale(const T& val, int32_t scale)
{
  return scale < 0 ? val / datatypes::scaleDivisor<T>((uint32_t)-scale)
                   : val * datatypes::scaleDivisor<T>((uint32_t)scale);
}

/**
    @brief The function to produce scale multiplier/divisor for
    wide decimals.
*/
template <typename T>
inline void getScaleDivisor(T& divisor, const int8_t scale)
{
  if (scale < 0)
  {
    std::string msg = "getScaleDivisor called with negative scale: " + std::to_string(scale);
    throw std::invalid_argument(msg);
  }
  divisor = scaleDivisor<T>((uint32_t)scale);
}

struct lldiv_t_128
{
  int128_t quot;
  int128_t rem;
  lldiv_t_128() : quot(0), rem(0)
  {
  }
  lldiv_t_128(const int128_t& a_quot, const int128_t& a_rem) : quot(a_quot), rem(a_rem)
  {
  }
};

inline lldiv_t_128 lldiv128(const int128_t& dividend, const int128_t& divisor)
{
  if (UNLIKELY(divisor == 0) || UNLIKELY(dividend == 0))
    return lldiv_t_128();

  return lldiv_t_128(dividend / divisor, dividend % divisor);
}

// TODO: derive it from TSInt64 eventually
class TDecimal64
{
 public:
  int64_t value;

 public:
  static constexpr uint8_t MAXLENGTH8BYTES = 23;

 public:
  TDecimal64() : value(0)
  {
  }
  explicit TDecimal64(int64_t val) : value(val)
  {
  }
  explicit TDecimal64(const TSInt64& val) : value(static_cast<int64_t>(val))
  {
  }
  // Divide to the scale divisor with rounding
  int64_t toSInt64Round(uint32_t scale) const
  {
    return DecomposedDecimal<int64_t>(value, scale).toSIntRound();
  }
  uint64_t toUInt64Round(uint32_t scale) const
  {
    return value < 0 ? 0 : static_cast<uint64_t>(toSInt64Round(scale));
  }

  int64_t toSInt64Floor(uint32_t scale) const
  {
    return DecomposedDecimal<int64_t>(value, scale).toSIntFloor();
  }

  int64_t toSInt64Ceil(uint32_t scale) const
  {
    return DecomposedDecimal<int64_t>(value, scale).toSIntCeil();
  }

  // Convert to an arbitrary floating point data type,
  // e.g. float, double, long double
  template <typename T>
  T toXFloat(uint32_t scale) const
  {
    return (T)value / scaleDivisor<T>(scale);
  }
};

class TDecimal128 : public TSInt128
{
 public:
  static constexpr uint8_t MAXLENGTH16BYTES = TSInt128::maxLength();
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

 public:
  TDecimal128()
  {
  }
  explicit TDecimal128(const int128_t& val) : TSInt128(val)
  {
  }
  explicit TDecimal128(const TSInt128& val) : TSInt128(val)
  {
  }
  explicit TDecimal128(const int128_t* valPtr) : TSInt128(valPtr)
  {
  }
  uint64_t toUInt64Round(uint32_t scale) const
  {
    if (s128Value <= 0)
      return 0;
    int128_t intg = DecomposedDecimal<int128_t>(s128Value, scale).toSIntRoundPositive();
    return intg > numeric_limits<uint64_t>::max() ? numeric_limits<uint64_t>::max()
                                                  : static_cast<uint64_t>(intg);
  }

  int128_t toSInt128Floor(uint32_t scale) const
  {
    return DecomposedDecimal<int128_t>(s128Value, scale).toSIntFloor();
  }

  int128_t toSInt128Ceil(uint32_t scale) const
  {
    return DecomposedDecimal<int128_t>(s128Value, scale).toSIntCeil();
  }
};

// @brief The class for Decimal related operations
// The methods and operators implemented in this class are
// scale and precision aware.
// We should eventually move the members "scale" and "precision"
// into a separate class DecimalMeta and derive Decimal from it.
class Decimal : public TDecimal128, public TDecimal64
{
 public:
  /**
      @brief Compares two Decimal taking scale into account.
  */
  static int compare(const Decimal& l, const Decimal& r);
  /**
      @brief Addition template that supports overflow check and
      two internal representations of decimal.
  */
  template <typename T, bool overflow>
  static void addition(const Decimal& l, const Decimal& r, Decimal& result);

  /**
      @brief Subtraction template that supports overflow check and
      two internal representations of decimal.
  */
  template <typename T, bool overflow>
  static void subtraction(const Decimal& l, const Decimal& r, Decimal& result);

  /**
      @brief Division template that supports overflow check and
      two internal representations of decimal.
  */
  template <typename T, bool overflow>
  static void division(const Decimal& l, const Decimal& r, Decimal& result);

  /**
      @brief Multiplication template that supports overflow check and
      two internal representations of decimal.
  */
  template <typename T, bool overflow>
  static void multiplication(const Decimal& l, const Decimal& r, Decimal& result);

  /**
      @brief The method detects whether decimal type is wide
      using precision.
  */
  static inline bool isWideDecimalTypeByPrecision(const int32_t precision)
  {
    return precision > INT64MAXPRECISION && precision <= INT128MAXPRECISION;
  }

  /**
      @brief MDB increases scale by up to 4 digits calculating avg()
  */
  static inline void setScalePrecision4Avg(unsigned int& precision, unsigned int& scale)
  {
    uint32_t scaleAvailable = INT128MAXPRECISION - scale;
    uint32_t precisionAvailable = INT128MAXPRECISION - precision;
    scale += (scaleAvailable >= MAXSCALEINC4AVG) ? MAXSCALEINC4AVG : scaleAvailable;
    precision += (precisionAvailable >= MAXSCALEINC4AVG) ? MAXSCALEINC4AVG : precisionAvailable;
  }

  Decimal() : scale(0), precision(0)
  {
  }

  Decimal(int64_t val, int8_t s, uint8_t p, const int128_t& val128 = 0)
   : TDecimal128(val128), TDecimal64(val), scale(s), precision(p)
  {
  }

  Decimal(const TSInt64& val, int8_t s, uint8_t p) : TDecimal64(val), scale(s), precision(p)
  {
  }

  Decimal(int64_t unused, int8_t s, uint8_t p, const int128_t* val128Ptr)
   : TDecimal128(val128Ptr), TDecimal64(unused), scale(s), precision(p)
  {
  }

  Decimal(const TSInt128& val128, int8_t s, uint8_t p) : TDecimal128(val128), scale(s), precision(p)
  {
  }

  Decimal(const char* str, size_t length, DataCondition& error, int8_t s, uint8_t p);

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
    datatypes::TFloat128 tmpval((float128_t)s128Value / scaleDivisor);
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
    datatypes::TFloat128 tmpval((float128_t)s128Value / scaleDivisor);
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
    datatypes::TFloat128 tmpval((float128_t)s128Value / scaleDivisor);
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
                          TFloat128((float128_t)(s128Value % scaleDivisor) / scaleDivisor));
  }

  // This method returns integral part as a TSInt128 and
  // fractional part as a TFloat128
  inline std::pair<TSInt128, TSInt128> getIntegralAndFractional() const
  {
    int128_t scaleDivisor;
    getScaleDivisor(scaleDivisor, scale);
    return std::make_pair(TSInt128(s128Value / scaleDivisor), TSInt128(s128Value % scaleDivisor));
  }

  inline std::tuple<TSInt128, TSInt128, TSInt128> getIntegralFractionalAndDivisor() const
  {
    int128_t scaleDivisor;
    getScaleDivisor(scaleDivisor, scale);
    return std::make_tuple(TSInt128(s128Value / scaleDivisor), TSInt128(s128Value % scaleDivisor),
                           TSInt128(scaleDivisor));
  }

  inline TSInt128 getIntegralPart() const
  {
    int128_t scaleDivisor = 0;
    if (LIKELY(utils::is_nonnegative(scale)))
    {
      return TSInt128(getIntegralPartNonNegativeScale(scaleDivisor));
    }
    return TSInt128(getIntegralPartNegativeScale(scaleDivisor));
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

  int64_t decimal64ToSInt64Round() const
  {
    return TDecimal64::toSInt64Round((uint32_t)scale);
  }
  uint64_t decimal64ToUInt64Round() const
  {
    return TDecimal64::toUInt64Round((uint32_t)scale);
  }

  template <typename T>
  T decimal64ToXFloat() const
  {
    return TDecimal64::toXFloat<T>((uint32_t)scale);
  }

  int64_t toSInt64Round() const
  {
    return isWideDecimalTypeByPrecision(precision) ? static_cast<int64_t>(getPosNegRoundedIntegralPart(4))
                                                   : TDecimal64::toSInt64Round((uint32_t)scale);
  }
  uint64_t toUInt64Round() const
  {
    return isWideDecimalTypeByPrecision(precision) ? TDecimal128::toUInt64Round((uint32_t)scale)
                                                   : TDecimal64::toUInt64Round((uint32_t)scale);
  }

  // FLOOR related routines
  int64_t toSInt64Floor() const
  {
    return isWideDecimalTypeByPrecision(precision)
               ? static_cast<int64_t>(TSInt128(TDecimal128::toSInt128Floor((uint32_t)scale)))
               : TDecimal64::toSInt64Floor((uint32_t)scale);
  }

  uint64_t toUInt64Floor() const
  {
    return isWideDecimalTypeByPrecision(precision)
               ? static_cast<uint64_t>(TSInt128(TDecimal128::toSInt128Floor((uint32_t)scale)))
               : static_cast<uint64_t>(TSInt64(TDecimal64::toSInt64Floor((uint32_t)scale)));
  }

  Decimal floor() const
  {
    return isWideDecimalTypeByPrecision(precision)
               ? Decimal(TSInt128(TDecimal128::toSInt128Floor((uint32_t)scale)), 0, precision)
               : Decimal(TSInt64(TDecimal64::toSInt64Floor((uint32_t)scale)), 0, precision);
  }

  // CEIL related routines
  int64_t toSInt64Ceil() const
  {
    return isWideDecimalTypeByPrecision(precision)
               ? static_cast<int64_t>(TSInt128(TDecimal128::toSInt128Ceil((uint32_t)scale)))
               : static_cast<int64_t>(TSInt64(TDecimal64::toSInt64Ceil((uint32_t)scale)));
  }

  uint64_t toUInt64Ceil() const
  {
    return isWideDecimalTypeByPrecision(precision)
               ? static_cast<uint64_t>(TSInt128(TDecimal128::toSInt128Ceil((uint32_t)scale)))
               : static_cast<uint64_t>(TSInt64(TDecimal64::toSInt64Ceil((uint32_t)scale)));
  }

  Decimal ceil() const
  {
    return isWideDecimalTypeByPrecision(precision)
               ? Decimal(TSInt128(TDecimal128::toSInt128Ceil((uint32_t)scale)), 0, precision)
               : Decimal(TSInt64(TDecimal64::toSInt64Ceil((uint32_t)scale)), 0, precision);
  }

  // MOD operator for an integer divisor to be used
  // for integer rhs
  inline TSInt128 operator%(const TSInt128& div) const
  {
    if (!isScaled())
    {
      return TSInt128(s128Value % div.getValue());
    }
    // Scale the value and calculate
    // (LHS.value % RHS.value) * LHS.scaleMultiplier + LHS.scale_div_remainder
    auto integralFractionalDivisor = getIntegralFractionalAndDivisor();
    return (std::get<0>(integralFractionalDivisor) % div.getValue()) *
               std::get<2>(integralFractionalDivisor) +
           std::get<1>(integralFractionalDivisor);
  }

  template <typename Op128, typename Op64>
  bool cmpOperatorTemplate(const Decimal& rhs) const
  {
    Op128 op128;
    Op64 op64;
    if (precision > datatypes::INT64MAXPRECISION && rhs.precision > datatypes::INT64MAXPRECISION)
    {
      if (scale == rhs.scale)
        return op128(s128Value, rhs.s128Value);
      else
        return op64(datatypes::Decimal::compare(*this, rhs), 0);
    }
    else if (precision > datatypes::INT64MAXPRECISION && rhs.precision <= datatypes::INT64MAXPRECISION)
    {
      if (scale == rhs.scale)
      {
        return op128(s128Value, (int128_t)rhs.value);
      }
      else
      {
        // comp_op<int64>(compare(l,r),0)
        return op64(
            datatypes::Decimal::compare(*this, Decimal(TSInt128(rhs.value), rhs.scale, rhs.precision)), 0);
      }
    }
    else if (precision <= datatypes::INT64MAXPRECISION && rhs.precision > datatypes::INT64MAXPRECISION)
    {
      if (scale == rhs.scale)
      {
        return op128((int128_t)value, rhs.s128Value);
      }
      else
      {
        // comp_op<int64>(compare(l,r),0)
        return op64(datatypes::Decimal::compare(Decimal(TSInt128(value), scale, precision), rhs), 0);
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
    return cmpOperatorTemplate<std::equal_to<int128_t>, std::equal_to<int64_t>>(rhs);
  }

  bool operator>(const Decimal& rhs) const
  {
    return cmpOperatorTemplate<std::greater<int128_t>, std::greater<int64_t>>(rhs);
  }

  bool operator>=(const Decimal& rhs) const
  {
    return cmpOperatorTemplate<std::greater_equal<int128_t>, std::greater_equal<int64_t>>(rhs);
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

  Decimal integralWideRound(const int128_t& scaleDivisor = 0) const
  {
    int128_t scaleDivisorInt = scaleDivisor;
    if (UNLIKELY(!scaleDivisorInt))
    {
      datatypes::getScaleDivisor(scaleDivisorInt, scale);
    }
    lldiv_t div = lldiv(s128Value, scaleDivisorInt);

    if (datatypes::abs(div.rem) * 2 >= scaleDivisorInt)
    {
      return Decimal(value, scale, precision, (div.quot < 0) ? div.quot-- : div.quot++);
    }
    return Decimal(value, scale, precision, div.quot);
  }

  inline bool isTSInt128ByPrecision() const
  {
    return precision > INT64MAXPRECISION && precision <= INT128MAXPRECISION;
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

  int8_t scale;       // 0~38
  uint8_t precision;  // 1~38

  // STRICTLY for unit tests!!!
  void setTSInt64Value(const int64_t x)
  {
    value = x;
  }
  void setTSInt128Value(const int128_t& x)
  {
    s128Value = x;
  }
  void setScale(const uint8_t x)
  {
    scale = x;
  }

 private:
  uint8_t writeIntPart(const int128_t& x, char* buf, const uint8_t buflen) const;
  uint8_t writeFractionalPart(const int128_t& x, char* buf, const uint8_t buflen) const;
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
};  // end of Decimal

/**
    @brief The structure contains an overflow check for int128
    division.
*/
struct DivisionOverflowCheck
{
  void operator()(const int128_t& x, const int128_t& y)
  {
    if (x == Decimal::minInt128 && y == -1)
    {
      throw logging::OperationOverflowExcept("Decimal::division<int128_t> produces an overflow.");
    }
  }
  void operator()(const int64_t x, const int64_t y)
  {
    if (x == std::numeric_limits<int64_t>::min() && y == -1)
    {
      throw logging::OperationOverflowExcept("Decimal::division<int64_t> produces an overflow.");
    }
  }
};

//
//  @brief The structure contains an overflow check for int128
//  and int64_t multiplication.
//
struct MultiplicationOverflowCheck
{
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
struct MultiplicationNoOverflowCheck
{
  void operator()(const int128_t& x, const int128_t& y, int128_t& r)
  {
    r = x * y;
  }
};

/**
    @brief The structure contains an overflow check for int128
    and int64 addition.
*/
struct AdditionOverflowCheck
{
  void operator()(const int128_t& x, const int128_t& y)
  {
    if ((y > 0 && x > Decimal::maxInt128 - y) || (y < 0 && x < Decimal::minInt128 - y))
    {
      throw logging::OperationOverflowExcept("Decimal::addition<int128_t> produces an overflow.");
    }
  }
  void operator()(const int64_t x, const int64_t y)
  {
    if ((y > 0 && x > std::numeric_limits<int64_t>::max() - y) ||
        (y < 0 && x < std::numeric_limits<int64_t>::min() - y))
    {
      throw logging::OperationOverflowExcept("Decimal::addition<int64_t> produces an overflow.");
    }
  }
};

/**
    @brief The structure contains an overflow check for int128
    subtraction.
*/
struct SubtractionOverflowCheck
{
  void operator()(const int128_t& x, const int128_t& y)
  {
    if ((y > 0 && x < Decimal::minInt128 + y) || (y < 0 && x > Decimal::maxInt128 + y))
    {
      throw logging::OperationOverflowExcept("Decimal::subtraction<int128_t> produces an overflow.");
    }
  }
  void operator()(const int64_t x, const int64_t y)
  {
    if ((y > 0 && x < std::numeric_limits<int64_t>::min() + y) ||
        (y < 0 && x > std::numeric_limits<int64_t>::max() + y))
    {
      throw logging::OperationOverflowExcept("Decimal::subtraction<int64_t> produces an overflow.");
    }
  }
};

/**
    @brief The strucuture runs an empty overflow check for int128
    operation.
*/
struct NoOverflowCheck
{
  void operator()(const int128_t& x, const int128_t& y)
  {
    return;
  }
};

}  // namespace datatypes
#endif

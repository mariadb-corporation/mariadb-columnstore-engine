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

#include <string>
#include <inttypes.h>

#include "utils/common/branchpred.h"
#include "mcs_decimal.h"
#include "numericliteral.h"

namespace datatypes
{
template <typename BinaryOperation, typename OpOverflowCheck, typename MultiplicationOverflowCheck>
void addSubtractExecute(const Decimal& l, const Decimal& r, Decimal& result, BinaryOperation op,
                        OpOverflowCheck opOverflowCheck, MultiplicationOverflowCheck mulOverflowCheck)
{
  int128_t lValue = Decimal::isWideDecimalTypeByPrecision(l.precision) ? l.s128Value : l.value;
  int128_t rValue = Decimal::isWideDecimalTypeByPrecision(r.precision) ? r.s128Value : r.value;

  if (result.scale == l.scale && result.scale == r.scale)
  {
    opOverflowCheck(lValue, rValue);
    result.s128Value = op(lValue, rValue);
    return;
  }

  if (result.scale > l.scale)
  {
    int128_t scaleMultiplier;
    getScaleDivisor(scaleMultiplier, result.scale - l.scale);
    mulOverflowCheck(lValue, scaleMultiplier);
    lValue *= scaleMultiplier;
  }
  else if (result.scale < l.scale)
  {
    int128_t scaleMultiplier;
    getScaleDivisor(scaleMultiplier, l.scale - result.scale);
    lValue = (int128_t)(lValue > 0 ? (float128_t)lValue / scaleMultiplier + 0.5
                                   : (float128_t)lValue / scaleMultiplier - 0.5);
  }

  if (result.scale > r.scale)
  {
    int128_t scaleMultiplier;
    getScaleDivisor(scaleMultiplier, result.scale - r.scale);
    mulOverflowCheck(rValue, scaleMultiplier);
    rValue *= scaleMultiplier;
  }
  else if (result.scale < r.scale)
  {
    int128_t scaleMultiplier;
    getScaleDivisor(scaleMultiplier, r.scale - result.scale);
    rValue = (int128_t)(rValue > 0 ? (float128_t)rValue / scaleMultiplier + 0.5
                                   : (float128_t)rValue / scaleMultiplier - 0.5);
  }

  // We assume there is no way that lValue or rValue calculations
  // give an overflow and this is an incorrect assumption.
  opOverflowCheck(lValue, rValue);

  result.s128Value = op(lValue, rValue);
}

template <typename OpOverflowCheck, typename MultiplicationOverflowCheck>
void divisionExecute(const Decimal& l, const Decimal& r, Decimal& result, OpOverflowCheck opOverflowCheck,
                     MultiplicationOverflowCheck mulOverflowCheck)
{
  int128_t lValue = Decimal::isWideDecimalTypeByPrecision(l.precision) ? l.s128Value : l.value;
  int128_t rValue = Decimal::isWideDecimalTypeByPrecision(r.precision) ? r.s128Value : r.value;

  opOverflowCheck(lValue, rValue);
  if (result.scale >= l.scale - r.scale)
  {
    int128_t scaleMultiplier;

    getScaleDivisor(scaleMultiplier, result.scale - (l.scale - r.scale));

    // TODO How do we check overflow of (int128_t)((float128_t)lValue / rValue * scaleMultiplier) ?

    result.s128Value = (int128_t)(((lValue > 0 && rValue > 0) || (lValue < 0 && rValue < 0)
                                       ? (float128_t)lValue / rValue * scaleMultiplier + 0.5
                                       : (float128_t)lValue / rValue * scaleMultiplier - 0.5));
  }
  else
  {
    int128_t scaleMultiplier;

    getScaleDivisor(scaleMultiplier, (l.scale - r.scale) - result.scale);

    result.s128Value = (int128_t)(((lValue > 0 && rValue > 0) || (lValue < 0 && rValue < 0)
                                       ? (float128_t)lValue / rValue / scaleMultiplier + 0.5
                                       : (float128_t)lValue / rValue / scaleMultiplier - 0.5));
  }
}

template <typename OpOverflowCheck, typename MultiplicationOverflowCheck>
void multiplicationExecute(const Decimal& l, const Decimal& r, Decimal& result,
                           OpOverflowCheck opOverflowCheck, MultiplicationOverflowCheck mulOverflowCheck)
{
  int128_t lValue = Decimal::isWideDecimalTypeByPrecision(l.precision) ? l.s128Value : l.value;
  int128_t rValue = Decimal::isWideDecimalTypeByPrecision(r.precision) ? r.s128Value : r.value;

  if (lValue == 0 || rValue == 0)
  {
    result.s128Value = 0;
    return;
  }

  if (result.scale >= l.scale + r.scale)
  {
    int128_t scaleMultiplier;

    getScaleDivisor(scaleMultiplier, result.scale - (l.scale + r.scale));

    opOverflowCheck(lValue, rValue, result.s128Value);
    opOverflowCheck(result.s128Value, scaleMultiplier, result.s128Value);
  }
  else
  {
    unsigned int diff = l.scale + r.scale - result.scale;

    int128_t scaleMultiplierL, scaleMultiplierR;

    getScaleDivisor(scaleMultiplierL, diff / 2);
    getScaleDivisor(scaleMultiplierR, diff - (diff / 2));

    lValue = (int128_t)(((lValue > 0) ? (float128_t)lValue / scaleMultiplierL + 0.5
                                      : (float128_t)lValue / scaleMultiplierL - 0.5));

    rValue = (int128_t)(((rValue > 0) ? (float128_t)rValue / scaleMultiplierR + 0.5
                                      : (float128_t)rValue / scaleMultiplierR - 0.5));

    opOverflowCheck(lValue, rValue, result.s128Value);
    ;
  }
}

Decimal::Decimal(const char* str, size_t length, DataCondition& convError, int8_t s, uint8_t p)
 : scale(s), precision(p)
{
  literal::Converter<literal::SignedNumericLiteral> conv(str, length, convError);
  // We don't check "convErr" here. Let the caller do it.
  // Let's just convert what has been parsed.

  // Remove redundant leading integral and trailing fractional digits
  conv.normalize();
  if (isTSInt128ByPrecision())
  {
    s128Value = conv.toPackedSDecimal<int128_t>((literal::scale_t)scale, convError);
    int128_t max_number_decimal = mcs_pow_10_128[precision - 19] - 1;
    convError.adjustSIntXRange(s128Value, max_number_decimal);
  }
  else
  {
    value = conv.toPackedSDecimal<int64_t>((literal::scale_t)scale, convError);
    int64_t max_number_decimal = (int64_t)mcs_pow_10[precision] - 1;
    convError.adjustSIntXRange(value, max_number_decimal);
  }
}

int Decimal::compare(const Decimal& l, const Decimal& r)
{
  int128_t divisorL, divisorR;
  getScaleDivisor(divisorL, l.scale);
  getScaleDivisor(divisorR, r.scale);

  lldiv_t_128 d1 = lldiv128(l.s128Value, divisorL);
  lldiv_t_128 d2 = lldiv128(r.s128Value, divisorR);

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
    int64_t s = l.scale - r.scale;
    int128_t divisor;
    getScaleDivisor(divisor, std::abs(s));

    if (s < 0)
    {
      if ((d1.rem * divisor) > d2.rem)
        ret = 1;
      else if ((d1.rem * divisor) < d2.rem)
        ret = -1;
    }
    else
    {
      if (d1.rem > (d2.rem * divisor))
        ret = 1;
      else if (d1.rem < (d2.rem * divisor))
        ret = -1;
    }
  }

  return ret;
}

// no overflow check
template <>
void Decimal::addition<int128_t, false>(const Decimal& l, const Decimal& r, Decimal& result)
{
  std::plus<int128_t> add;
  NoOverflowCheck noOverflowCheck;
  addSubtractExecute(l, r, result, add, noOverflowCheck, noOverflowCheck);
}

// with overflow check
template <>
void Decimal::addition<int128_t, true>(const Decimal& l, const Decimal& r, Decimal& result)
{
  std::plus<int128_t> add;
  AdditionOverflowCheck overflowCheck;
  MultiplicationOverflowCheck mulOverflowCheck;
  addSubtractExecute(l, r, result, add, overflowCheck, mulOverflowCheck);
}

// no overflow check
template <>
void Decimal::addition<int64_t, false>(const Decimal& l, const Decimal& r, Decimal& result)
{
  if (result.scale == l.scale && result.scale == r.scale)
  {
    result.value = l.value + r.value;
    return;
  }

  int64_t lValue = l.value, rValue = r.value;

  if (result.scale > l.scale)
    lValue *= mcs_pow_10[result.scale - l.scale];
  else if (result.scale < l.scale)
    lValue = (int64_t)(lValue > 0 ? (double)lValue / mcs_pow_10[l.scale - result.scale] + 0.5
                                  : (double)lValue / mcs_pow_10[l.scale - result.scale] - 0.5);

  if (result.scale > r.scale)
    rValue *= mcs_pow_10[result.scale - r.scale];
  else if (result.scale < r.scale)
    rValue = (int64_t)(rValue > 0 ? (double)rValue / mcs_pow_10[r.scale - result.scale] + 0.5
                                  : (double)rValue / mcs_pow_10[r.scale - result.scale] - 0.5);

  result.value = lValue + rValue;
}

// with overflow check
template <>
void Decimal::addition<int64_t, true>(const Decimal& l, const Decimal& r, Decimal& result)
{
  AdditionOverflowCheck additionOverflowCheck;
  MultiplicationOverflowCheck mulOverflowCheck;

  if (result.scale == l.scale && result.scale == r.scale)
  {
    additionOverflowCheck(l.value, r.value);
    result.value = l.value + r.value;
    return;
  }

  int64_t lValue = l.value, rValue = r.value;

  if (result.scale > l.scale)
    mulOverflowCheck(lValue, mcs_pow_10[result.scale - l.scale], lValue);
  else if (result.scale < l.scale)
    lValue = (int64_t)(lValue > 0 ? (double)lValue / mcs_pow_10[l.scale - result.scale] + 0.5
                                  : (double)lValue / mcs_pow_10[l.scale - result.scale] - 0.5);

  if (result.scale > r.scale)
    mulOverflowCheck(rValue, mcs_pow_10[result.scale - r.scale], rValue);
  else if (result.scale < r.scale)
    rValue = (int64_t)(rValue > 0 ? (double)rValue / mcs_pow_10[r.scale - result.scale] + 0.5
                                  : (double)rValue / mcs_pow_10[r.scale - result.scale] - 0.5);

  additionOverflowCheck(lValue, rValue);
  result.value = lValue + rValue;
}

// no overflow check
template <>
void Decimal::subtraction<int128_t, false>(const Decimal& l, const Decimal& r, Decimal& result)
{
  std::minus<int128_t> subtract;
  NoOverflowCheck noOverflowCheck;
  addSubtractExecute(l, r, result, subtract, noOverflowCheck, noOverflowCheck);
}

// with overflow check
template <>
void Decimal::subtraction<int128_t, true>(const Decimal& l, const Decimal& r, Decimal& result)
{
  std::minus<int128_t> subtract;
  SubtractionOverflowCheck overflowCheck;
  MultiplicationOverflowCheck mulOverflowCheck;
  addSubtractExecute(l, r, result, subtract, overflowCheck, mulOverflowCheck);
}

// no overflow check
template <>
void Decimal::subtraction<int64_t, false>(const Decimal& l, const Decimal& r, Decimal& result)
{
  if (result.scale == l.scale && result.scale == r.scale)
  {
    result.value = l.value - r.value;
    return;
  }

  int64_t lValue = l.value, rValue = r.value;

  if (result.scale > l.scale)
    lValue *= mcs_pow_10[result.scale - l.scale];
  else if (result.scale < l.scale)
    lValue = (int64_t)(lValue > 0 ? (double)lValue / mcs_pow_10[l.scale - result.scale] + 0.5
                                  : (double)lValue / mcs_pow_10[l.scale - result.scale] - 0.5);

  if (result.scale > r.scale)
    rValue *= mcs_pow_10[result.scale - r.scale];
  else if (result.scale < r.scale)
    rValue = (int64_t)(rValue > 0 ? (double)rValue / mcs_pow_10[r.scale - result.scale] + 0.5
                                  : (double)rValue / mcs_pow_10[r.scale - result.scale] - 0.5);

  result.value = lValue - rValue;
}

// with overflow check
template <>
void Decimal::subtraction<int64_t, true>(const Decimal& l, const Decimal& r, Decimal& result)
{
  SubtractionOverflowCheck subtractionOverflowCheck;
  MultiplicationOverflowCheck mulOverflowCheck;

  if (result.scale == l.scale && result.scale == r.scale)
  {
    subtractionOverflowCheck(l.value, r.value);
    result.value = l.value - r.value;
    return;
  }

  int64_t lValue = l.value, rValue = r.value;

  if (result.scale > l.scale)
    mulOverflowCheck(lValue, mcs_pow_10[result.scale - l.scale], lValue);
  else if (result.scale < l.scale)
    lValue = (int64_t)(lValue > 0 ? (double)lValue / mcs_pow_10[l.scale - result.scale] + 0.5
                                  : (double)lValue / mcs_pow_10[l.scale - result.scale] - 0.5);

  if (result.scale > r.scale)
    mulOverflowCheck(rValue, mcs_pow_10[result.scale - r.scale], rValue);
  else if (result.scale < r.scale)
    rValue = (int64_t)(rValue > 0 ? (double)rValue / mcs_pow_10[r.scale - result.scale] + 0.5
                                  : (double)rValue / mcs_pow_10[r.scale - result.scale] - 0.5);

  subtractionOverflowCheck(lValue, rValue);
  result.value = lValue - rValue;
}

// no overflow check
template <>
void Decimal::division<int128_t, false>(const Decimal& l, const Decimal& r, Decimal& result)
{
  NoOverflowCheck noOverflowCheck;
  divisionExecute(l, r, result, noOverflowCheck, noOverflowCheck);
}

// With overflow check
template <>
void Decimal::division<int128_t, true>(const Decimal& l, const Decimal& r, Decimal& result)
{
  DivisionOverflowCheck overflowCheck;
  MultiplicationOverflowCheck mulOverflowCheck;
  divisionExecute(l, r, result, overflowCheck, mulOverflowCheck);
}

// no overflow check
// We rely on the zero check from ArithmeticOperator::execute
template <>
void Decimal::division<int64_t, false>(const Decimal& l, const Decimal& r, Decimal& result)
{
  if (result.scale >= l.scale - r.scale)
    result.value = (int64_t)((
        (l.value > 0 && r.value > 0) || (l.value < 0 && r.value < 0)
            ? (long double)l.value / r.value * mcs_pow_10[result.scale - (l.scale - r.scale)] + 0.5
            : (long double)l.value / r.value * mcs_pow_10[result.scale - (l.scale - r.scale)] - 0.5));
  else
    result.value = (int64_t)((
        (l.value > 0 && r.value > 0) || (l.value < 0 && r.value < 0)
            ? (long double)l.value / r.value / mcs_pow_10[l.scale - r.scale - result.scale] + 0.5
            : (long double)l.value / r.value / mcs_pow_10[l.scale - r.scale - result.scale] - 0.5));
}

// With overflow check
template <>
void Decimal::division<int64_t, true>(const Decimal& l, const Decimal& r, Decimal& result)
{
  DivisionOverflowCheck divisionOverflowCheck;

  divisionOverflowCheck(l.value, r.value);

  if (result.scale >= l.scale - r.scale)
    // TODO How do we check overflow of (int64_t)((long double)l.value / r.value * mcs_pow_10[result.scale -
    // (l.scale - r.scale)]) ?
    result.value = (int64_t)((
        (l.value > 0 && r.value > 0) || (l.value < 0 && r.value < 0)
            ? (long double)l.value / r.value * mcs_pow_10[result.scale - (l.scale - r.scale)] + 0.5
            : (long double)l.value / r.value * mcs_pow_10[result.scale - (l.scale - r.scale)] - 0.5));
  else
    result.value = (int64_t)((
        (l.value > 0 && r.value > 0) || (l.value < 0 && r.value < 0)
            ? (long double)l.value / r.value / mcs_pow_10[l.scale - r.scale - result.scale] + 0.5
            : (long double)l.value / r.value / mcs_pow_10[l.scale - r.scale - result.scale] - 0.5));
}

// no overflow check
template <>
void Decimal::multiplication<int128_t, false>(const Decimal& l, const Decimal& r, Decimal& result)
{
  MultiplicationNoOverflowCheck noOverflowCheck;
  multiplicationExecute(l, r, result, noOverflowCheck, noOverflowCheck);
}

// With overflow check
template <>
void Decimal::multiplication<int128_t, true>(const Decimal& l, const Decimal& r, Decimal& result)
{
  MultiplicationOverflowCheck mulOverflowCheck;
  multiplicationExecute(l, r, result, mulOverflowCheck, mulOverflowCheck);
}

// no overflow check
template <>
void Decimal::multiplication<int64_t, false>(const Decimal& l, const Decimal& r, Decimal& result)
{
  if (result.scale >= l.scale + r.scale)
    result.value = l.value * r.value * mcs_pow_10[result.scale - (l.scale + r.scale)];
  else
    result.value =
        (int64_t)(((l.value > 0 && r.value > 0) || (l.value < 0 && r.value < 0)
                       ? (double)l.value * r.value / mcs_pow_10[l.scale + r.scale - result.scale] + 0.5
                       : (double)l.value * r.value / mcs_pow_10[l.scale + r.scale - result.scale] - 0.5));
}

// With overflow check
template <>
void Decimal::multiplication<int64_t, true>(const Decimal& l, const Decimal& r, Decimal& result)
{
  MultiplicationOverflowCheck mulOverflowCheck;

  if (result.scale >= l.scale + r.scale)
  {
    mulOverflowCheck(l.value, r.value, result.value);
    mulOverflowCheck(result.value, mcs_pow_10[result.scale - (l.scale + r.scale)], result.value);
  }
  else
  {
    mulOverflowCheck(l.value, r.value, result.value);

    result.value = (int64_t)((
        (result.value > 0) ? (double)result.value / mcs_pow_10[l.scale + r.scale - result.scale] + 0.5
                           : (double)result.value / mcs_pow_10[l.scale + r.scale - result.scale] - 0.5));
  }
}

// Writes integer part of a Decimal using int128 argument provided
uint8_t Decimal::writeIntPart(const int128_t& x, char* buf, const uint8_t buflen) const
{
  char* p = buf;
  int128_t intPart = x;
  int128_t high = 0, mid = 0, low = 0;
  uint64_t maxUint64divisor = 10000000000000000000ULL;

  // Assuming scale = [0, 56]
  switch (scale / datatypes::maxPowOf10)
  {
    case 2:  // scale = [38, 56]
      intPart /= datatypes::mcs_pow_10[datatypes::maxPowOf10];
      intPart /= datatypes::mcs_pow_10[datatypes::maxPowOf10];
      low = intPart;
      break;
    case 1:  // scale = [19, 37]
      intPart /= datatypes::mcs_pow_10[datatypes::maxPowOf10];
      intPart /= datatypes::mcs_pow_10[scale % datatypes::maxPowOf10];
      low = intPart % maxUint64divisor;
      mid = intPart / maxUint64divisor;
      break;
    case 0:  // scale = [0, 18]
      intPart /= datatypes::mcs_pow_10[scale % datatypes::maxPowOf10];
      low = intPart % maxUint64divisor;
      intPart /= maxUint64divisor;
      mid = intPart % maxUint64divisor;
      high = intPart / maxUint64divisor;
      break;
    default: throw logging::QueryDataExcept("Decimal::writeIntPart() bad scale", logging::formatErr);
  }

  p += printPodParts(p, high, mid, low);
  uint8_t written = p - buf;
  if (buflen <= written)
  {
    throw logging::QueryDataExcept("Decimal::writeIntPart() char buffer overflow.", logging::formatErr);
  }

  return written;
}

uint8_t Decimal::writeFractionalPart(const int128_t& x, char* buf, const uint8_t buflen) const
{
  int128_t scaleDivisor = 1;
  char* p = buf;

  switch (scale / datatypes::maxPowOf10)
  {
    case 2:
      scaleDivisor *= datatypes::mcs_pow_10[datatypes::maxPowOf10];
      scaleDivisor *= datatypes::mcs_pow_10[datatypes::maxPowOf10];
      break;
    case 1:
      scaleDivisor *= datatypes::mcs_pow_10[datatypes::maxPowOf10];
      // fallthrough
    case 0: scaleDivisor *= datatypes::mcs_pow_10[scale % datatypes::maxPowOf10];
  }

  int128_t fractionalPart = x % scaleDivisor;

  // divide by the base until we have non-zero quotient
  scaleDivisor /= 10;

  while (scaleDivisor > 1 && fractionalPart / scaleDivisor == 0)
  {
    *p++ = '0';
    scaleDivisor /= 10;
  }
  size_t written = p - buf;
  ;
  p += TSInt128::writeIntPart(fractionalPart, p, buflen - written);
  return p - buf;
}

// The method writes Decimal based on TSInt128 with scale provided.
// It first writes sign, then extracts integer part
// prints delimiter and then decimal part.
std::string Decimal::toStringTSInt128WithScale() const
{
  char buf[Decimal::MAXLENGTH16BYTES];
  uint8_t left = sizeof(buf);
  char* p = buf;
  int128_t tempValue = s128Value;
  // sign
  if (tempValue < static_cast<int128_t>(0))
  {
    *p++ = '-';
    tempValue *= -1;
    left--;
  }

  // integer part
  p += writeIntPart(tempValue, p, left);

  // decimal delimiter
  *p++ = '.';
  // decimal part
  left = sizeof(buf) - (p - buf);
  p += writeFractionalPart(tempValue, p, left);
  uint8_t written = p - buf;

  if (sizeof(buf) <= written)
  {
    throw logging::QueryDataExcept("Decimal::toString() char buffer overflow.", logging::formatErr);
  }

  *p = '\0';

  return std::string(buf);
}

std::string Decimal::toStringTSInt64() const
{
  char buf[Decimal::MAXLENGTH8BYTES];
  uint64_t divisor = scaleDivisor<uint64_t>(scale);
  uint64_t uvalue = value < 0 ? (uint64_t)-value : (uint64_t)value;
  uint64_t intg = uvalue / divisor;
  uint64_t frac = uvalue % divisor;
  int nbytes = snprintf(buf, sizeof(buf), "%s%" PRIu64, value < 0 ? "-" : "", intg);
  if (scale > 0)
    snprintf(buf + nbytes, sizeof(buf) - nbytes, ".%.*" PRIu64, (int)scale, frac);
  return std::string(buf);
}

// Dispatcher method for toString() implementations
std::string Decimal::toString(bool hasTSInt128) const
{
  // There must be no empty at this point though
  if (isNull())
  {
    return std::string("NULL");
  }

  if (LIKELY(hasTSInt128 || isTSInt128ByPrecision()))
  {
    if (scale)
    {
      return toStringTSInt128WithScale();
    }
    return TSInt128::toString();
  }
  // TSInt64 Decimal
  if (scale)
  {
    return toStringTSInt64();
  }
  return std::to_string(value);
}

std::ostream& operator<<(std::ostream& os, const Decimal& dec)
{
  os << dec.toString();
  return os;
}
}  // namespace datatypes

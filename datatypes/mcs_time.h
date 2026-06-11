/*
   Copyright (C) 2026 MariaDB Corporation

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
   MA 02110-1301, USA.
*/
#pragma once

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <string>

#include "mcs_decimal.h"

namespace datatypes
{
// MariaDB TIME range is [-838:59:59.999999, 838:59:59.999999].
constexpr int32_t TIME_MAX_HOUR = 838;
constexpr int64_t TIME_MAX_WHOLE_SECONDS = TIME_MAX_HOUR * 3600 + 59 * 60 + 59;  // 3020399 == 838:59:59
constexpr int64_t TIME_OVERFLOW_SECONDS = TIME_MAX_WHOLE_SECONDS + 1;            // 3020400 == 839:00:00

// TIME/DATETIME fractional seconds are microseconds: 6 fractional digits. The server
// likewise caps the fractional scale at TIME_SECOND_PART_DIGITS (6).
constexpr int32_t TIME_MAX_SCALE = 6;

inline std::string saturatedTimeMax(bool neg)
{
  return neg ? "-838:59:59.999999" : "838:59:59.999999";
}

inline std::string formatTimeWithUsec(int64_t wholeSec, int64_t usec, bool neg)
{
  uint32_t hour = wholeSec / 3600;
  uint32_t minute = (wholeSec % 3600) / 60;
  uint32_t second = wholeSec % 60;

  char buf[32];
  snprintf(buf, sizeof(buf), "%s%02u:%02u:%02u.%06u", neg ? "-" : "", hour, minute, second, (uint32_t)usec);
  return buf;
}

inline std::string secToTimeWithUsec(double dval)
{
  bool neg = dval < 0;
  double absVal = fabs(dval);

  if (absVal >= TIME_OVERFLOW_SECONDS)
    return saturatedTimeMax(neg);

  int64_t wholeSec = (int64_t)absVal;
  int64_t usec =
      (int64_t)((absVal - (double)wholeSec) *
                1'000'000.0);  // Truncate (do not round) the fractional seconds to microsecond precision
  return formatTimeWithUsec(wholeSec, usec, neg);
}

// Truncate (do not round) a DECIMAL fractional-part magnitude at the given scale down to
// microseconds, mirroring the server's handling of fractional seconds.
inline int64_t fracToMicroseconds(const int128_t& fracPart, int32_t scale)
{
  if (scale <= TIME_MAX_SCALE)
    return (int64_t)(fracPart * mcs_pow_10[TIME_MAX_SCALE - scale]);

  return (int64_t)(fracPart / scaleDivisor<int128_t>(scale - TIME_MAX_SCALE));
}

// Compute seconds and microseconds from the scaled integer with truncation, mirroring
// the server, which converts the argument to DECIMAL and truncates the fraction.
inline std::string secToTimeFromDecimal(const Decimal& dec, int32_t precision, int32_t scale)
{
  const int32_t s = (scale < 0) ? 0 : scale;

  const int128_t rawVal = toInt128ByPrecision(dec, precision);
  const bool neg = (rawVal < 0);
  const int128_t mag = neg ? -rawVal : rawVal;  // std::abs() has no int128_t overload
  const int128_t divisor = scaleDivisor<int128_t>(s);

  const int128_t intPart = mag / divisor;
  const int64_t usec = fracToMicroseconds(mag % divisor, s);

  if (intPart >= TIME_OVERFLOW_SECONDS)
    return saturatedTimeMax(neg);

  return formatTimeWithUsec((int64_t)intPart, usec, neg);
}
}  // namespace datatypes

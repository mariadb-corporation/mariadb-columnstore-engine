/*
   Copyright (C) 2024 MariaDB Corporation

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

#include <limits>

#include "joblisttypes.h"
#include "mcs_systemcatalog.h"

/*
  This file contains simple definitions that can be
  needed in multiple mcs_TYPE.h files.
*/

namespace datatypes
{
template <SystemCatalog::ColDataType CT>
struct ranges_limits
{
  using T = ColTypeToIntegral<CT>::type;
  static constexpr T min()
  {
    if constexpr (std::is_signed_v<T>)
      return std::numeric_limits<T>::min() + 2;
    else
      return std::numeric_limits<T>::min();
  }
  static constexpr T max()
  {
    if constexpr (std::is_signed_v<T>)
      return std::numeric_limits<T>::max();
    else
      return std::numeric_limits<T>::max() - 2;
  }
};

template <>
struct ranges_limits<SystemCatalog::MEDINT>
{
  using T = int32_t;
  static constexpr T min()
  {
    return static_cast<T>(-(1ULL << 23));
  }
  static constexpr T max()
  {
    return static_cast<T>((1ULL << 23) - 1);
  }
};

template <>
struct ranges_limits<SystemCatalog::UMEDINT>
{
  using T = int32_t;
  static constexpr T min()
  {
    return static_cast<T>(0);
  }
  static constexpr T max()
  {
    return static_cast<T>((1ULL << 24) - 1);
  }
};

}  // namespace datatypes

namespace
{
// Signed types
const int64_t MIN_TINYINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::TINYINT>::min();  // -126;
const int64_t MIN_SMALLINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::SMALLINT>::min();  // -32766;
const int64_t MIN_MEDINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::MEDINT>::min();  // -8388608;
const int64_t MIN_INT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::INT>::min();  // -2147483646;
const int64_t MIN_BIGINT =
    datatypes::ranges_limits<datatypes::SystemCatalog::BIGINT>::min();  // -9223372036854775806LL;

const int64_t MAX_TINYINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::TINYINT>::max();  //  127;
const int64_t MAX_SMALLINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::SMALLINT>::max();  //  32767;
const int64_t MAX_MEDINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::MEDINT>::max();  //  8388607;
const int64_t MAX_INT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::INT>::max();  //  2147483647;
const int64_t MAX_BIGINT =
    datatypes::ranges_limits<datatypes::SystemCatalog::BIGINT>::max();  //  9223372036854775807

// Unsigned types
const uint64_t MIN_UTINYINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::UTINYINT>::min();
const uint64_t MIN_USMALLINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::USMALLINT>::min();
const uint64_t MIN_UMEDINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::UMEDINT>::min();
const uint64_t MIN_UINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::UINT>::min();
const uint64_t MIN_UBIGINT = datatypes::ranges_limits<datatypes::SystemCatalog::UBIGINT>::min();

const uint64_t MAX_UTINYINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::UTINYINT>::max();  // 253;
const uint64_t MAX_USMALLINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::USMALLINT>::max();  // 65533;
const uint64_t MAX_UMEDINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::UMEDINT>::max();  // 16777215
const uint64_t MAX_UINT __attribute__((unused)) =
    datatypes::ranges_limits<datatypes::SystemCatalog::UINT>::max();  // 4294967293
const uint64_t MAX_UBIGINT =
    datatypes::ranges_limits<datatypes::SystemCatalog::UBIGINT>::max();  // 18446744073709551613

// Floating point types
const float MIN_FLOAT __attribute__((unused)) = -std::numeric_limits<float>::max();
const long double MAX_LONGDOUBLE __attribute__((unused)) =
    std::numeric_limits<long double>::max();  // 1.7976931348623157e+308
const long double MIN_LONGDOUBLE __attribute__((unused)) = -std::numeric_limits<long double>::max();

const float MAX_FLOAT __attribute__((unused)) = std::numeric_limits<float>::max();  // 3.402823466385289e+38
const double MAX_DOUBLE __attribute__((unused)) =
    std::numeric_limits<double>::max();  // 1.7976931348623157e+308
const double MIN_DOUBLE __attribute__((unused)) = -std::numeric_limits<double>::max();

const uint64_t AUTOINCR_SATURATED __attribute__((unused)) = std::numeric_limits<uint64_t>::max();
}  // namespace

namespace datatypes
{
// Convert a positive floating point value to
// a signed or unsigned integer with rounding
// SRC - a floating point data type (float, double, float128_t)
// DST - a signed or unsigned integer data type (int32_t, uint64_t, int128_t, etc)
template <typename SRC, typename DST>
DST positiveXFloatToXIntRound(SRC value, DST limit)
{
  SRC tmp = value + 0.5;
  if (tmp >= static_cast<SRC>(limit))
    return limit;
  return static_cast<DST>(tmp);
}

// Convert a negative floating point value to
// a signed integer with rounding
// SRC - a floating point data type (float, double, float128_t)
// DST - a signed integer data type (int32_t, int64_t, int128_t, etc)
template <typename SRC, typename DST>
DST negativeXFloatToXIntRound(SRC value, DST limit)
{
  SRC tmp = value - 0.5;
  if (tmp <= static_cast<SRC>(limit))
    return limit;
  return static_cast<DST>(tmp);
}

// Convert a floating point value to ColumnStore int64_t
// Magic values cannot be returned.
template <typename SRC>
int64_t xFloatToMCSSInt64Round(SRC value)
{
  if (value > 0)
  {
    return positiveXFloatToXIntRound<SRC, int64_t>(value, MAX_BIGINT);
  }
  if (value < 0)
  {
    //@Bug 4632 and 4648: Don't return marker value for NULL, but allow return of marker value for EMPTYROW.
    return negativeXFloatToXIntRound<SRC, int64_t>(value, joblist::BIGINTEMPTYROW);
  }
  return 0;
}

// Convert a floating point value to ColumnStore uint64_t
// Magic values cannot be returned.
template <typename SRC>
uint64_t xFloatToMCSUInt64Round(SRC value)
{
  if (value > 0)
  {
    //@Bug 4632 and 4648: Don't return marker value for NULL, but allow return of marker value for EMPTYROW.
    const auto val = positiveXFloatToXIntRound<SRC, uint64_t>(value, joblist::UBIGINTEMPTYROW);
    return val == joblist::UBIGINTNULL ? MAX_UBIGINT : val;
  }
  if (value < 0)
  {
    return negativeXFloatToXIntRound<SRC, uint64_t>(value, MIN_UBIGINT);
  }

  return 0;
}

}  // end of namespace datatypes

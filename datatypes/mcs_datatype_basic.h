/*
   Copyright (C) 2021 MariaDB Corporation

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
#ifndef MCS_DATATYPE_BASIC_H_INCLUDED
#define MCS_DATATYPE_BASIC_H_INCLUDED

/*
  This file contains simple definitions that can be
  needed in multiple mcs_TYPE.h files.
*/

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
    return positiveXFloatToXIntRound<SRC, int64_t>(value, numeric_limits<int64_t>::max());
  if (value < 0)
    return negativeXFloatToXIntRound<SRC, int64_t>(value, numeric_limits<int64_t>::min() + 2);
  return 0;
}

// Convert a floating point value to ColumnStore uint64_t
// Magic values cannot be returned.
template <typename SRC>
uint64_t xFloatToMCSUInt64Round(SRC value)
{
  if (value > 0)
    return positiveXFloatToXIntRound<SRC, uint64_t>(value, numeric_limits<uint64_t>::max() - 2);
  if (value < 0)
    return negativeXFloatToXIntRound<SRC, uint64_t>(value, 0);

  return 0;
}

}  // end of namespace datatypes

#endif  // MCS_DATATYPE_BASIC_H_INCLUDED
// vim:ts=2 sw=2:

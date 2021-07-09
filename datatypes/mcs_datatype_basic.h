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

#include "mcs_numeric_limits.h"

/*
  This file contains simple definitions that can be
  needed in multiple mcs_TYPE.h files.
*/

namespace datatypes
{


// Inline RowGroup data is rounded to 1,2,4 or 8 bytes.
// For example, CHAR(5) occupies 8 bytes.
// This function returns the size of the unused part.
// Note, on assignment, we need to zero the unused part
// of a field in a RowGroup to keep data memcmp-able.
static inline uint32_t unusedGapSizeByPackedWidth(uint32_t width)
{
  switch (width)
  {
    case 3:  return 1; // 3 is rounded to 4
    case 5:  return 3; // 5 is rounded to 8
    case 6:  return 2; // 6 is rounded to 8
    case 7:  return 1; // 7 is rounded to 8
  }
  return 0; /*1,2,4,8,9+ do not have unused gaps */
}


// Convert a positive floating point value to
// a signed or unsigned integer with rounding
// SRC - a floating point data type (float, double, float128_t)
// DST - a signed or unsigned integer data type (int32_t, uint64_t, int128_t, etc)
template<typename SRC, typename DST>
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
template<typename SRC, typename DST>
DST negativeXFloatToXIntRound(SRC value, DST limit)
{
  SRC tmp = value - 0.5;
  if (tmp <= static_cast<SRC>(limit))
    return limit;
  return static_cast<DST>(tmp);
}


// Convert a floating point value to ColumnStore int64_t
// Magic values cannot be returned.
template<typename SRC>
int64_t xFloatToMCSSInt64Round(SRC value)
{
  if (value > 0)
    return positiveXFloatToXIntRound<SRC, int64_t>(
                                           value,
                                           numeric_limits<int64_t>::max());
  if (value < 0)
    return negativeXFloatToXIntRound<SRC, int64_t>(
                                           value,
                                           numeric_limits<int64_t>::min() + 2);
  return 0;
}


// Convert a floating point value to ColumnStore uint64_t
// Magic values cannot be returned.
template<typename SRC>
uint64_t xFloatToMCSUInt64Round(SRC value)
{
  if (value > 0)
    return positiveXFloatToXIntRound<SRC, uint64_t>(
                                           value,
                                           numeric_limits<uint64_t>::max() - 2);
  if (value < 0)
    return negativeXFloatToXIntRound<SRC, uint64_t>(value, 0);

  return 0;
}


} //end of namespace datatypes

#endif // MCS_DATATYPE_BASIC_H_INCLUDED
// vim:ts=2 sw=2:

/*
   Copyright (C) 2020 MariaDB Corporation

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

#include "mcs_int128.h"

namespace datatypes
{
  //     The method converts a wide decimal s128Value to a double.
  inline double TSInt128::getDoubleFromWideDecimal()
  {
      return getDoubleFromFloat128(static_cast<__float128>(s128Value));
  }

  //     The method converts a wide decimal s128Value to a long double.
  inline long double TSInt128::getLongDoubleFromWideDecimal()
  {
      return getLongDoubleFromFloat128(static_cast<__float128>(s128Value));
  }

  //     The method converts a wide decimal s128Value to an int64_t,
  //    saturating the s128Value if necessary.
  inline int64_t TSInt128::getInt64FromWideDecimal()
  {
      if (s128Value > static_cast<int128_t>(INT64_MAX))
          return INT64_MAX;
      else if (s128Value < static_cast<int128_t>(INT64_MIN))
          return INT64_MIN;

      return static_cast<int64_t>(s128Value);
  }

  //     The method converts a wide decimal s128Value to an uint32_t.
  inline uint32_t TSInt128::getUInt32FromWideDecimal()
  {
      if (s128Value > static_cast<int128_t>(UINT32_MAX))
          return UINT32_MAX;
      else if (s128Value < 0)
          return 0;

      return static_cast<uint32_t>(s128Value);
  }

  //     The method converts a wide decimal s128Value to an uint64_t.
  inline uint64_t TSInt128::getUInt64FromWideDecimal()
  {
      if (s128Value > static_cast<int128_t>(UINT64_MAX))
          return UINT64_MAX;
      else if (s128Value < 0)
          return 0;

      return static_cast<uint64_t>(s128Value);
  }

  //     The method converts a wide decimal s128Value to an int32_t.
  inline int32_t TSInt128::getInt32FromWideDecimal()
  {
      if (s128Value > static_cast<int128_t>(INT32_MAX))
          return INT32_MAX;
      else if (s128Value < static_cast<int128_t>(INT32_MIN))
          return INT32_MIN;

      return static_cast<int32_t>(s128Value);
  }

} // end of namespace datatypes
// vim:ts=2 sw=2:

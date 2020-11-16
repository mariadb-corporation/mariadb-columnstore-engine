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

#include <iostream>

#include "mcs_int128.h"
#include "exceptclasses.h"

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

  uint8_t TSInt128::printPodParts(char* buf,
                                  const int128_t& high,
                                  const int128_t& mid,
                                  const int128_t& low) const
  {
    char* p = buf;
    // pod[0] is low 8 bytes, pod[1] is high 8 bytes
    const uint64_t* high_pod = reinterpret_cast<const uint64_t*>(&high);
    const uint64_t* mid_pod = reinterpret_cast<const uint64_t*>(&mid);
    const uint64_t* low_pod = reinterpret_cast<const uint64_t*>(&low);

    if (high_pod[0] != 0)
    {
        p += sprintf(p, "%lu", high_pod[0]);
        p += sprintf(p, "%019lu", mid_pod[0]);
        p += sprintf(p, "%019lu", low_pod[0]);
    }
    else if (mid_pod[0] != 0)
    {
        p += sprintf(p, "%lu", mid_pod[0]);
        p += sprintf(p, "%019lu", low_pod[0]);
    }
    else
    {
        p += sprintf(p, "%lu", low_pod[0]);
    }
    return p - buf;
  }

  //    This method writes unsigned integer representation of TSInt128
  uint8_t TSInt128::writeIntPart(const int128_t& x,
                                 char* buf,
                                 const uint8_t buflen) const
  {
    char* p = buf;
    int128_t high = 0, mid = 0, low = 0;
    uint64_t maxUint64divisor = 10000000000000000000ULL;

    low = x % maxUint64divisor;
    int128_t value = x / maxUint64divisor;
    mid = value % maxUint64divisor;
    high = value / maxUint64divisor;

    p += printPodParts(p, high, mid, low);
    uint8_t written = p - buf;
    if (buflen <= written)
    {
        throw logging::QueryDataExcept("TSInt128::writeIntPart() char buffer overflow.",
                                       logging::formatErr);
    }

    return written;
  }

  //    conversion to std::string
  std::string TSInt128::toString() const
  {
    if (isNull())
    {
      return std::string("NULL");
    }

    if (isEmpty())
    {
      return std::string("EMPTY");
    }

    int128_t tempValue = s128Value;
    char buf[TSInt128::MAXLENGTH16BYTES];
    uint8_t left = sizeof(buf);
    char* p = buf;
    // sign
    if (tempValue < static_cast<int128_t>(0))
    {
      *p++ = '-';
      tempValue *= -1;
      left--; 
    }
    // integer part
    // reduce the size by one to account for \0
    left--;
    p += writeIntPart(tempValue, p, left);
    *p = '\0';

    return std::string(buf);
  }

  std::ostream& operator<<(std::ostream& os, const TSInt128& x)
  {
    os << x.toString();
    return os;
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

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
#ifndef MCS_TSFLOAT128_H_INCLUDED
#define MCS_TSFLOAT128_H_INCLUDED

#include <cfloat>
#include <cstdint>

namespace datatypes
{

class TSInt128;
class TFloat128;
using int128_t = __int128;

// Type defined integral types
// for templates
template <typename T>
struct get_integral_type {
  typedef T type;
};

template<>
struct get_integral_type<TFloat128>{
  typedef __float128 type;
};

template<>
struct get_integral_type<TSInt128>{
  typedef int128_t type;
};


class TFloat128
{
  public:
    static constexpr int128_t minInt128 = int128_t(0x8000000000000000LL) << 64;
    static constexpr int128_t maxInt128 = (int128_t(0x7FFFFFFFFFFFFFFFLL) << 64) + 0xFFFFFFFFFFFFFFFFLL;

    static constexpr uint16_t MAXLENGTH16BYTES = 42;
    //  A variety of ctors for aligned and unaligned arguments
    TFloat128(): value(0) { }

    //    aligned argument
    TFloat128(const __float128& x) { value = x; }
    TFloat128(const int128_t& x) { value = static_cast<__float128>(x); }

    //    Method returns max length of a string representation
    static constexpr uint8_t maxLength()
    {
      return TFloat128::MAXLENGTH16BYTES;
    }

    inline int128_t toTSInt128() const
    {
      if (value > static_cast<__float128>(maxInt128))
        return maxInt128;
      else if (value < static_cast<__float128>(minInt128))
        return minInt128;

      return static_cast<int128_t>(value);
    }

    inline operator int128_t() const
    {
      return toTSInt128();
    }

    inline operator double() const
    {
      return toDouble();
    }

    inline double toDouble() const
    {
        if (value > static_cast<__float128>(DBL_MAX))
            return DBL_MAX;
        else if (value < -static_cast<__float128>(DBL_MAX))
            return -DBL_MAX;

        return static_cast<double>(value);
    }

    inline operator long double() const
    {
      return toLongDouble();
    }

    inline int64_t toTSInt64() const
    {
      if (value > static_cast<__float128>(INT64_MAX))
          return INT64_MAX;
      else if (value < static_cast<__float128>(INT64_MIN))
          return INT64_MIN;

      return static_cast<int64_t>(value);
    }

    inline operator int64_t() const
    {
      return toTSInt64();
    }

    inline uint64_t toTUInt64() const
    {
      if (value > static_cast<__float128>(UINT64_MAX))
          return UINT64_MAX;
      else if (value < 0)
          return 0;

      return static_cast<uint64_t>(value);
    }

    inline operator uint64_t() const
    {
      return toTUInt64();
    }

    inline TFloat128 operator+(const TFloat128& rhs) const
    {
      return TFloat128(value + rhs.value);
    }

    inline long double toLongDouble() const
    {
        if (value > static_cast<__float128>(LDBL_MAX))
            return LDBL_MAX;
        else if (value < -static_cast<__float128>(LDBL_MAX))
            return -LDBL_MAX;

        return static_cast<long double>(value);
    }
  private:
    __float128 value;
};

} //end of namespace

#endif // MCS_TSFLOAT128_H_INCLUDED
// vim:ts=2 sw=2:


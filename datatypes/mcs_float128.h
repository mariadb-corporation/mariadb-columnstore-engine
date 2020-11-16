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

//class TSInt128;
using int128_t = __int128;

class TFloat128
{
  public:
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

    //     The method converts a TFloat128 to integral types
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


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
#pragma once

#include "mcs_datatypes_limits.h"

namespace datatypes
{
class TLongDouble
{
 protected:
  long double mValue;

 public:
  TLongDouble() : mValue(0)
  {
  }

  explicit TLongDouble(long double value) : mValue(value)
  {
  }

  explicit operator long double() const
  {
    return mValue;
  }

  int64_t toMCSSInt64Round() const
  {
    return xFloatToMCSSInt64Round<long double>(mValue);
  }

  uint64_t toMCSUInt64Round() const
  {
    return xFloatToMCSUInt64Round<long double>(mValue);
  }
};

}  // end of namespace datatypes

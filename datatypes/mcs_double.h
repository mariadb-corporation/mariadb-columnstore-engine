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
#ifndef MCS_DOUBLE_H_INCLUDED
#define MCS_DOUBLE_H_INCLUDED

#include "mcs_datatype_basic.h"

namespace datatypes
{
class TDouble
{
 protected:
  double mValue;

 public:
  TDouble() : mValue(0)
  {
  }

  explicit TDouble(double value) : mValue(value)
  {
  }

  explicit operator double() const
  {
    return mValue;
  }

  int64_t toMCSSInt64Round() const
  {
    return xFloatToMCSSInt64Round<double>(mValue);
  }

  uint64_t toMCSUInt64Round() const
  {
    return xFloatToMCSUInt64Round<double>(mValue);
  }
};

}  // end of namespace datatypes

#endif  // MCS_DOUBLE_H_INCLUDED
// vim:ts=2 sw=2:

/* Copyright (C) 2020 MariaDB Corporation.

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
   MA 02110-1301, USA. */

#ifndef MCS_DATATYPES_STRING_H
#define MCS_DATATYPES_STRING_H

#include "conststring.h"
#include "collation.h"  // class Charset

namespace datatypes
{
class TCharShort
{
  int64_t mValue;

 public:
  TCharShort(int64_t value) : mValue(value)
  {
  }
  utils::ConstString toConstString(uint32_t width) const
  {
    utils::ConstString res = utils::ConstString((const char*)&mValue, width);
    return res.rtrimZero();
  }
  static int strnncollsp(const datatypes::Charset& cs, int64_t a, int64_t b, uint32_t width)
  {
    datatypes::TCharShort sa(a);
    datatypes::TCharShort sb(b);
    return cs.strnncollsp(sa.toConstString(width), sb.toConstString(width));
  }
};

}  // namespace datatypes

#endif  // MCS_DATATYPES_STRING_H

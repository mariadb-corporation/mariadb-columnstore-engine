/* Copyright (C) 2020 MariaDB Corporation.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335  USA */

#include "utils_utf8.h"

#include "mariadb_my_sys.h"

namespace datatypes
{
static inline CHARSET_INFO& get_charset_or_bin(int32_t charsetNumber)
{
  CHARSET_INFO* cs = get_charset(charsetNumber, MYF(MY_WME));
  return cs ? *cs : my_charset_bin;
}

Charset::Charset(uint32_t charsetNumber) : mCharset(&get_charset_or_bin(charsetNumber))
{
}

}  // namespace datatypes

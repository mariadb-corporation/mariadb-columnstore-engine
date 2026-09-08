/* Copyright (c) 2000, 2010, Oracle and/or its affiliates. All rights reserved.

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

/* Copied from MariaDB server sql/sql_crypt.h */

#pragma once

// #include "my_global.h"
/* Macros to make switching between C and C++ mode easier */
#ifdef __cplusplus
#define C_MODE_START \
  extern "C"         \
  {
#define C_MODE_END }
#else
#define C_MODE_START
#define C_MODE_END
#endif
#include "my_rnd.h"

namespace funcexp
{
class SQL_CRYPT
{
  struct my_rnd_struct rand, org_rand;
  char decode_buff[256], encode_buff[256];
  uint shift;

 public:
  SQL_CRYPT() = default;
  explicit SQL_CRYPT(ulong* seed)
  {
    init(seed);
  }
  ~SQL_CRYPT() = default;
  void init(ulong* seed);
  void reinit()
  {
    shift = 0;
    rand = org_rand;
  }
  void encode(char* str, uint length);
  void decode(char* str, uint length);
};

}  // namespace funcexp

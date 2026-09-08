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

/* Copied from MariaDB server sql/sql_crypt.cc */

#include <cstdlib>
#include <string>
using namespace std;

#include "sql_crypt.h"

namespace funcexp
{
void SQL_CRYPT::init(ulong* rand_nr)
{
  uint i;
  my_rnd_init(&rand, rand_nr[0], rand_nr[1]);

  for (i = 0; i <= 255; i++)
    decode_buff[i] = (char)i;

  for (i = 0; i <= 255; i++)
  {
    int idx = (uint)(my_rnd(&rand) * 255.0);
    char a = decode_buff[idx];
    decode_buff[idx] = decode_buff[i];
    decode_buff[+i] = a;
  }
  for (i = 0; i <= 255; i++)
    encode_buff[(unsigned char)decode_buff[i]] = i;
  org_rand = rand;
  shift = 0;
}

void SQL_CRYPT::encode(char* str, uint length)
{
  for (uint i = 0; i < length; i++)
  {
    shift ^= (uint)(my_rnd(&rand) * 255.0);
    uint idx = (uint)(unsigned char)str[0];
    *str++ = (char)((unsigned char)encode_buff[idx] ^ shift);
    shift ^= idx;
  }
}

void SQL_CRYPT::decode(char* str, uint length)
{
  for (uint i = 0; i < length; i++)
  {
    shift ^= (uint)(my_rnd(&rand) * 255.0);
    uint idx = (uint)((unsigned char)str[0] ^ shift);
    *str = decode_buff[idx];
    shift ^= (uint)(unsigned char)*str++;
  }
}

}  // namespace funcexp

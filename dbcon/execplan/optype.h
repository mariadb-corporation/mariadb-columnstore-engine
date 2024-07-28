/* Copyright (C) 2016-2024 MariaDB Corporation

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

#pragma once

#include <iostream>

namespace execplan
{
enum OpType
{
  OP_ADD = 0,
  OP_SUB,
  OP_MUL,
  OP_DIV,
  OP_EQ,
  OP_NE,
  OP_GT,
  OP_GE,
  OP_LT,
  OP_LE,
  OP_LIKE,
  OP_NOTLIKE,
  OP_AND,
  OP_OR,
  OP_ISNULL,
  OP_ISNOTNULL,
  OP_BETWEEN,
  OP_NOTBETWEEN,
  OP_IN,
  OP_NOTIN,
  OP_XOR,
  OP_UNKNOWN,
};

std::ostream& operator<<(std::ostream& os, execplan::OpType type);
}  // namespace execplan

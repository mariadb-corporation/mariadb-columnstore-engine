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

#include <iostream>

#include "optype.h"

namespace execplan
{
std::ostream& operator<<(std::ostream& os, execplan::OpType type)
{
  switch (type)
  {
    case execplan::OP_ADD: os << "+"; break;
    case execplan::OP_SUB: os << "-"; break;
    case execplan::OP_MUL: os << "*"; break;
    case execplan::OP_DIV: os << "/"; break;
    case execplan::OP_EQ: os << "=="; break;
    case execplan::OP_NE: os << "!="; break;
    case execplan::OP_GT: os << ">"; break;
    case execplan::OP_GE: os << ">="; break;
    case execplan::OP_LT: os << "<"; break;
    case execplan::OP_LE: os << "<="; break;
    case execplan::OP_LIKE: os << "LIKE"; break;
    case execplan::OP_NOTLIKE: os << "NOT LIKE"; break;
    case execplan::OP_AND: os << "AND"; break;
    case execplan::OP_OR: os << "OR"; break;
    case execplan::OP_ISNULL: os << "IS NULL"; break;
    case execplan::OP_ISNOTNULL: os << "IS NOT NULL"; break;
    case execplan::OP_BETWEEN: os << "BETWEEN"; break;
    case execplan::OP_NOTBETWEEN: os << "NOT BETWEEN"; break;
    case execplan::OP_IN: os << "IN"; break;
    case execplan::OP_NOTIN: os << "NOT IN"; break;
    case execplan::OP_XOR: os << "XOR"; break;
    case execplan::OP_UNKNOWN: os << "UNKNOWN"; break;
    default: os << "UNDEFINED";  // Catch-all for safety
  }
  return os;
}

}  // namespace execplan

/* Copyright (C) 2014 InfiniDB, Inc.

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

/****************************************************************************
 * $Id: func_instr.cpp 3645 2013-03-19 13:10:24Z rdempsey $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_instr::operationType(FunctionParm& fp,
                                                        CalpontSystemCatalog::ColType& resultType)
{
  CalpontSystemCatalog::ColType ct;
  ct.colDataType = CalpontSystemCatalog::VARCHAR;
  ct.colWidth = 255;
  return ct;
}

int64_t Func_instr::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                              CalpontSystemCatalog::ColType& colType)
{
  int64_t start = 0;
  int64_t start0 = 0;
  my_match_t match;

  const std::string& str = parm[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return 0;
  const char* s1 = str.c_str();
  uint32_t l1 = (uint32_t)str.length();

  const std::string& substr = parm[1]->data()->getStrVal(row, isNull);
  if (isNull)
    return 0;

  const char* s2 = substr.c_str();
  uint32_t l2 = (uint32_t)substr.length();
  if (l2 < 1)
    return start + 1;

  CHARSET_INFO* cs = colType.getCharset();

  if (parm.size() == 3)
  {
    start0 = start = parm[2]->data()->getIntVal(row, isNull) - 1;

    if ((start < 0) || (start > l1))
      return 0;

    start = (int64_t)cs->charpos(s1, s1 + l1, start);  // adjust start for multi-byte

    if (start + l2 > l1)  // Substring is longer than str at pos.
      return 0;
  }

  if (!cs->instr(s1 + start, l1 - start, s2, l2, &match, 1))
    return 0;
  return (int64_t)match.mb_len + start0 + 1;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

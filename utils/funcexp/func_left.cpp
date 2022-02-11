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
 * $Id: func_left.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
 *
 *
 ****************************************************************************/
#include <string>
using namespace std;

#include "functor_str.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_left::operationType(FunctionParm& fp,
                                                       CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

std::string Func_left::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                 execplan::CalpontSystemCatalog::ColType& type)
{
  CHARSET_INFO* cs = type.getCharset();
  // The original string
  const string& src = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";
  if (src.empty() || src.length() == 0)
    return src;
  // binLen represents the number of bytes in src
  size_t binLen = src.length();
  const char* pos = src.c_str();
  const char* end = pos + binLen;

  size_t trimLength = fp[1]->data()->getUintVal(row, isNull);
  if (isNull || trimLength <= 0)
    return "";

  size_t charPos;

  if ((binLen <= trimLength) || (binLen <= (charPos = cs->charpos(pos, end, trimLength))))
  {
    return src;
  }

  std::string ret(pos, charPos);
  return ret;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

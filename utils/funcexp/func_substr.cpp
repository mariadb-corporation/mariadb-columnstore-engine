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
 * $Id: func_substr.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
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
CalpontSystemCatalog::ColType Func_substr::operationType(FunctionParm& fp,
                                                         CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

std::string Func_substr::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType& ct)
{
  CHARSET_INFO* cs = ct.getCharset();

  const string& str = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";
  int64_t strLen = str.length();
  const char* strptr = str.c_str();
  const char* strend = strptr + strLen;
  uint32_t strChars = cs->numchars(strptr, strend);

  int64_t start = fp[1]->data()->getIntVal(row, isNull) - 1;
  if (isNull)
    return "";
  if (start < -1)  // negative pos, beginning from end
    start += strChars + 1;
  if (start < 0 || strChars <= start)
  {
    return "";
  }

  int64_t length;
  if (fp.size() == 3)
  {
    length = fp[2]->data()->getIntVal(row, isNull);
    if (isNull)
      return "";
    if (length < 1)
      return "";
  }
  else
  {
    length = strChars - start;
  }

  // start is now number of chars into str to start the substring
  // We convert it to number of bytes:
  start = cs->charpos(strptr, strend, start);
  // Convert length to bytes as well
  length = cs->charpos(strptr + start, strend, length);
  if ((start < 0) || (start + 1 > strLen))
    return "";

  if (start == 0 && strLen == length)
    return str;

  length = std::min(length, strLen - start);

  std::string ret(strptr + start, length);
  return ret;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

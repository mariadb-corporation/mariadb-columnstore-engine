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
 * $Id: func_insert.cpp 2477 2011-04-01 16:07:35Z rdempsey $
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

#include "utf8.h"
using namespace utf8;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_insert::operationType(FunctionParm& fp,
                                                         CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

std::string Func_insert::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType&)
{
  string src;
  string tnewstr;
  int64_t start, length;

  stringValue(fp[0], row, isNull, src);
  if (isNull)
    return "";

  stringValue(fp[3], row, isNull, tnewstr);
  if (isNull)
    return "";

  start = fp[1]->data()->getIntVal(row, isNull);
  if (isNull)
    return "";
  start--;  // Because SQL syntax is 1 based and we want 0 based.

  length = fp[2]->data()->getIntVal(row, isNull);
  if (isNull)
    return "";

  CHARSET_INFO* cs = fp[0]->data()->resultType().getCharset();

  // binLen represents the number of bytes
  int64_t binLen = static_cast<int64_t>(src.length());
  const char* pos = src.c_str();
  const char* end = pos + binLen;
  // strLen is number of characters
  int64_t strLen = cs->numchars(pos, end);

  // Return the original string if start isn't within the string.
  if ((start < 0) || start >= strLen)
    return src;

  if ((length < 0) || (length > strLen))
    length = strLen;

  // Convert start and length from characters to bytes.
  start = cs->charpos(pos, end, start);
  length = cs->charpos(pos + start, end, length);

  string out;
  out.reserve(binLen - length + tnewstr.length() + 1);

  out.append(src.c_str(), start);
  out.append(tnewstr.c_str(), tnewstr.length());
  if (binLen - start - length > 0)
    out.append(src.c_str() + start + length, binLen - start - length);

  return out;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

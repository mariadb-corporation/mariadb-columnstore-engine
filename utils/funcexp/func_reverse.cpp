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
 * $Id: func_reverse.cpp 2477 2011-04-01 16:07:35Z rdempsey $
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
CalpontSystemCatalog::ColType Func_reverse::operationType(FunctionParm& fp,
                                                          CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

std::string Func_reverse::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                    execplan::CalpontSystemCatalog::ColType& type)
{
  CHARSET_INFO* cs = type.getCharset();

  string str;
  stringValue(fp[0], row, isNull, str);
  if (isNull)
    return "";
  if (str.empty() || str.length() == 0)
    return str;
  // binLen represents the number of bytes in str
  size_t binLen = str.length();
  const char* pos = str.c_str();
  const char* end = pos + binLen;

  char* pbuf = new char[binLen + 1];
  pbuf[binLen] = 0;
  char* tmp = pbuf + binLen;

  if (cs->use_mb())  // uses multi-byte characters
  {
    uint32 l;
    while (pos < end)
    {
      if ((l = my_ismbchar(cs, pos, end)))
      {
        tmp -= l;
        idbassert(tmp >= pbuf);
        memcpy(tmp, pos, l);
        pos += l;
      }
      else
      {
        *--tmp = *pos++;
      }
    }
  }
  else
  {
    while (pos < end)
      *--tmp = *pos++;
  }

  string rstr = pbuf;
  delete[] pbuf;
  return rstr;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

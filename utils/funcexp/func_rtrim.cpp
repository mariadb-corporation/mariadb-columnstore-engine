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
 * $Id: func_rtrim.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
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
CalpontSystemCatalog::ColType Func_rtrim::operationType(FunctionParm& fp,
                                                        CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

std::string Func_rtrim::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
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
  // strLen = the number of characters in src
  size_t strLen = cs->numchars(pos, end);

  // The trim characters.
  const string& trim = (fp.size() > 1 ? fp[1]->data()->getStrVal(row, isNull) : " ");
  // binTLen represents the number of bytes in trim
  size_t binTLen = trim.length();
  const char* posT = trim.c_str();
  // strTLen = the number of characters in trim
  size_t strTLen = cs->numchars(posT, posT + binTLen);
  if (strTLen == 0 || strTLen > strLen)
    return src;

  if (binTLen == 1)
  {
    const char* ptr = pos;
    if (cs->use_mb())  // This is a multi-byte charset
    {
      const char* p = pos;
      uint32 l;
      // Multibyte characters in the string give us alignment problems
      // What we do here is skip past any multibyte characters. Whn
      // don with this loop, ptr is pointing to a singlebyte char that
      // is after all multibyte chars in the string, or to end.
      while (ptr < end)
      {
        if ((l = my_ismbchar(cs, ptr,
                             end)))  // returns the number of bytes in the leading char or zero if one byte
        {
          ptr += l;
          p = ptr;
        }
        else
        {
          ++ptr;
        }
      }
      ptr = p;
    }
    while (ptr < end && end[-1] == *posT)
    {
      --end;
      --binLen;
    }
  }
  else
  {
    // An uncommon case where the space character is > 1 byte
    if (cs->use_mb())  // This is a multi-byte charset
    {
      // The problem is that the byte pattern at the end could
      // match memcmp, but not be correct since the first byte compared
      // may actually be a second or later byte from a previous char.

      // We start at the beginning of the string and move forward
      // one character at a time until we reach the end. Then we can
      // safely compare and remove on character. Then back to the beginning
      // and try again.
      while (end - binTLen >= pos)
      {
        const char* p = pos;
        uint32 l;
        while (p + binTLen < end)
        {
          if ((l = my_ismbchar(cs, p,
                               end)))  // returns the number of bytes in the leading char or zero if one byte
            p += l;
          else
            ++p;
        }
        if (p + binTLen == end && memcmp(p, posT, binTLen) == 0)
        {
          end -= binTLen;
          binLen -= binTLen;
        }
        else
        {
          break;  // We've run out of places to look
        }
      }
    }
    else
    {
      // This implies we have a single byte charset and a multibyte
      // space character.
      // Should never get here, since rtrim only trims space characters
      // Included for completeness.
      while (end - binTLen >= pos && memcmp(end - binTLen, posT, binTLen) == 0)
      {
        end -= binTLen;
        binLen -= binTLen;
      }
    }
  }
  // Turn back to a string
  std::string ret(pos, binLen);
  return ret;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporation

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
 * $Id: func_substring_index.cpp 2477 2011-04-01 16:07:35Z rdempsey $
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
CalpontSystemCatalog::ColType Func_substring_index::operationType(FunctionParm& fp,
                                                                  CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

std::string Func_substring_index::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                            execplan::CalpontSystemCatalog::ColType& ct)
{
  CHARSET_INFO* cs = ct.getCharset();

  const string& str = fp[0]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";
  int64_t strLen = str.length();

  const string& delimstr = fp[1]->data()->getStrVal(row, isNull);
  if (isNull)
    return "";
  int64_t delimLen = delimstr.length();

  int64_t count = fp[2]->data()->getIntVal(row, isNull);
  if (isNull)
    return "";

  if (strLen == 0 || delimLen == 0 || count == 0)
    return "";

  if (count > strLen)
    return str;

  if ((count < 0) && ((count * -1) > strLen))
    return str;

  bool binaryCmp = (cs->state & MY_CS_BINSORT) || !cs->use_mb();
  std::string value;  // Only used if !use_mb()

  if (!binaryCmp)  // Charset supports multibyte characters
  {
    const char* src = str.c_str();
    const char* srcEnd = src + strLen;
    const char* srchEnd = srcEnd - delimLen + 1;
    const char* delim = delimstr.c_str();
    const char* delimEnd = delim + delimLen;
    char* ptr = const_cast<char*>(src);
    char *i, *j;
    uint32_t l;
    int32_t n = 0, c = count, pass;
    // For count > 0, this loop goes once.
    // For count < 0, it goes twice
    for (pass = (count > 0 ? 1 : 0); pass < 2; ++pass)
    {
      while (ptr < srchEnd)
      {
        bool found = false;
        if (*ptr == *delim)  // If the first byte matches, maybe we have a match
        {
          // Do a byte by byte compare of src at that spot against delim
          i = ptr + 1;
          j = const_cast<char*>(delim) + 1;
          found = true;
          while (j != delimEnd)
          {
            if (*i++ != *j++)
            {
              found = false;
              break;
            }
          }
        }
        if (found)
        {
          if (pass == 0)
            ++n;
          else if (!--c)
            break;

          ptr += delimLen;
          continue;
        }
        else
        {
          // move to the next character
          if ((l = my_ismbchar(
                   cs, ptr, srcEnd)))  // returns the number of bytes in the leading char or zero if one byte
            ptr += l;
          else
            ++ptr;
        }
      }
      if (pass == 0) /* count<0 */
      {
        c += n + 1;
        if (c <= 0)
        {
          return str;  // not found, return the original string
        }
        // Go back and do a second pass
        ptr = const_cast<char*>(src);
      }
      else
      {
        if (c)
        {
          return str;  // not found, return the original string
        }
      }
    }

    if (count > 0) /* return left part */
    {
      std::string ret(src, ptr - src);
      return ret;
    }
    else /* return right part */
    {
      ptr += delimLen;
      std::string ret(ptr, srcEnd - ptr);
      return ret;
    }
  }
  else
  {
    if (count > 0)
    {
      int pointer = 0;
      int64_t end = strLen;
      for (int64_t i = 0; i < count; i++)
      {
        string::size_type pos = str.find(delimstr, pointer);

        if (pos != string::npos)
          pointer = pos + 1;

        end = pos;
      }

      value = str.substr(0, end);
    }
    else
    {
      count = -count;
      int pointer = strLen;
      int start = 0;

      for (int64_t i = 0; i < count; i++)
      {
        string::size_type pos = str.rfind(delimstr, pointer);

        if (pos != string::npos)
        {
          if (count > strLen)
            return "";

          pointer = pos - 1;
          start = pos + 1;
        }
        else
          start = 0;
      }

      value = str.substr(start, strLen);
    }
  }
  return value;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

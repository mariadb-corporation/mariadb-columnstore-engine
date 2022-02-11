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
 * $Id: func_rpad.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
 *
 *
 ****************************************************************************/
#include "errorids.h"
#include <string>
using namespace std;

#include "functor_str.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

#define INT_MAX32 0x7FFFFFFF

namespace funcexp
{
const string Func_rpad::fPad = " ";

CalpontSystemCatalog::ColType Func_rpad::operationType(FunctionParm& fp,
                                                       CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

std::string Func_rpad::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
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

  // In the case where someone entered pad length as a quoted string,
  // it may be interpreted by columnstore to be an actual string
  // and stored in fResult.int as a htonl of that string,
  // However fResult.double is always correct, so we'll use that.
  size_t padLength = (size_t)fp[1]->data()->getDoubleVal(row, isNull);
  if (isNull || padLength <= 0)
    return "";
  if (padLength > (size_t)INT_MAX32)
    padLength = (size_t)INT_MAX32;

  if (padLength < strLen)
  {
    binLen = cs->charpos(pos, end, padLength);
    std::string ret(pos, binLen);
    return ret;
  }

  // The pad characters.
  const string* pad = &fPad;
  if (fp.size() > 2)
  {
    pad = &fp[2]->data()->getStrVal(row, isNull);
  }
  // binPLen represents the number of bytes in pad
  size_t binPLen = pad->length();
  const char* posP = pad->c_str();
  // plen = the number of characters in pad
  size_t plen = cs->numchars(posP, posP + binPLen);
  if (plen == 0)
    return src;

  size_t byteCount = (padLength + 1) * cs->mbmaxlen;  // absolute maximun number of bytes
  char* buf = new char[byteCount];
  char* pBuf = buf;

  byteCount = 0;

  memcpy(pBuf, pos, binLen);
  byteCount += binLen;
  padLength -= strLen;
  pBuf += binLen;

  while (padLength >= plen)
  {
    memcpy(pBuf, posP, binPLen);
    padLength -= plen;
    byteCount += binPLen;
    pBuf += binPLen;
  }
  // Sometimes, in a case with multi-char pad, we need to add a partial pad
  if (padLength > 0)
  {
    size_t partialSize = cs->charpos(posP, posP + plen, padLength);
    memcpy(pBuf, posP, partialSize);
    byteCount += partialSize;
  }

  std::string ret(buf, byteCount);
  delete[] buf;
  return ret;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

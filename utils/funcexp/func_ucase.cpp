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
 * $Id: func_ucase.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
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

class to_upper
{
 public:
  char operator()(char c) const  // notice the return type
  {
    return toupper(c);
  }
};

namespace funcexp
{
CalpontSystemCatalog::ColType Func_ucase::operationType(FunctionParm& fp,
                                                        CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

std::string Func_ucase::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& colType)
{
  const string& tstr = fp[0]->data()->getStrVal(row, isNull);

  if (isNull)
    return "";

  CHARSET_INFO* cs = colType.getCharset();
  uint64_t inLen = tstr.length();
  uint64_t bufLen = inLen * cs->caseup_multiply;
  char* outBuf = new char[bufLen];

  uint64_t outLen = cs->caseup(tstr.c_str(), inLen, outBuf, bufLen);

  string ret = string(outBuf, outLen);
  delete[] outBuf;
  return ret;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

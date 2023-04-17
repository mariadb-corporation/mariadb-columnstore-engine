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
 * $Id: func_lcase.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
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
CalpontSystemCatalog::ColType Func_lcase::operationType(FunctionParm& fp,
                                                        CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  return fp[0]->data()->resultType();
}

std::string Func_lcase::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& colType)
{
  const auto& tstr = fp[0]->data()->getStrVal(row, isNull);

  if (isNull)
    return "";

  CHARSET_INFO* cs = colType.getCharset();
  uint64_t inLen = tstr.length();
#if MYSQL_VERSION_ID >= 101004
  uint64_t bufLen = inLen * cs->casedn_multiply();
#else
  uint64_t bufLen = inLen * cs->casedn_multiply;
#endif
  char* outBuf = new char[bufLen];

  uint64_t outLen = cs->casedn(tstr.str(), inLen, outBuf, bufLen);

  string ret = string(outBuf, outLen);
  delete[] outBuf;
  return ret;
}

}  // namespace funcexp

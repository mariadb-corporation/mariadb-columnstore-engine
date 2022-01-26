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
 * $Id: func_least.cpp 3954 2013-07-08 16:30:15Z bpaul $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_all.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "joblisttypes.h"
using namespace joblist;

using namespace funcexp;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_least::operationType(FunctionParm& fp,
                                                        CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  // return fp[0]->data()->resultType();
  return resultType;
}

int64_t Func_least::getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                              execplan::CalpontSystemCatalog::ColType& op_ct)
{
  double str = fp[0]->data()->getDoubleVal(row, isNull);

  double leastStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    double str1 = fp[i]->data()->getDoubleVal(row, isNull);

    if (leastStr > str1)
      leastStr = str1;
  }

  return (int64_t)leastStr;
}

double Func_least::getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                execplan::CalpontSystemCatalog::ColType& op_ct)
{
  double str = fp[0]->data()->getDoubleVal(row, isNull);

  double leastStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    double str1 = fp[i]->data()->getDoubleVal(row, isNull);

    if (leastStr > str1)
      leastStr = str1;
  }

  return (double)leastStr;
}

long double Func_least::getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                         execplan::CalpontSystemCatalog::ColType& op_ct)
{
  long double str = fp[0]->data()->getLongDoubleVal(row, isNull);

  long double leastStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    long double str1 = fp[i]->data()->getLongDoubleVal(row, isNull);

    if (leastStr > str1)
      leastStr = str1;
  }

  return leastStr;
}

std::string Func_least::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& op_ct)
{
  string leastStr = fp[0]->data()->getStrVal(row, isNull);
  CHARSET_INFO* cs = fp[0]->data()->resultType().getCharset();

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    const string& str1 = fp[i]->data()->getStrVal(row, isNull);

    if (cs->strnncoll(leastStr.c_str(), leastStr.length(), str1.c_str(), str1.length()) > 0)
    {
      leastStr = str1;
    }
  }

  return leastStr;
}

IDB_Decimal Func_least::getDecimalVal(Row& row, FunctionParm& fp, bool& isNull,
                                      CalpontSystemCatalog::ColType& ct)
{
  IDB_Decimal str = fp[0]->data()->getDecimalVal(row, isNull);

  IDB_Decimal leastStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    IDB_Decimal str1 = fp[i]->data()->getDecimalVal(row, isNull);

    if (leastStr > str1)
      leastStr = str1;
  }

  return leastStr;
}

int32_t Func_least::getDateIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& op_ct)
{
  int32_t str = fp[0]->data()->getDateIntVal(row, isNull);

  int32_t leastStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    int32_t str1 = fp[i]->data()->getDateIntVal(row, isNull);

    if (leastStr > str1)
      leastStr = str1;
  }

  return leastStr;
}

int64_t Func_least::getDatetimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct)
{
  int64_t str = fp[0]->data()->getDatetimeIntVal(row, isNull);

  int64_t leastStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    int64_t str1 = fp[i]->data()->getDatetimeIntVal(row, isNull);

    if (leastStr > str1)
      leastStr = str1;
  }

  return leastStr;
}

int64_t Func_least::getTimestampIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                       execplan::CalpontSystemCatalog::ColType& op_ct)
{
  int64_t str = fp[0]->data()->getTimestampIntVal(row, isNull);

  int64_t leastStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    int64_t str1 = fp[i]->data()->getTimestampIntVal(row, isNull);

    if (leastStr > str1)
      leastStr = str1;
  }

  return leastStr;
}

int64_t Func_least::getTimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                  execplan::CalpontSystemCatalog::ColType& op_ct)
{
  // Strip off unused day
  int64_t leastStr = fp[0]->data()->getTimeIntVal(row, isNull);

  int64_t str = leastStr << 12;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    int64_t str1 = fp[i]->data()->getTimeIntVal(row, isNull);
    int64_t str2 = str1 << 12;

    if (str > str2)
    {
      leastStr = str1;
      str = str2;
    }
  }

  return leastStr;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

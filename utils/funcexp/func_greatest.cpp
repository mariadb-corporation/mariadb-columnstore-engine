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
 * $Id: func_greatest.cpp 3954 2013-07-08 16:30:15Z bpaul $
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
CalpontSystemCatalog::ColType Func_greatest::operationType(FunctionParm& fp,
                                                           CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  // return fp[0]->data()->resultType();
  return resultType;
}

int64_t Func_greatest::getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                 execplan::CalpontSystemCatalog::ColType& op_ct)
{
  double str = fp[0]->data()->getDoubleVal(row, isNull);

  double greatestStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    double str1 = fp[i]->data()->getDoubleVal(row, isNull);

    if (greatestStr < str1)
      greatestStr = str1;
  }

  uint64_t tmp = (uint64_t)greatestStr;
  return (int64_t)tmp;
}

uint64_t Func_greatest::getUintVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType& op_ct)
{
  double str = fp[0]->data()->getDoubleVal(row, isNull);

  double greatestStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    double str1 = fp[i]->data()->getDoubleVal(row, isNull);

    if (greatestStr < str1)
      greatestStr = str1;
  }

  return (uint64_t)greatestStr;
}

double Func_greatest::getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                   execplan::CalpontSystemCatalog::ColType& op_ct)
{
  double str = fp[0]->data()->getDoubleVal(row, isNull);

  double greatestStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    double str1 = fp[i]->data()->getDoubleVal(row, isNull);

    if (greatestStr < str1)
      greatestStr = str1;
  }

  return (double)greatestStr;
}

long double Func_greatest::getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                            execplan::CalpontSystemCatalog::ColType& op_ct)
{
  long double str = fp[0]->data()->getLongDoubleVal(row, isNull);

  long double greatestStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    long double str1 = fp[i]->data()->getLongDoubleVal(row, isNull);

    if (greatestStr < str1)
      greatestStr = str1;
  }

  return greatestStr;
}

std::string Func_greatest::getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                     execplan::CalpontSystemCatalog::ColType& op_ct)
{
  const string& str = fp[0]->data()->getStrVal(row, isNull);
  CHARSET_INFO* cs = fp[0]->data()->resultType().getCharset();

  string greatestStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    const string& str1 = fp[i]->data()->getStrVal(row, isNull);

    if (cs->strnncoll(greatestStr.c_str(), greatestStr.length(), str1.c_str(), str1.length()) < 0)
    {
      greatestStr = str1;
    }
  }

  return greatestStr;
}

IDB_Decimal Func_greatest::getDecimalVal(Row& row, FunctionParm& fp, bool& isNull,
                                         CalpontSystemCatalog::ColType& ct)
{
  //	double str = fp[0]->data()->getDoubleVal(row, isNull);
  IDB_Decimal str = fp[0]->data()->getDecimalVal(row, isNull);

  IDB_Decimal greatestStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    IDB_Decimal str1 = fp[i]->data()->getDecimalVal(row, isNull);

    if (greatestStr < str1)
      greatestStr = str1;
  }

  return greatestStr;
}

int32_t Func_greatest::getDateIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                     execplan::CalpontSystemCatalog::ColType& ct)
{
  int32_t str = fp[0]->data()->getDateIntVal(row, isNull);

  int32_t greatestStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    int32_t str1 = fp[i]->data()->getDateIntVal(row, isNull);

    if (greatestStr < str1)
      greatestStr = str1;
  }

  return greatestStr;
}

int64_t Func_greatest::getDatetimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                         execplan::CalpontSystemCatalog::ColType& ct)
{
  int64_t str = fp[0]->data()->getDatetimeIntVal(row, isNull);

  int64_t greatestStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    int64_t str1 = fp[i]->data()->getDatetimeIntVal(row, isNull);

    if (greatestStr < str1)
      greatestStr = str1;
  }

  return greatestStr;
}

int64_t Func_greatest::getTimestampIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                          execplan::CalpontSystemCatalog::ColType& ct)
{
  int64_t str = fp[0]->data()->getTimestampIntVal(row, isNull);

  int64_t greatestStr = str;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    int64_t str1 = fp[i]->data()->getTimestampIntVal(row, isNull);

    if (greatestStr < str1)
      greatestStr = str1;
  }

  return greatestStr;
}

int64_t Func_greatest::getTimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                     execplan::CalpontSystemCatalog::ColType& ct)
{
  // Strip off unused day
  int64_t greatestStr = fp[0]->data()->getTimeIntVal(row, isNull);

  int64_t str = greatestStr << 12;

  for (uint32_t i = 1; i < fp.size(); i++)
  {
    int64_t str1 = fp[i]->data()->getTimeIntVal(row, isNull);
    int64_t str2 = str1 << 12;

    if (str < str2)
    {
      greatestStr = str1;
      str = str2;
    }
  }

  return greatestStr;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

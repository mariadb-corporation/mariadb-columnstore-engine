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
 * $Id: func_add_time.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

#include "functor_dtm.h"
#include "funchelpers.h"

namespace funcexp
{
CalpontSystemCatalog::ColType Func_add_time::operationType(FunctionParm& fp,
                                                           CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_add_time::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                 CalpontSystemCatalog::ColType& op_ct)
{
  if (parm[0]->data()->resultType().colDataType == execplan::CalpontSystemCatalog::TIME)
  {
    return getTimeIntVal(row, parm, isNull, op_ct);
  }
  else
  {
    return getDatetimeIntVal(row, parm, isNull, op_ct);
  }
}

string Func_add_time::getStrVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                CalpontSystemCatalog::ColType& ct)
{
  return intToString(getIntVal(row, parm, isNull, ct));
}

int32_t Func_add_time::getDateIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                     CalpontSystemCatalog::ColType& ct)
{
  return (((getDatetimeIntVal(row, parm, isNull, ct) >> 32) & 0xFFFFFFC0) | 0x3E);
}

int64_t Func_add_time::getDatetimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                         CalpontSystemCatalog::ColType& ct)
{
  if (parm[0]->data()->resultType().colDataType == CalpontSystemCatalog::TIMESTAMP)
    return getTimestampIntVal(row, parm, isNull, ct);

  int64_t val1 = parm[0]->data()->getDatetimeIntVal(row, isNull);

  if (isNull)
    return -1;

  // Adding a zero date to a time is always NULL
  if (val1 == 0)
  {
    isNull = true;
    return -1;
  }

  const string& val2 = parm[1]->data()->getStrVal(row, isNull);
  int sign = parm[2]->data()->getIntVal(row, isNull);
  DateTime dt1;
  dt1.year = (val1 >> 48) & 0xffff;
  dt1.month = (val1 >> 44) & 0xf;
  dt1.day = (val1 >> 38) & 0x3f;
  dt1.hour = (val1 >> 32) & 0x3f;
  dt1.minute = (val1 >> 26) & 0x3f;
  dt1.second = (val1 >> 20) & 0x3f;
  dt1.msecond = val1 & 0xfffff;

  int64_t time = DataConvert::stringToTime(val2);

  if (time == -1)
  {
    isNull = true;
    return -1;
  }

  Time t2 = *(reinterpret_cast<Time*>(&time));

  // MySQL TIME type range '-838:59:59' and '838:59:59'
  if (t2.minute > 59 || t2.second > 59 || t2.msecond > 999999)
  {
    isNull = true;
    return -1;
  }

  int val_sign = 1;

  if (t2.hour < 0)
  {
    val_sign = -1;
  }

  if (abs(t2.hour) > 838)
  {
    t2.hour = 838;
    t2.minute = 59;
    t2.second = 59;
    t2.msecond = 999999;
  }

  if (val_sign * sign < 0)
  {
    t2.hour = -abs(t2.hour);
    t2.minute = -abs(t2.minute);
    t2.second = -abs(t2.second);
    t2.msecond = -abs(t2.msecond);
  }
  else
  {
    t2.hour = abs(t2.hour);
    t2.minute = abs(t2.minute);
    t2.second = abs(t2.second);
    t2.msecond = abs(t2.msecond);
  }

  t2.day = 0;

  return addTime(dt1, t2);
}

int64_t Func_add_time::getTimestampIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                          CalpontSystemCatalog::ColType& ct)
{
  int64_t val1 = parm[0]->data()->getTimestampIntVal(row, isNull);

  if (isNull)
    return -1;

  // Adding a zero date to a time is always NULL
  if (val1 == 0)
  {
    isNull = true;
    return -1;
  }

  const string& val2 = parm[1]->data()->getStrVal(row, isNull);
  int sign = parm[2]->data()->getIntVal(row, isNull);
  DateTime dt1;
  TimeStamp timestamp(val1);
  int64_t seconds = timestamp.second;
  MySQLTime m_time;
  gmtSecToMySQLTime(seconds, m_time, ct.getTimeZone());
  dt1.year = m_time.year;
  dt1.month = m_time.month;
  dt1.day = m_time.day;
  dt1.hour = m_time.hour;
  dt1.minute = m_time.minute;
  dt1.second = m_time.second;
  dt1.msecond = timestamp.msecond;

  int64_t time = DataConvert::stringToTime(val2);

  if (time == -1)
  {
    isNull = true;
    return -1;
  }

  Time t2 = *(reinterpret_cast<Time*>(&time));

  // MySQL TIME type range '-838:59:59' and '838:59:59'
  if (t2.minute > 59 || t2.second > 59 || t2.msecond > 999999)
  {
    isNull = true;
    return -1;
  }

  int val_sign = 1;

  if (t2.hour < 0)
  {
    val_sign = -1;
  }

  if (abs(t2.hour) > 838)
  {
    t2.hour = 838;
    t2.minute = 59;
    t2.second = 59;
    t2.msecond = 999999;
  }

  if (val_sign * sign < 0)
  {
    t2.hour = -abs(t2.hour);
    t2.minute = -abs(t2.minute);
    t2.second = -abs(t2.second);
    t2.msecond = -abs(t2.msecond);
  }
  else
  {
    t2.hour = abs(t2.hour);
    t2.minute = abs(t2.minute);
    t2.second = abs(t2.second);
    t2.msecond = abs(t2.msecond);
  }

  t2.day = 0;

  return addTime(dt1, t2);
}

int64_t Func_add_time::getTimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                     CalpontSystemCatalog::ColType& ct)
{
  int64_t val1 = parm[0]->data()->getTimeIntVal(row, isNull);

  if (isNull)
    return -1;

  const string& val2 = parm[1]->data()->getStrVal(row, isNull);
  int sign = parm[2]->data()->getIntVal(row, isNull);
  Time dt1;
  dt1.day = 0;
  dt1.is_neg = val1 >> 63;
  dt1.hour = (val1 >> 40) & 0xfff;
  dt1.minute = (val1 >> 32) & 0xff;
  dt1.second = (val1 >> 24) & 0xff;
  dt1.msecond = val1 & 0xffffff;

  int64_t time = DataConvert::stringToTime(val2);

  if (time == -1)
  {
    isNull = true;
    return -1;
  }

  Time t2 = *(reinterpret_cast<Time*>(&time));

  // MySQL TIME type range '-838:59:59' and '838:59:59'
  if (t2.minute > 59 || t2.second > 59 || t2.msecond > 999999)
  {
    isNull = true;
    return -1;
  }

  int val_sign = 1;

  if (t2.hour < 0)
  {
    val_sign = -1;
  }

  if (abs(t2.hour) > 838)
  {
    t2.hour = 838;
    t2.minute = 59;
    t2.second = 59;
    t2.msecond = 999999;
  }

  t2.day = 0;

  if (val_sign * sign < 0)
  {
    t2.hour = -abs(t2.hour);
    t2.minute = -abs(t2.minute);
    t2.second = -abs(t2.second);
    t2.msecond = -abs(t2.msecond);
  }
  else
  {
    t2.hour = abs(t2.hour);
    t2.minute = abs(t2.minute);
    t2.second = abs(t2.second);
    t2.msecond = abs(t2.msecond);
  }

  return addTime(dt1, t2);
}

}  // namespace funcexp
// vim:ts=4 sw=4:

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
 * $Id: func_time.cpp 3495 2013-01-21 14:09:51Z rdempsey $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_dtm.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{
CalpontSystemCatalog::ColType Func_time::operationType(FunctionParm& fp,
                                                       CalpontSystemCatalog::ColType& resultType)
{
  CalpontSystemCatalog::ColType ct;
  ct.colDataType = CalpontSystemCatalog::VARCHAR;
  ct.colWidth = 255;
  return ct;
}

string Func_time::getStrVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                            CalpontSystemCatalog::ColType& ct)
{
  int64_t val = 0;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      val = dataconvert::DataConvert::intToTime(parm[0]->data()->getIntVal(row, isNull));

      if (val == -1)
        isNull = true;

      // else
      //		return *(reinterpret_cast<uint64_t*>(&val));
      break;
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      if (parm[0]->data()->resultType().scale == 0)
      {
        val = dataconvert::DataConvert::intToTime(parm[0]->data()->getIntVal(row, isNull));

        if (val == -1)
          isNull = true;

        // else
        //	return *(reinterpret_cast<uint64_t*>(&val));
      }
      else
      {
        isNull = true;
      }

      break;
    }

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    {
      isNull = true;
      break;
    }

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      val = dataconvert::DataConvert::stringToTime(parm[0]->data()->getStrVal(row, isNull));

      if (val == -1)
        isNull = true;

      // else
      //	return *(reinterpret_cast<uint64_t*>(&val));
      break;
    }

    case execplan::CalpontSystemCatalog::TIME:
    {
      val = parm[0]->data()->getTimeIntVal(row, isNull);
      break;
    }

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      val = parm[0]->data()->getTimeIntVal(row, isNull);
      break;
    }

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      dataconvert::TimeStamp timestamp(parm[0]->data()->getTimestampIntVal(row, isNull));
      int64_t seconds = timestamp.second;
      dataconvert::MySQLTime m_time;
      dataconvert::gmtSecToMySQLTime(seconds, m_time, ct.getTimeZone());
      dataconvert::Time time;
      time.hour = m_time.hour;
      time.minute = m_time.minute;
      time.second = m_time.second;
      time.is_neg = 0;
      time.day = 0;
      time.msecond = 0;
      val = *(reinterpret_cast<int64_t*>(&time));
      break;
    }

    default:
    {
      isNull = true;
    }
  }

  if (isNull)
    return "";

  char buf[30] = {'\0'};
  dataconvert::DataConvert::timeToString(val, buf, sizeof(buf));
  string time(buf);
  return time;
}

int64_t Func_time::getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                             execplan::CalpontSystemCatalog::ColType& op_ct)
{
  return dataconvert::DataConvert::timeToInt(getStrVal(row, fp, isNull, op_ct));
}

double Func_time::getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct)
{
  // convert time value to int followiing mysql. e.g. 23:34:34 => 233434
  int64_t datetimevalue = dataconvert::DataConvert::stringToDatetime(fp[0]->data()->getStrVal(row, isNull));
  return ((unsigned)((datetimevalue >> 32) & 0x3f)) * 10000 +
         ((unsigned)((datetimevalue >> 26) & 0x3f)) * 100 + (unsigned)((datetimevalue >> 20) & 0x3f);
}

}  // namespace funcexp
// vim:ts=4 sw=4:

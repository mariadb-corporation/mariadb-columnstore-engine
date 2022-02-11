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
 * $Id: func_dayname.cpp 2477 2011-04-01 16:07:35Z rdempsey $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{
CalpontSystemCatalog::ColType Func_dayname::operationType(FunctionParm& fp,
                                                          CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_dayname::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                CalpontSystemCatalog::ColType& op_ct)
{
  uint32_t year = 0;
  uint32_t month = 0;
  uint32_t day = 0;
  int64_t val = 0;
  int32_t dayofweek = 0;

  dataconvert::DateTime aDateTime;
  dataconvert::Time aTime;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case CalpontSystemCatalog::DATE:
      val = parm[0]->data()->getIntVal(row, isNull);
      year = (uint32_t)((val >> 16) & 0xffff);
      month = (uint32_t)((val >> 12) & 0xf);
      day = (uint32_t)((val >> 6) & 0x3f);
      break;

    case CalpontSystemCatalog::DATETIME:
      val = parm[0]->data()->getIntVal(row, isNull);
      year = (uint32_t)((val >> 48) & 0xffff);
      month = (uint32_t)((val >> 44) & 0xf);
      day = (uint32_t)((val >> 38) & 0x3f);
      break;

    case CalpontSystemCatalog::TIMESTAMP:
    {
      val = parm[0]->data()->getIntVal(row, isNull);
      dataconvert::TimeStamp timestamp(val);
      int64_t seconds = timestamp.second;
      dataconvert::MySQLTime time;
      dataconvert::gmtSecToMySQLTime(seconds, time, op_ct.getTimeZone());
      year = time.year;
      month = time.month;
      day = time.day;
      break;
    }

    // Time adds to now() and then gets value
    case CalpontSystemCatalog::TIME:
      aDateTime = static_cast<dataconvert::DateTime>(nowDatetime());
      aTime = parm[0]->data()->getTimeIntVal(row, isNull);
      aTime.day = 0;
      val = addTime(aDateTime, aTime);
      year = (uint32_t)((val >> 48) & 0xffff);
      month = (uint32_t)((val >> 44) & 0xf);
      day = (uint32_t)((val >> 38) & 0x3f);
      break;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::VARCHAR:
      val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));

      if (val == -1)
      {
        isNull = true;
        return -1;
      }
      else
      {
        year = (uint32_t)((val >> 48) & 0xffff);
        month = (uint32_t)((val >> 44) & 0xf);
        day = (uint32_t)((val >> 38) & 0x3f);
      }

      break;

    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::INT:
      val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));

      if (val == -1)
      {
        isNull = true;
        return -1;
      }
      else
      {
        year = (uint32_t)((val >> 48) & 0xffff);
        month = (uint32_t)((val >> 44) & 0xf);
        day = (uint32_t)((val >> 38) & 0x3f);
      }

      break;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
      if (parm[0]->data()->resultType().scale == 0)
      {
        val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));

        if (val == -1)
        {
          isNull = true;
          return -1;
        }
        else
        {
          year = (uint32_t)((val >> 48) & 0xffff);
          month = (uint32_t)((val >> 44) & 0xf);
          day = (uint32_t)((val >> 38) & 0x3f);
        }
      }
      else
      {
        isNull = true;
        return -1;
      }

      break;

    default: isNull = true; return -1;
  }

  dayofweek = helpers::calc_mysql_weekday(year, month, day, false);
  return dayofweek;
}

string Func_dayname::getStrVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
  int32_t weekday = getIntVal(row, parm, isNull, op_ct);

  if (weekday == -1)
    return "";

  return helpers::weekdayFullNames[weekday];
}

}  // namespace funcexp
// vim:ts=4 sw=4:

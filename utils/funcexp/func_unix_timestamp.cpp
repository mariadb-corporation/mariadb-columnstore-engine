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
 * $Id: func_unix_timestamp.cpp 2521 2011-05-02 19:36:52Z zzhu $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
#include "funchelpers.h"

namespace funcexp
{
CalpontSystemCatalog::ColType Func_unix_timestamp::operationType(FunctionParm& /*fp*/,
                                                                 CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_unix_timestamp::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                       CalpontSystemCatalog::ColType& ct)
{
  int64_t val = parm[0]->data()->getIntVal(row, isNull);

  if (isNull)  // no paramter, return current unix_timestamp
  {
    // get current time in seconds
    time_t cal;
    time(&cal);
    return (int64_t)cal;
  }

  uint32_t year = 0, month = 0, day = 0, hour = 0, min = 0, sec = 0;

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
      hour = (uint32_t)((val >> 32) & 0x3f);
      min = (uint32_t)((val >> 26) & 0x3f);
      sec = (uint32_t)((val >> 20) & 0x3f);
      break;

    case CalpontSystemCatalog::TIMESTAMP:
      val = parm[0]->data()->getIntVal(row, isNull);
      // TimeStamp timeStamp(val);
      return ((val >> 20) & 0xFFFFFFFFFFFULL);
      break;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::TEXT:
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

  // same algorithm as my_time.c:my_system_gmt_sec
  time_t tmp_t = 0;
  int64_t my_time_zone = ct.getTimeZone();

  if ((year == helpers::TIMESTAMP_MAX_YEAR) && (month == 1) && (day > 4))
  {
    day -= 2;
    shift = 2;
  }

  tmp_t = ((helpers::calc_mysql_daynr(year, month, day) - 719528L) * 86400L + int64_t(hour) * 3600L +
                    int64_t(min * 60L + sec)) -
                   my_time_zone;

  return (int64_t)tmp_t;
}

string Func_unix_timestamp::getStrVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& ct)
{
  return dataconvert::DataConvert::datetimeToString(getIntVal(row, parm, isNull, ct));
}

}  // namespace funcexp

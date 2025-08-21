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
 * $Id: func_date_format.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_str.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

namespace funcexp
{
namespace helpers
{
const string IDB_date_format(const DateTime& dt, const string& format, bool& isNull)
{
  // assume 256 is enough. assume not allowing incomplete date
  // XXX: imagine %W gets repeated 60 times and day of week is "wednesday"..
  std::ostringstream oss;
  char buf[256];
  uint32_t weekday = 0;
  uint32_t dayval = 0;
  uint32_t weekval = 0;
  uint32_t weekyear = 0;

  for (uint32_t i = 0; !isNull && i < format.length(); i++)
  {
    if (format[i] != '%')
      oss << format[i];
    else
    {
      i++;

      switch (format[i])
      {
        case 'M':
          oss << helpers::monthFullNames[dt.month];
          break;

        case 'b':
          oss << helpers::monthAbNames[dt.month].c_str();
          break;

        case 'W':
          weekday = helpers::calc_mysql_weekday(dt.year, dt.month, dt.day, false, isNull);
          oss << helpers::weekdayFullNames[weekday];
          break;

        case 'w':
          weekday = helpers::calc_mysql_weekday(dt.year, dt.month, dt.day, true, isNull);
          sprintf(buf, "%01d", weekday);
          oss << buf;
          break;

        case 'a':
          weekday = helpers::calc_mysql_weekday(dt.year, dt.month, dt.day, false, isNull);
          oss << helpers::weekdayAbNames[weekday];
          break;

        case 'D':
          oss << helpers::dayOfMonth[dt.day].c_str();
          break;

        case 'Y':
          sprintf(buf, "%04d", dt.year);
          oss << buf;
          break;

        case 'y':
          sprintf(buf, "%02d", dt.year % 100);
          oss << buf;
          break;

        case 'm':
          sprintf(buf, "%02d", dt.month);
          oss << buf;
          break;

        case 'c':
          sprintf(buf, "%d", dt.month);
          oss << buf;
          break;

        case 'd':
          sprintf(buf, "%02d", dt.day);
          oss << buf;
          break;

        case 'e':
          sprintf(buf, "%d", dt.day);
          oss << buf;
          break;

        case 'f':
          sprintf(buf, "%06d", dt.msecond);
          oss << buf;
          break;

        case 'H':
          sprintf(buf, "%02d", dt.hour);
          oss << buf;
          break;

        case 'h':
        case 'I':
          sprintf(buf, "%02d", (dt.hour % 24 + 11) % 12 + 1);
          oss << buf;
          break;

        case 'i': /* minutes */
          sprintf(buf, "%02d", dt.minute);
          oss << buf;
          break;

        case 'j':
          dayval = helpers::calc_mysql_daynr(dt.year, dt.month, dt.day) -
                   helpers::calc_mysql_daynr(dt.year, 1, 1) + 1;
          sprintf(buf, "%03d", dayval);
          oss << buf;
          break;

        case 'k':
          sprintf(buf, "%d", dt.hour);
          oss << buf;
          break;

        case 'l':
          sprintf(buf, "%d", (dt.hour % 24 + 11) % 12 + 1);
          oss << buf;
          break;

        case 'p':
          sprintf(buf, "%s", (dt.hour % 24 < 12 ? "AM" : "PM"));
          oss << buf;
          break;

        case 'r':
          sprintf(buf, (dt.hour % 24 < 12 ? "%02d:%02d:%02d AM" : "%02d:%02d:%02d PM"),
                  (dt.hour + 11) % 12 + 1, dt.minute, dt.second);
          oss << buf;
          break;

        case 'S':
        case 's':
          sprintf(buf, "%02d", dt.second);
          oss << buf;
          break;

        case 'T':
          sprintf(buf, "%02d:%02d:%02d", dt.hour, dt.minute, dt.second);
          oss << buf;
          break;

        case 'U':
          weekval = helpers::calc_mysql_week(dt.year, dt.month, dt.day, 0);
          sprintf(buf, "%02d", weekval);
          oss << buf;
          break;

        case 'V':
          weekval = helpers::calc_mysql_week(dt.year, dt.month, dt.day, helpers::WEEK_NO_ZERO);
          sprintf(buf, "%02d", weekval);
          oss << buf;
          break;

        case 'u':
          weekval = helpers::calc_mysql_week(dt.year, dt.month, dt.day,
                                             helpers::WEEK_MONDAY_FIRST | helpers::WEEK_GT_THREE_DAYS);
          sprintf(buf, "%02d", weekval);
          oss << buf;
          break;

        case 'v':
          weekval = helpers::calc_mysql_week(
              dt.year, dt.month, dt.day,
              helpers::WEEK_NO_ZERO | helpers::WEEK_MONDAY_FIRST | helpers::WEEK_GT_THREE_DAYS);
          sprintf(buf, "%02d", weekval);
          oss << buf;
          break;

        case 'x':
          helpers::calc_mysql_week(
              dt.year, dt.month, dt.day,
              helpers::WEEK_NO_ZERO | helpers::WEEK_MONDAY_FIRST | helpers::WEEK_GT_THREE_DAYS, &weekyear);
          sprintf(buf, "%04d", weekyear);
          oss << buf;
          break;

        case 'X':
          helpers::calc_mysql_week(dt.year, dt.month, dt.day, helpers::WEEK_NO_ZERO, &weekyear);
          sprintf(buf, "%04d", weekyear);
          oss << buf;
          break;

        default: oss << format[i];
      }
    }
  }

  return oss.str();
}
}  // namespace helpers

CalpontSystemCatalog::ColType Func_date_format::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  CalpontSystemCatalog::ColType ct;
  ct.colDataType = CalpontSystemCatalog::VARCHAR;
  ct.colWidth = 255;
  return ct;
}

string Func_date_format::getStrVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& ct)
{
  int64_t val = 0;
  DateTime dt = 0;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case CalpontSystemCatalog::DATE:
      val = parm[0]->data()->getIntVal(row, isNull);
      dt.year = (uint32_t)((val >> 16) & 0xffff);
      dt.month = (uint32_t)((val >> 12) & 0xf);
      dt.day = (uint32_t)((val >> 6) & 0x3f);
      break;

    case CalpontSystemCatalog::DATETIME:
      val = parm[0]->data()->getDatetimeIntVal(row, isNull);
      dt.year = (uint32_t)((val >> 48) & 0xffff);
      dt.month = (uint32_t)((val >> 44) & 0xf);
      dt.day = (uint32_t)((val >> 38) & 0x3f);
      dt.hour = (uint32_t)((val >> 32) & 0x3f);
      dt.minute = (uint32_t)((val >> 26) & 0x3f);
      dt.second = (uint32_t)((val >> 20) & 0x3f);
      dt.msecond = (uint32_t)((val & 0xfffff));
      break;

    case CalpontSystemCatalog::TIMESTAMP:
    {
      val = parm[0]->data()->getTimestampIntVal(row, isNull);
      TimeStamp timestamp(val);
      int64_t seconds = timestamp.second;
      MySQLTime time;
      gmtSecToMySQLTime(seconds, time, ct.getTimeZone());
      dt.year = time.year;
      dt.month = time.month;
      dt.day = time.day;
      dt.hour = time.hour;
      dt.minute = time.minute;
      dt.second = time.second;
      dt.msecond = timestamp.msecond;
      break;
    }

    case CalpontSystemCatalog::TIME:
    {
      DateTime aDateTime = static_cast<DateTime>(nowDatetime());
      Time aTime = parm[0]->data()->getTimeIntVal(row, isNull);
      aTime.day = 0;
      aDateTime.hour = 0;
      aDateTime.minute = 0;
      aDateTime.second = 0;
      aDateTime.msecond = 0;
      if ((aTime.hour < 0) || (aTime.is_neg))
      {
        aTime.hour = -abs(aTime.hour);
        aTime.minute = -abs(aTime.minute);
        aTime.second = -abs(aTime.second);
        aTime.msecond = -abs(aTime.msecond);
      }
      val = addTime(aDateTime, aTime);
      dt.year = (uint32_t)((val >> 48) & 0xffff);
      dt.month = (uint32_t)((val >> 44) & 0xf);
      dt.day = (uint32_t)((val >> 38) & 0x3f);
      dt.hour = (uint32_t)((val >> 32) & 0x3f);
      dt.minute = (uint32_t)((val >> 26) & 0x3f);
      dt.second = (uint32_t)((val >> 20) & 0x3f);
      dt.msecond = (uint32_t)((val & 0xfffff));
      break;
    }

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::TEXT:
      val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull).safeString());

      if (val == -1)
      {
        isNull = true;
        return "";
      }
      else
      {
        dt.year = (uint32_t)((val >> 48) & 0xffff);
        dt.month = (uint32_t)((val >> 44) & 0xf);
        dt.day = (uint32_t)((val >> 38) & 0x3f);
        dt.hour = (uint32_t)((val >> 32) & 0x3f);
        dt.minute = (uint32_t)((val >> 26) & 0x3f);
        dt.second = (uint32_t)((val >> 20) & 0x3f);
        dt.msecond = (uint32_t)((val & 0xfffff));
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
        return "";
      }
      else
      {
        dt.year = (uint32_t)((val >> 48) & 0xffff);
        dt.month = (uint32_t)((val >> 44) & 0xf);
        dt.day = (uint32_t)((val >> 38) & 0x3f);
        dt.hour = (uint32_t)((val >> 32) & 0x3f);
        dt.minute = (uint32_t)((val >> 26) & 0x3f);
        dt.second = (uint32_t)((val >> 20) & 0x3f);
        dt.msecond = (uint32_t)((val & 0xfffff));
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
          return "";
        }
        else
        {
          dt.year = (uint32_t)((val >> 48) & 0xffff);
          dt.month = (uint32_t)((val >> 44) & 0xf);
          dt.day = (uint32_t)((val >> 38) & 0x3f);
          dt.hour = (uint32_t)((val >> 32) & 0x3f);
          dt.minute = (uint32_t)((val >> 26) & 0x3f);
          dt.second = (uint32_t)((val >> 20) & 0x3f);
          dt.msecond = (uint32_t)((val & 0xfffff));
        }
      }
      else
      {
        isNull = true;
        return "";
      }

      break;

    default: isNull = true; return "";
  }

  const string& format = parm[1]->data()->getStrVal(row, isNull).safeString("");

  return helpers::IDB_date_format(dt, format, isNull);
}

int32_t Func_date_format::getDateIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                        CalpontSystemCatalog::ColType& ct)
{
  return dataconvert::DataConvert::dateToInt(getStrVal(row, parm, isNull, ct));
}

int64_t Func_date_format::getDatetimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                            CalpontSystemCatalog::ColType& ct)
{
  return dataconvert::DataConvert::datetimeToInt(getStrVal(row, parm, isNull, ct));
}

int64_t Func_date_format::getTimestampIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                             CalpontSystemCatalog::ColType& ct)
{
  return dataconvert::DataConvert::timestampToInt(getStrVal(row, parm, isNull, ct), ct.getTimeZone());
}

}  // namespace funcexp

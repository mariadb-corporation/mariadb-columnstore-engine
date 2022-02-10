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
 * $Id: functor.cpp 3898 2013-06-17 20:41:05Z rdempsey $
 *
 *
 ****************************************************************************/
#ifndef _MSC_VER
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#endif
#include <inttypes.h>
#include <string>
#include <sstream>
#include <boost/date_time/posix_time/posix_time.hpp>

using namespace std;

#include "joblisttypes.h"

#include "dataconvert.h"
using namespace dataconvert;

#include "idberrorinfo.h"
#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "functor.h"
#include "funchelpers.h"

using namespace funcexp;

namespace funcexp
{
void Func::init()
{
  uint32_t fni = joblist::FLOATNULL;
  float* fp = reinterpret_cast<float*>(&fni);
  fFloatNullVal = *fp;

  uint64_t dni = joblist::DOUBLENULL;
  double* dp = reinterpret_cast<double*>(&dni);
  fDoubleNullVal = *dp;

  fLongDoubleNullVal = joblist::LONGDOUBLENULL;
}

Func::Func()
{
  init();
}

Func::Func(const string& funcName) : fFuncName(funcName)
{
  init();
}

uint32_t Func::stringToDate(const string str)
{
  int64_t ret = DataConvert::stringToDate(str);

  if (ret == -1)
  {
    Message::Args args;
    args.add("date");
    args.add(str);
    unsigned errcode = ERR_INCORRECT_VALUE;
    throw IDBExcept(IDBErrorInfo::instance()->errorMsg(errcode, args), errcode);
  }

  return ret;
}

uint64_t Func::stringToDatetime(const string str)
{
  int64_t ret = DataConvert::stringToDatetime(str);

  if (ret == -1)
  {
    Message::Args args;
    args.add("datetime");
    args.add(str);
    unsigned errcode = ERR_INCORRECT_VALUE;
    throw IDBExcept(IDBErrorInfo::instance()->errorMsg(errcode, args), errcode);
  }

  return ret;
}

uint64_t Func::stringToTimestamp(const std::string& str, long timeZone)
{
  int64_t ret = DataConvert::stringToTimestamp(str, timeZone);

  if (ret == -1)
  {
    Message::Args args;
    args.add("timestamp");
    args.add(str);
    unsigned errcode = ERR_INCORRECT_VALUE;
    throw IDBExcept(IDBErrorInfo::instance()->errorMsg(errcode, args), errcode);
  }

  return ret;
}

int64_t Func::stringToTime(const string str)
{
  int64_t ret = DataConvert::stringToTime(str);

  if (ret == -1)
  {
    Message::Args args;
    args.add("time");
    args.add(str);
    unsigned errcode = ERR_INCORRECT_VALUE;
    throw IDBExcept(IDBErrorInfo::instance()->errorMsg(errcode, args), errcode);
  }

  return ret;
}

uint32_t Func::intToDate(int64_t i)
{
  if ((uint64_t)i > 0xFFFFFFFFL)
    return ((((uint32_t)(i >> 32)) & 0xFFFFFFC0L) | 0x3E);

  return i;
}

uint64_t Func::intToDatetime(int64_t i)
{
  if ((uint64_t)i < 0xFFFFFFFFL)
    return (i << 32);

  return i;
}

uint64_t Func::intToTimestamp(int64_t i)
{
  return i;
}

int64_t Func::intToTime(int64_t i)
{
  // Don't think we need to do anything here?
  return i;
}

int64_t Func::nowDatetime()
{
  DateTime result;
  boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
  result.year = now.date().year();
  result.month = now.date().month();
  result.day = now.date().day();
  result.hour = now.time_of_day().hours();
  result.minute = now.time_of_day().minutes();
  result.second = now.time_of_day().seconds();
  result.msecond = now.time_of_day().total_microseconds();

  return (int64_t) * (reinterpret_cast<int64_t*>(&result));
}

int64_t Func::addTime(DateTime& dt1, Time& dt2)
{
  DateTime dt;
  dt.year = 0;
  dt.month = 0;
  dt.day = 0;
  dt.hour = 0;
  dt.minute = 0;
  dt.second = 0;
  dt.msecond = 0;

  int64_t month, day, hour, min, sec, msec, tmp;
  msec = (signed)(dt1.msecond + dt2.msecond);
  dt.msecond = tmp = msec % 1000000;

  if (tmp < 0)
  {
    dt.msecond = tmp + 1000000;
    dt2.second--;
  }

  sec = (signed)(dt1.second + dt2.second + msec / 1000000);
  dt.second = tmp = sec % 60;

  if (tmp < 0)
  {
    dt.second = tmp + 60;
    dt2.minute--;
  }

  min = (signed)(dt1.minute + dt2.minute + sec / 60);
  dt.minute = tmp = min % 60;

  if (tmp < 0)
  {
    dt.minute = tmp + 60;
    dt2.hour--;
  }

  hour = (signed)(dt1.hour + dt2.hour + min / 60);

  if ((hour < 0) || (hour > 23))
  {
    dt2.day = hour / 24;
    hour = hour % 24;
  }

  if (hour < 0)
  {
    dt.hour = hour + 24;
    dt2.day--;
  }
  else
  {
    dt.hour = hour;
  }

  day = (signed)(dt1.day + dt2.day);

  if (isLeapYear(dt1.year) && dt1.month == 2)
    day--;

  month = dt1.month;
  int addyear = 0;

  if (day <= 0)
  {
    int monthSave = month;

    while (day <= 0)
    {
      month = (month == 1 ? 12 : month - 1);

      for (; day <= 0 && month > 0; month--)
        day += getDaysInMonth(month, dt1.year);

      month++;
      //			month=12;
    }

    if (month > monthSave)
      addyear--;
  }
  else
  {
    int monthSave = month;

    while (day > getDaysInMonth(month, dt1.year))
    {
      for (; day > getDaysInMonth(month, dt1.year) && month <= 12; month++)
        day -= getDaysInMonth(month, dt1.year);

      if (month > 12)
        month = 1;
    }

    if (month < monthSave)
      addyear++;
  }

  dt.day = day;
  dt.month = month;
  dt.year = dt1.year + addyear;

  return *(reinterpret_cast<int64_t*>(&dt));
}

int64_t Func::addTime(Time& dt1, Time& dt2)
{
  Time dt;
  dt.is_neg = false;
  dt.hour = 0;
  dt.minute = 0;
  dt.second = 0;
  dt.msecond = 0;

  int64_t min, sec, msec, tmp;
  msec = (signed)(dt1.msecond + dt2.msecond);
  dt.msecond = tmp = msec % 1000000;

  if (tmp < 0)
  {
    dt.msecond = tmp + 1000000;
    dt2.second--;
  }

  sec = (signed)(dt1.second + dt2.second + msec / 1000000);
  dt.second = tmp = sec % 60;

  if (tmp < 0)
  {
    dt.second = tmp + 60;
    dt2.minute--;
  }

  min = (signed)(dt1.minute + dt2.minute + sec / 60);
  dt.minute = tmp = min % 60;

  if (tmp < 0)
  {
    dt.minute = tmp + 60;
    dt2.hour--;
  }

  dt.hour = tmp = (signed)(dt1.hour + dt2.hour + min / 60);

  // Saturation
  if (tmp > 838)
  {
    dt.hour = 838;
    dt.minute = 59;
    dt.second = 59;
    dt.msecond = 999999;
  }
  else if (tmp < -838)
  {
    dt.is_neg = true;
    dt.hour = -838;
    dt.minute = 59;
    dt.second = 59;
    dt.msecond = 999999;
  }

  return *(reinterpret_cast<int64_t*>(&dt));
}

string Func::intToString(int64_t i)
{
  return helpers::intToString(i);
}

string Func::doubleToString(double d)
{
  return helpers::doubleToString(d);
}

string Func::longDoubleToString(long double ld)
{
  return helpers::longDoubleToString(ld);
}

}  // namespace funcexp
// vim:ts=4 sw=4:

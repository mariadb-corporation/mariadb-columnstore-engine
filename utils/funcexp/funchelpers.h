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

//  $Id: funchelpers.h 3921 2013-06-19 18:59:56Z bwilkinson $

/** @file */

#ifndef FUNCHELPERS_H__
#define FUNCHELPERS_H__

#include <string>

#ifndef _MSC_VER
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#endif

#include <inttypes.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/regex.hpp>
#include <boost/tokenizer.hpp>

#include "dataconvert.h"
#include "operator.h"
#include "intervalcolumn.h"
#include "treenode.h"

#ifndef ULONGLONG_MAX
#define ULONGLONG_MAX ulonglong_max
#endif
namespace funcexp
{
namespace helpers
{
// 10 ** i
const int64_t powerOf10_c[] = {1ll,
                               10ll,
                               100ll,
                               1000ll,
                               10000ll,
                               100000ll,
                               1000000ll,
                               10000000ll,
                               100000000ll,
                               1000000000ll,
                               10000000000ll,
                               100000000000ll,
                               1000000000000ll,
                               10000000000000ll,
                               100000000000000ll,
                               1000000000000000ll,
                               10000000000000000ll,
                               100000000000000000ll,
                               1000000000000000000ll};

// max integer number of i digits
const int64_t maxNumber_c[] = {0ll,
                               9ll,
                               99ll,
                               999ll,
                               9999ll,
                               99999ll,
                               999999ll,
                               9999999ll,
                               99999999ll,
                               999999999ll,
                               9999999999ll,
                               99999999999ll,
                               999999999999ll,
                               9999999999999ll,
                               99999999999999ll,
                               999999999999999ll,
                               9999999999999999ll,
                               99999999999999999ll,
                               999999999999999999ll};

const uint32_t TIMESTAMP_MAX_YEAR = 2038;
const uint32_t TIMESTAMP_MIN_YEAR = (1970 - 1);
const int TIMESTAMP_MIN_VALUE = 1;
const int64_t TIMESTAMP_MAX_VALUE = 0x7FFFFFFFL;
const unsigned long long MAX_NEGATIVE_NUMBER = 0x8000000000000000ULL;
const long long LONGLONG_MIN = 0x8000000000000000LL;
const int INIT_CNT = 9;
const unsigned long LFACTOR = 1000000000;
const unsigned long long LFACTOR1 = 10000000000ULL;
const unsigned long long LFACTOR2 = 100000000000ULL;
const unsigned long long ulonglong_max = ~(unsigned long long)0;

static std::string monthFullNames[13] = {"NON_VALID", "January",  "February", "March",  "April",
                                         "May",       "June",     "July",     "August", "September",
                                         "October",   "November", "December"};

static std::string monthAbNames[13] = {"NON_VALID", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                       "Jul",       "Aug", "Sep", "Oct", "Nov", "Dec"};

static std::string weekdayFullNames[8] = {"Monday", "Tuesday",  "Wednesday", "Thursday",
                                          "Friday", "Saturday", "Sunday"};

static std::string weekdayAbNames[8] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"};

static std::string dayOfMonth[32] = {"0th",  "1st",  "2nd",  "3rd",  "4th",  "5th",  "6th",  "7th",
                                     "8th",  "9th",  "10th", "11th", "12th", "13th", "14th", "15th",
                                     "16th", "17th", "18th", "19th", "20th", "21st", "22nd", "23rd",
                                     "24th", "25th", "26th", "27th", "28th", "29th", "30th", "31st"};

static uint8_t days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0};

// Given a date, calculate the number of days since year 0
// This is a mirror of calc_daynr, at a later date we should use my_time.h
inline uint32_t calc_mysql_daynr(uint32_t year, uint32_t month, uint32_t day)
{
  int temp;
  int y = year;
  long delsum;

  if (!dataconvert::isDateValid(day, month, year))
    return 0;

  delsum = (long)(365 * y + 31 * ((int)month - 1) + (int)day);

  if (month <= 2)
    y--;
  else
    delsum -= (long)((int)month * 4 + 23) / 10;

  temp = (int)((y / 100 + 1) * 3) / 4;

  return delsum + (int)y / 4 - temp;
}

// used by get_date_from_mysql_daynr() and calc_mysql_week()
inline uint32_t calc_mysql_days_in_year(uint32_t year)
{
  return ((year & 3) == 0 && (year % 100 || (year % 400 == 0 && year)) ? 366 : 365);
}

// convert from a MySQL day number (offset from year 0) to a date
// This is a mirror of get_date_from_daynr, at a later date we should use sql_time.h
inline void get_date_from_mysql_daynr(long daynr, dataconvert::DateTime& dateTime)
{
  uint32_t year, temp, leap_day, day_of_year, days_in_year;
  uint8_t* month_pos;
  uint32_t ret_year, ret_month, ret_day;
  const int MAX_DAY_NUMBER = 3652424;

  if (daynr < 366 || daynr > MAX_DAY_NUMBER)
  {
    dateTime.year = dateTime.month = dateTime.day = 0;
    return;
  }

  year = (uint32_t)(daynr * 100 / 36525L);
  temp = (((year - 1) / 100 + 1) * 3) / 4;
  day_of_year = (uint)(daynr - (long)year * 365L) - (year - 1) / 4 + temp;

  while (day_of_year > (days_in_year = calc_mysql_days_in_year(year)))
  {
    day_of_year -= days_in_year;
    (year)++;
  }

  leap_day = 0;

  if (days_in_year == 366)
  {
    if (day_of_year > 31 + 28)
    {
      day_of_year--;

      if (day_of_year == 31 + 28)
        leap_day = 1; /* Handle leapyears leapday */
    }
  }

  ret_month = 1;

  for (month_pos = days_in_month; day_of_year > (uint32_t)*month_pos;
       day_of_year -= *(month_pos++), (ret_month)++)
    ;

  ret_year = year;
  ret_day = day_of_year + leap_day;

  dateTime.year = ret_year;
  dateTime.month = ret_month;
  dateTime.day = ret_day;
}

// Returns the weekday index for a given date:
//   if sundayFirst:
//       0 = Sunday, 1 = Monday, ..., 6 = Saturday
//   else:
//       0 = Monday, 1 = Tuesday, ..., 6 = Sunday
// This is a mirror of calc_weekday, at a later date we should use sql_time.h
inline uint32_t calc_mysql_weekday(uint32_t year, uint32_t month, uint32_t day, bool sundayFirst)
{
  if (!dataconvert::isDateValid(day, month, year))
    return 0;

  uint32_t daynr = calc_mysql_daynr(year, month, day);
  return ((int)((daynr + 5L + (sundayFirst ? 1L : 0L)) % 7));
}

// Flags for calc_mysql_week
const uint32_t WEEK_MONDAY_FIRST = 1;
const uint32_t WEEK_NO_ZERO = 2;
const uint32_t WEEK_GT_THREE_DAYS = 4;

// Takes a MySQL WEEK() function mode setting and converts to a bitmask
// used by calc_mysql_week() mirror of MariaDB's week_mode()
inline int16_t convert_mysql_mode_to_modeflags(int16_t mode)
{
  if (!(mode & WEEK_MONDAY_FIRST))
    mode ^= WEEK_GT_THREE_DAYS;

  return mode;
}

// Returns a week index conforming to the MySQL WEEK() function.  Note
// that modeflags is not the MySQL mode - it is a bitmask of the abvoe
// 3 flags.  The utility function convert_mysql_mode_to_modeflags should
// be applied to the MySQL mode before calling this function (or the
// flags may be used directly).  The optional argument is for callers
// that need to know the year that the week actually corresponds to -
// see MySQL documentation for how the year returned can be different
// than the year of the input date
//
// This is a mirror of calc_week, at a later date we should use sql_time.h
inline uint32_t calc_mysql_week(uint32_t year, uint32_t month, uint32_t day, int16_t modeflags,
                                uint32_t* weekyear = 0)
{
  // need to make sure that the date is valid
  if (!dataconvert::isDateValid(day, month, year))
    return 0;

  uint32_t days;
  uint32_t daynr = calc_mysql_daynr(year, month, day);
  uint32_t first_daynr = calc_mysql_daynr(year, 1, 1);
  bool monday_first = modeflags & WEEK_MONDAY_FIRST;
  bool week_year = modeflags & WEEK_NO_ZERO;
  bool first_weekday = modeflags & WEEK_GT_THREE_DAYS;

  uint32_t weekday = calc_mysql_weekday(year, 1, 1, !monday_first);

  if (weekyear)
  {
    *weekyear = year;
  }

  if (month == 1 && day <= 7 - weekday)
  {
    if (!week_year && ((first_weekday && weekday != 0) || (!first_weekday && weekday >= 4)))
      return 0;

    week_year = 1;

    if (weekyear)
    {
      (*weekyear)--;
    }

    year--;
    first_daynr -= (days = calc_mysql_days_in_year(year));
    weekday = (weekday + 53 * 7 - days) % 7;
  }

  if ((first_weekday && weekday != 0) || (!first_weekday && weekday >= 4))
    days = daynr - (first_daynr + (7 - weekday));
  else
    days = daynr - (first_daynr - weekday);

  if (week_year && days >= 52 * 7)
  {
    weekday = (weekday + calc_mysql_days_in_year(year)) % 7;

    if ((!first_weekday && weekday < 4) || (first_weekday && weekday == 0))
    {
      if (weekyear)
      {
        (*weekyear)++;
      }

      return 1;
    }
  }

  return days / 7 + 1;
}

inline bool calc_time_diff(int64_t time1, int64_t time2, int l_sign, long long* seconds_out,
                           long long* microseconds_out, bool isDateTime = true)
{
  int64_t days;
  bool neg;
  int64_t microseconds;

  uint64_t year1 = 0, month1 = 0, day1 = 0, hour1 = 0, min1 = 0, sec1 = 0, msec1 = 0;

  uint64_t year2 = 0, month2 = 0, day2 = 0, hour2 = 0, min2 = 0, sec2 = 0, msec2 = 0;

  if (isDateTime)
  {
    year1 = (uint32_t)((time1 >> 48) & 0xffff);
    month1 = (uint32_t)((time1 >> 44) & 0xf);
    day1 = (uint32_t)((time1 >> 38) & 0x3f);
    hour1 = (uint32_t)((time1 >> 32) & 0x3f);
    min1 = (uint32_t)((time1 >> 26) & 0x3f);
    sec1 = (uint32_t)((time1 >> 20) & 0x3f);
    msec1 = (uint32_t)((time1 & 0xfffff));

    year2 = (uint32_t)((time2 >> 48) & 0xffff);
    month2 = (uint32_t)((time2 >> 44) & 0xf);
    day2 = (uint32_t)((time2 >> 38) & 0x3f);
    hour2 = (uint32_t)((time2 >> 32) & 0x3f);
    min2 = (uint32_t)((time2 >> 26) & 0x3f);
    sec2 = (uint32_t)((time2 >> 20) & 0x3f);
    msec2 = (uint32_t)(time2 & 0xfffff);
  }
  else
  {
    year1 = 0;
    month1 = 0;
    day1 = (time1 >> 52) & 0x7ff;
    hour1 = (time1 >> 40) & 0xfff;
    min1 = (time1 >> 32) & 0xff;
    sec1 = (time1 >> 24) & 0xff;
    msec1 = time1 & 0xffffff;

    year2 = 0;
    month2 = 0;
    day2 = (time2 >> 52) & 0x7ff;
    hour2 = (time2 >> 40) & 0xfff;
    min2 = (time2 >> 32) & 0xff;
    sec2 = (time2 >> 24) & 0xff;
    msec2 = time2 & 0xffffff;
  }

  days = calc_mysql_daynr(year1, month1, day1);

  days -= l_sign * calc_mysql_daynr(year2, month2, day2);

  microseconds = ((long long)days * (long)(86400) + (long long)(hour1 * 3600L + min1 * 60L + sec1) -
                  l_sign * (long long)(hour2 * 3600L + min2 * 60L + sec2)) *
                     (long long)(1000000) +
                 (long long)msec1 - l_sign * (long long)msec2;

  neg = 0;

  if (microseconds < 0)
  {
    microseconds = -microseconds;
    neg = 1;
  }

  *seconds_out = microseconds / 1000000L;
  *microseconds_out = (long long)(microseconds % 1000000L);
  return neg;
}

inline int power(int16_t a)
{
  int b = 1;

  for (int i = 0; i < a; i++)
  {
    b = b * 10;
  }

  return b;
}

template <typename T>
inline void decimalPlaceDec(int64_t& d, T& p, int8_t& s)
{
  // find new scale if D < s
  if (d < s)
  {
    int64_t t = s;
    s = d;  // the new scale
    d -= t;
    int64_t i = (d >= 0) ? d : (-d);

    while (i--)
      p *= 10;
  }
  else
  {
    d = s;
  }
}

inline uint32_t convertMonth(std::string month)
{
  uint32_t value = 0;

  boost::algorithm::to_lower(month);

  if (month == "jan" || month == "january")
  {
    value = 1;
  }
  else if (month == "feb" || month == "february")
  {
    value = 2;
  }
  else if (month == "mar" || month == "march")
  {
    value = 3;
  }
  else if (month == "apr" || month == "april")
  {
    value = 4;
  }
  else if (month == "may")
  {
    value = 5;
  }
  else if (month == "jun" || month == "june")
  {
    value = 6;
  }
  else if (month == "jul" || month == "july")
  {
    value = 7;
  }
  else if (month == "aug" || month == "august")
  {
    value = 8;
  }
  else if (month == "sep" || month == "september")
  {
    value = 9;
  }
  else if (month == "oct" || month == "october")
  {
    value = 10;
  }
  else if (month == "nov" || month == "november")
  {
    value = 11;
  }
  else if (month == "dec" || month == "december")
  {
    value = 12;
  }
  else
  {
    value = 0;
  }

  return value;
}

class to_lower
{
 public:
  char operator()(char c) const  // notice the return type
  {
    return tolower(c);
  }
};

inline int getNumbers(const std::string& expr, int64_t* array, execplan::OpType funcType)
{
  int index = 0;

  int funcNeg = 1;

  if (funcType == execplan::OP_SUB)
    funcNeg = -1;

  if (expr.size() == 0)
    return 0;

  // @bug 4703 reworked this code to avoid use of incrementally
  //           built string to hold temporary values while
  //           scanning expr for numbers.  This function is now
  //           covered by a unit test in tdriver.cpp
  bool foundNumber = false;
  int64_t number = 0;
  int neg = 1;

  for (unsigned int i = 0; i < expr.size(); i++)
  {
    char value = expr[i];

    if ((value >= '0' && value <= '9'))
    {
      foundNumber = true;
      number = (number * 10) + (value - '0');
    }
    else if (value == '-' && !foundNumber)
    {
      neg = -1;
    }
    else if (value == '-')
    {
      // this is actually an error condition - it means that
      // input came in with something like NN-NN (i.e. a dash
      // between two numbers.  To match prior code we will
      // return the number up to the dash and just return
      array[index] = number * funcNeg * neg;
      index++;

      return index;
    }
    else
    {
      if (foundNumber)
      {
        array[index] = number * funcNeg * neg;
        number = 0;
        neg = 1;
        index++;

        if (index > 9)
          return index;
      }
    }
  }

  if (foundNumber)
  {
    array[index] = number * funcNeg * neg;
    index++;
  }

  return index;
}

inline int getNumbers(const std::string& expr, int* array, execplan::OpType funcType)
{
  int index = 0;

  int funcNeg = 1;

  if (funcType == execplan::OP_SUB)
    funcNeg = -1;

  if (expr.size() == 0)
    return 0;

  // @bug 4703 reworked this code to avoid use of incrementally
  //           built string to hold temporary values while
  //           scanning expr for numbers.  This function is now
  //           covered by a unit test in tdriver.cpp
  bool foundNumber = false;
  int number = 0;
  int neg = 1;

  for (unsigned int i = 0; i < expr.size(); i++)
  {
    char value = expr[i];

    if ((value >= '0' && value <= '9'))
    {
      foundNumber = true;
      number = (number * 10) + (value - '0');
    }
    else if (value == '-' && !foundNumber)
    {
      neg = -1;
    }
    else if (value == '-')
    {
      // this is actually an error condition - it means that
      // input came in with something like NN-NN (i.e. a dash
      // between two numbers.  To match prior code we will
      // return the number up to the dash and just return
      array[index] = number * funcNeg * neg;
      index++;

      return index;
    }
    else
    {
      if (foundNumber)
      {
        array[index] = number * funcNeg * neg;
        number = 0;
        neg = 1;
        index++;

        if (index > 9)
          return index;
      }
    }
  }

  if (foundNumber)
  {
    array[index] = number * funcNeg * neg;
    index++;
  }

  return index;
}

inline int dayOfWeek(std::string day)  // Sunday = 0
{
  int value = -1;
  boost::to_lower(day);

  if (day == "sunday" || day == "sun")
  {
    value = 0;
  }
  else if (day == "monday" || day == "mon")
  {
    value = 1;
  }
  else if (day == "tuesday" || day == "tue")
  {
    value = 2;
  }
  else if (day == "wednesday" || day == "wed")
  {
    value = 3;
  }
  else if (day == "thursday" || day == "thu")
  {
    value = 4;
  }
  else if (day == "friday" || day == "fri")
  {
    value = 5;
  }
  else if (day == "saturday" || day == "sat")
  {
    value = 6;
  }
  else
  {
    value = -1;
  }

  return value;
}

inline string intToString(int64_t i)
{
  char buf[32];
#ifndef _MSC_VER
  snprintf(buf, 32, "%" PRId64 "", i);
#else
  snprintf(buf, 32, "%lld", i);
#endif
  return buf;
}

inline string uintToString(uint64_t i)
{
  char buf[32];
#ifndef _MSC_VER
  snprintf(buf, 32, "%" PRIu64 "", i);
#else
  snprintf(buf, 32, "%llu", i);
#endif
  return buf;
}

inline string doubleToString(double d)
{
  // double's can be *really* long to print out.  Max mysql
  // is e308 so allow for 308 + 36 decimal places minimum.
  char buf[384];
  snprintf(buf, 384, "%f", d);
  return buf;
}

inline string longDoubleToString(long double ld)
{
  // long double's can be *really* long to print out.  Max mysql
  // is e308 so allow for 308 + 36 decimal places minimum.
  char buf[384];
  snprintf(buf, 384, "%Lf", ld);
  return buf;
}

uint64_t dateAdd(uint64_t time, const std::string& expr, execplan::IntervalColumn::interval_type unit,
                 bool dateType, execplan::OpType funcType);
const std::string IDB_date_format(const dataconvert::DateTime&, const std::string&);
const std::string timediff(int64_t, int64_t, bool isDateTime = true);
const char* convNumToStr(int64_t, char*, int);

}  // namespace helpers
}  // namespace funcexp

#endif

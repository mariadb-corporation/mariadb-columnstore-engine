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
* $Id: dataconvert.h 3693 2013-04-05 16:11:30Z chao $
*
*
****************************************************************************/
/** @file */

#ifndef DATACONVERT_H
#define DATACONVERT_H

#include <unistd.h>
#include <string>
#include <boost/any.hpp>
#include <vector>
#ifdef _MSC_VER
#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#else
#include <netinet/in.h>
#endif

#ifdef __linux__
#define POSIX_REGEX
#endif

#ifdef POSIX_REGEX
#include <regex.h>
#else
#include <boost/regex.hpp>
#endif

#include "mcs_datatype.h"
#include "columnresult.h"
#include "exceptclasses.h"
#include "common/branchpred.h"


// remove this block if the htonll is defined in library
#ifdef __linux__
#include <endian.h>
#if __BYTE_ORDER == __BIG_ENDIAN       // 4312
inline uint64_t htonll(uint64_t n)
{
    return n;
}
#elif __BYTE_ORDER == __LITTLE_ENDIAN  // 1234
inline uint64_t htonll(uint64_t n)
{
    return ((((uint64_t) htonl(n & 0xFFFFFFFFLLU)) << 32) | (htonl((n & 0xFFFFFFFF00000000LLU) >> 32)));
}
#else  // __BYTE_ORDER == __PDP_ENDIAN    3412
inline uint64_t htonll(uint64_t n);
// don't know 34127856 or 78563412, hope never be required to support this byte order.
#endif
#else //!__linux__
#if _MSC_VER < 1600
//Assume we're on little-endian
inline uint64_t htonll(uint64_t n)
{
    return ((((uint64_t) htonl(n & 0xFFFFFFFFULL)) << 32) | (htonl((n & 0xFFFFFFFF00000000ULL) >> 32)));
}
#endif //_MSC_VER
#endif //__linux__

// this method evalutes the uint64 that stores a char[] to expected value
inline uint64_t uint64ToStr(uint64_t n)
{
    return htonll(n);
}

using cscDataType = datatypes::SystemCatalog::ColDataType;

#if defined(_MSC_VER) && defined(xxxDATACONVERT_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

const int64_t IDB_pow[19] =
{
    1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000LL,
    100000000000LL,
    1000000000000LL,
    10000000000000LL,
    100000000000000LL,
    1000000000000000LL,
    10000000000000000LL,
    100000000000000000LL,
    1000000000000000000LL
};

const std::string columnstore_big_precision[20] =
{
    "9999999999999999999",
    "99999999999999999999",
    "999999999999999999999",
    "9999999999999999999999",
    "99999999999999999999999",
    "999999999999999999999999",
    "9999999999999999999999999",
    "99999999999999999999999999",
    "999999999999999999999999999",
    "9999999999999999999999999999",
    "99999999999999999999999999999",
    "999999999999999999999999999999",
    "9999999999999999999999999999999",
    "99999999999999999999999999999999",
    "999999999999999999999999999999999",
    "9999999999999999999999999999999999",
    "99999999999999999999999999999999999",
    "999999999999999999999999999999999999",
    "9999999999999999999999999999999999999",
    "99999999999999999999999999999999999999"
};

const int32_t SECS_PER_MIN = 60;
const int32_t MINS_PER_HOUR = 60;
const int32_t HOURS_PER_DAY = 24;
const int32_t DAYS_PER_WEEK = 7;
const int32_t DAYS_PER_NYEAR = 365;
const int32_t DAYS_PER_LYEAR = 366;
const int32_t SECS_PER_HOUR = SECS_PER_MIN * MINS_PER_HOUR;
const int32_t SECS_PER_DAY = SECS_PER_HOUR * HOURS_PER_DAY;
const int32_t EPOCH_YEAR = 1970;
const int32_t MONS_PER_YEAR = 12;
const int32_t MAX_TIMESTAMP_YEAR = 2038;
const int32_t MIN_TIMESTAMP_YEAR = 1969;
const int32_t MAX_TIMESTAMP_VALUE = (1ULL << 31) - 1;
const int32_t MIN_TIMESTAMP_VALUE = 0;


namespace dataconvert
{

enum CalpontDateTimeFormat
{
    CALPONTDATE_ENUM     = 1, // date format is: "YYYY-MM-DD"
    CALPONTDATETIME_ENUM = 2, // date format is: "YYYY-MM-DD HH:MI:SS"
    CALPONTTIME_ENUM     = 3
};

/** @brief a structure that represents a timestamp in broken down
 *  representation
 */
struct MySQLTime
{
    unsigned int year, month, day, hour, minute, second;
    unsigned long second_part;
    CalpontDateTimeFormat time_type;
    void reset()
    {
        year = month = day = 0;
        hour = minute = second = second_part = 0;
        time_type = CALPONTDATETIME_ENUM;
    }
};

/**
 * This function converts the timezone represented as a string
 * in the format "+HH:MM" or "-HH:MM" to a signed offset in seconds
 * Most of this code is taken from tztime.cc:str_to_offset
 */
inline
bool timeZoneToOffset(const char *str, std::string::size_type length, long *offset)
{
    const char *end = str + length;
    bool negative;
    unsigned long number_tmp;
    long offset_tmp;

    if (length < 4)
        return 1;

    if (*str == '+')
        negative = 0;
    else if (*str == '-')
        negative = 1;
    else
        return 1;
    str++;

    number_tmp = 0;

    while (str < end && isdigit(*str))
    {
        number_tmp = number_tmp * 10 + *str - '0';
        str++;
    }

    if (str + 1 >= end || *str != ':')
        return 1;
    str++;

    offset_tmp = number_tmp * 60L;
    number_tmp = 0;

    while (str < end && isdigit(*str))
    {
        number_tmp = number_tmp * 10 + *str - '0';
        str++;
    }

    if (str != end)
        return 1;

    offset_tmp = (offset_tmp + number_tmp) * 60L;

    if (negative)
        offset_tmp = -offset_tmp;

    /*
      Check if offset is in range prescribed by standard
      (from -12:59 to 13:00).
    */

    if (number_tmp > 59 || offset_tmp < -13 * 3600L + 1 ||
        offset_tmp > 13 * 3600L)
        return 1;

    *offset = offset_tmp;
 
    return 0;
}

const int32_t year_lengths[2] =
{
  DAYS_PER_NYEAR, DAYS_PER_LYEAR
};

const unsigned int mon_lengths[2][MONS_PER_YEAR]=
{
  { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 },
  { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 }
};

const unsigned int mon_starts[2][MONS_PER_YEAR]=
{
  { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 },
  { 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335 }
};

inline int32_t leapsThruEndOf(int32_t year)
{
    return (year / 4 - year / 100 + year / 400);
}

inline bool isLeapYear ( int year)
{
    if ( year % 400 == 0 )
        return true;

    if ( ( year % 4 == 0 ) && ( year % 100 != 0 ) )
        return true;

    return false;
}

static uint32_t daysInMonth[13] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0};

inline uint32_t getDaysInMonth(uint32_t month, int year)
{
    if (month < 1 || month > 12)
        return 0;

    uint32_t days = daysInMonth[month - 1];

    if ((month == 2) && isLeapYear(year))
        days++;

    return days;
}

inline
bool isDateValid ( int day, int month, int year)
{
    bool valid = true;

    if ( day == 0 && month == 0 && year == 0 )
    {
        return true;
    }

    int daycheck = getDaysInMonth( month, year );

    if ( ( year < 1000 ) || ( year > 9999 ) )
        valid = false;
    else if ( month < 1 || month > 12 )
        valid = false;
    else if ( day < 1 || day > daycheck )
        valid = false;

    return ( valid );
}

inline
bool isDateTimeValid ( int hour, int minute, int second, int microSecond)
{
    bool valid = false;

    if ( hour >= 0 && hour <= 24 )
    {
        if ( minute >= 0 && minute < 60 )
        {
            if ( second >= 0 && second < 60 )
            {
                if ( microSecond >= 0 && microSecond <= 999999 )
                {
                    valid = true;
                }
            }
        }
    }

    return valid;
}

inline
bool isTimeValid ( int hour, int minute, int second, int microSecond)
{
    bool valid = false;

    if ( hour >= -838 && hour <= 838 )
    {
        if ( minute >= 0 && minute < 60 )
        {
            if ( second >= 0 && second < 60 )
            {
                if ( microSecond >= 0 && microSecond <= 999999 )
                {
                    valid = true;
                }
            }
        }
    }

    return valid;
}

inline
bool isTimestampValid ( uint64_t second, uint64_t microsecond )
{
    bool valid = false;

    // MariaDB server currently sets the upper limit on timestamp to
    // 0x7FFFFFFF. So enforce the same restriction here.
    // TODO: We however store the seconds portion of the timestamp in
    // 44 bits, so change this limit when the server supports higher values.
    if ( second <= MAX_TIMESTAMP_VALUE )
    {
        if ( microsecond <= 999999 )
        {
            valid = true;
        }
    }

    return valid;
}

/**
 * @brief converts a timestamp (seconds in UTC since Epoch)
 * to broken-down representation. Most of this code is taken
 * from sec_to_TIME and Time_zone_system::gmt_sec_to_TIME
 * functions in tztime.cc in the server
 *
 * @param seconds the value to be converted
 * @param time the broken-down representation of the timestamp
 * @param timeZone a string with the server timezone of the machine
 * which initiated the query
 */
inline void gmtSecToMySQLTime(int64_t seconds, MySQLTime& time,
                              const std::string& timeZone)
{
    if (seconds == 0)
    {
        time.reset();
        return;
    }

    if (timeZone == "SYSTEM")
    {
        struct tm tmp_tm;
        time_t tmp_t = (time_t)seconds;
        localtime_r(&tmp_t, &tmp_tm);
        time.second_part = 0;
        time.year = (int) ((tmp_tm.tm_year + 1900) % 10000);
        time.month = (int) tmp_tm.tm_mon + 1;
        time.day = (int) tmp_tm.tm_mday;
        time.hour = (int) tmp_tm.tm_hour;
        time.minute = (int) tmp_tm.tm_min;
        time.second = (int) tmp_tm.tm_sec;
        time.time_type = CALPONTDATETIME_ENUM;
        if (time.second == 60 || time.second == 61)
            time.second = 59;
    }
    else
    {
        long offset;
        if (timeZoneToOffset(timeZone.c_str(), timeZone.size(), &offset))
        {
            time.reset();
            return;
        }

        int64_t days;
        int32_t rem;
        int32_t y;
        int32_t yleap;
        const unsigned int *ip;

        days = (int64_t) (seconds / SECS_PER_DAY);
        rem = (int32_t) (seconds % SECS_PER_DAY);

        rem += offset;
        while (rem < 0)
        {
            rem += SECS_PER_DAY;
            days--;
        }
        while (rem >= SECS_PER_DAY)
        {
            rem -= SECS_PER_DAY;
            days++;
        }
        time.hour = (unsigned int) (rem / SECS_PER_HOUR);
        rem = rem % SECS_PER_HOUR;
        time.minute = (unsigned int) (rem / SECS_PER_MIN);
        time.second = (unsigned int) (rem % SECS_PER_MIN);

        y = EPOCH_YEAR;
        while (days < 0 || days >= (int64_t) (year_lengths[yleap = isLeapYear(y)]))
        {
            int32_t newy;

            newy = y + days / DAYS_PER_NYEAR;
            if (days < 0)
                newy--;
            days -= (newy - y) * DAYS_PER_NYEAR +
                    leapsThruEndOf(newy - 1) -
                    leapsThruEndOf(y - 1);
            y = newy;
        }
        time.year = y;

        ip = mon_lengths[yleap];
        for (time.month = 0; days >= (int64_t) ip[time.month]; time.month++)
            days -= (int64_t) ip[time.month];
        time.month++;
        time.day = (unsigned int) (days + 1);

        time.second_part = 0;
        time.time_type = CALPONTDATETIME_ENUM;
    }
}

/**
 * @brief function that provides a rough estimate if a broken-down
 * representation of timestamp is in range
 *
 * @param t the broken-down representation of timestamp
 */
inline bool validateTimestampRange(const MySQLTime& t)
{
  if ((t.year > MAX_TIMESTAMP_YEAR || t.year < MIN_TIMESTAMP_YEAR) ||
      (t.year == MAX_TIMESTAMP_YEAR && (t.month > 1 || t.day > 19)))
    return false;

  return true;
}

inline
int64_t secSinceEpoch(int year, int month, int day, int hour, int min, int sec)
{
    int64_t days = (year - EPOCH_YEAR) * DAYS_PER_NYEAR +
                   leapsThruEndOf(year - 1) -
                   leapsThruEndOf(EPOCH_YEAR - 1);
    days += mon_starts[isLeapYear(year)][month - 1];
    days += day - 1;

    return ((days * HOURS_PER_DAY + hour) * MINS_PER_HOUR + min) *
           SECS_PER_MIN + sec;
}

// This is duplicate of funchelpers.h:calc_mysql_daynr,
// with one additional function parameter
inline uint32_t calc_mysql_daynr( uint32_t year, uint32_t month, uint32_t day, bool& isValid )
{
    int temp;
    int y = year;
    long delsum;

    if ( !isDateValid( day, month, year ) )
    {
        isValid = false;
        return 0;
    }

    delsum = (long) (365 * y + 31 * ((int) month - 1) + (int) day);

    if (month <= 2)
        y--;
    else
        delsum -= (long) ((int) month * 4 + 23) / 10;

    temp = (int) ((y / 100 + 1) * 3) / 4;

    return delsum + (int) y / 4 - temp;
}

/**
 * @brief converts a timestamp from broken-down representation
 * to seconds since UTC epoch
 *
 * @param time the broken-down representation of the timestamp
   @param timeZone a string with the server timezone of the machine
   which initiated the query
 */
inline int64_t mySQLTimeToGmtSec(const MySQLTime& time,
                                 const std::string& timeZone, bool& isValid)
{
    int64_t seconds;

    if (!validateTimestampRange(time))
    {
        isValid = false;
        return 0;
    }

    if (timeZone == "SYSTEM")
    {
        // This is mirror of code in func_unix_timestamp.cpp
        uint32_t loop;
        time_t tmp_t = 0;
        int shift = 0;
        struct tm* l_time, tm_tmp;
        int64_t diff;
        localtime_r(&tmp_t, &tm_tmp);
        // Get the system timezone offset at 0 seconds since epoch
        int64_t my_time_zone = tm_tmp.tm_gmtoff;
        int day = time.day;

        if ((time.year == MAX_TIMESTAMP_YEAR) && (time.month == 1) && (day > 4))
        {
            day -= 2;
            shift = 2;
        }

        tmp_t = (time_t)(((calc_mysql_daynr(time.year, time.month, day, isValid) -
                           719528) * 86400L + (int64_t)time.hour * 3600L +
                          (int64_t)(time.minute * 60 + time.second)) - (time_t)my_time_zone);
        if (!isValid)
            return 0;

        localtime_r(&tmp_t, &tm_tmp);
        l_time = &tm_tmp;

        for (loop = 0; loop < 2 && (time.hour != (uint32_t) l_time->tm_hour ||
                                    time.minute != (uint32_t) l_time->tm_min ||
                                    time.second != (uint32_t)l_time->tm_sec); loop++)
        {
            int days = day - l_time->tm_mday;

            if (days < -1)
                days = 1; /* Month has wrapped */
            else if (days > 1)
                days = -1;

            diff = (3600L * (int64_t) (days * 24 + ((int) time.hour - (int) l_time->tm_hour)) +
                    (int64_t) (60 * ((int) time.minute - (int) l_time->tm_min)) +
                    (int64_t) ((int) time.second - (int) l_time->tm_sec));
            tmp_t += (time_t) diff;
            localtime_r(&tmp_t, &tm_tmp);
            l_time = &tm_tmp;
        }

        if (loop == 2 && time.hour != (uint32_t)l_time->tm_hour)
        {
            int days = day - l_time->tm_mday;

            if (days < -1)
                days = 1; /* Month has wrapped */
            else if (days > 1)
                days = -1;

            diff = (3600L * (int64_t) (days * 24 + ((int) time.hour - (int) l_time->tm_hour)) +
                    (int64_t) (60 * ((int) time.minute - (int) l_time->tm_min)) +
                    (int64_t) ((int) time.second - (int) l_time->tm_sec));

            if (diff == 3600)
                tmp_t += 3600 - time.minute * 60 - time.second;	/* Move to next hour */
            else if (diff == -3600)
                tmp_t -= time.minute * 60 + time.second;	/* Move to previous hour */
        }


        /* shift back, if we were dealing with boundary dates */
        tmp_t += shift * 86400L;

        seconds = (int64_t)tmp_t;
    }
    else
    {
        long offset;
        if (timeZoneToOffset(timeZone.c_str(), timeZone.size(), &offset))
        {
            isValid = false;
            return 0;
        }
        seconds = secSinceEpoch(time.year, time.month, time.day,
                                time.hour, time.minute, time.second) - offset;
    }

    /* make sure we have legit timestamps (i.e. we didn't over/underflow anywhere above) */
    if (seconds >= MIN_TIMESTAMP_VALUE && seconds <= MAX_TIMESTAMP_VALUE)
        return seconds;

    isValid = false;
    return 0;

}


/** @brief a structure to hold a date
 */
struct Date
{
    unsigned spare  : 6;
    unsigned day    : 6;
    unsigned month  : 4;
    unsigned year   : 16;
    // NULL column value = 0xFFFFFFFE
    Date( ) :
        spare(0x3E), day(0x3F), month(0xF), year(0xFFFF) {}
    // Construct a Date from a 64 bit integer Calpont date.
    Date(uint64_t val) :
        spare(0x3E), day((val >> 6) & 077), month((val >> 12) & 0xF), year((val >> 16)) {}
    // Construct using passed in parameters, no value checking
    Date(unsigned y, unsigned m, unsigned d) : spare(0x3E), day(d), month(m), year(y) {}

    int32_t convertToMySQLint() const;
};

inline
int32_t Date::convertToMySQLint() const
{
    return (int32_t) (year * 10000) + (month * 100) + day;
}

/** @brief a structure to hold a datetime
 */
struct DateTime
{
    unsigned msecond : 20;
    unsigned second  : 6;
    unsigned minute  : 6;
    unsigned hour    : 6;
    unsigned day     : 6;
    unsigned month   : 4;
    unsigned year    : 16;
    // NULL column value = 0xFFFFFFFFFFFFFFFE
    DateTime( ) :
        msecond(0xFFFFE), second(0x3F), minute(0x3F), hour(0x3F), day(0x3F), month(0xF), year(0xFFFF) {}
    // Construct a DateTime from a 64 bit integer Calpont datetime.
    DateTime(uint64_t val) :
        msecond(val & 0xFFFFF), second((val >> 20) & 077), minute((val >> 26) & 077),
        hour((val >> 32) & 077), day((val >> 38) & 077), month((val >> 44) & 0xF),
        year(val >> 48) {}
    // Construct using passed in parameters, no value checking
    DateTime(unsigned y, unsigned m, unsigned d, unsigned h, unsigned min, unsigned sec, unsigned msec) :
        msecond(msec), second(sec), minute(min), hour(h), day(d), month(m), year(y) {}

    int64_t convertToMySQLint() const;
    void    reset();
};

inline
int64_t DateTime::convertToMySQLint() const
{
    return (int64_t) (year * 10000000000LL) + (month * 100000000) + (day * 1000000) + (hour * 10000) + (minute * 100) + second;
}

inline
void    DateTime::reset()
{
    msecond = 0xFFFFE;
    second  = 0x3F;
    minute  = 0x3F;
    hour    = 0x3F;
    day     = 0x3F;
    month   = 0xF;
    year    = 0xFFFF;
}

/** @brief a structure to hold a time
 *  range: -838:59:59 ~ 838:59:59
 */
struct Time
{
    signed msecond : 24;
    signed second  : 8;
    signed minute  : 8;
    signed hour    : 12;
    signed day     : 11;
    signed is_neg  : 1;

    // NULL column value = 0xFFFFFFFFFFFFFFFE
    Time() : msecond (0xFFFFFE),
        second (0xFF),
        minute (0xFF),
        hour (0xFFF),
        day (0x7FF),
        is_neg (0b1)
    {}

    // Construct a Time from a 64 bit integer InfiniDB time.
    Time(int64_t val) :
        msecond(val & 0xffffff),
        second((val >> 24) & 0xff),
        minute((val >> 32) & 0xff),
        hour((val >> 40) & 0xfff),
        day((val >> 52) & 0x7ff),
        is_neg(val >> 63)
    {}

    Time(signed d, signed h, signed min, signed sec, signed msec, bool neg) :
        msecond(msec), second(sec), minute(min), hour(h), day(d), is_neg(neg)
    {
        if (h < 0)
            is_neg = 0b1;
    }

    int64_t convertToMySQLint() const;
    void reset();
};

inline
void    Time::reset()
{
    msecond = 0xFFFFFE;
    second  = 0xFF;
    minute  = 0xFF;
    hour    = 0xFFF;
    is_neg  = 0b1;
    day     = 0x7FF;
}

inline
int64_t Time::convertToMySQLint() const
{
    if ((hour >= 0) && is_neg)
    {
        return (int64_t) ((hour * 10000) + (minute * 100) + second) * -1;
    }
    else if (hour >= 0)
    {
        return (int64_t) (hour * 10000) + (minute * 100) + second;
    }
    else
    {
        return (int64_t) (hour * 10000) - (minute * 100) - second;
    }
}

/** @brief a structure to hold a timestamp
 */
struct TimeStamp
{
    unsigned msecond          : 20;
    unsigned long long second : 44;
    // NULL column value = 0xFFFFFFFFFFFFFFFE
    TimeStamp( ) :
        msecond(0xFFFFE), second(0xFFFFFFFFFFF) {}
    // Construct a TimeStamp from a 64 bit integer Calpont timestamp.
    TimeStamp(uint64_t val) :
        msecond(val & 0xFFFFF), second(val >> 20) {}
    TimeStamp(unsigned msec, unsigned long long sec) :
        msecond(msec), second(sec) {}

    int64_t convertToMySQLint(const std::string& timeZone) const;
    void reset();
};

inline
int64_t TimeStamp::convertToMySQLint(const std::string& timeZone) const
{
    const int TIMESTAMPTOSTRING1_LEN = 22; // YYYYMMDDHHMMSSmmmmmm\0
    char buf[TIMESTAMPTOSTRING1_LEN];

    MySQLTime time;
    gmtSecToMySQLTime(second, time, timeZone);

    sprintf(buf, "%04d%02d%02d%02d%02d%02d", time.year, time.month, time.day, time.hour, time.minute, time.second);

    return (int64_t) atoll(buf);
}

inline
void TimeStamp::reset()
{
    msecond = 0xFFFFE;
    second = 0xFFFFFFFFFFF;
}

template<typename T=int64_t>
inline T string_to_ll( const std::string& data, bool& bSaturate )
{
    // This function doesn't take into consideration our special values
    // for NULL and EMPTY when setting the saturation point. Should it?
    char* ep = NULL;
    const char* str = data.c_str();
    errno = 0;
    int64_t value = strtoll(str, &ep, 10);

    //  (no digits) || (more chars)  || (other errors & value = 0)
    if ((ep == str) || (*ep != '\0') || (errno != 0 && value == 0))
        throw logging::QueryDataExcept("value is not numerical.", logging::formatErr);

    if (errno == ERANGE && (value == std::numeric_limits<int64_t>::max() || value == std::numeric_limits<int64_t>::min()))
        bSaturate = true;

    return value;
}

inline
uint64_t string_to_ull( const std::string& data, bool& bSaturate )
{
    // This function doesn't take into consideration our special values
    // for NULL and EMPTY when setting the saturation point. Should it?
    char* ep = NULL;
    const char* str = data.c_str();
    errno = 0;

    // check for negative number. saturate to 0;
    if (data.find('-') != data.npos)
    {
        bSaturate = true;
        return 0;
    }

    uint64_t value = strtoull(str, &ep, 10);

    //  (no digits) || (more chars)  || (other errors & value = 0)
    if ((ep == str) || (*ep != '\0') || (errno != 0 && value == 0))
        throw logging::QueryDataExcept("value is not numerical.", logging::formatErr);

    if (errno == ERANGE && (value == std::numeric_limits<uint64_t>::max()))
        bSaturate = true;

    return value;
}

template <typename T>
void number_int_value(const std::string& data,
                      cscDataType typeCode,
                      const datatypes::SystemCatalog::TypeAttributesStd &ct,
                      bool& pushwarning,
                      bool noRoundup,
                      T& intVal, bool* saturate = 0);

uint64_t number_uint_value(const string& data,
                           cscDataType typeCode,
                           const datatypes::SystemCatalog::TypeAttributesStd& ct,
                           bool& pushwarning,
                           bool  noRoundup);

/** @brief DataConvert is a component for converting string data to Calpont format
  */
class DataConvert
{
public:

    /**
      * @brief convert a columns data from native format to a string
      *
      * @param type the columns database type
      * @param data the columns string representation of it's data
      */
    EXPORT static std::string dateToString( int  datevalue );
    static inline void dateToString( int datevalue, char* buf, unsigned int buflen );

    /**
      * @brief convert a columns data from native format to a string
      *
      * @param type the columns database type
      * @param data the columns string representation of it's data
      */
    EXPORT static std::string datetimeToString( long long  datetimevalue, long decimals = 0 );
    static inline void datetimeToString( long long datetimevalue, char* buf, unsigned int buflen, long decimals = 0 );

    /**
      * @brief convert a columns data from native format to a string
      *
      * @param type the columns database type
      * @param data the columns string representation of it's data
      */
    EXPORT static std::string timestampToString( long long  timestampvalue, const std::string& timezone, long decimals = 0 );
    static inline void timestampToString( long long timestampvalue, char* buf, unsigned int buflen, const std::string& timezone, long decimals = 0 );

    /**
      * @brief convert a columns data from native format to a string
      *
      * @param type the columns database type
      * @param data the columns string representation of it's data
      */
    EXPORT static std::string timeToString( long long  timevalue, long decimals = 0 );
    static inline void timeToString( long long timevalue, char* buf, unsigned int buflen, long decimals = 0);

    /**
      * @brief convert a columns data from native format to a string
      *
      * @param type the columns database type
      * @param data the columns string representation of it's data
      */
    EXPORT static std::string dateToString1( int  datevalue );
    static inline void dateToString1( int datevalue, char* buf, unsigned int buflen );

    /**
      * @brief convert a columns data from native format to a string
      *
      * @param type the columns database type
      * @param data the columns string representation of it's data
      */
    EXPORT static std::string datetimeToString1( long long  datetimevalue );
    static inline void datetimeToString1( long long datetimevalue, char* buf, unsigned int buflen );

    /**
      * @brief convert a columns data from native format to a string
      *
      * @param type the columns database type
      * @param data the columns string representation of it's data
      */
    EXPORT static std::string timestampToString1( long long  timestampvalue, const std::string& timezone );
    static inline void timestampToString1( long long timestampvalue, char* buf, unsigned int buflen, const std::string& timezone );

    /**
      * @brief convert a columns data from native format to a string
      *
      * @param type the columns database type
      * @param data the columns string representation of it's data
      */
    EXPORT static std::string timeToString1( long long  timevalue );
    static inline void timeToString1( long long timevalue, char* buf, unsigned int buflen );

    /**
     * @brief convert a date column data, represnted as a string, to it's native
     * format. This function is for bulkload to use.
     *
     * @param type the columns data type
     * @param dataOrig the columns string representation of it's data
     * @param dateFormat the format the date value in
     * @param status 0 - success, -1 - fail
     * @param dataOrgLen length specification of dataOrg
     */
    EXPORT static int32_t convertColumnDate( const char* dataOrg,
            CalpontDateTimeFormat dateFormat,
            int& status, unsigned int dataOrgLen );

    /**
     * @brief Is specified date valid; used by binary bulk load
     */
    EXPORT static bool      isColumnDateValid( int32_t date );

    /**
     * @brief convert a datetime column data, represented as a string,
     * to it's native format. This function is for bulkload to use.
     *
     * @param type the columns data type
     * @param dataOrig the columns string representation of it's data
     * @param datetimeFormat the format the date value in
     * @param status 0 - success, -1 - fail
     * @param dataOrgLen length specification of dataOrg
     */
    EXPORT static int64_t convertColumnDatetime( const char* dataOrg,
            CalpontDateTimeFormat datetimeFormat,
            int& status, unsigned int dataOrgLen );

    /**
     * @brief convert a timestamp column data, represented as a string,
     * to it's native format. This function is for bulkload to use.
     *
     * @param dataOrg the columns string representation of it's data
     * @param datetimeFormat the format the date value in
     * @param status 0 - success, -1 - fail
     * @param dataOrgLen length specification of dataOrg
     * @param timeZone the timezone used for conversion to native format
     */
    EXPORT static int64_t convertColumnTimestamp( const char* dataOrg,
            CalpontDateTimeFormat datetimeFormat,
            int& status, unsigned int dataOrgLen,
            const std::string& timeZone );

    /**
     * @brief convert a time column data, represented as a string,
     * to it's native format. This function is for bulkload to use.
     *
     * @param type the columns data type
     * @param dataOrig the columns string representation of it's data
     * @param timeFormat the format the time value in
     * @param status 0 - success, -1 - fail
     * @param dataOrgLen length specification of dataOrg
     */
    EXPORT static int64_t convertColumnTime( const char* dataOrg,
            CalpontDateTimeFormat datetimeFormat,
            int& status, unsigned int dataOrgLen );

    /**
     * @brief Is specified datetime valid; used by binary bulk load
     */
    EXPORT static bool      isColumnDateTimeValid( int64_t dateTime );
    EXPORT static bool      isColumnTimeValid( int64_t time );
    EXPORT static bool      isColumnTimeStampValid( int64_t timeStamp );

    static inline void trimWhitespace(int64_t& charData);

    // convert string to date
    EXPORT static int64_t stringToDate(const std::string& data);
    // convert string to datetime
    EXPORT static int64_t stringToDatetime(const std::string& data, bool* isDate = NULL);
    // convert string to timestamp
    EXPORT static int64_t stringToTimestamp(const std::string& data, const std::string& timeZone);
    // convert integer to date
    EXPORT static int64_t intToDate(int64_t data);
    // convert integer to datetime
    EXPORT static int64_t intToDatetime(int64_t data, bool* isDate = NULL);
    // convert integer to date
    EXPORT static int64_t intToTime(int64_t data, bool fromString = false);
    // convert string to date. alias to stringToDate
    EXPORT static int64_t dateToInt(const std::string& date);
    // convert string to datetime. alias to datetimeToInt
    EXPORT static int64_t datetimeToInt(const std::string& datetime);
    EXPORT static int64_t timestampToInt(const std::string& timestamp, const std::string& timeZone);
    EXPORT static int64_t timeToInt(const std::string& time);
    EXPORT static int64_t stringToTime (const std::string& data);
    // bug4388, union type conversion
    EXPORT static void joinColTypeForUnion(datatypes::SystemCatalog::TypeHolderStd &unionedType,
                                           const datatypes::SystemCatalog::TypeHolderStd &type);

    static boost::any StringToBit(const datatypes::SystemCatalog::TypeAttributesStd& colType,
                                  const datatypes::ConvertFromStringParam &prm,
                                  const std::string& dataOrig,
                                  bool& pushWarning);

    static boost::any StringToSDecimal(const datatypes::SystemCatalog::TypeAttributesStd& colType,
                                       const datatypes::ConvertFromStringParam &prm,
                                       const std::string& data,
                                       bool& pushWarning);

    static boost::any StringToUDecimal(const datatypes::SystemCatalog::TypeAttributesStd& colType,
                                       const datatypes::ConvertFromStringParam &prm,
                                       const std::string& data,
                                       bool& pushWarning);

    static boost::any StringToFloat(cscDataType typeCode,
                                    const std::string& dataOrig,
                                    bool& pushWarning);

    static boost::any StringToDouble(cscDataType typeCode,
                                     const std::string& dataOrig,
                                     bool& pushWarning);

    static boost::any StringToString(const datatypes::SystemCatalog::TypeAttributesStd& colType,
                                     const std::string& dataOrig,
                                     bool& pushWarning);

    static boost::any StringToDate(const std::string& data,
                                   bool& pushWarning);

    static boost::any StringToDatetime(const std::string& data,
                                       bool& pushWarning);

    static boost::any StringToTime(const datatypes::SystemCatalog::TypeAttributesStd& colType,
                                   const std::string& data,
                                   bool& pushWarning);

    static boost::any StringToTimestamp(const datatypes::ConvertFromStringParam &prm,
                                        const std::string& data,
                                        bool& pushWarning);
};

inline void DataConvert::dateToString( int datevalue, char* buf, unsigned int buflen)
{
    snprintf( buf, buflen, "%04d-%02d-%02d",
              (unsigned)((datevalue >> 16) & 0xffff),
              (unsigned)((datevalue >> 12) & 0xf),
              (unsigned)((datevalue >> 6) & 0x3f)
            );
}

inline void DataConvert::datetimeToString( long long datetimevalue, char* buf, unsigned int buflen, long decimals )
{
    // 10 is default which means we don't need microseconds
    if (decimals > 6 || decimals < 0)
    {
        decimals = 0;
    }

    int msec = 0;

    if ((datetimevalue & 0xfffff) > 0)
    {
        msec = (unsigned)((datetimevalue) & 0xfffff);
    }

    snprintf( buf, buflen, "%04d-%02d-%02d %02d:%02d:%02d",
              (unsigned)((datetimevalue >> 48) & 0xffff),
              (unsigned)((datetimevalue >> 44) & 0xf),
              (unsigned)((datetimevalue >> 38) & 0x3f),
              (unsigned)((datetimevalue >> 32) & 0x3f),
              (unsigned)((datetimevalue >> 26) & 0x3f),
              (unsigned)((datetimevalue >> 20) & 0x3f)
            );

    if (msec || decimals)
    {
        snprintf(buf + strlen(buf), buflen - strlen(buf), ".%0*d", (int)decimals, msec);
    }
}

inline void DataConvert::timestampToString( long long timestampvalue, char* buf, unsigned int buflen, const std::string& timezone, long decimals )
{
    // 10 is default which means we don't need microseconds
    if (decimals > 6 || decimals < 0)
    {
        decimals = 0;
    }

    TimeStamp timestamp(timestampvalue);
    int64_t seconds = timestamp.second;

    MySQLTime time;
    gmtSecToMySQLTime(seconds, time, timezone);

    snprintf( buf, buflen, "%04d-%02d-%02d %02d:%02d:%02d",
              time.year, time.month, time.day,
              time.hour, time.minute, time.second
            );

    if (timestamp.msecond || decimals)
    {
        snprintf(buf + strlen(buf), buflen - strlen(buf), ".%0*d", (int)decimals, timestamp.msecond);
    }
}

inline void DataConvert::timeToString( long long timevalue, char* buf, unsigned int buflen, long decimals )
{
    // 10 is default which means we don't need microseconds
    if (decimals > 6 || decimals < 0)
    {
        decimals = 0;
    }

    // Handle negative correctly
    int hour = 0, msec = 0;

    if ((timevalue >> 40) & 0x800)
    {
        hour = 0xfffff000;
    }

    hour |= ((timevalue >> 40) & 0xfff);

    if ((timevalue & 0xffffff) > 0)
    {
        msec = (unsigned)((timevalue) & 0xffffff);
    }

    if ((hour >= 0) && (timevalue >> 63))
    {
        buf[0] = '-';
        buf++;
        buflen--;
    }

    snprintf( buf, buflen, "%02d:%02d:%02d",
              hour,
              (unsigned)((timevalue >> 32) & 0xff),
              (unsigned)((timevalue >> 24) & 0xff)
            );

    if (msec || decimals)
    {
        // Pad start with zeros
        snprintf(buf + strlen(buf), buflen - strlen(buf), ".%0*d", (int)decimals, msec);
    }
}

inline void DataConvert::dateToString1( int datevalue, char* buf, unsigned int buflen)
{
    snprintf( buf, buflen, "%04d%02d%02d",
              (unsigned)((datevalue >> 16) & 0xffff),
              (unsigned)((datevalue >> 12) & 0xf),
              (unsigned)((datevalue >> 6) & 0x3f)
            );
}

inline void DataConvert::datetimeToString1( long long datetimevalue, char* buf, unsigned int buflen )
{
    snprintf( buf, buflen, "%04d%02d%02d%02d%02d%02d",
              (unsigned)((datetimevalue >> 48) & 0xffff),
              (unsigned)((datetimevalue >> 44) & 0xf),
              (unsigned)((datetimevalue >> 38) & 0x3f),
              (unsigned)((datetimevalue >> 32) & 0x3f),
              (unsigned)((datetimevalue >> 26) & 0x3f),
              (unsigned)((datetimevalue >> 20) & 0x3f)
            );
}

inline void DataConvert::timestampToString1( long long timestampvalue, char* buf, unsigned int buflen, const std::string& timezone )
{
    TimeStamp timestamp(timestampvalue);
    int64_t seconds = timestamp.second;

    MySQLTime time;
    gmtSecToMySQLTime(seconds, time, timezone);

    snprintf( buf, buflen, "%04d%02d%02d%02d%02d%02d",
              time.year, time.month, time.day,
              time.hour, time.minute, time.second
            );
}

inline void DataConvert::timeToString1( long long timevalue, char* buf, unsigned int buflen )
{
    // Handle negative correctly
    int hour = 0;

    if ((timevalue >> 40) & 0x800)
    {
        hour = 0xfffff000;
    }

    hour |= ((timevalue >> 40) & 0xfff);

    if ((hour >= 0) && (timevalue >> 63))
    {
        buf[0] = '-';
        buf++;
        buflen--;
    }
    // this snprintf call causes a compiler warning b/c buffer size is less
    // then maximum string size.
#if defined(__GNUC__) && __GNUC__ >= 7
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation="
    snprintf( buf, buflen, "%02d%02d%02d",
              hour,
              (unsigned)((timevalue >> 32) & 0xff),
              (unsigned)((timevalue >> 14) & 0xff)
            );
#pragma GCC diagnostic pop
#else
    snprintf( buf, buflen, "%02d%02d%02d",
              hour,
              (unsigned)((timevalue >> 32) & 0xff),
              (unsigned)((timevalue >> 14) & 0xff)
            );
#endif
}

inline void DataConvert::trimWhitespace(int64_t& charData)
{
    // Trims whitespace characters off non-dict character data
    char* ch_data = (char*) &charData;

    for (int8_t i = 7; i > 0; i--)
    {
        if (ch_data[i] == ' ' || ch_data[i] == '\0')
            ch_data[i] = '\0';
        else
            break;
    }
}


inline int128_t add128(int128_t a, int128_t b)
{
    return a + b;
}

inline int128_t subtract128(int128_t a, int128_t b)
{
    return a - b;
}

inline bool lessThan128(int128_t a, int128_t b)
{
    return a < b;
}

inline bool greaterThan128(int128_t a, int128_t b)
{
    return a > b;
}

// Naive int128_t version of strtoll
inline int128_t strtoll128(const char* data, bool& saturate, char** ep)
{
    int128_t res = 0;

    if (*data == '\0')
    {
        if (ep) 
            *ep = (char*)data;
        return res;
    }

    // skip leading whitespace characters
    while (*data != '\0' && 
           (*data == ' ' || *data == '\t' || *data == '\n'))
        data++;

    int128_t (*op)(int128_t, int128_t);
    op = add128;
    bool (*compare)(int128_t, int128_t);
    compare = lessThan128;

    // check the -ve sign
    bool is_neg = false;
    if (*data == '-')
    {
        is_neg = true;
        op = subtract128;
        compare = greaterThan128;
        data++;
    }

    int128_t tmp;

    for (; *data != '\0' && isdigit(*data); data++)
    {
        tmp = op(res*10, *data - '0');

        if (UNLIKELY(compare(tmp, res)))
        {
            saturate = true;

            if (is_neg)
                utils::int128Min(res);
            else
                utils::int128Max(res);

            while (*data != '\0' && isdigit(*data))
                data++;

            if (ep)
                *ep = (char*)data;

            return res;
        }

        res = tmp;
    }

    if (ep)
        *ep = (char*)data;

    return res;
}

template<>
inline int128_t string_to_ll<int128_t> ( const std::string& data, bool& bSaturate )
{
    // This function doesn't take into consideration our special values
    // for NULL and EMPTY when setting the saturation point. Should it?
    char* ep = NULL;
    const char* str = data.c_str();
    int128_t value = strtoll128(str, bSaturate, &ep);

    //  (no digits) || (more chars)
    if ((ep == str) || (*ep != '\0'))
        throw logging::QueryDataExcept("value is not numerical.", logging::formatErr);

    return value;
}

} // namespace dataconvert

#undef EXPORT

#endif //DATACONVERT_H


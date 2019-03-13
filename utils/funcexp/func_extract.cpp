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
* $Id: func_extract.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_int.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "intervalcolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace
{
using namespace funcexp;

long long dateGet( uint64_t time, IntervalColumn::interval_type unit, bool dateType )
{
    uint64_t year = 0,
             month = 0,
             day = 0,
             hour = 0,
             min = 0,
             sec = 0,
             msec = 0;

    if (dateType)
    {
        year = (uint32_t)((time >> 16) & 0xffff);
        month = (uint32_t)((time >> 12) & 0xf);
        day = (uint32_t)((time >> 6) & 0x3f);
    }
    else
    {
        year = (uint32_t)((time >> 48) & 0xffff);
        month = (uint32_t)((time >> 44) & 0xf);
        day = (uint32_t)((time >> 38) & 0x3f);
        hour = (uint32_t)((time >> 32) & 0x3f);
        min = (uint32_t)((time >> 26) & 0x3f);
        sec = (uint32_t)((time >> 20) & 0x3f);
        msec = (uint32_t)((time & 0xfffff));
    }

    switch ( unit )
    {
        case IntervalColumn::INTERVAL_YEAR:
            return year;

        case IntervalColumn::INTERVAL_MONTH:
            return month;

        case IntervalColumn::INTERVAL_DAY:
            return day;

        case IntervalColumn::INTERVAL_HOUR:
            return hour;

        case IntervalColumn::INTERVAL_MINUTE:
            return min;

        case IntervalColumn::INTERVAL_SECOND:
            return sec;

        case IntervalColumn::INTERVAL_MICROSECOND:
            return msec;

        case IntervalColumn::INTERVAL_QUARTER:
            return month / 4 + 1;

        case IntervalColumn::INTERVAL_WEEK:
            return helpers::calc_mysql_week(year, month, day, 0);

        case IntervalColumn::INTERVAL_YEAR_MONTH:
            return (year * 100) + month;

        case IntervalColumn::INTERVAL_DAY_HOUR:
            return (day * 100) + hour;

        case IntervalColumn::INTERVAL_DAY_MINUTE:
            return (day * 10000) + (hour * 100) + min;

        case IntervalColumn::INTERVAL_DAY_SECOND:
            return (day * 1000000) + (hour * 10000) + (min * 100) + sec;

        case IntervalColumn::INTERVAL_HOUR_MINUTE:
            return (hour * 100) + min;

        case IntervalColumn::INTERVAL_HOUR_SECOND:
            return (hour * 10000) + (min * 100) + sec;

        case IntervalColumn::INTERVAL_MINUTE_SECOND:
            return (min * 100) + sec;

        case IntervalColumn::INTERVAL_DAY_MICROSECOND:
            return (((day * 1000000) + (hour * 10000) + (min * 100) + sec) * 1000000) + msec;

        case IntervalColumn::INTERVAL_HOUR_MICROSECOND:
            return (((hour * 10000) + (min * 100) + sec) * 1000000) + msec;

        case IntervalColumn::INTERVAL_MINUTE_MICROSECOND:
            return (((min * 100) + sec) * 1000000) + msec;

        case IntervalColumn::INTERVAL_SECOND_MICROSECOND:
            return (sec * 1000000) + msec;

        default:
            throw runtime_error("unit type is not supported: " + unit);
    };
}

long long timeGet( uint64_t time, IntervalColumn::interval_type unit )
{
    int32_t hour = 0,
            min = 0,
            sec = 0,
            msec = 0,
            day = 0;

    min = (int32_t)((time >> 32) & 0xff);
    sec = (int32_t)((time >> 24) & 0xff);
    msec = (int32_t)((time & 0xfffff));

    // If negative, mask so it doesn't turn positive
    int64_t mask = 0;

    if ((time >> 40) & 0x800)
        mask = 0xfffffffffffff000;

    hour = mask | ((time >> 40) & 0xfff);

    if ((hour >= 0) && (time >> 63))
        hour *= -1;

    // Always positive!
    day = abs(hour / 24);

    switch ( unit )
    {
        case IntervalColumn::INTERVAL_YEAR:
        case IntervalColumn::INTERVAL_MONTH:
            return 0;

        case IntervalColumn::INTERVAL_DAY:
            return day;

        case IntervalColumn::INTERVAL_HOUR:
            return hour;

        case IntervalColumn::INTERVAL_MINUTE:
            return min;

        case IntervalColumn::INTERVAL_SECOND:
            return sec;

        case IntervalColumn::INTERVAL_MICROSECOND:
            return msec;

        case IntervalColumn::INTERVAL_QUARTER:
        case IntervalColumn::INTERVAL_WEEK:
        case IntervalColumn::INTERVAL_YEAR_MONTH:
            return 0;

        case IntervalColumn::INTERVAL_DAY_HOUR:
            return (day * 100) + hour;

        case IntervalColumn::INTERVAL_DAY_MINUTE:
            return (day * 10000) + (hour * 100) + min;

        case IntervalColumn::INTERVAL_DAY_SECOND:
            return (day * 1000000) + (hour * 10000) + (min * 100) + sec;

        case IntervalColumn::INTERVAL_HOUR_MINUTE:
            return (hour * 100) + min;

        case IntervalColumn::INTERVAL_HOUR_SECOND:
            return (hour * 10000) + (min * 100) + sec;

        case IntervalColumn::INTERVAL_MINUTE_SECOND:
            return (min * 100) + sec;

        case IntervalColumn::INTERVAL_DAY_MICROSECOND:
            return (((day * 1000000) + (hour * 10000) + (min * 100) + sec) * 1000000) + msec;

        case IntervalColumn::INTERVAL_HOUR_MICROSECOND:
            return (((hour * 10000) + (min * 100) + sec) * 1000000) + msec;

        case IntervalColumn::INTERVAL_MINUTE_MICROSECOND:
            return (((min * 100) + sec) * 1000000) + msec;

        case IntervalColumn::INTERVAL_SECOND_MICROSECOND:
            return (sec * 1000000) + msec;

        default:
            throw runtime_error("unit type is not supported: " + unit);
    };
}


}

namespace funcexp
{

CalpontSystemCatalog::ColType Func_extract::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}

int64_t Func_extract::getIntVal(rowgroup::Row& row,
                                FunctionParm& parm,
                                bool& isNull,
                                CalpontSystemCatalog::ColType& ct)
{
    IntervalColumn::interval_type unit = static_cast<IntervalColumn::interval_type>(parm[1]->data()->getIntVal(row, isNull));
    uint64_t time;
    bool isTime = false;

    //@bug4678 handle conversion from non date/datetime datatype
    switch (parm[0]->data()->resultType().colDataType)
    {
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
            time = parm[0]->data()->getDatetimeIntVal(row, isNull);
            break;

        case CalpontSystemCatalog::TIME:
            time = parm[0]->data()->getTimeIntVal(row, isNull);
            isTime = true;
            break;

        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::TEXT:
        {
            const string& val = parm[0]->data()->getStrVal(row, isNull);
            time = dataconvert::DataConvert::stringToDatetime(val);
            break;
        }

        case CalpontSystemCatalog::INT:
        case CalpontSystemCatalog::TINYINT:
        case CalpontSystemCatalog::MEDINT:
        case CalpontSystemCatalog::BIGINT:
        case CalpontSystemCatalog::SMALLINT:
        {
            int64_t val = parm[0]->data()->getIntVal(row, isNull);
            time = dataconvert::DataConvert::intToDatetime(val);
            break;
        }

        default:
            time = parm[0]->data()->getIntVal(row, isNull);
    }

    long long value;

    if (isTime)
        value = timeGet( time, unit );
    else
        value = dateGet( time, unit, false );

    return value;
}


} // namespace funcexp
// vim:ts=4 sw=4:

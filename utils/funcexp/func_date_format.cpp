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
const string IDB_date_format(const DateTime& dt, const string& format)
{
    // assume 256 is enough. assume not allowing incomplete date
    char buf[256];
    char* ptr = buf;
    uint32_t weekday = 0;
    uint32_t dayval = 0;
    uint32_t weekval = 0;
    uint32_t weekyear = 0;

    for (uint32_t i = 0; i < format.length(); i++)
    {
        if (format[i] != '%')
            *ptr++ = format[i];
        else
        {
            i++;

            switch (format[i])
            {
                case 'M':
                    sprintf(ptr, "%s", helpers::monthFullNames[dt.month].c_str());
                    ptr += helpers::monthFullNames[dt.month].length();
                    break;

                case 'b':
                    sprintf(ptr, "%s", helpers::monthAbNames[dt.month].c_str());
                    ptr += helpers::monthAbNames[dt.month].length();
                    break;

                case 'W':
                    weekday = helpers::calc_mysql_weekday( dt.year, dt.month, dt.day, false);
                    sprintf(ptr, "%s", helpers::weekdayFullNames[weekday].c_str());
                    ptr += helpers::weekdayFullNames[weekday].length();
                    break;

                case 'w':
                    weekday = helpers::calc_mysql_weekday( dt.year, dt.month, dt.day, true);
                    sprintf(ptr, "%01d", weekday);
                    ptr += 1;
                    break;

                case 'a':
                    weekday = helpers::calc_mysql_weekday( dt.year, dt.month, dt.day, false);
                    sprintf(ptr, "%s", helpers::weekdayAbNames[weekday].c_str());
                    ptr += helpers::weekdayAbNames[weekday].length();
                    break;

                case 'D':
                    sprintf(ptr, "%s", helpers::dayOfMonth[dt.day].c_str());
                    ptr += helpers::dayOfMonth[dt.day].length();
                    break;

                case 'Y':
                    sprintf(ptr, "%04d", dt.year);
                    ptr += 4;
                    break;

                case 'y':
                    sprintf(ptr, "%02d", dt.year % 100);
                    ptr += 2;
                    break;

                case 'm':
                    sprintf(ptr, "%02d", dt.month);
                    ptr += 2;
                    break;

                case 'c':
                    sprintf(ptr, "%d", dt.month);
                    ptr = ptr + (dt.month >= 10 ? 2 : 1);
                    break;

                case 'd':
                    sprintf(ptr, "%02d", dt.day);
                    ptr += 2;
                    break;

                case 'e':
                    sprintf(ptr, "%d", dt.day);
                    ptr = ptr + (dt.day >= 10 ? 2 : 1);
                    break;

                case 'f':
                    sprintf(ptr, "%06d", dt.msecond);
                    ptr += 6;
                    break;

                case 'H':
                    sprintf(ptr, "%02d", dt.hour);
                    ptr += 2;
                    break;

                case 'h':
                case 'I':
                    sprintf(ptr, "%02d", (dt.hour % 24 + 11) % 12 + 1);
                    ptr += 2;
                    break;

                case 'i':					/* minutes */
                    sprintf(ptr, "%02d", dt.minute);
                    ptr += 2;
                    break;

                case 'j':
                    dayval = helpers::calc_mysql_daynr( dt.year, dt.month, dt.day ) -
                             helpers::calc_mysql_daynr( dt.year, 1, 1 ) + 1;
                    sprintf(ptr, "%03d", dayval);
                    ptr += 3;
                    break;

                case 'k':
                    sprintf(ptr, "%d", dt.hour);
                    ptr += (dt.hour >= 10 ? 2 : 1);
                    break;

                case 'l':
                    sprintf(ptr, "%d", (dt.hour % 24 + 11) % 12 + 1);
                    ptr += ((dt.hour % 24 + 11) % 12 + 1 >= 10 ? 2 : 1);
                    break;

                case 'p':
                    sprintf(ptr, "%s", (dt.hour % 24 < 12 ? "AM" : "PM"));
                    ptr += 2;
                    break;

                case 'r':
                    sprintf(ptr, (dt.hour % 24 < 12 ? "%02d:%02d:%02d AM" : "%02d:%02d:%02d PM"),
                            (dt.hour + 11) % 12 + 1, dt.minute, dt.second);
                    ptr += 11;
                    break;

                case 'S':
                case 's':
                    sprintf(ptr, "%02d", dt.second);
                    ptr += 2;
                    break;

                case 'T':
                    sprintf (ptr, "%02d:%02d:%02d", dt.hour, dt.minute, dt.second);
                    ptr += 8;
                    break;

                case 'U':
                    weekval = helpers::calc_mysql_week( dt.year, dt.month, dt.day, 0);
                    sprintf(ptr, "%02d", weekval);
                    ptr += 2;
                    break;

                case 'V':
                    weekval = helpers::calc_mysql_week( dt.year, dt.month, dt.day,
                                                        helpers::WEEK_NO_ZERO );
                    sprintf(ptr, "%02d", weekval);
                    ptr += 2;
                    break;

                case 'u':
                    weekval = helpers::calc_mysql_week( dt.year, dt.month, dt.day,
                                                        helpers::WEEK_MONDAY_FIRST | helpers::WEEK_GT_THREE_DAYS);
                    sprintf(ptr, "%02d", weekval);
                    ptr += 2;
                    break;

                case 'v':
                    weekval = helpers::calc_mysql_week( dt.year, dt.month, dt.day,
                                                        helpers::WEEK_NO_ZERO | helpers::WEEK_MONDAY_FIRST | helpers::WEEK_GT_THREE_DAYS);
                    sprintf(ptr, "%02d", weekval);
                    ptr += 2;
                    break;

                case 'x':
                    helpers::calc_mysql_week( dt.year, dt.month, dt.day,
                                              helpers::WEEK_NO_ZERO | helpers::WEEK_MONDAY_FIRST | helpers::WEEK_GT_THREE_DAYS, &weekyear);
                    sprintf(ptr, "%04d", weekyear);
                    ptr += 4;
                    break;

                case 'X':
                    helpers::calc_mysql_week( dt.year, dt.month, dt.day,
                                              helpers::WEEK_NO_ZERO, &weekyear);
                    sprintf(ptr, "%04d", weekyear);
                    ptr += 4;
                    break;

                default:
                    *ptr++ = format[i];
            }
        }
    }

    *ptr = 0;
    return string(buf);
}
}

CalpontSystemCatalog::ColType Func_date_format::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    CalpontSystemCatalog::ColType ct;
    ct.colDataType = CalpontSystemCatalog::VARCHAR;
    ct.colWidth = 255;
    return ct;
}

string Func_date_format::getStrVal(rowgroup::Row& row,
                                   FunctionParm& parm,
                                   bool& isNull,
                                   CalpontSystemCatalog::ColType&)
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
            val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));

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

            break;

        default:
            isNull = true;
            return "";
    }

    const string& format = parm[1]->data()->getStrVal(row, isNull);

    return helpers::IDB_date_format(dt, format);
}


int32_t Func_date_format::getDateIntVal(rowgroup::Row& row,
                                        FunctionParm& parm,
                                        bool& isNull,
                                        CalpontSystemCatalog::ColType& ct)
{
    return dataconvert::DataConvert::dateToInt(getStrVal(row, parm, isNull, ct));
}


int64_t Func_date_format::getDatetimeIntVal(rowgroup::Row& row,
        FunctionParm& parm,
        bool& isNull,
        CalpontSystemCatalog::ColType& ct)
{
    return dataconvert::DataConvert::datetimeToInt(getStrVal(row, parm, isNull, ct));
}


} // namespace funcexp
// vim:ts=4 sw=4:

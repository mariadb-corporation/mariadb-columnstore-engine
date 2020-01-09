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
* $Id: dataconvert.cpp 3901 2013-06-17 20:59:13Z rdempsey $
*
*
****************************************************************************/

#include <string>
#include <cmath>
#include <errno.h>
#include <ctime>
#include <stdlib.h>
#include <string.h>
using namespace std;
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string.hpp>
using namespace boost::algorithm;
#include <boost/tokenizer.hpp>
#include "calpontsystemcatalog.h"
#include "calpontselectexecutionplan.h"
#include "columnresult.h"
using namespace execplan;

#include "joblisttypes.h"

#define DATACONVERT_DLLEXPORT
#include "dataconvert.h"
#undef DATACONVERT_DLLEXPORT

#ifndef __linux__
typedef uint32_t ulong;
#endif

using namespace logging;

namespace
{

const int64_t columnstore_precision[19] =
{
    0,
    9,
    99,
    999,
    9999,
    99999,
    999999,
    9999999,
    99999999,
    999999999,
    9999999999LL,
    99999999999LL,
    999999999999LL,
    9999999999999LL,
    99999999999999LL,
    999999999999999LL,
    9999999999999999LL,
    99999999999999999LL,
    999999999999999999LL
};

template <class T>
bool from_string(T& t, const std::string& s, std::ios_base & (*f)(std::ios_base&))
{
    std::istringstream iss(s);
    return !(iss >> f >> t).fail();
}

bool number_value ( const string& data )
{
    for (unsigned int i = 0; i < strlen(data.c_str()); i++)
    {
        if (data[i] > '9' || data[i] < '0')
        {
            if (data[i] != '+' && data[i] != '-' && data[i] != '.' && data[i] != ' ' &&
                    data[i] != 'E' && data[i] != 'e')
            {
                throw QueryDataExcept("value is not numerical.", formatErr);
            }
        }
    }

    return true;
}

int64_t number_int_value(const string& data,
                         const CalpontSystemCatalog::ColType& ct,
                         bool& pushwarning,
                         bool  noRoundup)
{
    // copy of the original input
    string valStr(data);

    // in case, the values are in parentheses
    string::size_type x = valStr.find('(');
    string::size_type y = valStr.find(')');

    while (x < string::npos)
    {
        // erase y first
        if (y == string::npos)
            throw QueryDataExcept("'(' is not matched.", formatErr);

        valStr.erase(y, 1);
        valStr.erase(x, 1);
        x = valStr.find('(');
        y = valStr.find(')');
    }

    if (y != string::npos)
        throw QueryDataExcept("')' is not matched.", formatErr);

    if (boost::iequals(valStr, "true"))
    {
        return 1;
    }
    if (boost::iequals(valStr, "false"))
    {
        return 0;
    }

    // convert to fixed-point notation if input is in scientific notation
    if (valStr.find('E') < string::npos || valStr.find('e') < string::npos)
    {
        size_t epos = valStr.find('E');

        if (epos == string::npos)
            epos = valStr.find('e');

        // get the coefficient
        string coef = valStr.substr(0, epos);
        // get the exponent
        string exp = valStr.substr(epos + 1);
        bool overflow = false;
        int64_t exponent = dataconvert::string_to_ll(exp, overflow);

        // if the exponent can not be held in 64-bit, not supported or saturated.
        if (overflow)
            throw QueryDataExcept("value is invalid.", formatErr);

        // find the optional "." point
        size_t dpos = coef.find('.');

        if (dpos != string::npos)
        {
            // move "." to the end by mutiply 10 ** (# of fraction digits)
            coef.erase(dpos, 1);
            exponent -= coef.length() - dpos;
        }

        if (exponent >= 0)
        {
            coef.resize(coef.length() + exponent, '0');
        }
        else
        {
            size_t bpos = coef.find_first_of("0123456789");
            size_t epos = coef.length();
            size_t mpos = -exponent;
            dpos = epos - mpos;
            int64_t padding = (int64_t)mpos - (int64_t)(epos - bpos);

            if (padding > 0)
            {
                coef.insert(bpos, padding, '0');
                dpos = bpos;
            }

            coef.insert(dpos, ".");
        }

        valStr = coef;
    }

    // apply the scale
    if (ct.scale != 0)
    {
        uint64_t scale = (uint64_t) (ct.scale < 0) ? (-ct.scale) : (ct.scale);
        size_t dpos = valStr.find('.');
        string intPart = valStr.substr(0, dpos);
        string leftStr;

        if (ct.scale > 0)
        {
            if (dpos != string::npos)
            {
                // decimal point exist, prepare "#scale" digits in fraction part
                ++dpos;
                string frnStr = valStr.substr(dpos, scale);

                if (frnStr.length() < scale)
                    frnStr.resize(scale, '0');  // padding digit 0, not null.

                // effectly shift "#scale" digits to left.
                intPart += frnStr;
                leftStr = valStr.substr(dpos);
                leftStr.erase(0, scale);
            }
            else
            {
                // no decimal point, shift "#scale" digits to left.
                intPart.resize(intPart.length() + scale, '0'); // padding digit 0, not null.
            }
        }
        else // if (ct.scale < 0) -- in ct.scale != 0 block
        {
            if (dpos != string::npos)
            {
                // decimal point exist, get the fraction part
                ++dpos;
                leftStr = valStr.substr(dpos);
            }

// add above to keep the old behavior, to comply with tdriver
// uncomment code below to support negative scale
#if 0
            // check if enough digits in the integer part
            size_t spos = intPart.find_first_of("0123456789");

            if (string::npos == spos)
                spos = intPart.length();

            size_t len = intPart.substr(spos).length();

            if (len < scale)
                intPart.insert(spos, scale - len, '0');  // padding digit 0, not null.

            leftStr = intPart.substr(intPart.length() - scale) + leftStr;
            intPart.erase(intPart.length() - scale, scale);
#endif
        }

        valStr = intPart;

        if (leftStr.length() > 0)
            valStr += "." + leftStr;
    }

    // now, convert to long long int
    string intStr(valStr);
    string frnStr = "";
    size_t dp = valStr.find('.');
    int    roundup = 0;

    if (dp != string::npos)
    {
        //Check if need round up
        int frac1 = dataconvert::string_to_ll(valStr.substr(dp + 1, 1), pushwarning);

        if ((!noRoundup) && frac1 >= 5)
            roundup = 1;

        intStr.erase(dp);
        frnStr = valStr.substr(dp + 1);

        if ( intStr.length() == 0 )
            intStr = "0";
        else if (( intStr.length() == 1 ) && ( (intStr[0] == '+') || (intStr[0] == '-') ) )
        {
            intStr.insert( 1, 1, '0');
        }
    }

    int64_t intVal = dataconvert::string_to_ll(intStr, pushwarning);
    //@Bug 3350 negative value round up.
    intVal += intVal >= 0 ? roundup : -roundup;
    bool dummy = false;
    int64_t frnVal = (frnStr.length() > 0) ? dataconvert::string_to_ll(frnStr, dummy) : 0;

    if (frnVal != 0)
        pushwarning = true;

    switch (ct.colDataType)
    {
        case CalpontSystemCatalog::TINYINT:
            if (intVal < MIN_TINYINT)
            {
                intVal = MIN_TINYINT;
                pushwarning = true;
            }
            else if (intVal > MAX_TINYINT)
            {
                intVal = MAX_TINYINT;
                pushwarning = true;
            }

            break;

        case CalpontSystemCatalog::SMALLINT:
            if (intVal < MIN_SMALLINT)
            {
                intVal = MIN_SMALLINT;
                pushwarning = true;
            }
            else if (intVal > MAX_SMALLINT)
            {
                intVal = MAX_SMALLINT;
                pushwarning = true;
            }

            break;

        case CalpontSystemCatalog::MEDINT:
            if (intVal < MIN_MEDINT)
            {
                intVal = MIN_MEDINT;
                pushwarning = true;
            }
            else if (intVal > MAX_MEDINT)
            {
                intVal = MAX_MEDINT;
                pushwarning = true;
            }

            break;

        case CalpontSystemCatalog::INT:
            if (intVal < MIN_INT)
            {
                intVal = MIN_INT;
                pushwarning = true;
            }
            else if (intVal > MAX_INT)
            {
                intVal = MAX_INT;
                pushwarning = true;
            }

            break;

        case CalpontSystemCatalog::BIGINT:
            if (intVal < MIN_BIGINT)
            {
                intVal = MIN_BIGINT;
                pushwarning = true;
            }

            break;

        case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL:
            if (ct.colWidth == 1)
            {
                if (intVal < MIN_TINYINT)
                {
                    intVal = MIN_TINYINT;
                    pushwarning = true;
                }
                else if (intVal > MAX_TINYINT)
                {
                    intVal = MAX_TINYINT;
                    pushwarning = true;
                }
            }
            else if (ct.colWidth == 2)
            {
                if (intVal < MIN_SMALLINT)
                {
                    intVal = MIN_SMALLINT;
                    pushwarning = true;
                }
                else if (intVal > MAX_SMALLINT)
                {
                    intVal = MAX_SMALLINT;
                    pushwarning = true;
                }
            }
            else if (ct.colWidth == 4)
            {
                if (intVal < MIN_INT)
                {
                    intVal = MIN_INT;
                    pushwarning = true;
                }
                else if (intVal > MAX_INT)
                {
                    intVal = MAX_INT;
                    pushwarning = true;
                }
            }
            else if (ct.colWidth == 8)
            {
                if (intVal < MIN_BIGINT)
                {
                    intVal = MIN_BIGINT;
                    pushwarning = true;
                }
            }

            break;

        default:
            break;
    }

    // @ bug 3285 make sure the value is in precision range for decimal data type
    if ( (ct.colDataType == CalpontSystemCatalog::DECIMAL) ||
            (ct.colDataType == CalpontSystemCatalog::UDECIMAL) ||
            (ct.scale > 0))
    {
        int64_t rangeUp = columnstore_precision[ct.precision];
        int64_t rangeLow = -rangeUp;

        if (intVal > rangeUp)
        {
            intVal = rangeUp;
            pushwarning = true;
        }
        else if (intVal < rangeLow)
        {
            intVal = rangeLow;
            pushwarning = true;
        }
    }

    return intVal;
}

uint64_t number_uint_value(const string& data,
                           const CalpontSystemCatalog::ColType& ct,
                           bool& pushwarning,
                           bool  noRoundup)
{
    // copy of the original input
    string valStr(data);

    // in case, the values are in parentheses
    string::size_type x = valStr.find('(');
    string::size_type y = valStr.find(')');

    while (x < string::npos)
    {
        // erase y first
        if (y == string::npos)
            throw QueryDataExcept("'(' is not matched.", formatErr);

        valStr.erase(y, 1);
        valStr.erase(x, 1);
        x = valStr.find('(');
        y = valStr.find(')');
    }

    if (y != string::npos)
        throw QueryDataExcept("')' is not matched.", formatErr);

    // convert to fixed-point notation if input is in scientific notation
    if (valStr.find('E') < string::npos || valStr.find('e') < string::npos)
    {
        size_t epos = valStr.find('E');

        if (epos == string::npos)
            epos = valStr.find('e');

        // get the coefficient
        string coef = valStr.substr(0, epos);
        // get the exponent
        string exp = valStr.substr(epos + 1);
        bool overflow = false;
        int64_t exponent = dataconvert::string_to_ll(exp, overflow);

        // if the exponent can not be held in 64-bit, not supported or saturated.
        if (overflow)
            throw QueryDataExcept("value is invalid.", formatErr);

        // find the optional "." point
        size_t dpos = coef.find('.');

        if (dpos != string::npos)
        {
            // move "." to the end by mutiply 10 ** (# of fraction digits)
            coef.erase(dpos, 1);
            exponent -= coef.length() - dpos;
        }

        if (exponent >= 0)
        {
            coef.resize(coef.length() + exponent, '0');
        }
        else
        {
            size_t bpos = coef.find_first_of("0123456789");
            size_t epos = coef.length();
            size_t mpos = -exponent;
            dpos = epos - mpos;
            int64_t padding = (int64_t)mpos - (int64_t)(epos - bpos);

            if (padding > 0)
            {
                coef.insert(bpos, padding, '0');
                dpos = bpos;
            }

            coef.insert(dpos, ".");
        }

        valStr = coef;
    }

    // now, convert to uint64_t
    string intStr(valStr);
    string frnStr = "";
    size_t dp = valStr.find('.');

    if (dp != string::npos)
    {
        intStr.erase(dp);
        frnStr = valStr.substr(dp + 1);

        if ( intStr.length() == 0 )
            intStr = "0";
        else if (( intStr.length() == 1 ) && ( (intStr[0] == '+') || (intStr[0] == '-') ) )
        {
            intStr.insert( 1, 1, '0');
        }
    }

    uint64_t uintVal = dataconvert::string_to_ull(intStr, pushwarning);

    bool dummy = false;
    uint64_t frnVal = (frnStr.length() > 0) ? dataconvert::string_to_ull(frnStr, dummy) : 0;

    if (frnVal != 0)
        pushwarning = true;

    switch (ct.colDataType)
    {
        case CalpontSystemCatalog::UTINYINT:
            if (uintVal > MAX_UTINYINT)
            {
                uintVal = MAX_UTINYINT;
                pushwarning = true;
            }

            break;

        case CalpontSystemCatalog::USMALLINT:
            if (uintVal > MAX_USMALLINT)
            {
                uintVal = MAX_USMALLINT;
                pushwarning = true;
            }

            break;

        case CalpontSystemCatalog::UMEDINT:
            if (uintVal > MAX_UMEDINT)
            {
                uintVal = MAX_UMEDINT;
                pushwarning = true;
            }

            break;

        case CalpontSystemCatalog::UINT:
            if (uintVal > MAX_UINT)
            {
                uintVal = MAX_UINT;
                pushwarning = true;
            }

            break;

        case CalpontSystemCatalog::UBIGINT:
            if (uintVal > MAX_UBIGINT)
            {
                uintVal = MAX_UBIGINT;
                pushwarning = true;
            }

            break;

        default:
            break;
    }

    return uintVal;
}

} // namespace anon

namespace dataconvert
{

/**
 * This function reads a decimal value from a string.  It will stop processing
 * in 3 cases:
 *     1) end of input string (null-terminated)
 *     2) non-digit hit
 *     3) max characters read (if max != 0 then at most max characters read)
 *
 * It's up to the caller to figure out whether an error occurred based on
 * their definition of an error and how many characters were read
 */
uint32_t readDecimal( const char*& str, int32_t& value, uint32_t max = 0 )
{
    value = 0;
    uint32_t numread = 0;

    while ( (!max || numread < max) && *str && isdigit(*str) )
    {
        value = value * 10 + ((*str) - '0');
        ++numread;
        ++str;
    }

    return numread;
}

bool mysql_str_to_datetime( const string& input, DateTime& output, bool& isDate )
{
    /**
     *  First we are going to identify the stop/start of the date portion.
     *  The rules are:
     *      - Date portion must come before anything else
     *      - Date portion may only contain numbers and '-'
     *      - Date portion ends with ' ', 'T', or '\0'
     *      - Date portion always starts with Year
     *      - Without date separators ('-'):
     *            YYMMDD
     *            YYYYMMDD
     *      - With date separators there are no specific field length
     *        requirements
     */
    int32_t datesepct = 0;
    uint32_t dtend = 0;

    for ( ; dtend < input.length(); ++dtend )
    {
        char c = input[dtend];

        if ( isdigit( c ) )
        {
            continue;
        }
//		else if( dtend != 0 && c == '-' )
        else if ( dtend != 0 && ispunct(c) )
        {
            ++datesepct;
        }
        else if ( c == 'T' || c == ' ' )
        {
            break;
        }
        else
        {
            // some other character showed up
            output.reset();
            return false;
        }
    }

    int32_t year = -1;
    int32_t mon = -1;
    int32_t day = -1;
    const char* ptr = input.c_str();

    if ( datesepct == 0 )
    {
        if ( dtend == 6 || dtend == 12 )
        {
            readDecimal(ptr, year, 2);
            readDecimal(ptr, mon, 2);
            readDecimal(ptr, day, 2);
            year += 2000;

            if ( year > 2069 )
                year -= 100;

            if ( dtend == 12 )
                dtend -= 6;
        }
        else if ( dtend == 8 || dtend == 14 )
        {
            readDecimal(ptr, year, 4);
            readDecimal(ptr, mon, 2);
            readDecimal(ptr, day, 2);

            if ( dtend == 14 )
                dtend -= 6;
        }
        else
        {
            output.reset();
            return false;
        }
    }
    else if ( datesepct == 2 )
    {
        uint32_t numread = readDecimal(ptr, year);

        if ( numread == 2 )
        {
            // special handling if we read a 2-byte year
            year += 2000;

            if ( year > 2069 )
                year -= 100;
        }

        ++ptr; // skip one separator
        readDecimal(ptr, mon);
        ++ptr; // skip one separator
        readDecimal(ptr, day); // skip two separators
    }
    else
    {
        output.reset();
        return false;
    }

    if (!isDateValid(day, mon, year))
    {
        output.reset();
        return false;
    }

    output.year = year;
    output.month = mon;
    output.day = day;

    /**
     *  Now we need to deal with the time portion.
     *  The rules are:
     *      - Time portion may be empty
     *      - Time portion may start with 'T'
     *      - Time portion always ends with '\0'
     *      - Time portion always starts with hour
     *      - Without time separators (':'):
     *            HHMMSS
     *      - All Times can end with option .[microseconds]
     *      - With time separators there are no specific field length
     *        requirements
     */
    while ( input[dtend] == ' ' && dtend < input.length() )
    {
        ++dtend;
    }

    if ( dtend == input.length() )
    {
        isDate = true;
        return true;
    }

    uint32_t timesep_ct = 0;
    bool has_usec = false;
    uint32_t len_before_msec = 0;
    uint32_t tmstart = ( input[dtend] == ' ' || input[dtend] == 'T' ) ? dtend + 1 : dtend;
    uint32_t tmend = tmstart;

    for ( ; tmend < input.length(); ++tmend )
    {
        char c = input[tmend];

        if ( isdigit( c ) )
        {
            // digits always ok
            continue;
        }
//		else if( c == ':' )
//		{
//			timesep_ct++;
//		}
//		else if( c == '.' )
//		{
//			len_before_msec = ( tmend - tmstart );
//			has_usec = true;
//		}
        else if ( ispunct(c) )
        {
            if ( c == '.' && timesep_ct == 2 )
            {
                len_before_msec = ( tmend - tmstart );
                has_usec = true;
            }
            else
            {
                timesep_ct++;
            }
        }
        else
        {
            // some other character showed up
            output.reset();
            return false;
        }
    }

    if ( !len_before_msec )
        len_before_msec = ( tmend - tmstart );

    int32_t hour = -1;
    int32_t min = 0;
    int32_t sec = 0;
    int32_t usec = 0;
    const char* tstart = input.c_str() + tmstart;

    if ( timesep_ct == 2 )
    {
        readDecimal(tstart, hour);
        ++tstart; // skip one separator
        readDecimal(tstart, min);
        ++tstart; // skip one separator
        readDecimal(tstart, sec);
    }
    else if ( timesep_ct == 1 )
    {
        readDecimal(tstart, hour);
        ++tstart; // skip one separator
        readDecimal(tstart, min);
    }
    else if ( timesep_ct == 0 && len_before_msec == 6 )
    {
        readDecimal(tstart, hour, 2);
        readDecimal(tstart, min, 2);
        readDecimal(tstart, sec, 2);
    }
    else if ( timesep_ct == 0 && len_before_msec == 4 )
    {
        readDecimal(tstart, hour, 2);
        readDecimal(tstart, min, 2);
    }
    else if ( timesep_ct == 0 && len_before_msec == 2 )
    {
        readDecimal(tstart, hour, 2);
    }
    else
    {
        output.reset();
        return false;
    }

    if ( has_usec )
    {
        ++tstart; // skip '.' character.  We could error check if we wanted to
        uint32_t numread = readDecimal(tstart, usec);

        if ( numread > 6 || numread < 1 )
        {
            // don't allow more than 6 digits when specifying microseconds
            output.reset();
            return false;
        }

        // usec have to be scaled up so that it always represents microseconds
        for ( int i = numread; i < 6; i++ )
            usec *= 10;
    }

    if ( !isDateTimeValid( hour, min, sec, usec ) )
    {
        output.reset();
        return false;
    }

    output.hour = hour;
    output.minute = min;
    output.second = sec;
    output.msecond = usec;
    isDate = false;
    return true;
}

bool mysql_str_to_time( const string& input, Time& output, long decimals )
{
    uint32_t dtend = 0;
    bool isNeg = false;

    /**
     *  We need to deal with the time portion.
     *  The rules are:
     *      - Time portion always ends with '\0'
     *      - Time portion always starts with hour
     *      - Without time separators (':'):
     *            HHMMSS
     *      - All Times can end with option .[microseconds]
     *      - With time separators there are no specific field length
     *        requirements
     */
    while ( input[dtend] == ' ' && dtend < input.length() )
    {
        ++dtend;
    }

    if ( dtend == input.length() )
    {
        return false;
    }

    uint32_t timesep_ct = 0;
    bool has_usec = false;
    uint32_t len_before_msec = 0;
    uint32_t tmstart = dtend;
    uint32_t tmend = tmstart;

    for ( ; tmend < input.length(); ++tmend )
    {
        char c = input[tmend];

        if ( isdigit( c ) )
        {
            // digits always ok
            continue;
        }
//		else if( c == ':' )
//		{
//			timesep_ct++;
//		}
//		else if( c == '.' )
//		{
//			len_before_msec = ( tmend - tmstart );
//			has_usec = true;
//		}
        else if ( ispunct(c) )
        {
            if ( c == '.' && timesep_ct == 2 )
            {
                len_before_msec = ( tmend - tmstart );
                has_usec = true;
            }
            else if (c == '-' && (tmend == tmstart))
            {
                isNeg = true;
                ++tmstart;
            }
            else
            {
                timesep_ct++;
            }
        }
        else
        {
            // some other character showed up
            output.reset();
            return false;
        }
    }

    if ( !len_before_msec )
        len_before_msec = ( tmend - tmstart );

    int32_t hour = -1;
    int32_t min = 0;
    int32_t sec = 0;
    int32_t usec = 0;
    const char* tstart = input.c_str() + tmstart;

    if ( timesep_ct == 2 )
    {
        readDecimal(tstart, hour);
        ++tstart; // skip one separator
        readDecimal(tstart, min);
        ++tstart; // skip one separator
        readDecimal(tstart, sec);
    }
    else if ( timesep_ct == 1 )
    {
        readDecimal(tstart, hour);
        ++tstart; // skip one separator
        readDecimal(tstart, min);
    }
    else if ( timesep_ct == 0 && len_before_msec == 6 )
    {
        readDecimal(tstart, hour, 2);
        readDecimal(tstart, min, 2);
        readDecimal(tstart, sec, 2);
    }
    else if ( timesep_ct == 0 && len_before_msec == 4 )
    {
        readDecimal(tstart, hour, 2);
        readDecimal(tstart, min, 2);
    }
    else if ( timesep_ct == 0 && len_before_msec == 2 )
    {
        readDecimal(tstart, hour, 2);
    }
    else
    {
        output.reset();
        return false;
    }

    if ( has_usec )
    {
        ++tstart; // skip '.' character.  We could error check if we wanted to
        uint32_t numread = readDecimal(tstart, usec);

        if ( numread > 6 || numread < 1 )
        {
            // don't allow more than 6 digits when specifying microseconds
            output.reset();
            return false;
        }

        // usec have to be scaled up so that it always represents microseconds
        for ( int i = numread; i < 6; i++ )
            usec *= 10;
    }

    if ( !isTimeValid( hour, min, sec, usec ) )
    {
        // Emulate MariaDB's time saturation
        // TODO: msec saturation
        if ((hour > 838) && !isNeg)
        {
            output.hour = 838;
            output.minute = 59;
            output.second = 59;
            output.msecond = exp10(decimals) - 1;
            output.is_neg = 0;
        }
        else if ((hour < -838) || ((hour > 838) && isNeg))
        {
            output.hour = -838;
            output.minute = 59;
            output.second = 59;
            output.msecond = exp10(decimals) - 1;
            output.is_neg = 1;
        }
        // If neither of the above match then we return a 0 time
        else
        {
            output.reset();
        }

        return false;
    }

    output.hour = isNeg ? 0 - hour : hour;
    output.minute = min;
    output.second = sec;
    output.msecond = usec;
    output.is_neg = isNeg;
    return true;
}

bool stringToDateStruct( const string& data, Date& date )
{
    bool isDate;
    DateTime dt;

    if ( !mysql_str_to_datetime( data, dt, isDate ))
        return false;

    date.year = dt.year;
    date.month = dt.month;
    date.day = dt.day;
    return true;
}

bool stringToDatetimeStruct(const string& data, DateTime& dtime, bool* date)
{
    bool isDate;

    if ( !mysql_str_to_datetime( data, dtime, isDate ) )
        return false;

    if ( isDate )
    {
        if (date)
            *date = true;

        dtime.hour = 0;
        dtime.minute = 0;
        dtime.second = 0;
        dtime.msecond = 0;
    }

    return true;
}

bool stringToTimeStruct(const string& data, Time& dtime, long decimals)
{
    if ( !mysql_str_to_time( data, dtime, decimals ) )
        return false;

    return true;
}

bool stringToTimestampStruct(const string& data, TimeStamp& timeStamp, const string& timeZone)
{
    // special handling for 0000-00-00 00:00:00
    // "0" is sent by the server when checking for default value
    // in the DDL. This is equivalent of 0000-00-00 00:00:00
    if (data.substr(0, 19) == "0000-00-00 00:00:00" || data == "0")
    {
        timeStamp.second = 0;
        timeStamp.msecond = 0;
        return true;
    }

    // for alter table add column <columnname> timestamp,
    // if the table is non-empty, then columnstore will apply
    // default value to populate the new column
    if (data == "current_timestamp() ON UPDATE current_timestamp()")
    {
        struct timeval tv;
        gettimeofday(&tv, 0);
        timeStamp.second = tv.tv_sec;
        timeStamp.msecond = tv.tv_usec;
        return true;
    }

    bool isDate;

    DateTime dtime;

    if ( !mysql_str_to_datetime( data, dtime, isDate ) )
    {
        timeStamp.reset();
        return false;
    }

    if ( isDate )
    {
        dtime.hour = 0;
        dtime.minute = 0;
        dtime.second = 0;
        dtime.msecond = 0;
    }

    MySQLTime m_time;
    m_time.year = dtime.year;
    m_time.month = dtime.month;
    m_time.day = dtime.day;
    m_time.hour = dtime.hour;
    m_time.minute = dtime.minute;
    m_time.second = dtime.second;
    m_time.second_part = dtime.msecond;

    bool isValid = true;
    int64_t seconds = mySQLTimeToGmtSec(m_time, timeZone, isValid);

    if (!isValid)
    {
        timeStamp.reset();
        return false;
    }

    timeStamp.second = seconds;
    timeStamp.msecond = m_time.second_part;

    return true;

}

// WIP 
#include <stdio.h>
using int128_t = __int128;
using uint128_t = unsigned __int128;

struct uint128_pod
{ 
  uint64_t lo;
  uint64_t hi;
};

// WIP MCOL-641
void DataConvert::toString(unsigned __int128 i, char *p)
{ 
  uint64_t div = 10000000000000000000ULL;
  size_t div_log = 19;
  uint128_t high = i;
  uint128_t low;
  low = high % div;
  high /= div;
  uint128_t mid;
  mid = high % div;
  high /= div;
  
  uint128_pod *high_pod = reinterpret_cast<uint128_pod*>(&high);
  uint128_pod *mid_pod = reinterpret_cast<uint128_pod*>(&mid);
  uint128_pod *low_pod = reinterpret_cast<uint128_pod*>(&low);
  int printed_chars = 0;
 
  // WIP replace snprintf with streams 
  if (high_pod->lo != 0) {
    printed_chars = snprintf(p, div_log+1, "%lu", high_pod->lo);
    p += printed_chars; 
    printed_chars = snprintf(p, div_log+1, "%019lu", mid_pod->lo);
    p += printed_chars;  
  } else if (mid_pod->lo != 0) {
    printed_chars = snprintf(p, div_log+1, "%lu", mid_pod->lo);
    p += printed_chars;
  }
  snprintf(p, div_log+1, "%019lu", low_pod->lo);
}


// WIP MCOL-641
// Template this
// result must be calloc-ed
void atoi_(const string &arg, int128_t &res, size_t &size) 
{
    // WIP
    //char buf[40];
    //int128_t *res_ptr = reinterpret_cast<int128_t*>(result);
    res = 0;
    for (size_t j = 0; j < arg.size(); j++) 
    {
        // WIP
        res = res*10 + arg[j] - '0';
    }
    //toString(res, buf);
    //std::cerr << "atoi_ " << buf <<endl;
    //*res_ptr = res;
    size = 16;
}

// WIP MCOL-641
void DataConvert::decimalToString(unsigned __int128 int_val, uint8_t scale, char* buf, unsigned int buflen,
                                         execplan::CalpontSystemCatalog::ColDataType colDataType)
{
    toString(int_val, buf);

    // Biggest ColumnStore supports is DECIMAL(38,x), or 38 total digits+dp+sign for column

    if (scale == 0)
        return;

    //we want to move the last scale chars right by one spot to insert the dp
    //we want to move the trailing null as well, so it's really scale+1 chars
    size_t l1 = strlen(buf);
    char* ptr = &buf[0];

    if (int_val < 0)
    {
        ptr++;
        idbassert(l1 >= 2);
        l1--;
    }

    //need to make sure we have enough leading zeros for this to work...
    //at this point scale is always > 0
    size_t l2 = 1;

    if ((unsigned)scale > l1)
    {
        const char* zeros = "00000000000000000000000000000000000000"; //38 0's
        size_t diff = 0;

        if (int_val != 0)
            diff = scale - l1; //this will always be > 0
        else
            diff = scale;

        memmove((ptr + diff), ptr, l1 + 1); //also move null
        memcpy(ptr, zeros, diff);

        if (int_val != 0)
            l1 = 0;
        else
            l1 = 1;
    }
    else if ((unsigned)scale == l1)
    {
        l1 = 0;
        l2 = 2;
    }
    else
    {
        l1 -= scale;
    }

    memmove((ptr + l1 + l2), (ptr + l1), scale + 1); //also move null

    if (l2 == 2)
        *(ptr + l1++) = '0';

    *(ptr + l1) = '.';
}

boost::any
DataConvert::convertColumnData(const CalpontSystemCatalog::ColType& colType,
                               const std::string& dataOrig, bool& pushWarning, const std::string& timeZone, bool nulFlag, bool noRoundup, bool isUpdate)
{
    boost::any value;
    // WIP
    std::string data( dataOrig );
    pushWarning = false;
    CalpontSystemCatalog::ColDataType type = colType.colDataType;

    //if ( !data.empty() )
    if (!nulFlag)
    {
        switch (type)
        {
            case CalpontSystemCatalog::BIT:
            {
                unsigned int x = data.find("(");

                if (x <= data.length())
                {
                    data.replace ( x, 1, " ");
                }

                x = data.find(")");

                if (x <= data.length())
                {
                    data.replace (x, 1, " ");
                }

                if (number_int_value (data, colType, pushWarning, noRoundup))
                {
                    bool bitvalue;

                    if (from_string<bool>(bitvalue, data, std::dec ))
                    {
                        value = bitvalue;
                    }
                    else
                    {
                        throw QueryDataExcept("range, valid value or conversion error on BIT type.", formatErr);
                    }
                }
            }
            break;

            case CalpontSystemCatalog::TINYINT:
                value = (char) number_int_value(data, colType, pushWarning, noRoundup);
                break;

            case CalpontSystemCatalog::SMALLINT:
                value = (short) number_int_value(data, colType, pushWarning, noRoundup);
                break;

            case CalpontSystemCatalog::MEDINT:
            case CalpontSystemCatalog::INT:
                value = (int) number_int_value(data, colType, pushWarning, noRoundup);
                break;

            case CalpontSystemCatalog::BIGINT:
                value = (long long) number_int_value(data, colType, pushWarning, noRoundup);
                break;

            // MCOL-641 WIP
            case CalpontSystemCatalog::DECIMAL:
                if (colType.colWidth == 16)
                {
                    //value = data;
                    size_t size;
                    int128_t bigint;
                    atoi_(data, bigint, size);
                    value = bigint;
                }
                else if (colType.colWidth == 1)
                    value = (char) number_int_value(data, colType, pushWarning, noRoundup);
                else if (colType.colWidth == 2)
                    value = (short) number_int_value(data, colType, pushWarning, noRoundup);
                else if (colType.colWidth == 4)
                    value = (int) number_int_value(data, colType, pushWarning, noRoundup);
                else if (colType.colWidth == 8)
                    value = (long long) number_int_value(data, colType, pushWarning, noRoundup);
                else if (colType.colWidth == 32)
                    value = data;

                break;
            // MCOL-641 Implement UDECIMAL
            case CalpontSystemCatalog::UDECIMAL:

                // UDECIMAL numbers may not be negative
                if (colType.colWidth == 1)
                {
                    char ival = (char) number_int_value(data, colType, pushWarning, noRoundup);

                    if (ival < 0 &&
                            ival != static_cast<int8_t>(joblist::TINYINTEMPTYROW) &&
                            ival != static_cast<int8_t>(joblist::TINYINTNULL))
                    {
                        ival = 0;
                        pushWarning = true;
                    }

                    value = ival;
                }
                else if (colType.colWidth == 2)
                {
                    short ival = (short) number_int_value(data, colType, pushWarning, noRoundup);

                    if (ival < 0 &&
                            ival != static_cast<int16_t>(joblist::SMALLINTEMPTYROW) &&
                            ival != static_cast<int16_t>(joblist::SMALLINTNULL))
                    {
                        ival = 0;
                        pushWarning = true;
                    }

                    value = ival;
                }
                else if (colType.colWidth == 4)
                {
                    int ival = static_cast<int>(number_int_value(data, colType, pushWarning, noRoundup));

                    if (ival < 0 &&
                            ival != static_cast<int>(joblist::INTEMPTYROW) &&
                            ival != static_cast<int>(joblist::INTNULL))
                    {
                        ival = 0;
                        pushWarning = true;
                    }

                    value = ival;
                }
                else if (colType.colWidth == 8)
                {
                    long long ival = static_cast<long long>(number_int_value(data, colType, pushWarning, noRoundup));

                    if (ival < 0 &&
                            ival != static_cast<long long>(joblist::BIGINTEMPTYROW) &&
                            ival != static_cast<long long>(joblist::BIGINTNULL))
                    {
                        ival = 0;
                        pushWarning = true;
                    }

                    value = ival;
                }

                break;

            case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT:
            {
                string::size_type x = data.find('(');

                if (x < string::npos)
                    data.erase(x, 1);

                x = data.find(')');

                if (x < string::npos)
                    data.erase(x, 1);

                if ( number_value ( data ) )
                {
                    float floatvalue;
                    errno = 0;
#ifdef _MSC_VER
                    double dval = strtod(data.c_str(), 0);

                    if (dval > MAX_FLOAT)
                    {
                        pushWarning = true;
                        floatvalue = MAX_FLOAT;
                    }
                    else if (dval < MIN_FLOAT)
                    {
                        pushWarning = true;
                        floatvalue = MIN_FLOAT;
                    }
                    else
                    {
                        floatvalue = (float)dval;
                    }

#else
                    floatvalue = strtof(data.c_str(), 0);
#endif

                    if (errno == ERANGE)
                    {
                        pushWarning = true;
#ifdef _MSC_VER

                        if ( abs(floatvalue) == HUGE_VAL )
#else
                        if ( abs(floatvalue) == HUGE_VALF )
#endif
                        {
                            if ( floatvalue > 0 )
                                floatvalue = MAX_FLOAT;
                            else
                                floatvalue = MIN_FLOAT;
                        }
                        else
                            floatvalue = 0;
                    }

                    if (floatvalue < 0.0 && type == CalpontSystemCatalog::UFLOAT &&
                            floatvalue != joblist::FLOATEMPTYROW && floatvalue != joblist::FLOATNULL)
                    {
                        value = 0.0;
                        pushWarning = true;
                    }

                    value = floatvalue;
                }
                else
                    throw QueryDataExcept("range, valid value or conversion error on FLOAT type.", formatErr);
            }
            break;

            case CalpontSystemCatalog::DOUBLE:
            case CalpontSystemCatalog::UDOUBLE:
            {
                string::size_type x = data.find('(');

                if (x < string::npos)
                    data.erase(x, 1);

                x = data.find(')');

                if (x < string::npos)
                    data.erase(x, 1);

                if ( number_value ( data ) )
                {
                    double doublevalue;
                    errno = 0;
                    doublevalue = strtod(data.c_str(), 0);

                    if (errno == ERANGE)
                    {
                        pushWarning = true;
#ifdef _MSC_VER

                        if ( abs(doublevalue) == HUGE_VAL )
#else
                        if ( abs(doublevalue) == HUGE_VALL )
#endif
                        {
                            if ( doublevalue > 0 )
                                value = MAX_DOUBLE;
                            else
                                value = MIN_DOUBLE;
                        }
                        else
                            value = 0;
                    }
                    else
                        value = doublevalue;

                    if (doublevalue < 0.0 && type == CalpontSystemCatalog::UDOUBLE &&
                            doublevalue != joblist::DOUBLEEMPTYROW && doublevalue != joblist::DOUBLENULL)
                    {
                        doublevalue = 0.0;
                        pushWarning = true;
                    }
                }
                else
                {
                    throw QueryDataExcept("range, valid value or conversion error on DOUBLE type.", formatErr);
                }
            }
            break;

            case CalpontSystemCatalog::UTINYINT:
                value = (uint8_t)number_uint_value(data, colType, pushWarning, noRoundup);
                break;

            case CalpontSystemCatalog::USMALLINT:
                value = (uint16_t)number_uint_value(data, colType, pushWarning, noRoundup);
                break;

            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
                value = (uint32_t)number_uint_value(data, colType, pushWarning, noRoundup);
                break;

            case CalpontSystemCatalog::UBIGINT:
                value = (uint64_t)number_uint_value(data, colType, pushWarning, noRoundup);
                break;

            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::VARCHAR:
            case CalpontSystemCatalog::TEXT:
            {
                //check data length
                if ( data.length() > (unsigned int)colType.colWidth )
                {
                    data = data.substr(0, colType.colWidth);
                    pushWarning = true;
                }
                else
                {
                    if ( (unsigned int)colType.colWidth > data.length())
                    {
                        //Pad null character to the string
                        data.resize(colType.colWidth, 0);
                    }
                }

                value = data;
            }
            break;

            case CalpontSystemCatalog::DATE:
            {
                Date aDay;

                if (stringToDateStruct(data, aDay))
                {
                    value = (*(reinterpret_cast<uint32_t*> (&aDay)));
                }
                else
                {
                    value = (uint32_t) 0;
                    pushWarning = true;
                }
            }
            break;

            case CalpontSystemCatalog::DATETIME:
            {
                DateTime aDatetime;

                if (stringToDatetimeStruct(data, aDatetime, 0))
                {
                    value = *(reinterpret_cast<uint64_t*>(&aDatetime));
                }
                else
                {
                    value = (uint64_t) 0;
                    pushWarning = true;
                }
            }
            break;

            case CalpontSystemCatalog::TIME:
            {
                Time aTime;

                if (!stringToTimeStruct(data, aTime, colType.precision))
                {
                    pushWarning = true;
                }

                value = (int64_t) * (reinterpret_cast<int64_t*>(&aTime));
            }
            break;

            case CalpontSystemCatalog::TIMESTAMP:
            {
                TimeStamp aTimestamp;

                if (!stringToTimestampStruct(data, aTimestamp, timeZone))
                {
                    pushWarning = true;
                }

                value = (uint64_t) *(reinterpret_cast<uint64_t*>(&aTimestamp));
            }
            break;

            case CalpontSystemCatalog::BLOB:
            case CalpontSystemCatalog::CLOB:
                value = data;
                break;

            case CalpontSystemCatalog::VARBINARY:
                value = data;
                break;

            case CalpontSystemCatalog::BINARY:
                value = data;
                break;

            default:
                throw QueryDataExcept("convertColumnData: unknown column data type.", dataTypeErr);
                break;
        }
    }
    else									//null
    {
        switch (type)
        {
            case CalpontSystemCatalog::BIT:
            {
                //TODO: How to communicate with write engine?
            }
            break;

            case CalpontSystemCatalog::TINYINT:
            {
                char tinyintvalue = joblist::TINYINTNULL;
                value = tinyintvalue;
            }
            break;

            case CalpontSystemCatalog::SMALLINT:
            {
                short smallintvalue = joblist::SMALLINTNULL;
                value = smallintvalue;
            }
            break;

            case CalpontSystemCatalog::MEDINT:
            case CalpontSystemCatalog::INT:
            {
                int intvalue = joblist::INTNULL;
                value = intvalue;
            }
            break;

            case CalpontSystemCatalog::BIGINT:
            {
                long long bigint = joblist::BIGINTNULL;
                value = bigint;
            }
            break;

            case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
            {
                if (colType.colWidth == CalpontSystemCatalog::ONE_BYTE)
                {
                    char tinyintvalue = joblist::TINYINTNULL;
                    value = tinyintvalue;
                }
                else if (colType.colWidth == CalpontSystemCatalog::TWO_BYTE)
                {
                    short smallintvalue = joblist::SMALLINTNULL;
                    value = smallintvalue;
                }
                else if (colType.colWidth == CalpontSystemCatalog::FOUR_BYTE)
                {
                    int intvalue = joblist::INTNULL;
                    value = intvalue;
                }
                else if (colType.colWidth == CalpontSystemCatalog::EIGHT_BYTE)
                {
                    long long eightbyte = joblist::BIGINTNULL;
                    value = eightbyte;
                }
                else
                {
                    WriteEngine::Token nullToken;
                    value = nullToken;
                }
            }
            break;

            case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT:
            {
                uint32_t tmp = joblist::FLOATNULL;
                float* floatvalue = (float*)&tmp;
                value = *floatvalue;
            }
            break;

            case CalpontSystemCatalog::DOUBLE:
            case CalpontSystemCatalog::UDOUBLE:
            {
                uint64_t tmp = joblist::DOUBLENULL;
                double* doublevalue = (double*)&tmp;
                value = *doublevalue;
            }
            break;

            case CalpontSystemCatalog::DATE:
            {
                uint32_t d = joblist::DATENULL;
                value = d;
            }
            break;

            case CalpontSystemCatalog::DATETIME:
            {
                uint64_t d = joblist::DATETIMENULL;
                value = d;
            }
            break;

            case CalpontSystemCatalog::TIMESTAMP:
            {
                uint64_t d = joblist::TIMESTAMPNULL;
                value = d;
            }
            break;

            case CalpontSystemCatalog::TIME:
            {
                uint64_t d = joblist::TIMENULL;
                value = d;
            }
            break;

            case CalpontSystemCatalog::CHAR:
            {
                std::string charnull;

                if (colType.colWidth == 1)
                {
                    //charnull = joblist::CHAR1NULL;
                    charnull = '\376';
                    value = charnull;
                }
                else if (colType.colWidth == 2)
                {
                    //charnull = joblist::CHAR2NULL;
                    charnull = "\377\376";
                    value = charnull;
                }
                else if (( colType.colWidth < 5 ) && ( colType.colWidth > 2 ))
                {
                    //charnull = joblist::CHAR4NULL;
                    charnull = "\377\377\377\376";
                    value = charnull;
                }
                else if (( colType.colWidth < 9 ) && ( colType.colWidth > 4 ))
                {
                    //charnull = joblist::CHAR8NULL;
                    charnull = "\377\377\377\377\377\377\377\376";
                    value = charnull;
                }
                else
                {
                    WriteEngine::Token nullToken;
                    value = nullToken;
                }
            }
            break;

            case CalpontSystemCatalog::VARCHAR:
            case CalpontSystemCatalog::TEXT:
            {
                std::string charnull;

                if (colType.colWidth == 1 )
                {
                    //charnull = joblist::CHAR2NULL;
                    charnull = "\377\376";
                    value = charnull;
                }
                else if ((colType.colWidth < 4)  && (colType.colWidth > 1))
                {
                    //charnull = joblist::CHAR4NULL;
                    charnull = "\377\377\377\376";
                    value = charnull;
                }
                else if ((colType.colWidth < 8)  && (colType.colWidth > 3))
                {
                    //charnull = joblist::CHAR8NULL;
                    charnull = "\377\377\377\377\377\377\377\376";
                    value = charnull;
                }
                else if ( colType.colWidth > 7 )
                {
                    WriteEngine::Token nullToken;
                    value = nullToken;
                }
            }
            break;

            case CalpontSystemCatalog::VARBINARY:
            case CalpontSystemCatalog::BLOB:
            {
                WriteEngine::Token nullToken;
                value = nullToken;
            }
            break;

            case CalpontSystemCatalog::BINARY:
            {
                value = data;
            }
            break;
            
            case CalpontSystemCatalog::UTINYINT:
            {
                uint8_t utinyintvalue = joblist::UTINYINTNULL;
                value = utinyintvalue;
            }
            break;

            case CalpontSystemCatalog::USMALLINT:
            {
                uint16_t usmallintvalue = joblist::USMALLINTNULL;
                value = usmallintvalue;
            }
            break;

            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            {
                uint32_t uintvalue = joblist::UINTNULL;
                value = uintvalue;
            }
            break;

            case CalpontSystemCatalog::UBIGINT:
            {
                uint64_t ubigint = joblist::UBIGINTNULL;
                value = ubigint;
            }
            break;

            default:
                throw QueryDataExcept("convertColumnData: unknown column data type.", dataTypeErr);
                break;

        }
    }

    return value;
}

//------------------------------------------------------------------------------
// Convert date string to binary date.  Used by BulkLoad.
//------------------------------------------------------------------------------
int32_t DataConvert::convertColumnDate(
    const char* dataOrg,
    CalpontDateTimeFormat dateFormat,
    int& status,
    unsigned int dataOrgLen )
{
    status = 0;
    const char* p;
    p = dataOrg;
    char fld[10];
    int32_t value = 0;

    if ( dateFormat != CALPONTDATE_ENUM )
    {
        status = -1;
        return value;
    }

    // @bug 5787: allow for leading blanks
    unsigned int dataLen = dataOrgLen;

    if ((dataOrgLen > 0) && (dataOrg[0] == ' '))
    {
        unsigned nblanks = 0;

        for (unsigned nn = 0; nn < dataOrgLen; nn++)
        {
            if (dataOrg[nn] == ' ')
                nblanks++;
            else
                break;
        }

        p       = dataOrg    + nblanks;
        dataLen = dataOrgLen - nblanks;
    }

    if ( dataLen < 10)
    {
        status = -1;
        return value;
    }

    int inYear, inMonth, inDay;
    memcpy( fld, p, 4);
    fld[4] = '\0';

    inYear = strtol(fld, 0, 10);

    memcpy( fld, p + 5, 2);
    fld[2] = '\0';

    inMonth = strtol(fld, 0, 10);

    memcpy( fld, p + 8, 2);
    fld[2] = '\0';

    inDay = strtol(fld, 0, 10);

    if ( isDateValid (inDay, inMonth, inYear))
    {
        Date aDay;
        aDay.year = inYear;
        aDay.month = inMonth;
        aDay.day = inDay;
        memcpy( &value, &aDay, 4);
    }
    else
    {
        status = -1;
    }

    return value;
}

//------------------------------------------------------------------------------
// Verify that specified date is valid
//------------------------------------------------------------------------------
bool DataConvert::isColumnDateValid( int32_t date )
{
    Date d;
    void* dp = static_cast<void*>(&d);
    memcpy(dp, &date, sizeof(int32_t));
    return (isDateValid(d.day, d.month, d.year));
}

//------------------------------------------------------------------------------
// Convert date/time string to binary date/time.  Used by BulkLoad.
//------------------------------------------------------------------------------
int64_t DataConvert::convertColumnDatetime(
    const char* dataOrg,
    CalpontDateTimeFormat datetimeFormat,
    int& status,
    unsigned int dataOrgLen )
{
    status = 0;
    const char* p;
    p = dataOrg;
    char fld[10];
    int64_t value = 0;

    if ( datetimeFormat != CALPONTDATETIME_ENUM )
    {
        status = -1;
        return value;
    }

    // @bug 5787: allow for leading blanks
    unsigned int dataLen = dataOrgLen;

    if ((dataOrgLen > 0) && (dataOrg[0] == ' '))
    {
        unsigned nblanks = 0;

        for (unsigned nn = 0; nn < dataOrgLen; nn++)
        {
            if (dataOrg[nn] == ' ')
                nblanks++;
            else
                break;
        }

        p       = dataOrg    + nblanks;
        dataLen = dataOrgLen - nblanks;
    }

    if ( dataLen < 10)
    {
        status = -1;
        return value;
    }

    int inYear, inMonth, inDay, inHour, inMinute, inSecond, inMicrosecond;
    memcpy( fld, p, 4);
    fld[4] = '\0';

    inYear = strtol(fld, 0, 10);

    memcpy( fld, p + 5, 2);
    fld[2] = '\0';

    inMonth = strtol(fld, 0, 10);

    memcpy( fld, p + 8, 2);
    fld[2] = '\0';

    inDay = strtol(fld, 0, 10);

    inHour        = 0;
    inMinute      = 0;
    inSecond      = 0;
    inMicrosecond = 0;

    if (dataLen > 12)
    {
        // For backwards compatability we still allow leading blank
        if ((!isdigit(p[11]) && (p[11] != ' ')) ||
                !isdigit(p[12]))
        {
            status = -1;
            return value;
        }

        memcpy( fld, p + 11, 2);
        fld[2] = '\0';

        inHour = strtol(fld, 0, 10);

        if (dataLen > 15)
        {
            if (!isdigit(p[14]) || !isdigit(p[15]))
            {
                status = -1;
                return value;
            }

            memcpy( fld, p + 14, 2);
            fld[2] = '\0';

            inMinute = strtol(fld, 0, 10);

            if (dataLen > 18)
            {
                if (!isdigit(p[17]) || !isdigit(p[18]))
                {
                    status = -1;
                    return value;
                }

                memcpy( fld, p + 17, 2);
                fld[2] = '\0';

                inSecond = strtol(fld, 0, 10);

                if (dataLen > 20)
                {
                    unsigned int microFldLen = dataLen - 20;

                    if (microFldLen > (sizeof(fld) - 1))
                        microFldLen = sizeof(fld) - 1;

                    memcpy( fld, p + 20, microFldLen);
                    fld[microFldLen] = '\0';
                    inMicrosecond = strtol(fld, 0, 10);
                }
            }
        }
    }

    if ( isDateValid (inDay, inMonth, inYear) &&
            isDateTimeValid (inHour, inMinute, inSecond, inMicrosecond) )
    {
        DateTime aDatetime;
        aDatetime.year = inYear;
        aDatetime.month = inMonth;
        aDatetime.day = inDay;
        aDatetime.hour = inHour;
        aDatetime.minute = inMinute;
        aDatetime.second = inSecond;
        aDatetime.msecond = inMicrosecond;

        memcpy( &value, &aDatetime, 8);
    }
    else
    {
        status = -1;
    }

    return value;
}

//------------------------------------------------------------------------------
// Convert timestamp string to binary timestamp.  Used by BulkLoad.
// Most of this code is taken from DataConvert::convertColumnDatetime
//------------------------------------------------------------------------------
int64_t DataConvert::convertColumnTimestamp(
    const char* dataOrg,
    CalpontDateTimeFormat datetimeFormat,
    int& status,
    unsigned int dataOrgLen,
    const std::string& timeZone )
{
    char tmbuf[64];
    std::string dataOrgTemp = dataOrg;
    if (dataOrgTemp.substr(0, 19) == "0000-00-00 00:00:00")
    {
        return 0;
    }

    // this is the default value of the first timestamp field in a table,
    // which is stored in the system catalog
    if (strcmp(dataOrg, "current_timestamp() ON UPDATE current_timestamp()") == 0)
    {
        struct timeval tv;
        gettimeofday(&tv, 0);
        MySQLTime time;
        gmtSecToMySQLTime(tv.tv_sec, time, timeZone);
        sprintf(tmbuf, "%04d-%02d-%02d %02d:%02d:%02d.%06ld", time.year, time.month, time.day, time.hour, time.minute, time.second, tv.tv_usec);
        dataOrg = tmbuf;
        dataOrgLen = strlen(tmbuf);
    }

    status = 0;
    const char* p;
    p = dataOrg;
    char fld[10];
    int64_t value = 0;

    if ( datetimeFormat != CALPONTDATETIME_ENUM )
    {
        status = -1;
        return value;
    }

    unsigned int dataLen = dataOrgLen;

    if ((dataOrgLen > 0) && (dataOrg[0] == ' '))
    {
        unsigned nblanks = 0;

        for (unsigned nn = 0; nn < dataOrgLen; nn++)
        {
            if (dataOrg[nn] == ' ')
                nblanks++;
            else
                break;
        }

        p       = dataOrg    + nblanks;
        dataLen = dataOrgLen - nblanks;
    }

    if ( dataLen < 10)
    {
        status = -1;
        return value;
    }

    int inYear, inMonth, inDay, inHour, inMinute, inSecond, inMicrosecond;
    memcpy( fld, p, 4);
    fld[4] = '\0';

    inYear = strtol(fld, 0, 10);

    memcpy( fld, p + 5, 2);
    fld[2] = '\0';

    inMonth = strtol(fld, 0, 10);

    memcpy( fld, p + 8, 2);
    fld[2] = '\0';

    inDay = strtol(fld, 0, 10);

    inHour        = 0;
    inMinute      = 0;
    inSecond      = 0;
    inMicrosecond = 0;

    if (dataLen > 12)
    {
        // For backwards compatability we still allow leading blank
        if ((!isdigit(p[11]) && (p[11] != ' ')) ||
                !isdigit(p[12]))
        {
            status = -1;
            return value;
        }

        memcpy( fld, p + 11, 2);
        fld[2] = '\0';

        inHour = strtol(fld, 0, 10);

        if (dataLen > 15)
        {
            if (!isdigit(p[14]) || !isdigit(p[15]))
            {
                status = -1;
                return value;
            }

            memcpy( fld, p + 14, 2);
            fld[2] = '\0';

            inMinute = strtol(fld, 0, 10);

            if (dataLen > 18)
            {
                if (!isdigit(p[17]) || !isdigit(p[18]))
                {
                    status = -1;
                    return value;
                }

                memcpy( fld, p + 17, 2);
                fld[2] = '\0';

                inSecond = strtol(fld, 0, 10);

                if (dataLen > 20)
                {
                    unsigned int microFldLen = dataLen - 20;

                    if (microFldLen > (sizeof(fld) - 1))
                        microFldLen = sizeof(fld) - 1;

                    memcpy( fld, p + 20, microFldLen);
                    fld[microFldLen] = '\0';
                    inMicrosecond = strtol(fld, 0, 10);
                }
            }
        }
    }

    if ( isDateValid (inDay, inMonth, inYear) &&
            isDateTimeValid (inHour, inMinute, inSecond, inMicrosecond) )
    {
        MySQLTime m_time;
        m_time.year = inYear;
        m_time.month = inMonth;
        m_time.day = inDay;
        m_time.hour = inHour;
        m_time.minute = inMinute;
        m_time.second = inSecond;
        m_time.second_part = inMicrosecond;

        bool isValid = true;
        int64_t seconds = mySQLTimeToGmtSec(m_time, timeZone, isValid);

        if (!isValid)
        {
            status = -1;
            return value;
        }

        TimeStamp timestamp;
        timestamp.second = seconds;
        timestamp.msecond = m_time.second_part;

        memcpy( &value, &timestamp, 8 );
    }
    else
    {
        status = -1;
    }

    return value;
}

//------------------------------------------------------------------------------
// Convert time string to binary time.  Used by BulkLoad.
// Most of this is taken from str_to_time in sql-common/my_time.c
//------------------------------------------------------------------------------
int64_t DataConvert::convertColumnTime(
    const char* dataOrg,
    CalpontDateTimeFormat datetimeFormat,
    int& status,
    unsigned int dataOrgLen )
{
    status = 0;
    char* p;
    char* retp = NULL;
    char* savePoint = NULL;
    p = const_cast<char*>(dataOrg);
    int64_t value = 0;
    int inHour, inMinute, inSecond, inMicrosecond;
    inHour        = 0;
    inMinute      = 0;
    inSecond      = 0;
    inMicrosecond = 0;
    bool isNeg = false;

    if ( datetimeFormat != CALPONTTIME_ENUM )
    {
        status = -1;
        return value;
    }

    if (dataOrgLen == 0)
    {
        return value;
    }

    if (dataOrgLen < 3)
    {
        // Not enough chars to be a time
        status = -1;
        return value;
    }

    if (p[0] == '-')
    {
        isNeg = true;
    }

    errno = 0;

    p = strtok_r(p, ":.", &savePoint);
    inHour = strtol(p, &retp, 10);

    if (errno || !retp)
    {
        status = -1;
        return value;
    }

    p = strtok_r(NULL, ":.", &savePoint);

    if (p == NULL)
    {
        status = -1;
        return value;
    }

    inMinute = strtol(p, &retp, 10);

    if (errno || !retp)
    {
        status = -1;
        return value;
    }

    p = strtok_r(NULL, ":.", &savePoint);

    if (p == NULL)
    {
        status = -1;
        return value;
    }

    inSecond = strtol(p, &retp, 10);

    if (errno || !retp)
    {
        status = -1;
        return value;
    }

    p = strtok_r(NULL, ":.", &savePoint);

    if (p != NULL)
    {
        inMicrosecond = strtol(p, &retp, 10);

        if (errno || !retp)
        {
            status = -1;
            return value;
        }
    }

    if ( isTimeValid (inHour, inMinute, inSecond, inMicrosecond) )
    {
        Time atime;
        atime.hour = inHour;
        atime.minute = inMinute;
        atime.second = inSecond;
        atime.msecond = inMicrosecond;
        atime.is_neg = isNeg;

        memcpy( &value, &atime, 8);
    }
    else
    {
        // Emulate MariaDB's time saturation
        if (inHour > 838)
        {
            Time atime;
            atime.hour = 838;
            atime.minute = 59;
            atime.second = 59;
            atime.msecond = 999999;
            atime.is_neg = false;
            memcpy( &value, &atime, 8);
        }
        else if (inHour < -838)
        {
            Time atime;
            atime.hour = -838;
            atime.minute = 59;
            atime.second = 59;
            atime.msecond = 999999;
            atime.is_neg = false;
            memcpy( &value, &atime, 8);
        }

        // If neither of the above match then we return a 0 time

        status = -1;
    }

    return value;

}


//------------------------------------------------------------------------------
// Verify that specified datetime is valid
//------------------------------------------------------------------------------
bool DataConvert::isColumnDateTimeValid( int64_t dateTime )
{
    DateTime dt;
    void* dtp = static_cast<void*>(&dt);
    memcpy(dtp, &dateTime, sizeof(uint64_t));

    if (isDateValid(dt.day, dt.month, dt.year))
        return isDateTimeValid(dt.hour, dt.minute, dt.second, dt.msecond);

    return false;
}

bool DataConvert::isColumnTimeValid( int64_t time )
{
    Time dt;
    void* dtp = static_cast<void*>(&dt);
    memcpy(dtp, &time, sizeof(uint64_t));

    return isTimeValid(dt.hour, dt.minute, dt.second, dt.msecond);
}

bool DataConvert::isColumnTimeStampValid( int64_t timeStamp )
{
    TimeStamp dt;
    void* dtp = static_cast<void*>(&dt);
    memcpy(dtp, &timeStamp, sizeof(uint64_t));

    return isTimestampValid(dt.second, dt.msecond);
}

std::string DataConvert::dateToString( int  datevalue )
{
    // @bug 4703 abandon multiple ostringstream's for conversion
    Date d(datevalue);
    const int DATETOSTRING_LEN = 12; // YYYY-MM-DD\0
    char buf[DATETOSTRING_LEN];

    sprintf(buf, "%04d-%02d-%02d", d.year, d.month, d.day);
    return buf;
}

std::string DataConvert::datetimeToString( long long  datetimevalue, long decimals )
{
    // 10 is default which means we don't need microseconds
    if (decimals > 6 || decimals < 0)
    {
        decimals = 0;
    }

    // @bug 4703 abandon multiple ostringstream's for conversion
    DateTime dt(datetimevalue);
    const int DATETIMETOSTRING_LEN = 28; // YYYY-MM-DD HH:MM:SS.mmmmmm\0
    char buf[DATETIMETOSTRING_LEN];

    sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d", dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second);

    if (dt.msecond && decimals)
    {
        // Pad start with zeros
        sprintf(buf + strlen(buf), ".%0*d", (int)decimals, dt.msecond);
    }

    return buf;
}

std::string DataConvert::timestampToString( long long  timestampvalue, const std::string& timezone, long decimals )
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

    const int TIMESTAMPTOSTRING_LEN = 28; // YYYY-MM-DD HH:MM:SS.mmmmmm\0
    char buf[TIMESTAMPTOSTRING_LEN];

    sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d", time.year, time.month, time.day, time.hour, time.minute, time.second);

    if (timestamp.msecond && decimals)
    {
        // Pad start with zeros
        sprintf(buf + strlen(buf), ".%0*d", (int)decimals, timestamp.msecond);
    }

    return buf;
}

std::string DataConvert::timeToString( long long  timevalue, long decimals )
{
    // 10 is default which means we don't need microseconds
    if (decimals > 6 || decimals < 0)
    {
        decimals = 0;
    }

    // @bug 4703 abandon multiple ostringstream's for conversion
    Time dt(timevalue);
    const int TIMETOSTRING_LEN = 19; // (-H)HH:MM:SS.mmmmmm\0
    char buf[TIMETOSTRING_LEN];
    char* outbuf = buf;

    if ((dt.hour >= 0) && dt.is_neg)
    {
        outbuf[0] = '-';
        outbuf++;
    }

    sprintf(outbuf, "%02d:%02d:%02d", dt.hour, dt.minute, dt.second);

    if (dt.msecond && decimals)
    {
        // Pad start with zeros
        sprintf(buf + strlen(buf), ".%0*d", (int)decimals, dt.msecond);
    }

    return buf;
}

std::string DataConvert::dateToString1( int  datevalue )
{
    // @bug 4703 abandon multiple ostringstream's for conversion
    Date d(datevalue);
    const int DATETOSTRING1_LEN = 10; // YYYYMMDD\0
    char buf[DATETOSTRING1_LEN];

    sprintf(buf, "%04d%02d%02d", d.year, d.month, d.day);
    return buf;
}

std::string DataConvert::datetimeToString1( long long  datetimevalue )
{
    // @bug 4703 abandon multiple ostringstream's for conversion
    DateTime dt(datetimevalue);
    // Interesting, gcc 7 says the sprintf below generates between 21 and 23 bytes of output.
    const int DATETIMETOSTRING1_LEN = 23; // YYYYMMDDHHMMSSmmmmmm\0
    char buf[DATETIMETOSTRING1_LEN];

    sprintf(buf, "%04d%02d%02d%02d%02d%02d%06d", dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.msecond);
    return buf;
}

std::string DataConvert::timestampToString1( long long  timestampvalue, const std::string& timezone )
{
    const int TIMESTAMPTOSTRING1_LEN = 22; // YYYYMMDDHHMMSSmmmmmm\0
    char buf[TIMESTAMPTOSTRING1_LEN];

    TimeStamp timestamp(timestampvalue);
    int64_t seconds = timestamp.second;
    MySQLTime time;
    gmtSecToMySQLTime(seconds, time, timezone);

    sprintf(buf, "%04d%02d%02d%02d%02d%02d%06d", time.year, time.month, time.day, time.hour, time.minute, time.second, timestamp.msecond);
    return buf;
}

std::string DataConvert::timeToString1( long long  datetimevalue )
{
    // @bug 4703 abandon multiple ostringstream's for conversion
    DateTime dt(datetimevalue);
    const int TIMETOSTRING1_LEN = 14; // HHMMSSmmmmmm\0
    char buf[TIMETOSTRING1_LEN];

    char* outbuf = buf;

    sprintf(outbuf, "%02d%02d%02d%06d", dt.hour, dt.minute, dt.second, dt.msecond);
    return buf;
}

#if 0
bool DataConvert::isNullData(ColumnResult* cr, int rownum, CalpontSystemCatalog::ColType colType)
{
    switch (colType.colDataType)
    {
        case CalpontSystemCatalog::TINYINT:
            if (cr->GetData(rownum) == joblist::TINYINTNULL)
                return true;

            return false;

        case CalpontSystemCatalog::SMALLINT:
            if (cr->GetData(rownum) == joblist::SMALLINTNULL)
                return true;

            return false;

        case CalpontSystemCatalog::MEDINT:
        case CalpontSystemCatalog::INT:
            if (cr->GetData(rownum) == joblist::INTNULL)
                return true;

            return false;

        case CalpontSystemCatalog::BIGINT:
            if (cr->GetData(rownum) == static_cast<int64_t>(joblist::BIGINTNULL))
                return true;

            return false;

        case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL:
        {
            if (colType.colWidth <= CalpontSystemCatalog::FOUR_BYTE)
            {
                if (cr->GetData(rownum) == joblist::SMALLINTNULL)
                    return true;

                return false;
            }
            else if (colType.colWidth <= 9)
            {
                if (cr->GetData(rownum) == joblist::INTNULL)
                    return true;
                else return false;
            }
            else if (colType.colWidth <= 18)
            {
                if (cr->GetData(rownum) == static_cast<int64_t>(joblist::BIGINTNULL))
                    return true;

                return false;
            }
            else
            {
                if (cr->GetStringData(rownum) == "\376\377\377\377\377\377\377\377")
                    return true;

                return false;
            }
        }

        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:

            //if (cr->GetStringData(rownum) == joblist::FLOATNULL)
            if (cr->GetStringData(rownum).compare("null") == 0 )
                return true;

            return false;

        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:

            //if (cr->GetStringData(rownum) == joblist::DOUBLENULL)
            if (cr->GetStringData(rownum).compare("null") == 0 )
                return true;

            return false;

        case CalpontSystemCatalog::DATE:
            if (cr->GetData(rownum) == joblist::DATENULL)
                return true;

            return false;

        case CalpontSystemCatalog::DATETIME:
            if (cr->GetData(rownum) == static_cast<int64_t>(joblist::DATETIMENULL))
                return true;

            return false;

        case CalpontSystemCatalog::CHAR:
        {
            std::string charnull;

            if ( cr->GetStringData(rownum) == "")
            {
                return true;
            }

            if (colType.colWidth == 1)
            {
                if (cr->GetStringData(rownum) == "\376")
                    return true;

                return false;
            }
            else if (colType.colWidth == 2)
            {
                if (cr->GetStringData(rownum) == "\377\376")
                    return true;

                return false;
            }
            else if (( colType.colWidth < 5 ) && ( colType.colWidth > 2 ))
            {
                if (cr->GetStringData(rownum) == "\377\377\377\376")
                    return true;

                return false;
            }
            else if (( colType.colWidth < 9 ) && ( colType.colWidth > 4 ))
            {
                if (cr->GetStringData(rownum) == "\377\377\377\377\377\377\377\376")
                    return true;

                return false;
            }
            else
            {
                if (cr->GetStringData(rownum) == "\376\377\377\377\377\377\377\377")
                    return true;

                return false;
            }
        }

        case CalpontSystemCatalog::VARCHAR:
        {
            std::string charnull;

            if ( cr->GetStringData(rownum) == "")
            {
                return true;
            }

            if (colType.colWidth == 1)
            {
                if (cr->GetStringData(rownum) == "\377\376")
                    return true;

                return false;
            }
            else if ((colType.colWidth < 4)  && (colType.colWidth > 1))
            {
                if (cr->GetStringData(rownum) == "\377\377\377\376")
                    return true;

                return false;
            }
            else if ((colType.colWidth < 8)  && (colType.colWidth > 3))
            {
                if (cr->GetStringData(rownum) == "\377\377\377\377\377\377\377\376")
                    return true;

                return false;
            }
            else
            {
                WriteEngine::Token nullToken;

                // bytes reversed
                if (cr->GetStringData(rownum) == "\376\377\377\377\377\377\377\377")
                    return true;

                return false;
            }
        }

        case CalpontSystemCatalog::UTINYINT:
            if (cr->GetData(rownum) == joblist::UTINYINTNULL)
                return true;

            return false;

        case CalpontSystemCatalog::USMALLINT:
            if (cr->GetData(rownum) == joblist::USMALLINTNULL)
                return true;

            return false;

        case CalpontSystemCatalog::UMEDINT:
        case CalpontSystemCatalog::UINT:
            if (cr->GetData(rownum) == joblist::UINTNULL)
                return true;

            return false;

        case CalpontSystemCatalog::UBIGINT:
            if (cr->GetData(rownum) == joblist::UBIGINTNULL)
                return true;

            return false;

        default:
            throw QueryDataExcept("convertColumnData: unknown column data type.", dataTypeErr);
    }
}
#endif
int64_t DataConvert::dateToInt(const string& date)
{
    return stringToDate(date);
}

int64_t DataConvert::datetimeToInt(const string& datetime)
{
    return stringToDatetime(datetime);
}

int64_t DataConvert::timestampToInt(const string& timestamp, const string& timeZone)
{
    return stringToTimestamp(timestamp, timeZone);
}

int64_t DataConvert::timeToInt(const string& time)
{
    return stringToTime(time);
}

int64_t DataConvert::stringToDate(const string& data)
{
    Date aDay;

    if ( stringToDateStruct( data, aDay ) )
        return (((*(reinterpret_cast<uint32_t*> (&aDay))) & 0xFFFFFFC0) | 0x3E);
    else
        return -1;
}

int64_t DataConvert::stringToDatetime(const string& data, bool* date)
{
    DateTime dtime;

    if ( stringToDatetimeStruct( data, dtime, date ) )
        return *(reinterpret_cast<uint64_t*>(&dtime));
    else
        return -1;
}

int64_t DataConvert::stringToTimestamp(const string& data, const string& timeZone)
{
    TimeStamp aTimestamp;

    if ( stringToTimestampStruct( data, aTimestamp, timeZone ) )
        return *(reinterpret_cast<uint64_t*>(&aTimestamp));
    else
        return -1;
}

/* This is really painful and expensive b/c it seems the input is not normalized or
sanitized.  That should really be done on ingestion. */
int64_t DataConvert::intToDate(int64_t data)
{
    char buf[21] = {0};
    Date aday;

    if (data == 0)
    {
        aday.year = 0;
        aday.month = 0;
        aday.day = 0;
        return *(reinterpret_cast<uint32_t*>(&aday));
    }

    // this snprintf call causes a compiler warning b/c we're potentially copying a 20-digit #
    // into 15 bytes, however, that appears to be intentional.
#if defined(__GNUC__) && __GNUC__ >= 7
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation="
    snprintf( buf, 15, "%llu", (long long unsigned int)data);
#pragma GCC diagnostic pop
#else
    snprintf( buf, 15, "%llu", (long long unsigned int)data);
#endif

    string year, month, day, hour, min, sec, msec;
    int64_t y = 0, m = 0, d = 0, h = 0, minute = 0, s = 0, ms = 0;

    switch (strlen(buf))
    {
        case 14:
            year = string(buf, 4);
            month = string(buf + 4, 2);
            day = string(buf + 6, 2);
            hour = string(buf + 8, 2);
            min = string(buf + 10, 2);
            sec = string(buf + 12, 2);
            msec = string(buf + 14, 6);
            break;

        case 12:
            year = string(buf, 2);
            month = string(buf + 2, 2);
            day = string(buf + 4, 2);
            hour = string(buf + 6, 2);
            min = string(buf + 8, 2);
            sec = string(buf + 10, 2);
            msec = string(buf + 12, 6);
            break;

        case 10:
            month = string(buf, 2);
            day = string(buf + 2, 2);
            hour = string(buf + 4, 2);
            min = string(buf + 6, 2);
            sec = string(buf + 8, 2);
            msec = string(buf + 10, 6);
            break;

        case 9:
            month = string(buf, 1);
            day = string(buf + 1, 2);
            hour = string(buf + 3, 2);
            min = string(buf + 5, 2);
            sec = string(buf + 7, 2);
            msec = string(buf + 9, 6);
            break;

        case 8:
            year = string(buf, 4);
            month = string(buf + 4, 2);
            day = string(buf + 6, 2);
            break;

        case 6:
            year = string(buf, 2);
            month = string(buf + 2, 2);
            day = string(buf + 4, 2);
            break;

        case 4:
            month = string(buf, 2);
            day = string(buf + 2, 2);
            break;

        case 3:
            month = string(buf, 1);
            day = string(buf + 1, 2);
            break;

        default:
            return -1;
    }

    if (year.empty())
    {
        // MMDD format. assume current year
        time_t calender_time;
        struct tm todays_date;
        calender_time = time(NULL);
        localtime_r(&calender_time, &todays_date);
        y = todays_date.tm_year + 1900;
    }
    else
    {
        y = atoi(year.c_str());
    }

    m = atoi(month.c_str());
    d = atoi(day.c_str());
    h = atoi(hour.c_str());
    minute = atoi(min.c_str());
    s = atoi(sec.c_str());
    ms = atoi(msec.c_str());

    //if (!isDateValid(d, m, y))
    //	return -1;
    if (!isDateValid(d, m, y) || !isDateTimeValid(h, minute, s, ms))
        return -1;

    aday.year = y;
    aday.month = m;
    aday.day = d;
    return *(reinterpret_cast<uint32_t*>(&aday));
}

/* This is really painful and expensive b/c it seems the input is not normalized or
sanitized.  That should really be done on ingestion. */
int64_t DataConvert::intToDatetime(int64_t data, bool* date)
{
    bool isDate = false;
    char buf[21] = {0};
    DateTime adaytime;

    if (data == 0)
    {
        adaytime.year = 0;
        adaytime.month = 0;
        adaytime.day = 0;
        adaytime.hour = 0;
        adaytime.minute = 0;
        adaytime.second = 0;
        adaytime.msecond = 0;

        if (date)
            *date = true;

        return *(reinterpret_cast<uint64_t*>(&adaytime));
    }

    // this snprintf call causes a compiler warning b/c we're potentially copying a 20-digit #
    // into 15 bytes, however, that appears to be intentional.
#if defined(__GNUC__) && __GNUC__ >= 7
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation="
    snprintf( buf, 15, "%llu", (long long unsigned int)data);
#pragma GCC diagnostic pop
#else
    snprintf( buf, 15, "%llu", (long long unsigned int)data);
#endif

    //string date = buf;
    string year, month, day, hour, min, sec, msec;
    int64_t y = 0, m = 0, d = 0, h = 0, minute = 0, s = 0, ms = 0;

    switch (strlen(buf))
    {
        case 14:
            year = string(buf, 4);
            month = string(buf + 4, 2);
            day = string(buf + 6, 2);
            hour = string(buf + 8, 2);
            min = string(buf + 10, 2);
            sec = string(buf + 12, 2);
            break;

        case 12:
            year = string(buf, 2);
            month = string(buf + 2, 2);
            day = string(buf + 4, 2);
            hour = string(buf + 6, 2);
            min = string(buf + 8, 2);
            sec = string(buf + 10, 2);
            break;

        case 10:
            month = string(buf, 2);
            day = string(buf + 2, 2);
            hour = string(buf + 4, 2);
            min = string(buf + 6, 2);
            sec = string(buf + 8, 2);
            break;

        case 9:
            month = string(buf, 1);
            day = string(buf + 1, 2);
            hour = string(buf + 3, 2);
            min = string(buf + 5, 2);
            sec = string(buf + 7, 2);
            break;

        case 8:
            year = string(buf, 4);
            month = string(buf + 4, 2);
            day = string(buf + 6, 2);
            isDate = true;
            break;

        case 6:
            year = string(buf, 2);
            month = string(buf + 2, 2);
            day = string(buf + 4, 2);
            isDate = true;
            break;

        case 4:
            month = string(buf, 2);
            day = string(buf + 2, 2);
            break;

        case 3:
            month = string(buf, 1);
            day = string(buf + 1, 2);
            isDate = true;
            break;

        default:
            return -1;
    }

    if (year.empty())
    {
        // MMDD format. assume current year
        time_t calender_time;
        struct tm todays_date;
        calender_time = time(NULL);
        localtime_r(&calender_time, &todays_date);
        y = todays_date.tm_year + 1900;
    }
    else
    {
        y = atoi(year.c_str());

        // special handling for 2-byte year
        if (year.length() == 2)
        {
            y += 2000;
            if (y > 2069)
                y -= 100;
        }
    }

    m = atoi(month.c_str());
    d = atoi(day.c_str());
    h = atoi(hour.c_str());
    minute = atoi(min.c_str());
    s = atoi(sec.c_str());
    ms = 0;

    if (!isDateValid(d, m, y) || !isDateTimeValid(h, minute, s, ms))
        return -1;

    adaytime.year = y;
    adaytime.month = m;
    adaytime.day = d;
    adaytime.hour = h;
    adaytime.minute = minute;
    adaytime.second = s;
    adaytime.msecond = ms;

    if (date)
        *date = isDate;

    return *(reinterpret_cast<uint64_t*>(&adaytime));
}

/* This is really painful and expensive b/c it seems the input is not normalized or
sanitized.  That should really be done on ingestion. */
int64_t DataConvert::intToTime(int64_t data, bool fromString)
{
    char buf[21] = {0};
    char* bufread = buf;
    Time atime;
    bool isNeg = false;

    if (data == 0)
    {
        atime.hour = 0;
        atime.minute = 0;
        atime.second = 0;
        atime.msecond = 0;
        atime.is_neg = 0;

        return *(reinterpret_cast<int64_t*>(&atime));
    }

    // this snprintf call causes a compiler warning b/c we're potentially copying a 20-digit #
    // into 15 bytes, however, that appears to be intentional.
#if defined(__GNUC__) && __GNUC__ >= 7
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation="
    snprintf( buf, 15, "%lld", (long long int)data);
#pragma GCC diagnostic pop
#else
    snprintf( buf, 15, "%lld", (long long int)data);
#endif

    //string date = buf;
    string hour, min, sec, msec;
    int64_t h = 0, minute = 0, s = 0, ms = 0;

    if (bufread[0] == '-')
    {
        isNeg = true;
        bufread++;
    }

    bool zero = false;

    switch (strlen(bufread))
    {
        // A full datetime
        case 14:
            hour = string(buf + 8, 2);
            min = string(buf + 10, 2);
            sec = string(buf + 12, 2);
            break;

        // Date so this is all 0
        case 8:
            zero = true;
            break;

        case 7:
            hour = string(bufread, 3);
            min = string(bufread + 3, 2);
            sec = string(bufread + 5, 2);
            break;

        case 6:
            hour = string(bufread, 2);
            min = string(bufread + 2, 2);
            sec = string(bufread + 4, 2);
            break;

        case 5:
            hour = string(bufread, 1);
            min = string(bufread + 1, 2);
            sec = string(bufread + 3, 2);
            break;

        case 4:
            min = string(bufread, 2);
            sec = string(bufread + 2, 2);
            break;

        case 3:
            min = string(bufread, 1);
            sec = string(bufread + 1, 2);
            break;

        case 2:
            sec = string(bufread, 2);
            break;

        case 1:
            sec = string(bufread, 1);
            break;

        default:
            return -1;
    }

    if (!zero)
    {
        h = atoi(hour.c_str());
        minute = atoi(min.c_str());
        s = atoi(sec.c_str());
    }
    else if (fromString)
    {
        // Saturate fromString
        h = 838;
        minute = 59;
        s = 59;
        ms = 999999;
    }

    if (!isTimeValid(h, minute, s, 0))
        return -1;

    atime.hour = h;
    atime.minute = minute;
    atime.second = s;
    atime.msecond = ms;
    atime.is_neg = isNeg;

    return *(reinterpret_cast<int64_t*>(&atime));
}

int64_t DataConvert::stringToTime(const string& data)
{
    // MySQL supported time value format 'D HHH:MM:SS.fraction'
    // -34 <= D <= 34
    // -838 <= H <= 838
    uint64_t min = 0, sec = 0, msec = 0;
    int64_t day = -1, hour = 0;
    bool isNeg = false;
    bool hasDate = false;
    string time, hms, ms;
    char* end = NULL;


    size_t pos = data.find("-");

    if (pos != string::npos)
    {
        isNeg = true;
    }

    if (data.substr(pos+1, data.length()-pos-1).find("-") != string::npos)
    {
        // A second dash, this has a date
        hasDate = true;
        isNeg = false;
    }
    // Day
    pos = data.find(" ");

    if (pos != string::npos)
    {
        if (!hasDate)
        {
            day = strtol(data.substr(0, pos).c_str(), &end, 10);

            if (*end != '\0')
                return -1;

            hour = day * 24;
            day = -1;
        }
        time = data.substr(pos + 1, data.length() - pos - 1);
    }
    else
    {
        time = data;
    }

    if (time.find(":") == string::npos)
    {
        if (hasDate)
        {
            // Has dashes, no colons. This is just a date!
            // Or the length < 6 (MariaDB returns NULL)
            return -1;
        }
        else
        {
            // This is an int time
            return intToTime(atoll(time.c_str()), true);
        }
    }


    // Fraction
    pos = time.find(".");

    if (pos != string::npos)
    {
        msec = strtoll(time.substr(pos + 1, time.length() - pos - 1).c_str(), 0, 10);
        hms = time.substr(0, pos);
    }
    else
    {
        hms = time;
    }

    // HHH:MM:SS
    pos = hms.find(":");

    if (pos == string::npos)
    {
        if (hour >= 0)
            hour += atoi(hms.c_str());
        else
            hour -= atoi(hms.c_str());
    }
    else
    {
        if (hour >= 0)
            hour += atoi(hms.substr(0, pos).c_str());
        else
            hour -= atoi(hms.substr(0, pos).c_str());

        ms = hms.substr(pos + 1, hms.length() - pos - 1);
    }

    // MM:SS
    pos = ms.find(":");

    if (pos != string::npos)
    {
        min = atoi(ms.substr(0, pos).c_str());
        sec = atoi(ms.substr(pos + 1, ms.length() - pos - 1).c_str());
    }
    else
    {
        min = atoi(ms.c_str());
    }

    Time atime;
    atime.day = day;
    atime.hour = hour;
    atime.minute = min;
    atime.second = sec;
    atime.msecond = msec;
    atime.is_neg = isNeg;
    return *(reinterpret_cast<int64_t*>(&atime));
}

CalpontSystemCatalog::ColType DataConvert::convertUnionColType(vector<CalpontSystemCatalog::ColType>& types)
{
    idbassert(types.size());

    CalpontSystemCatalog::ColType unionedType = types[0];

    for (uint64_t i = 1; i < types.size(); i++)
    {
        // limited support for VARBINARY, no implicit conversion.
        if (types[i].colDataType == CalpontSystemCatalog::VARBINARY ||
                unionedType.colDataType == CalpontSystemCatalog::VARBINARY)
        {
            if (types[i].colDataType != unionedType.colDataType ||
                    types[i].colWidth != unionedType.colWidth)
                throw runtime_error("VARBINARY in UNION must be the same width.");
        }

        switch (types[i].colDataType)
        {
            case CalpontSystemCatalog::TINYINT:
            case CalpontSystemCatalog::SMALLINT:
            case CalpontSystemCatalog::MEDINT:
            case CalpontSystemCatalog::INT:
            case CalpontSystemCatalog::BIGINT:
            case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT:
            case CalpontSystemCatalog::UDECIMAL:
            {
                switch (unionedType.colDataType)
                {
                    case CalpontSystemCatalog::TINYINT:
                    case CalpontSystemCatalog::SMALLINT:
                    case CalpontSystemCatalog::MEDINT:
                    case CalpontSystemCatalog::INT:
                    case CalpontSystemCatalog::BIGINT:
                    case CalpontSystemCatalog::DECIMAL:
                    case CalpontSystemCatalog::UTINYINT:
                    case CalpontSystemCatalog::USMALLINT:
                    case CalpontSystemCatalog::UMEDINT:
                    case CalpontSystemCatalog::UINT:
                    case CalpontSystemCatalog::UBIGINT:
                    case CalpontSystemCatalog::UDECIMAL:
                        if (types[i].colWidth > unionedType.colWidth)
                        {
                            unionedType.colDataType = types[i].colDataType;
                            unionedType.colWidth = types[i].colWidth;
                        }

                        // If same size and result is signed but source is unsigned...
                        if (types[i].colWidth == unionedType.colWidth && !isUnsigned(unionedType.colDataType) && isUnsigned(types[i].colDataType))
                        {
                            unionedType.colDataType = types[i].colDataType;
                        }

                        if (types[i].colDataType == CalpontSystemCatalog::DECIMAL || types[i].colDataType == CalpontSystemCatalog::UDECIMAL)
                        {
                            unionedType.colDataType = CalpontSystemCatalog::DECIMAL;
                        }

                        unionedType.scale = (types[i].scale > unionedType.scale) ? types[i].scale : unionedType.scale;
                        break;

                    case CalpontSystemCatalog::DATE:
                        unionedType.colDataType = CalpontSystemCatalog::CHAR;
                        unionedType.colWidth = 20;
                        break;

                    case CalpontSystemCatalog::TIME:
                    case CalpontSystemCatalog::DATETIME:
                    case CalpontSystemCatalog::TIMESTAMP:
                        unionedType.colDataType = CalpontSystemCatalog::CHAR;
                        unionedType.colWidth = 26;
                        break;

                    case CalpontSystemCatalog::CHAR:
                        if (unionedType.colWidth < 20)
                            unionedType.colWidth = 20;

                        break;

                    case CalpontSystemCatalog::VARCHAR:
                        if (unionedType.colWidth < 21)
                            unionedType.colWidth = 21;

                        break;

                    case CalpontSystemCatalog::FLOAT:
                    case CalpontSystemCatalog::DOUBLE:
                    case CalpontSystemCatalog::UFLOAT:
                    case CalpontSystemCatalog::UDOUBLE:
                    case CalpontSystemCatalog::LONGDOUBLE:
                    default:
                        break;
                }

                break;
            }

            case CalpontSystemCatalog::DATE:
            {
                switch (unionedType.colDataType)
                {
                    case CalpontSystemCatalog::TINYINT:
                    case CalpontSystemCatalog::SMALLINT:
                    case CalpontSystemCatalog::MEDINT:
                    case CalpontSystemCatalog::INT:
                    case CalpontSystemCatalog::BIGINT:
                    case CalpontSystemCatalog::DECIMAL:
                    case CalpontSystemCatalog::FLOAT:
                    case CalpontSystemCatalog::DOUBLE:
                    case CalpontSystemCatalog::UTINYINT:
                    case CalpontSystemCatalog::USMALLINT:
                    case CalpontSystemCatalog::UMEDINT:
                    case CalpontSystemCatalog::UINT:
                    case CalpontSystemCatalog::UBIGINT:
                    case CalpontSystemCatalog::UDECIMAL:
                    case CalpontSystemCatalog::UFLOAT:
                    case CalpontSystemCatalog::UDOUBLE:
                    case CalpontSystemCatalog::LONGDOUBLE:
                        unionedType.colDataType = CalpontSystemCatalog::CHAR;
                        unionedType.scale = 0;
                        unionedType.colWidth = 20;
                        break;

                    case CalpontSystemCatalog::CHAR:
                        if (unionedType.colWidth < 10)
                            unionedType.colWidth = 10;

                        break;

                    case CalpontSystemCatalog::VARCHAR:
                        if (unionedType.colWidth < 11)
                            unionedType.colWidth = 11;

                        break;

                    case CalpontSystemCatalog::DATE:
                    case CalpontSystemCatalog::DATETIME:
                    case CalpontSystemCatalog::TIMESTAMP:
                    case CalpontSystemCatalog::TIME:
                    default:
                        break;
                }

                break;
            }

            case CalpontSystemCatalog::DATETIME:
            {
                switch (unionedType.colDataType)
                {
                    case CalpontSystemCatalog::TINYINT:
                    case CalpontSystemCatalog::SMALLINT:
                    case CalpontSystemCatalog::MEDINT:
                    case CalpontSystemCatalog::INT:
                    case CalpontSystemCatalog::BIGINT:
                    case CalpontSystemCatalog::DECIMAL:
                    case CalpontSystemCatalog::FLOAT:
                    case CalpontSystemCatalog::DOUBLE:
                    case CalpontSystemCatalog::UTINYINT:
                    case CalpontSystemCatalog::USMALLINT:
                    case CalpontSystemCatalog::UMEDINT:
                    case CalpontSystemCatalog::UINT:
                    case CalpontSystemCatalog::UBIGINT:
                    case CalpontSystemCatalog::UDECIMAL:
                    case CalpontSystemCatalog::UFLOAT:
                    case CalpontSystemCatalog::UDOUBLE:
                    case CalpontSystemCatalog::TIME:
                    case CalpontSystemCatalog::LONGDOUBLE:
                    case CalpontSystemCatalog::TIMESTAMP:
                        unionedType.colDataType = CalpontSystemCatalog::CHAR;
                        unionedType.scale = 0;
                        unionedType.colWidth = 26;
                        break;

                    case CalpontSystemCatalog::DATE:
                        unionedType.colDataType = CalpontSystemCatalog::DATETIME;
                        unionedType.colWidth = types[i].colWidth;
                        break;

                    case CalpontSystemCatalog::CHAR:
                        if (unionedType.colWidth < 26)
                            unionedType.colWidth = 26;

                        break;

                    case CalpontSystemCatalog::VARCHAR:
                        if (unionedType.colWidth < 27)
                            unionedType.colWidth = 27;

                        break;

                    case CalpontSystemCatalog::DATETIME:
                    default:
                        break;
                }

                break;
            }

            case CalpontSystemCatalog::TIMESTAMP:
            {
                switch (unionedType.colDataType)
                {
                    case CalpontSystemCatalog::TINYINT:
                    case CalpontSystemCatalog::SMALLINT:
                    case CalpontSystemCatalog::MEDINT:
                    case CalpontSystemCatalog::INT:
                    case CalpontSystemCatalog::BIGINT:
                    case CalpontSystemCatalog::DECIMAL:
                    case CalpontSystemCatalog::FLOAT:
                    case CalpontSystemCatalog::DOUBLE:
                    case CalpontSystemCatalog::UTINYINT:
                    case CalpontSystemCatalog::USMALLINT:
                    case CalpontSystemCatalog::UMEDINT:
                    case CalpontSystemCatalog::UINT:
                    case CalpontSystemCatalog::UBIGINT:
                    case CalpontSystemCatalog::UDECIMAL:
                    case CalpontSystemCatalog::UFLOAT:
                    case CalpontSystemCatalog::UDOUBLE:
                    case CalpontSystemCatalog::TIME:
                    case CalpontSystemCatalog::DATETIME:
                        unionedType.colDataType = CalpontSystemCatalog::CHAR;
                        unionedType.scale = 0;
                        unionedType.colWidth = 26;
                        break;

                    case CalpontSystemCatalog::DATE:
                        unionedType.colDataType = CalpontSystemCatalog::TIMESTAMP;
                        unionedType.colWidth = types[i].colWidth;
                        break;

                    case CalpontSystemCatalog::CHAR:
                        if (unionedType.colWidth < 26)
                            unionedType.colWidth = 26;

                        break;

                    case CalpontSystemCatalog::VARCHAR:
                        if (unionedType.colWidth < 27)
                            unionedType.colWidth = 27;

                        break;

                    case CalpontSystemCatalog::TIMESTAMP:
                    default:
                        break;
                }

                break;
            }

            case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::DOUBLE:
            case CalpontSystemCatalog::UFLOAT:
            case CalpontSystemCatalog::UDOUBLE:
            {
                switch (unionedType.colDataType)
                {
                    case CalpontSystemCatalog::DATE:
                        unionedType.colDataType = CalpontSystemCatalog::CHAR;
                        unionedType.scale = 0;
                        unionedType.colWidth = 20;
                        break;

                    case CalpontSystemCatalog::DATETIME:
                    case CalpontSystemCatalog::TIMESTAMP:
                        unionedType.colDataType = CalpontSystemCatalog::CHAR;
                        unionedType.scale = 0;
                        unionedType.colWidth = 26;
                        break;

                    case CalpontSystemCatalog::CHAR:
                        if (unionedType.colWidth < 20)
                            unionedType.colWidth = 20;

                        break;

                    case CalpontSystemCatalog::VARCHAR:
                        if (unionedType.colWidth < 21)
                            unionedType.colWidth = 21;

                        break;

                    case CalpontSystemCatalog::TINYINT:
                    case CalpontSystemCatalog::SMALLINT:
                    case CalpontSystemCatalog::MEDINT:
                    case CalpontSystemCatalog::INT:
                    case CalpontSystemCatalog::BIGINT:
                    case CalpontSystemCatalog::DECIMAL:
                    case CalpontSystemCatalog::FLOAT:
                    case CalpontSystemCatalog::DOUBLE:
                    case CalpontSystemCatalog::UTINYINT:
                    case CalpontSystemCatalog::USMALLINT:
                    case CalpontSystemCatalog::UMEDINT:
                    case CalpontSystemCatalog::UINT:
                    case CalpontSystemCatalog::UBIGINT:
                    case CalpontSystemCatalog::UDECIMAL:
                    case CalpontSystemCatalog::UFLOAT:
                    case CalpontSystemCatalog::UDOUBLE:
                        unionedType.colDataType = CalpontSystemCatalog::DOUBLE;
                        unionedType.scale = 0;
                        unionedType.colWidth = sizeof(double);
                        break;

                    default:
                        break;
                }

                break;
            }

            case CalpontSystemCatalog::LONGDOUBLE:
            {
                switch (unionedType.colDataType)
                {
                    case CalpontSystemCatalog::DATE:
                        unionedType.colDataType = CalpontSystemCatalog::CHAR;
                        unionedType.scale = 0;
                        unionedType.colWidth = 20;
                        break;

                    case CalpontSystemCatalog::DATETIME:
                        unionedType.colDataType = CalpontSystemCatalog::CHAR;
                        unionedType.scale = 0;
                        unionedType.colWidth = 26;
                        break;

                    case CalpontSystemCatalog::CHAR:
                        if (unionedType.colWidth < 20)
                            unionedType.colWidth = 20;

                        break;

                    case CalpontSystemCatalog::VARCHAR:
                        if (unionedType.colWidth < 21)
                            unionedType.colWidth = 21;

                        break;

                    case CalpontSystemCatalog::TINYINT:
                    case CalpontSystemCatalog::SMALLINT:
                    case CalpontSystemCatalog::MEDINT:
                    case CalpontSystemCatalog::INT:
                    case CalpontSystemCatalog::BIGINT:
                    case CalpontSystemCatalog::DECIMAL:
                    case CalpontSystemCatalog::FLOAT:
                    case CalpontSystemCatalog::DOUBLE:
                    case CalpontSystemCatalog::UTINYINT:
                    case CalpontSystemCatalog::USMALLINT:
                    case CalpontSystemCatalog::UMEDINT:
                    case CalpontSystemCatalog::UINT:
                    case CalpontSystemCatalog::UBIGINT:
                    case CalpontSystemCatalog::UDECIMAL:
                    case CalpontSystemCatalog::UFLOAT:
                    case CalpontSystemCatalog::UDOUBLE:
                    case CalpontSystemCatalog::LONGDOUBLE:
                        unionedType.colDataType = CalpontSystemCatalog::LONGDOUBLE;
                        unionedType.scale = (types[i].scale > unionedType.scale) ? types[i].scale : unionedType.scale;
                        unionedType.colWidth = sizeof(long double);
                        unionedType.precision = -1;
                        break;

                    default:
                        break;
                }

                break;
            }

            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::VARCHAR:
            {
                switch (unionedType.colDataType)
                {
                    case CalpontSystemCatalog::TINYINT:
                    case CalpontSystemCatalog::SMALLINT:
                    case CalpontSystemCatalog::MEDINT:
                    case CalpontSystemCatalog::INT:
                    case CalpontSystemCatalog::BIGINT:
                    case CalpontSystemCatalog::DECIMAL:
                    case CalpontSystemCatalog::FLOAT:
                    case CalpontSystemCatalog::DOUBLE:
                    case CalpontSystemCatalog::UTINYINT:
                    case CalpontSystemCatalog::USMALLINT:
                    case CalpontSystemCatalog::UMEDINT:
                    case CalpontSystemCatalog::UINT:
                    case CalpontSystemCatalog::UBIGINT:
                    case CalpontSystemCatalog::UDECIMAL:
                    case CalpontSystemCatalog::UFLOAT:
                    case CalpontSystemCatalog::UDOUBLE:
                    case CalpontSystemCatalog::LONGDOUBLE:
                        unionedType.scale = 0;
                        unionedType.colWidth = (types[i].colWidth > 20) ? types[i].colWidth : 20;
                        break;

                    case CalpontSystemCatalog::DATE:
                        unionedType.colWidth = (types[i].colWidth > 10) ? types[i].colWidth : 10;
                        break;

                    case CalpontSystemCatalog::DATETIME:
                    case CalpontSystemCatalog::TIMESTAMP:
                        unionedType.colWidth = (types[i].colWidth > 26) ? types[i].colWidth : 26;
                        break;

                    case CalpontSystemCatalog::CHAR:
                    case CalpontSystemCatalog::VARCHAR:

                        // VARCHAR will fit in CHAR of the same width
                        if (unionedType.colWidth < types[i].colWidth)
                            unionedType.colWidth = types[i].colWidth;

                        break;

                    default:
                        break;
                }

                // MariaDB bug 651. Setting to CHAR broke union in subquery
                unionedType.colDataType = CalpontSystemCatalog::VARCHAR;
                break;
            }

            default:
            {
                break;
            }
        } // switch
    } // for

    return unionedType;
}

} // namespace dataconvert
// vim:ts=4 sw=4:


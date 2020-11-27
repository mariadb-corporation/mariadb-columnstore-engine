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
* $Id: func_from_unixtime.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
*
*
****************************************************************************/

#include <unistd.h>
#include <cstdlib>
#include <string>
using namespace std;

#include "functor_dtm.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;


namespace
{
using namespace funcexp;

class TUSec6
{
protected:
    uint64_t  mSec;       // The integer part, between 0 and LONGLONG_MAX
    uint32_t  mUsec;      // The fractional part, between 0 and 999999
public:
    explicit TUSec6(uint64_t sec, uint32_t usec = 0)
        :mSec(sec), mUsec(usec)
    { }

    static TUSec6 fromDecimal(uint64_t val, uint8_t scale)
    {
        if (scale == 0)
            return TUSec6(val);
        if (scale <= 6)  // SSSuuu -> SSSuuuu00
        {
            uint64_t k = IDB_pow[scale];
            return TUSec6(val / k, (val % k) * IDB_pow[6-scale]);
        }
        // scale>6 :  SSSuuuuuunnnn -> SSSuuuuuu
        val /= IDB_pow[scale-6];
        return TUSec6(val / 1000000, val % 1000000);
    }

    DateTime from_unixtime() const
    {
        if (mSec > helpers::TIMESTAMP_MAX_VALUE)
            return 0;

        DateTime dt;

        struct tm tmp_tm;
        time_t tmp_t = (time_t) mSec;
        localtime_r(&tmp_t, &tmp_tm);

        //to->neg=0;
        dt.year = (int64_t) ((tmp_tm.tm_year + 1900) % 10000);
        dt.month = (int64_t) tmp_tm.tm_mon + 1;
        dt.day = (int64_t) tmp_tm.tm_mday;
        dt.hour = (int64_t) tmp_tm.tm_hour;
        dt.minute = (int64_t) tmp_tm.tm_min;
        dt.second = (int64_t) tmp_tm.tm_sec;
        dt.msecond = mUsec;
        return dt;
    }
};


class TSSec6: public TUSec6
{
    bool mNeg;       // false if positive, true of negative
public:

    TSSec6(bool neg, const TUSec6 &sec)
       :TUSec6(sec), mNeg(neg)
    { }
    static TSSec6 fromDecimal(int64_t val, uint8_t scale)
    {
      if (val >= 0)
          return TSSec6(false, TUSec6::fromDecimal((uint64_t) val, scale));
      if (val == (int64_t) 0x8000000000000000LL) // Avoid UDB
          return TSSec6(true, TUSec6(0x8000000000000000ULL));
      return TSSec6(true, TUSec6((uint64_t) -val, scale));
    }
    DateTime from_unixtime() const
    {
        if (mNeg)
            return 0;
        return TUSec6::from_unixtime();
    }
};


DateTime getDateTime(rowgroup::Row& row,
                     FunctionParm& parm,
                     bool& isNull)
{
    switch (parm[0]->data()->resultType().colDataType)
    {
        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::DOUBLE:
        {
            double value = parm[0]->data()->getDoubleVal(row, isNull);
            return TSSec6::fromDecimal((int64_t) (value * 1000000), 6).from_unixtime();
        }
        case execplan::CalpontSystemCatalog::DECIMAL:
        {
            IDB_Decimal dec = parm[0]->data()->getDecimalVal(row, isNull);
            return TSSec6::fromDecimal((int64_t) dec.value, dec.scale).from_unixtime();
        }

        default:
        {
            int64_t val = parm[0]->data()->getDatetimeIntVal(row, isNull);
            return TSSec6::fromDecimal((int64_t) val, 0).from_unixtime();
        }
    }
}
}

namespace funcexp
{

CalpontSystemCatalog::ColType Func_from_unixtime::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    CalpontSystemCatalog::ColType ct;
    ct.colDataType = CalpontSystemCatalog::VARCHAR;
    ct.colWidth = 255;
    return ct;
}

string Func_from_unixtime::getStrVal(rowgroup::Row& row,
                                     FunctionParm& parm,
                                     bool& isNull,
                                     CalpontSystemCatalog::ColType&)
{
    DateTime dt = getDateTime(row, parm, isNull);

    if (*reinterpret_cast<int64_t*>(&dt) == 0)
    {
        isNull = true;
        return "";
    }

    if (parm.size() == 2)
    {
        const string& format = parm[1]->data()->getStrVal(row, isNull);
        return helpers::IDB_date_format(dt, format);
    }

    char buf[256] = {0};
    DataConvert::datetimeToString(*(reinterpret_cast<int64_t*>(&dt)), buf, 255);
    return string(buf, 255);
}

int32_t Func_from_unixtime::getDateIntVal(rowgroup::Row& row,
        FunctionParm& parm,
        bool& isNull,
        CalpontSystemCatalog::ColType& ct)
{
    return (((getDatetimeIntVal(row, parm, isNull, ct) >> 32) & 0xFFFFFFC0) | 0x3E);
}

int64_t Func_from_unixtime::getDatetimeIntVal(rowgroup::Row& row,
        FunctionParm& parm,
        bool& isNull,
        CalpontSystemCatalog::ColType& ct)
{
    DateTime dt = getDateTime(row, parm, isNull);

    if (*reinterpret_cast<int64_t*>(&dt) == 0)
    {
        isNull = true;
        return 0;
    }

    return *reinterpret_cast<int64_t*>(&dt);
}

int64_t Func_from_unixtime::getTimeIntVal(rowgroup::Row& row,
        FunctionParm& parm,
        bool& isNull,
        CalpontSystemCatalog::ColType& ct)
{
    DateTime dt = getDateTime(row, parm, isNull);

    if (*reinterpret_cast<int64_t*>(&dt) == 0)
    {
        isNull = true;
        return 0;
    }

    return *reinterpret_cast<int64_t*>(&dt);
}

int64_t Func_from_unixtime::getIntVal(rowgroup::Row& row,
                                      FunctionParm& parm,
                                      bool& isNull,
                                      CalpontSystemCatalog::ColType& ct)
{
    DateTime dt = getDateTime(row, parm, isNull);

    if (*reinterpret_cast<int64_t*>(&dt) == 0)
    {
        isNull = true;
        return 0;
    }

    char buf[32];  // actual string guaranteed to be 22
    snprintf( buf, 32, "%04d%02d%02d%02d%02d%02d",
              dt.year, dt.month, dt.day, dt.hour,
              dt.minute, dt.second );
    return atoll(buf);
}

double Func_from_unixtime::getDoubleVal(rowgroup::Row& row,
                                        FunctionParm& parm,
                                        bool& isNull,
                                        CalpontSystemCatalog::ColType& ct)
{
    if (parm.size() == 1)
    {
        DateTime dt = getDateTime(row, parm, isNull);

        if (*reinterpret_cast<int64_t*>(&dt) == 0)
        {
            isNull = true;
            return 0;
        }

        char buf[32];  // actual string guaranteed to be 22
        snprintf( buf, 32, "%04d%02d%02d%02d%02d%02d.%06d",
                  dt.year, dt.month, dt.day, dt.hour,
                  dt.minute, dt.second, dt.msecond );
        return atof(buf);
    }

    return (double) atoi(getStrVal(row, parm, isNull, ct).c_str());
}

long double Func_from_unixtime::getLongDoubleVal(rowgroup::Row& row,
                                        FunctionParm& parm,
                                        bool& isNull,
                                        CalpontSystemCatalog::ColType& ct)
{
    return (long double) getDoubleVal(row, parm, isNull, ct);
}


} // namespace funcexp
// vim:ts=4 sw=4:

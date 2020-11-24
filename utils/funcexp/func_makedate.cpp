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
* $Id: func_makedate.cpp 2665 2011-06-01 20:42:52Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_dtm.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "intervalcolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

using namespace funcexp;

namespace
{
uint64_t makedate(rowgroup::Row& row,
                  FunctionParm& parm,
                  bool& isNull)
{
    int64_t year = 0;
    string dayofyear;

    //get year
    switch (parm[0]->data()->resultType().colDataType)
    {
        case CalpontSystemCatalog::BIGINT:
        case CalpontSystemCatalog::MEDINT:
        case CalpontSystemCatalog::SMALLINT:
        case CalpontSystemCatalog::TINYINT:
        case CalpontSystemCatalog::INT:
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::VARCHAR:
        {
            double value = parm[0]->data()->getDoubleVal(row, isNull);
            year = (int64_t) value;
            break;
        }

        case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL:
        {
            IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);

            if (parm[0]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
            {
                year = static_cast<int64_t>(d.getPosNegRoundedIntegralPart(4));
            }
            else
            {
                double dscale = d.scale;
                year = d.value / pow(10.0, dscale);
                int lefto = (d.value - year * pow(10.0, dscale)) / pow(10.0, dscale - 1);

                if ( year >= 0 && lefto > 4 )
                    year++;

                if ( year < 0 && lefto < -4 )
                    year--;
            }

            break;
        }

        default:
            isNull = true;
            return 0;
    }

    if (year < 70)
    {
        year = 2000 + year;
    }
    else if (year < 100)
    {
        year = 1900 + year;
    }
    else if (year < 1000 || year > 9999)
    {
        isNull = true;
        return 0;
    }

    //get dayofyear
    switch (parm[1]->data()->resultType().colDataType)
    {
        case CalpontSystemCatalog::BIGINT:
        case CalpontSystemCatalog::MEDINT:
        case CalpontSystemCatalog::SMALLINT:
        case CalpontSystemCatalog::TINYINT:
        case CalpontSystemCatalog::INT:
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::VARCHAR:
        {
            dayofyear = parm[1]->data()->getStrVal(row, isNull);

            if (atoi(dayofyear.c_str()) < 1)
            {
                isNull = true;
                return 0;
            }

            break;
        }

        case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL:
        {
            IDB_Decimal d = parm[1]->data()->getDecimalVal(row, isNull);

            if (parm[1]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
            {
                int64_t tmpval = static_cast<int64_t>(d.getPosNegRoundedIntegralPart(4));
                if (tmpval < 1)
                {
                    isNull = true;
                    return 0;
                }

                dayofyear = helpers::intToString(tmpval);
            }
            else
            {
                double dscale = d.scale;
                int64_t tmp = d.value / pow(10.0, dscale);
                int lefto = (d.value - tmp * pow(10.0, dscale)) / pow(10.0, dscale - 1);

                if (tmp >= 0 && lefto > 4)
                    tmp++;

                if (tmp < 0 && lefto < -4)
                    tmp--;

                if (tmp < 1)
                {
                    isNull = true;
                    return 0;
                }

                dayofyear = helpers::intToString(tmp);
            }
            break;
        }

        case CalpontSystemCatalog::TIME:
        {
            std::ostringstream ss;
            char buf[9];
            uint64_t aTime = parm[1]->data()->getTimeIntVal(row, isNull);
            DataConvert::timeToString1(aTime, buf, 9);
            dayofyear = buf;
            break;
        }

        default:
            isNull = true;
            return 0;
    }

    if (atoi(dayofyear.c_str()) == 0)
    {
        isNull = true;
        return 0;
    }

    // convert the year to a date in our internal format, then subtract
    // one since we are about to add the day of year back in
    Date d(year, 1, 1);
    //@Bug 5232. spare bit is set, cannot use regular substraction
    d.day -= 1;
    //uint64_t intDate = ((*(reinterpret_cast<uint32_t *> (&d))) & 0xFFFFFFC) - 1;
    uint64_t intDate = *(reinterpret_cast<uint32_t*> (&d));

    uint64_t value = helpers::dateAdd( intDate, dayofyear, IntervalColumn::INTERVAL_DAY, true, OP_ADD );

    if ( value == 0 )
    {
        isNull = true;
    }

    return value;
}

}


namespace funcexp
{

CalpontSystemCatalog::ColType Func_makedate::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}


int64_t Func_makedate::getIntVal(rowgroup::Row& row,
                                 FunctionParm& parm,
                                 bool& isNull,
                                 CalpontSystemCatalog::ColType&)
{
    return makedate(row, parm, isNull);
}


string Func_makedate::getStrVal(rowgroup::Row& row,
                                FunctionParm& parm,
                                bool& isNull,
                                CalpontSystemCatalog::ColType&)
{
    uint64_t value = makedate(row, parm, isNull);

    if (isNull)
        return "";

    return dataconvert::DataConvert::dateToString(value);
}


} // namespace funcexp
// vim:ts=4 sw=4:

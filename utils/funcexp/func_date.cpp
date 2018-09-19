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
* $Id: func_date.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_dtm.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;


namespace funcexp
{

CalpontSystemCatalog::ColType Func_date::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}

int64_t Func_date::getIntVal(rowgroup::Row& row,
                             FunctionParm& parm,
                             bool& isNull,
                             CalpontSystemCatalog::ColType&)
{
    CalpontSystemCatalog::ColDataType type = parm[0]->data()->resultType().colDataType;

    string value = "";

    DateTime aDateTime;
    Time     aTime;

    switch (type)
    {
        case execplan::CalpontSystemCatalog::DATE:
        {
            return parm[0]->data()->getDatetimeIntVal(row, isNull);
            break;
        }

        case execplan::CalpontSystemCatalog::DATETIME:
        {
            int64_t val1  = parm[0]->data()->getDatetimeIntVal(row, isNull);
            value = dataconvert::DataConvert::datetimeToString(val1);
            value = value.substr(0, 10);
            break;
        }

        // Time adds to now() and then gets value
        case CalpontSystemCatalog::TIME:
        {
            int64_t val;
            aDateTime = static_cast<DateTime>(nowDatetime());
            aTime = parm[0]->data()->getTimeIntVal(row, isNull);
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
            value = dataconvert::DataConvert::datetimeToString(val);
            value = value.substr(0, 10);
            break;
        }


        case execplan::CalpontSystemCatalog::BIGINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::TINYINT:
        case execplan::CalpontSystemCatalog::SMALLINT:
        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
        case execplan::CalpontSystemCatalog::VARCHAR:
        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            isNull = true;
            return 0;
        }
        break;

        default:
        {
            std::ostringstream oss;
            oss << "date: datatype of " << execplan::colDataTypeToString(type);
            throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
        }
    }

    return dataconvert::DataConvert::datetimeToInt(value);
}

string Func_date::getStrVal(rowgroup::Row& row,
                            FunctionParm& parm,
                            bool& isNull,
                            CalpontSystemCatalog::ColType&)
{
    const string& val = parm[0]->data()->getStrVal(row, isNull);

    return val.substr(0, 10);
}


} // namespace funcexp
// vim:ts=4 sw=4:

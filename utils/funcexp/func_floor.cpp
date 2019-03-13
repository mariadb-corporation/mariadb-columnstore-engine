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

//  $Id: func_floor.cpp 3923 2013-06-19 21:43:06Z bwilkinson $


#include <cstdlib>
#include <iomanip>
#include <string>
using namespace std;

#include "functor_real.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;
using namespace dataconvert;

#include "funchelpers.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_floor::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
    return fp[0]->data()->resultType();
}


int64_t Func_floor::getIntVal(Row& row,
                              FunctionParm& parm,
                              bool& isNull,
                              CalpontSystemCatalog::ColType& op_ct)
{
    int64_t ret = 0;

    switch (op_ct.colDataType)
    {
        case execplan::CalpontSystemCatalog::BIGINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::TINYINT:
        case execplan::CalpontSystemCatalog::SMALLINT:
        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            if (op_ct.scale == 0)
            {
                ret = parm[0]->data()->getIntVal(row, isNull);
                break;
            }

            IDB_Decimal decimal = parm[0]->data()->getDecimalVal(row, isNull);

            if (isNull)
                break;

            ret = decimal.value;

            // negative scale is not supported by CNX yet
            if (decimal.scale > 0)
            {

                if (decimal.scale >= 19)
                {
                    std::ostringstream oss;
                    oss << "floor: datatype of " << execplan::colDataTypeToString(op_ct.colDataType)
                        << " with scale " << (int) decimal.scale << " is beyond supported scale";
                    throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
                }

                int64_t tmp = ret;
                ret /= helpers::powerOf10_c[decimal.scale];

                // Largest integer value not greater than X.
                if (tmp < 0 && tmp < ret)
                    ret -= 1;
            }
        }
        break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            ret = (int64_t)parm[0]->data()->getUintVal(row, isNull);
        }
        break;

        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
        {
            ret = (int64_t) floor(parm[0]->data()->getDoubleVal(row, isNull));
        }
        break;

        case execplan::CalpontSystemCatalog::VARCHAR:
        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        {
            const string& str = parm[0]->data()->getStrVal(row, isNull);

            if (!isNull)
                ret = (int64_t) floor(strtod(str.c_str(), 0));
        }
        break;

        case execplan::CalpontSystemCatalog::DATE:
        {
            string str = DataConvert::dateToString1(parm[0]->data()->getDateIntVal(row, isNull));

            if (!isNull)
                ret = atoll(str.c_str());
        }
        break;

        case execplan::CalpontSystemCatalog::DATETIME:
        {
            string str =
                DataConvert::datetimeToString1(parm[0]->data()->getDatetimeIntVal(row, isNull));

            // strip off micro seconds
            str = str.substr(0, 14);

            if (!isNull)
                ret = atoll(str.c_str());
        }
        break;

        case execplan::CalpontSystemCatalog::TIME:
        {
            string str =
                DataConvert::timeToString1(parm[0]->data()->getTimeIntVal(row, isNull));

            // strip off micro seconds
            str = str.substr(0, 14);

            if (!isNull)
                ret = atoll(str.c_str());
        }
        break;

        default:
        {
            std::ostringstream oss;
            oss << "floor: datatype of " << execplan::colDataTypeToString(op_ct.colDataType);
            throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
        }
    }

    return ret;
}


uint64_t Func_floor::getUintVal(Row& row,
                                FunctionParm& parm,
                                bool& isNull,
                                CalpontSystemCatalog::ColType& op_ct)
{
    int64_t ret = 0;

    switch (op_ct.colDataType)
    {
        case execplan::CalpontSystemCatalog::BIGINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::TINYINT:
        case execplan::CalpontSystemCatalog::SMALLINT:
        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            ret = parm[0]->data()->getIntVal(row, isNull);
        }
        break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            ret = (int64_t)parm[0]->data()->getUintVal(row, isNull);
        }
        break;

        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
        {
            ret = (uint64_t)floor(parm[0]->data()->getDoubleVal(row, isNull));
        }
        break;

        case execplan::CalpontSystemCatalog::VARCHAR:
        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        {
            const string& str = parm[0]->data()->getStrVal(row, isNull);

            if (!isNull)
                ret = (uint64_t)floor(strtod(str.c_str(), 0));
        }
        break;

        case execplan::CalpontSystemCatalog::DATE:
        {
            string str = DataConvert::dateToString1(parm[0]->data()->getDateIntVal(row, isNull));

            if (!isNull)
                ret = strtoull(str.c_str(), NULL, 10);
        }
        break;

        case execplan::CalpontSystemCatalog::DATETIME:
        {
            string str =
                DataConvert::datetimeToString1(parm[0]->data()->getDatetimeIntVal(row, isNull));

            // strip off micro seconds
            str = str.substr(0, 14);

            if (!isNull)
                ret = strtoull(str.c_str(), NULL, 10);
        }
        break;

        case execplan::CalpontSystemCatalog::TIME:
        {
            string str =
                DataConvert::timeToString1(parm[0]->data()->getTimeIntVal(row, isNull));

            // strip off micro seconds
            str = str.substr(0, 14);

            if (!isNull)
                ret = strtoull(str.c_str(), NULL, 10);
        }
        break;

        default:
        {
            std::ostringstream oss;
            oss << "floor: datatype of " << execplan::colDataTypeToString(op_ct.colDataType);
            throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
        }
    }

    return ret;
}


double Func_floor::getDoubleVal(Row& row,
                                FunctionParm& parm,
                                bool& isNull,
                                CalpontSystemCatalog::ColType& op_ct)
{
    double ret = 0.0;

    if (op_ct.colDataType == CalpontSystemCatalog::DOUBLE ||
            op_ct.colDataType == CalpontSystemCatalog::FLOAT)
    {
        ret = floor(parm[0]->data()->getDoubleVal(row, isNull));
    }
    else if (op_ct.colDataType == CalpontSystemCatalog::VARCHAR ||
             op_ct.colDataType == CalpontSystemCatalog::CHAR ||
             op_ct.colDataType == CalpontSystemCatalog::TEXT)
    {
        const string& str = parm[0]->data()->getStrVal(row, isNull);

        if (!isNull)
            ret = floor(strtod(str.c_str(), 0));
    }
    else
    {
        ret = (double) getIntVal(row, parm, isNull, op_ct);
    }

    return ret;
}


string Func_floor::getStrVal(Row& row,
                             FunctionParm& parm,
                             bool& isNull,
                             CalpontSystemCatalog::ColType& op_ct)
{
    char tmp[512] = {'\0'};

    if (op_ct.colDataType == CalpontSystemCatalog::DOUBLE ||
            op_ct.colDataType == CalpontSystemCatalog::UDOUBLE ||
            op_ct.colDataType == CalpontSystemCatalog::FLOAT ||
            op_ct.colDataType == CalpontSystemCatalog::UFLOAT ||
            op_ct.colDataType == CalpontSystemCatalog::VARCHAR ||
            op_ct.colDataType == CalpontSystemCatalog::CHAR ||
            op_ct.colDataType == CalpontSystemCatalog::TEXT)
    {
        snprintf(tmp, 511, "%f", getDoubleVal(row, parm, isNull, op_ct));

        // remove the decimals in the oss string.
        char* d = tmp;

        while ((*d != '.') && (*d != '\0'))
            d++;

        *d = '\0';
    }
    else if (isUnsigned(op_ct.colDataType))
    {
#ifndef __LP64__
        snprintf(tmp, 511, "%llu", getUintVal(row, parm, isNull, op_ct));
#else
        snprintf(tmp, 511, "%lu", getUintVal(row, parm, isNull, op_ct));
#endif
    }
    else
    {
#ifndef __LP64__
        snprintf(tmp, 511, "%lld", getIntVal(row, parm, isNull, op_ct));
#else
        snprintf(tmp, 511, "%ld", getIntVal(row, parm, isNull, op_ct));
#endif
    }

    return string(tmp);
}


} // namespace funcexp
// vim:ts=4 sw=4:

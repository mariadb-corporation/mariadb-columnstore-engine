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
* $Id: func_ceil.cpp 3923 2013-06-19 21:43:06Z bwilkinson $
*
*
****************************************************************************/

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

CalpontSystemCatalog::ColType Func_ceil::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
    return fp[0]->data()->resultType();
}


int64_t Func_ceil::getIntVal(Row& row,
                             FunctionParm& parm,
                             bool& isNull,
                             CalpontSystemCatalog::ColType& op_ct)
{
    int64_t ret = 0;

    switch (op_ct.colDataType)
    {
        case CalpontSystemCatalog::BIGINT:
        case CalpontSystemCatalog::INT:
        case CalpontSystemCatalog::MEDINT:
        case CalpontSystemCatalog::TINYINT:
        case CalpontSystemCatalog::SMALLINT:
        {
            ret = parm[0]->data()->getIntVal(row, isNull);
        }
        break;

        // ceil(decimal(38,38)) leads to this path
        // otherwise Func_ceil::getDecimalVal() is called
        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);

            if (isNull)
                break;

            // negative scale is not supported by CNX yet
            if (d.scale > 0)
            {
                if (d.scale > datatypes::INT128MAXPRECISION)
                {
                    std::ostringstream oss;
                    oss << "ceil: datatype of " << execplan::colDataTypeToString(op_ct.colDataType)
                        << " with scale " << (int) d.scale << " is beyond supported scale";
                    throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
                }

                if (op_ct.colWidth == datatypes::MAXDECIMALWIDTH)
                {
                    int128_t tmp = d.s128Value;
                    int128_t scaleDivisor;
                    datatypes::getScaleDivisor(scaleDivisor, d.scale);
                    d.s128Value /= scaleDivisor;

                    // Add 1 if this is a positive number and there were values to the right of the
                    // decimal point so that we return the largest integer value not less than X.
                    if ((tmp - (d.s128Value * scaleDivisor)) > 0)
                        d.s128Value += 1;

                    ret = datatypes::Decimal::getInt64FromWideDecimal(d.s128Value);
                }
                else
                {
                    int64_t tmp = d.value;
                    d.value /= helpers::powerOf10_c[d.scale];

                    // Add 1 if this is a positive number and there were values to the right of the
                    // decimal point so that we return the largest integer value not less than X.
                    if ((tmp - (d.value * helpers::powerOf10_c[d.scale])) > 0)
                        d.value += 1;

                    ret = d.value;
                }
            }
        }
        break;

        case CalpontSystemCatalog::UBIGINT:
        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UMEDINT:
        case CalpontSystemCatalog::UTINYINT:
        case CalpontSystemCatalog::USMALLINT:
        {
            ret = (int64_t)(parm[0]->data()->getUintVal(row, isNull));
        }
        break;

        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
        {
            ret = (int64_t) ceil(parm[0]->data()->getDoubleVal(row, isNull));
        }
        break;

        case CalpontSystemCatalog::LONGDOUBLE:
        {
            ret = (int64_t) ceill(parm[0]->data()->getLongDoubleVal(row, isNull));
        }
        break;

        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::TEXT:
        {
            const string& str = parm[0]->data()->getStrVal(row, isNull);

            if (!isNull)
                ret = (int64_t) ceil(strtod(str.c_str(), 0));
        }
        break;

        case CalpontSystemCatalog::DATE:
        {
            Date d (parm[0]->data()->getDateIntVal(row, isNull));

            if (!isNull)
                ret = d.convertToMySQLint();
        }
        break;

        case CalpontSystemCatalog::DATETIME:
        {
            DateTime dt(parm[0]->data()->getDatetimeIntVal(row, isNull));

            if (!isNull)
                ret = dt.convertToMySQLint();
        }
        break;

        case CalpontSystemCatalog::TIMESTAMP:
        {
            TimeStamp dt(parm[0]->data()->getTimestampIntVal(row, isNull));

            if (!isNull)
                ret = dt.convertToMySQLint(timeZone());
        }
        break;

        case CalpontSystemCatalog::TIME:
        {
            Time dt(parm[0]->data()->getTimeIntVal(row, isNull));

            if (!isNull)
                ret = dt.convertToMySQLint();
        }
        break;

        default:
        {
            std::ostringstream oss;
            oss << "ceil: datatype of " << colDataTypeToString(op_ct.colDataType)
                << " is not supported";
            throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
        }
    }

    return ret;
}

uint64_t Func_ceil::getUintVal(Row& row,
                               FunctionParm& parm,
                               bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
    uint64_t ret = 0;

    switch (op_ct.colDataType)
    {
        case CalpontSystemCatalog::BIGINT:
        case CalpontSystemCatalog::INT:
        case CalpontSystemCatalog::MEDINT:
        case CalpontSystemCatalog::TINYINT:
        case CalpontSystemCatalog::SMALLINT:
        case CalpontSystemCatalog::DECIMAL:
        case CalpontSystemCatalog::UDECIMAL:
        {
            ret = (uint64_t)parm[0]->data()->getIntVal(row, isNull);
        }
        break;

        case CalpontSystemCatalog::UBIGINT:
        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UMEDINT:
        case CalpontSystemCatalog::UTINYINT:
        case CalpontSystemCatalog::USMALLINT:
        {
            ret = (parm[0]->data()->getUintVal(row, isNull));
        }
        break;

        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
        {
            ret = (uint64_t) ceil(parm[0]->data()->getDoubleVal(row, isNull));
        }
        break;

        case CalpontSystemCatalog::LONGDOUBLE:
        {
            ret = (uint64_t) ceill(parm[0]->data()->getLongDoubleVal(row, isNull));
        }
        break;

        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::TEXT:
        {
            const string& str = parm[0]->data()->getStrVal(row, isNull);

            if (!isNull)
                ret = (uint64_t) ceil(strtod(str.c_str(), 0));
        }
        break;

        case CalpontSystemCatalog::DATE:
        {
            Date d (parm[0]->data()->getDateIntVal(row, isNull));

            if (!isNull)
                ret = d.convertToMySQLint();
        }
        break;

        case CalpontSystemCatalog::DATETIME:
        {
            DateTime dt(parm[0]->data()->getDatetimeIntVal(row, isNull));

            if (!isNull)
                ret = dt.convertToMySQLint();
        }
        break;

        case CalpontSystemCatalog::TIMESTAMP:
        {
            TimeStamp dt(parm[0]->data()->getTimestampIntVal(row, isNull));

            if (!isNull)
                ret = dt.convertToMySQLint(timeZone());
        }
        break;

        case CalpontSystemCatalog::TIME:
        {
            Time dt(parm[0]->data()->getTimeIntVal(row, isNull));

            if (!isNull)
                ret = dt.convertToMySQLint();
        }
        break;

        default:
        {
            std::ostringstream oss;
            oss << "ceil: datatype of " << colDataTypeToString(op_ct.colDataType)
                << " is not supported";
            throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
        }
    }

    return ret;
}


double Func_ceil::getDoubleVal(Row& row,
                               FunctionParm& parm,
                               bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
    double ret = 0.0;

    if (op_ct.colDataType == CalpontSystemCatalog::DOUBLE ||
            op_ct.colDataType == CalpontSystemCatalog::UDOUBLE ||
            op_ct.colDataType == CalpontSystemCatalog::FLOAT ||
            op_ct.colDataType == CalpontSystemCatalog::UFLOAT)
    {
        ret = ceil(parm[0]->data()->getDoubleVal(row, isNull));
    }
    else if (op_ct.colDataType == CalpontSystemCatalog::VARCHAR ||
             op_ct.colDataType == CalpontSystemCatalog::CHAR ||
             op_ct.colDataType == CalpontSystemCatalog::TEXT)
    {
        const string& str = parm[0]->data()->getStrVal(row, isNull);

        if (!isNull)
            ret = ceil(strtod(str.c_str(), 0));
    }
    else if (op_ct.colDataType == CalpontSystemCatalog::LONGDOUBLE)
    {
        ret = (double)ceill(parm[0]->data()->getLongDoubleVal(row, isNull));
    }
    else if (op_ct.colDataType == CalpontSystemCatalog::DECIMAL ||
             op_ct.colDataType == CalpontSystemCatalog::UDECIMAL)
    {
        IDB_Decimal tmp = getDecimalVal(row, parm, isNull, op_ct);

        if (op_ct.colWidth == datatypes::MAXDECIMALWIDTH)
        {
            ret = datatypes::Decimal::getDoubleFromWideDecimal(tmp.s128Value);
        }
        else
        {
            ret = (double) tmp.value;
        }
    }
    else
    {
        if (isUnsigned(op_ct.colDataType))
        {
            ret = (double) getUintVal(row, parm, isNull, op_ct);
        }
        else
        {
            ret = (double) getIntVal(row, parm, isNull, op_ct);
        }
    }

    return ret;
}

long double Func_ceil::getLongDoubleVal(Row& row,
                               FunctionParm& parm,
                               bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
    long double ret = 0.0;

    if (op_ct.colDataType == CalpontSystemCatalog::LONGDOUBLE)
    {
        ret = ceill(parm[0]->data()->getLongDoubleVal(row, isNull));
    }
    else if (op_ct.colDataType == CalpontSystemCatalog::DOUBLE ||
            op_ct.colDataType == CalpontSystemCatalog::UDOUBLE ||
            op_ct.colDataType == CalpontSystemCatalog::FLOAT ||
            op_ct.colDataType == CalpontSystemCatalog::UFLOAT)
    {
        ret = ceil(parm[0]->data()->getDoubleVal(row, isNull));
    }
    else if (op_ct.colDataType == CalpontSystemCatalog::VARCHAR ||
             op_ct.colDataType == CalpontSystemCatalog::CHAR ||
             op_ct.colDataType == CalpontSystemCatalog::TEXT)
    {
        const string& str = parm[0]->data()->getStrVal(row, isNull);

        if (!isNull)
            ret = ceil(strtod(str.c_str(), 0));
    }
    else if (op_ct.colDataType == CalpontSystemCatalog::DECIMAL ||
             op_ct.colDataType == CalpontSystemCatalog::UDECIMAL)
    {
        IDB_Decimal tmp = getDecimalVal(row, parm, isNull, op_ct);

        if (op_ct.colWidth == datatypes::MAXDECIMALWIDTH)
        {
            ret = datatypes::Decimal::getLongDoubleFromWideDecimal(tmp.s128Value);
        }
        else
        {
            ret = (long double) tmp.value;
        }
    }
    else
    {
        if (isUnsigned(op_ct.colDataType))
        {
            ret = (double) getUintVal(row, parm, isNull, op_ct);
        }
        else
        {
            ret = (double) getIntVal(row, parm, isNull, op_ct);
        }
    }

    return ret;
}


string Func_ceil::getStrVal(Row& row,
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
    else if (op_ct.colDataType == CalpontSystemCatalog::LONGDOUBLE)
    {
        snprintf(tmp, 511, "%Lf", getLongDoubleVal(row, parm, isNull, op_ct));

        // remove the decimals in the oss string.
        char* d = tmp;

        while ((*d != '.') && (*d != '\0'))
            d++;

        *d = '\0';
    }
    else if (op_ct.colDataType == CalpontSystemCatalog::DECIMAL ||
             op_ct.colDataType == CalpontSystemCatalog::UDECIMAL)
    {
        IDB_Decimal d = getDecimalVal(row, parm, isNull, op_ct);

        if (op_ct.colWidth == datatypes::MAXDECIMALWIDTH)
        {
            dataconvert::DataConvert::decimalToString(&d.s128Value, d.scale, tmp, 511, op_ct.colDataType);
        }
        else
        {
            dataconvert::DataConvert::decimalToString(d.value, d.scale, tmp, 511, op_ct.colDataType);
        }
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


IDB_Decimal Func_ceil::getDecimalVal(Row& row,
                                      FunctionParm& parm,
                                      bool& isNull,
                                      CalpontSystemCatalog::ColType& op_ct)
{
    IDB_Decimal ret;

    switch (op_ct.colDataType)
    {
        case execplan::CalpontSystemCatalog::BIGINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::TINYINT:
        case execplan::CalpontSystemCatalog::SMALLINT:
        {
            ret.value = parm[0]->data()->getIntVal(row, isNull);
        }
        break;

        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            ret = parm[0]->data()->getDecimalVal(row, isNull);

            if (isNull)
                break;

            // negative scale is not supported by CNX yet
            if (ret.scale > 0)
            {
                if (ret.scale > datatypes::INT128MAXPRECISION)
                {
                    std::ostringstream oss;
                    oss << "ceil: datatype of " << execplan::colDataTypeToString(op_ct.colDataType)
                        << " with scale " << (int) ret.scale << " is beyond supported scale";
                    throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
                }

                if (op_ct.colWidth == datatypes::MAXDECIMALWIDTH)
                {
                    int128_t tmp = ret.s128Value;
                    int128_t scaleDivisor;
                    datatypes::getScaleDivisor(scaleDivisor, ret.scale);
                    ret.s128Value /= scaleDivisor;

                    // Add 1 if this is a positive number and there were values to the right of the
                    // decimal point so that we return the largest integer value not less than X.
                    if ((tmp - (ret.s128Value * scaleDivisor)) > 0)
                        ret.s128Value += 1;
                }
                else
                {
                    int64_t tmp = ret.value;
                    ret.value /= helpers::powerOf10_c[ret.scale];

                    // Add 1 if this is a positive number and there were values to the right of the
                    // decimal point so that we return the largest integer value not less than X.
                    if ((tmp - (ret.value * helpers::powerOf10_c[ret.scale])) > 0)
                        ret.value += 1;
                }
            }
        }
        break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            ret.value = (int64_t)parm[0]->data()->getUintVal(row, isNull);
        }
        break;

        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
        {
            ret.value = (int64_t) ceil(parm[0]->data()->getDoubleVal(row, isNull));
        }
        break;

        case execplan::CalpontSystemCatalog::LONGDOUBLE:
        {
            ret.value = (int64_t) ceill(parm[0]->data()->getLongDoubleVal(row, isNull));
        }
        break;

        case execplan::CalpontSystemCatalog::VARCHAR:
        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        {
            const string& str = parm[0]->data()->getStrVal(row, isNull);

            if (!isNull)
                ret.value = (int64_t) ceil(strtod(str.c_str(), 0));
        }
        break;

        case CalpontSystemCatalog::DATE:
        {
            Date d (parm[0]->data()->getDateIntVal(row, isNull));

            if (!isNull)
                ret.value = d.convertToMySQLint();
        }
        break;

        case CalpontSystemCatalog::DATETIME:
        {
            DateTime dt(parm[0]->data()->getDatetimeIntVal(row, isNull));

            if (!isNull)
                ret.value = dt.convertToMySQLint();
        }
        break;

        case CalpontSystemCatalog::TIMESTAMP:
        {
            TimeStamp dt(parm[0]->data()->getTimestampIntVal(row, isNull));

            if (!isNull)
                ret.value = dt.convertToMySQLint(timeZone());
        }
        break;

        case CalpontSystemCatalog::TIME:
        {
            Time dt(parm[0]->data()->getTimeIntVal(row, isNull));

            if (!isNull)
                ret.value = dt.convertToMySQLint();
        }
        break;

        default:
        {
            std::ostringstream oss;
            oss << "ceil: datatype of " << colDataTypeToString(op_ct.colDataType)
                << " is not supported";
            throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
        }
    }

    ret.scale = 0;

    return ret;
}


} // namespace funcexp
// vim:ts=4 sw=4:

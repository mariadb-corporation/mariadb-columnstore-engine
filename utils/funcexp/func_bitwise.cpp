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
* $Id: func_bitwise.cpp 3616 2013-03-04 14:56:29Z rdempsey $
*
*
****************************************************************************/

#include <string>
using namespace std;

#include "functor_int.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "predicateoperator.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "dataconvert.h"
using namespace dataconvert;

namespace
{
using namespace funcexp;

// @bug 4703 - the actual bug was only in the DATETIME case
// part of this statement below, but instead of leaving 5 identical
// copies of this code, extracted into a single utility function
// here.  This same method is potentially useful in other methods
// and could be extracted into a utility class with its own header
// if that is the case - this is left as future exercise
bool getUIntValFromParm(
    Row&  row,
    const execplan::SPTP& parm,
    uint64_t& value,
    bool& isNull,
    const funcexp::Func& thisFunc,
    bool& isBigVal,
    int128_t& bigval)
{
    isBigVal = false;

    switch (parm->data()->resultType().colDataType)
    {
        case execplan::CalpontSystemCatalog::BIGINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::TINYINT:
        case execplan::CalpontSystemCatalog::SMALLINT:
        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UDOUBLE:
        case execplan::CalpontSystemCatalog::UFLOAT:
        {
            value = parm->data()->getIntVal(row, isNull);
        }
        break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            value = parm->data()->getUintVal(row, isNull);
        }
        break;

        case execplan::CalpontSystemCatalog::VARCHAR:
        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        {
            value = parm->data()->getIntVal(row, isNull);

            if (isNull)
            {
                isNull = true;
            }
        }
        break;

        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            IDB_Decimal d = parm->data()->getDecimalVal(row, isNull);

            if (parm->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
            {
                isBigVal = true;

                if (parm->data()->resultType().colDataType == execplan::CalpontSystemCatalog::UDECIMAL &&
                        d.value < 0)
                {
                    bigval = 0;
                    break;
                }

                int128_t scaleDivisor, scaleDivisor2;

                datatypes::getScaleDivisor(scaleDivisor, d.scale);

                scaleDivisor2 = (scaleDivisor <= 10) ? 1 : (scaleDivisor / 10);

                int128_t tmpval = d.s128Value / scaleDivisor;
                int128_t lefto = (d.s128Value - tmpval * scaleDivisor) / scaleDivisor2;

                if (tmpval >= 0 && lefto > 4)
                    tmpval++;

                if (tmpval < 0 && lefto < -4)
                    tmpval--;

                bigval = tmpval;
            }
            else
            {
                if (parm->data()->resultType().colDataType == execplan::CalpontSystemCatalog::UDECIMAL &&
                        d.value < 0)
                {
                    d.value = 0;
                }
                double dscale = d.scale;
                int64_t tmpval = d.value / pow(10.0, dscale);
                int lefto = (d.value - tmpval * pow(10.0, dscale)) / pow(10.0, dscale - 1);

                if (tmpval >= 0 && lefto > 4)
                    tmpval++;

                if (tmpval < 0 && lefto < -4)
                    tmpval--;

                value = tmpval;
            }
        }
        break;

        case execplan::CalpontSystemCatalog::DATE:
        {
            int32_t time = parm->data()->getDateIntVal(row, isNull);

            Date d(time);
            value = d.convertToMySQLint();
        }
        break;

        case execplan::CalpontSystemCatalog::DATETIME:
        {
            int64_t time = parm->data()->getDatetimeIntVal(row, isNull);

            // @bug 4703 - missing year when convering to int
            DateTime dt(time);
            value = dt.convertToMySQLint();
        }
        break;

        case execplan::CalpontSystemCatalog::TIMESTAMP:
        {
            int64_t time = parm->data()->getTimestampIntVal(row, isNull);

            TimeStamp dt(time);
            value = dt.convertToMySQLint(thisFunc.timeZone());
        }
        break;

        case execplan::CalpontSystemCatalog::TIME:
        {
            int64_t time = parm->data()->getTimeIntVal(row, isNull);

            Time dt(time);
            value = dt.convertToMySQLint();
        }
        break;

        default:
        {
            return false;
        }
    }

    return true;
}
}

namespace funcexp
{

//
// BITAND
//


CalpontSystemCatalog::ColType Func_bitand::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}

int64_t Func_bitand::getIntVal(Row& row,
                               FunctionParm& parm,
                               bool& isNull,
                               CalpontSystemCatalog::ColType& operationColType)
{
    if ( parm.size() < 2 )
    {
        isNull = true;
        return 0;
    }

    uint64_t val1 = 0;
    uint64_t val2 = 0;

    int128_t bigval1 = 0;
    int128_t bigval2 = 0;
    bool isBigVal1;
    bool isBigVal2;

    if (!getUIntValFromParm(row, parm[0], val1, isNull, *this, isBigVal1, bigval1) ||
            !getUIntValFromParm(row, parm[1], val2, isNull, *this, isBigVal2, bigval2))
    {
        std::ostringstream oss;
        oss << "bitand: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
        throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }

    if (LIKELY(!isBigVal1 && !isBigVal2))
    {
        return val1 & val2;
    }

    // Type promotion to int128_t
    if (!isBigVal1)
        bigval1 = val1;

    if (!isBigVal2)
        bigval2 = val2;

    int128_t res = bigval1 & bigval2;

    if (res > static_cast<int128_t>(UINT64_MAX))
        res = UINT64_MAX;
    else if (res < static_cast<int128_t>(INT64_MIN))
        res = INT64_MIN;

    return (int64_t) res;
}


//
// LEFT SHIFT
//


CalpontSystemCatalog::ColType Func_leftshift::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}

int64_t Func_leftshift::getIntVal(Row& row,
                                  FunctionParm& parm,
                                  bool& isNull,
                                  CalpontSystemCatalog::ColType& operationColType)
{
    if ( parm.size() < 2 )
    {
        isNull = true;
        return 0;
    }

    uint64_t val1 = 0;
    uint64_t val2 = 0;

    int128_t bigval1 = 0;
    int128_t bigval2 = 0;
    bool isBigVal1;
    bool isBigVal2;

    if (!getUIntValFromParm(row, parm[0], val1, isNull, *this, isBigVal1, bigval1) ||
            !getUIntValFromParm(row, parm[1], val2, isNull, *this, isBigVal2, bigval2))
    {
        std::ostringstream oss;
        oss << "leftshift: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
        throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }

    if (LIKELY(!isBigVal1 && !isBigVal2))
    {
        return val1 << val2;
    }

    // Type promotion to int128_t
    if (!isBigVal1)
        bigval1 = val1;

    if (!isBigVal2)
        bigval2 = val2;

    int128_t res = bigval1 << bigval2;

    if (res > static_cast<int128_t>(UINT64_MAX))
        res = UINT64_MAX;
    else if (res < static_cast<int128_t>(INT64_MIN))
        res = INT64_MIN;

    return (int64_t) res;
}


//
// RIGHT SHIFT
//


CalpontSystemCatalog::ColType Func_rightshift::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}

int64_t Func_rightshift::getIntVal(Row& row,
                                   FunctionParm& parm,
                                   bool& isNull,
                                   CalpontSystemCatalog::ColType& operationColType)
{
    if ( parm.size() < 2 )
    {
        isNull = true;
        return 0;
    }

    uint64_t val1 = 0;
    uint64_t val2 = 0;

    int128_t bigval1 = 0;
    int128_t bigval2 = 0;
    bool isBigVal1;
    bool isBigVal2;

    if (!getUIntValFromParm(row, parm[0], val1, isNull, *this, isBigVal1, bigval1) ||
            !getUIntValFromParm(row, parm[1], val2, isNull, *this, isBigVal2, bigval2))
    {
        std::ostringstream oss;
        oss << "rightshift: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
        throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }

    if (LIKELY(!isBigVal1 && !isBigVal2))
    {
        return val1 >> val2;
    }

    // Type promotion to int128_t
    if (!isBigVal1)
        bigval1 = val1;

    if (!isBigVal2)
        bigval2 = val2;

    int128_t res = bigval1 >> bigval2;

    if (res > static_cast<int128_t>(UINT64_MAX))
        res = UINT64_MAX;
    else if (res < static_cast<int128_t>(INT64_MIN))
        res = INT64_MIN;

    return (int64_t) res;
}


//
// BIT OR
//


CalpontSystemCatalog::ColType Func_bitor::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}

int64_t Func_bitor::getIntVal(Row& row,
                              FunctionParm& parm,
                              bool& isNull,
                              CalpontSystemCatalog::ColType& operationColType)
{
    if ( parm.size() < 2 )
    {
        isNull = true;
        return 0;
    }

    uint64_t val1 = 0;
    uint64_t val2 = 0;

    int128_t bigval1 = 0;
    int128_t bigval2 = 0;
    bool isBigVal1;
    bool isBigVal2;

    if (!getUIntValFromParm(row, parm[0], val1, isNull, *this, isBigVal1, bigval1) ||
            !getUIntValFromParm(row, parm[1], val2, isNull, *this, isBigVal2, bigval2))
    {
        std::ostringstream oss;
        oss << "bitor: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
        throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }

    if (LIKELY(!isBigVal1 && !isBigVal2))
    {
        return val1 | val2;
    }

    // Type promotion to int128_t
    if (!isBigVal1)
        bigval1 = val1;

    if (!isBigVal2)
        bigval2 = val2;

    int128_t res = bigval1 | bigval2;

    if (res > static_cast<int128_t>(UINT64_MAX))
        res = UINT64_MAX;
    else if (res < static_cast<int128_t>(INT64_MIN))
        res = INT64_MIN;

    return (int64_t) res;
}

uint64_t Func_bitor::getUintVal(rowgroup::Row& row,
                                FunctionParm& fp,
                                bool& isNull,
                                execplan::CalpontSystemCatalog::ColType& op_ct)
{
    return static_cast<uint64_t>(getIntVal(row, fp, isNull, op_ct));
}


//
// BIT XOR
//


CalpontSystemCatalog::ColType Func_bitxor::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}

int64_t Func_bitxor::getIntVal(Row& row,
                               FunctionParm& parm,
                               bool& isNull,
                               CalpontSystemCatalog::ColType& operationColType)
{
    if ( parm.size() < 2 )
    {
        isNull = true;
        return 0;
    }

    uint64_t val1 = 0;
    uint64_t val2 = 0;

    int128_t bigval1 = 0;
    int128_t bigval2 = 0;
    bool isBigVal1;
    bool isBigVal2;

    if (!getUIntValFromParm(row, parm[0], val1, isNull, *this, isBigVal1, bigval1) ||
            !getUIntValFromParm(row, parm[1], val2, isNull, *this, isBigVal2, bigval2))
    {
        std::ostringstream oss;
        oss << "bitxor: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
        throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }

    if (LIKELY(!isBigVal1 && !isBigVal2))
    {
        return val1 ^ val2;
    }

    // Type promotion to int128_t
    if (!isBigVal1)
        bigval1 = val1;

    if (!isBigVal2)
        bigval2 = val2;

    int128_t res = bigval1 ^ bigval2;

    if (res > static_cast<int128_t>(UINT64_MAX))
        res = UINT64_MAX;
    else if (res < static_cast<int128_t>(INT64_MIN))
        res = INT64_MIN;

    return (int64_t) res;
}


//
// BIT COUNT
//


CalpontSystemCatalog::ColType Func_bit_count::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}

inline int64_t bitCount(uint64_t val)
{
    // Refer to Hacker's Delight Chapter 5
    // for the bit counting algo used here
    val = val - ((val >> 1) & 0x5555555555555555);
    val = (val & 0x3333333333333333) + ((val >> 2) & 0x3333333333333333);
    val = (val + (val >> 4)) & 0x0F0F0F0F0F0F0F0F;
    val = val + (val >> 8);
    val = val + (val >> 16);
    val = val + (val >> 32);

    return (int64_t)(val & 0x000000000000007F);
}

int64_t Func_bit_count::getIntVal(Row& row,
                                  FunctionParm& parm,
                                  bool& isNull,
                                  CalpontSystemCatalog::ColType& operationColType)
{
    if ( parm.size() != 1 )
    {
        isNull = true;
        return 0;
    }

    uint64_t val = 0;

    int128_t bigval = 0;
    bool isBigVal;

    if (!getUIntValFromParm(row, parm[0], val, isNull, *this, isBigVal, bigval))
    {
        std::ostringstream oss;
        oss << "bit_count: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
        throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }

    if (LIKELY(!isBigVal))
    {
        return bitCount(val);
    }
    else
    {
        return (bitCount(*reinterpret_cast<uint64_t*>(&bigval)) +
                bitCount(*(reinterpret_cast<uint64_t*>(&bigval) + 1)));
    }
}


} // namespace funcexp
// vim:ts=4 sw=4:

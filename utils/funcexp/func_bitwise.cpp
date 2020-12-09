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

#include "functionhelper.h"
#include "mcs_functional.h"

namespace
{
using namespace funcexp;

// @bug 4703 - the actual bug was only in the DATETIME case
// part of this statement below, but instead of leaving 5 identical
// copies of this code, extracted into a single utility function
// here.  This same method is potentially useful in other methods
// and could be extracted into a utility class with its own header
// if that is the case - this is left as future exercise
uint64_t getUIntValFromParm(
    Row&  row,
    const execplan::SPTP& parm,
    bool& isNull,
    bool& isNegative,
    bool& isUnknown,
    const funcexp::Func& thisFunc)
{
    if (isNull)
    {
        return 0;
    }
    
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
            return parm->data()->getIntVal(row, isNull);
        }
        break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            return parm->data()->getUintVal(row, isNull);
        }
        break;

        case execplan::CalpontSystemCatalog::VARCHAR:
        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        {
            return parm->data()->getIntVal(row, isNull);
        }
        break;

        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            IDB_Decimal d = parm->data()->getDecimalVal(row, isNull);

            uint8_t roundingFactor = 4;
            bool isUnsignedDecimal = parm->data()->resultType().colDataType ==
                                        execplan::CalpontSystemCatalog::UDECIMAL;

            auto methodPtr = (parm->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH) ?
                                &datatypes::Decimal::strangeMDBTypeCast :
                                &datatypes::Decimal::strangeMDBTypeCastTUInt64;
            auto valueAndIsNegative = CALL_MEMBER_FN(d, methodPtr)(roundingFactor);
            isNegative = valueAndIsNegative.second;
            return (isUnsignedDecimal && isNegative) ? 0 : valueAndIsNegative.first;

        }
        break;

        case execplan::CalpontSystemCatalog::DATE:
        {
            int32_t time = parm->data()->getDateIntVal(row, isNull);

            Date d(time);
            return d.convertToMySQLint();
        }
        break;

        case execplan::CalpontSystemCatalog::DATETIME:
        {
            int64_t time = parm->data()->getDatetimeIntVal(row, isNull);

            // @bug 4703 - missing year when convering to int
            DateTime dt(time);
            return dt.convertToMySQLint();
        }
        break;

        case execplan::CalpontSystemCatalog::TIMESTAMP:
        {
            int64_t time = parm->data()->getTimestampIntVal(row, isNull);

            TimeStamp dt(time);
            return dt.convertToMySQLint(thisFunc.timeZone());
        }
        break;

        case execplan::CalpontSystemCatalog::TIME:
        {
            int64_t time = parm->data()->getTimeIntVal(row, isNull);

            Time dt(time);
            return dt.convertToMySQLint();
        }
        break;

        default:
        {
            isUnknown = true;
        }
    }
    return 0;
}

void bitWiseExceptionHandler(const std::string& prefix,
                             const CalpontSystemCatalog::ColType& colType)
{
    std::ostringstream oss;
    oss << prefix << execplan::colDataTypeToString(colType.colDataType);
    throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
}

template<typename Op>
int64_t BitwiseFunctional(rowgroup::Row& row,
                          FunctionParm& parm,
                          const CalpontSystemCatalog::ColType& operationColType,
                          bool& isNull,
                          const funcexp::Func& callingFunc,
                          const std::string& errorPrefix)
{
    if ( parm.size() < 2 )
    {
        isNull = true;
        return 0;
    }

    bool isNegativeL = false, isNegativeR = false;
    bool isUnknownL = false, isUnknownR = false;
    uint64_t valL = getUIntValFromParm(row,
                                       parm[0],
                                       isNull,
                                       isNegativeL,
                                       isUnknownL,
                                       callingFunc);
    uint64_t valR = getUIntValFromParm(row,
                                       parm[1],
                                       isNull,
                                       isNegativeR,
                                       isUnknownR,
                                       callingFunc);

    if (isUnknownL || isUnknownR)
    {
        bitWiseExceptionHandler(errorPrefix, operationColType);
    }
    int64_t signL = (isNegativeL) ? -1 : 1;
    int64_t signR = (isNegativeR) ? -1 : 1;

    Op op;

    return (int64_t)op(signL * valL, signR * valR);
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
    return BitwiseFunctional<std::bit_and<int64_t>>(row,
                                                    parm,
                                                    operationColType,
                                                    isNull,
                                                    *this,
                                                    std::string("bitand: datatype of "));
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
    return BitwiseFunctional<utils::left_shift<uint64_t>>(row,
                                                         parm,
                                                         operationColType,
                                                         isNull,
                                                         *this,
                                                         std::string("leftshift: datatype of "));
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
    return BitwiseFunctional<utils::right_shift<uint64_t>>(row,
                                                          parm,
                                                          operationColType,
                                                          isNull,
                                                          *this,
                                                          std::string("rightshift: datatype of "));
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
    return BitwiseFunctional<std::bit_or<uint64_t>>(row,
                                                   parm,
                                                   operationColType,
                                                   isNull,
                                                   *this,
                                                   std::string("bitor: datatype of "));
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
    return BitwiseFunctional<std::bit_xor<int64_t>>(row,
                                                    parm,
                                                    operationColType,
                                                    isNull,
                                                    *this,
                                                    std::string("bitxor: datatype of "));
}


//
// BIT COUNT
//


CalpontSystemCatalog::ColType Func_bit_count::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
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

    bool isNegative = false, isUnknown = false;
    uint64_t val = getUIntValFromParm(row,
                                      parm[0],
                                      isNull,
                                      isNegative,
                                      isUnknown,
                                      *this);
    if (isUnknown)
    {
        bitWiseExceptionHandler(std::string("bit_count: datatype of "),
                                operationColType);
    }
    int64_t sign = (isNegative) ? -1 : 1;

    return bitCount(val * sign);
}


} // namespace funcexp
// vim:ts=4 sw=4:

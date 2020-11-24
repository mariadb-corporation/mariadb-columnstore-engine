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
* $Id: func_mod.cpp 3616 2013-03-04 14:56:29Z rdempsey $
*
*
****************************************************************************/

#include <string>
using namespace std;

#include <quadmath.h>

#include "functor_real.h"
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


namespace funcexp
{

CalpontSystemCatalog::ColType Func_mod::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}


IDB_Decimal Func_mod::getDecimalVal(Row& row,
                                    FunctionParm& parm,
                                    bool& isNull,
                                    CalpontSystemCatalog::ColType& operationColType)
{
    IDB_Decimal retValue;
    retValue.value = 0;
    retValue.scale = 0;

    if ( parm.size() < 2 )
    {
        isNull = true;
        return retValue;
    }

    if (parm[0]->data()->resultType().isWideDecimalType() ||
        parm[1]->data()->resultType().isWideDecimalType())
    {
        IDB_Decimal div = parm[1]->data()->getDecimalVal(row, isNull);

        int128_t divInt, dividendInt;

        if (parm[1]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
            divInt = div.s128Value;
        else
            divInt = div.value;

        if (divInt == 0)
        {
            isNull = true;
            return retValue;
        }

        IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);

        if (parm[0]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
            dividendInt = d.s128Value;
        else
            dividendInt = d.value;

        // integer division
        if (d.scale == 0 && div.scale == 0)
        {
            retValue.s128Value = dividendInt % divInt;
        }
        // special case integer division
        else if (div.scale == 0)
        {
            int128_t scaleDivisor;
            datatypes::getScaleDivisor(scaleDivisor, d.scale);
            int128_t value = dividendInt / scaleDivisor;
            int128_t lefto = dividendInt % scaleDivisor;
            int128_t mod = (value % divInt) * scaleDivisor + lefto;
            retValue.s128Value = mod;
        }
        // float division
        else
        {
            __float128 divF, dividendF;

            int128_t scaleDivisor;

            datatypes::getScaleDivisor(scaleDivisor, div.scale);
            divF = (__float128) divInt / scaleDivisor;

            datatypes::getScaleDivisor(scaleDivisor, d.scale);
            dividendF = (__float128) dividendInt / scaleDivisor;

            __float128 mod = fmodq(dividendF, divF) * scaleDivisor;

            retValue.s128Value = (int128_t) mod;
        }
        retValue.scale = d.scale;
        retValue.precision = datatypes::INT128MAXPRECISION;
    }
    else
    {
        int64_t div = parm[1]->data()->getIntVal(row, isNull);

        if ( div == 0 )
        {
            isNull = true;
            return retValue;
        }

        IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
        int64_t value = d.value / pow(10.0, d.scale);
        int lefto = d.value % (int)pow(10.0, d.scale);

        int64_t mod = (value % div) * pow(10.0, d.scale) + lefto;

        retValue.value = mod;
        retValue.scale = d.scale;
    }

    return retValue;
}


double Func_mod::getDoubleVal(Row& row,
                              FunctionParm& parm,
                              bool& isNull,
                              CalpontSystemCatalog::ColType& operationColType)
{
    if ( parm.size() < 2 )
    {
        isNull = true;
        return 0;
    }

    int64_t div = parm[1]->data()->getIntVal(row, isNull);

    if ( div == 0 )
    {
        isNull = true;
        return 0;
    }

    double mod = 0;

    switch (parm[0]->data()->resultType().colDataType)
    {
        case execplan::CalpontSystemCatalog::BIGINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::TINYINT:
        case execplan::CalpontSystemCatalog::SMALLINT:
        {
            int64_t value = parm[0]->data()->getIntVal(row, isNull);

            mod = value % div;
        }
        break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            uint64_t udiv = parm[1]->data()->getIntVal(row, isNull);
            uint64_t uvalue = parm[0]->data()->getUintVal(row, isNull);

            mod = uvalue % udiv;
        }
        break;

        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
        {
            double value = parm[0]->data()->getDoubleVal(row, isNull);

            mod = fmod(value, div);
        }
        break;

        case execplan::CalpontSystemCatalog::LONGDOUBLE:
        {
            long double value = parm[0]->data()->getLongDoubleVal(row, isNull);

            mod = (double)fmodl(value, div);
        }
        break;

        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
        {
            float value = parm[0]->data()->getFloatVal(row, isNull);

            mod = fmod(value, div);
        }
        break;

        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);

            if (parm[0]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
            {
                if (d.scale == 0)
                {
                    mod = d.s128Value % div;
                }
                else
                {
                    auto intAndFract = d.getIntegralAndFractional<datatypes::TFloat128>();

                    datatypes::TSInt128 integralRemainder = intAndFract.first % div;
                    mod = static_cast<double>(integralRemainder.toTFloat128() +
                                                   intAndFract.second);

                }
            }
            else
            {
                int64_t value = d.value / pow(10.0, d.scale);
                mod = value % div;
            }
        }
        break;

        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::VARCHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        {
            double value = parm[0]->data()->getDoubleVal(row, isNull);
            mod = fmod(value, div);
            break;
        }

        default:
        {
            std::ostringstream oss;
            oss << "mod: datatype of " << execplan::colDataTypeToString(parm[0]->data()->resultType().colDataType);
            throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
        }
    }

    return mod;
}

long double Func_mod::getLongDoubleVal(Row& row,
                              FunctionParm& parm,
                              bool& isNull,
                              CalpontSystemCatalog::ColType& operationColType)
{
    if ( parm.size() < 2 )
    {
        isNull = true;
        return 0;
    }

    int64_t div = parm[1]->data()->getIntVal(row, isNull);

    if ( div == 0 )
    {
        isNull = true;
        return 0;
    }

    long double mod = 0;

    switch (parm[0]->data()->resultType().colDataType)
    {
        case execplan::CalpontSystemCatalog::BIGINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::TINYINT:
        case execplan::CalpontSystemCatalog::SMALLINT:
        {
            int64_t value = parm[0]->data()->getIntVal(row, isNull);

            mod = value % div;
        }
        break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            uint64_t udiv = parm[1]->data()->getIntVal(row, isNull);
            uint64_t uvalue = parm[0]->data()->getUintVal(row, isNull);

            mod = uvalue % udiv;
        }
        break;

        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
        {
            double value = parm[0]->data()->getDoubleVal(row, isNull);

            mod = fmod(value, div);
        }
        break;

        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
        {
            float value = parm[0]->data()->getFloatVal(row, isNull);

            mod = fmod(value, div);
        }
        break;

        case execplan::CalpontSystemCatalog::LONGDOUBLE:
        {
            long double value = parm[0]->data()->getLongDoubleVal(row, isNull);

            mod = fmodl(value, div);
        }
        break;

        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);

            if (parm[0]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
            {
                if (d.scale == 0)
                {
                    mod = d.s128Value % div;
                }
                else
                {
                    auto intAndFract = d.getIntegralAndFractional<datatypes::TFloat128>();

                    datatypes::TSInt128 integralRemainder = intAndFract.first % div;
                    mod = static_cast<long double>(integralRemainder.toTFloat128() +
                                                   intAndFract.second);
                }
            }
            else
            {
                int64_t value = d.value / pow(10.0, d.scale);
                mod = value % div;
            }
        }
        break;

        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::VARCHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        {
            double value = parm[0]->data()->getDoubleVal(row, isNull);
            mod = fmod(value, div);
            break;
        }

        default:
        {
            std::ostringstream oss;
            oss << "mod: datatype of " << execplan::colDataTypeToString(parm[0]->data()->resultType().colDataType);
            throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
        }
    }

    return mod;
}

int64_t Func_mod::getIntVal(Row& row,
                            FunctionParm& parm,
                            bool& isNull,
                            CalpontSystemCatalog::ColType& operationColType)
{
    if ( parm.size() < 2 )
    {
        isNull = true;
        return 0;
    }

    int64_t div = parm[1]->data()->getIntVal(row, isNull);

    if ( div == 0 )
    {
        isNull = true;
        return 0;
    }

    int64_t mod = 0;

    switch (parm[0]->data()->resultType().colDataType)
    {
        case execplan::CalpontSystemCatalog::BIGINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::TINYINT:
        case execplan::CalpontSystemCatalog::SMALLINT:
        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        case execplan::CalpontSystemCatalog::VARCHAR:
        {
            int64_t value = parm[0]->data()->getIntVal(row, isNull);

            mod = value % div;
        }
        break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            uint64_t udiv = parm[1]->data()->getIntVal(row, isNull);
            uint64_t uvalue = parm[0]->data()->getUintVal(row, isNull);

            mod = uvalue % udiv;
        }
        break;

        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
        {
            double value = parm[0]->data()->getDoubleVal(row, isNull);

            mod = fmod(value, div);
        }
        break;

        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
        {
            float value = parm[0]->data()->getFloatVal(row, isNull);

            mod = fmod(value, div);
        }
        break;

        case execplan::CalpontSystemCatalog::LONGDOUBLE:
        {
            long double value = parm[0]->data()->getLongDoubleVal(row, isNull);

            mod = fmodl(value, div);
        }
        break;

        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);

            if (parm[0]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
            {
                if (d.scale == 0)
                {
                    mod = d.s128Value % div;
                }
                else
                {
                    auto intAndFract = d.getIntegralAndFractional<datatypes::TFloat128>();
                    datatypes::TSInt128 integralRemainder = intAndFract.first % div;
                    mod = static_cast<int64_t>(integralRemainder.toTFloat128() +
                                               intAndFract.second);
                }
            }
            else
            {
                int64_t value = d.value / pow(10.0, d.scale);
                mod = value % div;
            }
        }
        break;

        default:
        {
            std::ostringstream oss;
            oss << "mod: datatype of " << execplan::colDataTypeToString(parm[0]->data()->resultType().colDataType);
            throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
        }
    }

    return mod;
}

uint64_t Func_mod::getUIntVal(Row& row,
                              FunctionParm& parm,
                              bool& isNull,
                              CalpontSystemCatalog::ColType& operationColType)
{
    if ( parm.size() < 2 )
    {
        isNull = true;
        return 0;
    }

    int64_t div = parm[1]->data()->getIntVal(row, isNull);

    if ( div == 0 )
    {
        isNull = true;
        return 0;
    }

    uint64_t mod = 0;

    switch (parm[0]->data()->resultType().colDataType)
    {
        case execplan::CalpontSystemCatalog::BIGINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::TINYINT:
        case execplan::CalpontSystemCatalog::SMALLINT:
        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        case execplan::CalpontSystemCatalog::VARCHAR:
        {
            int64_t value = parm[0]->data()->getIntVal(row, isNull);

            mod = value % div;
        }
        break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            uint64_t udiv = parm[1]->data()->getIntVal(row, isNull);
            uint64_t uvalue = parm[0]->data()->getUintVal(row, isNull);

            mod = uvalue % udiv;
        }
        break;

        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
        {
            double value = parm[0]->data()->getDoubleVal(row, isNull);

            mod = fmod(value, div);
        }
        break;

        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
        {
            float value = parm[0]->data()->getFloatVal(row, isNull);

            mod = fmod(value, div);
        }
        break;

        case execplan::CalpontSystemCatalog::LONGDOUBLE:
        {
            long double value = parm[0]->data()->getLongDoubleVal(row, isNull);

            mod = fmodl(value, div);
        }
        break;

        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);

            if (parm[0]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
            {
                if (d.scale == 0)
                {
                    mod = d.s128Value % div;
                }
                else
                {
                    auto intAndFract = d.getIntegralAndFractional<datatypes::TFloat128>();
                    datatypes::TSInt128 integralRemainder = intAndFract.first % div;
                    mod = static_cast<uint64_t>(integralRemainder.toTFloat128() +
                                               intAndFract.second);

                }
            }
            else
            {
                int64_t value = d.value / pow(10.0, d.scale);
                mod = value % div;
            }
        }
        break;

        default:
        {
            std::ostringstream oss;
            oss << "mod: datatype of " << execplan::colDataTypeToString(parm[0]->data()->resultType().colDataType);
            throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
        }
    }

    return mod;
}

std::string Func_mod::getStrVal(Row& row,
                                FunctionParm& fp,
                                bool& isNull,
                                CalpontSystemCatalog::ColType& op_ct)
{
    if ( fp.size() < 2 )
    {
        isNull = true;
        return std::string();
    }

    switch (fp[0]->data()->resultType().colDataType)
    {
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
            return intToString(getIntVal(row, fp, isNull, op_ct));
            break;

        case execplan::CalpontSystemCatalog::LONGDOUBLE:
            return longDoubleToString(getLongDoubleVal(row, fp, isNull, op_ct));
            break;

        default:
            return doubleToString(getDoubleVal(row, fp, isNull, op_ct));
            break;
    }
}

} // namespace funcexp
// vim:ts=4 sw=4:

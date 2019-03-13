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
* $Id: func_char.cpp 3931 2013-06-21 20:17:28Z bwilkinson $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_str.h"
#include "funchelpers.h"
#include "functioncolumn.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

namespace
{

// buf must be at least 9 characters since given 64-bit input
// we will convert at most 8 characters and then add the null
inline bool getChar( uint64_t value, char* buf )
{
    uint32_t cur_offset = 0; // current index into buf
    int  cur_bitpos = 56; // 8th octet in input val

    while ( cur_bitpos >= 0 )
    {
        if ( ( ( value >> cur_bitpos ) & 0xff ) != 0 )
        {
            buf[cur_offset++] = char( ( value >> cur_bitpos ) & 0xff );
        }

        cur_bitpos -= 8;
    }

    buf[cur_offset] = '\0';

    return true;
}

// see comment above regarding buf assumptions
inline bool getChar( int64_t value, char* buf )
{
    if ( value < 0 )
        return false;
    else
        return getChar( (uint64_t) value, buf );
}
}

namespace funcexp
{
CalpontSystemCatalog::ColType Func_char::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
    // operation type is not used by this functor
    return fp[0]->data()->resultType();
}

string Func_char::getStrVal(Row& row,
                            FunctionParm& parm,
                            bool& isNull,
                            CalpontSystemCatalog::ColType& ct)
{
    const int BUF_SIZE = 9; // see comment above for size requirement
    char buf[BUF_SIZE];

    switch (ct.colDataType)
    {
        case execplan::CalpontSystemCatalog::BIGINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::TINYINT:
        case execplan::CalpontSystemCatalog::SMALLINT:
        {
            int64_t value = parm[0]->data()->getIntVal(row, isNull);

            if ( !getChar(value, buf) )
            {
                isNull = true;
                return "";
            }
        }
        break;

        case execplan::CalpontSystemCatalog::UBIGINT:
        case execplan::CalpontSystemCatalog::UINT:
        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UTINYINT:
        case execplan::CalpontSystemCatalog::USMALLINT:
        {
            uint64_t value = parm[0]->data()->getUintVal(row, isNull);

            if ( !getChar(value, buf) )
            {
                isNull = true;
                return "";
            }
        }
        break;

        case execplan::CalpontSystemCatalog::VARCHAR: // including CHAR'
        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
        {
            double value = parm[0]->data()->getDoubleVal(row, isNull);

            if ( !getChar((int64_t)value, buf) )
            {
                isNull = true;
                return "";
            }
        }
        break;

        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
        {
            float value = parm[0]->data()->getFloatVal(row, isNull);

            if ( !getChar((int64_t)value, buf) )
            {
                isNull = true;
                return "";
            }
        }
        break;

        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
        {
            IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
            // get decimal and round up
            int value = d.value / helpers::power(d.scale);
            int lefto = (d.value - value * helpers::power(d.scale)) / helpers::power(d.scale - 1);

            if ( lefto > 4 )
                value++;

            if ( !getChar((int64_t)value, buf) )
            {
                isNull = true;
                return "";
            }
        }
        break;

        case execplan::CalpontSystemCatalog::DATE:
        case execplan::CalpontSystemCatalog::DATETIME:
        {
            isNull = true;
            return "";
        }
        break;

        default:
        {
            std::ostringstream oss;
            oss << "char: datatype of " << execplan::colDataTypeToString(ct.colDataType);
            throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
        }

    }

    // Bug 5110 : Here the data in col is null. But there might have other
    // non-null columns we processed before and we do not want entire value
    // to become null. Therefore we set isNull flag to false.
    if (isNull)
    {
        isNull = false;
        return "";
    }

    return buf;
}


} // namespace funcexp
// vim:ts=4 sw=4:

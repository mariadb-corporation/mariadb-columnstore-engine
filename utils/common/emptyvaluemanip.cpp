/* Copyright (C) 2020 MariaDB Corporation

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

#include "emptyvaluemanip.h"

namespace utils
{

void getEmptyRowValue(const execplan::CalpontSystemCatalog::ColDataType colDataType,
                      const int width, uint8_t* emptyVal)
{
    switch (colDataType)
    {
        case execplan::CalpontSystemCatalog::TINYINT:
            *(uint8_t*)emptyVal = joblist::TINYINTEMPTYROW;
            break;

        case execplan::CalpontSystemCatalog::SMALLINT:
            *(uint16_t*)emptyVal = joblist::SMALLINTEMPTYROW;
            break;

        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::INT:
            *(uint32_t*)emptyVal = joblist::INTEMPTYROW;
            break;

        case execplan::CalpontSystemCatalog::BIGINT:
            *(uint64_t*)emptyVal = joblist::BIGINTEMPTYROW;
            break;

        case execplan::CalpontSystemCatalog::UTINYINT:
            *(uint8_t*)emptyVal = joblist::UTINYINTEMPTYROW;
            break;

        case execplan::CalpontSystemCatalog::USMALLINT:
            *(uint16_t*)emptyVal = joblist::USMALLINTEMPTYROW;
            break;

        case execplan::CalpontSystemCatalog::UMEDINT:
        case execplan::CalpontSystemCatalog::UINT:
            *(uint32_t*)emptyVal = joblist::UINTEMPTYROW;
            break;

        case execplan::CalpontSystemCatalog::UBIGINT:
            *(uint64_t*)emptyVal = joblist::UBIGINTEMPTYROW;
            break;

        case execplan::CalpontSystemCatalog::FLOAT:
        case execplan::CalpontSystemCatalog::UFLOAT:
            *(uint32_t*)emptyVal = joblist::FLOATEMPTYROW;
            break;

        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::UDOUBLE:
            *(uint64_t*)emptyVal = joblist::DOUBLEEMPTYROW;
            break;

        case execplan::CalpontSystemCatalog::DECIMAL:
        case execplan::CalpontSystemCatalog::UDECIMAL:
            if (width <= 1)
                *(uint8_t*)emptyVal = joblist::TINYINTEMPTYROW;
            else if (width <= 2)
                *(uint16_t*)emptyVal = joblist::SMALLINTEMPTYROW;
            else if (width <= 4)
                *(uint32_t*)emptyVal = joblist::INTEMPTYROW;
            else if (width <= 8)
                *(uint64_t*)emptyVal = joblist::BIGINTEMPTYROW;
            else
                datatypes::Decimal::setWideDecimalEmptyValue(*(reinterpret_cast<int128_t*>(emptyVal)));
            break;

        //case CalpontSystemCatalog::BINARY:
        //    emptyVal = joblist::BINARYEMPTYROW;
        //    break;

        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::VARCHAR:
        case execplan::CalpontSystemCatalog::DATE:
        case execplan::CalpontSystemCatalog::DATETIME:
        case execplan::CalpontSystemCatalog::TIMESTAMP:
        case execplan::CalpontSystemCatalog::TIME:
        case execplan::CalpontSystemCatalog::VARBINARY:
        case execplan::CalpontSystemCatalog::BLOB:
        case execplan::CalpontSystemCatalog::TEXT:
        default:
            *(uint8_t*)emptyVal = joblist::CHAR1EMPTYROW;
            int offset = (colDataType == execplan::CalpontSystemCatalog::VARCHAR) ? -1 : 0;

            if (width == (2 + offset))
                *(uint16_t*)emptyVal = joblist::CHAR2EMPTYROW;
            else if (width >= (3 + offset) && width <= (4 + offset))
                *(uint32_t*)emptyVal = joblist::CHAR4EMPTYROW;
            else if (width >= (5 + offset))
                *(uint64_t*)emptyVal = joblist::CHAR8EMPTYROW;

            break;
    }
}

} // namespace utils

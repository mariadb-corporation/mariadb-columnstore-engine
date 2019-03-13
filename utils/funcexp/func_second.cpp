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
* $Id: func_second.cpp 3495 2013-01-21 14:09:51Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_second::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}


int64_t Func_second::getIntVal(rowgroup::Row& row,
                               FunctionParm& parm,
                               bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
    int64_t val = 0;

    switch (parm[0]->data()->resultType().colDataType)
    {
        case execplan::CalpontSystemCatalog::BIGINT:
        case execplan::CalpontSystemCatalog::INT:
        case execplan::CalpontSystemCatalog::MEDINT:
        case execplan::CalpontSystemCatalog::TINYINT:
        case execplan::CalpontSystemCatalog::SMALLINT:
        {
            val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));

            if (val == -1)
                isNull = true;

            break;
        }

        case execplan::CalpontSystemCatalog::DECIMAL:
        {
            if (parm[0]->data()->resultType().scale)
            {
                val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));

                if (val == -1)
                    isNull = true;
            }

            break;
        }

        case execplan::CalpontSystemCatalog::DOUBLE:
        case execplan::CalpontSystemCatalog::FLOAT:
        {
            isNull = true;
        }

        case execplan::CalpontSystemCatalog::VARCHAR:
        case execplan::CalpontSystemCatalog::CHAR:
        case execplan::CalpontSystemCatalog::TEXT:
        {
            val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));

            if (val == -1)
                isNull = true;

            break;
        }

        case execplan::CalpontSystemCatalog::DATE:
        {
            val = parm[0]->data()->getDatetimeIntVal(row, isNull);
            break;
        }

        case execplan::CalpontSystemCatalog::DATETIME:
        {
            val = parm[0]->data()->getDatetimeIntVal(row, isNull);
            break;
        }

        case execplan::CalpontSystemCatalog::TIME:
        {
            val = parm[0]->data()->getTimeIntVal(row, isNull);
            return (uint32_t)((val >> 24) & 0xff);
            break;
        }

        default:
        {
            isNull = true;
            break;
        }
    }

    if (isNull)
        return -1;

    if ( val < 1000000000 )
        return 0;

    return (uint32_t)((val >> 20) & 0x3f);
}


} // namespace funcexp
// vim:ts=4 sw=4:

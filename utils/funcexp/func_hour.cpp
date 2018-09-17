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
* $Id: func_hour.cpp 3495 2013-01-21 14:09:51Z rdempsey $
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

CalpontSystemCatalog::ColType Func_hour::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}


int64_t Func_hour::getIntVal(rowgroup::Row& row,
                             FunctionParm& parm,
                             bool& isNull,
                             CalpontSystemCatalog::ColType& op_ct)
{
    int64_t val = 0;
    bool isTime = false;

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
            isTime = true;
            val = parm[0]->data()->getTimeIntVal(row, isNull);
            break;
        }

        default:
        {
            isNull = true;
        }
    }

    if (isNull)
        return -1;

    if (isTime)
    {
        // HOUR() is always positive in MariaDB, even for negative time
        int64_t mask = 0;

        if ((val >> 40) & 0x800)
            mask = 0xfffffffffffff000;

        val = abs(mask | ((val >> 40) & 0xfff));
    }
    else
    {
        val = (val >> 32) & 0x3f;
    }

    return val;
}


} // namespace funcexp
// vim:ts=4 sw=4:

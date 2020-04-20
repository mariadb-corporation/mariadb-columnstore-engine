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
* $Id: func_elt.cpp 2665 2011-06-01 20:42:52Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
using namespace std;

#include "functor_str.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

#include "checks.h"

namespace funcexp
{

CalpontSystemCatalog::ColType Func_elt::operationType( FunctionParm& fp, CalpontSystemCatalog::ColType& resultType )
{
    return resultType;
}

string Func_elt::getStrVal(rowgroup::Row& row,
                           FunctionParm& parm,
                           bool& isNull,
                           CalpontSystemCatalog::ColType&)
{
    uint64_t number = 0;

    //get number
    switch (parm[0]->data()->resultType().colDataType)
    {
        case CalpontSystemCatalog::BIGINT:
        case CalpontSystemCatalog::MEDINT:
        case CalpontSystemCatalog::SMALLINT:
        case CalpontSystemCatalog::TINYINT:
        case CalpontSystemCatalog::INT:
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::VARCHAR:
        {
            double value = parm[0]->data()->getDoubleVal(row, isNull);
            number = (int64_t) value;
            break;
        }

        case CalpontSystemCatalog::DECIMAL:
        {
            IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);

            if (parm[0]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
            {
                int128_t scaleDivisor, scaleDivisor2;

                datatypes::getScaleDivisor(scaleDivisor, d.scale);

                scaleDivisor2 = (scaleDivisor <= 10) ? 1 : (scaleDivisor / 10);

                int128_t tmpval = d.s128Value / scaleDivisor;
                int128_t lefto = (d.s128Value - tmpval * scaleDivisor) / scaleDivisor2;

                if (utils::is_nonnegative(tmpval) >= 0 && lefto > 4)
                    tmpval++;

                if (utils::is_negative(tmpval) < 0 && lefto < -4)
                    tmpval--;

                number = datatypes::Decimal::getInt64FromWideDecimal(tmpval);
            }
            else
            {
                double dscale = d.scale;
                number = d.value / pow(10.0, dscale);
                int lefto = (d.value - number * pow(10.0, dscale)) / pow(10.0, dscale - 1);

                if ( utils::is_nonnegative(number) && lefto > 4 )
                    number++;

                if ( utils::is_negative(number) && lefto < -4 )
                    number--;
            }

            break;
        }

        default:
            isNull = true;
            return "";
    }

    if (number < 1)
    {
        isNull = true;
        return "";
    }

    if (number > parm.size() - 1 )
    {
        isNull = true;
        return "";
    }

    std::string ret;
    stringValue(parm[number], row, isNull, ret);
    return ret;

}


} // namespace funcexp
// vim:ts=4 sw=4:

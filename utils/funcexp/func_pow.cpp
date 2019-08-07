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
* $Id: func_pow.cpp 3495 2013-01-21 14:09:51Z rdempsey $
*
*
****************************************************************************/

#include <cstdlib>
#include <string>
#include <cmath>
#if defined(_MSC_VER) && !defined(isfinite)
#include <float.h>
#define isfinite _finite
#endif
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

namespace funcexp
{

CalpontSystemCatalog::ColType Func_pow::operationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType)
{
    // operation type is not used by this functor
    return fp[0]->data()->resultType();
}


double Func_pow::getDoubleVal(Row& row,
                              FunctionParm& parm,
                              bool& isNull,
                              CalpontSystemCatalog::ColType&)
{
    // null value is indicated by isNull
    double base = parm[0]->data()->getDoubleVal(row, isNull);

    if (!isNull)
    {
        double exponent = parm[1]->data()->getDoubleVal(row, isNull);

        if (!isNull)
        {
            errno = 0;
            double x = pow(base, exponent);

            // @bug3490, 4461, rule out domain error, pole error and overflow range error.
            if (!isfinite(x))
            {
                isNull = true;
                Message::Args args;
                args.add("pow");
                args.add(base);
                args.add(exponent);
                unsigned errcode = ERR_FUNC_OUT_OF_RANGE_RESULT;
                throw IDBExcept(IDBErrorInfo::instance()->errorMsg(errcode, args), errcode);
            }


            return x;
        }
    }

    return 0.0;
}

long double Func_pow::getLongDoubleVal(Row& row,
                              FunctionParm& parm,
                              bool& isNull,
                              CalpontSystemCatalog::ColType&)
{
    // null value is indicated by isNull
    long double base = parm[0]->data()->getLongDoubleVal(row, isNull);

    if (!isNull)
    {
        // Should this be long double? Not sure on usage.
        double exponent = parm[1]->data()->getDoubleVal(row, isNull);

        if (!isNull)
        {
            errno = 0;
            long double x = powl(base, (long double)exponent);

            // @bug3490, 4461, rule out domain error, pole error and overflow range error.
            if (!isfinite(x))
            {
                isNull = true;
                Message::Args args;
                args.add("pow");
                args.add((double)base);
                args.add(exponent);
                unsigned errcode = ERR_FUNC_OUT_OF_RANGE_RESULT;
                throw IDBExcept(IDBErrorInfo::instance()->errorMsg(errcode, args), errcode);
            }


            return x;
        }
    }

    return 0.0;
}


} // namespace funcexp
// vim:ts=4 sw=4:

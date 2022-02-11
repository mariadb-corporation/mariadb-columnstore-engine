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
 * $Id: func_div.cpp 3921 2013-06-19 18:59:56Z bwilkinson $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
#define __STDC_LIMIT_MACROS
#include <stdint.h>
using namespace std;

#include "functor_real.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{
CalpontSystemCatalog::ColType Func_div::operationType(FunctionParm& fp,
                                                      CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_div::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                            CalpontSystemCatalog::ColType& op_ct)
{
  double val1 = parm[0]->data()->getDoubleVal(row, isNull);
  double val2 = parm[1]->data()->getDoubleVal(row, isNull);

  if (val2 == 0 || val2 == NAN)
  {
    isNull = true;
    return 0;
  }
  // MCOL-179 InnoDB doesn't round or convert to int before dividing.
  return static_cast<int64_t>(val1 / val2);

#if 0    
    int64_t int_val2 = (int64_t)(val2 > 0 ? val2 + 0.5 : val2 - 0.5);

    if (int_val2 == 0)
    {
        isNull = true;
        return 0;
    }

    int64_t int_val1 = (int64_t)(val1 > 0 ? val1 + 0.5 : val1 - 0.5);

    // MCOL-176 If abs(int_val2) is small enough (like -1), then, this may cause overflow.
    // This kludge stops the crash.
    if (int_val1 == INT64_MIN)
    {
        --int_val1;
    }

    return int_val1 / int_val2;
#endif
}

uint64_t Func_div::getUintVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                              CalpontSystemCatalog::ColType& op_ct)
{
  uint64_t val1 = parm[0]->data()->getUintVal(row, isNull);
  uint64_t val2 = parm[1]->data()->getUintVal(row, isNull);

  if (val2 == 0)
  {
    isNull = true;
    return 0;
  }

  return val1 / val2;
}

double Func_div::getDoubleVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                              execplan::CalpontSystemCatalog::ColType& ct)
{
  return getIntVal(row, parm, isNull, ct);
}

string Func_div::getStrVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                           CalpontSystemCatalog::ColType& ct)
{
  return intToString(getIntVal(row, parm, isNull, ct));
}

}  // namespace funcexp
// vim:ts=4 sw=4:

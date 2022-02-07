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
 * $Id: func_period_diff.cpp 2477 2011-04-01 16:07:35Z rdempsey $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{
#define YY_PART_YEAR 70

inline uint64_t convert_period_to_month(uint64_t period)
{
  uint64_t a, b;

  if (period == 0 || period > 999912)
    return 0L;

  if ((a = period / 100) < YY_PART_YEAR)
    a += 2000;
  else if (a < 100)
    a += 1900;

  b = period % 100;
  return a * 12 + b - 1;
}

int64_t getArgSInt64Val(rowgroup::Row& row, TreeNode* exp, bool& isNull)
{
  switch (exp->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::DATE:
    case execplan::CalpontSystemCatalog::DATETIME:
    case execplan::CalpontSystemCatalog::TIMESTAMP: return exp->getIntVal(row, isNull);

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      IDB_Decimal d = exp->getDecimalVal(row, isNull);
      return d.toSInt64Round();
    }

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT: return atoi(exp->getStrVal(row, isNull).c_str());

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    {
      datatypes::TDouble d(exp->getDoubleVal(row, isNull));
      return d.toMCSSInt64Round();
    }

    default: isNull = true;
  }
  return 0;
}

CalpontSystemCatalog::ColType Func_period_diff::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_period_diff::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& op_ct)
{
  uint64_t period1 = (uint64_t)getArgSInt64Val(row, parm[0]->data(), isNull);

  if (isNull)
    return 0;

  uint64_t period2 = (uint64_t)getArgSInt64Val(row, parm[1]->data(), isNull);

  if (isNull)
    return 0;

  return convert_period_to_month(period1) - convert_period_to_month(period2);
}

}  // namespace funcexp
// vim:ts=4 sw=4:

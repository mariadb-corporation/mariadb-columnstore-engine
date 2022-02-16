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
 * $Id: func_if.cpp 3871 2013-06-07 16:25:01Z bpaul $
 *
 *
 ****************************************************************************/

#include <string>
using namespace std;

#include "functor_all.h"
#include "functioncolumn.h"
#include "predicateoperator.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

namespace
{
bool boolVal(SPTP& parm, Row& row, long timeZone)
{
  bool ret = true;
  bool isNull = false;  // Keep it local. We don't want to mess with the global one here.

  try
  {
    ret = parm->getBoolVal(row, isNull) && !isNull;
  }
  catch (logging::NotImplementedExcept&)
  {
    switch (parm->data()->resultType().colDataType)
    {
      case CalpontSystemCatalog::CHAR:
      case CalpontSystemCatalog::TEXT:
      case CalpontSystemCatalog::VARCHAR:
        ret = (atoi((char*)(parm->data()->getStrVal(timeZone).c_str())) != 0);
        break;
      case CalpontSystemCatalog::FLOAT:
      case CalpontSystemCatalog::UFLOAT: ret = (parm->data()->getFloatVal(row, isNull) != 0); break;
      case CalpontSystemCatalog::DOUBLE:
      case CalpontSystemCatalog::UDOUBLE: ret = (parm->data()->getDoubleVal(row, isNull) != 0); break;
      case CalpontSystemCatalog::LONGDOUBLE: ret = (parm->data()->getLongDoubleVal(row, isNull) != 0); break;
      case CalpontSystemCatalog::DECIMAL:
      case CalpontSystemCatalog::UDECIMAL:
        if (parm->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
          ret = (parm->data()->getDecimalVal(row, isNull).s128Value != 0);
        else
          ret = (parm->data()->getDecimalVal(row, isNull).value != 0);
        break;
      case CalpontSystemCatalog::BIGINT:
      case CalpontSystemCatalog::SMALLINT:
      case CalpontSystemCatalog::MEDINT:
      case CalpontSystemCatalog::INT:
      case CalpontSystemCatalog::UBIGINT:
      case CalpontSystemCatalog::USMALLINT:
      case CalpontSystemCatalog::UMEDINT:
      case CalpontSystemCatalog::UINT:
      case CalpontSystemCatalog::DATE:
      case CalpontSystemCatalog::DATETIME:
      case CalpontSystemCatalog::TIMESTAMP:
      case CalpontSystemCatalog::TIME:
      default: ret = (parm->data()->getIntVal(row, isNull) != 0); break;
    }
  }

  return ret;
}

}  // namespace

namespace funcexp
{
// IF(expression1, expression12, expression3)
//
// if parm order:
//   expression1 expression2 expression3
//
CalpontSystemCatalog::ColType Func_if::operationType(FunctionParm& fp,
                                                     CalpontSystemCatalog::ColType& resultType)
{
  // operation type is not used by this functor
  // The result type given by the connector may not be right if there's derived table (MySQL bug?)
  // We want to double check on our own.
  // If any parm is of string type, the result type should be string.
  if (fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::CHAR ||
      fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::VARCHAR ||
      fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::TEXT ||
      fp[2]->data()->resultType().colDataType == CalpontSystemCatalog::CHAR ||
      fp[2]->data()->resultType().colDataType == CalpontSystemCatalog::TEXT ||
      fp[2]->data()->resultType().colDataType == CalpontSystemCatalog::VARCHAR)
  {
    CalpontSystemCatalog::ColType ct;
    ct.colDataType = CalpontSystemCatalog::VARCHAR;
    ct.colWidth = 255;
    resultType = ct;
    return ct;
  }

  CalpontSystemCatalog::ColType ct = fp[1]->data()->resultType();
  PredicateOperator op;
  op.setOpType(ct, fp[2]->data()->resultType());
  ct = op.operationType();
  resultType = ct;
  return ct;
}

int64_t Func_if::getIntVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& ct)
{
  if (boolVal(parm[0], row, ct.getTimeZone()))
  {
    return parm[1]->data()->getIntVal(row, isNull);
  }
  else
  {
    return parm[2]->data()->getIntVal(row, isNull);
  }
}

string Func_if::getStrVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& ct)
{
  if (boolVal(parm[0], row, ct.getTimeZone()))
  {
    return parm[1]->data()->getStrVal(row, isNull);
  }
  else
  {
    return parm[2]->data()->getStrVal(row, isNull);
  }
}

IDB_Decimal Func_if::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& ct)
{
  if (boolVal(parm[0], row, ct.getTimeZone()))
  {
    return parm[1]->data()->getDecimalVal(row, isNull);
  }
  else
  {
    return parm[2]->data()->getDecimalVal(row, isNull);
  }
}

double Func_if::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& ct)
{
  if (boolVal(parm[0], row, ct.getTimeZone()))
  {
    return parm[1]->data()->getDoubleVal(row, isNull);
  }
  else
  {
    return parm[2]->data()->getDoubleVal(row, isNull);
  }
}

long double Func_if::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& ct)
{
  if (boolVal(parm[0], row, ct.getTimeZone()))
  {
    return parm[1]->data()->getLongDoubleVal(row, isNull);
  }
  else
  {
    return parm[2]->data()->getLongDoubleVal(row, isNull);
  }
}

int32_t Func_if::getDateIntVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& ct)
{
  if (boolVal(parm[0], row, ct.getTimeZone()))
  {
    return parm[1]->data()->getDateIntVal(row, isNull);
  }
  else
  {
    return parm[2]->data()->getDateIntVal(row, isNull);
  }
}

int64_t Func_if::getDatetimeIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& ct)
{
  if (boolVal(parm[0], row, ct.getTimeZone()))
  {
    return parm[1]->data()->getDatetimeIntVal(row, isNull);
  }
  else
  {
    return parm[2]->data()->getDatetimeIntVal(row, isNull);
  }
}

int64_t Func_if::getTimestampIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& ct)
{
  if (boolVal(parm[0], row, ct.getTimeZone()))
  {
    return parm[1]->data()->getTimestampIntVal(row, isNull);
  }
  else
  {
    return parm[2]->data()->getTimestampIntVal(row, isNull);
  }
}

int64_t Func_if::getTimeIntVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& ct)
{
  if (boolVal(parm[0], row, ct.getTimeZone()))
  {
    return parm[1]->data()->getTimeIntVal(row, isNull);
  }
  else
  {
    return parm[2]->data()->getTimeIntVal(row, isNull);
  }
}
}  // namespace funcexp
// vim:ts=4 sw=4:

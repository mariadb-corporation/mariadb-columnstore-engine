/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (c) 2019 MariaDB Corporation

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

/***********************************************************************
 *   $Id$
 *
 *
 ***********************************************************************/

//#include <my_config.h>
#include <cmath>
#include <iostream>
using namespace std;

#include "udfsdk.h"

#include "treenode.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "funcexp.h"
using namespace funcexp;

namespace udfsdk
{
/**
 * UDFSDK definition
 */
UDFSDK::UDFSDK()
{
}

UDFSDK::~UDFSDK()
{
}

/**
 * All UDF functions should be registered in the function map. They will be
 * picked up by the MariaDB ColumnStore F&E framework when the servers are started.
 * That will make sure the UDF functions runs distributedly in ColumnStore
 * engines just like the internal ColumnStore functions.
 */
FuncMap UDFSDK::UDFMap() const
{
  FuncMap fm;

  // first: function name
  // second: Function pointer
  // please use lower case for the function name. Because the names might be
  // case-insensitive in MariaDB depending on the setting. In such case,
  // the function names passed to the interface is always in lower case.
  fm["mcs_add"] = new MCS_add();
  fm["mcs_isnull"] = new MCS_isnull();

  return fm;
}

/***************************************************************************
 * MCS_ADD implementation
 *
 * OperationType() definition
 */
CalpontSystemCatalog::ColType MCS_add::operationType(FunctionParm& fp,
                                                     CalpontSystemCatalog::ColType& resultType)
{
  // operation type of MCS_add is determined by the argument types
  assert(fp.size() == 2);
  CalpontSystemCatalog::ColType rt;

  if (fp[0]->data()->resultType() == fp[1]->data()->resultType())
  {
    rt = fp[0]->data()->resultType();
  }
  else if (fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::CHAR ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::CHAR ||
           fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::VARCHAR ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::VARCHAR ||
           fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::DOUBLE ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::DOUBLE)
  {
    rt.colDataType = CalpontSystemCatalog::DOUBLE;
    rt.colWidth = 8;
  }
  else if (fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::DATE ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::DATE ||
           fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::DATETIME ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::DATETIME ||
           fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::TIME ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::TIME)
  {
    rt.colDataType = CalpontSystemCatalog::BIGINT;
    rt.colWidth = 8;
  }
  else if (fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::DECIMAL ||
           fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::UDECIMAL ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::DECIMAL ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::UDECIMAL)
  {
    rt.colDataType = CalpontSystemCatalog::DECIMAL;
    rt.colWidth = 8;
  }
  else
  {
    if (isUnsigned(fp[0]->data()->resultType().colDataType) ||
        isUnsigned(fp[1]->data()->resultType().colDataType))
    {
      rt.colDataType = CalpontSystemCatalog::UBIGINT;
      rt.colWidth = 8;
    }
    else
    {
      rt.colDataType = CalpontSystemCatalog::BIGINT;
      rt.colWidth = 8;
    }
  }

  return rt;
}

/**
 * getDoubleVal API definition
 *
 * This API is called when an double value is needed to return from the UDF function
 */
double MCS_add::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  switch (op_ct.colDataType)
  {
    // The APIs for the evaluation of the function parameters are the same getXXXval()
    // functions. However, only two arguments are passed in, the current
    // row reference and the NULL indicator isNull.
    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::TINYINT:
      return (parm[0]->data()->getIntVal(row, isNull) + parm[1]->data()->getIntVal(row, isNull));

    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UTINYINT:
      return (parm[0]->data()->getUintVal(row, isNull) + parm[1]->data()->getUintVal(row, isNull));

    case CalpontSystemCatalog::DOUBLE:
      return (parm[0]->data()->getDoubleVal(row, isNull) + parm[1]->data()->getDoubleVal(row, isNull));

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      IDB_Decimal dec;
      IDB_Decimal op1 = parm[0]->data()->getDecimalVal(row, isNull);
      IDB_Decimal op2 = parm[1]->data()->getDecimalVal(row, isNull);

      if (op1.scale == op2.scale)
      {
        dec.scale = op1.scale;
      }
      else if (op1.scale >= op2.scale)
      {
        dec.scale = op2.scale;
        op1.value *= datatypes::scaleDivisor<int64_t>((uint32_t)(op1.scale - op2.scale));
      }
      else
      {
        dec.scale = op1.scale;
        op2.value *= datatypes::scaleDivisor<int64_t>((uint32_t)(op2.scale - op1.scale));
      }

      dec.value = op1.value + op2.value;
      return dec.decimal64ToXFloat<double>();
    }

    default: return (parm[0]->data()->getDoubleVal(row, isNull) + parm[1]->data()->getDoubleVal(row, isNull));
  }

  return 0;
}

long double MCS_add::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& op_ct)
{
  return getDoubleVal(row, parm, isNull, op_ct);
}

float MCS_add::getFloatVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return (float)getDoubleVal(row, parm, isNull, op_ct);
}

/**
 * getIntVal API definition
 *
 * This API is called when an integer value is needed to return from the UDF function
 *
 * Because the result type MCS_add is double(real), all the other API can simply call
 * getDoubleVal and apply the conversion. This method may not fit for all the UDF
 * implementation.
 */
int64_t MCS_add::getIntVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return (int64_t)getDoubleVal(row, parm, isNull, op_ct);
}

string MCS_add::getStrVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  // One will need a more efficient implementation if this API is frequently
  // called for this UDF function. This code is for demonstration purpose.
  ostringstream oss;
  oss << getDoubleVal(row, parm, isNull, op_ct);
  return oss.str();
}

IDB_Decimal MCS_add::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal dec;
  dec.value = getIntVal(row, parm, isNull, op_ct);
  dec.scale = 0;
  return dec;
}

/**
 * This API should never be called for MCS_add, because the latter
 * is not for date/datetime values addition. In such case, one can
 * either not implement this API and an MCS5001 error will be thrown,
 * or throw a customized exception here.
 */
int32_t MCS_add::getDateIntVal(Row& row, FunctionParm& parm, bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
  throw logic_error("Invalid API called for MCS_ADD");
}

/**
 * This API should never be called for MCS_add, because the latter
 * is not for date/datetime values addition. In such case, one can
 * either not implement this API and an MCS-5001 error will be thrown,
 * or throw a customized exception here.
 */
int64_t MCS_add::getDatetimeIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  return (int64_t)getDoubleVal(row, parm, isNull, op_ct);
}

bool MCS_add::getBoolVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return false;
}

/***************************************************************************
 * MCS_ISNULL implementation
 *
 * OperationType() definition
 */
CalpontSystemCatalog::ColType MCS_isnull::operationType(FunctionParm& fp,
                                                        CalpontSystemCatalog::ColType& resultType)
{
  // operation type of MCS_isnull should be the same as the argument type
  assert(fp.size() == 1);
  return fp[0]->data()->resultType();
}

/**
 * getBoolVal API definition
 *
 * This would be the most commonly called API for MCS_isnull function
 */
bool MCS_isnull::getBoolVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  switch (op_ct.colDataType)
  {
    // For the purpose of this function, one does not need to get the value of
    // the argument. One only need to know if the argument is NULL. The passed
    // in parameter isNull will be set if the parameter is evaluated NULL.
    // Please note that before this function returns, isNull should be set to
    // false, otherwise the result of the function would be considered NULL,
    // which is not possible for MCS_isnull().
    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL: parm[0]->data()->getDecimalVal(row, isNull); break;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR: parm[0]->data()->getStrVal(row, isNull); break;

    default: parm[0]->data()->getIntVal(row, isNull);
  }

  bool ret = isNull;
  // It's important to reset isNull indicator.
  isNull = false;
  return ret;
}

/**
 * getDoubleVal API definition
 *
 * This API is called when a double value is needed to return from the UDF function
 */
double MCS_isnull::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                CalpontSystemCatalog::ColType& op_ct)
{
  return (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
}

long double MCS_isnull::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                         CalpontSystemCatalog::ColType& op_ct)
{
  return (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
}

float MCS_isnull::getFloatVal(Row& row, FunctionParm& parm, bool& isNull,
                              CalpontSystemCatalog::ColType& op_ct)
{
  return (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
}

/**
 * getIntVal API definition
 *
 * This API is called when an integer value is needed to return from the UDF function
 *
 * Because the result type MCS_add is double(real), all the other API can simply call
 * getDoubleVal and apply the conversion. This method may not fit for all the UDF
 * implementations.
 */
int64_t MCS_isnull::getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                              CalpontSystemCatalog::ColType& op_ct)
{
  return (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
}

string MCS_isnull::getStrVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  // This needs to be more efficient if this API will be frequently
  // called for this UDF function.
  return (getBoolVal(row, parm, isNull, op_ct) ? "1" : "0");
}

IDB_Decimal MCS_isnull::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal dec;
  dec.value = (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
  dec.scale = 0;
  return dec;
}

int32_t MCS_isnull::getDateIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                  CalpontSystemCatalog::ColType& op_ct)
{
  return (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
}

int64_t MCS_isnull::getDatetimeIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& op_ct)
{
  return (getBoolVal(row, parm, isNull, op_ct) ? 1 : 0);
}

}  // namespace udfsdk
// vim:ts=4 sw=4:

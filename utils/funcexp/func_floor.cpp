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

//  $Id: func_floor.cpp 3923 2013-06-19 21:43:06Z bwilkinson $

#include <cstdlib>
#include <iomanip>
#include <string>
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
using namespace dataconvert;

#include "funchelpers.h"

namespace funcexp
{
CalpontSystemCatalog::ColType Func_floor::operationType(FunctionParm& fp,
                                                        CalpontSystemCatalog::ColType& resultType)
{
  return fp[0]->data()->resultType();
}

int64_t Func_floor::getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                              CalpontSystemCatalog::ColType& op_ct)
{
  int64_t ret = 0;

  switch (op_ct.colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      ret = parm[0]->data()->getIntVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      ret = (int64_t)parm[0]->data()->getUintVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      ret = (int64_t)floor(parm[0]->data()->getDoubleVal(row, isNull));
    }
    break;

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
    {
      ret = (int64_t)floorl(parm[0]->data()->getLongDoubleVal(row, isNull));
    }
    break;

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      const string& str = parm[0]->data()->getStrVal(row, isNull);

      if (!isNull)
        ret = (int64_t)floor(strtod(str.c_str(), 0));
    }
    break;

    case execplan::CalpontSystemCatalog::DATE:
    {
      // For some reason, MDB doesn't return this as a date,
      // but datetime is returned as a datetime. Expect
      // this to change in the future.
      Date d(parm[0]->data()->getDateIntVal(row, isNull));
      if (!isNull)
        ret = d.convertToMySQLint();
    }
    break;

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      ret = parm[0]->data()->getDatetimeIntVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      ret = parm[0]->data()->getTimestampIntVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::TIME:
    {
      ret = parm[0]->data()->getTimeIntVal(row, isNull);
    }
    break;

    // floor(decimal(X,Y)) leads to this path if X, Y allows to
    // downcast to INT otherwise Func_floor::getDecimalVal() is called
    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      ret = parm[0]->data()->getDecimalVal(row, isNull).toSInt64Floor();
      break;
    }

    default:
    {
      std::ostringstream oss;
      oss << "floor: datatype of " << execplan::colDataTypeToString(op_ct.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  return ret;
}

uint64_t Func_floor::getUintVal(Row& row, FunctionParm& parm, bool& isNull,
                                CalpontSystemCatalog::ColType& op_ct)
{
  int64_t ret = 0;

  switch (op_ct.colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      ret = parm[0]->data()->getIntVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      ret = (int64_t)parm[0]->data()->getUintVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      ret = (uint64_t)floor(parm[0]->data()->getDoubleVal(row, isNull));
    }
    break;

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
    {
      ret = (uint64_t)floorl(parm[0]->data()->getLongDoubleVal(row, isNull));
    }
    break;

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      const string& str = parm[0]->data()->getStrVal(row, isNull);

      if (!isNull)
        ret = (uint64_t)floor(strtod(str.c_str(), 0));
    }
    break;

    case execplan::CalpontSystemCatalog::DATE:
    {
      string str = DataConvert::dateToString1(parm[0]->data()->getDateIntVal(row, isNull));

      if (!isNull)
        ret = strtoull(str.c_str(), NULL, 10);
    }
    break;

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      string str = DataConvert::datetimeToString1(parm[0]->data()->getDatetimeIntVal(row, isNull));

      // strip off micro seconds
      str = str.substr(0, 14);

      if (!isNull)
        ret = strtoull(str.c_str(), NULL, 10);
    }
    break;

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      string str =
          DataConvert::timestampToString1(parm[0]->data()->getTimestampIntVal(row, isNull), op_ct.getTimeZone());

      // strip off micro seconds
      str = str.substr(0, 14);

      if (!isNull)
        ret = strtoull(str.c_str(), NULL, 10);
    }
    break;

    case execplan::CalpontSystemCatalog::TIME:
    {
      string str = DataConvert::timeToString1(parm[0]->data()->getTimeIntVal(row, isNull));

      // strip off micro seconds
      str = str.substr(0, 14);

      if (!isNull)
        ret = strtoull(str.c_str(), NULL, 10);
    }
    break;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      ret = parm[0]->data()->getDecimalVal(row, isNull).toUInt64Floor();
      break;
    }

    default:
    {
      std::ostringstream oss;
      oss << "floor: datatype of " << execplan::colDataTypeToString(op_ct.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  return ret;
}

double Func_floor::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                CalpontSystemCatalog::ColType& op_ct)
{
  double ret = 0.0;

  if (op_ct.colDataType == CalpontSystemCatalog::DOUBLE || op_ct.colDataType == CalpontSystemCatalog::FLOAT)
  {
    ret = floor(parm[0]->data()->getDoubleVal(row, isNull));
  }
  else if (op_ct.colDataType == CalpontSystemCatalog::LONGDOUBLE)
  {
    ret = floorl(parm[0]->data()->getLongDoubleVal(row, isNull));
  }
  else if (op_ct.colDataType == CalpontSystemCatalog::VARCHAR ||
           op_ct.colDataType == CalpontSystemCatalog::CHAR || op_ct.colDataType == CalpontSystemCatalog::TEXT)
  {
    const string& str = parm[0]->data()->getStrVal(row, isNull);

    if (!isNull)
      ret = floor(strtod(str.c_str(), 0));
  }
  else if (op_ct.colDataType == CalpontSystemCatalog::DECIMAL ||
           op_ct.colDataType == CalpontSystemCatalog::UDECIMAL)
  {
    IDB_Decimal tmp = getDecimalVal(row, parm, isNull, op_ct);

    if (op_ct.colWidth == datatypes::MAXDECIMALWIDTH)
    {
      ret = static_cast<double>(tmp.toTSInt128());
    }
    else
    {
      ret = (double)tmp.value;
    }
  }
  else
  {
    ret = (double)getIntVal(row, parm, isNull, op_ct);
  }

  return ret;
}

long double Func_floor::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                         CalpontSystemCatalog::ColType& op_ct)
{
  long double ret = 0.0;

  if (op_ct.colDataType == CalpontSystemCatalog::LONGDOUBLE ||
      op_ct.colDataType == CalpontSystemCatalog::FLOAT)
  {
    ret = floor(parm[0]->data()->getDoubleVal(row, isNull));
  }
  else if (op_ct.colDataType == CalpontSystemCatalog::LONGDOUBLE)
  {
    ret = floorl(parm[0]->data()->getLongDoubleVal(row, isNull));
  }
  else if (op_ct.colDataType == CalpontSystemCatalog::VARCHAR ||
           op_ct.colDataType == CalpontSystemCatalog::CHAR || op_ct.colDataType == CalpontSystemCatalog::TEXT)
  {
    const string& str = parm[0]->data()->getStrVal(row, isNull);

    if (!isNull)
      ret = floor(strtod(str.c_str(), 0));
  }
  else if (op_ct.colDataType == CalpontSystemCatalog::DECIMAL ||
           op_ct.colDataType == CalpontSystemCatalog::UDECIMAL)
  {
    IDB_Decimal tmp = getDecimalVal(row, parm, isNull, op_ct);

    if (op_ct.colWidth == datatypes::MAXDECIMALWIDTH)
    {
      ret = static_cast<long double>(tmp.toTSInt128());
    }
    else
    {
      ret = (long double)tmp.value;
    }
  }
  else
  {
    ret = (long double)getIntVal(row, parm, isNull, op_ct);
  }

  return ret;
}

string Func_floor::getStrVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  char tmp[512] = {'\0'};

  if (op_ct.colDataType == CalpontSystemCatalog::DOUBLE ||
      op_ct.colDataType == CalpontSystemCatalog::UDOUBLE ||
      op_ct.colDataType == CalpontSystemCatalog::FLOAT || op_ct.colDataType == CalpontSystemCatalog::UFLOAT ||
      op_ct.colDataType == CalpontSystemCatalog::VARCHAR || op_ct.colDataType == CalpontSystemCatalog::CHAR ||
      op_ct.colDataType == CalpontSystemCatalog::TEXT)
  {
    snprintf(tmp, 511, "%f", getDoubleVal(row, parm, isNull, op_ct));

    // remove the decimals in the oss string.
    char* d = tmp;

    while ((*d != '.') && (*d != '\0'))
      d++;

    *d = '\0';
  }
  if (op_ct.colDataType == CalpontSystemCatalog::LONGDOUBLE)
  {
    snprintf(tmp, 511, "%Lf", getLongDoubleVal(row, parm, isNull, op_ct));

    // remove the decimals in the oss string.
    char* d = tmp;

    while ((*d != '.') && (*d != '\0'))
      d++;

    *d = '\0';
  }
  else if (op_ct.colDataType == CalpontSystemCatalog::DECIMAL ||
           op_ct.colDataType == CalpontSystemCatalog::UDECIMAL)
  {
    IDB_Decimal d = getDecimalVal(row, parm, isNull, op_ct);

    if (op_ct.colWidth == datatypes::MAXDECIMALWIDTH)
    {
      return d.toString(true);
    }
    else
    {
      return d.toString();
    }
  }
  else if (isUnsigned(op_ct.colDataType))
  {
#ifndef __LP64__
    snprintf(tmp, 511, "%llu", getUintVal(row, parm, isNull, op_ct));
#else
    snprintf(tmp, 511, "%lu", getUintVal(row, parm, isNull, op_ct));
#endif
  }
  else
  {
#ifndef __LP64__
    snprintf(tmp, 511, "%lld", getIntVal(row, parm, isNull, op_ct));
#else
    snprintf(tmp, 511, "%ld", getIntVal(row, parm, isNull, op_ct));
#endif
  }

  return string(tmp);
}

IDB_Decimal Func_floor::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal ret;

  switch (op_ct.colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      ret.value = parm[0]->data()->getIntVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      ret = parm[0]->data()->getDecimalVal(row, isNull);

      if (isNull)
        break;

      // negative scale is not supported by CNX yet
      if (ret.scale > 0)
      {
        if (ret.scale > datatypes::INT128MAXPRECISION)
        {
          std::ostringstream oss;
          oss << "floor: datatype of " << execplan::colDataTypeToString(op_ct.colDataType) << " with scale "
              << (int)ret.scale << " is beyond supported scale";
          throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
        }

        return ret.floor();
      }
    }
    break;

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      ret.value = (int64_t)parm[0]->data()->getUintVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      ret.value = (int64_t)floor(parm[0]->data()->getDoubleVal(row, isNull));
    }
    break;

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
    {
      ret.value = (int64_t)floorl(parm[0]->data()->getLongDoubleVal(row, isNull));
    }
    break;

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      const string& str = parm[0]->data()->getStrVal(row, isNull);

      if (!isNull)
        ret.value = (int64_t)floor(strtod(str.c_str(), 0));
    }
    break;

    case execplan::CalpontSystemCatalog::DATE:
    {
      string str = DataConvert::dateToString1(parm[0]->data()->getDateIntVal(row, isNull));

      if (!isNull)
        ret.value = atoll(str.c_str());
    }
    break;

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      string str = DataConvert::datetimeToString1(parm[0]->data()->getDatetimeIntVal(row, isNull));

      // strip off micro seconds
      str = str.substr(0, 14);

      if (!isNull)
        ret.value = atoll(str.c_str());
    }
    break;

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      string str =
          DataConvert::timestampToString1(parm[0]->data()->getTimestampIntVal(row, isNull), op_ct.getTimeZone());

      // strip off micro seconds
      str = str.substr(0, 14);

      if (!isNull)
        ret.value = atoll(str.c_str());
    }
    break;

    case execplan::CalpontSystemCatalog::TIME:
    {
      string str = DataConvert::timeToString1(parm[0]->data()->getTimeIntVal(row, isNull));

      // strip off micro seconds
      str = str.substr(0, 14);

      if (!isNull)
        ret.value = atoll(str.c_str());
    }
    break;

    default:
    {
      std::ostringstream oss;
      oss << "floor: datatype of " << execplan::colDataTypeToString(op_ct.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  ret.scale = 0;

  return ret;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

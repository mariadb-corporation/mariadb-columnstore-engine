/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019-20 MariaDB Corporation

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

//  $Id: func_cast.cpp 3923 2013-06-19 21:43:06Z bwilkinson $

#include <string>
using namespace std;

#include "functor_dtm.h"
#include "functor_int.h"
#include "functor_real.h"
#include "functor_str.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "predicateoperator.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "dataconvert.h"
#include "numericliteral.h"
using namespace dataconvert;

#include "checks.h"

namespace
{
struct lconv* convData = localeconv();
}

namespace funcexp
{
// Why isn't "return resultType" the base default behavior?
CalpontSystemCatalog::ColType Func_cast_signed::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

CalpontSystemCatalog::ColType Func_cast_unsigned::operationType(FunctionParm& fp,
                                                                CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

CalpontSystemCatalog::ColType Func_cast_char::operationType(FunctionParm& fp,
                                                            CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

CalpontSystemCatalog::ColType Func_cast_date::operationType(FunctionParm& fp,
                                                            CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

CalpontSystemCatalog::ColType Func_cast_datetime::operationType(FunctionParm& fp,
                                                                CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

CalpontSystemCatalog::ColType Func_cast_decimal::operationType(FunctionParm& fp,
                                                               CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

CalpontSystemCatalog::ColType Func_cast_double::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

//
//	Func_cast_signed
//
int64_t Func_cast_signed::getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& operationColType)
{
  switch (parm[0]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      return (int64_t)parm[0]->data()->getIntVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      return (int64_t)parm[0]->data()->getUintVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    {
      datatypes::TDouble d(parm[0]->data()->getDoubleVal(row, isNull));
      return d.toMCSSInt64Round();
    }
    break;

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
    {
      datatypes::TLongDouble d(parm[0]->data()->getLongDoubleVal(row, isNull));
      return d.toMCSSInt64Round();
    }
    break;

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      const string& value = parm[0]->data()->getStrVal(row, isNull);

      if (isNull)
      {
        isNull = true;
        return 0;
      }

      return atoll(value.c_str());
    }
    break;

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      return parm[0]->data()->getDecimalVal(row, isNull).toSInt64Round();
    }
    break;

    case execplan::CalpontSystemCatalog::DATE:
    {
      int64_t time = parm[0]->data()->getDateIntVal(row, isNull);

      Date d(time);
      return d.convertToMySQLint();
    }
    break;

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      int64_t time = parm[0]->data()->getDatetimeIntVal(row, isNull);

      // @bug 4703 need to include year
      DateTime dt(time);
      return dt.convertToMySQLint();
    }
    break;

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      int64_t time = parm[0]->data()->getTimestampIntVal(row, isNull);

      TimeStamp dt(time);
      return dt.convertToMySQLint(operationColType.getTimeZone());
    }
    break;

    case execplan::CalpontSystemCatalog::TIME:
    {
      int64_t time = parm[0]->data()->getTimeIntVal(row, isNull);

      Time dt(time);
      return dt.convertToMySQLint();
    }
    break;

    default:
    {
      std::ostringstream oss;
      oss << "cast: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  return 0;
}

//
//	Func_cast_unsigned
//
uint64_t Func_cast_unsigned::getUintVal(Row& row, FunctionParm& parm, bool& isNull,
                                        CalpontSystemCatalog::ColType& operationColType)
{
  switch (parm[0]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      return (int64_t)parm[0]->data()->getUintVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      return parm[0]->data()->getUintVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    {
      datatypes::TDouble d(parm[0]->data()->getDoubleVal(row, isNull));
      return d.toMCSUInt64Round();
    }
    break;

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
    {
      datatypes::TLongDouble d(parm[0]->data()->getLongDoubleVal(row, isNull));
      return d.toMCSUInt64Round();
    }
    break;

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      const string& value = parm[0]->data()->getStrVal(row, isNull);

      if (isNull)
      {
        isNull = true;
        return 0;
      }

      uint64_t ret = strtoul(value.c_str(), 0, 0);
      return ret;
    }
    break;

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);
      return d.toUInt64Round();
    }
    break;

    case execplan::CalpontSystemCatalog::DATE:
    {
      int64_t time = parm[0]->data()->getDateIntVal(row, isNull);

      Date d(time);
      return d.convertToMySQLint();
    }
    break;

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      int64_t time = parm[0]->data()->getDatetimeIntVal(row, isNull);

      // @bug 4703 need to include year
      DateTime dt(time);
      return dt.convertToMySQLint();
    }
    break;

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      int64_t time = parm[0]->data()->getTimestampIntVal(row, isNull);

      TimeStamp dt(time);
      return dt.convertToMySQLint(operationColType.getTimeZone());
    }
    break;

    case execplan::CalpontSystemCatalog::TIME:
    {
      int64_t time = parm[0]->data()->getTimeIntVal(row, isNull);

      Time dt(time);
      return dt.convertToMySQLint();
    }
    break;

    default:
    {
      std::ostringstream oss;
      oss << "cast: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  return 0;
}

//
//	Func_cast_char
//
string Func_cast_char::getStrVal(Row& row, FunctionParm& parm, bool& isNull,
                                 CalpontSystemCatalog::ColType& operationColType)
{
  // check for convert with 1 arg, return the argument
  if (parm.size() == 1)
    return parm[0]->data()->getStrVal(row, isNull);
  ;

  int64_t length = parm[1]->data()->getIntVal(row, isNull);

  // @bug3488, a dummy parm is appended even the optional N is not present.
  if (length < 0)
    return parm[0]->data()->getStrVal(row, isNull);
  ;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      return helpers::intToString(parm[0]->data()->getIntVal(row, isNull)).substr(0, length);
    }
    break;

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      return helpers::uintToString(parm[0]->data()->getUintVal(row, isNull)).substr(0, length);
    }
    break;

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    {
      return helpers::doubleToString(parm[0]->data()->getDoubleVal(row, isNull)).substr(0, length);
    }
    break;

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
    {
      return helpers::longDoubleToString(parm[0]->data()->getLongDoubleVal(row, isNull)).substr(0, length);
    }
    break;

    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      return doubleToString(parm[0]->data()->getFloatVal(row, isNull)).substr(0, length);
    }
    break;

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      const string& value = parm[0]->data()->getStrVal(row, isNull);

      if (isNull)
      {
        isNull = true;
        return value;
      }

      return value.substr(0, length);
    }
    break;

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      IDB_Decimal d = parm[0]->data()->getDecimalVal(row, isNull);

      if (parm[0]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
        return d.toString(true).substr(0, length);
      else
        return d.toString().substr(0, length);
    }
    break;

    case execplan::CalpontSystemCatalog::DATE:
    {
      return dataconvert::DataConvert::dateToString(parm[0]->data()->getDateIntVal(row, isNull))
          .substr(0, length);
    }
    break;

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      return dataconvert::DataConvert::datetimeToString(parm[0]->data()->getDatetimeIntVal(row, isNull))
          .substr(0, length);
    }
    break;

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      return dataconvert::DataConvert::timestampToString(parm[0]->data()->getTimestampIntVal(row, isNull),
                                                         operationColType.getTimeZone())
          .substr(0, length);
    }
    break;

    default:
    {
      std::ostringstream oss;
      oss << "cast: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }
}

//
//	Func_cast_date
//

int64_t Func_cast_date::getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                  CalpontSystemCatalog::ColType& operationColType)
{
  if (operationColType.colDataType == execplan::CalpontSystemCatalog::DATE)
    return Func_cast_date::getDateIntVal(row, parm, isNull, operationColType);

  return Func_cast_date::getDatetimeIntVal(row, parm, isNull, operationColType);
}

string Func_cast_date::getStrVal(Row& row, FunctionParm& parm, bool& isNull,
                                 CalpontSystemCatalog::ColType& operationColType)
{
  int64_t value;

  if (operationColType.colDataType == execplan::CalpontSystemCatalog::DATE)
  {
    value = Func_cast_date::getDateIntVal(row, parm, isNull, operationColType);
    char buf[30] = {'\0'};
    dataconvert::DataConvert::dateToString(value, buf, sizeof(buf));
    return string(buf);
  }

  value = Func_cast_date::getDatetimeIntVal(row, parm, isNull, operationColType);

  char buf[30] = {'\0'};
  dataconvert::DataConvert::datetimeToString(value, buf, sizeof(buf));
  return string(buf);
}

IDB_Decimal Func_cast_date::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                          CalpontSystemCatalog::ColType& operationColType)
{
  IDB_Decimal decimal;

  if (parm[0]->data()->resultType().isWideDecimalType())
    decimal.s128Value = Func_cast_date::getDatetimeIntVal(row, parm, isNull, operationColType);
  else
    decimal.value = Func_cast_date::getDatetimeIntVal(row, parm, isNull, operationColType);

  return decimal;
}

double Func_cast_date::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& operationColType)
{
  return (double)Func_cast_date::getDatetimeIntVal(row, parm, isNull, operationColType);
}

long double Func_cast_date::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                             CalpontSystemCatalog::ColType& operationColType)
{
  return (long double)Func_cast_date::getDatetimeIntVal(row, parm, isNull, operationColType);
}

int32_t Func_cast_date::getDateIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct)
{
  int64_t val;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      val = dataconvert::DataConvert::intToDate(parm[0]->data()->getIntVal(row, isNull));

      if (val == -1)
        isNull = true;
      else
        return val;

      break;
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      val = dataconvert::DataConvert::intToDate(parm[0]->data()->getIntVal(row, isNull));

      if (val == -1)
        isNull = true;
      else
        return val;

      break;
    }

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      val = dataconvert::DataConvert::stringToDate(parm[0]->data()->getStrVal(row, isNull));

      if (val == -1)
        isNull = true;
      else
        return val;

      break;
    }

    case execplan::CalpontSystemCatalog::DATE:
    case execplan::CalpontSystemCatalog::DATETIME:
    {
      return parm[0]->data()->getDateIntVal(row, isNull);
    }
    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      int64_t val1 = parm[0]->data()->getTimestampIntVal(row, isNull);
      string value = dataconvert::DataConvert::timestampToString(val1, op_ct.getTimeZone());
      value = value.substr(0, 10);
      return dataconvert::DataConvert::stringToDate(value);
    }
    case execplan::CalpontSystemCatalog::TIME:
    {
      int64_t val1;
      string value = "";
      DateTime aDateTime = static_cast<DateTime>(nowDatetime());
      Time aTime = parm[0]->data()->getTimeIntVal(row, isNull);
      aTime.day = 0;
      if ((aTime.hour < 0) || (aTime.is_neg))
      {
        aTime.hour = -abs(aTime.hour);
        aTime.minute = -abs(aTime.minute);
        aTime.second = -abs(aTime.second);
        aTime.msecond = -abs(aTime.msecond);
      }

      aDateTime.hour = 0;
      aDateTime.minute = 0;
      aDateTime.second = 0;
      aDateTime.msecond = 0;
      val1 = addTime(aDateTime, aTime);
      value = dataconvert::DataConvert::datetimeToString(val1);
      value = value.substr(0, 10);
      return dataconvert::DataConvert::stringToDate(value);
      break;
    }

    default:
    {
      isNull = true;
    }
  }

  return 0;
}

int64_t Func_cast_date::getDatetimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                          execplan::CalpontSystemCatalog::ColType& operationColType)
{
  int64_t val;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));

      if (val == -1)
        isNull = true;
      else
        return val;

      break;
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      if (parm[0]->data()->resultType().scale != 0)
      {
        isNull = true;
        break;
      }
      else
      {
        val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));

        if (val == -1)
          isNull = true;
        else
          return val;

        break;
      }
    }

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));

      if (val == -1)
        isNull = true;
      else
        return val;

      break;
    }

    case execplan::CalpontSystemCatalog::DATE:
    {
      return parm[0]->data()->getDatetimeIntVal(row, isNull);
    }

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      // @bug 4703 eliminated unnecessary conversion from datetime to string and back
      //           need to zero out the time portions since we are casting to date
      DateTime val1(parm[0]->data()->getDatetimeIntVal(row, isNull));
      val1.hour = 0;
      val1.minute = 0;
      val1.second = 0;
      val1.msecond = 0;
      return *(reinterpret_cast<uint64_t*>(&val1));
    }
    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      TimeStamp timestamp(parm[0]->data()->getTimestampIntVal(row, isNull));
      int64_t seconds = timestamp.second;
      MySQLTime m_time;
      gmtSecToMySQLTime(seconds, m_time, operationColType.getTimeZone());
      DateTime dt;
      dt.year = m_time.year;
      dt.month = m_time.month;
      dt.day = m_time.day;
      dt.hour = 0;
      dt.minute = 0;
      dt.second = 0;
      dt.msecond = 0;
      return *(reinterpret_cast<uint64_t*>(&dt));
    }
    case CalpontSystemCatalog::TIME:
    {
      DateTime aDateTime = static_cast<DateTime>(nowDatetime());
      Time aTime = parm[0]->data()->getTimeIntVal(row, isNull);
      aTime.day = 0;
      if ((aTime.hour < 0) || (aTime.is_neg))
      {
        aTime.hour = -abs(aTime.hour);
        aTime.minute = -abs(aTime.minute);
        aTime.second = -abs(aTime.second);
        aTime.msecond = -abs(aTime.msecond);
      }

      aDateTime.hour = 0;
      aDateTime.minute = 0;
      aDateTime.second = 0;
      aDateTime.msecond = 0;
      val = addTime(aDateTime, aTime);
      return val;
    }

    default:
    {
      isNull = true;
    }
  }

  return 0;
}

//
//	Func_cast_datetime
//

int64_t Func_cast_datetime::getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& operationColType)
{
  return Func_cast_datetime::getDatetimeIntVal(row, parm, isNull, operationColType);
}

string Func_cast_datetime::getStrVal(Row& row, FunctionParm& parm, bool& isNull,
                                     CalpontSystemCatalog::ColType& operationColType)
{
  int64_t value = Func_cast_datetime::getDatetimeIntVal(row, parm, isNull, operationColType);

  char buf[30] = {'\0'};
  dataconvert::DataConvert::datetimeToString(value, buf, sizeof(buf));
  return string(buf);
}

IDB_Decimal Func_cast_datetime::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                              CalpontSystemCatalog::ColType& operationColType)
{
  IDB_Decimal decimal;

  if (parm[0]->data()->resultType().isWideDecimalType())
    decimal.s128Value = Func_cast_datetime::getDatetimeIntVal(row, parm, isNull, operationColType);
  else
    decimal.value = Func_cast_datetime::getDatetimeIntVal(row, parm, isNull, operationColType);

  return decimal;
}

double Func_cast_datetime::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                        CalpontSystemCatalog::ColType& operationColType)
{
  return (double)Func_cast_datetime::getDatetimeIntVal(row, parm, isNull, operationColType);
}

long double Func_cast_datetime::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                                 CalpontSystemCatalog::ColType& operationColType)
{
  return (long double)Func_cast_datetime::getDatetimeIntVal(row, parm, isNull, operationColType);
}

int64_t Func_cast_datetime::getDatetimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                              execplan::CalpontSystemCatalog::ColType& operationColType)
{
  int64_t val;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));

      if (val == -1)
        isNull = true;
      else
        return val;

      break;
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));

      if (val == -1)
        isNull = true;
      else
        return val;

      break;
    }

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));

      if (val == -1)
        isNull = true;
      else
        return val;

      break;
    }

    case execplan::CalpontSystemCatalog::DATE:
    {
      return parm[0]->data()->getDatetimeIntVal(row, isNull);
    }

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      return parm[0]->data()->getDatetimeIntVal(row, isNull);
    }

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      TimeStamp timestamp(parm[0]->data()->getTimestampIntVal(row, isNull));
      int64_t seconds = timestamp.second;
      MySQLTime m_time;
      gmtSecToMySQLTime(seconds, m_time, operationColType.getTimeZone());
      DateTime dt;
      dt.year = m_time.year;
      dt.month = m_time.month;
      dt.day = m_time.day;
      dt.hour = m_time.hour;
      dt.minute = m_time.minute;
      dt.second = m_time.second;
      dt.msecond = timestamp.msecond;
      return *(reinterpret_cast<int64_t*>(&dt));
    }

    case CalpontSystemCatalog::TIME:
    {
      DateTime aDateTime = static_cast<DateTime>(nowDatetime());
      Time aTime = parm[0]->data()->getTimeIntVal(row, isNull);
      aDateTime.hour = 0;
      aDateTime.minute = 0;
      aDateTime.second = 0;
      aDateTime.msecond = 0;
      if ((aTime.hour < 0) || (aTime.is_neg))
      {
        aTime.hour = -abs(aTime.hour);
        aTime.minute = -abs(aTime.minute);
        aTime.second = -abs(aTime.second);
        aTime.msecond = -abs(aTime.msecond);
      }
      aTime.day = 0;
      return addTime(aDateTime, aTime);
      break;
    }

    default:
    {
      isNull = true;
    }
  }

  return -1;
}

int64_t Func_cast_datetime::getTimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                          execplan::CalpontSystemCatalog::ColType& operationColType)
{
  int64_t val;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      val = dataconvert::DataConvert::intToTime(parm[0]->data()->getIntVal(row, isNull));

      if (val == -1)
        isNull = true;
      else
        return val;

      break;
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      val = dataconvert::DataConvert::intToTime(parm[0]->data()->getIntVal(row, isNull));

      if (val == -1)
        isNull = true;
      else
        return val;

      break;
    }

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      val = dataconvert::DataConvert::stringToTime(parm[0]->data()->getStrVal(row, isNull));

      if (val == -1)
        isNull = true;
      else
        return val;

      break;
    }

    case execplan::CalpontSystemCatalog::DATE:
    {
      return parm[0]->data()->getTimeIntVal(row, isNull);
    }

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      return parm[0]->data()->getTimeIntVal(row, isNull);
    }

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      TimeStamp timestamp(parm[0]->data()->getTimestampIntVal(row, isNull));
      int64_t seconds = timestamp.second;
      MySQLTime m_time;
      gmtSecToMySQLTime(seconds, m_time, operationColType.getTimeZone());
      Time time;
      time.hour = m_time.hour;
      time.minute = m_time.minute;
      time.second = m_time.second;
      time.is_neg = 0;
      time.day = 0;
      time.msecond = 0;
      return *(reinterpret_cast<int64_t*>(&time));
    }

    default:
    {
      isNull = true;
    }
  }

  return -1;
}

//
//	Func_cast_decimal
//

int64_t Func_cast_decimal::getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                     CalpontSystemCatalog::ColType& operationColType)
{
  IDB_Decimal decimal = Func_cast_decimal::getDecimalVal(row, parm, isNull, operationColType);
  return decimal.toSInt64Round();
}

string Func_cast_decimal::getStrVal(Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& operationColType)
{
  IDB_Decimal decimal = Func_cast_decimal::getDecimalVal(row, parm, isNull, operationColType);
  if (operationColType.isWideDecimalType())
    return decimal.toString(true);
  else
    return decimal.toString();
}

IDB_Decimal Func_cast_decimal::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                             CalpontSystemCatalog::ColType& operationColType)
{
  IDB_Decimal decimal;

  int32_t decimals = parm[1]->data()->getIntVal(row, isNull);
  int64_t max_length = parm[2]->data()->getIntVal(row, isNull);

  if (max_length > datatypes::INT128MAXPRECISION || max_length <= 0)
    max_length = datatypes::INT128MAXPRECISION;

  decimal.precision = max_length;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      if (decimal.isTSInt128ByPrecision())
      {
        bool dummy = false;
        char* ep = NULL;
        int128_t max_number_decimal =
            dataconvert::strtoll128(columnstore_big_precision[max_length - 19].c_str(), dummy, &ep);
        decimal.s128Value = parm[0]->data()->getIntVal(row, isNull);
        decimal.scale = 0;
        int128_t scaleDivisor;
        datatypes::getScaleDivisor(scaleDivisor, decimals);
        int128_t value = decimal.s128Value * scaleDivisor;

        if (value > max_number_decimal)
        {
          decimal.s128Value = max_number_decimal;
          decimal.scale = decimals;
        }
        else if (value < -max_number_decimal)
        {
          decimal.s128Value = -max_number_decimal;
          decimal.scale = decimals;
        }
      }
      else
      {
        int64_t max_number_decimal = helpers::maxNumber_c[max_length];
        decimal.value = parm[0]->data()->getIntVal(row, isNull);
        decimal.scale = 0;
        int64_t value = decimal.value * helpers::powerOf10_c[decimals];

        if (value > max_number_decimal)
        {
          decimal.value = max_number_decimal;
          decimal.scale = decimals;
        }
        else if (value < -max_number_decimal)
        {
          decimal.value = -max_number_decimal;
          decimal.scale = decimals;
        }
      }
    }
    break;

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      if (decimal.isTSInt128ByPrecision())
      {
        bool dummy = false;
        char* ep = NULL;
        int128_t max_number_decimal =
            dataconvert::strtoll128(columnstore_big_precision[max_length - 19].c_str(), dummy, &ep);

        uint128_t uval = parm[0]->data()->getUintVal(row, isNull);

        if (uval > (uint128_t)datatypes::Decimal::maxInt128)
        {
          uval = datatypes::Decimal::maxInt128;
        }

        decimal.s128Value = uval;
        decimal.scale = 0;
        int128_t scaleDivisor;
        datatypes::getScaleDivisor(scaleDivisor, decimals);
        int128_t value = decimal.s128Value * scaleDivisor;

        if (value > max_number_decimal)
        {
          decimal.s128Value = max_number_decimal;
          decimal.scale = decimals;
        }
      }
      else
      {
        int64_t max_number_decimal = helpers::maxNumber_c[max_length];

        uint64_t uval = parm[0]->data()->getUintVal(row, isNull);

        if (uval > (uint64_t)numeric_limits<int64_t>::max())
        {
          uval = numeric_limits<int64_t>::max();
        }

        decimal.value = uval;
        decimal.scale = 0;
        int64_t value = decimal.value * helpers::powerOf10_c[decimals];

        if (value > max_number_decimal)
        {
          decimal.value = max_number_decimal;
          decimal.scale = decimals;
        }
      }
    }
    break;

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      if (decimal.isTSInt128ByPrecision())
      {
        bool dummy = false;
        char* ep = NULL;
        int128_t max_number_decimal =
            dataconvert::strtoll128(columnstore_big_precision[max_length - 19].c_str(), dummy, &ep);

        float128_t value = parm[0]->data()->getDoubleVal(row, isNull);

        int128_t scaleDivisor;
        datatypes::getScaleDivisor(scaleDivisor, decimals);

        if (value > 0)
          decimal.s128Value = (int128_t)(value * scaleDivisor + 0.5);
        else if (value < 0)
          decimal.s128Value = (int128_t)(value * scaleDivisor - 0.5);
        else
          decimal.s128Value = 0;

        decimal.scale = decimals;

        if (value > max_number_decimal)
          decimal.s128Value = max_number_decimal;
        else if (value < -max_number_decimal)
          decimal.s128Value = -max_number_decimal;
      }
      else
      {
        int64_t max_number_decimal = helpers::maxNumber_c[max_length];

        double value = parm[0]->data()->getDoubleVal(row, isNull);

        if (value > 0)
          decimal.value = (int64_t)(value * helpers::powerOf10_c[decimals] + 0.5);
        else if (value < 0)
          decimal.value = (int64_t)(value * helpers::powerOf10_c[decimals] - 0.5);
        else
          decimal.value = 0;

        decimal.scale = decimals;

        if (value > max_number_decimal)
          decimal.value = max_number_decimal;
        else if (value < -max_number_decimal)
          decimal.value = -max_number_decimal;
      }
    }
    break;

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
    {
      if (decimal.isTSInt128ByPrecision())
      {
        bool dummy = false;
        char* ep = NULL;
        int128_t max_number_decimal =
            dataconvert::strtoll128(columnstore_big_precision[max_length - 19].c_str(), dummy, &ep);

        float128_t value = parm[0]->data()->getLongDoubleVal(row, isNull);

        int128_t scaleDivisor;
        datatypes::getScaleDivisor(scaleDivisor, decimals);

        if (value > 0)
          decimal.s128Value = (int128_t)(value * scaleDivisor + 0.5);
        else if (value < 0)
          decimal.s128Value = (int128_t)(value * scaleDivisor - 0.5);
        else
          decimal.s128Value = 0;

        decimal.scale = decimals;

        if (value > max_number_decimal)
          decimal.s128Value = max_number_decimal;
        else if (value < -max_number_decimal)
          decimal.s128Value = -max_number_decimal;
      }
      else
      {
        int64_t max_number_decimal = helpers::maxNumber_c[max_length];

        long double value = parm[0]->data()->getLongDoubleVal(row, isNull);

        if (value > 0)
          decimal.value = (int64_t)(value * helpers::powerOf10_c[decimals] + 0.5);
        else if (value < 0)
          decimal.value = (int64_t)(value * helpers::powerOf10_c[decimals] - 0.5);
        else
          decimal.value = 0;

        decimal.scale = decimals;

        if (value > max_number_decimal)
          decimal.value = max_number_decimal;
        else if (value < -max_number_decimal)
          decimal.value = -max_number_decimal;
      }
    }
    break;

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      if (decimal.isTSInt128ByPrecision())
      {
        bool dummy = false;
        char* ep = NULL;
        int128_t max_number_decimal =
            dataconvert::strtoll128(columnstore_big_precision[max_length - 19].c_str(), dummy, &ep);

        decimal = parm[0]->data()->getDecimalVal(row, isNull);

        int128_t scaleDivisor;
        datatypes::getScaleDivisor(scaleDivisor, abs(decimals - decimal.scale));

        if (!decimal.isTSInt128ByPrecision())
          decimal.s128Value = decimal.value;

        decimal.precision = max_length;

        if (scaleDivisor > 1)
        {
          if (decimals > decimal.scale)
            decimal.s128Value *= scaleDivisor;
          else
            decimal.s128Value =
                (int128_t)(decimal.s128Value > 0 ? (float128_t)decimal.s128Value / scaleDivisor + 0.5
                                                 : (float128_t)decimal.s128Value / scaleDivisor - 0.5);
        }

        decimal.scale = decimals;

        if (decimal.s128Value > max_number_decimal)
          decimal.s128Value = max_number_decimal;
        else if (decimal.s128Value < -max_number_decimal)
          decimal.s128Value = -max_number_decimal;
      }
      else
      {
        int64_t max_number_decimal = helpers::maxNumber_c[max_length];

        decimal = parm[0]->data()->getDecimalVal(row, isNull);

        if (decimal.isTSInt128ByPrecision())
        {
          if (decimal.s128Value > (int128_t)max_number_decimal)
            decimal.value = max_number_decimal;
          else if (decimal.s128Value < (int128_t)-max_number_decimal)
            decimal.value = -max_number_decimal;
          else
            decimal.value = decimal.s128Value;
        }

        decimal.precision = max_length;

        if (decimals > decimal.scale)
          decimal.value *= helpers::powerOf10_c[decimals - decimal.scale];
        else
          decimal.value =
              (int64_t)(decimal.value > 0
                            ? (double)decimal.value / helpers::powerOf10_c[decimal.scale - decimals] + 0.5
                            : (double)decimal.value / helpers::powerOf10_c[decimal.scale - decimals] - 0.5);

        decimal.scale = decimals;

        if (decimal.value > max_number_decimal)
          decimal.value = max_number_decimal;
        else if (decimal.value < -max_number_decimal)
          decimal.value = -max_number_decimal;
      }
    }
    break;

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      const string& strValue = parm[0]->data()->getStrVal(row, isNull);
      if (strValue.empty())
      {
        isNull = true;
        return IDB_Decimal();  // need a null value for IDB_Decimal??
      }
      datatypes::DataCondition convError;
      return IDB_Decimal(strValue.data(), strValue.length(), convError, decimals, max_length);
    }

    break;

    case execplan::CalpontSystemCatalog::DATE:
    {
      int32_t s = 0;

      string value = dataconvert::DataConvert::dateToString1(parm[0]->data()->getDateIntVal(row, isNull));
      int32_t x = atol(value.c_str());

      if (!isNull)
      {
        if (decimal.isTSInt128ByPrecision())
          decimal.s128Value = x;
        else
          decimal.value = x;

        decimal.scale = s;
      }
    }
    break;

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      int32_t s = 0;

      string value =
          dataconvert::DataConvert::datetimeToString1(parm[0]->data()->getDatetimeIntVal(row, isNull));

      // strip off micro seconds
      string date = value.substr(0, 14);

      int64_t x = atoll(date.c_str());

      if (!isNull)
      {
        if (decimal.isTSInt128ByPrecision())
          decimal.s128Value = x;
        else
          decimal.value = x;

        decimal.scale = s;
      }
    }
    break;

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      int32_t s = 0;

      string value = dataconvert::DataConvert::timestampToString1(
          parm[0]->data()->getTimestampIntVal(row, isNull), operationColType.getTimeZone());

      // strip off micro seconds
      string date = value.substr(0, 14);

      int64_t x = atoll(date.c_str());

      if (!isNull)
      {
        if (max_length > decimal.isTSInt128ByPrecision())
          decimal.s128Value = x;
        else
          decimal.value = x;

        decimal.scale = s;
      }
    }
    break;

    case execplan::CalpontSystemCatalog::TIME:
    {
      int32_t s = 0;

      string value = dataconvert::DataConvert::timeToString1(parm[0]->data()->getTimeIntVal(row, isNull));

      // strip off micro seconds
      string date = value.substr(0, 14);

      int64_t x = atoll(date.c_str());

      if (!isNull)
      {
        if (decimal.isTSInt128ByPrecision())
          decimal.s128Value = x;
        else
          decimal.value = x;

        decimal.scale = s;
      }
    }
    break;

    default:
    {
      std::ostringstream oss;
      oss << "cast: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  return decimal;
}

double Func_cast_decimal::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                       CalpontSystemCatalog::ColType& operationColType)
{
  IDB_Decimal decimal = Func_cast_decimal::getDecimalVal(row, parm, isNull, operationColType);

  // TODO MCOL-641 This could deliver wrong result b/c wide DECIMAL might have
  // p <= INT64MAXPRECISION || p > INT128MAXPRECISION
  if (decimal.isTSInt128ByPrecision())
  {
    return static_cast<double>(decimal);
  }

  return decimal.decimal64ToXFloat<double>();
}

//
//	Func_cast_double
//

int64_t Func_cast_double::getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& operationColType)
{
  double dblval = Func_cast_double::getDoubleVal(row, parm, isNull, operationColType);

  return (int64_t)dblval;
}

string Func_cast_double::getStrVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& operationColType)
{
  double dblval = Func_cast_double::getDoubleVal(row, parm, isNull, operationColType);

  std::string value = helpers::doubleToString(dblval);

  return value;
}

double Func_cast_double::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& operationColType)
{
  double dblval;

  // TODO: Here onwards
  switch (parm[0]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::DATE:
    case execplan::CalpontSystemCatalog::DATETIME:
    {
      int64_t intval = parm[0]->data()->getIntVal(row, isNull);
      dblval = (double)intval;
    }
    break;

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      string str = DataConvert::timestampToString1(parm[0]->data()->getTimestampIntVal(row, isNull),
                                                   operationColType.getTimeZone());

      // strip off micro seconds
      str = str.substr(0, 14);

      dblval = atof(str.c_str());
    }
    break;

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      uint64_t uintval = parm[0]->data()->getUintVal(row, isNull);
      dblval = (double)uintval;
    }
    break;

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      dblval = parm[0]->data()->getDoubleVal(row, isNull);
    }
    break;

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
    {
      dblval = static_cast<double>(parm[0]->data()->getLongDoubleVal(row, isNull));
    }
    break;

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      IDB_Decimal decimal = parm[0]->data()->getDecimalVal(row, isNull);

      if (parm[0]->data()->resultType().colWidth == datatypes::MAXDECIMALWIDTH)
      {
        dblval = static_cast<double>(decimal);
      }
      else
      {
        dblval = decimal.decimal64ToXFloat<double>();
      }
    }
    break;

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      const string& strValue = parm[0]->data()->getStrVal(row, isNull);

      dblval = strtod(strValue.c_str(), NULL);
    }
    break;

    default:
    {
      std::ostringstream oss;
      oss << "cast: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  return dblval;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

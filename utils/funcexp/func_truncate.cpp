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

//  $Id: func_truncate.cpp 3921 2013-06-19 18:59:56Z bwilkinson $

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

namespace
{
using namespace funcexp;

// P should be double or long double
template <typename P>
inline void decimalPlaceDouble(FunctionParm& fp, int64_t& s, P& p, Row& row, bool& isNull)
{
  s = fp[1]->data()->getIntVal(row, isNull);
  int64_t d = s;

  if (isNull)
    return;

  int64_t i = (d >= 0) ? d : (-d);
  int64_t r = 1;

  while (i--)
    r *= 10;

  if (d >= 0)
    p = (P)r;
  else
    p = 1.0 / ((P)r);
}

}  // namespace

namespace funcexp
{
CalpontSystemCatalog::ColType Func_truncate::operationType(FunctionParm& fp,
                                                           CalpontSystemCatalog::ColType& resultType)
{
  if (resultType.colDataType == execplan::CalpontSystemCatalog::DECIMAL ||
      resultType.colDataType == execplan::CalpontSystemCatalog::UDECIMAL)
  {
    CalpontSystemCatalog::ColType ct = fp[0]->data()->resultType();

    switch (ct.colDataType)
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
      case execplan::CalpontSystemCatalog::DECIMAL:
      case execplan::CalpontSystemCatalog::UDECIMAL:
      {
        if (resultType.scale > ct.scale)
          (resultType).scale = ct.scale;

        break;
      }

      default:
      {
        break;
      }
    }
  }

  return fp[0]->data()->resultType();
}

// truncate(X, D)
//

int64_t Func_truncate::getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                 CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal x = getDecimalVal(row, parm, isNull, op_ct);

  if (!op_ct.isWideDecimalType())
  {
    if (x.scale > 0)
    {
      while (x.scale-- > 0)
        x.value /= 10;
    }
    else
    {
      while (x.scale++ < 0)
        x.value *= 10;
    }

    return x.value;
  }
  else
  {
    return static_cast<int64_t>(x.getIntegralPart());
  }
}

uint64_t Func_truncate::getUintVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  uint64_t val = parm[0]->data()->getUintVal(row, isNull);

  if (isNull)
    return val;

  int64_t d = parm[1]->data()->getIntVal(row, isNull);

  if (isNull || d >= 0)
    return val;

  uint64_t p = 1;
  int64_t i = (-d);

  // Handle overflow since p can't have more than 19 0's
  if (i >= 20)
  {
    val = 0;
  }
  else
  {
    while (i--)
      p *= 10;

    val /= p;
    val *= p;
  }

  return val;
}

double Func_truncate::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  if (execplan::CalpontSystemCatalog::DOUBLE == op_ct.colDataType ||
      execplan::CalpontSystemCatalog::FLOAT == op_ct.colDataType)
  {
    int64_t d = 0;
    double p = 1;
    decimalPlaceDouble(parm, d, p, row, isNull);

    if (isNull)
      return 0.0;

    double x = parm[0]->data()->getDoubleVal(row, isNull);

    if (!isNull)
    {
      x *= p;

      if (x > 0)
        x = floor(x);
      else
        x = ceil(x);

      if (p != 0.0)
        x /= p;
      else
        x = 0.0;
    }

    return x;
  }

  IDB_Decimal x = getDecimalVal(row, parm, isNull, op_ct);

  if (isNull)
    return 0.0;

  double d;

  if (!op_ct.isWideDecimalType())
    d = x.value;
  else
    d = static_cast<double>(x.toTSInt128());

  if (x.scale > 0)
  {
    while (x.scale-- > 0)
      d /= 10.0;
  }
  else
  {
    while (x.scale++ < 0)
      d *= 10.0;
  }

  return d;
}

long double Func_truncate::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                            CalpontSystemCatalog::ColType& op_ct)
{
  if (execplan::CalpontSystemCatalog::LONGDOUBLE == op_ct.colDataType)
  {
    int64_t d = 0;
    long double p = 1;
    decimalPlaceDouble(parm, d, p, row, isNull);

    if (isNull)
      return 0.0;

    long double x = parm[0]->data()->getLongDoubleVal(row, isNull);

    if (!isNull)
    {
      x *= p;

      if (x > 0)
        x = floor(x);
      else
        x = ceil(x);

      if (p != 0.0)
        x /= p;
      else
        x = 0.0;
    }

    return x;
  }

  IDB_Decimal x = getDecimalVal(row, parm, isNull, op_ct);

  if (isNull)
    return 0.0;

  double d;

  if (!op_ct.isWideDecimalType())
    d = x.value;
  else
    d = static_cast<double>(x.toTSInt128());

  if (x.scale > 0)
  {
    while (x.scale-- > 0)
      d /= 10.0;
  }
  else
  {
    while (x.scale++ < 0)
      d *= 10.0;
  }

  return d;
}

IDB_Decimal Func_truncate::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                         CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal decimal;

  switch (op_ct.colDataType)
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
    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      int64_t d = 0;
      decimal = parm[0]->data()->getDecimalVal(row, isNull);

      if (!op_ct.isWideDecimalType())
      {
        //@Bug 3101 - GCC 4.5.1 optimizes too aggressively here. Mark as volatile.
        volatile int64_t p = 1;

        if (!isNull)
        {
          int64_t nvp = p;
          d = parm[1]->data()->getIntVal(row, isNull);

          if (!isNull)
            helpers::decimalPlaceDec(d, nvp, decimal.scale);

          p = nvp;
        }

        if (isNull)
          break;

        int64_t x = decimal.value;

        if (d > 0)
        {
          x = x * p;
        }
        else if (d < 0)
        {
          if ((x >= p) || (x <= -p))
          {
            if (p != 0)
              x = x / p;
            else
              x = 0;
          }
          else
          {
            x = 0;
          }
        }

        // negative scale is not supported by CNX yet, set d to 0.
        if (decimal.scale < 0)
        {
          do
            x *= 10;

          while (++decimal.scale < 0);
        }

        decimal.value = x;
      }
      else
      {
        //@Bug 3101 - GCC 4.5.1 optimizes too aggressively here. Mark as volatile.
        volatile int128_t p = 1;

        if (!isNull)
        {
          int128_t nvp = p;
          d = parm[1]->data()->getIntVal(row, isNull);

          if (!isNull)
            helpers::decimalPlaceDec(d, nvp, decimal.scale);

          p = nvp;
        }

        if (isNull)
          break;

        if (d < -datatypes::INT128MAXPRECISION)
        {
          decimal.s128Value = 0;
          break;
        }

        int128_t x = decimal.s128Value;

        if (d > 0)
        {
          x = x * p;
        }
        else if (d < 0)
        {
          if ((x >= p) || (x <= -p))
          {
            if (p != 0)
              x = x / p;
            else
              x = 0;
          }
          else
          {
            x = 0;
          }
        }

        // negative scale is not supported by CNX yet, set d to 0.
        if (decimal.scale < 0)
        {
          do
            x *= 10;

          while (++decimal.scale < 0);
        }

        decimal.s128Value = x;
      }
    }
    break;

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      int64_t s = 0;
      double p = 1;
      decimalPlaceDouble(parm, s, p, row, isNull);

      if (isNull)
        break;

      double x = parm[0]->data()->getDoubleVal(row, isNull);

      if (!isNull)
      {
        x *= p;
        decimal.value = (int64_t)x;
        decimal.scale = s;
      }
    }
    break;

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
    {
      int64_t s = 0;
      long double p = 1;
      decimalPlaceDouble(parm, s, p, row, isNull);

      if (isNull)
        break;

      long double x = parm[0]->data()->getLongDoubleVal(row, isNull);

      if (!isNull)
      {
        x *= p;
        decimal.value = (int64_t)x;
        decimal.scale = s;
      }
    }
    break;

    case execplan::CalpontSystemCatalog::DATE:
    {
      int32_t s = 0;
      int64_t x = 0;

      string value = DataConvert::dateToString1(parm[0]->data()->getDateIntVal(row, isNull));

      s = parm[1]->data()->getIntVal(row, isNull);

      if (!isNull)
      {
        x = atoll(value.c_str());

        if (s > 11)
          s = 0;

        if (s > 0)
        {
          x *= helpers::powerOf10_c[s];
        }
        else if (s < 0)
        {
          s = -s;

          if (s >= (int32_t)value.size())
          {
            x = 0;
          }
          else
          {
            x /= helpers::powerOf10_c[s];
            x *= helpers::powerOf10_c[s];
          }

          s = 0;
        }
      }

      decimal.value = x;
      decimal.scale = s;
    }
    break;

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      int32_t s = 0;
      int64_t x = 0;

      string value = DataConvert::datetimeToString1(parm[0]->data()->getDatetimeIntVal(row, isNull));

      s = parm[1]->data()->getIntVal(row, isNull);

      if (!isNull)
      {
        // strip off micro seconds
        value = value.substr(0, 14);
        int64_t x = atoll(value.c_str());

        if (s > 5)
          s = 0;

        if (s > 0)
        {
          x *= helpers::powerOf10_c[s];
        }
        else if (s < 0)
        {
          s = -s;

          if (s >= (int32_t)value.size())
          {
            x = 0;
          }
          else
          {
            x /= helpers::powerOf10_c[s];
            x *= helpers::powerOf10_c[s];
          }

          s = 0;
        }
      }

      decimal.value = x;
      decimal.scale = s;
    }
    break;

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      int32_t s = 0;
      int64_t x = 0;

      string value =
          DataConvert::timestampToString1(parm[0]->data()->getTimestampIntVal(row, isNull), op_ct.getTimeZone());

      s = parm[1]->data()->getIntVal(row, isNull);

      if (!isNull)
      {
        // strip off micro seconds
        value = value.substr(0, 14);
        x = atoll(value.c_str());

        if (s > 5)
          s = 0;

        if (s > 0)
        {
          x *= helpers::powerOf10_c[s];
        }
        else if (s < 0)
        {
          s = -s;

          if (s >= (int32_t)value.size())
          {
            x = 0;
          }
          else
          {
            x /= helpers::powerOf10_c[s];
            x *= helpers::powerOf10_c[s];
          }

          s = 0;
        }
      }

      decimal.value = x;
      decimal.scale = s;
    }
    break;

    case execplan::CalpontSystemCatalog::TIME:
    {
      int32_t s = 0;
      int64_t x = 0;

      string value = DataConvert::timeToString1(parm[0]->data()->getTimeIntVal(row, isNull));

      s = parm[1]->data()->getIntVal(row, isNull);

      if (!isNull)
      {
        // strip off micro seconds
        value = value.substr(0, 14);
        int64_t x = atoll(value.c_str());

        if (s > 5)
          s = 0;

        if (s > 0)
        {
          x *= helpers::powerOf10_c[s];
        }
        else if (s < 0)
        {
          s = -s;

          if (s >= (int32_t)value.size())
          {
            x = 0;
          }
          else
          {
            x /= helpers::powerOf10_c[s];
            x *= helpers::powerOf10_c[s];
          }

          s = 0;
        }
      }

      decimal.value = x;
      decimal.scale = s;
    }
    break;

    default:
    {
      std::ostringstream oss;
      oss << "truncate: datatype of " << execplan::colDataTypeToString(op_ct.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  return decimal;
}

string Func_truncate::getStrVal(Row& row, FunctionParm& parm, bool& isNull,
                                CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal x = getDecimalVal(row, parm, isNull, op_ct);

  switch (op_ct.colDataType)
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
      if (x.scale != 0)
      {
        if (x.scale > 0 && x.scale < 19)
        {
          x.value /= IDB_pow[x.scale];
        }
        else if (x.scale < 0 && x.scale > -19)
        {
          x.value *= IDB_pow[-x.scale];  // may overflow
        }
        else if (x.scale > 0)
        {
          x.value = 0;
        }
        else  // overflow may need throw exception
        {
          int64_t e = -x.scale % 18;
          x.value *= IDB_pow[e];
          e = -x.scale - e;

          while (e > 0)
          {
            x.value *= IDB_pow[18];
            e -= 18;
          }
        }

        x.scale = 0;
      }

      break;

    default: break;
  }

  if (!op_ct.isWideDecimalType())
    return x.toString();
  else
    return x.toString(true);
}

int64_t Func_truncate::getTimestampIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                          execplan::CalpontSystemCatalog::ColType& op_ct)
{
  return parm[0]->data()->getTimestampIntVal(row, isNull);
}

}  // namespace funcexp
// vim:ts=4 sw=4:

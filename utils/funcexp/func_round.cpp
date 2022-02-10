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
 * $Id: func_round.cpp 3921 2013-06-19 18:59:56Z bwilkinson $
 *
 *
 ****************************************************************************/

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

#include "funchelpers.h"

namespace
{
using namespace funcexp;

// P should either be double or long double
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
CalpontSystemCatalog::ColType Func_round::operationType(FunctionParm& fp,
                                                        CalpontSystemCatalog::ColType& resultType)
{
  if (resultType.colDataType == execplan::CalpontSystemCatalog::DECIMAL)
  {
    CalpontSystemCatalog::ColType ct = fp[0]->data()->resultType();

    switch (ct.colDataType)
    {
      case execplan::CalpontSystemCatalog::BIGINT:
      case execplan::CalpontSystemCatalog::INT:
      case execplan::CalpontSystemCatalog::MEDINT:
      case execplan::CalpontSystemCatalog::TINYINT:
      case execplan::CalpontSystemCatalog::SMALLINT:
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

// round(X), round(X, D)
//

int64_t Func_round::getIntVal(Row& row, FunctionParm& parm, bool& isNull,
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

uint64_t Func_round::getUintVal(Row& row, FunctionParm& parm, bool& isNull,
                                CalpontSystemCatalog::ColType& op_ct)
{
  uint64_t x;
  if (UNLIKELY(op_ct.colDataType == execplan::CalpontSystemCatalog::DATE))
  {
    IDB_Decimal d = getDecimalVal(row, parm, isNull, op_ct);
    x = static_cast<uint64_t>(d.value);
  }
  else
  {
    x = parm[0]->data()->getUintVal(row, isNull);
  }

  return x;
}

double Func_round::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                CalpontSystemCatalog::ColType& op_ct)
{
  if (execplan::CalpontSystemCatalog::DOUBLE == op_ct.colDataType ||
      execplan::CalpontSystemCatalog::FLOAT == op_ct.colDataType)
  {
    int64_t d = 0;
    double p = 1;

    if (parm.size() > 1)  // round(X, D)
      decimalPlaceDouble(parm, d, p, row, isNull);

    if (isNull)
      return 0.0;

    double x = parm[0]->data()->getDoubleVal(row, isNull);

    if (!isNull)
    {
      x *= p;

      if (x >= 0)
        x = floor(x + 0.5);
      else
        x = ceil(x - 0.5);

      if (p != 0.0)
        x /= p;
      else
        x = 0.0;
    }

    return x;
  }

  if (isUnsigned(op_ct.colDataType))
  {
    return getUintVal(row, parm, isNull, op_ct);
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

long double Func_round::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                         CalpontSystemCatalog::ColType& op_ct)
{
  if (execplan::CalpontSystemCatalog::LONGDOUBLE == op_ct.colDataType ||
      execplan::CalpontSystemCatalog::DOUBLE == op_ct.colDataType ||
      execplan::CalpontSystemCatalog::FLOAT == op_ct.colDataType)
  {
    int64_t d = 0;
    long double p = 1;

    if (parm.size() > 1)  // round(X, D)
      decimalPlaceDouble(parm, d, p, row, isNull);

    if (isNull)
      return 0.0;

    long double x = parm[0]->data()->getLongDoubleVal(row, isNull);

    if (!isNull)
    {
      x *= p;

      if (x >= 0)
        x = floor(x + 0.5);
      else
        x = ceil(x - 0.5);

      if (p != 0.0)
        x /= p;
      else
        x = 0.0;
    }

    return x;
  }

  if (isUnsigned(op_ct.colDataType))
  {
    return getUintVal(row, parm, isNull, op_ct);
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

IDB_Decimal Func_round::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
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
    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      int64_t d = 0;
      decimal = parm[0]->data()->getDecimalVal(row, isNull);

      if (!op_ct.isWideDecimalType())
      {
        //@Bug 3101 - GCC 4.5.1 optimizes too aggressively here. Mark as volatile.
        volatile int64_t p = 1;

        if (!isNull && parm.size() > 1)  // round(X, D)
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
          int64_t h = p / 2;  // 0.5

          if ((x >= h) || (x <= -h))
          {
            if (x >= 0)
              x += h;
            else
              x -= h;

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

        if (!isNull && parm.size() > 1)  // round(X, D)
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
          int128_t h = p / 2;  // 0.5

          if ((x >= h) || (x <= -h))
          {
            if (x >= 0)
              x += h;
            else
              x -= h;

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

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      uint64_t x = parm[0]->data()->getUintVal(row, isNull);

      if (x > (uint64_t)helpers::maxNumber_c[18])
      {
        x = helpers::maxNumber_c[18];
      }

      decimal.value = x;
      decimal.scale = 0;
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

      if (parm.size() > 1)  // round(X, D)
        decimalPlaceDouble(parm, s, p, row, isNull);

      if (isNull)
        break;

      double x = parm[0]->data()->getDoubleVal(row, isNull);

      if (!isNull)
      {
        x *= p;

        if (x >= 0)
          x = floor(x + 0.5);
        else
          x = ceil(x - 0.5);

        decimal.value = (int64_t)x;
        decimal.scale = s;
      }
    }
    break;

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
    {
      int64_t s = 0;
      long double p = 1;

      if (parm.size() > 1)  // round(X, D)
        decimalPlaceDouble(parm, s, p, row, isNull);

      if (isNull)
        break;

      long double x = parm[0]->data()->getDoubleVal(row, isNull);

      if (!isNull)
      {
        x *= p;

        if (x >= 0)
          x = floor(x + 0.5);
        else
          x = ceil(x - 0.5);

        decimal.value = (int64_t)x;
        decimal.scale = s;
      }
    }
    break;

    case execplan::CalpontSystemCatalog::DATE:
    {
      int32_t s = 0;

      string value = dataconvert::DataConvert::dateToString1(parm[0]->data()->getDateIntVal(row, isNull));

      if (parm.size() > 1)  // round(X, D)
      {
        s = parm[1]->data()->getIntVal(row, isNull);

        if (s > 0)
        {
          s = 0;  // Dates don't have digits after int part
        }
        else
        {
          if (-s >= (int32_t)value.size())
            value = "0";
          else
          {
            // check to see if last digit needs to be rounded up
            int firstcutdigit = atoi(value.substr(value.size() + s, 1).c_str());
            value = value.substr(0, value.size() + s);
            int lastdigit = atoi(value.substr(value.size() - 1, 1).c_str());

            if (firstcutdigit > 5)
            {
              lastdigit++;
              string lastStr = intToString(lastdigit);
              value = value.substr(0, value.size() - 1) + lastStr;
            }

            s = -s;

            for (int i = 0; i < s; i++)
            {
              value = value + "0";
            }
          }

          s = 0;
        }
      }

      int64_t x = atoll(value.c_str());

      if (!isNull)
      {
        decimal.value = x;
        decimal.scale = s;
      }
    }
    break;

    case execplan::CalpontSystemCatalog::TIME:
    case execplan::CalpontSystemCatalog::DATETIME:
    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      int32_t s = 0;

      string value;
      if (op_ct.colDataType == execplan::CalpontSystemCatalog::TIMESTAMP)
        value = dataconvert::DataConvert::timestampToString1(parm[0]->data()->getTimestampIntVal(row, isNull),
                                                             op_ct.getTimeZone());
      else
        value = dataconvert::DataConvert::datetimeToString1(parm[0]->data()->getDatetimeIntVal(row, isNull));

      // strip off micro seconds
      value = value.substr(0, 14);

      if (parm.size() > 1)  // round(X, D)
      {
        s = parm[1]->data()->getIntVal(row, isNull);

        if (s > 5)
          s = 0;

        if (s > 0)
        {
          for (int i = 0; i < s; i++)
          {
            value = value + "0";
          }
        }
        else
        {
          if (-s >= (int32_t)value.size())
            value = "0";
          else
          {
            // check to see if last digit needs to be rounded up
            int firstcutdigit = atoi(value.substr(value.size() + s, 1).c_str());
            value = value.substr(0, value.size() + s);
            int lastdigit = atoi(value.substr(value.size() - 1, 1).c_str());

            if (firstcutdigit > 5)
            {
              lastdigit++;
              string lastStr = intToString(lastdigit);
              value = value.substr(0, value.size() - 1) + lastStr;
            }

            s = -s;

            for (int i = 0; i < s; i++)
            {
              value = value + "0";
            }
          }

          s = 0;
        }
      }

      int64_t x = atoll(value.c_str());

      if (!isNull)
      {
        decimal.value = x;
        decimal.scale = s;
      }
    }
    break;

    default:
    {
      std::ostringstream oss;
      oss << "round: datatype of " << execplan::colDataTypeToString(op_ct.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  return decimal;
}

string Func_round::getStrVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal x = getDecimalVal(row, parm, isNull, op_ct);
  int64_t e = (x.scale < 0) ? (-x.scale) : x.scale;
  int64_t p = 1;

  while (e-- > 0)
    p *= 10;

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

int64_t Func_round::getDatetimeIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& op_ct)
{
  return parm[0]->data()->getIntVal(row, isNull);
}

int64_t Func_round::getTimestampIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                       execplan::CalpontSystemCatalog::ColType& op_ct)
{
  return parm[0]->data()->getTimestampIntVal(row, isNull);
}

}  // namespace funcexp
// vim:ts=4 sw=4:

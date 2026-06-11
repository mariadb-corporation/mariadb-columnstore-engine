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
 * $Id: func_time_to_sec.cpp 2477 2011-04-01 16:07:35Z rdempsey $
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
#include "mcs_time.h"

namespace funcexp
{
namespace
{
constexpr int64_t MAX_NUMBER_AS_TIME = 999'99'99LL;                // HHH'MM'SS
constexpr int64_t MAX_NUMBER_AS_DATETIME = 9999'12'31'23'59'59LL;  // YYYY'MM'DD'HH'MM'SS
}  // namespace

CalpontSystemCatalog::ColType Func_time_to_sec::operationType(FunctionParm& /*fp*/,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

std::pair<int64_t, int64_t> Func_time_to_sec::secondsWithUsec(rowgroup::Row& row, FunctionParm& parm,
                                                              bool& isNull,
                                                              CalpontSystemCatalog::ColType& op_ct)
{
  // assume 256 is enough. assume not allowing incomplete date
  int32_t hour = 0, min = 0, sec = 0;
  bool bIsNegative = false;  // Only set to true if CHAR or VARCHAR with a '-'

  int64_t val = 0;
  int64_t mask = 0;
  int64_t usec = 0;  // microsecond fraction magnitude (0..999999)
  dataconvert::Time tval;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case CalpontSystemCatalog::DATE: return {0, 0};

    case CalpontSystemCatalog::DATETIME:
      val = parm[0]->data()->getIntVal(row, isNull);
      hour = (int32_t)((val >> 32) & 0x3f);
      min = (int32_t)((val >> 26) & 0x3f);
      sec = (int32_t)((val >> 20) & 0x3f);
      usec = (int64_t)(val & 0xfffff);
      break;

    case CalpontSystemCatalog::TIMESTAMP:
    {
      val = parm[0]->data()->getIntVal(row, isNull);
      dataconvert::TimeStamp timestamp(val);
      int64_t seconds = timestamp.second;
      dataconvert::MySQLTime time;
      dataconvert::gmtSecToMySQLTime(seconds, time, op_ct.getTimeZone());
      hour = time.hour;
      min = time.minute;
      sec = time.second;
      usec = (int64_t)(val & 0xfffff);
      break;
    }

    case CalpontSystemCatalog::TIME:
      val = parm[0]->data()->getTimeIntVal(row, isNull);

      // If negative, mask so it doesn't turn positive
      if ((val >> 40) & 0x800)
        mask = 0xfffffffffffff000;

      bIsNegative = val >> 63;
      hour = (int32_t)(mask | ((val >> 40) & 0xfff));

      if ((hour >= 0) && bIsNegative)
        hour *= -1;
      else
        bIsNegative = false;

      min = (int32_t)((val >> 32) & 0xff);
      sec = (int32_t)((val >> 24) & 0xff);
      usec = (int64_t)(val & 0xffffff);
      break;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::VARCHAR:
    {
      std::string strVal = parm[0]->data()->getStrVal(row, isNull).safeString("");

      if (strVal.length() > 0 && strVal[0] == '-')
      {
        bIsNegative = true;
        strVal.replace(0, 1, 1, ' ');
      }

      val = dataconvert::DataConvert::stringToTime(strVal);

      if (val == -1)
      {
        isNull = true;
        return {-1, 0};
      }
      else
      {
        tval = *(reinterpret_cast<dataconvert::Time*>(&val));
        hour = (uint32_t)(tval.hour);
        min = (uint32_t)(tval.minute);
        sec = (uint32_t)(tval.second);
        usec = (int64_t)(tval.msecond);
      }
    }
    break;

    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::INT:
      val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));

      if (val == -1)
      {
        isNull = true;
        return {-1, 0};
      }
      else
      {
        hour = (int32_t)((val >> 32) & 0x3f);
        min = (int32_t)((val >> 26) & 0x3f);
        sec = (int32_t)((val >> 20) & 0x3f);
      }

      break;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      execplan::IDB_Decimal argDec = parm[0]->data()->getDecimalVal(row, isNull);

      if (isNull)
        return {0, 0};

      const CalpontSystemCatalog::ColType& argType = parm[0]->data()->resultType();
      const int32_t argScale = (argType.scale < 0) ? 0 : argType.scale;

      datatypes::int128_t rawVal = datatypes::toInt128ByPrecision(argDec, argType.precision);

      const bool valNeg = (rawVal < 0);
      bool saturated = false;   // set when an out-of-range TIME is capped to the maximum
      bool timeInterp = false;  // set when the value is read as a TIME (HHMMSS[.ffffff])

      const datatypes::int128_t divisor = datatypes::scaleDivisor<datatypes::int128_t>(argScale);

      datatypes::int128_t intPart =
          rawVal / divisor;  // Keep the integer part 128-bit: a wide DECIMAL(38,0) can exceed int64
      datatypes::int128_t fracPart =
          (rawVal < 0 ? -rawVal : rawVal) %
          divisor;  // Take the magnitude by hand: std::abs() has no int128_t overload

      // Mirror the server's rule (Sec6::to_datetime_or_time):
      // a number whose integer part is in (9999999, 99991231235959] and is
      // non-negative is interpreted as a DATE/DATETIME and TIME_TO_SEC takes its
      // time-of-day portion; otherwise it is interpreted as a TIME value.
      if (!valNeg && intPart > MAX_NUMBER_AS_TIME && intPart <= MAX_NUMBER_AS_DATETIME)
      {
        // DATE/DATETIME interpretation: keep only the time-of-day portion.

        bool dateOnly = false;
        val = dataconvert::DataConvert::intToDatetime((int64_t)intPart, &dateOnly);

        if (val == -1)
        {
          isNull = true;
          return {-1, 0};
        }

        hour = (int32_t)((val >> 32) & 0x3f);
        min = (int32_t)((val >> 26) & 0x3f);
        sec = (int32_t)((val >> 20) & 0x3f);

        // A pure date (YYYYMMDD) has no time-of-day; the server also truncates
        // any fractional digits in that case (e.g. 20010203.5 -> time 0).
        if (dateOnly)
          fracPart = 0;
      }
      else
      {
        // TIME interpretation: HHMMSS[.ffffff]. Mirror the server's
        // number_to_time_only(): extract hh/mm/ss from the full magnitude, reject a
        // malformed minute/second (e.g. 9909090 -> 99:90:90 -> NULL), saturate
        // hours beyond the TIME_MAX_HOUR to 838:59:59.999999.

        bIsNegative = valNeg;
        timeInterp = true;

        const datatypes::int128_t mag = (intPart < 0) ? -intPart : intPart;
        const int32_t ss = (int32_t)(mag % 100);
        const int32_t mm = (int32_t)((mag / 100) % 100);
        const datatypes::int128_t hh = mag / 10000;

        if (mm >= 60 || ss >= 60)
        {
          isNull = true;
          return {-1, 0};
        }

        if (hh > datatypes::TIME_MAX_HOUR)
        {
          hour = datatypes::TIME_MAX_HOUR;
          min = 59;
          sec = 59;
          saturated = true;
        }
        else
        {
          hour = (int32_t)hh;
          min = mm;
          sec = ss;
        }
      }

      // Rescale the fractional part to microseconds. A capped (out-of-range) TIME
      // also takes the server's maximum fractional seconds.
      if (saturated)
        usec = 999999;
      else
        usec = datatypes::fracToMicroseconds(fracPart, argScale);

      // Mirror number_to_time_only(): a numeric TIME whose truncated microseconds
      // reach TIME_MAX_SECOND_PART (999999) is rejected as an invalid time -> NULL.
      // This does not apply to a capped (saturated) maximum or to the DATETIME path.
      if (timeInterp && !saturated && usec >= 999999)
      {
        isNull = true;
        return {-1, 0};
      }

      break;
    }

    default: isNull = true; return {-1, 0};
  }

  int64_t rtn;

  if (hour < 0)
  {
    rtn = (int64_t)(hour * 60 * 60) - (min * 60) - sec;
  }
  else
  {
    rtn = (int64_t)(hour * 60 * 60) + (min * 60) + sec;
  }

  if (bIsNegative)
  {
    rtn *= -1;
  }

  // Carry the time's sign onto the fractional part so the numeric accessors can
  // simply add usec to rtn
  bool resultNeg = (rtn < 0) || (rtn == 0 && bIsNegative);

  return {rtn, resultNeg ? -usec : usec};
}

int64_t Func_time_to_sec::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& op_ct)
{
  return secondsWithUsec(row, parm, isNull, op_ct).first;
}

double Func_time_to_sec::getDoubleVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& op_ct)
{
  auto [sec, usec] = secondsWithUsec(row, parm, isNull, op_ct);
  return (double)sec + (double)usec / 1'000'000.0;
}

long double Func_time_to_sec::getLongDoubleVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                               CalpontSystemCatalog::ColType& op_ct)
{
  auto [sec, usec] = secondsWithUsec(row, parm, isNull, op_ct);
  return (long double)sec + (long double)usec / 1'000'000.0L;
}

execplan::IDB_Decimal Func_time_to_sec::getDecimalVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                                      CalpontSystemCatalog::ColType& op_ct)
{
  // For TIME_TO_SEC the server caps the result scale at MAX_SCALE via
  // Item::time_precision -> MY_MIN(decimals, TIME_SECOND_PART_DIGITS)
  const int32_t scale =
      (op_ct.scale < 0) ? 0
                        : (op_ct.scale > datatypes::TIME_MAX_SCALE ? datatypes::TIME_MAX_SCALE : op_ct.scale);

  auto [sec, usec] = secondsWithUsec(row, parm, isNull, op_ct);

  const int64_t microseconds = sec * 1'000'000 + usec;  // fits int64 (< 4e12), may be negative
  const int64_t value = microseconds / (int64_t)datatypes::mcs_pow_10[datatypes::TIME_MAX_SCALE - scale];

  // Populate both the int64 and int128 fields (val == val128) so the result is
  // correct both for a narrow and a wide decimal column type.
  return execplan::IDB_Decimal(value, scale, op_ct.precision, value);
}

}  // namespace funcexp

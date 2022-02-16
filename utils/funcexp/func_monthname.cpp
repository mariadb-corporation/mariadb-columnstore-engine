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
 * $Id: func_monthname.cpp 2477 2011-04-01 16:07:35Z rdempsey $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_str.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{
CalpontSystemCatalog::ColType Func_monthname::operationType(FunctionParm& fp,
                                                            CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

string Func_monthname::getStrVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                 CalpontSystemCatalog::ColType& op_ct)
{
  int32_t month = getIntVal(row, parm, isNull, op_ct);

  if (month == -1)
    return "";

  return helpers::monthFullNames[month];
}

int32_t Func_monthname::getDateIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& ct)
{
  uint32_t val = getIntVal(row, parm, isNull, ct);
  return val;
}

int64_t Func_monthname::getDatetimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                          CalpontSystemCatalog::ColType& ct)
{
  uint32_t val = getIntVal(row, parm, isNull, ct);
  return val;
}

int64_t Func_monthname::getTimestampIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                           CalpontSystemCatalog::ColType& ct)
{
  uint32_t val = getIntVal(row, parm, isNull, ct);
  return val;
}

int64_t Func_monthname::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                  CalpontSystemCatalog::ColType& op_ct)
{
  int64_t val = 0;
  dataconvert::DateTime aDateTime;
  dataconvert::Time aTime;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case CalpontSystemCatalog::DATE:
      val = parm[0]->data()->getIntVal(row, isNull);
      return (unsigned)((val >> 12) & 0xf);

    case CalpontSystemCatalog::DATETIME:
      val = parm[0]->data()->getIntVal(row, isNull);
      return (unsigned)((val >> 44) & 0xf);

    case CalpontSystemCatalog::TIMESTAMP:
    {
      val = parm[0]->data()->getIntVal(row, isNull);
      dataconvert::TimeStamp timestamp(val);
      int64_t seconds = timestamp.second;
      dataconvert::MySQLTime time;
      dataconvert::gmtSecToMySQLTime(seconds, time, op_ct.getTimeZone());
      return time.month;
    }

    // Time adds to now() and then gets value
    case CalpontSystemCatalog::TIME:
      aDateTime = static_cast<dataconvert::DateTime>(nowDatetime());
      aTime = parm[0]->data()->getTimeIntVal(row, isNull);
      aTime.day = 0;
      val = addTime(aDateTime, aTime);
      return (unsigned)((val >> 44) & 0xf);
      break;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::VARCHAR:
      val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));

      if (val == -1)
      {
        isNull = true;
        return -1;
      }
      else
      {
        return (unsigned)((val >> 44) & 0xf);
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
        return -1;
      }
      else
      {
        return (unsigned)((val >> 44) & 0xf);
      }

      break;

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
      if (parm[0]->data()->resultType().scale == 0)
      {
        val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));

        if (val == -1)
        {
          isNull = true;
          return -1;
        }
        else
        {
          return (unsigned)((val >> 44) & 0xf);
        }
      }

      break;

    default: isNull = true; return -1;
  }

  return -1;
}

double Func_monthname::getDoubleVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                    execplan::CalpontSystemCatalog::ColType& op_ct)
{
  return 0;
}

execplan::IDB_Decimal Func_monthname::getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                                    execplan::CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal d;

  if (fp[0]->data()->resultType().isWideDecimalType())
    d.s128Value = getIntVal(row, fp, isNull, op_ct);
  else
    d.value = getIntVal(row, fp, isNull, op_ct);
  d.scale = 0;
  return d;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

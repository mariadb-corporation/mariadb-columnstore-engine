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

namespace funcexp
{
CalpontSystemCatalog::ColType Func_time_to_sec::operationType(FunctionParm& fp,
                                                              CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_time_to_sec::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& op_ct)
{
  // assume 256 is enough. assume not allowing incomplete date
  int32_t hour = 0, min = 0, sec = 0;
  bool bIsNegative = false;  // Only set to true if CHAR or VARCHAR with a '-'

  int64_t val = 0;
  int64_t mask = 0;
  dataconvert::Time tval;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case CalpontSystemCatalog::DATE: return 0;

    case CalpontSystemCatalog::DATETIME:
      val = parm[0]->data()->getIntVal(row, isNull);
      hour = (int32_t)((val >> 32) & 0x3f);
      min = (int32_t)((val >> 26) & 0x3f);
      sec = (int32_t)((val >> 20) & 0x3f);
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
      break;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::VARCHAR:
    {
      std::string strVal = parm[0]->data()->getStrVal(row, isNull);

      if (strVal[0] == '-')
      {
        bIsNegative = true;
        strVal.replace(0, 1, 1, ' ');
      }

      val = dataconvert::DataConvert::stringToTime(strVal);

      if (val == -1)
      {
        isNull = true;
        return -1;
      }
      else
      {
        tval = *(reinterpret_cast<dataconvert::Time*>(&val));
        hour = (uint32_t)(tval.hour);
        min = (uint32_t)(tval.minute);
        sec = (uint32_t)(tval.second);
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
        return -1;
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
          hour = (int32_t)((val >> 32) & 0x3f);
          min = (int32_t)((val >> 26) & 0x3f);
          sec = (int32_t)((val >> 20) & 0x3f);
        }
      }
      else
      {
        isNull = true;
        return -1;
      }

      break;

    default: isNull = true; return -1;
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

  return rtn;
}

}  // namespace funcexp
// vim:ts=4 sw=4:

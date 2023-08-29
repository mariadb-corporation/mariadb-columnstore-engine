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
 * $Id: func_day.cpp 3495 2013-01-21 14:09:51Z rdempsey $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
#include <llvm/IR/IRBuilder.h>
using namespace std;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{
CalpontSystemCatalog::ColType Func_day::operationType(FunctionParm& fp,
                                                      CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_day::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                            CalpontSystemCatalog::ColType& op_ct)
{
  int64_t val = 0;

  dataconvert::DateTime aDateTime;
  dataconvert::Time aTime;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case CalpontSystemCatalog::DATE:
      val = parm[0]->data()->getIntVal(row, isNull);
      return (uint32_t)((val >> 6) & 0x3f);

    case CalpontSystemCatalog::DATETIME:
      val = parm[0]->data()->getIntVal(row, isNull);
      return (uint32_t)((val >> 38) & 0x3f);

    case CalpontSystemCatalog::TIMESTAMP:
    {
      dataconvert::TimeStamp timestamp(parm[0]->data()->getTimestampIntVal(row, isNull));
      int64_t seconds = timestamp.second;
      dataconvert::MySQLTime m_time;
      dataconvert::gmtSecToMySQLTime(seconds, m_time, op_ct.getTimeZone());
      return m_time.day;
    }

    // Time adds to now() and then gets value
    case CalpontSystemCatalog::TIME:
      aDateTime = static_cast<dataconvert::DateTime>(nowDatetime());
      aTime = parm[0]->data()->getTimeIntVal(row, isNull);
      aTime.day = 0;
      val = addTime(aDateTime, aTime);
      return (uint32_t)((val >> 38) & 0x3f);
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
        return (uint32_t)((val >> 38) & 0x3f);
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
        return (uint32_t)((val >> 38) & 0x3f);
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
          return (uint32_t)((val >> 38) & 0x3f);
        }
      }
      else
      {
        isNull = true;
      }

      break;

    default: isNull = true; return -1;
  }

  return -1;
}

bool Func_day::isCompilable(const execplan::CalpontSystemCatalog::ColType& colType)
{
  switch (colType.colDataType)
  {
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
      return true;
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME:
    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::INT:
    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL: return false;
    default: return false;
  }
}

llvm::Value* Func_day::compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                               rowgroup::Row& row, FunctionParm& fp,
                               execplan::CalpontSystemCatalog::ColType& op_ct)
{
  llvm::Value* val;
  CalpontSystemCatalog::ColDataType dataType;
  switch (fp[0]->data()->resultType().colDataType)
  {
    case CalpontSystemCatalog::DATE:
      dataType = CalpontSystemCatalog::INT;
      val = fp[0]->compile(b, data, isNull, row, dataType);
      return b.CreateTrunc(b.CreateAnd(b.CreateLShr(val, 6), 0x3f), b.getInt32Ty());
    case CalpontSystemCatalog::DATETIME:
      dataType = CalpontSystemCatalog::INT;
      val = fp[0]->compile(b, data, isNull, row, dataType);
      return b.CreateTrunc(b.CreateAnd(b.CreateLShr(val, 38), 0x3f), b.getInt32Ty());
    default: throw ::logic_error("Func_day::compile: unsupported type");
  }
}

}  // namespace funcexp
// vim:ts=4 sw=4:

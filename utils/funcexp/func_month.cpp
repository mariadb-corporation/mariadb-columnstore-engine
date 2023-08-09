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
 * $Id: func_month.cpp 3495 2013-01-21 14:09:51Z rdempsey $
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
CalpontSystemCatalog::ColType Func_month::operationType(FunctionParm& fp,
                                                        CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_month::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
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

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      dataconvert::TimeStamp timestamp(parm[0]->data()->getTimestampIntVal(row, isNull));
      int64_t seconds = timestamp.second;
      dataconvert::MySQLTime m_time;
      dataconvert::gmtSecToMySQLTime(seconds, m_time, op_ct.getTimeZone());
      return m_time.month;
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
      else
      {
        isNull = true;
      }

      break;

    default: isNull = true; return -1;
  }

  return -1;
}
bool Func_month::isCompilable(const execplan::CalpontSystemCatalog::ColType& colType)
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
llvm::Value* Func_month::compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                               rowgroup::Row& row, FunctionParm& fp,
                               execplan::CalpontSystemCatalog::ColType& op_ct)
{
  llvm::Value* val;
  switch (fp[0]->data()->resultType().colDataType)
  {
    case CalpontSystemCatalog::DATE:
      val = fp[0]->compile(b, data, isNull, row, CalpontSystemCatalog::INT);
      return b.CreateTrunc(b.CreateAnd(b.CreateLShr(val, 12), 0xf), b.getInt32Ty());
    case CalpontSystemCatalog::DATETIME:
      val = fp[0]->compile(b, data, isNull, row, CalpontSystemCatalog::INT);
      return b.CreateTrunc(b.CreateAnd(b.CreateLShr(val, 44), 0xf), b.getInt32Ty());
    default: throw ::logic_error("Func_month::compile: unsupported type");
  }
}
}  // namespace funcexp
// vim:ts=4 sw=4:

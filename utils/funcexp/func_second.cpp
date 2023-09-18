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
 * $Id: func_second.cpp 3495 2013-01-21 14:09:51Z rdempsey $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{
CalpontSystemCatalog::ColType Func_second::operationType(FunctionParm& fp,
                                                         CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_second::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
  int64_t val = 0;

  switch (parm[0]->data()->resultType().colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));

      if (val == -1)
        isNull = true;

      break;
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      if (parm[0]->data()->resultType().scale == 0)
      {
        val = dataconvert::DataConvert::intToDatetime(parm[0]->data()->getIntVal(row, isNull));

        if (val == -1)
          isNull = true;
      }
      else
      {
        isNull = true;
      }

      break;
    }

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    {
      isNull = true;
    }
      /* fall through */

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      val = dataconvert::DataConvert::stringToDatetime(parm[0]->data()->getStrVal(row, isNull));

      if (val == -1)
        isNull = true;

      break;
    }

    case execplan::CalpontSystemCatalog::DATE:
    {
      val = parm[0]->data()->getDatetimeIntVal(row, isNull);
      break;
    }

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      val = parm[0]->data()->getDatetimeIntVal(row, isNull);
      break;
    }

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      dataconvert::TimeStamp timestamp(parm[0]->data()->getTimestampIntVal(row, isNull));
      int64_t seconds = timestamp.second;
      dataconvert::MySQLTime m_time;
      dataconvert::gmtSecToMySQLTime(seconds, m_time, op_ct.getTimeZone());
      return m_time.second;
    }

    case execplan::CalpontSystemCatalog::TIME:
    {
      val = parm[0]->data()->getTimeIntVal(row, isNull);
      return (uint32_t)((val >> 24) & 0xff);
      break;
    }

    default:
    {
      isNull = true;
      break;
    }
  }

  if (isNull)
    return -1;

  if (val < 1000000000)
    return 0;

  return (uint32_t)((val >> 20) & 0x3f);
}
bool Func_second::isCompilable(const execplan::CalpontSystemCatalog::ColType& colType)
{
  switch (colType.colDataType)
  {
    case CalpontSystemCatalog::TIME:
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::INT:
    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL: return true;
    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::VARCHAR: return false;
    default: return false;
  }
}
llvm::Value* Func_second::compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                                  rowgroup::Row& row, FunctionParm& fp,
                                  execplan::CalpontSystemCatalog::ColType& op_ct)
{
  llvm::Value* val;
  llvm::Function* func;
  switch (fp[0]->data()->resultType().colDataType)
  {
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
      val = fp[0]->compile(b, data, isNull, row, CalpontSystemCatalog::DATETIME);
      break;
    case execplan::CalpontSystemCatalog::TIMESTAMP:
      func = b.GetInsertBlock()->getParent()->getParent()->getFunction(
          "dataconvert::DataConvert::timestampValueToInt");
      if (!func)
      {
        throw ::logic_error(
            "Func_second::compile: dataconvert::DataConvert::timestampValueToInt function not found");
      }
      val = b.CreateCall(func, {fp[0]->compile(b, data, isNull, row, CalpontSystemCatalog::TIMESTAMP),
                                b.getInt64(op_ct.getTimeZone())});
      b.CreateStore(b.CreateOr(b.CreateLoad(b.getInt1Ty(), isNull), b.CreateICmpEQ(val, b.getInt64(-1))),
                    isNull);
      break;
    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::INT:
      func = b.GetInsertBlock()->getParent()->getParent()->getFunction(
          "dataconvert::DataConvert::intToDatetime");
      if (!func)
      {
        throw ::logic_error(
            "Func_second::compile: dataconvert::DataConvert::intToDatetime function not found");
      }
      val = b.CreateCall(func, {fp[0]->compile(b, data, isNull, row, CalpontSystemCatalog::INT), isNull});
      b.CreateStore(b.CreateOr(b.CreateLoad(b.getInt1Ty(), isNull), b.CreateICmpEQ(val, b.getInt64(-1))),
                    isNull);
      break;
    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
      if (fp[0]->data()->resultType().scale == 0)
      {
        func = b.GetInsertBlock()->getParent()->getParent()->getFunction(
            "dataconvert::DataConvert::intToDatetime");
        if (!func)
        {
          throw ::logic_error(
              "Func_second::compile: dataconvert::DataConvert::intToDatetime function not found");
        }
        val = b.CreateCall(func, {fp[0]->compile(b, data, isNull, row, CalpontSystemCatalog::INT), isNull});
        b.CreateStore(b.CreateOr(b.CreateLoad(b.getInt1Ty(), isNull), b.CreateICmpEQ(val, b.getInt64(-1))),
                      isNull);
      }
      else
      {
        b.CreateStore(b.getTrue(), isNull);
        val = b.getInt64(-1);
      }
      break;
    case CalpontSystemCatalog::TIME:
      val = fp[0]->compile(b, data, isNull, row, CalpontSystemCatalog::TIME);
      return b.CreateTrunc(b.CreateAnd(b.CreateLShr(val, 24), 0xff), b.getInt32Ty());
      break;
    default: throw ::logic_error("Func_month::compile: unsupported type");
  }

  val = b.CreateSelect(b.CreateICmpSLT(val, b.getInt64(1000000000)), b.getInt32(0),
                       b.CreateTrunc(b.CreateAnd(b.CreateLShr(val, 20), 0x3f), b.getInt32Ty()));

  return b.CreateSelect(isNull, b.getInt32(0), val);
}
}  // namespace funcexp

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
 * $Id: func_hour.cpp 3495 2013-01-21 14:09:51Z rdempsey $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
#include <llvm/IR/Constants.h>
using namespace std;

#include "functor_int.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"

namespace funcexp
{
CalpontSystemCatalog::ColType Func_hour::operationType(FunctionParm& fp,
                                                       CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_hour::getIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                             CalpontSystemCatalog::ColType& op_ct)
{
  int64_t val = 0;
  bool isTime = false;

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
      return m_time.hour;
    }

    case execplan::CalpontSystemCatalog::TIME:
    {
      isTime = true;
      val = parm[0]->data()->getTimeIntVal(row, isNull);
      break;
    }

    default:
    {
      isNull = true;
    }
  }

  if (isNull)
    return -1;

  if (isTime)
  {
    // HOUR() is always positive in MariaDB, even for negative time
    int64_t mask = 0;

    if ((val >> 40) & 0x800)
      mask = 0xfffffffffffff000;

    val = abs(mask | ((val >> 40) & 0xfff));
  }
  else
  {
    val = (val >> 32) & 0x3f;
  }

  return val;
}

bool Func_hour::isCompilable(const execplan::CalpontSystemCatalog::ColType& colType)
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
llvm::Value* Func_hour::compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                                llvm::Value* dataConditionError, rowgroup::Row& row, FunctionParm& fp,
                                execplan::CalpontSystemCatalog::ColType& op_ct)
{
  llvm::Value* val;
  llvm::Function* func;
  switch (fp[0]->data()->resultType().colDataType)
  {
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
      val = fp[0]->compile(b, data, isNull, dataConditionError, row, CalpontSystemCatalog::DATETIME);
      break;
    case execplan::CalpontSystemCatalog::TIMESTAMP:
      func = b.GetInsertBlock()->getParent()->getParent()->getFunction(
          "dataconvert::DataConvert::timestampValueToInt");
      if (!func)
      {
        throw ::logic_error(
            "Func_month::compile: dataconvert::DataConvert::timestampValueToInt function not found");
      }
      val = b.CreateCall(
          func, {fp[0]->compile(b, data, isNull, dataConditionError, row, CalpontSystemCatalog::TIMESTAMP),
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
            "Func_month::compile: dataconvert::DataConvert::intToDatetime function not found");
      }
      val = b.CreateCall(
          func,
          {fp[0]->compile(b, data, isNull, dataConditionError, row, CalpontSystemCatalog::INT), isNull});
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
              "Func_month::compile: dataconvert::DataConvert::intToDatetime function not found");
        }
        val = b.CreateCall(
            func,
            {fp[0]->compile(b, data, isNull, dataConditionError, row, CalpontSystemCatalog::INT), isNull});
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
    {
      val = fp[0]->compile(b, data, isNull, dataConditionError, row, CalpontSystemCatalog::TIME);
      llvm::Value* mask = b.CreateSelect(b.CreateAnd(b.CreateLShr(val, 40), 0x800),
                                         b.getInt64(0xfffffffffffff000), b.getInt64(0));
      llvm::Value* value = b.CreateOr(mask, b.CreateAnd(b.CreateLShr(val, 40), 0xfff));
      llvm::Value* cmp = b.CreateICmpSGE(value, llvm::ConstantInt::get(value->getType(), 0));
      return b.CreateSelect(cmp, value, b.CreateNeg(value));
    }

    default: throw ::logic_error("Func_month::compile: unsupported type");
  }

  return b.CreateAnd(b.CreateLShr(val, 32), 0x3f);
}
}  // namespace funcexp

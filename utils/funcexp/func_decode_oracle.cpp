/* Copyright (C) 2021 MariaDB Corporation

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

#include <string>
//#define NDEBUG
#include <cassert>
using namespace std;

#include "functor_all.h"
#include "functioncolumn.h"
#include "predicateoperator.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

using namespace funcexp;

namespace
{
using namespace funcexp;

inline uint64_t simple_case_cmp(Row& row, FunctionParm& parm, bool& isNull,
                                CalpontSystemCatalog::ColType& operationColType)
{
  uint64_t i = 0;                            // index to the parm list
  uint64_t n = 0;                            // remove expression from count of expression_i + result_i
  uint64_t hasElse = (parm.size() - 1) % 2;  // if 1, then ELSE exist
  uint64_t whereCount = hasElse ? (parm.size() - 2) / 2 : (parm.size() - 1) / 2;
  bool foundIt = false;

  switch (operationColType.colDataType)
  {
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::DATE:
    {
      int64_t ev = parm[n]->data()->getIntVal(row, isNull);

      if (isNull)
        break;

      for (i = 1; i <= whereCount; i++)
      {
        if (ev == parm[i]->data()->getIntVal(row, isNull) && !isNull)
        {
          foundIt = true;
          break;
        }
        else
          isNull = false;
      }

      break;
    }

    case execplan::CalpontSystemCatalog::DATETIME:
    {
      int64_t ev = parm[n]->data()->getDatetimeIntVal(row, isNull);

      if (isNull)
        break;

      for (i = 1; i <= whereCount; i++)
      {
        if (ev == parm[i]->data()->getDatetimeIntVal(row, isNull) && !isNull)
        {
          foundIt = true;
          break;
        }
        else
          isNull = false;
      }

      break;
    }

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      int64_t ev = parm[n]->data()->getTimestampIntVal(row, isNull);

      if (isNull)
        break;

      for (i = 1; i <= whereCount; i++)
      {
        if (ev == parm[i]->data()->getTimestampIntVal(row, isNull) && !isNull)
        {
          foundIt = true;
          break;
        }
        else
          isNull = false;
      }

      break;
    }

    case execplan::CalpontSystemCatalog::TIME:
    {
      int64_t ev = parm[n]->data()->getTimeIntVal(row, isNull);

      if (isNull)
        break;

      for (i = 1; i <= whereCount; i++)
      {
        if (ev == parm[i]->data()->getTimeIntVal(row, isNull) && !isNull)
        {
          foundIt = true;
          break;
        }
        else
          isNull = false;
      }

      break;
    }

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      uint64_t ev = parm[n]->data()->getUintVal(row, isNull);

      if (isNull)
        break;

      for (i = 1; i <= whereCount; i++)
      {
        if (ev == parm[i]->data()->getUintVal(row, isNull) && !isNull)
        {
          foundIt = true;
          break;
        }
        else
          isNull = false;
      }

      break;
    }

    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    case execplan::CalpontSystemCatalog::VARCHAR:
    {
      const string& ev = parm[n]->data()->getStrVal(row, isNull);
      if (isNull)
        break;
      CHARSET_INFO* cs = parm[n]->data()->resultType().getCharset();

      for (i = 1; i <= whereCount; i++)
      {
        // BUG 5362
        const string& p1 = parm[i]->data()->getStrVal(row, isNull);
        if (isNull)
          break;
        if (cs->strnncoll(ev.c_str(), ev.length(), p1.c_str(), p1.length()) == 0)
        {
          foundIt = true;
          break;
        }
      }

      break;
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      IDB_Decimal ev = parm[n]->data()->getDecimalVal(row, isNull);

      if (isNull)
        break;

      for (i = 1; i <= whereCount; i++)
      {
        if (ev == parm[i]->data()->getDecimalVal(row, isNull) && !isNull)
        {
          foundIt = true;
          break;
        }
        else
          isNull = false;
      }

      break;
    }

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    {
      double ev = parm[n]->data()->getDoubleVal(row, isNull);

      if (isNull)
        break;

      for (i = 1; i <= whereCount; i++)
      {
        if (ev == parm[i]->data()->getDoubleVal(row, isNull) && !isNull)
        {
          foundIt = true;
          break;
        }
        else
          isNull = false;
      }

      break;
    }

    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      float ev = parm[n]->data()->getFloatVal(row, isNull);

      if (isNull)
        break;

      for (i = 1; i <= whereCount; i++)
      {
        if (ev == parm[i]->data()->getFloatVal(row, isNull) && !isNull)
        {
          foundIt = true;
          break;
        }
        else
          isNull = false;
      }

      break;
    }

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
    {
      long double ev = parm[n]->data()->getLongDoubleVal(row, isNull);

      if (isNull)
        break;

      for (i = 1; i <= whereCount; i++)
      {
        if (ev == parm[i]->data()->getLongDoubleVal(row, isNull) && !isNull)
        {
          foundIt = true;
          break;
        }
        else
          isNull = false;
      }

      break;
    }

    default:
    {
      std::ostringstream oss;
      oss << "case: datatype of " << execplan::colDataTypeToString(operationColType.colDataType);
      throw logging::IDBExcept(oss.str(), ERR_DATATYPE_NOT_SUPPORT);
    }
  }

  if (!foundIt && !hasElse)
    isNull = true;
  else if (!foundIt && hasElse && !isNull)
  {
    i = parm.size() - 1;
  }
  else if (isNull && hasElse)
  // BUG 5110. Only way we can exit above with isNull == true is when ev is NULL
  // if so and we have else condition we need to use it by setting i = else
  {
    i = parm.size() - 1;
    isNull = false;
  }

  if (foundIt)
  {
    i += whereCount;
  }

  return i;
}

CalpontSystemCatalog::ColType caseOperationType(FunctionParm& fp, CalpontSystemCatalog::ColType& resultType,
                                                bool simpleCase)
{
  uint64_t simple = simpleCase ? 1 : 0;
  bool hasElse = (((fp.size() - simple) % 2) != 0);  // if 1, then ELSE exist

  uint64_t parmCount = hasElse ? (fp.size() - 2) : (fp.size() - 1);
  uint64_t whereCount = hasElse ? (fp.size() - 2 + simple) / 2 : (fp.size() - 1) / 2 + simple;

  bool allStringO = true;
  bool allStringR = true;

  FunctionParm::size_type l = fp.size() - 1;  // last fp index
  idbassert(fp[l]->data());
  CalpontSystemCatalog::ColType oct = fp[l]->data()->resultType();
  CalpontSystemCatalog::ColType rct = resultType;
  bool operation = true;

  for (uint64_t i = 0; i <= parmCount; i++)
  {
    // for SimpleCase, we return the type of the case expression,
    // which will always be in position 0.
    if (i == 0 && simpleCase)
    {
      if (fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::CHAR &&
          fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::TEXT &&
          fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::VARCHAR)
      {
        PredicateOperator op;
        op.setOpType(oct, fp[i]->data()->resultType());
        allStringO = false;
        oct = op.operationType();
      }

      i += 1;
    }

    // operation or result type
    operation = ((i > 0 + simple) && (i <= whereCount));

    if (fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::CHAR &&
        fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::TEXT &&
        fp[i]->data()->resultType().colDataType != CalpontSystemCatalog::VARCHAR)
    {
      // this is not a string column
      PredicateOperator op;

      if (operation)
      {
        if (!simpleCase)
        {
          op.setOpType(oct, fp[i]->data()->resultType());
          allStringO = false;
          oct = op.operationType();
        }
      }

      // If any parm is of string type, the result type should be string. (same as if)
      else if (rct.colDataType != CalpontSystemCatalog::CHAR &&
               rct.colDataType != CalpontSystemCatalog::TEXT &&
               rct.colDataType != CalpontSystemCatalog::VARCHAR)
      {
        op.setOpType(rct, fp[i]->data()->resultType());
        allStringR = false;
        rct = op.operationType();
      }
    }
    else
    {
      // this is a string
      // If any parm is of string type, the result type should be string. (same as if)
      allStringR = true;
    }
  }

  if (allStringO)
  {
    oct.colDataType = CalpontSystemCatalog::VARCHAR;
    oct.colWidth = 255;
  }

  if (allStringR)
  {
    rct.colDataType = CalpontSystemCatalog::VARCHAR;
    rct.colWidth = 255;
  }

  if (rct.scale != 0 && rct.colDataType == CalpontSystemCatalog::BIGINT)
    rct.colDataType = CalpontSystemCatalog::DECIMAL;

  if (oct.scale != 0 && oct.colDataType == CalpontSystemCatalog::BIGINT)
    oct.colDataType = CalpontSystemCatalog::DECIMAL;

  resultType = rct;
  return oct;
}

}  // namespace

namespace funcexp
{
// simple CASE:
// SELECT CASE ("expression")
// WHEN "condition1" THEN "result1"
// WHEN "condition2" THEN "result2"
// ...
// [ELSE "resultN"]
// END
//
// simple CASE parm order:
//   expression condition1 condition2 ... result1 result2 ... [resultN]
//
// Note that this order changed in 10.2.14, see MCOL-1341

CalpontSystemCatalog::ColType Func_decode_oracle::operationType(FunctionParm& fp,
                                                                CalpontSystemCatalog::ColType& resultType)
{
  return caseOperationType(fp, resultType, true);
}

bool Func_decode_oracle::getBoolVal(Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& operationColType)
{
  uint64_t i = simple_case_cmp(row, parm, isNull, operationColType);

  if (isNull)
    return false;

  ParseTree* lop = parm[i]->left();
  ParseTree* rop = parm[i]->right();
  if (lop && rop)
  {
    return (reinterpret_cast<Operator*>(parm[i]->data()))->getBoolVal(row, isNull, lop, rop);
  }

  return parm[i]->data()->getBoolVal(row, isNull);
}

int64_t Func_decode_oracle::getIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& operationColType)
{
  uint64_t i = simple_case_cmp(row, parm, isNull, operationColType);

  if (isNull)
    return joblist::BIGINTNULL;

  return parm[i]->data()->getIntVal(row, isNull);
}

string Func_decode_oracle::getStrVal(Row& row, FunctionParm& parm, bool& isNull,
                                     CalpontSystemCatalog::ColType& operationColType)
{
  uint64_t i = simple_case_cmp(row, parm, isNull, operationColType);

  if (isNull)
    return string("");

  return parm[i]->data()->getStrVal(row, isNull);
}

IDB_Decimal Func_decode_oracle::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                              CalpontSystemCatalog::ColType& operationColType)
{
  uint64_t i = simple_case_cmp(row, parm, isNull, operationColType);

  if (isNull)
    return IDB_Decimal();  // need a null value for IDB_Decimal??

  return parm[i]->data()->getDecimalVal(row, isNull);
}

double Func_decode_oracle::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                        CalpontSystemCatalog::ColType& operationColType)
{
  uint64_t i = simple_case_cmp(row, parm, isNull, operationColType);

  if (isNull)
    return doubleNullVal();

  return parm[i]->data()->getDoubleVal(row, isNull);
}

long double Func_decode_oracle::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                                 CalpontSystemCatalog::ColType& operationColType)
{
  uint64_t i = simple_case_cmp(row, parm, isNull, operationColType);

  if (isNull)
    return doubleNullVal();

  return parm[i]->data()->getLongDoubleVal(row, isNull);
}

int32_t Func_decode_oracle::getDateIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                          execplan::CalpontSystemCatalog::ColType& op_ct)
{
  uint64_t i = simple_case_cmp(row, parm, isNull, op_ct);

  if (isNull)
    return joblist::DATENULL;

  return parm[i]->data()->getDateIntVal(row, isNull);
}

int64_t Func_decode_oracle::getDatetimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                              execplan::CalpontSystemCatalog::ColType& op_ct)
{
  uint64_t i = simple_case_cmp(row, parm, isNull, op_ct);

  if (isNull)
    return joblist::DATETIMENULL;

  return parm[i]->data()->getDatetimeIntVal(row, isNull);
}

int64_t Func_decode_oracle::getTimestampIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                               execplan::CalpontSystemCatalog::ColType& op_ct)
{
  uint64_t i = simple_case_cmp(row, parm, isNull, op_ct);

  if (isNull)
    return joblist::TIMESTAMPNULL;

  return parm[i]->data()->getTimestampIntVal(row, isNull);
}

int64_t Func_decode_oracle::getTimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                          execplan::CalpontSystemCatalog::ColType& op_ct)
{
  uint64_t i = simple_case_cmp(row, parm, isNull, op_ct);

  if (isNull)
    return joblist::TIMENULL;

  return parm[i]->data()->getTimeIntVal(row, isNull);
}

}  // namespace funcexp

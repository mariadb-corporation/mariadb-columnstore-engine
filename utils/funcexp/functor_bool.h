/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2020 MariaDB Corporation

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

//  $Id: functor_bool.h 3495 2013-01-21 14:09:51Z rdempsey $

/** @file */

#pragma once

#include "functor.h"

namespace funcexp
{
/** @brief Func_Bool class
 *    For function that returns a boolean result.
 *        Must implement getBoolVal()
 *        Implement any other methods that behave differently from the default.
 */
class Func_Bool : public Func
{
 public:
  Func_Bool()
  {
  }
  Func_Bool(const std::string& funcName) : Func(funcName)
  {
  }
  virtual ~Func_Bool()
  {
  }

  /*
      virtual bool getBoolVal(rowgroup::Row& row,
                                                              FunctionParm& fp,
                                                              bool& isNull,
                                                              execplan::CalpontSystemCatalog::ColType& op_ct)
     = 0;
  */

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                    execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    return (getBoolVal(row, fp, isNull, op_ct) ? 1 : 0);
  }

  double getDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                      execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    return (getBoolVal(row, fp, isNull, op_ct) ? 1.0 : 0.0);
  }

  long double getLongDoubleVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                               execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    return (getBoolVal(row, fp, isNull, op_ct) ? 1.0 : 0.0);
  }

  std::string getStrVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    return (getBoolVal(row, fp, isNull, op_ct) ? "1" : "0");
  }

  execplan::IDB_Decimal getDecimalVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                                      execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    return execplan::IDB_Decimal(getIntVal(row, fp, isNull, op_ct), 0, 0);
  }

  int32_t getDateIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    isNull = true;
    return 0;
  }

  int64_t getDatetimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                            execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    isNull = true;
    return 0;
  }

  int64_t getTimestampIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                             execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    isNull = true;
    return 0;
  }

  int64_t getTimeIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                        execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    isNull = true;
    return 0;
  }
};

/** @brief Func_between class
 */
class Func_between : public Func_Bool
{
 public:
  Func_between() : Func_Bool("between")
  {
  }
  virtual ~Func_between()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_notbetween class
 */
class Func_notbetween : public Func_Bool
{
 public:
  Func_notbetween() : Func_Bool("notbetween")
  {
  }
  virtual ~Func_notbetween()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_in class
 */
class Func_in : public Func_Bool
{
 public:
  Func_in() : Func_Bool("in")
  {
  }
  virtual ~Func_in()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_notin class
 */
class Func_notin : public Func_Bool
{
 public:
  Func_notin() : Func_Bool("notin")
  {
  }
  virtual ~Func_notin()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_regexp class
 */
class Func_regexp : public Func_Bool
{
 public:
  Func_regexp() : Func_Bool("regexp")
  {
  }
  virtual ~Func_regexp()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct);
};

/** @brief Func_isnull class
 */
class Func_isnull : public Func_Bool
{
 public:
  /*
   * Constructor. Pass the function name to the base constructor.
   */
  Func_isnull() : fIsNotNull(false)
  {
  }
  Func_isnull(bool isnotnull) : fIsNotNull(isnotnull)
  {
  }
  /*
   * Destructor. isnull does not need to do anything here to clean up.
   */
  virtual ~Func_isnull()
  {
  }

  /**
   * Decide on the function's operation type
   */
  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType);

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct);

 private:
  bool fIsNotNull;
};

/** @brief Func_Truth class
 */
class Func_Truth : public Func_Bool
{
 public:
  Func_Truth(const std::string& funcName, bool a_value, bool a_affirmative)
   : Func_Bool(funcName), value(a_value), affirmative(a_affirmative)
  {
  }

  virtual ~Func_Truth()
  {
  }

  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType)
  {
    assert(fp.size() == 1);
    return fp[0]->data()->resultType();
  }

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    bool val = fp[0]->data()->getBoolVal(row, isNull);

    /*
      NULL val IS {TRUE, FALSE} --> FALSE
      NULL val IS NOT {TRUE, FALSE} --> TRUE
      {TRUE, FALSE} val IS {TRUE, FALSE} value --> val == value
      {TRUE, FALSE} val IS NOT {TRUE, FALSE} value --> val != value
      These cases can be reduced to the following bitwise operation.
    */
    bool ret = (isNull & (!affirmative)) | ((!isNull) & (affirmative ^ (value ^ val)));

    isNull = false;

    return ret;
  }

 private:
  const bool value, affirmative;
};

/** @brief Func_IsTrue class
 */
class Func_IsTrue : public Func_Truth
{
 public:
  Func_IsTrue() : Func_Truth("istrue", true, true)
  {
  }
  ~Func_IsTrue()
  {
  }
};

/** @brief Func_IsNotTrue class
 */
class Func_IsNotTrue : public Func_Truth
{
 public:
  Func_IsNotTrue() : Func_Truth("isnottrue", true, false)
  {
  }
  ~Func_IsNotTrue()
  {
  }
};

/** @brief Func_IsFalse class
 */
class Func_IsFalse : public Func_Truth
{
 public:
  Func_IsFalse() : Func_Truth("isfalse", false, true)
  {
  }
  ~Func_IsFalse()
  {
  }
};

/** @brief Func_IsNotFalse class
 */
class Func_IsNotFalse : public Func_Truth
{
 public:
  Func_IsNotFalse() : Func_Truth("isnotfalse", false, false)
  {
  }
  ~Func_IsNotFalse()
  {
  }
};

class Func_Compare : public Func_Bool
{
  bool fAllowEqual;
  bool fAllowGreater;
  bool fAllowLess;
 public:
  Func_Compare(const std::string& name, bool eq, bool gt, bool lt) :
    Func_Bool(name), fAllowEqual(eq),
    fAllowGreater(gt), fAllowLess(lt)
  {
  }
  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType)
  {
idblog("result data type is " << ((int)resultType.colDataType) << ", op name is " << fFuncName);
    return resultType;
  }

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    return getBoolVal(row, fp, isNull, op_ct);
  }

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    idbassert(fp.size() == 2);
    int64_t r = 0;
    switch (fp[0]->data()->resultType().colDataType)
    {
      case execplan::CalpontSystemCatalog::TEXT:
      case execplan::CalpontSystemCatalog::CHAR:
      case execplan::CalpontSystemCatalog::VARCHAR:
      {
        CHARSET_INFO* cs = fp[0]->data()->resultType().getCharset();
        const auto& str = fp[0]->data()->getStrVal(row, isNull);
        const auto& str1 = fp[1]->data()->getStrVal(row, isNull);

        r = cs->strnncollsp(str.str(), str.length(), str1.str(), str1.length());

        break;
      }
      case execplan::CalpontSystemCatalog::DECIMAL:
      case execplan::CalpontSystemCatalog::UDECIMAL:
      {
        execplan::IDB_Decimal a1 = fp[0]->data()->getDecimalVal(row, isNull);
	execplan::IDB_Decimal a2 = fp[1]->data()->getDecimalVal(row, isNull);
	r = a1 > a2 ? 1 : a1 < a2 ? -1 : 0;
        break;
      }
      case execplan::CalpontSystemCatalog::LONGDOUBLE:
      {
        long double a1 = fp[0]->data()->getLongDoubleVal(row, isNull);
        long double a2 = fp[1]->data()->getLongDoubleVal(row, isNull);
	r = a1 > a2 ? 1 : a1 < a2 ? -1 : 0;
        break;
      }
      case execplan::CalpontSystemCatalog::DOUBLE:
      case execplan::CalpontSystemCatalog::UDOUBLE:
      {
        double a1 = fp[0]->data()->getDoubleVal(row, isNull);
        double a2 = fp[1]->data()->getDoubleVal(row, isNull);
	r = a1 > a2 ? 1 : a1 < a2 ? -1 : 0;
        break;
      }
      case execplan::CalpontSystemCatalog::FLOAT:
      case execplan::CalpontSystemCatalog::UFLOAT:
      {
        float a1 = fp[0]->data()->getFloatVal(row, isNull);
        float a2 = fp[1]->data()->getFloatVal(row, isNull);
	r = a1 > a2 ? 1 : a1 < a2 ? -1 : 0;
        break;
      }
      case execplan::CalpontSystemCatalog::UBIGINT:
      case execplan::CalpontSystemCatalog::UINT:
      case execplan::CalpontSystemCatalog::UMEDINT:
      case execplan::CalpontSystemCatalog::UTINYINT:
      case execplan::CalpontSystemCatalog::USMALLINT:
      {
        uint64_t lval = fp[0]->data()->getUintVal(row, isNull);
	uint64_t rval = fp[1]->data()->getUintVal(row, isNull);
	r = lval > rval ? 1 : lval < rval ? -1 : 0;
	break;
      }
      case execplan::CalpontSystemCatalog::BIGINT:
      case execplan::CalpontSystemCatalog::INT:
      case execplan::CalpontSystemCatalog::MEDINT:
      case execplan::CalpontSystemCatalog::TINYINT:
      case execplan::CalpontSystemCatalog::SMALLINT:
      {
        int64_t lval = fp[0]->data()->getIntVal(row, isNull);
	int64_t rval = fp[1]->data()->getIntVal(row, isNull);
	r = lval - rval;
	break;
      }
      case execplan::CalpontSystemCatalog::DATE:
      {
        int64_t lval = fp[0]->data()->getDateIntVal(row, isNull);
	int64_t rval = fp[1]->data()->getDateIntVal(row, isNull);
	r = lval - rval;
	break;
      }
      case execplan::CalpontSystemCatalog::DATETIME:
      {
        int64_t lval = fp[0]->data()->getDatetimeIntVal(row, isNull);
	int64_t rval = fp[1]->data()->getDatetimeIntVal(row, isNull);
	r = lval - rval;
	break;
      }
      case execplan::CalpontSystemCatalog::TIME:
      {
        int64_t lval = fp[0]->data()->getTimeIntVal(row, isNull);
	int64_t rval = fp[1]->data()->getTimeIntVal(row, isNull);
	r = lval - rval;
	break;
      }
      case execplan::CalpontSystemCatalog::TIMESTAMP:
      {
        int64_t lval = fp[0]->data()->getTimestampIntVal(row, isNull);
	int64_t rval = fp[1]->data()->getTimestampIntVal(row, isNull);
	r = lval - rval;
	break;
      }
      default:
      {
        std::ostringstream oss;
        oss << "comparison: datatype of " << execplan::colDataTypeToString(fp[0]->data()->resultType().colDataType);
        throw logging::IDBExcept(oss.str(), logging::ERR_DATATYPE_NOT_SUPPORT);
      }
    }
    return (fAllowEqual && r == 0) || (fAllowGreater && r > 0) || (fAllowLess && r < 0);
  }
};

class Func_Logic_Op : public Func_Bool
{
 public:
  enum LogicOp { AND, OR, XOR };
 private:
  LogicOp fLogicOp;
 public:
  Func_Logic_Op(const std::string& name, LogicOp op) :
    Func_Bool(name), fLogicOp(op)
  {
  }
  execplan::CalpontSystemCatalog::ColType operationType(FunctionParm& fp,
                                                        execplan::CalpontSystemCatalog::ColType& resultType)
  {
idblog("result data type is " << ((int)resultType.colDataType));
    return resultType;
  }

  int64_t getIntVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    return getBoolVal(row, fp, isNull, op_ct);
  }

  bool getBoolVal(rowgroup::Row& row, FunctionParm& fp, bool& isNull,
                  execplan::CalpontSystemCatalog::ColType& op_ct)
  {
    bool result = fLogicOp == AND ? true : false;
    for (uint32_t i = 0; i < fp.size(); i++)
    {
      // we must use a specific value for x when its' isNull is true.
      // yet, we have to maintain whole-expression isNull which may
      // be returned.
      bool xIsNull = false;
      bool x = fp[i]->data()->getBoolVal(row, xIsNull);
      isNull = isNull || xIsNull;
      switch (fLogicOp)
      {
        case AND:
	{
          result = result && (xIsNull ? true : x);
	  if (!result) // short circuit.
	  {
            isNull = false;
            return result;
	  }
	  break;
	}
        case OR:
	{
          result = result || (xIsNull ? false : x);
	  if (result) // short circuit.
	  {
            isNull = false;
            return result;
	  }
	  break;
	}
        case XOR:
	{
          if (isNull)
	  {
            return false;
	  }
          result = result ? !x : x;
	  break;
	}
      }
    }
    return result;
  }
};

}  // namespace funcexp

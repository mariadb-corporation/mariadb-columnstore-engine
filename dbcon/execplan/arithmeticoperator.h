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

/***********************************************************************
 *   $Id$
 *
 *
 ***********************************************************************/
/** @file */

#pragma once
#include <string>
#include <iosfwd>
#include <cmath>
#include <sstream>
#include <limits>

#include "operator.h"
#include "parsetree.h"
#include "mcs_datatype.h"
#include "exceptclasses.h"
#include "overflow_config.h"

namespace messageqcpp
{
class ByteStream;
}

namespace execplan
{

class ArithmeticOperator : public Operator
{
  using cscType = execplan::CalpontSystemCatalog::ColType;

 public:
  ArithmeticOperator();
  explicit ArithmeticOperator(const std::string& operatorName);
  ArithmeticOperator(const ArithmeticOperator& rhs);

  ~ArithmeticOperator() override;

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  inline ArithmeticOperator* clone() const override
  {
    return new ArithmeticOperator(*this);
  }

  inline long timeZone() const
  {
    return fTimeZone;
  }
  inline void timeZone(const long timeZone)
  {
    fTimeZone = timeZone;
  }

  /**
   * The serialization interface
   */
  void serialize(messageqcpp::ByteStream&) const override;
  void unserialize(messageqcpp::ByteStream&) override;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
   */
  bool operator==(const TreeNode* t) const override;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
   */
  bool operator==(const ArithmeticOperator& t) const;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return false iff every member of t is a duplicate copy of every member of this; true otherwise
   */
  bool operator!=(const TreeNode* t) const override;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return false iff every member of t is a duplicate copy of every member of this; true otherwise
   */
  bool operator!=(const ArithmeticOperator& t) const;

  /***********************************************************
   *                 F&E framework                           *
   ***********************************************************/
  using Operator::evaluate;
  inline void evaluate(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override;

  using Operator::getStrVal;
  const utils::NullString& getStrVal(rowgroup::Row& row, bool& isNull, ParseTree* lop,
                                     ParseTree* rop) override
  {
    bool localIsNull = false;
    evaluate(row, localIsNull, lop, rop);
    isNull = isNull || localIsNull;
    return localIsNull ? fResult.strVal.dropString() : TreeNode::getStrVal(fTimeZone);
  }
  using Operator::getIntVal;
  int64_t getIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getIntVal();
  }
  using Operator::getUintVal;
  uint64_t getUintVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getUintVal();
  }
  using Operator::getFloatVal;
  float getFloatVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getFloatVal();
  }
  using Operator::getDoubleVal;
  double getDoubleVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getDoubleVal();
  }
  using Operator::getLongDoubleVal;
  long double getLongDoubleVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getLongDoubleVal();
  }
  using Operator::getDecimalVal;
  IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);

    // @bug5736, double type with precision -1 indicates that this type is for decimal math,
    //      the original decimal scale is stored in scale field, which is no use for double.
    if (fResultType.colDataType == CalpontSystemCatalog::DOUBLE && fResultType.precision == -1)
    {
      IDB_Decimal rv;
      rv.scale = fResultType.scale;
      rv.precision = 15;
      rv.value = (int64_t)(TreeNode::getDoubleVal() * IDB_pow[rv.scale]);

      return rv;
    }

    return TreeNode::getDecimalVal();
  }
  using Operator::getDateIntVal;
  int32_t getDateIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getDateIntVal();
  }
  using Operator::getDatetimeIntVal;
  int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getDatetimeIntVal();
  }
  using Operator::getTimestampIntVal;
  int64_t getTimestampIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getTimestampIntVal();
  }
  using Operator::getTimeIntVal;
  int64_t getTimeIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getTimeIntVal();
  }
  using Operator::getBoolVal;
  bool getBoolVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getBoolVal();
  }
  void adjustResultType(const CalpontSystemCatalog::ColType& m);
  inline bool getOverflowCheck() const
  {
    return fDecimalOverflowCheck;
  }
  inline void setOverflowCheck(bool check)
  {
    fDecimalOverflowCheck = check;
  }

  inline std::string toCppCode(IncludeSet& includes) const override
  {
    includes.insert("arithmeticoperator.h");
    std::stringstream ss;
    ss << "ArithmeticOperator(" << std::quoted(fData) << ")";

    return ss.str();
  }

 private:
  template <typename result_t>
  inline result_t execute(result_t op1, result_t op2, bool& isNull);
  inline void execute(IDB_Decimal& result, IDB_Decimal op1, IDB_Decimal op2, bool& isNull);
  long fTimeZone;
  bool fDecimalOverflowCheck;
};

#include "parsetree.h"

inline void ArithmeticOperator::evaluate(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
{
  // fOpType should have already been set on the connector during parsing
  switch (fOperationType.colDataType)
  {
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::TINYINT:
      fResult.intVal = execute(lop->getIntVal(row, isNull), rop->getIntVal(row, isNull), isNull);
      if (isNull)
      {
        fResult.intVal = joblist::INTNULL;
      }
      break;

    case execplan::CalpontSystemCatalog::UBIGINT:
    {
      // XXX: this is bandaid solution for specific customer case (MCOL-5568).
      // Despite that I tried to implement a proper solution: to have operations
      // performed using int128_t amd then check the result.
      int128_t x, y;
      bool signedLeft = lop->data()->resultType().isSignedInteger();
      bool signedRight = rop->data()->resultType().isSignedInteger();
      if (signedLeft)
      {
        x = static_cast<int128_t>(lop->getIntVal(row, isNull));
      }
      else
      {
        x = static_cast<int128_t>(lop->getUintVal(row, isNull));
      }
      if (signedRight)
      {
        y = static_cast<int128_t>(rop->getIntVal(row, isNull));
      }
      else
      {
        y = static_cast<int128_t>(rop->getUintVal(row, isNull));
      }
      int128_t result = execute(x, y, isNull);
      if (!isNull && (result > MAX_UBIGINT || result < 0))
      {
        if (columnstore::isStrictOverflowMode())
	{
          logging::Message::Args args;
          std::string func = "<unknown>";
          switch (fOp)
          {
            case OP_ADD: func = "\"+\""; break;
            case OP_SUB: func = "\"-\""; break;
            case OP_MUL: func = "\"*\""; break;
            case OP_DIV: func = "\"/\""; break;
            default: break;
          }
          args.add(func);
          args.add(static_cast<double>(x));
          args.add(static_cast<double>(y));
          unsigned errcode = logging::ERR_FUNC_OUT_OF_RANGE_RESULT;
          throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(errcode, args), errcode);
	}
	isNull = true;
      }
      fResult.uintVal = static_cast<uint64_t>(result);
    }
    break;
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
      fResult.uintVal = execute(lop->getUintVal(row, isNull), rop->getUintVal(row, isNull), isNull);
      if (isNull)
      {
        fResult.uintVal = joblist::UBIGINTNULL;
      }
      break;

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    case execplan::CalpontSystemCatalog::UFLOAT:
      fResult.doubleVal = execute(lop->getDoubleVal(row, isNull), rop->getDoubleVal(row, isNull), isNull);
      break;

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
      fResult.longDoubleVal =
          execute(lop->getLongDoubleVal(row, isNull), rop->getLongDoubleVal(row, isNull), isNull);
      break;

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
      execute(fResult.decimalVal, lop->getDecimalVal(row, isNull), rop->getDecimalVal(row, isNull), isNull);
      break;

    default:
    {
      std::ostringstream oss;
      oss << "invalid arithmetic operand type: " << fOperationType.colDataType;
      throw logging::InvalidArgumentExcept(oss.str());
    }
  }
}

template <typename result_t>
inline result_t ArithmeticOperator::execute(result_t op1, result_t op2, bool& isNull)
{
  if (isNull)
  {
    // at least one operand is NULL.
    // do nothing, return 0.
    if constexpr (std::is_same<result_t, datatypes::TSInt128>::value)
    {
      return datatypes::TSInt128();  // returns 0
    }
    else
    {
      return 0;
    }
  }
  switch (fOp)
  {
    case OP_ADD:
    {
      // Check for addition overflow
      // XXX: __int128 is not signed for some reason.
      if constexpr (std::is_signed_v<result_t> || std::is_same<__int128, result_t>::value)
      {
        if ((op2 > 0 && op1 > std::numeric_limits<result_t>::max() - op2) ||
            (op2 < 0 && op1 < std::numeric_limits<result_t>::min() - op2))
        {
          if (columnstore::isStrictOverflowMode())
          {
            // Strict mode: throw exception for out of range
            std::ostringstream oss;
            oss << "BIGINT value is out of range in addition";
            throw logging::IDBExcept(oss.str(), logging::ERR_FUNC_OUT_OF_RANGE_RESULT);
          }
          else
          {
            // Permissive mode: set null and return clamped value
            isNull = true;
            return (op2 > 0) ? std::numeric_limits<result_t>::max() : std::numeric_limits<result_t>::min();
          }
        }
      }
      else
      {
        if (op1 > std::numeric_limits<result_t>::max() - op2)
        {
          if (columnstore::isStrictOverflowMode())
          {
            // Strict mode: throw exception for out of range
            std::ostringstream oss;
            oss << "BIGINT UNSIGNED value is out of range in addition";
            throw logging::IDBExcept(oss.str(), logging::ERR_FUNC_OUT_OF_RANGE_RESULT);
          }
          else
          {
            // Permissive mode: set null and return max value
            isNull = true;
            return std::numeric_limits<result_t>::max();
          }
        }
      }
      return op1 + op2;
    }

    case OP_SUB:
    {
      // Check for subtraction overflow
      if constexpr (std::is_signed_v<result_t> || std::is_same<__int128, result_t>::value)
      {
        if ((op2 < 0 && op1 > std::numeric_limits<result_t>::max() + op2) ||
            (op2 > 0 && op1 < std::numeric_limits<result_t>::min() + op2))
        {
          if (columnstore::isStrictOverflowMode())
          {
            // Strict mode: throw exception for out of range
            std::ostringstream oss;
            oss << "BIGINT value is out of range in subtraction";
            throw logging::IDBExcept(oss.str(), logging::ERR_FUNC_OUT_OF_RANGE_RESULT);
          }
          else
          {
            // Permissive mode: set null and return clamped value
            isNull = true;
            return (op2 < 0) ? std::numeric_limits<result_t>::max() : std::numeric_limits<result_t>::min();
          }
        }
      }
      else
      {
        if (op1 < op2)
        {
          if (columnstore::isStrictOverflowMode())
          {
            // Handle underflow - throw exception for out of range
            std::ostringstream oss;
            oss << "BIGINT UNSIGNED value is out of range in subtraction";
            throw logging::IDBExcept(oss.str(), logging::ERR_FUNC_OUT_OF_RANGE_RESULT);
	  }
	  else
	  {
            isNull = true;
	  }
        }
      }
      return op1 - op2;
    }

    case OP_MUL:
    {
      // Check for multiplication overflow
      if (op1 != 0 && op2 != 0)
      {
        if constexpr (std::is_signed_v<result_t> || std::is_same<__int128, result_t>::value)
        {
          // For signed types, check both positive and negative overflow
          if ((op1 > 0 && op2 > 0 && op1 > std::numeric_limits<result_t>::max() / op2) ||
              (op1 < 0 && op2 < 0 && op1 < std::numeric_limits<result_t>::max() / op2) ||
              (op1 > 0 && op2 < 0 && op2 < std::numeric_limits<result_t>::min() / op1) ||
              (op1 < 0 && op2 > 0 && op1 < std::numeric_limits<result_t>::min() / op2))
          {
            if (columnstore::isStrictOverflowMode())
	    {
              // Handle overflow - throw exception for out of range
              std::ostringstream oss;
              oss << "BIGINT value is out of range in multiplication";
              throw logging::IDBExcept(oss.str(), logging::ERR_FUNC_OUT_OF_RANGE_RESULT);
	    }
	    else
	    {
              isNull = true;
	    }
          }
        }
        else
        {
          // For unsigned types
          if (op1 > std::numeric_limits<result_t>::max() / op2)
          {
            if (columnstore::isStrictOverflowMode())
	    {
              // Handle overflow - throw exception for out of range
              std::ostringstream oss;
              oss << "BIGINT UNSIGNED value is out of range in multiplication";
              throw logging::IDBExcept(oss.str(), logging::ERR_FUNC_OUT_OF_RANGE_RESULT);
	    }
	    else
	    {
              isNull = true;
	    }
          }
        }
      }
      return op1 * op2;
    }

    case OP_DIV:
      if (op2)
        return op1 / op2;
      else
        isNull = true;

      return 0;

    default:
    {
      std::ostringstream oss;
      oss << "invalid arithmetic operation: " << fOp;
      throw logging::InvalidOperationExcept(oss.str());
    }
  }
}

inline void ArithmeticOperator::execute(IDB_Decimal& result, IDB_Decimal op1, IDB_Decimal op2, bool& isNull)
{
  switch (fOp)
  {
    case OP_ADD:
      if (fOperationType.colWidth == datatypes::MAXDECIMALWIDTH)
      {
        if (LIKELY(!fDecimalOverflowCheck))
        {
          datatypes::Decimal::addition<decltype(result.s128Value), false>(op1, op2, result);
        }
        else
        {
          datatypes::Decimal::addition<decltype(result.s128Value), true>(op1, op2, result);
        }
      }
      else if (fOperationType.colWidth == utils::MAXLEGACYWIDTH)
      {
        if (LIKELY(!fDecimalOverflowCheck))
        {
          datatypes::Decimal::addition<decltype(result.value), false>(op1, op2, result);
        }
        else
        {
          datatypes::Decimal::addition<decltype(result.value), true>(op1, op2, result);
        }
      }
      else
      {
        throw logging::InvalidArgumentExcept("Unexpected result width");
      }
      break;

    case OP_SUB:
      if (fOperationType.colWidth == datatypes::MAXDECIMALWIDTH)
      {
        if (LIKELY(!fDecimalOverflowCheck))
        {
          datatypes::Decimal::subtraction<decltype(result.s128Value), false>(op1, op2, result);
        }
        else
        {
          datatypes::Decimal::subtraction<decltype(result.s128Value), true>(op1, op2, result);
        }
      }
      else if (fOperationType.colWidth == utils::MAXLEGACYWIDTH)
      {
        if (LIKELY(!fDecimalOverflowCheck))
        {
          datatypes::Decimal::subtraction<decltype(result.value), false>(op1, op2, result);
        }
        else
        {
          datatypes::Decimal::subtraction<decltype(result.value), true>(op1, op2, result);
        }
      }
      else
      {
        throw logging::InvalidArgumentExcept("Unexpected result width");
      }
      break;

    case OP_MUL:
      if (fOperationType.colWidth == datatypes::MAXDECIMALWIDTH)
      {
        if (LIKELY(!fDecimalOverflowCheck))
        {
          datatypes::Decimal::multiplication<decltype(result.s128Value), false>(op1, op2, result);
        }
        else
        {
          datatypes::Decimal::multiplication<decltype(result.s128Value), true>(op1, op2, result);
        }
      }
      else if (fOperationType.colWidth == utils::MAXLEGACYWIDTH)
      {
        if (LIKELY(!fDecimalOverflowCheck))
        {
          datatypes::Decimal::multiplication<decltype(result.value), false>(op1, op2, result);
        }
        else
        {
          datatypes::Decimal::multiplication<decltype(result.value), true>(op1, op2, result);
        }
      }
      else
      {
        throw logging::InvalidArgumentExcept("Unexpected result width");
      }
      break;

    case OP_DIV:
      if (fOperationType.colWidth == datatypes::MAXDECIMALWIDTH)
      {
        if ((datatypes::Decimal::isWideDecimalTypeByPrecision(op2.precision) && op2.s128Value == 0) ||
            (!datatypes::Decimal::isWideDecimalTypeByPrecision(op2.precision) && op2.value == 0))
        {
          isNull = true;
          break;
        }

        if (LIKELY(!fDecimalOverflowCheck))
        {
          datatypes::Decimal::division<decltype(result.s128Value), false>(op1, op2, result);
        }
        else
        {
          datatypes::Decimal::division<decltype(result.s128Value), true>(op1, op2, result);
        }
      }
      else if (fOperationType.colWidth == utils::MAXLEGACYWIDTH)
      {
        if (op2.value == 0)
        {
          isNull = true;
          break;
        }

        if (LIKELY(!fDecimalOverflowCheck))
        {
          datatypes::Decimal::division<decltype(result.value), false>(op1, op2, result);
        }
        else
        {
          datatypes::Decimal::division<decltype(result.value), true>(op1, op2, result);
        }
      }
      else
      {
        throw logging::InvalidArgumentExcept("Unexpected result width");
      }
      break;

    default:
    {
      std::ostringstream oss;
      oss << "invalid arithmetic operation: " << fOp;
      throw logging::InvalidOperationExcept(oss.str());
    }
  }
}

std::ostream& operator<<(std::ostream& os, const ArithmeticOperator& rhs);
}  // namespace execplan

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

#ifndef ARITHMETICOPERATOR_H
#define ARITHMETICOPERATOR_H
#include <string>
#include <iosfwd>
#include <cmath>
#include <sstream>

#include "operator.h"
#include "parsetree.h"

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
  ArithmeticOperator(const std::string& operatorName);
  ArithmeticOperator(const ArithmeticOperator& rhs);

  virtual ~ArithmeticOperator();

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  inline virtual ArithmeticOperator* clone() const
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
  virtual void serialize(messageqcpp::ByteStream&) const;
  virtual void unserialize(messageqcpp::ByteStream&);

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
   */
  virtual bool operator==(const TreeNode* t) const;

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
  virtual bool operator!=(const TreeNode* t) const;

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
  inline virtual void evaluate(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop);

  using Operator::getStrVal;
  virtual const std::string& getStrVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getStrVal(fTimeZone);
  }
  using Operator::getIntVal;
  virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getIntVal();
  }
  using Operator::getUintVal;
  virtual uint64_t getUintVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getUintVal();
  }
  using Operator::getFloatVal;
  virtual float getFloatVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getFloatVal();
  }
  using Operator::getDoubleVal;
  virtual double getDoubleVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getDoubleVal();
  }
  using Operator::getLongDoubleVal;
  virtual long double getLongDoubleVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getLongDoubleVal();
  }
  using Operator::getDecimalVal;
  virtual IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
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
  virtual int32_t getDateIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getDateIntVal();
  }
  using Operator::getDatetimeIntVal;
  virtual int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getDatetimeIntVal();
  }
  using Operator::getTimestampIntVal;
  virtual int64_t getTimestampIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getTimestampIntVal();
  }
  using Operator::getTimeIntVal;
  virtual int64_t getTimeIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getTimeIntVal();
  }
  using Operator::getBoolVal;
  virtual bool getBoolVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
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
      break;

    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
      fResult.uintVal = execute(lop->getUintVal(row, isNull), rop->getUintVal(row, isNull), isNull);
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
  switch (fOp)
  {
    case OP_ADD: return op1 + op2;

    case OP_SUB: return op1 - op2;

    case OP_MUL: return op1 * op2;

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

#endif

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
#include <llvm-16/llvm/IR/BasicBlock.h>
#include <llvm-16/llvm/IR/Instructions.h>
#include <string>
#include <iosfwd>
#include <cmath>
#include <sstream>

#include "mcs_data_condition.h"
#include "mcs_datatype_basic.h"
#include "mcs_outofrange.h"
#include "mcs_int128.h"
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
  inline virtual ArithmeticOperator* clone() const override
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
  virtual void serialize(messageqcpp::ByteStream&) const override;
  virtual void unserialize(messageqcpp::ByteStream&) override;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this; false otherwise
   */
  virtual bool operator==(const TreeNode* t) const override;

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
  virtual bool operator!=(const TreeNode* t) const override;

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
  inline virtual void evaluate(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override;

  using Operator::getStrVal;
  virtual const utils::NullString& getStrVal(rowgroup::Row& row, bool& isNull, ParseTree* lop,
                                             ParseTree* rop) override
  {
    bool localIsNull = false;
    evaluate(row, localIsNull, lop, rop);
    isNull = isNull || localIsNull;
    return localIsNull ? fResult.strVal.dropString() : TreeNode::getStrVal(fTimeZone);
  }
  using Operator::getIntVal;
  virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getIntVal();
  }
  using Operator::getUintVal;
  virtual uint64_t getUintVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getUintVal();
  }
  using Operator::getFloatVal;
  virtual float getFloatVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getFloatVal();
  }
  using Operator::getDoubleVal;
  virtual double getDoubleVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getDoubleVal();
  }
  using Operator::getLongDoubleVal;
  virtual long double getLongDoubleVal(rowgroup::Row& row, bool& isNull, ParseTree* lop,
                                       ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getLongDoubleVal();
  }
  using Operator::getDecimalVal;
  virtual IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
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
  virtual int32_t getDateIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getDateIntVal();
  }
  using Operator::getDatetimeIntVal;
  virtual int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getDatetimeIntVal();
  }
  using Operator::getTimestampIntVal;
  virtual int64_t getTimestampIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop,
                                     ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getTimestampIntVal();
  }
  using Operator::getTimeIntVal;
  virtual int64_t getTimeIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
  {
    evaluate(row, isNull, lop, rop);
    return TreeNode::getTimeIntVal();
  }
  using Operator::getBoolVal;
  virtual bool getBoolVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop) override
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

  inline virtual std::string toCppCode(IncludeSet& includes) const override
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

 public:
  using Operator::compile;
  inline llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                              llvm::Value* dataConditionError, rowgroup::Row& row,
                              CalpontSystemCatalog::ColDataType dataType, ParseTree* lop,
                              ParseTree* rop) override;

  inline llvm::Value* compileInt_(llvm::IRBuilder<>& b, llvm::Value* l, llvm::Value* r);
  template <datatypes::SystemCatalog::ColDataType CT>
  inline llvm::Value* compileIntWithConditions_(llvm::IRBuilder<>& b, llvm::Value* l, llvm::Value* r,
                                                llvm::Value* isNull, llvm::Value* dataConditionError,
                                                ParseTree* lop, ParseTree* rop);
  inline llvm::Value* compileFloat_(llvm::IRBuilder<>& b, llvm::Value* l, llvm::Value* r);
  using Operator::isCompilable;
  inline bool isCompilable(rowgroup::Row& row, ParseTree* lop, ParseTree* rop) override;
};
// Can be easily replaced with a template over T if MDB changes the result return type.
inline uint64_t rangesCheck(const datatypes::TSInt128 x, const OpType op, const bool isNull)
{
  auto result = x.toUBIGINTWithDomainCheck();
  datatypes::throwOutOfRangeExceptionIfNeeded(isNull, !result);
  return result.value();  // if isNull returns some value
}

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
    {
      // XXX: this is bandaid solution for specific customer case (MCOL-5568).
      // Despite that I tried to implement a proper solution: to have operations
      // performed using int128_t amd then check the result.
      bool signedLeft = lop->data()->resultType().isSignedInteger();
      bool signedRight = rop->data()->resultType().isSignedInteger();
      const datatypes::TSInt128 x((signedLeft) ? static_cast<int128_t>(lop->getIntVal(row, isNull))
                                               : lop->getUintVal(row, isNull));
      const datatypes::TSInt128 y((signedRight) ? static_cast<int128_t>(rop->getIntVal(row, isNull))
                                                : rop->getUintVal(row, isNull));
      fResult.uintVal = rangesCheck(execute(x, y, isNull), fOp, isNull);  // throws
    }
    break;
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

template <typename T>
inline T ArithmeticOperator::execute(T op1, T op2, bool& isNull)
{
  switch (fOp)
  {
    case OP_ADD: return op1 + op2;

    case OP_SUB: return op1 - op2;

    case OP_MUL: return op1 * op2;

    case OP_DIV:
      if (op2)
      {
        return op1 / op2;
      }
      else
      {
        isNull = true;
      }

      if constexpr (std::is_same<T, datatypes::TSInt128>::value)
      {
        return datatypes::TSInt128();  // returns 0
      }
      else
      {
        return T{0};
      }

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

inline llvm::Value* ArithmeticOperator::compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                                                llvm::Value* dataConditionError, rowgroup::Row& row,
                                                CalpontSystemCatalog::ColDataType dataType, ParseTree* lop,
                                                ParseTree* rop)
{
  switch (fOperationType.colDataType)
  {
    case execplan::CalpontSystemCatalog::UBIGINT:
      return compileIntWithConditions_<execplan::CalpontSystemCatalog::UBIGINT>(
          b, lop->compile(b, data, isNull, dataConditionError, row, dataType),
          rop->compile(b, data, isNull, dataConditionError, row, dataType), isNull, dataConditionError, lop,
          rop);

    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    case execplan::CalpontSystemCatalog::UTINYINT:
      return compileInt_(b, lop->compile(b, data, isNull, dataConditionError, row, dataType),
                         rop->compile(b, data, isNull, dataConditionError, row, dataType));

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    case execplan::CalpontSystemCatalog::UFLOAT:
      return compileFloat_(b, lop->compile(b, data, isNull, dataConditionError, row, dataType),
                           rop->compile(b, data, isNull, dataConditionError, row, dataType));
    default:
    {
      std::ostringstream oss;
      oss << "invalid arithmetic operand type: " << fOperationType.colDataType;
      throw logging::InvalidArgumentExcept(oss.str());
    }
  }
}

template <datatypes::SystemCatalog::ColDataType CT>
inline llvm::Value* ArithmeticOperator::compileIntWithConditions_(llvm::IRBuilder<>& b, llvm::Value* l,
                                                                  llvm::Value* r, llvm::Value* isNull,
                                                                  llvm::Value* dataConditionError,
                                                                  ParseTree* lop, ParseTree* rop)
{
  constexpr size_t intTypeBitSize = datatypes::ColTypeToIntegral<CT>::bitWidth;
  static_assert(intTypeBitSize < sizeof(int128_t) * 8, "intTypeBitSize is too big");
  constexpr size_t intBiggerTypeBitSize = intTypeBitSize << 1;
  constexpr size_t errorTypeBitSize = sizeof(datatypes::DataCondition::X_NUMERIC_VALUE_OUT_OF_RANGE) * 8;
  auto* intBigerType = llvm::IntegerType::get(b.getContext(), intBiggerTypeBitSize);
  auto* intCurrentType = llvm::IntegerType::get(b.getContext(), intTypeBitSize);
  auto* intErrorType = llvm::IntegerType::get(b.getContext(), errorTypeBitSize);

  bool signedLeft = lop->data()->resultType().isSignedInteger();
  bool signedRight = rop->data()->resultType().isSignedInteger();

  auto* lCasted = (signedLeft) ? b.CreateSExt(l, intBigerType, "l_to_int128")
                               : b.CreateZExt(l, intBigerType, "l_to_int128");
  auto* rCasted = (signedRight) ? b.CreateSExt(r, intBigerType, "r_to_int128")
                                : b.CreateZExt(r, intBigerType, "r_to_int128");
  auto* operReturn = compileInt_(b, lCasted, rCasted);

  auto minValue = datatypes::ranges_limits<CT>::min();
  auto maxValue = datatypes::ranges_limits<CT>::max();
  auto* cmpGTMax = b.CreateICmpSGT(operReturn, llvm::ConstantInt::get(operReturn->getType(), maxValue));
  auto* cmpLTMin = b.CreateICmpSLT(operReturn, llvm::ConstantInt::get(operReturn->getType(), minValue));
  auto* cmpOutOfRange = b.CreateOr(cmpGTMax, cmpLTMin);

  auto* getDataConditionError = b.CreateLoad(intErrorType, dataConditionError);
  // Left shift might be faster here than multiplication but this would add up another one-time constant in
  // DataCondition.
  auto* getErrorCode = b.CreateMul(b.CreateZExt(cmpOutOfRange, intErrorType, "castBoolToInt"),
                                   b.getInt32(datatypes::DataCondition::X_NUMERIC_VALUE_OUT_OF_RANGE));
  auto* setOutOfRangeBitInDataCondition = b.CreateOr(getDataConditionError, getErrorCode);
  [[maybe_unused]] auto* storeIsOutOfRange =
      b.CreateStore(setOutOfRangeBitInDataCondition, dataConditionError);

  auto* res = b.CreateTrunc(operReturn, intCurrentType, "truncatedToSmaller");

  return res;
}

// template <datatypes::SystemCatalog::ColDataType CT>
// inline llvm::Value* ArithmeticOperator::compileIntWithConditions_(llvm::IRBuilder<>& b, llvm::Value* l,
//                                                                   llvm::Value* r, llvm::Value* isNull,
//                                                                   llvm::Value* dataConditionError,
//                                                                   ParseTree* lop, ParseTree* rop)
// {
//   auto* intBigerType = llvm::IntegerType::get(b.getContext(), 128);
//   auto* intCurrentType = llvm::IntegerType::get(b.getContext(), 64);
//   auto* intErrorType = llvm::IntegerType::get(b.getContext(), 32);

//   bool signedLeft = lop->data()->resultType().isSignedInteger();
//   bool signedRight = rop->data()->resultType().isSignedInteger();

//   auto* lCasted = (signedLeft) ? b.CreateSExt(l, intBigerType, "l_to_int128")
//                                : b.CreateZExt(l, intBigerType, "l_to_int128");
//   auto* rCasted = (signedRight) ? b.CreateSExt(r, intBigerType, "r_to_int128")
//                                 : b.CreateZExt(r, intBigerType, "r_to_int128");
//   auto* operReturn = compileInt_(b, lCasted, rCasted);

//   auto minValue = datatypes::ranges_limits<CT>::min();
//   auto maxValue = datatypes::ranges_limits<CT>::max();
//   auto* cmpGTMax = b.CreateICmpSGT(operReturn, llvm::ConstantInt::get(operReturn->getType(), maxValue));
//   auto* cmpLTMin = b.CreateICmpSLT(operReturn, llvm::ConstantInt::get(operReturn->getType(), minValue));
//   auto* cmpOutOfRange = b.CreateOr(cmpGTMax, cmpLTMin);

//   auto* getDataConditionError = b.CreateLoad(intErrorType, dataConditionError);
//   // Left shift might be faster here than multiplication but this would add up another one-time constant in
//   // DataCondition.
//   auto* getErrorCode = b.CreateMul(b.CreateZExt(cmpOutOfRange, intErrorType, "castBoolToInt"),
//                                    b.getInt32(datatypes::DataCondition::X_NUMERIC_VALUE_OUT_OF_RANGE));
//   auto* setOutOfRangeBitInDataCondition = b.CreateOr(getDataConditionError, getErrorCode);
//   [[maybe_unused]] auto* storeIsOutOfRange =
//       b.CreateStore(setOutOfRangeBitInDataCondition, dataConditionError);

//   auto* res = b.CreateTrunc(operReturn, intCurrentType, "truncatedToSmaller");

//   return res;
// }

inline llvm::Value* ArithmeticOperator::compileInt_(llvm::IRBuilder<>& b, llvm::Value* l, llvm::Value* r)
{
  switch (fOp)
  {
    case OP_ADD: return b.CreateAdd(l, r);

    case OP_SUB: return b.CreateSub(l, r);

    case OP_MUL: return b.CreateMul(l, r);

    case OP_DIV:
      if (l->getType()->isIntegerTy())
        throw std::logic_error("ArithmeticOperator::compile(): Div not support int");
      else
      {
        // WIP check if this handles zero div case.
        return b.CreateFDiv(l, r);
      }
    default:
    {
      std::ostringstream oss;
      oss << "invalid arithmetic operation: " << fOp;
      throw logging::InvalidOperationExcept(oss.str());
    }
  }
}

inline llvm::Value* ArithmeticOperator::compileFloat_(llvm::IRBuilder<>& b, llvm::Value* l, llvm::Value* r)
{
  switch (fOp)
  {
    case OP_ADD: return b.CreateFAdd(l, r);

    case OP_SUB: return b.CreateFSub(l, r);

    case OP_MUL: return b.CreateFMul(l, r);

    case OP_DIV:
      if (l->getType()->isIntegerTy())
        throw std::logic_error("ArithmeticOperator::compile(): Div not support int");
      else
        return b.CreateFDiv(l, r);
    default:
    {
      std::ostringstream oss;
      oss << "invalid arithmetic operation: " << fOp;
      throw logging::InvalidOperationExcept(oss.str());
    }
  }
}
inline bool ArithmeticOperator::isCompilable(rowgroup::Row& row, ParseTree* lop, ParseTree* rop)
{
  return lop->isCompilable(row) && rop->isCompilable(row);
}
std::ostream& operator<<(std::ostream& os, const ArithmeticOperator& rhs);
}  // namespace execplan

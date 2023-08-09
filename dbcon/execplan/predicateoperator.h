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
 *   $Id: predicateoperator.h 9667 2013-07-08 16:37:10Z bpaul $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef PREDICATEOPERATOR_H
#define PREDICATEOPERATOR_H

#include <string>
#include <sstream>
#if defined(_MSC_VER)
#include <malloc.h>
#elif defined(__FreeBSD__)
#include <cstdlib>
#else
#include <alloca.h>
#endif
#include <cstring>
#include <cmath>
#include <boost/regex.hpp>

#include "expressionparser.h"
#include "returnedcolumn.h"
#include "dataconvert.h"

#include "collation.h"  // CHARSET_INFO

namespace messageqcpp
{
class ByteStream;
}

namespace execplan
{
class PredicateOperator : public Operator
{
 public:
  PredicateOperator();
  PredicateOperator(const std::string& operatorName);
  PredicateOperator(const PredicateOperator& rhs);
  virtual ~PredicateOperator();

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  inline virtual PredicateOperator* clone() const
  {
    return new PredicateOperator(*this);
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
  bool operator==(const PredicateOperator& t) const;

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
  bool operator!=(const PredicateOperator& t) const;

  const CHARSET_INFO* getCharset() const
  {
    return cs;
  }
  /***********************************************************
   *                  F&E framework                          *
   ***********************************************************/
  using Operator::getBoolVal;
  virtual bool getBoolVal(rowgroup::Row& row, bool& isNull, ReturnedColumn* lop, ReturnedColumn* rop);
  void setOpType(Type& l, Type& r);

 private:
  inline bool numericCompare(const IDB_Decimal& op1, const IDB_Decimal& op2);
  template <typename result_t>
  inline bool numericCompare(const result_t op1, const result_t op2);
  inline bool strTrimCompare(const std::string& op1, const std::string& op2);

  const CHARSET_INFO* cs;

 public:
  inline llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                              rowgroup::Row& row, ParseTree* lop, ParseTree* rop,
                              CalpontSystemCatalog::ColDataType dataType) override;
  inline llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* l, llvm::Value* r);
};

inline bool PredicateOperator::numericCompare(const IDB_Decimal& op1, const IDB_Decimal& op2)
{
  switch (fOp)
  {
    case OP_EQ: return op1 == op2;

    case OP_NE: return op1 != op2;

    case OP_GT: return op1 > op2;

    case OP_GE: return op1 >= op2;

    case OP_LT: return op1 < op2;

    case OP_LE: return op1 <= op2;

    default:
    {
      std::ostringstream oss;
      oss << "invalid predicate operation: " << fOp;
      throw logging::InvalidOperationExcept(oss.str());
    }
  }
}

template <typename result_t>
inline bool PredicateOperator::numericCompare(const result_t op1, const result_t op2)
{
  switch (fOp)
  {
    case OP_EQ: return op1 == op2;

    case OP_NE: return op1 != op2;

    case OP_GT: return op1 > op2;

    case OP_GE: return op1 >= op2;

    case OP_LT: return op1 < op2;

    case OP_LE: return op1 <= op2;

    default:
    {
      std::ostringstream oss;
      oss << "invalid predicate operation: " << fOp;
      throw logging::InvalidOperationExcept(oss.str());
    }
  }
}

inline llvm::Value* PredicateOperator::compile(llvm::IRBuilder<>& b, llvm::Value* data, llvm::Value* isNull,
                                               rowgroup::Row& row, ParseTree* lop, ParseTree* rop,
                                               CalpontSystemCatalog::ColDataType dataType)
{
  return compile(b, lop->compile(b, data, isNull, row, dataType), rop->compile(b, data, isNull, row, dataType));
}
inline llvm::Value* PredicateOperator::compile(llvm::IRBuilder<>& b, llvm::Value* l, llvm::Value* r)
{
  switch (fOp)
  {
    case OP_EQ: return b.CreateICmpEQ(l, r);

    case OP_NE: return b.CreateICmpNE(l, r);

    case OP_GT: return b.CreateICmpSGT(l, r);

    case OP_GE: return b.CreateICmpSGE(l, r);

    case OP_LT: return b.CreateICmpSLT(l, r);

    case OP_LE: return b.CreateICmpSLE(l, r);

    default:
    {
      std::ostringstream oss;
      oss << "invalid predicate operation: " << fOp;
      throw logging::InvalidOperationExcept(oss.str());
    }
  }
}

std::ostream& operator<<(std::ostream& os, const PredicateOperator& rhs);
}  // namespace execplan

#endif  // PREDICATEOPERATOR_H

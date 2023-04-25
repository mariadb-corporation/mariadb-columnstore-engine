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
 *   $Id: operator.h 9635 2013-06-19 21:42:30Z bwilkinson $
 *
 *
 ***********************************************************************/
/** @file */

#pragma once
#include <string>
#include <iosfwd>
#include <boost/shared_ptr.hpp>

#include "treenode.h"

namespace messageqcpp
{
class ByteStream;
}

namespace execplan
{
class ParseTree;
class ReturnedColumn;

enum OpType
{
  OP_ADD = 0,
  OP_SUB,
  OP_MUL,
  OP_DIV,
  OP_EQ,
  OP_NE,
  OP_GT,
  OP_GE,
  OP_LT,
  OP_LE,
  OP_LIKE,
  OP_NOTLIKE,
  OP_AND,
  OP_OR,
  OP_ISNULL,
  OP_ISNOTNULL,
  OP_BETWEEN,
  OP_NOTBETWEEN,
  OP_IN,
  OP_NOTIN,
  OP_XOR,
  OP_UNKNOWN,
};

class Operator : public TreeNode
{
 public:
  Operator();
  Operator(const std::string& operatorName);
  Operator(const Operator& rhs);

  virtual ~Operator();

  virtual const std::string toString() const override;
  virtual const std::string data() const override
  {
    return fData;
  }
  virtual void data(const std::string data) override;

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  inline virtual Operator* clone() const override
  {
    return new Operator(*this);
  }

  /** return the opposite operator
   * @note this is not a compliment of operator. this is for the case that
   * the two operands of operator get switched, and the operator needs to
   * be reversed. e.g., where C1 > C2 ==> where C2 < C1
   */
  virtual Operator* opposite() const;

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
  bool operator==(const Operator& t) const;

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
  bool operator!=(const Operator& t) const;

  /** @breif reverse the operator
   *
   * Literally reverse the operator, as left operand and right operand are swapped, or a NOT
   * is applied before the operator
   */
  virtual void reverseOp();

  virtual std::string toCppCode(IncludeSet& includes) const override;

 protected:
  std::string fData;

  /***********************************************************
   *                  F&E framework                          *
   ***********************************************************/
 public:
  virtual OpType op() const
  {
    return fOp;
  }
  virtual void op(const OpType op)
  {
    fOp = op;
  }
  using TreeNode::evaluate;
  virtual void evaluate(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
  }

  // The following methods should be pure virtual. Currently too many instanslization exists.
  using TreeNode::getStrVal;
  virtual const utils::NullString& getStrVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    isNull = isNull || fResult.strVal.isNull();
    return fResult.strVal;
  }
  using TreeNode::getIntVal;
  virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    return fResult.intVal;
  }
  using TreeNode::getUintVal;
  virtual uint64_t getUintVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    return fResult.uintVal;
  }
  using TreeNode::getFloatVal;
  virtual float getFloatVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    return fResult.floatVal;
  }
  using TreeNode::getDoubleVal;
  virtual double getDoubleVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    return fResult.doubleVal;
  }
  using TreeNode::getLongDoubleVal;
  virtual long double getLongDoubleVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    return fResult.longDoubleVal;
  }
  using TreeNode::getDecimalVal;
  virtual IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    return fResult.decimalVal;
  }
  using TreeNode::getDateIntVal;
  virtual int32_t getDateIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    return fResult.intVal;
  }
  using TreeNode::getDatetimeIntVal;
  virtual int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    return fResult.intVal;
  }
  using TreeNode::getTimestampIntVal;
  virtual int64_t getTimestampIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    return fResult.intVal;
  }
  using TreeNode::getTimeIntVal;
  virtual int64_t getTimeIntVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    return fResult.intVal;
  }
  using TreeNode::getBoolVal;
  virtual bool getBoolVal(rowgroup::Row& row, bool& isNull, ParseTree* lop, ParseTree* rop)
  {
    return fResult.boolVal;
  }
  virtual bool getBoolVal(rowgroup::Row& row, bool& isNull, ReturnedColumn* lop, ReturnedColumn* rop)
  {
    return fResult.boolVal;
  }

  virtual void setOpType(Type& l, Type& r)
  {
  }
  virtual void operationType(const Type& ot) override
  {
    fOperationType = ot;
  }
  virtual const Type& operationType() const override
  {
    return fOperationType;
  }

 protected:
  OpType fOp;
};

typedef boost::shared_ptr<Operator> SOP;

std::ostream& operator<<(std::ostream& os, const Operator& rhs);
}  // namespace execplan

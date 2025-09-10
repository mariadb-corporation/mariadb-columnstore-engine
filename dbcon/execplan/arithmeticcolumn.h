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
 *   $Id: arithmeticcolumn.h 9679 2013-07-11 22:32:03Z zzhu $
 *
 *
 ***********************************************************************/
/** @file */

#pragma once
#include <string>
#include <iosfwd>
#include <stack>

#include "returnedcolumn.h"
#include "expressionparser.h"

/**
 * Namespace
 */
namespace execplan
{
class SimpleColumn;
class AggregateColumn;

/**
 * @brief A class to represent a arithmetic expression return column
 *
 * This class is a specialization of class ReturnedColumn that
 * handles an arithmetic expression.
 */
class ArithmeticColumn : public ReturnedColumn
{
 public:
  ArithmeticColumn();
  ArithmeticColumn(const std::string& sql, const uint32_t sessionID = 0);
  ArithmeticColumn(const ArithmeticColumn& rhs, const uint32_t sessionID = 0);
  ~ArithmeticColumn() override;

  inline ParseTree* expression() const
  {
    return fExpression;
  }

  /** get table alias name
   *
   * get the table alias name for this arithmetic function
   */
  inline const std::string& tableAlias() const
  {
    return fTableAlias;
  }

  /** set table alias name
   *
   * set the table alias name for this arithmetic function
   */
  inline void tableAlias(const std::string& tableAlias)
  {
    fTableAlias = tableAlias;
  }

  /** set the parse tree
   *
   * set the parse tree to expression.
   * @note this object takes ownership of the tree. The supplied pointer is set to null.
   */
  void expression(ParseTree*& expression);

  /**
   * get asc flag
   */
  inline bool asc() const override
  {
    return fAsc;
  }

  /**
   * set asc flag
   */
  inline void asc(const bool asc) override
  {
    fAsc = asc;
  }

  /**
   * get SQL representation of this object
   */
  const std::string data() const override
  {
    return fData;
  }

  /**
   * set SQL representation of this object
   */
  void data(const std::string data) override
  {
    fData = data;
  }

  /**
   * virtual stream method
   */
  const std::string toString() const override;

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  inline ArithmeticColumn* clone() const override
  {
    return new ArithmeticColumn(*this);
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
  bool operator==(const ArithmeticColumn& t) const;

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
  bool operator!=(const ArithmeticColumn& t) const;

  using ReturnedColumn::hasAggregate;
  bool hasAggregate() override;
  bool hasWindowFunc() override;

  void setDerivedTable() override;
  void replaceRealCol(std::vector<SRCP>&) override;
  void setSimpleColumnList() override;

  void setSimpleColumnListExtended() override;

  /**
   * Return the table that the column arguments belong to.
   *
   * @return tablename, if all arguments belong to one table
   *         empty string "", if multiple tables are involved in this func
   */
  std::optional<CalpontSystemCatalog::TableAliasName> singleTable() override;

  std::string toCppCode(IncludeSet& includes) const override;

 private:
  std::string fTableAlias;  // table alias for this column
  bool fAsc = false;        // asc flag for order by column
  std::string fData;

  /** build expression tree
   *
   * this function is called by the constructor. the incomming
   * sql string is parsed and tokenized and built into a
   * parse tree.
   */
  void buildTree();

  /** get next token from expression
   *
   * this is a util function used by buildTree(). next token string
   * is retrived from expression from curPos, until end char.
   * return the retrived token. curPos reference is updated to the
   * new position after end char
   */
  const std::string nextToken(std::string::size_type& curPos, char end) const;

  /***********************************************************
   *                 F&E framework                           *
   ***********************************************************/
 public:
  const utils::NullString& getStrVal(rowgroup::Row& row, bool& isNull) override
  {
    return fExpression->getStrVal(row, isNull);
  }

  int64_t getIntVal(rowgroup::Row& row, bool& isNull) override
  {
    return fExpression->getIntVal(row, isNull);
  }

  uint64_t getUintVal(rowgroup::Row& row, bool& isNull) override
  {
    return fExpression->getUintVal(row, isNull);
  }

  float getFloatVal(rowgroup::Row& row, bool& isNull) override
  {
    return fExpression->getFloatVal(row, isNull);
  }

  double getDoubleVal(rowgroup::Row& row, bool& isNull) override
  {
    return fExpression->getDoubleVal(row, isNull);
  }

  long double getLongDoubleVal(rowgroup::Row& row, bool& isNull) override
  {
    return fExpression->getLongDoubleVal(row, isNull);
  }

  IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull) override
  {
    return fExpression->getDecimalVal(row, isNull);
  }

  int32_t getDateIntVal(rowgroup::Row& row, bool& isNull) override
  {
    return fExpression->getDateIntVal(row, isNull);
  }

  int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull) override
  {
    return fExpression->getDatetimeIntVal(row, isNull);
  }

  int64_t getTimestampIntVal(rowgroup::Row& row, bool& isNull) override
  {
    return fExpression->getTimestampIntVal(row, isNull);
  }

  int64_t getTimeIntVal(rowgroup::Row& row, bool& isNull) override
  {
    return fExpression->getTimeIntVal(row, isNull);
  }

  bool getBoolVal(rowgroup::Row& row, bool& isNull) override
  {
    return fExpression->getBoolVal(row, isNull);
  }

 private:
  ParseTree* fExpression = nullptr;
  using TreeNode::evaluate;
  void evaluate(rowgroup::Row& /*row*/)
  {
  }
};

/**
 *  ostream operator
 */
std::ostream& operator<<(std::ostream& output, const ArithmeticColumn& rhs);

}  // namespace execplan

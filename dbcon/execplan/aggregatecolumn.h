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
 *   $Id: aggregatecolumn.h 9679 2013-07-11 22:32:03Z zzhu $
 *
 *
 ***********************************************************************/
/** @file */

#pragma once
#include <string>

#include "calpontselectexecutionplan.h"
#include "returnedcolumn.h"

namespace messageqcpp
{
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan
{
typedef std::vector<execplan::SRCP> AggParms;

/**
 * @brief A class to represent a aggregate return column
 *
 * This class is a specialization of class ReturnedColumn that
 * handles an aggregate function call (e.g., SUM, COUNT, MIN, MAX).
 */
class AggregateColumn : public ReturnedColumn
{
 public:
  /**
   * AggOp enum
   */
  enum AggOp
  {
    NOOP = 0,
    COUNT_ASTERISK,
    COUNT,
    SUM,
    AVG,
    MIN,
    MAX,
    CONSTANT,
    DISTINCT_COUNT,
    DISTINCT_SUM,
    DISTINCT_AVG,
    STDDEV_POP,
    STDDEV_SAMP,
    VAR_POP,
    VAR_SAMP,
    BIT_AND,
    BIT_OR,
    BIT_XOR,
    GROUP_CONCAT,
    JSON_ARRAYAGG,
    SELECT_SOME,
    UDAF,
    MULTI_PARM
  };
  /**
   * typedef
   */
  typedef std::vector<SRCP> ColumnList;
  /*
   * Constructors
   */
  /**
   * ctor
   */
  AggregateColumn();

  /**
   * ctor
   */
  explicit AggregateColumn(const uint32_t sessionID);

  /**
   * ctor
   */
  AggregateColumn(const std::string& functionName, const std::string& content, const uint32_t sessionID = 0);

  /**
   * ctor
   */
  AggregateColumn(const AggregateColumn& rhs, const uint32_t sessionID = 0);

  /**
   * Destructors
   */
  ~AggregateColumn() override = default;

  /**
   * Accessor Methods
   */
  virtual const std::string functionName() const
  {
    return fFunctionName;
  }

  /**
   * accessor
   */
  virtual void functionName(const std::string& functionName)
  {
    fFunctionName = functionName;
  }

  /**
   * accessor
   */
  virtual uint8_t aggOp() const
  {
    return fAggOp;
  }
  /**
   * accessor
   */
  virtual void aggOp(const uint8_t aggOp)
  {
    fAggOp = aggOp;
  }

  /** get function parms
   */
  virtual AggParms& aggParms()
  {
    return fAggParms;
  }

  virtual const AggParms& aggParms() const
  {
    return fAggParms;
  }

  /** set function parms
   */
  virtual void aggParms(const AggParms& parms)
  {
    fAggParms = parms;
  }

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  inline AggregateColumn* clone() const override
  {
    return new AggregateColumn(*this);
  }

  /**
   * table alias name
   */
  virtual const std::string tableAlias() const
  {
    return fTableAlias;
  }
  /**
   * table alias name
   */
  virtual void tableAlias(const std::string& tableAlias)
  {
    fTableAlias = tableAlias;
  }

  /**
   * ASC flag
   */
  inline bool asc() const override
  {
    return fAsc;
  }
  /**
   * ASC flag
   */
  inline void asc(const bool asc) override
  {
    fAsc = asc;
  }

  /**
   * fData: SQL representation of this object
   */
  const std::string data() const override
  {
    return fData;
  }
  /**
   * fData: SQL representation of this object
   */
  using ReturnedColumn::data;
  virtual void data(const std::string& data)
  {
    fData = data;
  }

  /**
   * Overloaded stream operator
   */
  const std::string toString() const override;
  const std::string toString(bool compact) const;
  std::string toCppCode(IncludeSet& includes) const override;

  /**
   * Serialize interface
   */
  void serialize(messageqcpp::ByteStream&) const override;
  /**
   * Serialize interface
   */
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
  using ReturnedColumn::operator=;
  virtual bool operator==(const AggregateColumn& t) const;

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
  using ReturnedColumn::operator!=;
  virtual bool operator!=(const AggregateColumn& t) const;

  /** @brief push back arg to group by column list*/
  virtual void addGroupByCol(SRCP ac)
  {
    fGroupByColList.push_back(ac);
  }

  /** @brief push back arg to project by column list*/
  virtual void addProjectCol(SRCP ac)
  {
    fProjectColList.push_back(ac);
  }

  /**
   * accessor
   */
  virtual const ColumnList& groupByColList() const
  {
    return fGroupByColList;
  }
  /**
   * accessor
   */
  virtual const ColumnList& projectColList() const
  {
    return fProjectColList;
  }

  /** @brief constant argument for aggregate with constant */
  inline const SRCP constCol() const
  {
    return fConstCol;
  }
  /**
   * accessor
   */
  inline void constCol(const SRCP& constCol)
  {
    fConstCol = constCol;
  }

  /**
   * convert an aggregate name to an AggOp enum
   */
  static AggOp agname2num(const std::string&);

  using ReturnedColumn::hasAggregate;
  bool hasAggregate() override;
  bool hasWindowFunc() override
  {
    return false;
  }

  inline long timeZone() const
  {
    return fTimeZone;
  }

  inline void timeZone(const long timeZone)
  {
    fTimeZone = timeZone;
  }

 protected:
  std::string fFunctionName;  // deprecated field
  uint8_t fAggOp;

  /**
   * ReturnedColumn objects that are the arguments to this
   * function
   */
  AggParms fAggParms;

  /** table alias
   * A string to represent table alias name which contains this column
   */
  std::string fTableAlias;

  /**
   * Flag to indicate asc or desc order for order by column
   */
  bool fAsc;
  std::string fData;
  ColumnList fGroupByColList;
  ColumnList fProjectColList;
  SRCP fConstCol;
  long fTimeZone;

 public:
  /***********************************************************
   *                 F&E framework                           *
   ***********************************************************/
  /**
   * F&E
   */
  const utils::NullString& getStrVal(rowgroup::Row& row, bool& isNull) override
  {
    bool localIsNull = false;
    evaluate(row, localIsNull);
    isNull = isNull || localIsNull;
    return localIsNull ? fResult.strVal.dropString() : TreeNode::getStrVal(fTimeZone);
  }

  /**
   * F&E
   */
  int64_t getIntVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getIntVal();
  }

  /**
   * F&E
   */
  uint64_t getUintVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getUintVal();
  }

  /**
   * F&E
   */
  float getFloatVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getFloatVal();
  }

  /**
   * F&E
   */
  double getDoubleVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getDoubleVal();
  }

  /**
   * F&E
   */
  long double getLongDoubleVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getLongDoubleVal();
  }

  /**
   * F&E
   */
  IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getDecimalVal();
  }
  /**
   * F&E
   */
  int32_t getDateIntVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getDateIntVal();
  }
  /**
   * F&E
   */
  int64_t getTimeIntVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getTimeIntVal();
  }
  /**
   * F&E
   */
  int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getDatetimeIntVal();
  }
  /**
   * F&E
   */
  int64_t getTimestampIntVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getTimestampIntVal();
  }

 private:
  void evaluate(rowgroup::Row& row, bool& isNull) override;
};

/**
 * stream operator
 */
std::ostream& operator<<(std::ostream& os, const AggregateColumn& rhs);
/**
 * utility function to extract all aggregate columns from a parse tree
 */
void getAggCols(ParseTree* n, void* obj);

}  // namespace execplan

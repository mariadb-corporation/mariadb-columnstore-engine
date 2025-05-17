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
 *   $Id: windowfunctioncolumn.h 9679 2013-07-11 22:32:03Z zzhu $
 *
 *
 ***********************************************************************/

/** @file */

#pragma once
#include <string>
#include <iosfwd>
#include <vector>

#include "returnedcolumn.h"
#include "functor.h"
#include "mcsv1_udaf.h"
#include "wf_frame.h"

namespace messageqcpp
{
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan
{
/**
 * @brief A class to represent a functional column
 *
 * This class is a specialization of class ReturnedColumn that
 * handles a window function.
 */
class WindowFunctionColumn : public ReturnedColumn
{
 public:
  WindowFunctionColumn();
  explicit WindowFunctionColumn(const std::string& functionName, const uint32_t sessionID = 0);
  WindowFunctionColumn(const std::string& functionName, const std::vector<SRCP>& functionParms,
                       const std::vector<SRCP>& partitions, WF_OrderBy& orderby,
                       const uint32_t sessionID = 0);
  WindowFunctionColumn(const WindowFunctionColumn& rhs, const uint32_t sessionID = 0);
  ~WindowFunctionColumn() override = default;

  /** get function name */
  inline const std::string& functionName() const
  {
    return fFunctionName;
  }

  /** set function name */
  inline void functionName(const std::string functionName)
  {
    fFunctionName = functionName;
  }

  /** get function parameters */
  inline const std::vector<SRCP>& functionParms() const
  {
    return fFunctionParms;
  }

  /** set function parameters*/
  inline void functionParms(const std::vector<SRCP>& functionParms)
  {
    fFunctionParms = functionParms;
  }

  /** get partition columns */
  inline const std::vector<SRCP>& partitions() const
  {
    return fPartitions;
  }

  /** set partition columns */
  inline void partitions(const std::vector<SRCP>& partitions)
  {
    fPartitions = partitions;
  }

  /** get order by clause */
  inline const WF_OrderBy& orderBy() const
  {
    return fOrderBy;
  }

  /** set order by clause */
  inline void orderBy(const WF_OrderBy& orderBy)
  {
    fOrderBy = orderBy;
  }

  /** make a clone of this window function */
  inline WindowFunctionColumn* clone() const override
  {
    return new WindowFunctionColumn(*this);
  }

  std::vector<SRCP> getColumnList() const;

  /** output the function for debug purpose */
  const std::string toString() const override;

  std::string toCppCode(IncludeSet& includes) const override;
  /**
   * The serialization interface
   */
  void serialize(messageqcpp::ByteStream&) const override;
  void unserialize(messageqcpp::ByteStream&) override;

  // util function for connector to use.
  void addToPartition(std::vector<SRCP>& groupByList);

  using ReturnedColumn::hasAggregate;
  bool hasAggregate() override
  {
    return false;
  }
  bool hasWindowFunc() override;
  void adjustResultType();

  // UDAnF support
  mcsv1sdk::mcsv1Context& getUDAFContext()
  {
    return udafContext;
  }
  const mcsv1sdk::mcsv1Context& getUDAFContext() const
  {
    return udafContext;
  }

  inline long timeZone() const
  {
    return fTimeZone;
  }

  inline void timeZone(const long timeZone)
  {
    fTimeZone = timeZone;
  }

 private:
  /**
   * Fields
   */
  std::string fFunctionName;         /// function name
  std::vector<SRCP> fFunctionParms;  /// function arguments
  std::vector<SRCP> fPartitions;     /// partition by clause
  WF_OrderBy fOrderBy;               /// order by clause

  // not support for window functions for now.
  bool operator==(const TreeNode* /*t*/) const override
  {
    return false;
  }
  bool operator==(const WindowFunctionColumn& t) const;
  bool operator!=(const TreeNode* /*t*/) const override
  {
    return false;
  }
  bool operator!=(const WindowFunctionColumn& t) const;

  // UDAnF support
  mcsv1sdk::mcsv1Context udafContext;

  long fTimeZone;
  /***********************************************************
   *                 F&E framework                           *
   ***********************************************************/
 public:
  using TreeNode::getStrVal;
  const utils::NullString& getStrVal(rowgroup::Row& row, bool& isNull) override
  {
    bool localIsNull = false;
    evaluate(row, localIsNull);
    isNull = isNull || localIsNull;
    return localIsNull ? fResult.strVal.dropString() : TreeNode::getStrVal(fTimeZone);
  }

  int64_t getIntVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getIntVal();
  }

  uint64_t getUintVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getUintVal();
  }

  float getFloatVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getFloatVal();
  }

  double getDoubleVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getDoubleVal();
  }

  long double getLongDoubleVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getLongDoubleVal();
  }

  IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getDecimalVal();
  }
  int32_t getDateIntVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getDateIntVal();
  }
  int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getDatetimeIntVal();
  }
  int64_t getTimestampIntVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getTimestampIntVal();
  }
  int64_t getTimeIntVal(rowgroup::Row& row, bool& isNull) override
  {
    evaluate(row, isNull);
    return TreeNode::getTimeIntVal();
  }

 private:
  void evaluate(rowgroup::Row& row, bool& isNull) override;
};

/**
 * ostream operator
 */
std::ostream& operator<<(std::ostream& output, const WindowFunctionColumn& rhs);

/**
 * utility function to extract all window function columns from a parse tree
 */
void getWindowFunctionCols(ParseTree* n, void* obj);

}  // namespace execplan

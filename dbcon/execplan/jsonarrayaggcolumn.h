/* Copyright (C) 2022 MariaDB Corporation

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

/** @file */

#pragma once
#include <string>

#include "calpontselectexecutionplan.h"
#include "aggregatecolumn.h"

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
 * @brief A class to represent a aggregate return column
 *
 * This class is a specialization of class ReturnedColumn that
 * handles an aggregate function call (e.g., SUM, COUNT, MIN, MAX).
 */
class JsonArrayAggColumn : public AggregateColumn
{
 public:
  /**
   * Constructors
   */
  JsonArrayAggColumn();

  explicit JsonArrayAggColumn(const uint32_t sessionID);

  JsonArrayAggColumn(const JsonArrayAggColumn& rhs, const uint32_t sessionID = 0);

  /**
   * Destructors
   */
  ~JsonArrayAggColumn() override;

  /**
   * Overloaded stream operator
   */
  const std::string toString() const override;

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  JsonArrayAggColumn* clone() const override
  {
    return new JsonArrayAggColumn(*this);
  }

  /**
   * Accessors and Mutators
   */
  void orderCols(const std::vector<SRCP>& orderCols)
  {
    fOrderCols = orderCols;
  }
  std::vector<SRCP>& orderCols()
  {
    return fOrderCols;
  }
  void separator(const std::string& separator)
  {
    fSeparator = separator;
  }
  std::string& separator()
  {
    return fSeparator;
  }

  /**
   * Serialize interface
   */
  void serialize(messageqcpp::ByteStream&) const override;
  void unserialize(messageqcpp::ByteStream&) override;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this;
   *         false otherwise
   */
  bool operator==(const TreeNode* t) const override;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this;
   *         false otherwise
   */
  using AggregateColumn::operator==;
  virtual bool operator==(const JsonArrayAggColumn& t) const;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return false iff every member of t is a duplicate copy of every member of this;
   *         true otherwise
   */
  bool operator!=(const TreeNode* t) const override;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return false iff every member of t is a duplicate copy of every member of this;
   *         true otherwise
   */
  using AggregateColumn::operator!=;
  virtual bool operator!=(const JsonArrayAggColumn& t) const;

  std::string toCppCode(IncludeSet& includes) const override;

 private:
  std::vector<SRCP> fOrderCols;
  std::string fSeparator;
};

/**
 * stream operator
 */
std::ostream& operator<<(std::ostream& os, const JsonArrayAggColumn& rhs);

}  // namespace execplan

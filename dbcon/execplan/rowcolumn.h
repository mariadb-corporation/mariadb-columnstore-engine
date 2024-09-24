/* Copyright (C) 2014 InfiniDB, Inc.

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
 *   $Id: rowcolumn.h 6309 2010-03-04 19:33:12Z zzhu $
 *
 *
 ***********************************************************************/
/** @file */

#pragma once
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>

#include "returnedcolumn.h"
#include "treenode.h"
#include "calpontsystemcatalog.h"
#include "dataconvert.h"

namespace messageqcpp
{
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan
{
class ParseTree;
/**
 * @brief A class to represent a simple returned column
 *
 * This class is a specialization of class ReturnedColumn that handles
 * a group of columns. Mostly used in subquery context. This class is
 * internal to the connector. No serialization interface is provided.
 * The joblist factory will not recognize this class.
 */
class RowColumn : public ReturnedColumn
{
 public:
  /**
   * Constructors
   */
  explicit RowColumn(const uint32_t sessionID = 0);
  RowColumn(const RowColumn& rhs, const uint32_t sessionID = 0);
  explicit RowColumn(const std::vector<SRCP>& columnVec, const uint32_t sessionID = 0);
  /**
   * Destructor
   */
  ~RowColumn() override;

  /**
   * Accessor Methods
   */
  const std::vector<SRCP>& columnVec() const
  {
    return fColumnVec;
  }
  void columnVec(const std::vector<SRCP>& columnVec)
  {
    fColumnVec = columnVec;
  }

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  inline RowColumn* clone() const override
  {
    return new RowColumn(*this);
  }
  /**
   * Overloaded assignment operator
   */
  RowColumn& operator=(const RowColumn& rhs);

  /**
   * The serialize interface
   */
  // virtual void serialize(messageqcpp::ByteStream&) const;
  // virtual void unserialize(messageqcpp::ByteStream&);

  const std::string toString() const override;

  /**
   * Serialization interface
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
  bool operator==(const RowColumn& t) const;

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
  bool operator!=(const RowColumn& t) const;
  using ReturnedColumn::hasAggregate;
  bool hasAggregate() override
  {
    return false;
  }
  bool hasWindowFunc() override
  {
    return false;
  }

  std::string toCppCode(IncludeSet& includes) const override;

 private:
  /**
   * Fields
   */
  std::vector<SRCP> fColumnVec;
};

/** dummy class. For the connector to use in gp_walk*/
class SubSelect : public ReturnedColumn
{
 public:
  SubSelect() : ReturnedColumn()
  {
  }
  ~SubSelect() override = default;
  SubSelect* clone() const override
  {
    return new SubSelect();
  }
  using ReturnedColumn::hasAggregate;
  bool hasAggregate() override
  {
    return false;
  }
  bool hasWindowFunc() override
  {
    return false;
  }
  const std::string toString() const override;
};

/**
 * ostream operator
 */
std::ostream& operator<<(std::ostream& output, const RowColumn& rhs);

}  // namespace execplan

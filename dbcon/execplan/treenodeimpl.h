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
 *   $Id: treenodeimpl.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */

#pragma once
#include <string>
#include <iosfwd>

#include "treenode.h"

namespace messageqcpp
{
class ByteStream;
}

/**
 * Namespace
 */
namespace execplan
{
/** @brief A class to represent a generic TreeNode
 *
 * This class is a concrete implementation of the abstract class TreeNode. It's only used to
 *  hold generic data long enough to parse into specific derrived classes.
 */
class TreeNodeImpl : public TreeNode
{
 public:
  /**
   * Constructors
   */
  TreeNodeImpl();
  explicit TreeNodeImpl(const std::string& sql);
  // not needed yet
  // TreeNodeImpl(const TreeNodeImpl& rhs);

  /**
   * Destructors
   */
  ~TreeNodeImpl() override;
  /**
   * Accessor Methods
   */

  /**
   * Operations
   */
  const std::string toString() const override;

  const std::string data() const override
  {
    return fData;
  }
  void data(const std::string data) override
  {
    fData = data;
  }

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  inline TreeNodeImpl* clone() const override
  {
    return new TreeNodeImpl(*this);
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
  bool operator==(const TreeNodeImpl& t) const;

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
  bool operator!=(const TreeNodeImpl& t) const;

  std::string toCppCode(IncludeSet& includes) const override;

 private:
  // default okay
  // TreeNodeImpl& operator=(const TreeNodeImpl& rhs);

  std::string fData;
};

std::ostream& operator<<(std::ostream& os, const TreeNodeImpl& rhs);

}  // namespace execplan

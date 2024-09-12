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
 *   $Id: outerjoinonfilter.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */

#pragma once
#include <string>

#include "filter.h"
#include "calpontselectexecutionplan.h"

/**
 * Namespace
 */
namespace execplan
{
/**
 * @brief A class to represent a outer join on clause
 *
 * This class is a specialization of class Filter that handles a
 * outer join on clause like t1 left join t2 on (t1.c1=t2.c1 and t1.c2 < t2.c2)
 */
class OuterJoinOnFilter : public Filter
{
  /**
   * Public stuff
   */
 public:
  /**
   * Constructors
   */
  OuterJoinOnFilter();
  explicit OuterJoinOnFilter(const SPTP& pt);
  OuterJoinOnFilter(const OuterJoinOnFilter& rhs);
  ~OuterJoinOnFilter() override;

  /**
   * Accessor Methods
   */
  const SPTP& pt() const
  {
    return fPt;
  }
  void pt(SPTP& pt)
  {
    fPt = pt;
  }

  /**
   * Overloaded stream operator
   */
  // virtual std::ostream& operator<< (std::ostream& output);
  const std::string toString() const override;

  /**
   * The serialization interface
   */
  void serialize(messageqcpp::ByteStream&) const override;
  void unserialize(messageqcpp::ByteStream&) override;

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  inline OuterJoinOnFilter* clone() const override
  {
    return new OuterJoinOnFilter(*this);
  }

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
  bool operator==(const OuterJoinOnFilter& t) const;

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
  bool operator!=(const OuterJoinOnFilter& t) const;
  std::string toCppCode(IncludeSet& includes) const override;

 private:
  // default okay?
  // OuterJoinOnFilter& operator=(const OuterJoinOnFilter& rhs);
  SPTP fPt;
  std::string fData;
};

std::ostream& operator<<(std::ostream& output, const OuterJoinOnFilter& rhs);

}  // namespace execplan

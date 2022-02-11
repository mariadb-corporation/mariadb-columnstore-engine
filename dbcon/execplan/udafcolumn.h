/*
   Copyright (c) 2017, MariaDB

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
   MA 02110-1301, USA.
*/

#ifndef UDAFCOLUMN_H
#define UDAFCOLUMN_H
#include <string>

#include "calpontselectexecutionplan.h"
#include "aggregatecolumn.h"
#include "mcsv1_udaf.h"

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
 * @brief A class to represent an aggregate return column
 *
 * This class is a specialization of class ReturnedColumn that
 * handles a user defined aggregate function (UDAF) call.
 */
class UDAFColumn : public AggregateColumn
{
 public:
  /**
   * Constructors
   */
  UDAFColumn();

  UDAFColumn(const uint32_t sessionID);

  UDAFColumn(const UDAFColumn& rhs, const uint32_t sessionID = 0);

  /**
   * Destructors
   */
  virtual ~UDAFColumn();

  /**
   * Overloaded stream operator
   */
  virtual const std::string toString() const;

  /** return a copy of this pointer
   *
   * deep copy of this pointer and return the copy
   */
  virtual UDAFColumn* clone() const
  {
    return new UDAFColumn(*this);
  }

  /**
   * Accessors and Mutators
   */
  mcsv1sdk::mcsv1Context& getContext()
  {
    return context;
  }

  /**
   * Serialize interface
   */
  virtual void serialize(messageqcpp::ByteStream&) const;
  virtual void unserialize(messageqcpp::ByteStream&);

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this;
   *         false otherwise
   */
  virtual bool operator==(const TreeNode* t) const;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return true iff every member of t is a duplicate copy of every member of this;
   *         false otherwise
   */
  using AggregateColumn::operator==;
  virtual bool operator==(const UDAFColumn& t) const;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return false iff every member of t is a duplicate copy of every member of this;
   *         true otherwise
   */
  virtual bool operator!=(const TreeNode* t) const;

  /** @brief Do a deep, strict (as opposed to semantic) equivalence test
   *
   * Do a deep, strict (as opposed to semantic) equivalence test.
   * @return false iff every member of t is a duplicate copy of every member of this;
   *         true otherwise
   */
  using AggregateColumn::operator!=;
  virtual bool operator!=(const UDAFColumn& t) const;

 private:
  mcsv1sdk::mcsv1Context context;
};

/**
 * stream operator
 */
std::ostream& operator<<(std::ostream& os, const UDAFColumn& rhs);

}  // namespace execplan
#endif  // UDAFCOLUMN_H

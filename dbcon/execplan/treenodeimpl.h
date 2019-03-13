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

#ifndef EXECPLAN_TREENODEIMPL_H
#define EXECPLAN_TREENODEIMPL_H
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
    TreeNodeImpl(const std::string& sql);
    // not needed yet
    //TreeNodeImpl(const TreeNodeImpl& rhs);

    /**
     * Destructors
     */
    virtual ~TreeNodeImpl();
    /**
     * Accessor Methods
     */

    /**
     * Operations
     */
    virtual const std::string toString() const;

    virtual const std::string data() const
    {
        return fData;
    }
    virtual void data(const std::string data)
    {
        fData = data;
    }

    /** return a copy of this pointer
     *
     * deep copy of this pointer and return the copy
     */
    inline virtual TreeNodeImpl* clone() const
    {
        return new TreeNodeImpl (*this);
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
    bool operator==(const TreeNodeImpl& t) const;

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
    bool operator!=(const TreeNodeImpl& t) const;

private:
    //default okay
    //TreeNodeImpl& operator=(const TreeNodeImpl& rhs);

    std::string fData;

};

std::ostream& operator<<(std::ostream& os, const TreeNodeImpl& rhs);

}
#endif //EXECPLAN_TREENODEIMPL_H


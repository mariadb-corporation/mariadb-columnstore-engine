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
 *   $Id: parsetree.h 9635 2013-06-19 21:42:30Z bwilkinson $
 *
 *
 ***********************************************************************/
/** @file */

#pragma once

#include <iostream>
#include <fstream>
#include "treenode.h"
#include "operator.h"
#include "mcs_decimal.h"
#include <boost/core/demangle.hpp>
#include <sstream>
#include <string>
#include <unordered_set>

namespace rowgroup
{
class Row;
}

namespace execplan
{

using ParseTreeWalker = void (*)(ParseTree* n);
using ParseTreeConstWalker = void (*)(const ParseTree* n);

using ParseTreePrinter = void (*)(const ParseTree* n, std::ostream& output);

using ParseTreeWalkerWithContext = void (*)(ParseTree* n, void* obj);
using ParseTreeConstWalkerWithContext = void (*)(const ParseTree* n, void* obj);
// class ParseTree;
/**
 * @brief A template class template to represent an expression tree
 *
 * This class is a simple binary tree implementation which
 * represents an expression tree. The expression can be used
 * for both arithmetic and logical expression.
 * Please note that to use this template class T can only be pointers.
 */

class ParseTree
{
 public:
  /**
   * Constructor / Destructor
   */
  inline ParseTree();
  inline explicit ParseTree(TreeNode* data);
  inline ParseTree(TreeNode* data, ParseTree* left, ParseTree* right);
  inline ParseTree(const ParseTree& rhs);
  inline virtual ~ParseTree();

  /**
   * Access methods
   */
  inline void left(ParseTree* expressionTree)
  {
    fLeft = expressionTree;
  }

  inline void left(TreeNode* node)
  {
    ParseTree* pt = new ParseTree(node);
    left(pt);
  }

  inline ParseTree* left() const
  {
    return fLeft;
  }

  inline ParseTree** leftRef()
  {
    return &fLeft;
  }

  inline void nullRight()
  {
    fRight = nullptr;
  }

  inline void nullLeft()
  {
    fLeft = nullptr;
  }

  inline void right(ParseTree* expressionTree)
  {
    fRight = expressionTree;
  }

  inline void right(TreeNode* node)
  {
    ParseTree* pt = new ParseTree(node);
    right(pt);
  }

  inline ParseTree* right() const
  {
    return fRight;
  }

  inline ParseTree** rightRef()
  {
    return &fRight;
  }

  inline void data(TreeNode* data)
  {
    fData = data;
  }

  inline TreeNode* data() const
  {
    return fData;
  }

  /** walk the tree
   *
   * postfix walking of a const tree
   */
  inline void walk(ParseTreeWalker fn) const;

  /** walk the tree
   *
   * postfix walking of a non-const tree. This is for deleting the tree
   */
  inline void walk(ParseTreeConstWalker fn) const;

  /** output the tree
   *
   * take ostream argument to walk and output the tree
   */
  inline void walk(ParseTreePrinter fn, std::ostream& output) const;

  /** output the tree
   *
   * take user argument to walk and output the tree
   */
  inline void walk(ParseTreeConstWalkerWithContext fn, void* object) const;

  /** output the tree
   *
   * take user argument to walk and output the tree
   */
  inline void walk(ParseTreeWalkerWithContext fn, void* object) const;

  /** output the tree to string
   * for debug purpose
   */
  inline std::string toString() const;

  inline std::string toCppCode(std::unordered_set<std::string>& includes) const;

  inline void codeToFile(std::string filename, std::string varname) const;
  /** assignment operator
   *
   */
  inline ParseTree& operator=(const ParseTree& rhs);

  /** deep copy of a tree
   *
   * copy src tree on top of dest tree. Dest tree is destroyed before copy
   * takes place. Src tree still owns the tree data, though. Assume tree node
   * are pointers.
   */
  inline void copyTree(const ParseTree& src);

  /** destroy a tree
   *
   * destroy a tree where tree nodes are values
   */
  inline static void destroyTree(ParseTree* root);

  /** draw the tree using dot
   *
   * this function is mostly for debug purpose, where T represent a TreeNode
   * pointer
   */
  inline void drawTree(std::string filename);

  /** print the tree
   *
   * this function is mostly for debug purpose, where T represent a TreeNode
   * pointer
   */
  inline static void print(const ParseTree* n, std::ostream& output)
  {
    output << *n->data() << std::endl;
  }

  /** delete treenode
   *
   * delete fData of a tree node, and then delete the treenode
   */
  inline static void deleter(ParseTree*& n)
  {
    delete n->fData;
    n->fData = nullptr;
    delete n;
    n = nullptr;
  }

  inline void derivedTable(const std::string& derivedTable)
  {
    fDerivedTable = derivedTable;
  }

  inline std::string& derivedTable()
  {
    return fDerivedTable;
  }

  inline void setDerivedTable();

  enum class GoTo
  {
    Left,
    Right,
    Up
  };

  struct StackFrame
  {
    ParseTree* node;
    GoTo direction;
    explicit StackFrame(ParseTree* node_, GoTo direction_ = GoTo::Left) : node(node_), direction(direction_)
    {
    }
  };

 private:
  TreeNode* fData;
  ParseTree* fLeft;
  ParseTree* fRight;
  std::string fDerivedTable;

  /**********************************************************************
   *                      F&E Framework                                 *
   **********************************************************************/

 public:
  inline const utils::NullString& getStrVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getStrVal(row, isNull, fLeft, fRight);
    else
      return fData->getStrVal(row, isNull);
  }

  inline int64_t getIntVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getIntVal(row, isNull, fLeft, fRight);
    else
      return fData->getIntVal(row, isNull);
  }

  inline uint64_t getUintVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getUintVal(row, isNull, fLeft, fRight);
    else
      return fData->getUintVal(row, isNull);
  }

  inline float getFloatVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getFloatVal(row, isNull, fLeft, fRight);
    else
      return fData->getFloatVal(row, isNull);
  }

  inline double getDoubleVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getDoubleVal(row, isNull, fLeft, fRight);
    else
      return fData->getDoubleVal(row, isNull);
  }

  inline long double getLongDoubleVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getLongDoubleVal(row, isNull, fLeft, fRight);
    else
      return fData->getLongDoubleVal(row, isNull);
  }

  inline IDB_Decimal getDecimalVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getDecimalVal(row, isNull, fLeft, fRight);
    else
      return fData->getDecimalVal(row, isNull);
  }

  inline bool getBoolVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getBoolVal(row, isNull, fLeft, fRight);
    else
      return fData->getBoolVal(row, isNull);
  }

  inline int32_t getDateIntVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getDateIntVal(row, isNull, fLeft, fRight);
    else
      return fData->getDateIntVal(row, isNull);
  }

  inline int64_t getDatetimeIntVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getDatetimeIntVal(row, isNull, fLeft, fRight);
    else
      return fData->getDatetimeIntVal(row, isNull);
  }

  inline int64_t getTimestampIntVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getTimestampIntVal(row, isNull, fLeft, fRight);
    else
      return fData->getTimestampIntVal(row, isNull);
  }

  inline int64_t getTimeIntVal(rowgroup::Row& row, bool& isNull)
  {
    if (fLeft && fRight)
      return (reinterpret_cast<Operator*>(fData))->getTimeIntVal(row, isNull, fLeft, fRight);
    else
      return fData->getTimeIntVal(row, isNull);
  }

 private:
  /** draw the tree
   *
   * this function is used by draw tree to print out dot file
   */
  static void draw(const ParseTree* n, std::ostream& dotFile);

  // F&E framework
  void evaluate(rowgroup::Row& row, bool& isNull);
};

}  // namespace execplan

#include "operator.h"

namespace execplan
{
/**
 * Class Definition
 */
inline ParseTree::ParseTree() : fData(nullptr), fLeft(nullptr), fRight(nullptr), fDerivedTable("")
{
}

inline ParseTree::ParseTree(TreeNode* data) : fData(data), fLeft(nullptr), fRight(nullptr)
{
  // bug5984. Need to validate data to be not null
  if (data)
    fDerivedTable = data->derivedTable();
}

inline ParseTree::ParseTree(TreeNode* data, ParseTree* left, ParseTree* right)
 : fData(data), fLeft(left), fRight(right)
{
  if (data)
    fDerivedTable = data->derivedTable();
}

inline ParseTree::ParseTree(const ParseTree& rhs)
 : fData(nullptr), fLeft(nullptr), fRight(nullptr), fDerivedTable(rhs.fDerivedTable)
{
  copyTree(rhs);
}

using DFSStack = std::vector<ParseTree::StackFrame>;

inline ParseTree::~ParseTree()
{
  if (fLeft == nullptr && fRight == nullptr)
  {
    delete fData;
    fData = nullptr;
  }
  else
  {
    DFSStack stack;
    stack.emplace_back(this);
    while (!stack.empty())
    {
      auto [node, dir] = stack.back();
      if (dir == GoTo::Left)
      {
        stack.back().direction = GoTo::Right;
        if (node->fLeft != nullptr)
        {
          stack.emplace_back(node->fLeft);
        }
      }
      else if (dir == GoTo::Right)
      {
        stack.back().direction = GoTo::Up;
        if (node->fRight != nullptr)
        {
          stack.emplace_back(node->fRight);
        }
      }
      else
      {
        if (stack.size() == 1)
        {
          node->fLeft = nullptr;
          node->fRight = nullptr;
          delete fData;
          fData = nullptr;
          stack.pop_back();
        }
        else
        {
          node->fLeft = nullptr;
          node->fRight = nullptr;
          delete node;
          stack.pop_back();
        }
      }
    }
  }
}

inline void ParseTree::walk(ParseTreeWalker fn) const
{
  DFSStack stack;
  stack.emplace_back(const_cast<ParseTree*>(this));

  while (!stack.empty())
  {
    auto [node, dir] = stack.back();
    if (dir == GoTo::Left)
    {
      stack.back().direction = GoTo::Right;
      if (node->fLeft != nullptr)
        stack.emplace_back(node->fLeft);
    }
    else if (dir == GoTo::Right)
    {
      stack.back().direction = GoTo::Up;
      if (node->fRight != nullptr)
        stack.emplace_back(node->fRight);
    }
    else
    {
      ParseTree* temp = const_cast<ParseTree*>(node);
      fn(temp);
      stack.pop_back();
    }
  }
}

inline void ParseTree::walk(ParseTreeConstWalker fn) const
{
  DFSStack stack;
  stack.emplace_back(const_cast<ParseTree*>(this));

  while (!stack.empty())
  {
    auto [node, dir] = stack.back();
    if (dir == GoTo::Left)
    {
      stack.back().direction = GoTo::Right;
      if (node->fLeft != nullptr)
        stack.emplace_back(node->fLeft);
    }
    else if (dir == GoTo::Right)
    {
      stack.back().direction = GoTo::Up;
      if (node->fRight != nullptr)
        stack.emplace_back(node->fRight);
    }
    else
    {
      ParseTree* temp = const_cast<ParseTree*>(node);
      fn(temp);
      stack.pop_back();
    }
  }
}

inline void ParseTree::walk(ParseTreePrinter fn, std::ostream& output) const
{
  DFSStack stack;
  stack.emplace_back(const_cast<ParseTree*>(this));

  while (!stack.empty())
  {
    auto [node, dir] = stack.back();
    if (dir == GoTo::Left)
    {
      stack.back().direction = GoTo::Right;
      if (node->fLeft != nullptr)
        stack.emplace_back(node->fLeft);
    }
    else if (dir == GoTo::Right)
    {
      stack.back().direction = GoTo::Up;
      if (node->fRight != nullptr)
        stack.emplace_back(node->fRight);
    }
    else
    {
      ParseTree* temp = const_cast<ParseTree*>(node);
      fn(temp, output);
      stack.pop_back();
    }
  }
}

inline void ParseTree::walk(ParseTreeConstWalkerWithContext fn, void* obj) const
{
  DFSStack stack;
  stack.emplace_back(const_cast<ParseTree*>(this));

  while (!stack.empty())
  {
    auto [node, dir] = stack.back();
    if (dir == GoTo::Left)
    {
      stack.back().direction = GoTo::Right;
      if (node->fLeft != nullptr)
        stack.emplace_back(node->fLeft);
    }
    else if (dir == GoTo::Right)
    {
      stack.back().direction = GoTo::Up;
      if (node->fRight != nullptr)
        stack.emplace_back(node->fRight);
    }
    else
    {
      ParseTree* temp = const_cast<ParseTree*>(node);
      fn(temp, obj);
      stack.pop_back();
    }
  }
}

inline std::string ParseTree::toString() const
{
  std::ostringstream oss;
  walk(ParseTree::print, oss);
  return oss.str();
}

inline void ParseTree::walk(ParseTreeWalkerWithContext fn, void* obj) const
{
  DFSStack stack;
  stack.emplace_back(const_cast<ParseTree*>(this));

  while (!stack.empty())
  {
    auto [node, dir] = stack.back();
    if (dir == GoTo::Left)
    {
      stack.back().direction = GoTo::Right;
      if (node->fLeft != nullptr)
        stack.emplace_back(node->fLeft);
    }
    else if (dir == GoTo::Right)
    {
      stack.back().direction = GoTo::Up;
      if (node->fRight != nullptr)
        stack.emplace_back(node->fRight);
    }
    else
    {
      ParseTree* temp = const_cast<ParseTree*>(node);
      fn(temp, obj);
      stack.pop_back();
    }
  }
}

inline ParseTree& ParseTree::operator=(const ParseTree& rhs)
{
  if (this != &rhs)
  {
    copyTree(rhs);
  }

  return *this;
}

inline void ParseTree::copyTree(const ParseTree& src)
{
  if (fLeft != nullptr)
    delete fLeft;

  if (fRight != nullptr)
    delete fRight;

  fLeft = nullptr;
  fRight = nullptr;

  if (src.left() != nullptr)
  {
    fLeft = new ParseTree();
    fLeft->copyTree(*(src.left()));
  }

  if (src.right() != nullptr)
  {
    fRight = new ParseTree();
    fRight->copyTree(*(src.right()));
  }

  delete fData;

  if (src.data() == nullptr)
    fData = nullptr;
  else
    fData = src.data()->clone();
}

inline void ParseTree::destroyTree(ParseTree* root)
{
  if (root == nullptr)
    return;

  if (root->left() != nullptr)
  {
    destroyTree(root->fLeft);
  }

  if (root->right() != nullptr)
  {
    destroyTree(root->fRight);
  }

  delete root;
  root = nullptr;
}

inline void ParseTree::draw(const ParseTree* n, std::ostream& dotFile)
{
  const ParseTree* r;
  const ParseTree* l;
  l = n->left();
  r = n->right();

  if (l != nullptr)
    dotFile << "n" << (void*)n << " -> " << "n" << (void*)l << std::endl;

  if (r != nullptr)
    dotFile << "n" << (void*)n << " -> " << "n" << (void*)r << std::endl;

  auto& node = *(n->data());
  dotFile << "n" << (void*)n << " [label=\"" << n->data()->data() << " (" << n << ") "
          << boost::core::demangle(typeid(node).name()) << "\"]" << std::endl;
}

inline void ParseTree::drawTree(std::string filename)
{
  std::ofstream dotFile(filename.c_str(), std::ios::out);

  dotFile << "digraph G {" << std::endl;
  walk(draw, dotFile);
  dotFile << "}" << std::endl;

  dotFile.close();
}

inline std::string ParseTree::toCppCode(IncludeSet& includes) const
{
  includes.insert("parsetree.h");
  std::stringstream ss;
  ss << "ParseTree(" << (data() ? ("new " + data()->toCppCode(includes)) : "nullptr") << ", "
     << (left() ? ("new " + left()->toCppCode(includes)) : "nullptr") << ", "
     << (right() ? ("new " + right()->toCppCode(includes)) : "nullptr") << ")";

  return ss.str();
}

inline void ParseTree::codeToFile(std::string filename, std::string varname) const
{
  std::ofstream hFile(filename.c_str(), std::ios::app);
  IncludeSet includes;
  auto result = toCppCode(includes);
  for (const auto& inc : includes)
    hFile << "#include \"" << inc << "\"\n";
  hFile << "\n";
  hFile << "namespace execplan \n{ auto " << varname << " = new " << result << ";\n}\n\n";

  hFile.close();
}

inline void ParseTree::evaluate(rowgroup::Row& row, bool& isNull)
{
  // Non-leaf node is operator. leaf node is SimpleFilter for logical expression,
  // or ReturnedColumn for arithmetical expression.
  if (fLeft && fRight)
  {
    Operator* op = reinterpret_cast<Operator*>(fData);
    op->evaluate(row, isNull, fLeft, fRight);
  }
  else
  {
    fData->evaluate(row, isNull);
  }
}

inline void ParseTree::setDerivedTable()
{
  std::string lDerivedTable = "";
  std::string rDerivedTable = "";

  if (fLeft)
  {
    fLeft->setDerivedTable();
    lDerivedTable = fLeft->derivedTable();
  }
  else
  {
    lDerivedTable = "*";
  }

  if (fRight)
  {
    fRight->setDerivedTable();
    rDerivedTable = fRight->derivedTable();
  }
  else
  {
    rDerivedTable = "*";
  }

  Operator* op = dynamic_cast<Operator*>(fData);

  if (op)
  {
    if (lDerivedTable == "*")
    {
      fDerivedTable = rDerivedTable;
    }
    else if (rDerivedTable == "*")
    {
      fDerivedTable = lDerivedTable;
    }
    else if (lDerivedTable == rDerivedTable)
    {
      fDerivedTable = lDerivedTable;  // should be the same as rhs
    }
    else
    {
      fDerivedTable = "";
    }
  }
  else
  {
    fData->setDerivedTable();
    fDerivedTable = fData->derivedTable();
    fDerivedTable = fData->derivedTable();
  }
}

typedef boost::shared_ptr<ParseTree> SPTP;

}  // namespace execplan

/*
 Copyright (C) 2022 MariaDB Corporation
 This program is free software; you can redistribute it and/or modify it under the terms of the GNU General
 Public License as published by the Free Software Foundation; version 2 of the License. This program is
 distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 You should have received a copy of the GNU General Public License along with this program; if not, write to
 the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/



#include "rewrites.h"
#include <typeinfo>
#include "objectreader.h"
#include "installdir.h"
#include "parsetree.h"
#include "operator.h"
#include "simplefilter.h"
#include "logicoperator.h"
#include <boost/core/demangle.hpp>
#include <set>
#include <string>
#include <ostream>

namespace execplan
{
namespace details
{
template <typename T, typename F>
void printContainer(std::ostream& os, const T& container, const std::string& delimiter, const F& printer,
                    const std::string& preambule = {})
{
  os << preambule << "\n";
  for (auto i = container.begin(); i != container.end();)
  {
    os << printer(*i);
    ++i;
    if (i != container.end())
      os << delimiter;
  }
  os << std::endl;
}


using CommonContainer =
    std::pair<std::set<execplan::ParseTree*, NodeSemanticComparator>, std::set<execplan::ParseTree*>>;

execplan::Filter* castToFilter(execplan::ParseTree* node)
{
  return dynamic_cast<execplan::Filter*>(node->data());
}

SimpleFilter* castToSimpleFilter(execplan::TreeNode* node)
{
  return dynamic_cast<SimpleFilter*>(node);
}

bool commonContainsSemantic(const CommonContainer& common, execplan::ParseTree* node)
{
  auto filter = castToFilter(node);
  return filter && common.first.contains(node);
}

bool commonContainsPtr(const CommonContainer& common, execplan::ParseTree* node)
{
  return common.second.contains(node);
}

OpType operatorType(execplan::ParseTree* node)
{
  auto op = dynamic_cast<Operator*>(node->data());
  if (!op)
    return OP_UNKNOWN;

  return op->op();
}

enum class ChildType
{
  Unchain,
  Delete,
  Leave
};

void printTreeLevel(execplan::ParseTree* root, int level)
{
#if debug_rewrites
  auto sep = std::string(level * 4, '-');
  auto& node = *(root->data());
  std::cerr << sep << ": " << root->data()->data() << " " << boost::core::demangle(typeid(node).name()) << " "
            << root << std::endl;
#endif
}

auto normalizeNode(std::string const& left, std::string const& right, execplan::OpType op)
{
  if (left < right)
    return std::make_tuple(op, std::ref(left), std::ref(right));

  execplan::OpType oppositeOp = oppositeOperator(op);

  return std::make_tuple(oppositeOp, std::ref(right), std::ref(left));
}

bool simpleFiltersCmp(const SimpleFilter* left, const SimpleFilter* right)
{
  return normalizeNode(left->lhs()->data(), left->rhs()->data(), left->op()->op()) <
         normalizeNode(right->lhs()->data(), right->rhs()->data(), right->op()->op());
}

// Walk the tree and find out common conjuctions
void collectCommonConjuctions(execplan::ParseTree* root, CommonContainer& accumulator, int level = 0,
                              bool orMeeted = false, bool andParent = false)
{
  if (root == nullptr)
  {
    return;
  }

  printTreeLevel(root, level);
  // the condition below means leaf node
  if (root->left() == nullptr && root->right() == nullptr && orMeeted && andParent)
  {
    // we want to collect it if it is a child of and node and or node was met before
    if (castToFilter(root))
    {
      accumulator.first.insert(root);
    }
    return;
  }
  // we do set intersection for all the lower levels for the or node
  if (operatorType(root) == OP_OR)
  {
    CommonContainer leftAcc;
    CommonContainer rightAcc;
    collectCommonConjuctions(root->left(), leftAcc, ++level, true, false);
    collectCommonConjuctions(root->right(), rightAcc, ++level, true, false);
    CommonContainer intersection;
    std::set_intersection(leftAcc.first.begin(), leftAcc.first.end(), rightAcc.first.begin(),
                          rightAcc.first.end(), std::inserter(intersection.first, intersection.first.begin()),
                          NodeSemanticComparator{});

    accumulator = intersection;
    return;
  }

  collectCommonConjuctions(root->left(), accumulator, ++level, orMeeted, operatorType(root) == OP_AND);
  collectCommonConjuctions(root->right(), accumulator, ++level, orMeeted, operatorType(root) == OP_AND);
  return;
}

// this utility function creates new and node
execplan::ParseTree* newAndNode()
{
  execplan::Operator* op = new execplan::Operator();
  op->data("and");
  return new execplan::ParseTree(op);
}

template <typename Common>
execplan::ParseTree* appendToRoot(execplan::ParseTree* tree, const Common& common)
{
  if (common.empty())
    return tree;

  // TODO: refactor to debug
  execplan::ParseTree* result = newAndNode();
  auto current = result;
  for (auto treenode = common.begin(); treenode != common.end();)
  {
    execplan::ParseTree* andCondition = *treenode;

    ++treenode;
    current->right(andCondition);

    if ((treenode != common.end() && std::next(treenode) != common.end()) ||
        (std::next(treenode) == common.end() && tree != nullptr))
    {
      execplan::ParseTree* andOp = newAndNode();
      current->left(andOp);
      current = andOp;
    }
    else if (std::next(treenode) == common.end() && tree == nullptr)
    {
      current->left(andCondition);
    }
  }
  if (tree)
    current->left(tree);

  return result;
}

enum class GoTo
{
  Left,
  Right,
  Up
};

struct StackFrame
{
  execplan::ParseTree** node;
  GoTo direction;
  ChildType containsLeft;
  ChildType containsRight;
  StackFrame(execplan::ParseTree** node_, GoTo direction_)
   : node(node_), direction(direction_), containsLeft(ChildType::Leave), containsRight(ChildType::Leave)
  {
  }
};

using DFSStack = std::vector<StackFrame>;

void deleteOneNode(execplan::ParseTree** node)
{
  if (!node || !*node)
    return;

  (*node)->nullLeft();
  (*node)->nullRight();

#if debug_rewrites
  std::cerr << "   Deleting: " << (*node)->data() << " " << boost::core::demangle(typeid(**node).name())
            << " "
            << "ptr: " << *node << std::endl;
#endif

  delete *node;
  *node = nullptr;
}

// this utility function adds one stack frame to a stack for dfs traversal
void addStackFrame(DFSStack& stack, GoTo direction, execplan::ParseTree* node)
{
  if (direction == GoTo::Left)
  {
    stack.back().direction = GoTo::Right;
    if (node->left() != nullptr)
    {
      auto left = node->leftRef();
      stack.emplace_back(left, GoTo::Left);
    }
  }
  else if (direction == GoTo::Right)
  {
    stack.back().direction = GoTo::Up;
    if (node->right() != nullptr)
    {
      auto right = node->rightRef();
      stack.emplace_back(right, GoTo::Left);
    }
  }
}

// this utility function reaplces the flag for in the stack frame,
// indicating whether to delete, unchain or leave child node. It depends on the direction
// specified in the stack frame
void replaceContainsTypeFlag(StackFrame& stackframe, ChildType containsflag)
{
  if (stackframe.direction == GoTo::Right)
    stackframe.containsLeft = containsflag;
  else
    stackframe.containsRight = containsflag;
}

// this utility function does the main transformation
void fixUpTree(execplan::ParseTree** node, ChildType ltype, ChildType rtype,
               StackFrame* parentframe = nullptr)
{
  if (ltype == ChildType::Leave)
  {
    if (rtype != ChildType::Leave) // if we don't leave the right node, we replace
    {                              // the parent node with the left child
      execplan::ParseTree* oldNode = *node;
      if (rtype == ChildType::Delete) // we delete the node that is a duplicate
        deleteOneNode((*node)->rightRef()); // of something in the common
      *node = (*node)->left();
      deleteOneNode(&oldNode);
    }
  }
  else
  {
    if (ltype == ChildType::Delete) // same as above
      deleteOneNode((*node)->leftRef());
    if (rtype == ChildType::Leave)  // replace the parent with the right child
    {
      execplan::ParseTree* oldNode = *node;
      *node = (*node)->right();
      deleteOneNode(&oldNode);
    }
    else
    {
      if (rtype == ChildType::Delete)
        deleteOneNode((*node)->rightRef());
      // if parent exists and botht children are deleted/unchained
      // we mark the node for deletion
      // otherwise it is the root and we just delete it
      if (parentframe)
        replaceContainsTypeFlag(*parentframe, ChildType::Delete);
      else
        deleteOneNode(node);
    }
  }
}

void removeFromTreeIterative(execplan::ParseTree** root, const CommonContainer& common)
{
  if (common.first.empty())
    return;

  DFSStack stack;
  stack.emplace_back(root, GoTo::Left);
  while (!stack.empty())
  {
    auto [node, flag, ltype, rtype] = stack.back();
    if (flag != GoTo::Up)
    {
      addStackFrame(stack, flag, *node);
      continue;
    }

    auto sz = stack.size();
    if (castToFilter(*node) && sz > 1)
    {
      if (commonContainsPtr(common, *node))
        replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Unchain);
      else if (!commonContainsPtr(common, *node) && commonContainsSemantic(common, *node))
        replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Delete);
      else
        replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Leave);

      stack.pop_back();
      continue;
    }

    if (sz == 1)
      fixUpTree(node, ltype, rtype);
    else
      fixUpTree(node, ltype, rtype, &stack[sz - 2]);

    stack.pop_back();
  }
}

}  // namespace details

void dumpTreeFiles(execplan::ParseTree* filters, const std::string& name, std::string dumpfolder = {})
{
#if debug_rewrites
  messageqcpp::ByteStream beforetree;
  ObjectReader::writeParseTree(filters, beforetree);

  if (dumpfolder.empty())
  {
    dumpfolder = startup::StartUp::tmpDir();
  }

  std::ofstream before(dumpfolder + "filters" + name + ".data");
  before << beforetree;
  std::string dotname = dumpfolder + "filters" + name + ".dot";
  filters->drawTree(dotname);
  filters->codeToFile(dumpfolder + "filters" + name + ".h");
  std::string dotInvoke = "dot -Tpng ";
  std::string convert = dotInvoke + dotname + " -o " + dotname + ".png";
  [[maybe_unused]] auto _ = std::system(convert.c_str());
#endif
}

template <bool stableSort>
execplan::ParseTree* extractCommonLeafConjunctionsToRoot(execplan::ParseTree* tree)
{
  dumpTreeFiles(tree, ".initial", stableSort ? "/tmp/" : "");

  details::CommonContainer common;
  details::collectCommonConjuctions(tree, common);
  std::copy(common.first.begin(), common.first.end(), std::inserter(common.second, common.second.begin()));

#if debug_rewrites
  details::printContainer(
      std::cerr, common.first, "\n", [](auto treenode) { return treenode->data()->data(); },
      "Common Leaf Conjunctions:");
#endif

  details::removeFromTreeIterative(&tree, common);

  execplan::ParseTree* result = nullptr;
  if constexpr (stableSort)
  {
    std::vector<execplan::ParseTree*> commonSorted;
    std::copy(common.first.begin(), common.first.end(), std::back_inserter(commonSorted));
    std::sort(commonSorted.begin(), commonSorted.end(),
              [](auto left, auto right) { return left->data()->data() < right->data()->data(); });
    result = details::appendToRoot(tree, commonSorted);
  }
  else
  {
    result = details::appendToRoot(tree, common.first);
  }

  dumpTreeFiles(result, ".final", stableSort ? "/tmp/" : "");
  return result;
}

execplan::OpType oppositeOperator(execplan::OpType op)
{
  if (op == OP_GT)
    return OP_LT;

  if (op == OP_GE)
    return OP_LE;

  if (op == OP_LT)
    return OP_GT;

  if (op == OP_LE)
    return OP_GE;

  return op;
}


template execplan::ParseTree* extractCommonLeafConjunctionsToRoot<false>(execplan::ParseTree* tree);
template execplan::ParseTree* extractCommonLeafConjunctionsToRoot<true>(execplan::ParseTree* tree);

bool NodeSemanticComparator::operator()(execplan::ParseTree* left, execplan::ParseTree* right) const
{
  auto filterLeft = details::castToSimpleFilter(left->data());
  auto filterRight = details::castToSimpleFilter(right->data());

  if (filterLeft && filterRight)
    return details::simpleFiltersCmp(filterLeft, filterRight);

  return left->data()->data() < right->data()->data();
}

}  // namespace execplan

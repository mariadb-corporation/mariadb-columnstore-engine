/* Copyright (C) 2026 MariaDB Corporation

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

#include "parse_tree_ops.h"

#include "execplan/logicoperator.h"
#include "execplan/operator.h"

namespace optimizer
{
namespace lib
{

execplan::ParseTree* newLogicNode(const std::string& opName)
{
  return new execplan::ParseTree(new execplan::LogicOperator(opName));
}

execplan::ParseTree* newAndNode()
{
  return newLogicNode("and");
}

execplan::ParseTree* newOrNode()
{
  return newLogicNode("or");
}

namespace
{

// Right-deep fold of `leaves` under a binary op produced by `mkNode()`.
// Consumes ownership of every `ParseTree*` in `leaves`.
//
// Result shape:
//   []            -> nullptr
//   [a]           -> a
//   [a, b]        -> OP(a, b)
//   [a, b, c]     -> OP(a, OP(b, c))
execplan::ParseTree* foldRight(const std::vector<execplan::ParseTree*>& leaves,
                               execplan::ParseTree* (*mkNode)())
{
  if (leaves.empty())
    return nullptr;
  execplan::ParseTree* result = leaves.back();
  for (ssize_t i = static_cast<ssize_t>(leaves.size()) - 2; i >= 0; --i)
  {
    execplan::ParseTree* op = mkNode();
    op->left(leaves[i]);
    op->right(result);
    result = op;
  }
  return result;
}

}  // namespace

execplan::ParseTree* andAll(const std::vector<execplan::ParseTree*>& leaves)
{
  return foldRight(leaves, newAndNode);
}

execplan::ParseTree* orAll(const std::vector<execplan::ParseTree*>& leaves)
{
  return foldRight(leaves, newOrNode);
}

execplan::ParseTree* andWith(execplan::ParseTree* lhs, execplan::ParseTree* rhs)
{
  if (lhs == nullptr)
    return rhs;
  if (rhs == nullptr)
    return lhs;
  execplan::ParseTree* op = newAndNode();
  op->left(lhs);
  op->right(rhs);
  return op;
}

execplan::ParseTree* orWith(execplan::ParseTree* lhs, execplan::ParseTree* rhs)
{
  if (lhs == nullptr)
    return rhs;
  if (rhs == nullptr)
    return lhs;
  execplan::ParseTree* op = newOrNode();
  op->left(lhs);
  op->right(rhs);
  return op;
}

void deleteOneNode(execplan::ParseTree** node)
{
  if (node == nullptr || *node == nullptr)
    return;
  (*node)->nullLeft();
  (*node)->nullRight();
  delete *node;
  *node = nullptr;
}

void replaceInPlace(execplan::ParseTree* target, execplan::ParseTree* src)
{
  if (target == nullptr || src == nullptr)
    return;

  // Destroy whatever `target` currently holds.  Children and data are owned
  // by target and must be released before we overwrite the pointers; we use
  // the ParseTree dtor (via destroyTree for children) so nested subtrees are
  // freed recursively.
  if (target->left() != nullptr)
  {
    execplan::ParseTree::destroyTree(target->left());
    target->nullLeft();
  }
  if (target->right() != nullptr)
  {
    execplan::ParseTree::destroyTree(target->right());
    target->nullRight();
  }
  delete target->data();

  // Move src's contents into target.
  target->data(src->data());
  target->left(src->left());
  target->right(src->right());

  // Detach src so its destructor does not free the moved-out data / subtree.
  src->data(nullptr);
  src->nullLeft();
  src->nullRight();
  delete src;
}

bool collectConjuncts(execplan::ParseTree* root, std::vector<execplan::ParseTree*>& out)
{
  if (root == nullptr)
    return true;
  if (isAnd(root))
  {
    return collectConjuncts(root->left(), out) && collectConjuncts(root->right(), out);
  }
  // A non-AND internal node is only rejected if it is a LogicOperator.
  // Arbitrary leaves (SimpleFilter, SelectFilter, ...) are accepted and
  // treated as conjuncts.
  if (auto* lop = dynamic_cast<execplan::LogicOperator*>(root->data()))
  {
    (void)lop;
    return false;
  }
  out.push_back(root);
  return true;
}

execplan::OpType logicOpType(const execplan::ParseTree* node)
{
  if (node == nullptr)
    return execplan::OP_UNKNOWN;
  auto* op = dynamic_cast<execplan::LogicOperator*>(node->data());
  if (!op)
    return execplan::OP_UNKNOWN;
  return op->op();
}

bool isAnd(const execplan::ParseTree* node)
{
  return logicOpType(node) == execplan::OP_AND;
}

bool isOr(const execplan::ParseTree* node)
{
  return logicOpType(node) == execplan::OP_OR;
}

}  // namespace lib
}  // namespace optimizer

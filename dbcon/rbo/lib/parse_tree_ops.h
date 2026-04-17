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

// RBO primitive library: low-level ParseTree construction / destruction /
// traversal helpers shared across all RBO rules.  Rules MUST use these
// primitives rather than calling `new ParseTree(...)` / `delete` directly.
//
// Invariants:
//   * Every helper that returns `ParseTree*` transfers ownership to the caller.
//   * Every helper that takes `ParseTree*` by pointer does NOT take ownership
//     (unless explicitly documented otherwise).
//   * `andAll` / `orAll` / `andWith` / `orWith` consume (take ownership of)
//     their `ParseTree*` arguments because the resulting tree reuses them as
//     children.

#pragma once

#include <vector>

#include "execplan/parsetree.h"

namespace execplan
{
class LogicOperator;
class Filter;
class SimpleFilter;
}  // namespace execplan

namespace optimizer
{
namespace lib
{

// ---------------------------------------------------------------------------
// Logic-node factories.  Always return a freshly-allocated ParseTree owning a
// freshly-allocated LogicOperator.
// ---------------------------------------------------------------------------

// Creates `ParseTree(LogicOperator(opName))` with null left/right children.
execplan::ParseTree* newLogicNode(const std::string& opName);

// Shorthand for newLogicNode("and") / newLogicNode("or").
execplan::ParseTree* newAndNode();
execplan::ParseTree* newOrNode();

// ---------------------------------------------------------------------------
// Right-deep AND/OR construction.  Consumes ownership of every `ParseTree*`
// passed in `leaves`.
//
//   andAll({})          -> nullptr
//   andAll({a})         -> a                          (no wrapping node created)
//   andAll({a, b})      -> AND(a, b)
//   andAll({a, b, c})   -> AND(a, AND(b, c))
//
// Same semantics for `orAll` but with OR.
// ---------------------------------------------------------------------------

execplan::ParseTree* andAll(const std::vector<execplan::ParseTree*>& leaves);
execplan::ParseTree* orAll(const std::vector<execplan::ParseTree*>& leaves);

// Combine `lhs` and `rhs` under `op`.  nullptr arguments are allowed:
//   andWith(nullptr, x)  -> x
//   andWith(x, nullptr)  -> x
//   andWith(nullptr, nullptr) -> nullptr
//   andWith(x, y)        -> AND(x, y)
// Consumes ownership of lhs and rhs.
execplan::ParseTree* andWith(execplan::ParseTree* lhs, execplan::ParseTree* rhs);
execplan::ParseTree* orWith(execplan::ParseTree* lhs, execplan::ParseTree* rhs);

// ---------------------------------------------------------------------------
// Deletion helpers.
// ---------------------------------------------------------------------------

// Deletes *node (safe on nullptr and safe re-entry) and sets *node = nullptr.
// Before deleting, null out left/right pointers so the subtree dtor does not
// recurse into siblings we still reference elsewhere.  Matches the historical
// `deleteOneNode` behaviour from common_leaf_conjunctions.cpp.
void deleteOneNode(execplan::ParseTree** node);

// ---------------------------------------------------------------------------
// In-place replacement.
//
// Swaps the payload of `target` (data/left/right) with that of `src`, then
// destroys `src` (without destroying the subtree that was just moved out of
// it).  This is used by rules that rewrite a leaf inside a larger tree they
// don't own and must preserve the pointer identity of the leaf.
//
// After the call, `src` becomes invalid.  Caller must NOT delete `src`.
// ---------------------------------------------------------------------------

void replaceInPlace(execplan::ParseTree* target, execplan::ParseTree* src);

// ---------------------------------------------------------------------------
// Conjunction splitting.
//
// collectConjuncts(root, out): if `root` is a pure right-/left-deep AND tree
// (every internal node is `LogicOperator("and")`), appends each leaf to
// `out` in left-to-right order and returns true.  Returns false if any
// internal node is a non-AND operator (OR/XOR/...).  `root == nullptr` is
// treated as an empty conjunction (returns true, out unchanged).
//
// The `out` vector is NOT cleared before use.
// ---------------------------------------------------------------------------

bool collectConjuncts(execplan::ParseTree* root, std::vector<execplan::ParseTree*>& out);

// ---------------------------------------------------------------------------
// Operator-type utilities.
// ---------------------------------------------------------------------------

// Returns the OpType of `node`'s LogicOperator, or OP_UNKNOWN if node or its
// data is not a LogicOperator.
execplan::OpType logicOpType(const execplan::ParseTree* node);

// Returns true iff `node->data()` is LogicOperator("and").
bool isAnd(const execplan::ParseTree* node);
// Returns true iff `node->data()` is LogicOperator("or").
bool isOr(const execplan::ParseTree* node);

}  // namespace lib
}  // namespace optimizer

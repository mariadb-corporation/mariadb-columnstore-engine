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

// RBO primitive library: walkers for CSEP sub-plan trees and ParseTree
// sub-trees rooted at OuterJoinOnFilter nodes.

#pragma once

#include <functional>
#include <vector>

#include "execplan/calpontselectexecutionplan.h"
#include "execplan/parsetree.h"

namespace execplan
{
class TreeNode;
}  // namespace execplan

namespace optimizer
{
namespace lib
{

// ---------------------------------------------------------------------------
// Collect leaves of `root` that satisfy `predicate(TreeNode*)` and live
// inside an OuterJoinOnFilter.  Non-OJF branches of `root` are descended
// into to find other OJF subtrees; the predicate is ONLY evaluated inside an
// OJF's `pt()` parse tree.
//
// The function appends matching ParseTree* leaves to `out` in pre-order.
// The `out` vector is NOT cleared.
//
// Used by rbo_decorrelate_outer_join_sub to find SelectFilter /
// SimpleScalarFilter leaves inside ON-clause sub-trees.
// ---------------------------------------------------------------------------
void collectLeavesInOuterJoinOn(execplan::ParseTree* root,
                                std::vector<execplan::ParseTree*>& out,
                                std::function<bool(execplan::TreeNode*)> predicate);

// ---------------------------------------------------------------------------
// Recursively visits every CSEP reachable from `csep` via subSelectList(),
// derivedTableList() and unionVec() (but NOT `csep` itself).  `visitor` is
// called in pre-order; returning true from the visitor stops the walk.
//
// Returns true iff the visitor ever stopped the walk (i.e. returned true).
//
// Header-only template: `F` is expected to have signature
//     bool operator()(const execplan::CalpontSelectExecutionPlan&).
// Non-CSEP entries (entries whose shared_ptr doesn't cast to CSEP) are
// silently skipped.
// ---------------------------------------------------------------------------
template <typename F>
inline bool walkNestedCSEPs(const execplan::CalpontSelectExecutionPlan& csep, F&& visitor)
{
  auto descend = [&visitor](const auto& list) -> bool
  {
    for (const auto& entry : list)
    {
      auto* sub = dynamic_cast<const execplan::CalpontSelectExecutionPlan*>(entry.get());
      if (!sub)
        continue;
      if (visitor(*sub))
        return true;
      if (walkNestedCSEPs(*sub, visitor))
        return true;
    }
    return false;
  };

  if (descend(csep.subSelectList()))
    return true;
  if (descend(csep.derivedTableList()))
    return true;
  if (descend(csep.unionVec()))
    return true;
  return false;
}

}  // namespace lib
}  // namespace optimizer

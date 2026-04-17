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

#include "rbo_decorrelate_outer_join_sub.h"

#include "execplan/outerjoinonfilter.h"
#include "execplan/parsetree.h"
#include "execplan/selectfilter.h"
#include "execplan/simplescalarfilter.h"

namespace optimizer
{
namespace
{

// Returns true if any node of the ParseTree rooted at `root` carries a
// scalar-subquery filter (SelectFilter or SimpleScalarFilter).  Even the
// non-correlated SimpleScalarFilter is flagged because the executor's
// Expression-step builder does not know how to evaluate it when wrapped
// inside an OuterJoinOnFilter.
bool treeHasScalarSubFilter(const execplan::ParseTree* root)
{
  if (!root)
    return false;

  const execplan::TreeNode* data = root->data();
  if (dynamic_cast<const execplan::SelectFilter*>(data) != nullptr ||
      dynamic_cast<const execplan::SimpleScalarFilter*>(data) != nullptr)
  {
    return true;
  }

  return treeHasScalarSubFilter(root->left()) || treeHasScalarSubFilter(root->right());
}

// Returns true if any OuterJoinOnFilter under `root` contains (anywhere in
// its own ParseTree) a scalar-subquery filter.  This is the pattern that
// the executor does not support today and for which IDB-1015 is emitted.
bool treeHasUnsupportedOuterJoinSub(const execplan::ParseTree* root)
{
  if (!root)
    return false;

  if (auto* ojf = dynamic_cast<const execplan::OuterJoinOnFilter*>(root->data()))
  {
    if (treeHasScalarSubFilter(ojf->pt().get()))
      return true;
  }

  return treeHasUnsupportedOuterJoinSub(root->left()) ||
         treeHasUnsupportedOuterJoinSub(root->right());
}

}  // namespace

bool decorrelateOuterJoinSubFilter(execplan::CalpontSelectExecutionPlan& csep,
                                   optimizer::RBOptimizerContext& /*ctx*/)
{
  // Only walk the plan if there is a chance of matching the pattern.  The
  // shared Rule::walk() mechanism will additionally recurse into subSelectList
  // and unionVec for us, so we only inspect `csep.filters()` directly here.
  return treeHasUnsupportedOuterJoinSub(csep.filters());
}

bool applyDecorrelateOuterJoinSub(execplan::CalpontSelectExecutionPlan& csep,
                                  optimizer::RBOptimizerContext& ctx)
{
  // TODO(MCOL-4250): implement the actual decorrelation rewrite.
  //
  // For each OuterJoinOnFilter in csep.filters(), locate any child
  // SelectFilter / SimpleScalarFilter matching the
  //     <outer_lhs> <op> (SELECT agg(expr) FROM T WHERE T.k_i = X_i ...)
  // pattern and rewrite it to:
  //   * a new derived-table CSEP grouping T by the correlation keys and
  //     projecting the aggregate,
  //   * a SimpleFilter comparing the original LHS to the new derived column,
  //   * equi-join predicates between the new derived table and the right-side
  //     table of the LEFT JOIN.
  //
  // Until this is in place the rule is a no-op: the post-RBO validator in
  // cs_get_select_plan() keeps emitting IDB-1015 for exactly the same queries
  // that errored out before, so visible behaviour is unchanged.
  (void)csep;
  (void)ctx;
  return false;
}

bool outerJoinOnContainsScalarSubselect(const execplan::CalpontSelectExecutionPlan& csep)
{
  if (treeHasUnsupportedOuterJoinSub(csep.filters()))
    return true;

  for (const auto& sub : csep.subSelectList())
  {
    auto* subCsep = dynamic_cast<const execplan::CalpontSelectExecutionPlan*>(sub.get());
    if (subCsep && outerJoinOnContainsScalarSubselect(*subCsep))
      return true;
  }

  for (const auto& derived : csep.derivedTableList())
  {
    auto* derivedCsep = dynamic_cast<const execplan::CalpontSelectExecutionPlan*>(derived.get());
    if (derivedCsep && outerJoinOnContainsScalarSubselect(*derivedCsep))
      return true;
  }

  for (const auto& unionUnit : csep.unionVec())
  {
    auto* unionCsep = dynamic_cast<const execplan::CalpontSelectExecutionPlan*>(unionUnit.get());
    if (unionCsep && outerJoinOnContainsScalarSubselect(*unionCsep))
      return true;
  }

  return false;
}

}  // namespace optimizer

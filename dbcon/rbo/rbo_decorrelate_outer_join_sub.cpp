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

#include <iostream>
#include <sstream>
#include <vector>

#include "execplan/aggregatecolumn.h"
#include "execplan/logicoperator.h"
#include "execplan/operator.h"
#include "execplan/outerjoinonfilter.h"
#include "execplan/parsetree.h"
#include "execplan/selectfilter.h"
#include "execplan/simplecolumn.h"
#include "execplan/simplefilter.h"
#include "execplan/simplescalarfilter.h"

namespace optimizer
{
namespace
{

// ---------------------------------------------------------------------------
// Matcher: detects the decorrelatable subset of MCOL-4250.
// ---------------------------------------------------------------------------
//
// Minimal pattern we aim to rewrite is:
//
//     ... LEFT JOIN T_rhs ON ...
//                AND <outer_lhs> <op> (SELECT <agg>(<expr>)
//                                      FROM   T_sub
//                                      WHERE  T_sub.k_i = <outer.x_i> ...
//                                        AND  <local predicates on T_sub>)
//
// Constraints enforced here (anything else fails the match and leaves
// IDB-1015 to be raised by the post-RBO validator):
//
//   * The subquery plan has exactly one table and no derived tables / set
//     operations / nested subqueries / HAVING clause.
//   * Its projection is exactly one scalar aggregate from the supported set
//     (COUNT_ASTERISK/COUNT/SUM/AVG/MIN/MAX), non-distinct.
//   * Its WHERE is a conjunction of SimpleFilters where each leaf is either
//       - an equality `T_sub.k_i = <column referencing another table>`
//         (correlation predicate), or
//       - a predicate that references only T_sub (local predicate).
//   * At least one correlation predicate is present.

struct CorrEqui
{
  execplan::SimpleColumn* subSide;      // T_sub.k_i  (tableAlias == subAlias)
  execplan::SimpleColumn* outerSide;    // outer column reference
  execplan::ParseTree* filterNode;      // owning ParseTree leaf in sub.filters()
};

struct SubqueryPattern
{
  execplan::ParseTree* leafNode{nullptr};          // ParseTree node inside the OJF whose data() is the SelectFilter
  execplan::SelectFilter* selectFilter{nullptr};   // pointer equal to leafNode->data()
  execplan::CalpontSelectExecutionPlan* sub{nullptr};
  std::string subAlias;                            // alias of sub->tableList()[0]
  execplan::AggregateColumn* aggCol{nullptr};
  size_t aggColPos{0};                             // position of aggCol inside sub->returnedCols()
  std::vector<CorrEqui> corrEquis;                 // one per grouping key / correlation column
  std::vector<execplan::ParseTree*> localPreds;    // leaves in sub.filters() that reference only T_sub
};

// Pre-order collector of (outerJoinOnFilter -> inside-parse-tree-leaf-holding-SelectFilter) pairs.
void collectSelectFiltersInOJF(execplan::ParseTree* root,
                               std::vector<execplan::ParseTree*>& outLeaves)
{
  if (!root)
    return;

  if (auto* ojf = dynamic_cast<execplan::OuterJoinOnFilter*>(root->data()))
  {
    // Descend into the ON-clause parse tree and collect SelectFilter leaves.
    std::vector<execplan::ParseTree*> stack{ojf->pt().get()};
    while (!stack.empty())
    {
      execplan::ParseTree* n = stack.back();
      stack.pop_back();
      if (!n)
        continue;
      if (dynamic_cast<execplan::SelectFilter*>(n->data()) ||
          dynamic_cast<execplan::SimpleScalarFilter*>(n->data()))
      {
        outLeaves.push_back(n);
        continue;
      }
      stack.push_back(n->left());
      stack.push_back(n->right());
    }
    return;
  }

  collectSelectFiltersInOJF(root->left(), outLeaves);
  collectSelectFiltersInOJF(root->right(), outLeaves);
}

// Walks a ParseTree that should be a conjunction of SimpleFilter leaves.
// Returns false if the shape is not a plain AND-of-leaves of SimpleFilters.
bool collectConjuncts(execplan::ParseTree* root, std::vector<execplan::ParseTree*>& leaves)
{
  if (!root)
    return true;
  if (auto* lop = dynamic_cast<execplan::LogicOperator*>(root->data()))
  {
    if (lop->data() != "and")
      return false;
    return collectConjuncts(root->left(), leaves) && collectConjuncts(root->right(), leaves);
  }
  leaves.push_back(root);
  return true;
}

bool isSupportedAggOp(uint8_t op)
{
  using A = execplan::AggregateColumn;
  return op == A::COUNT_ASTERISK || op == A::COUNT || op == A::SUM || op == A::AVG ||
         op == A::MIN || op == A::MAX;
}

// Attempt to match one leaf (whose data is a SelectFilter*) against the
// decorrelatable pattern.  On success fills `out`.
//
// By the time RBO sees a correlated scalar aggregate subquery, MariaDB has
// already rewritten the inner SELECT so it projects the correlation columns
// alongside the aggregate and groups by them.  The sub-CSEP we work with
// therefore looks like:
//
//     SELECT T.k_1, T.k_2, ..., AGG(x) FROM T
//       WHERE T.k_1 = <outer_1> AND T.k_2 = <outer_2> AND ...
//             [AND <local predicates on T only>]
//       GROUP BY T.k_1, T.k_2, ...
//
// We only have to promote this CSEP to a FROM-subquery and rewrite the
// enclosing SelectFilter into an equi-join against the derived table.
bool matchSubqueryPattern(execplan::ParseTree* leaf, SubqueryPattern& out)
{
  auto* sf = dynamic_cast<execplan::SelectFilter*>(leaf->data());
  if (!sf || !sf->correlated())
    return false;

  auto* sub = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(sf->sub().get());
  if (!sub)
    return false;

  if (sub->tableList().size() != 1)
    return false;
  if (!sub->unionVec().empty())
    return false;
  if (!sub->derivedTableList().empty())
    return false;
  if (!sub->subSelectList().empty())
    return false;
  if (sub->having() != nullptr)
    return false;
  if (sub->distinct())
    return false;

  const std::string subAlias = sub->tableList()[0].alias;
  if (subAlias.empty())
    return false;

  // Outer LHS(es) of the SelectFilter must all be plain SimpleColumns.  For a
  // single-row scalar subquery there is always exactly one.
  if (sf->cols().size() != 1)
    return false;
  if (!dynamic_cast<execplan::SimpleColumn*>(sf->cols()[0].get()))
    return false;

  // Exactly one AggregateColumn in the projection, the rest must be
  // SimpleColumns on the sub's single table that will serve as join keys.
  execplan::AggregateColumn* agg = nullptr;
  size_t aggPos = 0;
  std::vector<execplan::SimpleColumn*> groupSideCols;  // non-agg returned columns (join keys)
  for (size_t i = 0; i < sub->returnedCols().size(); ++i)
  {
    auto* rc = sub->returnedCols()[i].get();
    if (auto* a = dynamic_cast<execplan::AggregateColumn*>(rc))
    {
      if (agg)
        return false;  // two aggregates — unsupported
      agg = a;
      aggPos = i;
      continue;
    }
    auto* sc = dynamic_cast<execplan::SimpleColumn*>(rc);
    if (!sc || sc->tableAlias() != subAlias)
      return false;
    groupSideCols.push_back(sc);
  }
  if (!agg || !isSupportedAggOp(agg->aggOp()))
    return false;

  // GROUP BY must match the non-agg returned cols one-to-one by column name.
  if (sub->groupByCols().size() != groupSideCols.size())
    return false;
  for (const auto& g : sub->groupByCols())
  {
    auto* gsc = dynamic_cast<execplan::SimpleColumn*>(g.get());
    if (!gsc || gsc->tableAlias() != subAlias)
      return false;
    bool found = false;
    for (auto* rc : groupSideCols)
    {
      if (rc->columnName() == gsc->columnName())
      {
        found = true;
        break;
      }
    }
    if (!found)
      return false;
  }

  // Walk the WHERE tree; classify each leaf SimpleFilter as correlation equi
  // or local.  The number of correlation equi-predicates must match the group
  // key count so every derived column gets a unique join partner.
  std::vector<execplan::ParseTree*> conjuncts;
  if (!collectConjuncts(sub->filters(), conjuncts))
    return false;

  std::vector<CorrEqui> corrEquis;
  std::vector<execplan::ParseTree*> localPreds;
  for (auto* node : conjuncts)
  {
    auto* simple = dynamic_cast<execplan::SimpleFilter*>(node->data());
    if (!simple)
      return false;

    auto* lhsSc = dynamic_cast<execplan::SimpleColumn*>(simple->lhs());
    auto* rhsSc = dynamic_cast<execplan::SimpleColumn*>(simple->rhs());
    const bool lhsIsSub = lhsSc && lhsSc->tableAlias() == subAlias;
    const bool rhsIsSub = rhsSc && rhsSc->tableAlias() == subAlias;
    const bool lhsIsOuter = lhsSc && !lhsIsSub && !lhsSc->tableAlias().empty();
    const bool rhsIsOuter = rhsSc && !rhsIsSub && !rhsSc->tableAlias().empty();
    const std::string opSym = simple->op() ? simple->op()->data() : std::string();

    if (opSym == "=" && ((lhsIsSub && rhsIsOuter) || (lhsIsOuter && rhsIsSub)))
    {
      CorrEqui ce;
      ce.filterNode = node;
      ce.subSide = lhsIsSub ? lhsSc : rhsSc;
      ce.outerSide = lhsIsSub ? rhsSc : lhsSc;
      corrEquis.push_back(ce);
      continue;
    }
    if (lhsIsOuter || rhsIsOuter)
      return false;  // non-equi predicate referencing outer — cannot decorrelate
    localPreds.push_back(node);
  }

  if (corrEquis.size() != groupSideCols.size())
    return false;

  // Sanity: each corrEqui.subSide.columnName must match one of groupSideCols.
  for (auto& ce : corrEquis)
  {
    bool found = false;
    for (auto* gsc : groupSideCols)
    {
      if (gsc->columnName() == ce.subSide->columnName())
      {
        found = true;
        break;
      }
    }
    if (!found)
      return false;
  }

  out.leafNode = leaf;
  out.selectFilter = sf;
  out.sub = sub;
  out.subAlias = subAlias;
  out.aggCol = agg;
  out.aggColPos = aggPos;
  out.corrEquis = std::move(corrEquis);
  out.localPreds = std::move(localPreds);
  return true;
}

// ---------------------------------------------------------------------------
// Top-level walkers over the outer CSEP's `filters()`.
// ---------------------------------------------------------------------------

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
  // The shared Rule::walk() already recurses into subSelectList and unionVec,
  // so only the current CSEP's top-level filters need to be examined here.
  return treeHasUnsupportedOuterJoinSub(csep.filters());
}

bool applyDecorrelateOuterJoinSub(execplan::CalpontSelectExecutionPlan& csep,
                                  optimizer::RBOptimizerContext& ctx)
{
  std::vector<execplan::ParseTree*> leaves;
  collectSelectFiltersInOJF(csep.filters(), leaves);
  if (leaves.empty())
    return false;

  bool anyMatched = false;
  for (auto* leaf : leaves)
  {
    SubqueryPattern pat;
    if (!matchSubqueryPattern(leaf, pat))
      continue;

    anyMatched = true;

    if (csep.traceOn() || ctx.logRulesEnabled())
    {
      std::cerr << "decorrelate_outer_join_sub: matched pattern"
                << " sub_alias=" << pat.subAlias
                << " agg_op=" << static_cast<int>(pat.aggCol->aggOp())
                << " corr_equi=" << pat.corrEquis.size()
                << " local_preds=" << pat.localPreds.size() << std::endl;
    }

    // TODO(MCOL-4250): implement the actual rewrite.  At this point `pat`
    // carries everything required to rebuild the plan:
    //   * pat.leafNode   — ParseTree leaf inside the OJF currently holding
    //                       the SelectFilter.
    //   * pat.sub        — the correlated subquery CSEP, which MariaDB has
    //                       already shaped as "GROUP BY correlation-keys".
    //   * pat.subAlias   — alias of its single table.
    //   * pat.aggCol     — the scalar aggregate, projected at pat.aggColPos.
    //   * pat.corrEquis  — one T_sub.k_i = outer.x_i predicate per group key.
    //   * pat.localPreds — non-correlated predicates on T_sub.
    //
    // Until the rewrite lands, leave the plan untouched so the post-RBO
    // validator keeps raising IDB-1015.
  }

  (void)anyMatched;
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

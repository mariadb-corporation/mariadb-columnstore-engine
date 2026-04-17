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
#include "execplan/calpontsystemcatalog.h"
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

namespace
{

// Builds a right-deep AND tree over the given leaves.  Caller retains
// ownership of the returned root.  Assumes `leaves.size() >= 1`.
execplan::ParseTree* buildAndTree(const std::vector<execplan::ParseTree*>& leaves)
{
  execplan::ParseTree* result = leaves.back();
  for (ssize_t i = static_cast<ssize_t>(leaves.size()) - 2; i >= 0; --i)
  {
    auto* andOp = new execplan::LogicOperator("and");
    auto* andNode = new execplan::ParseTree(andOp);
    andNode->left(leaves[i]);
    andNode->right(result);
    result = andNode;
  }
  return result;
}

// Constructs a fresh SimpleColumn that references column `refCol` (projected
// at position `colPos` of the derived CSEP whose alias is `derivedAlias`).
execplan::SimpleColumn* makeDerivedColumnRef(execplan::ReturnedColumn* refCol,
                                             const std::string& derivedAlias,
                                             size_t colPos,
                                             long timeZone)
{
  auto* sc = new execplan::SimpleColumn();
  sc->columnName(refCol->alias());
  sc->tableAlias(derivedAlias);
  sc->derivedTable(derivedAlias);
  sc->derivedRefCol(refCol);
  sc->colPosition(static_cast<int>(colPos));
  sc->resultType(refCol->resultType());
  sc->timeZone(timeZone);
  sc->sequence(static_cast<int>(colPos));
  refCol->incRefCount();
  return sc;
}

// Performs the actual rewrite for one matched pattern.  Returns true on
// success; the CSEP is either fully rewritten or left untouched.
bool rewriteMatchedPattern(execplan::CalpontSelectExecutionPlan& csep,
                           SubqueryPattern& pat,
                           optimizer::RBOptimizerContext& ctx)
{
  const long timeZone = ctx.getGwi().timeZone;

  // ----- 1. Pick unique aliases for every derived column so the outer CSEP
  //       can reference them by name.
  const std::string uniqSuffix = "__dec" + std::to_string(ctx.getUniqueId());
  const std::string derivedAlias = "$dec_sub" + uniqSuffix;
  const std::string aggAlias = "dec_agg" + uniqSuffix;

  std::vector<std::string> groupAliases(pat.sub->returnedCols().size());
  size_t nonAggIdx = 0;
  for (size_t i = 0; i < pat.sub->returnedCols().size(); ++i)
  {
    if (i == pat.aggColPos)
    {
      pat.sub->returnedCols()[i]->alias(aggAlias);
      groupAliases[i] = aggAlias;
    }
    else
    {
      std::string a = "dec_k" + std::to_string(nonAggIdx++) + uniqSuffix;
      pat.sub->returnedCols()[i]->alias(a);
      groupAliases[i] = a;
    }
  }

  // ----- 2. Capture everything we need from the correlation equi-predicates
  //       BEFORE dismantling the sub's filter tree — those leaves are owned
  //       by sub->filters() and will be freed together with it below.
  struct CorrInfo
  {
    std::string subColumnName;                          // T_sub.k_i
    std::unique_ptr<execplan::SimpleColumn> outerSide;  // deep copy of outer SC
  };
  std::vector<CorrInfo> corrInfos;
  corrInfos.reserve(pat.corrEquis.size());
  for (const auto& ce : pat.corrEquis)
  {
    corrInfos.push_back({ce.subSide->columnName(),
                         std::unique_ptr<execplan::SimpleColumn>(new execplan::SimpleColumn(*ce.outerSide))});
  }
  // Main predicate LHS lives inside the SelectFilter's own cols() vector,
  // which stays alive until we delete the SelectFilter at step 6.  No need
  // to clone it here beyond constructing the new SimpleColumn then.

  // Strip the correlation equi-predicates from the sub's WHERE tree.
  // Each surviving local predicate is deep-copied so we can safely destroy
  // the whole old filter tree (which owns the correlation leaves).
  std::vector<execplan::ParseTree*> newLocals;
  newLocals.reserve(pat.localPreds.size());
  for (auto* lp : pat.localPreds)
    newLocals.push_back(new execplan::ParseTree(*lp));

  execplan::ParseTree* newFilters = newLocals.empty() ? nullptr : buildAndTree(newLocals);
  delete pat.sub->filters();
  pat.sub->filters(newFilters);

  // ----- 3. Convert the subquery CSEP into a FROM-subquery.
  pat.sub->location(execplan::CalpontSelectExecutionPlan::FROM);
  pat.sub->subType(execplan::CalpontSelectExecutionPlan::FROM_SUBS);
  pat.sub->derivedTbAlias(derivedAlias);

  // The SelectFilter's sub() is a shared_ptr we can reuse as the derived
  // CSEP entry (avoids re-cloning or leaking ownership).  We take the shared
  // pointer out of the SelectFilter so that when we later delete the
  // SelectFilter its destructor does not also destroy the CSEP.
  execplan::SCSEP derivedScep = pat.selectFilter->sub();
  execplan::SCSEP empty;
  pat.selectFilter->sub(empty);  // release the SCSEP from the SelectFilter

  // ----- 4. Attach the derived CSEP to the outer plan.
  csep.derivedTableList().push_back(derivedScep);
  {
    auto tl = csep.tableList();
    tl.push_back(execplan::CalpontSystemCatalog::TableAliasName("", "", derivedAlias, ""));
    csep.tableList(tl);
  }

  // ----- 5. Construct the replacement parse tree for the SelectFilter leaf.
  //       It is the AND of:
  //         * one equi-predicate per group key (derived.key_i = outer.x_i)
  //         * the main predicate   (outer_lhs  sf->op  derived.agg)
  std::vector<execplan::ParseTree*> replacementLeaves;
  replacementLeaves.reserve(pat.corrEquis.size() + 1);

  for (auto& ci : corrInfos)
  {
    // Find the position in returnedCols whose columnName matches ci.subColumnName.
    size_t pos = 0;
    for (; pos < pat.sub->returnedCols().size(); ++pos)
    {
      if (pos == pat.aggColPos)
        continue;
      auto* sc = dynamic_cast<execplan::SimpleColumn*>(pat.sub->returnedCols()[pos].get());
      if (sc && sc->columnName() == ci.subColumnName)
        break;
    }
    if (pos == pat.sub->returnedCols().size())
    {
      // Should not happen — matcher validated this invariant.
      return false;
    }

    auto* refCol = pat.sub->returnedCols()[pos].get();
    auto* lhs = makeDerivedColumnRef(refCol, derivedAlias, pos, timeZone);
    auto* rhs = ci.outerSide.release();  // transfer ownership to SimpleFilter
    auto* eqOp = new execplan::Operator("=");
    auto* sf = new execplan::SimpleFilter(execplan::SOP(eqOp), lhs, rhs, timeZone);
    replacementLeaves.push_back(new execplan::ParseTree(sf));
  }

  // Main predicate: outer_lhs <op> derived.agg
  {
    auto* outerLhs = dynamic_cast<execplan::SimpleColumn*>(pat.selectFilter->cols()[0].get());
    if (!outerLhs)
      return false;
    auto* lhs = new execplan::SimpleColumn(*outerLhs);
    auto* rhs = makeDerivedColumnRef(pat.sub->returnedCols()[pat.aggColPos].get(),
                                     derivedAlias, pat.aggColPos, timeZone);
    auto* op = pat.selectFilter->op() ? pat.selectFilter->op()->clone() : new execplan::Operator("=");
    auto* sf = new execplan::SimpleFilter(execplan::SOP(op), lhs, rhs, timeZone);
    replacementLeaves.push_back(new execplan::ParseTree(sf));
  }

  execplan::ParseTree* replacementRoot = buildAndTree(replacementLeaves);

  // ----- 6. Swap replacementRoot's contents into pat.leafNode in place so the
  //       enclosing OJF parse tree keeps its shape.
  execplan::TreeNode* oldData = pat.leafNode->data();
  pat.leafNode->data(replacementRoot->data());
  pat.leafNode->left(replacementRoot->left());
  pat.leafNode->right(replacementRoot->right());
  replacementRoot->data(nullptr);
  replacementRoot->left(static_cast<execplan::ParseTree*>(nullptr));
  replacementRoot->right(static_cast<execplan::ParseTree*>(nullptr));
  delete replacementRoot;
  delete oldData;  // releases the old SelectFilter (which no longer owns the sub CSEP)

  return true;
}

}  // namespace

bool applyDecorrelateOuterJoinSub(execplan::CalpontSelectExecutionPlan& csep,
                                  optimizer::RBOptimizerContext& ctx)
{
  std::vector<execplan::ParseTree*> leaves;
  collectSelectFiltersInOJF(csep.filters(), leaves);
  if (leaves.empty())
    return false;

  bool anyRewrote = false;
  for (auto* leaf : leaves)
  {
    SubqueryPattern pat;
    if (!matchSubqueryPattern(leaf, pat))
      continue;

    if (csep.traceOn() || ctx.logRulesEnabled())
    {
      std::cerr << "decorrelate_outer_join_sub: matched pattern"
                << " sub_alias=" << pat.subAlias
                << " agg_op=" << static_cast<int>(pat.aggCol->aggOp())
                << " corr_equi=" << pat.corrEquis.size()
                << " local_preds=" << pat.localPreds.size() << std::endl;
    }

    if (rewriteMatchedPattern(csep, pat, ctx))
      anyRewrote = true;
  }

  return anyRewrote;
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

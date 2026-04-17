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
#include "lib/csep_walk.h"
#include "lib/derived_column.h"
#include "lib/derived_table.h"
#include "lib/filter_builders.h"
#include "lib/parse_tree_ops.h"

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
// Thin wrapper over optimizer::lib::collectLeavesInOuterJoinOn with a
// decorrelate-specific predicate (SelectFilter or SimpleScalarFilter).
void collectSelectFiltersInOJF(execplan::ParseTree* root,
                               std::vector<execplan::ParseTree*>& outLeaves)
{
  optimizer::lib::collectLeavesInOuterJoinOn(
      root, outLeaves,
      [](execplan::TreeNode* tn) -> bool
      {
        return dynamic_cast<execplan::SelectFilter*>(tn) != nullptr ||
               dynamic_cast<execplan::SimpleScalarFilter*>(tn) != nullptr;
      });
}

bool isSupportedAggOp(uint8_t op)
{
  using A = execplan::AggregateColumn;
  // AVG is deliberately excluded: its result type is DOUBLE/DECIMAL while the
  // outer LHS is typically INT, which makes the executor reject the rewritten
  // equi-join with MCS-1002 ("incompatible column type").  COUNT/SUM keep the
  // int family (BIGINT), MIN/MAX preserve the input type, so both are safe
  // for the hash-join's type-compatibility check.
  return op == A::COUNT_ASTERISK || op == A::COUNT || op == A::SUM ||
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

  // A single-row scalar subquery has exactly one outer LHS expression.  We
  // require it to be a plain SimpleColumn for two reasons:
  //  * constants / arithmetic expressions leave the rewritten main predicate
  //    with no direct column-to-derived link (see MCS-1000 "tables are not
  //    joined");
  //  * the executor's join-inference needs the outer LHS to share a table
  //    with at least one correlation equi-predicate so it can pull the
  //    derived table into the same OJF group as the rest of the join.
  if (sf->cols().size() != 1)
    return false;
  auto* outerLhsSc = dynamic_cast<execplan::SimpleColumn*>(sf->cols()[0].get());
  if (!outerLhsSc)
    return false;

  // The outer comparison operator must be '=' as well.  For inequalities
  // (<, >, <=, >=, <>) the executor does not pick up the derived table as a
  // join partner and raises MCS-1000.  Dropping this guard produces
  // semantically-correct plans that simply fail to execute.
  if (!sf->op() || sf->op()->data() != "=")
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
  if (!optimizer::lib::collectConjuncts(sub->filters(), conjuncts))
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

  // The outer LHS must share a table with at least one correlation equi so
  // that, after rewrite, the executor's join inference can pull the derived
  // table into the same OJF group as the other participating tables.
  {
    bool shares = false;
    for (const auto& ce : corrEquis)
    {
      if (ce.outerSide->tableAlias() == outerLhsSc->tableAlias())
      {
        shares = true;
        break;
      }
    }
    if (!shares)
      return false;
  }

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

// Thin shim around optimizer::lib::makeDerivedColumnRef, preserved for
// call-site readability.
inline execplan::SimpleColumn* makeDerivedColumnRef(execplan::ReturnedColumn* refCol,
                                                    const std::string& derivedAlias,
                                                    size_t colPos,
                                                    long timeZone)
{
  return optimizer::lib::makeDerivedColumnRef(refCol, derivedAlias, static_cast<int64_t>(colPos),
                                              timeZone);
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

  execplan::ParseTree* newFilters = optimizer::lib::andAll(newLocals);
  delete pat.sub->filters();
  pat.sub->filters(newFilters);

  // ----- 3. Convert the subquery CSEP into a FROM-subquery.
  optimizer::lib::promoteCSEPToDerived(pat.sub, derivedAlias);

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
    replacementLeaves.push_back(optimizer::lib::makeCmpFilter(lhs, "=", rhs, timeZone));
  }

  // Main predicate: outer_lhs <op> derived.agg.  `outer_lhs` is any
  // ReturnedColumn; we clone it via its virtual clone() so constants and
  // arithmetic expressions work the same as bare columns.
  {
    auto* outerLhsSrc = pat.selectFilter->cols()[0].get();
    if (!outerLhsSrc)
      return false;
    auto* lhs = outerLhsSrc->clone();
    auto* rhs = makeDerivedColumnRef(pat.sub->returnedCols()[pat.aggColPos].get(),
                                     derivedAlias, pat.aggColPos, timeZone);
    const std::string opSym = pat.selectFilter->op() ? pat.selectFilter->op()->data() : std::string("=");
    replacementLeaves.push_back(optimizer::lib::makeCmpFilter(lhs, opSym, rhs, timeZone));
  }

  execplan::ParseTree* replacementRoot = optimizer::lib::andAll(replacementLeaves);

  // ----- 6. Swap replacementRoot's contents into pat.leafNode in place so the
  //       enclosing OJF parse tree keeps its shape.  replaceInPlace takes
  //       care of freeing the old SelectFilter and the replacementRoot shell.
  optimizer::lib::replaceInPlace(pat.leafNode, replacementRoot);

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
    {
      anyRewrote = true;
      // Several scalar subqueries inside the same OJF would otherwise share
      // the same uniqSuffix and clash on derived-table aliases / column names.
      ctx.incrementUniqueId();
    }
  }

  return anyRewrote;
}

bool outerJoinOnContainsScalarSubselect(const execplan::CalpontSelectExecutionPlan& csep)
{
  if (treeHasUnsupportedOuterJoinSub(csep.filters()))
    return true;

  return optimizer::lib::walkNestedCSEPs(
      csep, [](const execplan::CalpontSelectExecutionPlan& sub) -> bool
      { return treeHasUnsupportedOuterJoinSub(sub.filters()); });
}

}  // namespace optimizer

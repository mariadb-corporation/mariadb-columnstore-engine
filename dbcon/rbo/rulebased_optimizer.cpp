/* Copyright (C) 2025 MariaDB Corporation

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

#include "rulebased_optimizer.h"

#include "configcpp.h"
#include "constantcolumn.h"
#include "execplan/calpontselectexecutionplan.h"
#include "predicateoperator.h"
#include "rbo_apply_parallel_ces.h"
#include "rbo_apply_rewrite_distinct.h"
#include "rbo_common_leaf_conjunctions_to_top.h"
#include "rbo_decorrelate_outer_join_sub.h"
#include "rbo_groupby_wrap_columns.h"
#include "rbo_predicate_pushdown.h"
#include "utils/pron/pron.h"

#include "calpontsystemcatalog.h"
#include "functioncolumn.h"

namespace optimizer
{

std::string getRewrittenSubTableAlias(const execplan::CalpontSystemCatalog::TableAliasName& table,
                                      const RBOptimizerContext& ctx)
{
  static const std::string rewrittenSubTableAliasPrefix{"$added_sub_"};
  return rewrittenSubTableAliasPrefix + table.schema + "_" + table.table + "_" +
         std::to_string(ctx.getUniqueId());
}

// Apply a list of rules to a CSEP
bool optimizeCSEPWithRules(execplan::CalpontSelectExecutionPlan& root, const std::vector<Rule>& rules,
                           optimizer::RBOptimizerContext& ctx)
{
  bool changed = false;
  config::Config* cfg = config::Config::makeConfig();
  const auto& pronMap = utils::Pron::instance().pron();

  for (const auto& rule : rules)
  {
    bool apply_rule = true;
    try
    {
      const std::string val = cfg->getConfig("OptimizerRules", rule.getName());
      apply_rule = config::parseBooleanParamValue(val);

      const std::string k1 = std::string("OptimizerRules.") + rule.getName();
      // PRON params override the config file
      auto it = pronMap.find(k1);
      if (it != pronMap.end())
      {
        apply_rule = config::parseBooleanParamValue(it->second);
      }
    }
    catch (...)
    {
      // Missing section/name or other config issues – keep default behavior
    }
    if (apply_rule)
    {
      changed |= rule.apply(root, ctx);
    }
  }
  return changed;
}

// high level API call for optimizer
bool optimizeCSEP(execplan::CalpontSelectExecutionPlan& root, optimizer::RBOptimizerContext& ctx,
                  bool useUnstableOptimizer)
{
  std::vector<optimizer::Rule> rules;

  // Normalize the WHERE tree up-front so subsequent structural rules
  // (predicate pushdown, decorrelation, ...) see the lifted common
  // conjunctions at the CSEP root.
  optimizer::Rule commonLeafConjunctionsToTop{"common_leaf_conjunctions_to_top",
                                               optimizer::commonLeafConjunctionsToTopFilter,
                                               optimizer::applyCommonLeafConjunctionsToTop};
  rules.push_back(commonLeafConjunctionsToTop);

  if (useUnstableOptimizer)
  {
    optimizer::Rule parallelCES{"parallel_ces", optimizer::parallelCESFilter, optimizer::applyParallelCES};
    rules.push_back(parallelCES);

    optimizer::Rule rewriteDistinct{"rewrite_distinct", optimizer::rewriteDistinctFilter,
                                    optimizer::applyRewriteDistinct};
    rules.push_back(rewriteDistinct);
  }
  optimizer::Rule rewriteGroupBy{"groupby_wrap", optimizer::groupByWrapColumnsFilter,
                                  optimizer::applyGroupByWrapColumns};
  rules.push_back(rewriteGroupBy);
  // MCOL-4250: rewrite scalar subqueries inside OUTER JOIN ON into equi-joins
  // with a GROUP-BY derived table.  Must run before predicate_pushdown so the
  // latter can push through the freshly-introduced derived tables.
  optimizer::Rule decorrelateOuterJoinSub{"decorrelate_outer_join_sub",
                                          optimizer::decorrelateOuterJoinSubFilter,
                                          optimizer::applyDecorrelateOuterJoinSub};
  rules.push_back(decorrelateOuterJoinSub);
  optimizer::Rule predicatePushdown{"predicate_pushdown", optimizer::predicatePushdownFilter,
                                    optimizer::applyPredicatePushdown};
  rules.push_back(predicatePushdown);

  return optimizeCSEPWithRules(root, rules, ctx);
}

// Apply iteratively until CSEP is converged by rule
bool Rule::apply(execplan::CalpontSelectExecutionPlan& root, optimizer::RBOptimizerContext& ctx) const
{
  bool changedThisRound = false;
  bool hasBeenApplied = false;

  do
  {
    changedThisRound = walk(root, ctx);
    hasBeenApplied |= changedThisRound;
    if (ctx.logRulesEnabled() && changedThisRound)
    {
      std::cout << "MCS RBO: " << name << " has been applied this round." << std::endl;
    }
    if (changedThisRound)
    {
      // Record rule application
      ctx.addAppliedRule(name);
    }
  } while (changedThisRound && !applyOnlyOnce);

  return hasBeenApplied;
}

// DFS walk to match CSEP and apply rules if match
bool Rule::walk(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx) const
{
  bool rewrite = false;

  std::stack<execplan::CalpontSelectExecutionPlan*> planStack;
  planStack.push(&csep);

  while (!planStack.empty())
  {
    execplan::CalpontSelectExecutionPlan* current = planStack.top();
    planStack.pop();

    // Walk nested UNION UNITS
    for (auto& unionUnit : current->unionVec())
    {
      auto* unionUnitPtr = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(unionUnit.get());
      if (unionUnitPtr)
      {
        planStack.push(unionUnitPtr);
      }
    }

    // Walk nested subselect in filters, e.g. SEMI-JOIN and also derived tables
    for (auto& subselect : current->subSelectList())
    {
      auto* subselectPtr = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(subselect.get());
      if (subselectPtr)
      {
        planStack.push(subselectPtr);
      }
    }

    // TODO add walking nested subselect in projection. See CSEP::fSelectSubList

    if (mayApply(*current, ctx))
    {
      rewrite |= applyRule(*current, ctx);
      ctx.incrementUniqueId();
    }
  }

  return rewrite;
}

}  // namespace optimizer

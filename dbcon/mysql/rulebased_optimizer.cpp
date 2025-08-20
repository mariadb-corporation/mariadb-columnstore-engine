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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "rulebased_optimizer.h"

#include "configcpp.h"
#include "constantcolumn.h"
#include "execplan/calpontselectexecutionplan.h"
#include "execplan/simplecolumn.h"
#include "existsfilter.h"
#include "logicoperator.h"
#include "operator.h"
#include "predicateoperator.h"
#include "simplefilter.h"
#include "rbo_apply_parallel_ces.h"
#include "rbo_predicate_pushdown.h"
#include "utils/pron/pron.h"

namespace optimizer
{

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
  if (useUnstableOptimizer)
  {
    optimizer::Rule parallelCES{"parallel_ces", optimizer::parallelCESFilter, optimizer::applyParallelCES};
    rules.push_back(parallelCES);
  }

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
    if (ctx.logRules && changedThisRound)
    {
      std::cout << "MCS RBO: " << name << " has been applied this round." << std::endl;
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
      ++ctx.uniqueId;
    }
  }

  return rewrite;
}

}  // namespace optimizer

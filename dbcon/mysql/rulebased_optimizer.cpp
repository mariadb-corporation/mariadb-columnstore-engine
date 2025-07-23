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

#include "constantcolumn.h"
#include "execplan/calpontselectexecutionplan.h"
#include "execplan/simplecolumn.h"
#include "existsfilter.h"
#include "logicoperator.h"
#include "operator.h"
#include "predicateoperator.h"
#include "simplefilter.h"
#include "rbo_apply_parallel_ces.h"

namespace optimizer
{

// Apply a list of rules to a CSEP
bool optimizeCSEPWithRules(execplan::CalpontSelectExecutionPlan& root, const std::vector<Rule>& rules,
                           optimizer::RBOptimizerContext& ctx)
{
  bool changed = false;
  for (const auto& rule : rules)
  {
    changed |= rule.apply(root, ctx);
  }
  return changed;
}

// high level API call for optimizer
bool optimizeCSEP(execplan::CalpontSelectExecutionPlan& root, optimizer::RBOptimizerContext& ctx)
{
  optimizer::Rule parallelCES{"parallelCES", optimizer::matchParallelCES, optimizer::applyParallelCES};

  std::vector<optimizer::Rule> rules = {parallelCES};

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

    for (auto& table : current->derivedTableList())
    {
      auto* csepPtr = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(table.get());
      if (csepPtr)
      {
        planStack.push(csepPtr);
      }
    }

    for (auto& unionUnit : current->unionVec())
    {
      auto* unionUnitPtr = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(unionUnit.get());
      if (unionUnitPtr)
      {
        planStack.push(unionUnitPtr);
      }
    }

    if (matchRule(*current, ctx))
    {
      applyRule(*current, ctx);
      ++ctx.uniqueId;
      rewrite = true;
    }
  }

  return rewrite;
}

}  // namespace optimizer

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
#include "rbo_predicate_pushdown.h"
#include "rbo_apply_rewrite_distinct.h"
#include "utils/pron/pron.h"

#include "calpontsystemcatalog.h"
#include "functioncolumn.h"

namespace optimizer
{

std::string getRewrittenSubTableAlias(const execplan::CalpontSystemCatalog::TableAliasName& table,
                                      const RBOptimizerContext& ctx)
{
#if 01
  static const std::string rewrittenSubTableAliasPrefix{"$added_sub_"};
  return rewrittenSubTableAliasPrefix + table.schema + "_" + table.table + "_" +
         std::to_string(ctx.getUniqueId());
#else
  if (table.alias.empty())
  {
    return table.table;
  }
  else
  {
    return table.alias;  
  }
#endif
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
  if (useUnstableOptimizer)
  {
    optimizer::Rule parallelCES{"parallel_ces", optimizer::parallelCESFilter, optimizer::applyParallelCES};
    rules.push_back(parallelCES);

//    optimizer::Rule rewriteDistinct{"rewrite_distinct", optimizer::rewriteDistinctFilter,
//                                    optimizer::applyRewriteDistinct};
//    rules.push_back(rewriteDistinct);
  }

//  optimizer::Rule predicatePushdown{"predicate_pushdown", optimizer::predicatePushdownFilter,
//                                    optimizer::applyPredicatePushdown};
//  rules.push_back(predicatePushdown);

  return optimizeCSEPWithRules(root, rules, ctx);
}

// Apply iteratively until CSEP is converged by rule
bool Rule::apply(execplan::CalpontSelectExecutionPlan& root, optimizer::RBOptimizerContext& ctx) const
{
  bool changedThisRound = false;
  bool hasBeenApplied = false;

  idblog("Rule::apply START rule=" << name);
  do
  {
    changedThisRound = walk(root, ctx);
    hasBeenApplied |= changedThisRound;
    idblog("Rule::apply round complete: changedThisRound=" << changedThisRound << " hasBeenApplied=" << hasBeenApplied);
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

  idblog("Rule::apply END rule=" << name << " hasBeenApplied=" << hasBeenApplied);
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
    idblog("Rule::walk loop iteration, planStack.size=" << planStack.size());
    execplan::CalpontSelectExecutionPlan* current = planStack.top();
    planStack.pop();
    
    idblog("Rule::walk processing CSEP subType=" << current->subType() 
           << " unionVec.size=" << current->unionVec().size()
           << " subSelectList.size=" << current->subSelectList().size());

    // Walk nested UNION UNITS
    for (auto& unionUnit : current->unionVec())
    {
      auto* unionUnitPtr = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(unionUnit.get());
      if (unionUnitPtr)
      {
        idblog("  pushing unionUnit subType=" << unionUnitPtr->subType());
        planStack.push(unionUnitPtr);
      }
    }

    // Walk nested subselect in filters, e.g. SEMI-JOIN and also derived tables
    for (auto& subselect : current->subSelectList())
    {
      auto* subselectPtr = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(subselect.get());
      if (subselectPtr)
      {
        idblog("  pushing subselect subType=" << subselectPtr->subType());
        planStack.push(subselectPtr);
      }
    }

    // TODO add walking nested subselect in projection. See CSEP::fSelectSubList

    if (mayApply(*current, ctx))
    {
      idblog("  mayApply=true, calling applyRule");
      try
      {
        rewrite |= applyRule(*current, ctx);
        ctx.incrementUniqueId();
        idblog("  applyRule completed, planStack.size=" << planStack.size());
      }
      catch (const std::exception& e)
      {
        idblog("  STD EXCEPTION in applyRule: " << e.what());
        throw;
      }
      catch (...)
      {
        idblog("  UNKNOWN EXCEPTION in applyRule");
        throw;
      }
    }
    else
    {
      idblog("  mayApply=false, skipping applyRule");
    }
  }

  idblog("Rule::walk END, rewrite=" << rewrite << " planStack.empty=" << planStack.empty());
  return rewrite;
}

}  // namespace optimizer

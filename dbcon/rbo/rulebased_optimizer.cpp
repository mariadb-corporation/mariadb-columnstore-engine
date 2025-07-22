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

#include "functioncolumn.h"

namespace
{
bool matchDistinct(execplan::CalpontSelectExecutionPlan& csep);
void applyDistinct(execplan::CalpontSelectExecutionPlan& csep);
}  // namespace

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

    optimizer::Rule rewriteDistinct{"rewriteDistinct", optimizer::matchDistinct, optimizer::applyDistinct};
    rules.push_back(rewriteDistinct);
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

namespace
{
bool matchDistinct(execplan::CalpontSelectExecutionPlan& csep)
{
  return csep.distinct();
}

void applyDistinct(execplan::CalpontSelectExecutionPlan& csep)
{
  csep.distinct(false);
  auto new_query = csep.clone();
#if 0
  std::swap(csep, *new_query);
  new_query->orderByCols({});
  new_query->having(nullptr);
  new_query->limitNum = 0;
  new_query->limitStart = 0;
#endif
  auto tableAlias = optimizer::RewrittenSubTableAliasPrefix + csep.schemaName() + "_" + csep.tableName();
  new_query->location(execplan::CalpontSelectExecutionPlan::FROM);
  new_query->subType(execplan::CalpontSelectExecutionPlan::FROM_SUBS);
  new_query->derivedTbAlias(tableAlias);

  execplan::CalpontSelectExecutionPlan::TableList tblList;
  tblList.push_back(execplan::make_aliasview("", "", tableAlias, ""));
  csep.tableList(tblList);
  execplan::CalpontSelectExecutionPlan::SelectList derivedTblList;
  derivedTblList.emplace_back(new_query);
  csep.derivedTableList(derivedTblList);

  csep.filters(nullptr);

  csep.returnedCols({});
  csep.groupByCols({});
  size_t colPos = 0;
  for (auto& rc : new_query->returnedCols())
  {
    auto* rcsc = dynamic_cast<execplan::SimpleColumn*>(rc.get());
    // auto* rcfc = dynamic_cast<execplan::FunctionColumn*>(rc.get());
#if 0
    if (!rcsc)
    {
      throw std::runtime_error("applyDistinct: unexpected column type");
    }
    auto rcCloned = boost::make_shared<execplan::SimpleColumn>(*rc);
    rcCloned->tableName("");
    rcCloned->schemaName("");
    rcCloned->tableAlias(tableAlias);
    rcCloned->colPosition(colPos);
    rcCloned->resultType(rc->resultType());
    rcCloned->distinct(false);
    rcCloned->derivedTable(tableAlias);
    rcCloned->alias(tableAlias + "." + rc->alias());
    csep.returnedCols().emplace_back(rcCloned);

    auto grpByCloned = boost::make_shared<execplan::SimpleColumn>(*rcCloned);
    csep.groupByCols().emplace_back(grpByCloned);
#endif
    auto rcCloned = boost::make_shared<execplan::SimpleColumn>();
    // fill SimpleColumn data
    rcCloned->schemaName("");
    rcCloned->tableName(tableAlias);
    if (rcsc)
      rcCloned->columnName(rcsc->columnName());
    rcCloned->oid(0);
    rcCloned->tableAlias(tableAlias);
    rcCloned->data("");
    if (rcsc)
      rcCloned->timeZone(rcsc->timeZone());

    // fill ReturnedColumn data
    rcCloned->charsetNumber(rc->charsetNumber());
    rcCloned->alias(tableAlias + "." + rc->alias());

    // fill TreeNode data
    rcCloned->derivedTable(tableAlias);
    rcCloned->derivedRefCol(rc.get());
    rcCloned->resultType(rc->resultType());
    rcCloned->operationType(rc->operationType());
    rcCloned->colPosition(colPos);
    rc->refCount(rc->refCount() + 1);

    csep.returnedCols().emplace_back(rcCloned);

    auto grpByCloned = boost::make_shared<execplan::SimpleColumn>(*rcCloned);
    grpByCloned->orderPos(colPos);
    rc->refCount(rc->refCount() + 1);
    csep.groupByCols().emplace_back(grpByCloned);

    ++colPos;
  }

  new_query->distinct(false);
}

}  // namespace

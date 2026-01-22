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

#pragma once

#include <map>
#include <string>
#include <tuple>
#include <vector>

#define PREFER_MY_CONFIG_H
#include <my_config.h>
// #include "idb_mysql.h"

#include <dbcon/mysql/ha_mcs_impl_if.h>

#include "execplan/calpontselectexecutionplan.h"
#include "execplan/calpontsystemcatalog.h"
#include "execplan/simplecolumn.h"

namespace optimizer
{

// Forward declarations for table alias mapping types
// Compares by schema, table, AND alias - for exact table matching
struct TableAliasLessThan
{
  bool operator()(const execplan::CalpontSystemCatalog::TableAliasName& lhs,
                  const execplan::CalpontSystemCatalog::TableAliasName& rhs) const
  {
    if (lhs.schema < rhs.schema)
      return true;
    if (lhs.schema == rhs.schema)
    {
      if (lhs.table < rhs.table)
        return true;
      if (lhs.table == rhs.table)
        return lhs.alias < rhs.alias;
    }
    return false;
  }
};

// Compares by schema and table ONLY (ignores alias) - for finding same base table across queries
struct TableSchemaTableLessThan
{
  bool operator()(const execplan::CalpontSystemCatalog::TableAliasName& lhs,
                  const execplan::CalpontSystemCatalog::TableAliasName& rhs) const
  {
    if (lhs.schema < rhs.schema)
      return true;
    if (lhs.schema == rhs.schema)
      return lhs.table < rhs.table;
    return false;
  }
};

struct SimpleColumnLessThan
{
  bool operator()(const execplan::SimpleColumn* lhs, const execplan::SimpleColumn* rhs) const
  {
    return lhs->columnName() < rhs->columnName();
  }
};

using SCToPosCounterMap = std::map<execplan::SimpleColumn*, size_t, SimpleColumnLessThan>;
using TableAliasToNewAliasAndSCPositionsMap =
    std::map<execplan::CalpontSystemCatalog::TableAliasName,
             std::tuple<std::string, SCToPosCounterMap, size_t>, TableAliasLessThan>;

class RBOptimizerContext
{
 public:
  RBOptimizerContext() = delete;
  RBOptimizerContext(cal_impl_if::gp_walk_info& walk_info, THD& thd, bool logRules,
                     uint cesOptimizationParallelFactor = 50)
   : gwi_(walk_info)
   , thd_(thd)
   , logRules_(logRules)
   , cesOptimizationParallelFactor_(cesOptimizationParallelFactor)
  {
  }

  // Accessors
  cal_impl_if::gp_walk_info& getGwi()
  {
    return gwi_;
  }
  THD& getThd()
  {
    return thd_;
  }
  uint64_t getUniqueId() const
  {
    return uniqueId_;
  }
  void incrementUniqueId()
  {
    ++uniqueId_;
  }
  uint32_t getMaxExpressionId() const
  {
    return maxExpressionId_;
  }
  void setMaxExpressionId(uint32_t exprId)
  {
    if (exprId > maxExpressionId_)
      maxExpressionId_ = exprId;
  }
  bool logRulesEnabled() const
  {
    return logRules_;
  }
  uint getCesOptimizationParallelFactor() const
  {
    return cesOptimizationParallelFactor_;
  }

  // Applied rules API
  void addAppliedRule(const std::string& name)
  {
    appliedRules_.push_back(name);
  }
  const std::vector<std::string>& getAppliedRules() const
  {
    return appliedRules_;
  }
  bool hasAppliedRules() const
  {
    return !appliedRules_.empty();
  }
  std::string serializeAppliedRules() const
  {
    std::string out;
    for (size_t i = 0; i < appliedRules_.size(); ++i)
    {
      if (i)
        out += ",";
      out += appliedRules_[i];
    }
    return out;
  }

  // Accumulated table alias mapping for parallel CES rule
  // This allows subqueries to see outer query's rewritten table aliases
  TableAliasToNewAliasAndSCPositionsMap& getAccumulatedTableAliasMap()
  {
    return accumulatedTableAliasMap_;
  }
  const TableAliasToNewAliasAndSCPositionsMap& getAccumulatedTableAliasMap() const
  {
    return accumulatedTableAliasMap_;
  }

 private:
  // gwi lifetime should be longer than optimizer context.
  // In plugin runtime this is always true.
  cal_impl_if::gp_walk_info& gwi_;
  THD& thd_;
  uint64_t uniqueId_{0};
  uint32_t maxExpressionId_{0};
  bool logRules_{false};
  uint cesOptimizationParallelFactor_;
  // Names of rules that were actually applied in order
  std::vector<std::string> appliedRules_;
  // Accumulated table alias mapping for parallel CES - allows subqueries to see outer query's mappings
  TableAliasToNewAliasAndSCPositionsMap accumulatedTableAliasMap_;
};

struct Rule
{
  // returns true if rule may be applied
  using RuleApplierFilter = bool (*)(execplan::CalpontSelectExecutionPlan&, RBOptimizerContext&);
  // returns true if rule was applied
  using RuleApplier = bool (*)(execplan::CalpontSelectExecutionPlan&, RBOptimizerContext&);

  Rule(std::string&& name, RuleApplierFilter mayApply, RuleApplier applyRule)
   : name(name), mayApply(mayApply), applyRule(applyRule) {};

  std::string name;
  RuleApplierFilter mayApply;
  RuleApplier applyRule;
  // TODO Wrap CSEP into Nodes to be able to navigate up and down the tree and remove this flag
  bool applyOnlyOnce = true;

  Rule() = default;
  Rule(const Rule&) = default;
  Rule(Rule&&) = default;

  std::string getName() const
  {
    return name;
  }

  Rule& operator=(const Rule&) = default;
  Rule& operator=(Rule&&) = default;

  bool apply(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& ctx) const;
  bool walk(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& ctx) const;
};

bool optimizeCSEP(execplan::CalpontSelectExecutionPlan& root, RBOptimizerContext& ctx,
                  bool useUnstableOptimizer);
std::string getRewrittenSubTableAlias(const execplan::CalpontSystemCatalog::TableAliasName& table,
                                      const RBOptimizerContext& ctx);

}  // namespace optimizer

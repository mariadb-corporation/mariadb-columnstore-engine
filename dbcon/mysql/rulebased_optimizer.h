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

#include <string>

#define PREFER_MY_CONFIG_H
#include <my_config.h>
#include "idb_mysql.h"

#include "ha_mcs_impl_if.h"

#include "execplan/calpontselectexecutionplan.h"

namespace optimizer {

class RBOptimizerContext {
public:
  RBOptimizerContext() = delete;
  RBOptimizerContext(cal_impl_if::gp_walk_info& walk_info) : gwi(walk_info) {}
  // gwi lifetime should be longer than optimizer context. 
  // In plugin runtime this is always true.
  cal_impl_if::gp_walk_info& gwi;
  uint64_t uniqueId {0};
};

struct Rule
{
  using RuleMatcher = bool (*)(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx);
  using RuleApplier = void (*)(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx);

  Rule(std::string&& name, RuleMatcher matchRule, RuleApplier applyRule)
   : name(name), matchRule(matchRule), applyRule(applyRule) {};

  std::string name;
  RuleMatcher matchRule;
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

bool optimizeCSEP(execplan::CalpontSelectExecutionPlan& root, RBOptimizerContext& ctx);
}
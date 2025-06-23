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
#include "execplan/calpontselectexecutionplan.h"

namespace optimizer {

struct Rule
{
  using RuleMatcher = bool (*)(execplan::CalpontSelectExecutionPlan&);
  using RuleApplier = void (*)(execplan::CalpontSelectExecutionPlan&);

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
  Rule& operator=(const Rule&) = default;
  Rule& operator=(Rule&&) = default;

  bool apply(execplan::CalpontSelectExecutionPlan& csep) const;
  bool walk(execplan::CalpontSelectExecutionPlan& csep) const;
};

bool matchParallelCES(execplan::CalpontSelectExecutionPlan& csep);
void applyParallelCES(execplan::CalpontSelectExecutionPlan& csep);
bool optimizeCSEP(execplan::CalpontSelectExecutionPlan& root);

}
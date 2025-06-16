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
  Rule(std::string&& name, bool (*matchRule)(execplan::CalpontSelectExecutionPlan&),
       void (*applyRule)(execplan::CalpontSelectExecutionPlan&))
   : name(name), matchRule(matchRule), applyRule(applyRule) {};

  std::string name;
  bool (*matchRule)(execplan::CalpontSelectExecutionPlan&);
  void (*applyRule)(execplan::CalpontSelectExecutionPlan&);
  bool apply(execplan::CalpontSelectExecutionPlan& csep) const;
  bool walk(execplan::CalpontSelectExecutionPlan& csep) const;
};

bool matchParallelCES(execplan::CalpontSelectExecutionPlan& csep);
void applyParallelCES(execplan::CalpontSelectExecutionPlan& csep);
bool optimizeCSEP(execplan::CalpontSelectExecutionPlan& root);

}
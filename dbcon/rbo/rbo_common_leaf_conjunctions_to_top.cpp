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

#include "rbo_common_leaf_conjunctions_to_top.h"

#include "execplan/calpontselectexecutionplan.h"
#include "execplan/parsetree.h"
#include "common_leaf_conjunctions.h"

namespace optimizer
{

bool commonLeafConjunctionsToTopFilter(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& /*ctx*/)
{
  return csep.filters() != nullptr;
}

bool applyCommonLeafConjunctionsToTop(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& /*ctx*/)
{
  execplan::ParseTree* original = csep.filters();
  if (original == nullptr)
    return false;

  execplan::ParseTree* rewritten = execplan::extractCommonLeafConjunctionsToRoot(original);
  if (rewritten == original)
    return false;

  csep.filters(rewritten);
  return true;
}

}  // namespace optimizer

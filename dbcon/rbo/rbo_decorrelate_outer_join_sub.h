/* Copyright (C) 2026 MariaDB Corporation

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

#define PREFER_MY_CONFIG_H
#include <my_config.h>
#include <dbcon/mysql/idb_mysql.h>
#include <dbcon/mysql/ha_mcs_impl_if.h>

#include "execplan/calpontselectexecutionplan.h"
#include "rulebased_optimizer.h"

namespace optimizer
{

// Rule entry points (MCOL-4250).
//
// Detects scalar-subquery filters (SelectFilter / SimpleScalarFilter) sitting
// inside an OuterJoinOnFilter in the plan and, where possible, rewrites them
// into an equivalent LEFT JOIN against a GROUP-BY derived table so the plan
// no longer needs runtime subquery evaluation inside the OUTER JOIN ON clause.
bool decorrelateOuterJoinSubFilter(execplan::CalpontSelectExecutionPlan& csep,
                                   optimizer::RBOptimizerContext& ctx);
bool applyDecorrelateOuterJoinSub(execplan::CalpontSelectExecutionPlan& csep,
                                  optimizer::RBOptimizerContext& ctx);

// Helper exported for the post-RBO validator in ha_mcs_execplan.cpp.
// Returns true if the given CSEP (any depth) still contains an
// OuterJoinOnFilter with a subquery filter inside (scalar SelectFilter /
// SimpleScalarFilter, or an ExistsFilter from IN / NOT IN / EXISTS / NOT
// EXISTS).  The executor cannot evaluate any of those shapes, and the
// decorrelate rule only rewrites the SelectFilter subset, so this validator
// preserves the pre-MCOL-4250 IDB-1015 contract for everything it leaves
// behind.
bool outerJoinOnContainsSubselect(const execplan::CalpontSelectExecutionPlan& csep);

}  // namespace optimizer

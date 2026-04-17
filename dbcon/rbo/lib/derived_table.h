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

// RBO primitive library: CSEP-level state transitions that configure a
// CalpontSelectExecutionPlan as (or to contain) a FROM-clause derived
// subquery.

#pragma once

#include <string>

#include "execplan/calpontselectexecutionplan.h"

namespace optimizer
{
namespace lib
{

// ---------------------------------------------------------------------------
// Flip the three flags that mark `csep` as a FROM-clause derived subquery:
//
//   csep->location(CalpontSelectExecutionPlan::FROM);
//   csep->subType(CalpontSelectExecutionPlan::FROM_SUBS);
//   csep->derivedTbAlias(alias);
//
// This triplet is applied in three rule files (rewrite_distinct,
// decorrelate_outer_join_sub, parallel_ces) before they hand the plan to
// their parent's derivedTableList.
// ---------------------------------------------------------------------------
void promoteCSEPToDerived(execplan::CalpontSelectExecutionPlan* csep, const std::string& alias);

// ---------------------------------------------------------------------------
// Rewrite `outer` so it represents a trivial SELECT over a single FROM-clause
// derived subquery whose body is `origCSEP`, exposed under `alias`.
//
// Concretely:
//   * origCSEP is promoted via promoteCSEPToDerived(origCSEP, alias);
//   * outer.subSelectList / subSelects / selectSubList / unionVec are cleared;
//   * outer.tableList is set to a single make_aliasview("", "", alias, "");
//   * outer.derivedTableList is set to { origCSEP };
//   * outer.distinct(false), outer.filters(nullptr), outer.having(nullptr);
//   * outer.returnedCols and outer.groupByCols are cleared (the caller is
//     expected to repopulate these from the derived projection).
//
// Ownership: `origCSEP` is moved into outer.derivedTableList() and outer
// takes sole responsibility for its lifetime.
// ---------------------------------------------------------------------------
void wrapCSEPAsDerived(execplan::CalpontSelectExecutionPlan& outer, execplan::SCSEP origCSEP,
                       const std::string& alias);

}  // namespace lib
}  // namespace optimizer

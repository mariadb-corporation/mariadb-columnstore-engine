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

#include "derived_table.h"

#include "execplan/calpontsystemcatalog.h"

namespace optimizer
{
namespace lib
{

void promoteCSEPToDerived(execplan::CalpontSelectExecutionPlan* csep, const std::string& alias)
{
  csep->location(execplan::CalpontSelectExecutionPlan::FROM);
  csep->subType(execplan::CalpontSelectExecutionPlan::FROM_SUBS);
  csep->derivedTbAlias(alias);
}

void wrapCSEPAsDerived(execplan::CalpontSelectExecutionPlan& outer, execplan::SCSEP origCSEP,
                       const std::string& alias)
{
  promoteCSEPToDerived(origCSEP.get(), alias);

  // Clear sub-plan containers on the outer CSEP.
  outer.subSelectList({});
  outer.subSelects({});
  outer.selectSubList({});
  outer.unionVec({});

  // Install the 1-entry tableList pointing at the derived-table alias.
  execplan::CalpontSelectExecutionPlan::TableList tblList;
  tblList.push_back(execplan::make_aliasview("", "", alias, ""));
  outer.tableList(tblList);

  // Install the 1-entry derivedTableList containing the moved-in plan.
  execplan::CalpontSelectExecutionPlan::SelectList derivedTblList;
  derivedTblList.emplace_back(std::move(origCSEP));
  outer.derivedTableList(derivedTblList);

  // Clear flags/fields that the derived wrap makes meaningless.
  outer.distinct(false);
  outer.filters(nullptr);
  outer.having(nullptr);

  // Projection and group-by are left empty; the caller repopulates them with
  // references to the derived projection (via lib::cloneAsSimpleColumn).
  outer.returnedCols({});
  outer.groupByCols({});
}

}  // namespace lib
}  // namespace optimizer

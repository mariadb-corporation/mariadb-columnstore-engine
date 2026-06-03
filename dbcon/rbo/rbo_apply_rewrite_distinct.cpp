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

#include "calpontselectexecutionplan.h"
#include "aggregatecolumn.h"
#include "simplecolumn.h"
#include "existsfilter.h"
#include "functioncolumn.h"
#include "lib/agg_wrap.h"
#include "lib/derived_column.h"
#include "lib/derived_table.h"
#include "logicoperator.h"

namespace optimizer
{

bool rewriteDistinctFilter(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& /*ctx*/)
{
  return csep.distinct() && csep.tableList().size() > 0;
}

bool applyRewriteDistinct(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& ctx)
{
  auto origCSEP = csep.clone();
  auto tableAlias = getRewrittenSubTableAlias(csep.tableList()[0], ctx);

  // Fully wrap csep around origCSEP: promotes origCSEP to FROM-subquery,
  // clears csep's sub*/union/filters/having/distinct and resets
  // returnedCols/groupByCols (repopulated below from origCSEP's projection).
  lib::wrapCSEPAsDerived(csep, origCSEP, tableAlias);
  int64_t colPos = 0;
  for (const auto& rc : origCSEP->returnedCols())
  {
    auto rcCloned = lib::cloneAsSimpleColumn(rc, tableAlias, colPos);
    csep.returnedCols().emplace_back(rcCloned);

    auto grpByCloned = lib::cloneAsSimpleColumn(rc, tableAlias, colPos);
    grpByCloned->orderPos(colPos);
    csep.groupByCols().emplace_back(grpByCloned);

    ++colPos;
  }

  // order by
  csep.orderByCols({});
  int64_t orderByColPos = 0;
  for (const auto& obc : origCSEP->orderByCols())
  {
    bool found = false;
    int64_t retColPos = 0;
    for (const auto& rc : origCSEP->returnedCols())
    {
      if (*obc == *rc)
      {
        // lucky me, order by column is in the result set
        found = true;
        execplan::SRCP outerRC;
        if (retColPos < colPos)
        {
          outerRC = csep.returnedCols()[retColPos];
        }
        else
        {
          outerRC = csep.orderByCols()[retColPos - colPos];
        }
        auto obcCloned = lib::cloneAsSimpleColumn(outerRC, tableAlias, retColPos);
        obcCloned->asc(obc->asc());
        obcCloned->nullsFirst(obc->nullsFirst());
        csep.orderByCols().emplace_back(obcCloned);
        break;
      }
      ++retColPos;
    }

    if (found)
    {
      continue;
    }

    // order by column is not in the result set of the original query, so add it to the resultset
    auto rc = boost::shared_ptr<execplan::ReturnedColumn>(obc->clone());
    origCSEP->returnedCols().emplace_back(rc);

    auto rcCloned = lib::cloneAsSimpleColumn(rc, tableAlias, colPos + orderByColPos);
    // This "order by" column does not belong to "group by" columns, so it
    // should be an aggregated column.  lib::wrapIntoSelectSomeAgg sets the
    // full column-level attribute set (alias/asc/charsetNumber/orderPos/
    // resultType/sessionID/timeZone/aggOp/aggParms) from `rcCloned`;
    // nullsFirst is an ORDER-BY-specific concept that the factory does not
    // touch, so we copy it from the source ORDER BY column here.
    auto* aggCol = lib::wrapIntoSelectSomeAgg(rcCloned, ctx.getGwi().timeZone);
    aggCol->nullsFirst(obc->nullsFirst());
    auto obcCloned = boost::shared_ptr<execplan::ReturnedColumn>(aggCol);

    csep.orderByCols().emplace_back(obcCloned);

    ++orderByColPos;
  }
  origCSEP->orderByCols().clear();
  origCSEP->distinct(false);

  return true;
}

}  // namespace optimizer

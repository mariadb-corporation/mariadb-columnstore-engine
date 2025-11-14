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
#include "logicoperator.h"

namespace optimizer
{

bool rewriteDistinctFilter(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& /*ctx*/)
{
  return csep.distinct() && csep.tableList().size() > 0;
}

execplan::SRCP cloneAsSimpleColumn(const execplan::SRCP& rc, const std::string& tableAlias, int64_t colPos)
{
  auto rcCloned = boost::make_shared<execplan::SimpleColumn>(*rc);
  // fill SimpleColumn data
  rcCloned->schemaName("");
  rcCloned->tableName(tableAlias);
  rcCloned->oid(0);
  rcCloned->tableAlias(tableAlias);
  rcCloned->data("");

  // fill ReturnedColumn data
  rcCloned->charsetNumber(rc->charsetNumber());

  // fill TreeNode data
  rcCloned->derivedTable(tableAlias);
  rcCloned->derivedRefCol(rc.get());
  rcCloned->resultType(rc->resultType());
  rcCloned->operationType(rc->operationType());
  rcCloned->colPosition(colPos);

  if (const auto* rcsc = dynamic_cast<execplan::SimpleColumn*>(rc.get()); rcsc != nullptr)
  {
    rcCloned->timeZone(rcsc->timeZone());
  }
  else if (const auto* rcfc = dynamic_cast<execplan::FunctionColumn*>(rc.get()))
  {
    rcCloned->timeZone(rcfc->timeZone());
  }
  else if (const auto* rcac = dynamic_cast<execplan::AggregateColumn*>(rc.get()))
  {
    rcCloned->timeZone(rcac->timeZone());
  }
  else if (const auto* rcwc = dynamic_cast<execplan::WindowFunctionColumn*>(rc.get()))
  {
    rcCloned->timeZone(rcwc->timeZone());
  }
  rc->incRefCount();

  auto colName = getSimpleColumnAlias(*rc, colPos);
  rcCloned->columnName(colName);
  rcCloned->alias("`" + tableAlias + "`." + colName);
  rcCloned->colSource(0);

  return rcCloned;
}

bool applyRewriteDistinct(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& ctx)
{
  auto origCSEP = csep.clone();
  auto tableAlias = getRewrittenSubTableAlias(csep.tableList()[0], ctx);
  origCSEP->location(execplan::CalpontSelectExecutionPlan::FROM);
  origCSEP->subType(execplan::CalpontSelectExecutionPlan::FROM_SUBS);
  origCSEP->derivedTbAlias(tableAlias);

  csep.subSelectList({});
  csep.subSelects({});
  csep.selectSubList({});
  csep.unionVec({});

  execplan::CalpontSelectExecutionPlan::TableList tblList;
  tblList.push_back(execplan::make_aliasview("", "", tableAlias, ""));
  csep.tableList(tblList);
  execplan::CalpontSelectExecutionPlan::SelectList derivedTblList;
  derivedTblList.emplace_back(origCSEP);
  csep.derivedTableList(derivedTblList);

  csep.distinct(false);
  csep.filters(nullptr);
  csep.having(nullptr);

  csep.returnedCols({});
  csep.groupByCols({});
  int64_t colPos = 0;
  for (const auto& rc : origCSEP->returnedCols())
  {
    auto rcCloned = cloneAsSimpleColumn(rc, tableAlias, colPos);
    csep.returnedCols().emplace_back(rcCloned);

    auto grpByCloned = cloneAsSimpleColumn(rc, tableAlias, colPos);
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
        auto obcCloned = cloneAsSimpleColumn(outerRC, tableAlias, retColPos);
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

    auto rcCloned = cloneAsSimpleColumn(rc, tableAlias, colPos + orderByColPos);
    //This "order by" column does not belong to "group by" columns, so it should be an aggregated column
    auto* aggCol = new execplan::AggregateColumn();
    auto obcCloned = boost::shared_ptr<execplan::ReturnedColumn>(aggCol);

    aggCol->asc(obc->asc());
    aggCol->nullsFirst(obc->nullsFirst());
    aggCol->aggOp(execplan::AggregateColumn::SELECT_SOME);
    aggCol->aggParms().emplace_back(rcCloned);

    csep.orderByCols().emplace_back(obcCloned);

    ++orderByColPos;
  }
  origCSEP->orderByCols().clear();
  origCSEP->distinct(false);

  return true;
}

}  // namespace optimizer

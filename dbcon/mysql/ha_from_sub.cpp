/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation

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

/***********************************************************************
 *   $Id: ha_from_sub.cpp 6377 2010-03-22 20:18:47Z zzhu $
 *
 *
 ***********************************************************************/
/** @file */
/** class FromSubSelect definition */

// #define NDEBUG
#define PREFER_MY_CONFIG_H
#include <my_config.h>
#include <cassert>
#include <map>

#include "idb_mysql.h"

#include "parsetree.h"
#include "simplefilter.h"
#include "logicoperator.h"
#include "constantcolumn.h"
#include "simplecolumn.h"
using namespace execplan;
using namespace std;
#include "ha_subquery.h"

namespace cal_impl_if
{
void derivedTableOptimization(gp_walk_info* gwip, SCSEP& csep)
{
  // @bug5634. replace the unused column with ConstantColumn from derived table column list,
  // ExeMgr will not project ConstantColumn. Only count for local derived column.
  // subquery may carry main query derived table list for column reference, those
  // derived tables are not checked for optimization in this scope.
  CalpontSelectExecutionPlan::SelectList derivedTbList = csep->derivedTableList();

  for (uint i = 0; i < derivedTbList.size(); i++)
  {
    CalpontSelectExecutionPlan* plan = reinterpret_cast<CalpontSelectExecutionPlan*>(derivedTbList[i].get());
    CalpontSelectExecutionPlan::ReturnedColumnList cols = plan->returnedCols();
    vector<CalpontSelectExecutionPlan::ReturnedColumnList> unionColVec;

    // only do vertical optimization for union all
    // @bug6134. Also skip the vertical optimization for select distinct
    // because all columns need to be projected to check the distinctness.
    bool verticalOptimization = false;

    if (plan->distinctUnionNum() == 0 && !plan->distinct())
    {
      verticalOptimization = true;

      for (uint j = 0; j < plan->unionVec().size(); j++)
      {
        unionColVec.push_back(
            reinterpret_cast<CalpontSelectExecutionPlan*>(plan->unionVec()[j].get())->returnedCols());
      }
    }

    if (verticalOptimization)
    {
      int64_t val = 1;

      // TODO MCOL-4543 Only project those columns from the subquery
      // which are referenced in the outer select. So for example,
      // if a table t contains 10 columns c1 ... c10 :
      // "select count(c2) from (select * from t) q;"
      // with p being the subquery execution plan, p->columnMap()
      // and p->returnedCols() should both be of size 1, instead
      // of 10, with entries for c2 in each.
      //
      // We are currently performing a dumb optimization:
      // Instead of just referencing c2, we are referencing (c1,c2)
      // for the above query. This is due to complexity associated
      // with modifying ReturnedColumn::colPosition()
      // (from a value of 1 to a value of 0) of the outer query
      // which references c2. So essentially, if c2 is replaced by c10
      // in the above query, we fallback to projecting all 10 columns
      // of the subquery in ExeMgr.
      // This will be addressed in future.
      CalpontSelectExecutionPlan::ReturnedColumnList nonConstCols;
      vector<CalpontSelectExecutionPlan::ReturnedColumnList> nonConstUnionColVec(unionColVec.size());

      int64_t lastNonConstIndex = -1;

      for (int64_t i = cols.size() - 1; i >= 0; i--)
      {
        if (cols[i]->refCount() == 0)
        {
          if (cols[i]->derivedRefCol())
            cols[i]->derivedRefCol()->decRefCount();

          if (lastNonConstIndex == -1)
          {
            SimpleColumn* sc = dynamic_cast<SimpleColumn*>(cols[i].get());

            if (sc && (plan->columnMap().count(sc->columnName()) == 1))
            {
              plan->columnMap().erase(sc->columnName());
            }
          }
          else
          {
            cols[i].reset(new ConstantColumn(val));
            (reinterpret_cast<ConstantColumn*>(cols[i].get()))->timeZone(gwip->timeZone);
          }

          for (uint j = 0; j < unionColVec.size(); j++)
          {
            if (lastNonConstIndex == -1)
            {
              CalpontSelectExecutionPlan* unionSubPlan =
                  reinterpret_cast<CalpontSelectExecutionPlan*>(plan->unionVec()[j].get());

              SimpleColumn* sc = dynamic_cast<SimpleColumn*>(unionSubPlan->returnedCols()[i].get());

              if (sc && (unionSubPlan->columnMap().count(sc->columnName()) == 1))
              {
                unionSubPlan->columnMap().erase(sc->columnName());
              }
            }
            else
            {
              unionColVec[j][i].reset(new ConstantColumn(val));
              (reinterpret_cast<ConstantColumn*>(unionColVec[j][i].get()))->timeZone(gwip->timeZone);
            }
          }
        }
        else if (lastNonConstIndex == -1)
        {
          lastNonConstIndex = i;
        }
      }

      if (lastNonConstIndex == -1)
      {
        // None of the subquery columns are referenced, just use the first one
        if (!cols.empty())
        {
          cols[0].reset(new ConstantColumn(val));
          (reinterpret_cast<ConstantColumn*>(cols[0].get()))->timeZone(gwip->timeZone);
          nonConstCols.push_back(cols[0]);

          for (uint j = 0; j < unionColVec.size(); j++)
          {
            unionColVec[j][0].reset(new ConstantColumn(val));
            (reinterpret_cast<ConstantColumn*>(unionColVec[j][0].get()))->timeZone(gwip->timeZone);
            nonConstUnionColVec[j].push_back(unionColVec[j][0]);
          }
        }
      }
      else
      {
        nonConstCols.assign(cols.begin(), cols.begin() + lastNonConstIndex + 1);

        for (uint j = 0; j < unionColVec.size(); j++)
        {
          nonConstUnionColVec[j].assign(unionColVec[j].begin(),
                                        unionColVec[j].begin() + lastNonConstIndex + 1);
        }
      }

      // set back
      plan->returnedCols(nonConstCols);

      for (uint j = 0; j < unionColVec.size(); j++)
      {
        CalpontSelectExecutionPlan* unionSubPlan =
            reinterpret_cast<CalpontSelectExecutionPlan*>(plan->unionVec()[j].get());
        unionSubPlan->returnedCols(nonConstUnionColVec[j]);
      }
    }
  }
  // recursively process the nested derived table
  for (uint i = 0; i < csep->subSelectList().size(); i++)
  {
    SCSEP subselect(boost::dynamic_pointer_cast<CalpontSelectExecutionPlan>(csep->subSelectList()[i]));
    derivedTableOptimization(gwip, subselect);
  }
}

FromSubQuery::FromSubQuery(gp_walk_info& gwip) : SubQuery(gwip)
{
}

FromSubQuery::FromSubQuery(gp_walk_info& gwip, SELECT_LEX* sub) : SubQuery(gwip), fFromSub(sub)
{
}

FromSubQuery::~FromSubQuery()
{
}

SCSEP FromSubQuery::transform()
{
  assert(fFromSub);
  SCSEP csep(new CalpontSelectExecutionPlan());
  csep->sessionID(fGwip.sessionid);
  csep->location(CalpontSelectExecutionPlan::FROM);
  csep->subType(CalpontSelectExecutionPlan::FROM_SUBS);

  // gwi for the sub query
  gp_walk_info gwi(fGwip.timeZone, fGwip.subQueriesChain);
  gwi.thd = fGwip.thd;
  gwi.subQuery = this;
  gwi.viewName = fGwip.viewName;
  csep->derivedTbAlias(fAlias);  // always lower case
  csep->derivedTbView(fGwip.viewName.alias, lower_case_table_names);

  if (getSelectPlan(gwi, *fFromSub, csep, false) != 0)
  {
    fGwip.fatalParseError = true;

    if (!gwi.parseErrorText.empty())
      fGwip.parseErrorText = gwi.parseErrorText;
    else
      fGwip.parseErrorText = "Error occurred in FromSubQuery::transform()";

    csep.reset();
    return csep;
  }

  // Insert column statistics
  fGwip.mergeTableStatistics(gwi.tableStatisticsMap);

  fGwip.subselectList.push_back(csep);
  return csep;
}

}  // namespace cal_impl_if

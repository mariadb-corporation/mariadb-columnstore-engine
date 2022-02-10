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

//#define NDEBUG
#define PREFER_MY_CONFIG_H
#include <my_config.h>
#include <cassert>
#include <map>
using namespace std;

#include "idb_mysql.h"

#include "parsetree.h"
#include "simplefilter.h"
#include "logicoperator.h"
#include "constantcolumn.h"
#include "simplecolumn.h"
using namespace execplan;

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

  // @bug6156. Skip horizontal optimization for no table union.
  bool horizontalOptimization = true;

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

    if (plan->tableList().empty())
      horizontalOptimization = false;

    for (uint j = 0; j < plan->unionVec().size(); j++)
    {
      if (reinterpret_cast<CalpontSelectExecutionPlan*>(plan->unionVec()[j].get())->tableList().empty())
      {
        horizontalOptimization = false;
        break;
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
        // if (cols[i]->derivedTable().empty())
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

  /*
   * @bug5635. Move filters that only belongs to a derived table to inside the derived table.
   * 1. parse tree walk to populate derivedTableFilterMap and set null candidate on the tree.
   * 2. remove the null filters
   * 3. and the filters of derivedTableFilterMap and append to the WHERE filter of the derived table
   *
   * Note:
   * 1. Subquery filters is ignored because derived table can not be in subquery
   * 2. While walking tree, whenever a single derive simplefilter is encountered,
   * this filter is pushed to the corresponding stack
   * 2. Whenever an OR operator is encountered, all the filter stack of
   * that OR involving derived table are emptied and null candidate of each
   * stacked filter needs to be reset (not null)
   */
  ParseTree* pt = csep->filters();
  map<string, ParseTree*> derivedTbFilterMap;

  if (horizontalOptimization && pt)
  {
    pt->walk(setDerivedTable);
    setDerivedFilter(gwip, pt, derivedTbFilterMap, derivedTbList);
    csep->filters(pt);
  }

  // AND the filters of individual stack to the derived table filter tree
  // @todo union filters.
  // @todo outer join complication
  map<string, ParseTree*>::iterator mapIt;

  for (uint i = 0; i < derivedTbList.size(); i++)
  {
    CalpontSelectExecutionPlan* plan = reinterpret_cast<CalpontSelectExecutionPlan*>(derivedTbList[i].get());
    CalpontSelectExecutionPlan::ReturnedColumnList derivedColList = plan->returnedCols();
    mapIt = derivedTbFilterMap.find(plan->derivedTbAlias());

    if (mapIt != derivedTbFilterMap.end())
    {
      // replace all derived column of this filter with real column from
      // derived table projection list.
      ParseTree* mainFilter = new ParseTree();
      mainFilter->copyTree(*(mapIt->second));
      replaceRefCol(mainFilter, derivedColList);
      ParseTree* derivedFilter = plan->filters();

      if (derivedFilter)
      {
        LogicOperator* op = new LogicOperator("and");
        ParseTree* filter = new ParseTree(op);
        filter->left(derivedFilter);
        filter->right(mainFilter);
        plan->filters(filter);
      }
      else
      {
        plan->filters(mainFilter);
      }

      // union filter handling
      for (uint j = 0; j < plan->unionVec().size(); j++)
      {
        CalpontSelectExecutionPlan* unionPlan =
            reinterpret_cast<CalpontSelectExecutionPlan*>(plan->unionVec()[j].get());
        CalpontSelectExecutionPlan::ReturnedColumnList unionColList = unionPlan->returnedCols();
        ParseTree* mainFilterForUnion = new ParseTree();
        mainFilterForUnion->copyTree(*(mapIt->second));
        replaceRefCol(mainFilterForUnion, unionColList);
        ParseTree* unionFilter = unionPlan->filters();

        if (unionFilter)
        {
          LogicOperator* op = new LogicOperator("and");
          ParseTree* filter = new ParseTree(op);
          filter->left(unionFilter);
          filter->right(mainFilterForUnion);
          unionPlan->filters(filter);
        }
        else
        {
          unionPlan->filters(mainFilterForUnion);
        }
      }
    }
  }

  // clean derivedTbFilterMap because all the filters are copied
  for (mapIt = derivedTbFilterMap.begin(); mapIt != derivedTbFilterMap.end(); ++mapIt)
    delete (*mapIt).second;

  // recursively process the nested derived table
  for (uint i = 0; i < csep->subSelectList().size(); i++)
  {
    SCSEP subselect(boost::dynamic_pointer_cast<CalpontSelectExecutionPlan>(csep->subSelectList()[i]));
    derivedTableOptimization(gwip, subselect);
  }
}

void setDerivedTable(execplan::ParseTree* n)
{
  ParseTree* lhs = n->left();
  ParseTree* rhs = n->right();

  Operator* op = dynamic_cast<Operator*>(n->data());

  // if logic operator then lhs and rhs can't be both null
  if (op)
  {
    if (!lhs || lhs->derivedTable() == "*")
    {
      n->derivedTable(rhs ? rhs->derivedTable() : "*");
    }
    else if (!rhs || rhs->derivedTable() == "*")
    {
      n->derivedTable(lhs->derivedTable());
    }
    else if (lhs->derivedTable() == rhs->derivedTable())
    {
      n->derivedTable(lhs->derivedTable());
    }
    else
    {
      n->derivedTable("");
    }
  }
  else
  {
    n->data()->setDerivedTable();
    n->derivedTable(n->data()->derivedTable());
  }
}

ParseTree* setDerivedFilter(gp_walk_info* gwip, ParseTree*& n, map<string, ParseTree*>& filterMap,
                            CalpontSelectExecutionPlan::SelectList& derivedTbList)
{
  if (!(n->derivedTable().empty()))
  {
    // @todo replace virtual column of n to real column
    // all simple columns should belong to the same derived table
    CalpontSelectExecutionPlan* csep = NULL;

    for (uint i = 0; i < derivedTbList.size(); i++)
    {
      CalpontSelectExecutionPlan* plan = dynamic_cast<CalpontSelectExecutionPlan*>(derivedTbList[i].get());

      if (plan->derivedTbAlias() == n->derivedTable())
      {
        csep = plan;
        break;
      }
    }

    // should never be null; if null then give up optimization.
    if (!csep)
      return n;

    // 2. push the filter to the derived table filter stack, or 'and' with
    // the filters in the stack
    map<string, ParseTree*>::iterator mapIter = filterMap.find(n->derivedTable());

    if (mapIter == filterMap.end())
    {
      filterMap.insert(pair<string, ParseTree*>(n->derivedTable(), n));
    }
    else
    {
      ParseTree* pt = new ParseTree(new LogicOperator("and"));
      pt->left(mapIter->second);
      pt->right(n);
      mapIter->second = pt;
    }

    int64_t val = 1;
    n = new ParseTree(new ConstantColumn(val));
    (dynamic_cast<ConstantColumn*>(n->data()))->timeZone(gwip->timeZone);
  }
  else
  {
    Operator* op = dynamic_cast<Operator*>(n->data());

    if (op && (op->op() == OP_OR || op->op() == OP_XOR))
    {
      return n;
    }
    else
    {
      ParseTree* lhs = n->left();
      ParseTree* rhs = n->right();

      if (lhs)
        n->left(setDerivedFilter(gwip, lhs, filterMap, derivedTbList));

      if (rhs)
        n->right(setDerivedFilter(gwip, rhs, filterMap, derivedTbList));
    }
  }

  return n;
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
  gp_walk_info gwi(fGwip.timeZone);
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
      fGwip.parseErrorText = "Error occured in FromSubQuery::transform()";

    csep.reset();
    return csep;
  }

  fGwip.subselectList.push_back(csep);
  return csep;
}

}  // namespace cal_impl_if

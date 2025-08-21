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

#include <cstddef>
#include <cstdint>

#include "rulebased_optimizer.h"

#include "constantcolumn.h"
#include "execplan/calpontselectexecutionplan.h"
#include "execplan/simplecolumn.h"
#include "logicoperator.h"
#include "operator.h"

namespace optimizer
{

   using DerivedToFiltersMap = std::map<std::string, execplan::ParseTree*>;

bool predicatePushdownFilter(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& /*ctx*/)
{
  // The original rule match contains questionable decision to filter out
  // queries that contains any UNION UNIT with only derived tables.
  // See ha_from_sub.cpp before MCS 23.10.7 for more details and @bug6156.
  // All tables are derived thus nothing to optimize.
  return !csep.tableList().empty();
}


void setDerivedTable(execplan::ParseTree* n)
{
  execplan::ParseTree* lhs = n->left();
  execplan::ParseTree* rhs = n->right();

  execplan::Operator* op = dynamic_cast<execplan::Operator*>(n->data());

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

execplan::ParseTree* setDerivedFilter(cal_impl_if::gp_walk_info* gwip, execplan::ParseTree*& n, DerivedToFiltersMap& filterMap,
                            const execplan::CalpontSelectExecutionPlan::SelectList& derivedTbList)
{
  if (!(n->derivedTable().empty()))
  {
    // @todo replace virtual column of n to real column
    // all simple columns should belong to the same derived table
    execplan::CalpontSelectExecutionPlan* csep = NULL;

    for (uint i = 0; i < derivedTbList.size(); i++)
    {
      execplan::CalpontSelectExecutionPlan* plan = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(derivedTbList[i].get());

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
    auto mapIter = filterMap.find(n->derivedTable());

    if (mapIter == filterMap.end())
    {
      filterMap.insert({n->derivedTable(), n});
    }
    else
    {
      execplan::ParseTree* pt = new execplan::ParseTree(new execplan::LogicOperator("and"));
      pt->left(mapIter->second);
      pt->right(n);
      mapIter->second = pt;
    }

    int64_t val = 1;
    n = new execplan::ParseTree(new execplan::ConstantColumn(val));
    (dynamic_cast<execplan::ConstantColumn*>(n->data()))->timeZone(gwip->timeZone);
  }
  else
  {
    execplan::Operator* op = dynamic_cast<execplan::Operator*>(n->data());

    if (op && (op->op() == execplan::OP_OR || op->op() == execplan::OP_XOR))
    {
      return n;
    }
    else
    {
      execplan::ParseTree* lhs = n->left();
      execplan::ParseTree* rhs = n->right();

      if (lhs)
        n->left(optimizer::setDerivedFilter(gwip, lhs, filterMap, derivedTbList));

      if (rhs)
        n->right(optimizer::setDerivedFilter(gwip, rhs, filterMap, derivedTbList));
    }
  }

  return n;
}

bool applyPredicatePushdown(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& ctx)
{
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
   execplan::ParseTree* pt = csep.filters();
   DerivedToFiltersMap derivedTbFilterMap;
   auto& derivedTbList = csep.derivedTableList();
   bool hasBeenApplied = false;

   if (pt)
   {
     pt->walk(setDerivedTable);
     setDerivedFilter(&ctx.gwi, pt, derivedTbFilterMap, derivedTbList);
     csep.filters(pt);
   }
 
   // AND the filters of individual stack to the derived table filter tree
   // @todo union filters.
   // @todo outer join complication
   for (uint i = 0; i < derivedTbList.size(); i++)
   {
     execplan::CalpontSelectExecutionPlan* plan = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(derivedTbList[i].get());
     execplan::CalpontSelectExecutionPlan::ReturnedColumnList derivedColList = plan->returnedCols();
     auto mapIt = derivedTbFilterMap.find(plan->derivedTbAlias());
 
     if (mapIt != derivedTbFilterMap.end())
     {
       // replace all derived column of this filter with real column from
       // derived table projection list.
       execplan::ParseTree* mainFilter = new execplan::ParseTree();
       mainFilter->copyTree(*(mapIt->second));
       replaceRefCol(mainFilter, derivedColList);
       execplan::ParseTree* derivedFilter = plan->filters();
 
       if (derivedFilter)
       {
         execplan::LogicOperator* op = new execplan::LogicOperator("and");
         execplan::ParseTree* filter = new execplan::ParseTree(op);
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
         execplan::CalpontSelectExecutionPlan* unionPlan =
             reinterpret_cast<execplan::CalpontSelectExecutionPlan*>(plan->unionVec()[j].get());
         execplan::CalpontSelectExecutionPlan::ReturnedColumnList unionColList = unionPlan->returnedCols();
         execplan::ParseTree* mainFilterForUnion = new execplan::ParseTree();
         mainFilterForUnion->copyTree(*(mapIt->second));
         replaceRefCol(mainFilterForUnion, unionColList);
         execplan::ParseTree* unionFilter = unionPlan->filters();
 
         if (unionFilter)
         {
           execplan::LogicOperator* op = new execplan::LogicOperator("and");
           execplan::ParseTree* filter = new execplan::ParseTree(op);
           filter->left(unionFilter);
           filter->right(mainFilterForUnion);
           unionPlan->filters(filter);
         }
         else
         {
           unionPlan->filters(mainFilterForUnion);
         }
       }
       hasBeenApplied = true;
     }
   }
 
   // clean derivedTbFilterMap because all the filters are copied
   for (auto mapIt = derivedTbFilterMap.begin(); mapIt != derivedTbFilterMap.end(); ++mapIt)
   {
    delete (*mapIt).second;
   }

  return hasBeenApplied;
}

}  // namespace optimizer

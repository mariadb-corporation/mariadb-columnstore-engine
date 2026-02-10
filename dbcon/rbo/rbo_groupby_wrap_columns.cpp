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

#include <sstream>
#include <stack>
#include <variant>
#include "rulebased_optimizer.h"

#include "calpontselectexecutionplan.h"
#include "aggregatecolumn.h"
#include "arithmeticcolumn.h"
#include "arithmeticoperator.h"
#include "constantcolumn.h"
#include "simplecolumn.h"
#include "simplefilter.h"
#include "existsfilter.h"
#include "functioncolumn.h"
#include "logicoperator.h"

namespace optimizer
{

namespace
{


struct ColumnWrapperContext
{
  ColumnWrapperContext(
      const std::vector<execplan::SRCP>* gbcols,
      const execplan::CalpontSelectExecutionPlan::TableList& tablelist)
    : gbCols(gbcols)
    , tableList(tablelist)
  {}

  const std::vector<execplan::SRCP>* gbCols;
  const execplan::CalpontSelectExecutionPlan::TableList& tableList;
  std::vector<std::pair<execplan::AggregateColumn*, uint32_t>> aggExprs;
  uint32_t nextId{0};
  bool applied{false};
  static std::vector<execplan::SRCP> emptySRCPVec;
};

std::vector<execplan::SRCP> ColumnWrapperContext::emptySRCPVec;

bool isAggregateColumn(const std::variant<execplan::ParseTree*, execplan::TreeNode*>& col,
                       RBOptimizerContext& ctx)
{
  bool ret = false;
  std::vector<std::variant<execplan::ParseTree*, execplan::TreeNode*>> stack;
  stack.emplace_back(col);
  while (!stack.empty())
  {
    auto node = stack.back();
    stack.pop_back();
    if (auto* ptp = std::get_if<execplan::ParseTree*>(&node))
    {
      auto* pt = *ptp;
      if (pt->left() != nullptr)
        stack.emplace_back(pt->left());
      if (pt->right() != nullptr)
        stack.emplace_back(pt->right());
      stack.emplace_back(pt->data());
    }
    else
    {
      auto* tn = std::get<execplan::TreeNode*>(node);
      if (auto* rc = dynamic_cast<execplan::ReturnedColumn*>(tn))
      {
        if (rc->expressionId() != -1u)
          ctx.setMaxExpressionId(rc->expressionId());
      }

      if (auto* agc = dynamic_cast<execplan::AggregateColumn*>(tn))
      {
        ret = true;
        for (auto& arg : agc->aggParms())
        {
          stack.emplace_back(arg.get());
        }
      }
      else if (auto* ac = dynamic_cast<execplan::ArithmeticColumn*>(tn))
      {
        stack.emplace_back(ac->expression());
      }
      else if (auto* fc = dynamic_cast<execplan::FunctionColumn*>(tn))
      {
        for (auto& arg : fc->functionParms())
        {
          stack.emplace_back(arg.get());
        }
      }
      else if (auto* sf = dynamic_cast<execplan::SimpleFilter*>(tn))
      {
        if (sf->lhs() != nullptr)
          stack.emplace_back(sf->lhs());
        if (sf->rhs() != nullptr)
          stack.emplace_back(sf->rhs());
      }
      else if (auto* wf = dynamic_cast<execplan::WindowFunctionColumn*>(tn))
      {
        for (auto& arg : wf->functionParms())
        {
          stack.emplace_back(arg.get());
        }
        for (auto& part : wf->partitions())
        {
          stack.emplace_back(part.get());
        }
      }
    }
  }
  return ret;
}

bool hasAggregateColumns(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& ctx)
{
  bool ret = false;
  for (const auto* cols : {&csep.returnedCols(), &csep.orderByCols()})
  {
    for (const auto& col : *cols)
    {
      if (isAggregateColumn(col.get(), ctx))
        ret = true;
    }
  }
  if (csep.having())
    ret |= isAggregateColumn(csep.having(), ctx);
  return ret;
}

bool needWrap(execplan::TreeNode* tn, ColumnWrapperContext& lctx)
{
  if (dynamic_cast<execplan::AggregateColumn*>(tn) != nullptr ||
    dynamic_cast<execplan::ConstantColumn*>(tn) != nullptr)
  {
    return false;
  }

  if (auto* sc = dynamic_cast<const execplan::SimpleColumn*>(tn))
  {
    bool ourTable = false;
    for (const auto& tbl : lctx.tableList)
    {
      if (!tbl.isColumnstore())
      {
        continue;
      }
      if (tbl.schema == sc->schemaName() &&
          (tbl.table == sc->tableName() || (tbl.table.empty() && tbl.alias == sc->tableName())) &&
          tbl.alias == sc->tableAlias())
      {
        ourTable = true;
        break;
      }
    }
    if (!ourTable)
    {
      return false;
    }
  }

  if (dynamic_cast<execplan::SimpleFilter*>(tn))
    return true;

  auto* rc = dynamic_cast<execplan::ReturnedColumn*>(tn);
  if (!rc)
  {
    return false;
  }

  bool asc = rc->asc();
  rc->asc(true);
  bool ret = true;
  for (const auto& gbCol : *lctx.gbCols)
  {
    // FIXME
    if (*rc == *gbCol)
    {
      ret = false;
      break;
    }
  }
  rc->asc(asc);
  return ret;
}

template <typename T>
execplan::AggregateColumn* wrapColumn(const T& rc, ColumnWrapperContext& lctx, RBOptimizerContext& ctx)
{
  auto ac = std::make_unique<execplan::AggregateColumn>(rc->sessionID());
  ac->timeZone(ctx.getGwi().timeZone);
  ac->alias(rc->alias());
  ac->aggOp(execplan::AggregateColumn::SELECT_SOME);
  ac->asc(rc->asc());
  ac->charsetNumber(rc->charsetNumber());
  ac->orderPos(rc->orderPos());
  ac->aggParms().emplace_back(rc);
  ac->resultType(rc->resultType());

  size_t i;
  for (i = 0; i < lctx.aggExprs.size(); i++)
  {
    if (*ac == *lctx.aggExprs[i].first)
      break;
  }
  if (i < lctx.aggExprs.size())
  {
    ac->expressionId(lctx.aggExprs[i].second);
  }
  else
  {
    ac->expressionId(lctx.nextId);
    lctx.aggExprs.emplace_back(ac.get(), lctx.nextId++);
  }
  lctx.applied = true;
  return ac.release();
}

struct Stack
{
  using FrameType = std::variant<execplan::ParseTree*, execplan::TreeNode*>;
  struct Frame
  {
    FrameType node;
    size_t step;
  };
  std::vector<Frame> frames;
  std::vector<bool> results;
  std::vector<const std::vector<execplan::SRCP>*> gbCols;
};

bool processColumn(const Stack::FrameType& rc, ColumnWrapperContext& lctx, RBOptimizerContext& ctx)
{
  Stack stack;
  stack.frames.push_back(Stack::Frame{rc, 0});
  while (!stack.frames.empty())
  {
    auto& [node, step] = stack.frames.back();
    if (auto* parseTreePtrPtr = std::get_if<execplan::ParseTree*>(&node))
    {
      auto* pt = *parseTreePtrPtr;
      if (pt->left() == nullptr && pt->right() == nullptr)
      {
        // The only way when data() is a column is than it is a "leaf"
        if (step == 0)
        {
          step++;
          stack.frames.push_back(Stack::Frame{pt->data(), 0});
          continue;
        }
        auto wrap = stack.results.back();
        stack.results.pop_back();
        if (wrap)
        {
          auto* col = dynamic_cast<execplan::ReturnedColumn*>(pt->data());
          idbassert(col != nullptr);
          // replace fData with the wrapped one
          pt->data(wrapColumn(col, lctx, ctx));
        }
        stack.frames.pop_back();
        stack.results.push_back(false);
        continue;
      }

      // walk to the left
      if (step == 0)
      {
        step++;
        stack.frames.push_back(Stack::Frame{pt->left(), 0});
        continue;
      }
      // walk to the right
      if (step == 1)
      {
        step++;
        stack.results.pop_back();
        stack.frames.push_back(Stack::Frame{pt->right(), 0});
        continue;
      }
      // we are not interested in the results because the wrapping is performed in "leaf" processing above
      stack.frames.pop_back();
      stack.results.back() = false;
    }
    else
    {
      auto* tn = std::get<execplan::TreeNode*>(node);
      if (!needWrap(tn, lctx))
      {
        stack.frames.pop_back();
        stack.results.push_back(false);
        continue;
      }
      if (dynamic_cast<execplan::AggregateColumn*>(tn))
      {
        // there is nothing to do with the aggregates subtrees, so just return
        stack.frames.pop_back();
        stack.results.push_back(false);
        continue;
      }
      if (auto* sc = dynamic_cast<execplan::SimpleColumn*>(tn))
      {
        // mark column for wrapping if needed, real wrapping will be performed upper by stack
        stack.frames.pop_back();
        stack.results.push_back(needWrap(sc, lctx));
        continue;
      }
      if (auto* ac = dynamic_cast<execplan::ArithmeticColumn*>(tn))
      {
        stack.frames.pop_back();
        stack.frames.push_back(Stack::Frame{ac->expression(), 0});
        continue;
      }
      if (auto* fc = dynamic_cast<execplan::FunctionColumn*>(tn))
      {
        if (step > 0)
        {
          // prev parameter has been processed, wrap it if needed
          bool wrap = stack.results.back();
          stack.results.pop_back();
          if (wrap)
          {
            auto* col = dynamic_cast<execplan::ReturnedColumn*>(fc->functionParms()[step - 1]->data());
            idbassert(col != nullptr);
            fc->functionParms()[step - 1]->data(wrapColumn(col, lctx, ctx));
          }
        }
        if (step < fc->functionParms().size())
        {
          if (step == 0)
          {
            stack.gbCols.push_back(lctx.gbCols);
          }
          lctx.gbCols = &lctx.emptySRCPVec;
          // there are some params left, push the next one
          stack.frames.push_back(Stack::Frame{fc->functionParms()[step++].get(), 0});
        }
        else
        {
          lctx.gbCols = stack.gbCols.back();
          stack.gbCols.pop_back();
          stack.frames.pop_back();
          stack.results.push_back(false);
        }
        continue;
      }
      if (auto* sf = dynamic_cast<execplan::SimpleFilter*>(tn))
      {
        // left part of the filter
        if (step == 0)
        {
          step++;
          stack.frames.push_back(Stack::Frame{sf->lhs(), 0});
          continue;
        }
        // left part is done, wrap it if needed and go to the right
        if (step == 1)
        {
          step++;
          bool wrap = stack.results.back();
          stack.results.pop_back();
          if (wrap)
          {
            sf->lhs(wrapColumn(sf->lhs(), lctx, ctx));
          }
          stack.frames.push_back(Stack::Frame{sf->rhs(), 0});
          continue;
        }
        // wrap rhs if needed and return
        bool wrap = stack.results.back();
        stack.results.pop_back();
        if (wrap)
        {
          sf->rhs(wrapColumn(sf->rhs(), lctx, ctx));
        }
        stack.frames.pop_back();
        stack.results.push_back(false);
        continue;
      }
      if (auto* wf = dynamic_cast<execplan::WindowFunctionColumn*>(tn))
      {
        constexpr size_t PART_FLAG{0x80000000};
        constexpr size_t ORD_FLAG{0x40000000};
        if (!(step & (PART_FLAG | ORD_FLAG)))
        {
          // window function param
          if (step > 0)
          {
            // prev param has been processed, wrap it if needed
            bool wrap = stack.results.back();
            stack.results.pop_back();
            if (wrap)
            {
              auto col = wf->functionParms()[step - 1];
              wf->functionParms()[step - 1] = execplan::SRCP(wrapColumn(col, lctx, ctx));
            }
          }
          if (step < wf->functionParms().size())
          {
            // there are some params left, push the next one
            stack.frames.push_back(Stack::Frame{wf->functionParms()[step++].get(), 0});
          }
          else if (!wf->partitions().empty())
          {
            // wrap partitions
            step = PART_FLAG | 1;
            stack.frames.push_back(Stack::Frame{wf->partitions()[0].get(), 0});
          }
          else if (!wf->orderBy().fOrders.empty())
          {
            // wrap order by columns
            step = ORD_FLAG | 1;
            stack.frames.push_back(Stack::Frame{wf->orderBy().fOrders[0].get(), 0});
          }
          else
          {
            // all done
            stack.frames.pop_back();
            stack.results.push_back(false);
          }
        }
        else if (step & PART_FLAG)
        {
          // window function partition
          size_t partStep = step & ~PART_FLAG;
          if (partStep > 0)
          {
            // prev partition has been processed, wrap it if needed
            bool wrap = stack.results.back();
            stack.results.pop_back();
            if (wrap)
            {
              auto col = wf->partitions()[partStep - 1];
              wf->partitions()[partStep - 1] = execplan::SRCP(wrapColumn(col, lctx, ctx));
            }
          }
          if (partStep < wf->partitions().size())
          {
            // there are some partitions left, push the next one
            step++;
            partStep++;
            stack.frames.push_back(Stack::Frame{wf->partitions()[partStep].get(), 0});
          }
          else if (!wf->orderBy().fOrders.empty())
          {
            // wrap order by columns
            step = ORD_FLAG | 1;
            stack.frames.push_back(Stack::Frame{wf->orderBy().fOrders[0].get(), 0});
          }
          else
          {
            stack.frames.pop_back();
            stack.results.push_back(false);
          }
        }
        else if (step & ORD_FLAG)
        {
          // window function ordering
          size_t ordStep = step & ~ORD_FLAG;
          if (ordStep > 0)
          {
            // prev order by column has been processed, wrap it if needed
            bool wrap = stack.results.back();
            stack.results.pop_back();
            if (wrap)
            {
              auto col = wf->orderBy().fOrders[ordStep - 1];
              wf->orderBy().fOrders[ordStep - 1] = execplan::SRCP(wrapColumn(col, lctx, ctx));
            }
          }
          if (ordStep < wf->orderBy().fOrders.size())
          {
            step++;
            ordStep++;
            stack.frames.push_back(Stack::Frame{wf->orderBy().fOrders[ordStep].get(), 0});
          }
          else
          {
            stack.frames.pop_back();
            stack.results.push_back(false);
          }
        }
        continue;
      }
    }
  }

  if (!stack.results.empty())
  {
    return stack.results.back();
  }
  return false;
}

void wrapIntoAggregate(execplan::SRCP& rc, ColumnWrapperContext& lctx, RBOptimizerContext& ctx)
{
  if (processColumn(rc.get(), lctx, ctx))
  {
    auto* ac = wrapColumn(rc, lctx, ctx);
    rc.reset(ac);
  }
}

} // namespace

bool groupByWrapColumnsFilter(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& ctx)
{
  bool hasCSTables = std::any_of(csep.tableList().begin(), csep.tableList().end(),
                                 [](const auto& table) { return table.isColumnstore(); });
  if (!hasCSTables)
  {
    return false;
  }

  // If the GROUP BY clause is missing, we need to go through all the columns in the SELECT
  // and ORDER BY expressions. The gwi.implicitExplicitGroupBy flag cannot be used as it is
  // global for the entire query, and we are interested in each specific subquery individually.
  if (hasAggregateColumns(csep, ctx) || !csep.groupByCols().empty())
  {
    return true;
  }

  return false;
}

bool applyGroupByWrapColumns(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& ctx)
{
  ColumnWrapperContext lctx{&csep.groupByCols(), csep.tableList()};
  // Find the next expression ID. Since this is the only place where the SELECT_SOME can appear,
  // there is no need to check if such an expression has occurred before.
  lctx.nextId = ctx.getMaxExpressionId() + 1;
  for (const auto& p : ctx.getGwi().processed) {
    if (p.second != -1u && p.second >= lctx.nextId)
    {
      lctx.nextId = p.second + 1;
    }
  }

  for (auto* cols : {&csep.returnedCols(), &csep.orderByCols()})
  {
    for (auto& rc : *cols)
    {
      if (!needWrap(rc.get(), lctx))
      {
        continue;
      }
      wrapIntoAggregate(rc, lctx, ctx);
    }
  }
  if (csep.having() != nullptr)
  {
    // HAVING is a parse tree, not column
    processColumn(csep.having(), lctx, ctx);
  }

  return lctx.applied;
}

} // namespace optimizer

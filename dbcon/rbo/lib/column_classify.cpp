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

#include "column_classify.h"

#include <vector>

#include "execplan/aggregatecolumn.h"
#include "execplan/arithmeticcolumn.h"
#include "execplan/functioncolumn.h"
#include "execplan/returnedcolumn.h"
#include "execplan/simplecolumn.h"
#include "execplan/simplefilter.h"
#include "execplan/windowfunctioncolumn.h"

namespace optimizer
{
namespace lib
{

bool columnBelongsToCSTableList(const execplan::SimpleColumn* sc,
                                const execplan::CalpontSelectExecutionPlan::TableList& tableList)
{
  if (sc == nullptr)
    return false;

  for (const auto& tbl : tableList)
  {
    if (!tbl.isColumnstore())
      continue;

    const bool schemaMatches = (tbl.schema == sc->schemaName());
    const bool tableMatches =
        (tbl.table == sc->tableName()) || (tbl.table.empty() && tbl.alias == sc->tableName());
    const bool aliasMatches = (tbl.alias == sc->tableAlias());

    if (schemaMatches && tableMatches && aliasMatches)
      return true;
  }
  return false;
}

bool containsAggregate(const std::variant<execplan::ParseTree*, execplan::TreeNode*>& col,
                       uint32_t* maxExprIdSink)
{
  bool foundAgg = false;
  std::vector<std::variant<execplan::ParseTree*, execplan::TreeNode*>> stack;
  stack.emplace_back(col);

  while (!stack.empty())
  {
    auto node = stack.back();
    stack.pop_back();

    if (auto* ptp = std::get_if<execplan::ParseTree*>(&node))
    {
      auto* pt = *ptp;
      if (pt == nullptr)
        continue;
      if (pt->left() != nullptr)
        stack.emplace_back(pt->left());
      if (pt->right() != nullptr)
        stack.emplace_back(pt->right());
      stack.emplace_back(pt->data());
      continue;
    }

    auto* tn = std::get<execplan::TreeNode*>(node);
    if (tn == nullptr)
      continue;

    if (auto* rc = dynamic_cast<execplan::ReturnedColumn*>(tn))
    {
      if (maxExprIdSink != nullptr && rc->expressionId() != static_cast<uint32_t>(-1))
      {
        if (rc->expressionId() > *maxExprIdSink)
          *maxExprIdSink = rc->expressionId();
      }
    }

    if (auto* agc = dynamic_cast<execplan::AggregateColumn*>(tn))
    {
      foundAgg = true;
      for (auto& arg : agc->aggParms())
        stack.emplace_back(arg.get());
    }
    else if (auto* ac = dynamic_cast<execplan::ArithmeticColumn*>(tn))
    {
      stack.emplace_back(ac->expression());
    }
    else if (auto* fc = dynamic_cast<execplan::FunctionColumn*>(tn))
    {
      for (auto& arg : fc->functionParms())
        stack.emplace_back(arg.get());
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
        stack.emplace_back(arg.get());
      for (auto& part : wf->partitions())
        stack.emplace_back(part.get());
    }
  }

  return foundAgg;
}

}  // namespace lib
}  // namespace optimizer

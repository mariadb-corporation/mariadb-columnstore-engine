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

// RBO primitive library: classifiers for columns and column-bearing
// expressions.  Used by rules that need to decide whether a SimpleColumn
// belongs to the current CSEP's table list, or whether an arbitrary
// column-tree reaches an AggregateColumn somewhere inside.

#pragma once

#include <cstdint>
#include <variant>

#include "execplan/calpontselectexecutionplan.h"
#include "execplan/parsetree.h"

namespace execplan
{
class SimpleColumn;
class TreeNode;
}  // namespace execplan

namespace optimizer
{
namespace lib
{

// ---------------------------------------------------------------------------
// Returns true iff `sc` references a ColumnStore-backed table whose
// (schema, table-or-alias-as-table, alias) triple matches an entry of
// `tableList`.  Non-CS tables in the list are skipped.
//
// The alias-as-table fallback reproduces the historical rbo_groupby_wrap_
// columns::needWrap rule: an entry with an empty `table` matches a column
// whose `sc->tableName()` equals the entry's `alias` (the parser sometimes
// stashes the alias in tableName when the physical name is unknown).
// ---------------------------------------------------------------------------
bool columnBelongsToCSTableList(const execplan::SimpleColumn* sc,
                                const execplan::CalpontSelectExecutionPlan::TableList& tableList);

// ---------------------------------------------------------------------------
// Walks `col` reaching through the following expression scaffolding:
//   * ParseTree    (descends into left, right and data)
//   * AggregateColumn    (descends into aggParms)
//   * ArithmeticColumn   (descends into expression)
//   * FunctionColumn     (descends into functionParms)
//   * SimpleFilter       (descends into lhs, rhs)
//   * WindowFunctionColumn (descends into functionParms and partitions)
//
// Returns true iff any AggregateColumn is reachable from `col`.
//
// If `maxExprIdSink` is non-null, every ReturnedColumn leaf whose
// `expressionId()` is not the sentinel value (uint32_t)-1 updates
// *maxExprIdSink to the running maximum.  The walker does not short-circuit
// on the first AggregateColumn so the tracker sees every id.
// ---------------------------------------------------------------------------
bool containsAggregate(const std::variant<execplan::ParseTree*, execplan::TreeNode*>& col,
                       uint32_t* maxExprIdSink = nullptr);

}  // namespace lib
}  // namespace optimizer

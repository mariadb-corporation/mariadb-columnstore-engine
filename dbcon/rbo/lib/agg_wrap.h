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

// RBO primitive library: helpers for wrapping a ReturnedColumn into an
// AggregateColumn(SELECT_SOME) node and for deduplicating identical such
// wrappers by expression id.
//
// Historical use site: rbo_groupby_wrap_columns.cpp — covers queries that
// mix GROUP BY with non-aggregate projection columns so every non-GB
// projection element becomes an "ANY_VALUE"-style SELECT_SOME aggregate.
//
// NOT used by rbo_apply_rewrite_distinct.cpp's ORDER-BY-not-in-projection
// wrapping.  That callsite sets a narrower field subset (asc/nullsFirst/
// aggOp/aggParms only) and is intentionally left inline until a dedicated
// audit pass confirms the missing fields are safe to add.

#pragma once

#include <cstdint>
#include <utility>
#include <vector>

namespace execplan
{
class AggregateColumn;
class ReturnedColumn;
}  // namespace execplan

namespace optimizer
{
namespace lib
{

// ---------------------------------------------------------------------------
// Wrap `rc` in a freshly-allocated AggregateColumn(SELECT_SOME) that uses
// `rc` as its single parameter.  Copies over the standard column-level
// attributes (alias / asc / charsetNumber / orderPos / resultType /
// sessionID) and sets the caller-supplied `timeZone`.
//
// Ownership:
//   * The returned AggregateColumn owns its `aggParms()` entry; the caller
//     must supply `rc` as an SRCP or transfer ownership via shared_ptr as
//     needed by the call site.  The template form accepts any smart-pointer-
//     or raw-pointer-to-ReturnedColumn that exposes the standard accessors.
//   * The returned AggregateColumn itself is owned by the caller.
//
// `expressionId` is NOT set here; use AggExprDedup::assignId to populate it.
// ---------------------------------------------------------------------------
template <typename ColPtr>
execplan::AggregateColumn* wrapIntoSelectSomeAgg(const ColPtr& rc, long timeZone);

// ---------------------------------------------------------------------------
// AggExprDedup — small shared store that deduplicates AggregateColumn
// expressions by structural equality (AggregateColumn::operator==).  Assigns
// a dense, monotonically-increasing expressionId to every fresh expression
// so downstream stages can recognise the same SELECT_SOME twice through the
// same id.
// ---------------------------------------------------------------------------
struct AggExprDedup
{
  std::vector<std::pair<execplan::AggregateColumn*, uint32_t>> entries;
  uint32_t nextId = 0;

  // Assigns `ac->expressionId(...)` to either the id of a structurally
  // equal entry already in the store (if present) or to a fresh `nextId++`
  // and records `ac` under that id.  Returns the chosen id.
  uint32_t assignId(execplan::AggregateColumn* ac);
};

}  // namespace lib
}  // namespace optimizer

// ------ Template definition (header-only because it is a free template) ----

#include "execplan/aggregatecolumn.h"
#include "execplan/returnedcolumn.h"

namespace optimizer
{
namespace lib
{

template <typename ColPtr>
inline execplan::AggregateColumn* wrapIntoSelectSomeAgg(const ColPtr& rc, long timeZone)
{
  auto* ac = new execplan::AggregateColumn(rc->sessionID());
  ac->timeZone(timeZone);
  ac->alias(rc->alias());
  ac->aggOp(execplan::AggregateColumn::SELECT_SOME);
  ac->asc(rc->asc());
  ac->charsetNumber(rc->charsetNumber());
  ac->orderPos(rc->orderPos());
  ac->aggParms().emplace_back(rc);
  ac->resultType(rc->resultType());
  return ac;
}

}  // namespace lib
}  // namespace optimizer

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

// RBO primitive library: factories and rebinders for SimpleColumn references
// pointing at a derived-table projection slot.
//
// Historically three rule files reimplemented this with different and
// incompletely overlapping field sets:
//
//   * rbo_apply_rewrite_distinct.cpp      :: cloneAsSimpleColumn
//   * rbo_decorrelate_outer_join_sub.cpp  :: makeDerivedColumnRef
//   * rbo_apply_parallel_ces.cpp          :: updateScToUseRewrittenDerived
//
// The three entry points below preserve each rule's historical field-set
// exactly (see implementation comments for the per-field audit).  The shared
// subset of fields is factored into `bindSCToDerivedProjectionCore`.

#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "execplan/returnedcolumn.h"

namespace execplan
{
class SimpleColumn;
}  // namespace execplan

namespace optimizer
{
namespace lib
{

// ---------------------------------------------------------------------------
// Shared core: sets the minimal "this SimpleColumn references a derived-table
// column at position `colPos` of table alias `derivedAlias`" field subset
// that every historical impl agrees on (tableAlias, derivedTable,
// colPosition).  Other fields that most but not all impls also set (oid,
// schemaName, tableName, data, ...) are left to the per-rule entry points
// below so each one preserves its historical field-set bit-for-bit.
// ---------------------------------------------------------------------------
void bindSCToDerivedProjectionCore(execplan::SimpleColumn* sc, const std::string& derivedAlias,
                                   int64_t colPos);

// ---------------------------------------------------------------------------
// Entry point 1 :: cloneAsSimpleColumn (rewrite_distinct flavour).
//
// Builds a brand-new SimpleColumn from an arbitrary ReturnedColumn `rc`
// (SimpleColumn / FunctionColumn / AggregateColumn / WindowFunctionColumn),
// intended to be used as a projection element of the outer CSEP that now
// references the derived sub-CSEP's projection slot `colPos`.
//
// Name resolution:
//   columnName = execplan::getSimpleColumnAlias(*rc, colPos)
//   alias      = "`<tableAlias>`.<columnName>"
//
// Additional fields set (beyond the core):
//   data=""; charsetNumber(rc->charsetNumber()); resultType(rc->resultType());
//   operationType(rc->operationType()); colSource(0);
//   timeZone: dispatched by dynamic_cast<SimpleColumn/FunctionColumn/
//             AggregateColumn/WindowFunctionColumn>(rc).
//   derivedRefCol: if rc already has one, reuses rc->derivedRefCol() with
//                  incRefCount(); otherwise uses rc.get() with incRefCount().
//
// Returns an owning SRCP (boost::shared_ptr<ReturnedColumn>) because the
// historical caller feeds it straight into SRCP-typed projection vectors.
// ---------------------------------------------------------------------------
execplan::SRCP cloneAsSimpleColumn(const execplan::SRCP& rc, const std::string& tableAlias,
                                   int64_t colPos);

// ---------------------------------------------------------------------------
// Entry point 2 :: makeDerivedColumnRef (decorrelate flavour).
//
// Builds a minimal SimpleColumn whose `columnName` matches `refCol->alias()`
// (which the caller is expected to have set before invoking this routine to
// the desired projection alias, e.g. "group_N" or "dec_agg").
//
// Additional fields set (beyond the core):
//   resultType(refCol->resultType()); timeZone(timeZone);
//   sequence(colPos); derivedRefCol(refCol); refCol->incRefCount().
//
// Caveat: this entry point deliberately does NOT set charsetNumber,
// operationType, data, colSource, alias.  Callers that need them should use
// cloneAsSimpleColumn instead.  See NOTES_PRIMITIVES.md for the historical
// audit.
// ---------------------------------------------------------------------------
execplan::SimpleColumn* makeDerivedColumnRef(execplan::ReturnedColumn* refCol,
                                             const std::string& derivedAlias,
                                             int64_t colPos,
                                             long timeZone);

// ---------------------------------------------------------------------------
// Entry point 3 :: rebindSCToDerivedInPlace (parallel_ces flavour).
//
// Mutates an EXISTING SimpleColumn so it references derived-table projection
// slot `colPos` at `derivedAlias`.  Unlike the two builders above this keeps
// the column's existing columnName and type/charset/tz information intact.
//
// Additional fields set (beyond the core):
//   isColumnStore(true);
//   data = "``.`<derivedAlias>`.`<sc->columnName()>`"
//   alias(*scAlias) iff scAlias is non-nullopt.
// ---------------------------------------------------------------------------
void rebindSCToDerivedInPlace(execplan::SimpleColumn* sc, const std::string& derivedAlias,
                              int64_t colPos,
                              std::optional<std::string> scAlias = std::nullopt);

}  // namespace lib
}  // namespace optimizer

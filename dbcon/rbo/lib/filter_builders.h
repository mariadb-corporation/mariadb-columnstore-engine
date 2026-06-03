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

// RBO primitive library: builders for SimpleFilter / ConstantColumn /
// PredicateOperator.  Replaces the ad-hoc constructor sequences scattered
// across rules (filtersWithNewRange, rewriteMatchedPattern, ...).

#pragma once

#include <cstdint>
#include <string>

#include "execplan/constantcolumn.h"
#include "execplan/operator.h"
#include "execplan/parsetree.h"

namespace execplan
{
class ReturnedColumn;
class SimpleFilter;
}  // namespace execplan

namespace optimizer
{
namespace lib
{

// ---------------------------------------------------------------------------
// PredicateOperator factory.
//
// Creates a PredicateOperator(opSym) wrapped in an SOP, and runs the
// standard op-type-resolution sequence:
//   op->setOpType(lhsType, rhsType);
//   op->resultType(op->operationType());
//
// Supported symbols are whatever PredicateOperator understands:
//   "=", "<>", "<", "<=", ">", ">=", "isnull", "isnotnull", "like", ...
// ---------------------------------------------------------------------------
execplan::SOP makePredicateOp(const std::string& opSym,
                              const execplan::CalpontSystemCatalog::ColType& lhsType,
                              const execplan::CalpontSystemCatalog::ColType& rhsType);

// ---------------------------------------------------------------------------
// SimpleFilter + ParseTree combined factory.
//
// Takes ownership of `lhs` and `rhs`.  Creates a PredicateOperator(opSym)
// with setOpType derived from lhs/rhs result types, builds a SimpleFilter
// and returns it wrapped in a freshly-allocated ParseTree.
//
// `timeZone` is forwarded to SimpleFilter's ctor.
// ---------------------------------------------------------------------------
execplan::ParseTree* makeCmpFilter(execplan::ReturnedColumn* lhs, const std::string& opSym,
                                   execplan::ReturnedColumn* rhs, long timeZone = 0);

// ---------------------------------------------------------------------------
// IS NULL / IS NOT NULL factories.
//
// Consumes ownership of `col` and returns `col IS [NOT] NULL` wrapped in a
// freshly-allocated ParseTree.  The null-side ConstantColumnNull inherits
// the column's resultType to satisfy the operator's type-compatibility
// check.
// ---------------------------------------------------------------------------
execplan::ParseTree* makeIsNullFilter(execplan::ReturnedColumn* col);
execplan::ParseTree* makeIsNotNullFilter(execplan::ReturnedColumn* col);

// ---------------------------------------------------------------------------
// ConstantColumn factories.
//
// Plain wrappers so rules don't have to remember the 3-argument
// scale/precision form of ConstantColumnUInt.  All pointers are owned by the
// caller on return.
// ---------------------------------------------------------------------------
execplan::ConstantColumn* makeConstUInt(uint64_t value, int8_t scale = 0, uint8_t precision = 0);
execplan::ConstantColumn* makeConstNull();

}  // namespace lib
}  // namespace optimizer

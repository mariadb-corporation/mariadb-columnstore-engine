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

#include <gtest/gtest.h>

#include <memory>

#include <dbcon/execplan/calpontsystemcatalog.h>
#include <dbcon/execplan/constantcolumn.h>
#include <dbcon/execplan/parsetree.h>
#include <dbcon/execplan/predicateoperator.h>
#include <dbcon/execplan/simplefilter.h>
#include <dbcon/rbo/lib/filter_builders.h>

using namespace optimizer::lib;

namespace
{

// Build a BIGINT ColType usable for the predicate-op test sites below.
execplan::CalpontSystemCatalog::ColType bigintType()
{
  execplan::CalpontSystemCatalog::ColType t;
  t.colDataType = execplan::CalpontSystemCatalog::BIGINT;
  t.colWidth = 8;
  return t;
}

// Fresh uint constant with BIGINT result type.
execplan::ConstantColumn* newBigintConst(uint64_t v)
{
  auto* c = makeConstUInt(v);
  c->resultType(bigintType());
  return c;
}

}  // namespace

// ---------------------------------------------------------------------------
// makeConstUInt / makeConstNull
// ---------------------------------------------------------------------------

TEST(FilterBuildersTest, MakeConstUIntProducesConstantColumn)
{
  std::unique_ptr<execplan::ConstantColumn> c(makeConstUInt(42));
  ASSERT_NE(c.get(), nullptr);
  // ConstantColumnUInt stores in the NUM slot with the unsigned value.
  EXPECT_EQ(c->type(), execplan::ConstantColumn::NUM);
}

TEST(FilterBuildersTest, MakeConstNullIsNullType)
{
  std::unique_ptr<execplan::ConstantColumn> c(makeConstNull());
  ASSERT_NE(c.get(), nullptr);
  EXPECT_EQ(c->type(), execplan::ConstantColumn::NULLDATA);
}

// ---------------------------------------------------------------------------
// makePredicateOp
// ---------------------------------------------------------------------------

TEST(FilterBuildersTest, MakePredicateOpSetsSymbolAndResultType)
{
  auto t = bigintType();
  execplan::SOP op = makePredicateOp(">=", t, t);
  ASSERT_NE(op.get(), nullptr);
  EXPECT_EQ(op->data(), ">=");
  // setOpType + resultType(op->operationType()) should have populated
  // operationType; just verify it was actually filled in.
  EXPECT_NE(op->operationType().colDataType,
            execplan::CalpontSystemCatalog::UNDEFINED);
}

// ---------------------------------------------------------------------------
// makeCmpFilter
// ---------------------------------------------------------------------------

TEST(FilterBuildersTest, MakeCmpFilterBuildsSimpleFilterTree)
{
  auto* lhs = newBigintConst(10);
  auto* rhs = newBigintConst(20);
  std::unique_ptr<execplan::ParseTree> pt(makeCmpFilter(lhs, "<", rhs, 0));

  ASSERT_NE(pt.get(), nullptr);
  auto* sf = dynamic_cast<execplan::SimpleFilter*>(pt->data());
  ASSERT_NE(sf, nullptr);
  ASSERT_NE(sf->op().get(), nullptr);
  EXPECT_EQ(sf->op()->data(), "<");
  // Verify lhs/rhs are the same pointers we passed in.
  EXPECT_EQ(sf->lhs(), lhs);
  EXPECT_EQ(sf->rhs(), rhs);
}

TEST(FilterBuildersTest, MakeCmpFilterSupportsEqSymbol)
{
  auto* lhs = newBigintConst(1);
  auto* rhs = newBigintConst(1);
  std::unique_ptr<execplan::ParseTree> pt(makeCmpFilter(lhs, "=", rhs, 0));

  ASSERT_NE(pt.get(), nullptr);
  auto* sf = dynamic_cast<execplan::SimpleFilter*>(pt->data());
  ASSERT_NE(sf, nullptr);
  EXPECT_EQ(sf->op()->data(), "=");
}

// ---------------------------------------------------------------------------
// makeIsNullFilter / makeIsNotNullFilter
// ---------------------------------------------------------------------------

TEST(FilterBuildersTest, MakeIsNullFilterBuildsIsNullFilter)
{
  auto* col = newBigintConst(0);
  std::unique_ptr<execplan::ParseTree> pt(makeIsNullFilter(col));

  ASSERT_NE(pt.get(), nullptr);
  auto* sf = dynamic_cast<execplan::SimpleFilter*>(pt->data());
  ASSERT_NE(sf, nullptr);
  // PredicateOperator normalizes "isnull" to "is null".
  EXPECT_EQ(sf->op()->data(), "is null");
  auto* rhs = dynamic_cast<execplan::ConstantColumnNull*>(sf->rhs());
  EXPECT_NE(rhs, nullptr);
}

TEST(FilterBuildersTest, MakeIsNotNullFilterBuildsIsNotNullFilter)
{
  auto* col = newBigintConst(0);
  std::unique_ptr<execplan::ParseTree> pt(makeIsNotNullFilter(col));

  ASSERT_NE(pt.get(), nullptr);
  auto* sf = dynamic_cast<execplan::SimpleFilter*>(pt->data());
  ASSERT_NE(sf, nullptr);
  // PredicateOperator normalizes "isnotnull" to "is not null".
  EXPECT_EQ(sf->op()->data(), "is not null");
}

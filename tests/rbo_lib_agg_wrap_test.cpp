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

#include <boost/make_shared.hpp>

#include <dbcon/execplan/aggregatecolumn.h>
#include <dbcon/execplan/calpontsystemcatalog.h>
#include <dbcon/execplan/simplecolumn.h>
#include <dbcon/rbo/lib/agg_wrap.h>

using namespace optimizer::lib;

namespace
{

execplan::CalpontSystemCatalog::ColType bigintType(int colWidth = 8, uint32_t charset = 33u)
{
  execplan::CalpontSystemCatalog::ColType t;
  t.colDataType = execplan::CalpontSystemCatalog::BIGINT;
  t.colWidth = colWidth;
  t.charsetNumber = charset;
  return t;
}

// The `distinguisher` is baked into columnName (for debug readability) but
// the *actual* distinguisher observed by ReturnedColumn::operator== is
// `resultType.colWidth` — that's one of the few ColType fields that
// participates in ColType::operator== (which does NOT compare charsetNumber
// or colDataType).  See ColType::operator== in calpontsystemcatalog.h.
boost::shared_ptr<execplan::SimpleColumn> newSC(const std::string& distinguisher,
                                                int colWidth = 8)
{
  auto sc = boost::make_shared<execplan::SimpleColumn>();
  sc->columnName("col_" + distinguisher);
  sc->alias("alias_" + distinguisher);
  sc->asc(true);
  sc->orderPos(42);
  sc->resultType(bigintType(colWidth));
  return sc;
}

}  // namespace

// ---------------------------------------------------------------------------
// wrapIntoSelectSomeAgg
// ---------------------------------------------------------------------------

TEST(AggWrapTest, WrapIntoSelectSomeAggSetsAllFields)
{
  auto rc = newSC("orig");
  std::unique_ptr<execplan::AggregateColumn> ac(wrapIntoSelectSomeAgg(rc, 17));

  ASSERT_NE(ac.get(), nullptr);
  EXPECT_EQ(ac->aggOp(), execplan::AggregateColumn::SELECT_SOME);
  EXPECT_EQ(ac->alias(), "alias_orig");
  EXPECT_EQ(ac->asc(), true);
  EXPECT_EQ(ac->orderPos(), 42);
  EXPECT_EQ(ac->timeZone(), 17);
  EXPECT_EQ(ac->resultType().colDataType, execplan::CalpontSystemCatalog::BIGINT);
  // charsetNumber() reads through resultType().charsetNumber; confirm the
  // propagation through resultType() succeeded.
  EXPECT_EQ(ac->resultType().charsetNumber, 33u);

  // The wrapped column is the single aggParms entry.
  ASSERT_EQ(ac->aggParms().size(), 1u);
  EXPECT_EQ(ac->aggParms()[0].get(), rc.get());
}

// ---------------------------------------------------------------------------
// AggExprDedup
// ---------------------------------------------------------------------------

TEST(AggWrapTest, DedupAssignsFreshIdToNewExpressions)
{
  AggExprDedup dedup;
  dedup.nextId = 100;

  // NOTE: AggregateColumn::operator== compares aggParms[] via
  // ReturnedColumn::operator== (through the `**it != **it2` on SRCP) which
  // only looks at base-class fields (fData/fCardinality/fDistinct/fAsc/
  // fResultType/fOperationType/...), NOT at SimpleColumn-specific
  // columnName/tableAlias/etc.  To make two SCs actually distinct under
  // this equality we must differ in one of those base fields.  We pick
  // resultType (different charsetNumber) as the distinguisher.
  auto rc1 = newSC("first", /*colWidth=*/8);
  auto rc2 = newSC("second", /*colWidth=*/4);
  std::unique_ptr<execplan::AggregateColumn> ac1(wrapIntoSelectSomeAgg(rc1, 0));
  std::unique_ptr<execplan::AggregateColumn> ac2(wrapIntoSelectSomeAgg(rc2, 0));

  EXPECT_EQ(dedup.assignId(ac1.get()), 100u);
  EXPECT_EQ(dedup.assignId(ac2.get()), 101u);
  EXPECT_EQ(ac1->expressionId(), 100u);
  EXPECT_EQ(ac2->expressionId(), 101u);
  EXPECT_EQ(dedup.nextId, 102u);
  EXPECT_EQ(dedup.entries.size(), 2u);
}

TEST(AggWrapTest, DedupReusesIdForStructurallyEqualExpression)
{
  AggExprDedup dedup;
  dedup.nextId = 10;

  auto rc = newSC("a");
  std::unique_ptr<execplan::AggregateColumn> ac1(wrapIntoSelectSomeAgg(rc, 5));
  std::unique_ptr<execplan::AggregateColumn> ac2(wrapIntoSelectSomeAgg(rc, 5));

  EXPECT_EQ(dedup.assignId(ac1.get()), 10u);
  // ac2 is structurally equal to ac1 (same rc, same params): reuse the id.
  EXPECT_EQ(dedup.assignId(ac2.get()), 10u);
  EXPECT_EQ(ac2->expressionId(), 10u);
  EXPECT_EQ(dedup.nextId, 11u);
  EXPECT_EQ(dedup.entries.size(), 1u);
}

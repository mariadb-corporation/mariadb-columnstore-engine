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
#include <dbcon/execplan/arithmeticcolumn.h>
#include <dbcon/execplan/calpontselectexecutionplan.h>
#include <dbcon/execplan/calpontsystemcatalog.h>
#include <dbcon/execplan/parsetree.h>
#include <dbcon/execplan/simplecolumn.h>
#include <dbcon/rbo/lib/column_classify.h>

using namespace optimizer::lib;

namespace
{

boost::shared_ptr<execplan::SimpleColumn> makeSC(const std::string& schema,
                                                  const std::string& table,
                                                  const std::string& alias)
{
  auto sc = boost::make_shared<execplan::SimpleColumn>();
  sc->schemaName(schema);
  sc->tableName(table);
  sc->tableAlias(alias);
  return sc;
}

}  // namespace

// ---------------------------------------------------------------------------
// columnBelongsToCSTableList
// ---------------------------------------------------------------------------

TEST(ColumnClassifyTest, ColumnMatchesExactTripleAndCSFlag)
{
  auto sc = makeSC("db1", "customer", "c");
  execplan::CalpontSelectExecutionPlan::TableList tl{
      execplan::make_aliasview("db1", "customer", "c", "", /*isColumnStore=*/true),
  };
  EXPECT_TRUE(columnBelongsToCSTableList(sc.get(), tl));
}

TEST(ColumnClassifyTest, ColumnMismatchAliasIsRejected)
{
  auto sc = makeSC("db1", "customer", "c");
  execplan::CalpontSelectExecutionPlan::TableList tl{
      execplan::make_aliasview("db1", "customer", "other", "", /*isColumnStore=*/true),
  };
  EXPECT_FALSE(columnBelongsToCSTableList(sc.get(), tl));
}

TEST(ColumnClassifyTest, NonColumnStoreTablesAreSkipped)
{
  auto sc = makeSC("db1", "customer", "c");
  execplan::CalpontSelectExecutionPlan::TableList tl{
      execplan::make_aliasview("db1", "customer", "c", "", /*isColumnStore=*/false),
  };
  EXPECT_FALSE(columnBelongsToCSTableList(sc.get(), tl));
}

TEST(ColumnClassifyTest, AliasAsTableFallbackMatches)
{
  // tbl.table is empty, sc->tableName() equals tbl.alias: fallback triggers.
  auto sc = makeSC("db1", "c", "c");
  execplan::CalpontSelectExecutionPlan::TableList tl{
      execplan::make_aliasview("db1", /*table=*/"", /*alias=*/"c", "", /*isColumnStore=*/true),
  };
  EXPECT_TRUE(columnBelongsToCSTableList(sc.get(), tl));
}

TEST(ColumnClassifyTest, NullColumnReturnsFalse)
{
  execplan::CalpontSelectExecutionPlan::TableList tl;
  EXPECT_FALSE(columnBelongsToCSTableList(nullptr, tl));
}

// ---------------------------------------------------------------------------
// containsAggregate
// ---------------------------------------------------------------------------

TEST(ColumnClassifyTest, ContainsAggregateLeafSimpleColumnFalse)
{
  auto sc = makeSC("db", "t", "t");
  std::variant<execplan::ParseTree*, execplan::TreeNode*> v{
      static_cast<execplan::TreeNode*>(sc.get())};
  EXPECT_FALSE(containsAggregate(v));
}

TEST(ColumnClassifyTest, ContainsAggregateDirectAggregateColumnTrue)
{
  auto ac = boost::make_shared<execplan::AggregateColumn>();
  ac->aggOp(execplan::AggregateColumn::COUNT);
  std::variant<execplan::ParseTree*, execplan::TreeNode*> v{
      static_cast<execplan::TreeNode*>(ac.get())};
  EXPECT_TRUE(containsAggregate(v));
}

TEST(ColumnClassifyTest, ContainsAggregateThroughArithmetic)
{
  // ArithmeticColumn whose expression contains an AggregateColumn.
  auto inner = boost::make_shared<execplan::AggregateColumn>();
  inner->aggOp(execplan::AggregateColumn::SUM);

  auto arith = boost::make_shared<execplan::ArithmeticColumn>();
  execplan::ParseTree* exprTree = new execplan::ParseTree(inner->clone());
  arith->expression(exprTree);  // takes ownership; sets exprTree to null

  std::variant<execplan::ParseTree*, execplan::TreeNode*> v{
      static_cast<execplan::TreeNode*>(arith.get())};
  EXPECT_TRUE(containsAggregate(v));
}

TEST(ColumnClassifyTest, MaxExprIdTrackerUpdatesFromReturnedColumns)
{
  auto sc = makeSC("db", "t", "t");
  sc->expressionId(42);

  uint32_t maxId = 5;
  std::variant<execplan::ParseTree*, execplan::TreeNode*> v{
      static_cast<execplan::TreeNode*>(sc.get())};
  EXPECT_FALSE(containsAggregate(v, &maxId));
  EXPECT_EQ(maxId, 42u);
}

TEST(ColumnClassifyTest, MaxExprIdTrackerPreservesExistingWhenLower)
{
  auto sc = makeSC("db", "t", "t");
  sc->expressionId(3);

  uint32_t maxId = 10;
  std::variant<execplan::ParseTree*, execplan::TreeNode*> v{
      static_cast<execplan::TreeNode*>(sc.get())};
  EXPECT_FALSE(containsAggregate(v, &maxId));
  EXPECT_EQ(maxId, 10u);
}

TEST(ColumnClassifyTest, MaxExprIdTrackerIgnoresSentinelMinusOne)
{
  auto sc = makeSC("db", "t", "t");
  // Default expressionId is the sentinel (uint32_t)-1; verify we don't treat
  // that as a real id when tracking.
  uint32_t maxId = 7;
  std::variant<execplan::ParseTree*, execplan::TreeNode*> v{
      static_cast<execplan::TreeNode*>(sc.get())};
  containsAggregate(v, &maxId);
  EXPECT_EQ(maxId, 7u);
}

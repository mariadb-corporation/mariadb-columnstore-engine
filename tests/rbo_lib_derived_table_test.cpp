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

#include <dbcon/execplan/calpontselectexecutionplan.h>
#include <dbcon/execplan/calpontsystemcatalog.h>
#include <dbcon/rbo/lib/derived_table.h>

using namespace optimizer::lib;

namespace
{

boost::shared_ptr<execplan::CalpontSelectExecutionPlan> newCSEP()
{
  return boost::make_shared<execplan::CalpontSelectExecutionPlan>();
}

}  // namespace

// ---------------------------------------------------------------------------
// promoteCSEPToDerived
// ---------------------------------------------------------------------------

TEST(DerivedTableTest, PromoteSetsThreeFlags)
{
  auto csep = newCSEP();
  promoteCSEPToDerived(csep.get(), "dt_alias");

  EXPECT_EQ(csep->location(), execplan::CalpontSelectExecutionPlan::FROM);
  EXPECT_EQ(csep->subType(), execplan::CalpontSelectExecutionPlan::FROM_SUBS);
  EXPECT_EQ(csep->derivedTbAlias(), "dt_alias");
}

// ---------------------------------------------------------------------------
// wrapCSEPAsDerived
// ---------------------------------------------------------------------------

TEST(DerivedTableTest, WrapMovesOrigIntoDerivedAndResetsOuter)
{
  auto outer = newCSEP();
  auto orig = newCSEP();

  // Populate outer with state that wrapCSEPAsDerived is expected to clear.
  outer->distinct(true);
  outer->subSelects({newCSEP()});
  outer->unionVec({newCSEP()});

  wrapCSEPAsDerived(*outer, orig, "wrap_alias");

  // orig was promoted to FROM-subquery flavour.
  EXPECT_EQ(orig->location(), execplan::CalpontSelectExecutionPlan::FROM);
  EXPECT_EQ(orig->subType(), execplan::CalpontSelectExecutionPlan::FROM_SUBS);
  EXPECT_EQ(orig->derivedTbAlias(), "wrap_alias");

  // outer's sub-plan containers are empty.
  EXPECT_TRUE(outer->subSelectList().empty());
  EXPECT_TRUE(outer->subSelects().empty());
  EXPECT_TRUE(outer->selectSubList().empty());
  EXPECT_TRUE(outer->unionVec().empty());

  // tableList is a single make_aliasview("", "", alias, "") entry.
  ASSERT_EQ(outer->tableList().size(), 1u);
  EXPECT_EQ(outer->tableList()[0].alias, "wrap_alias");
  EXPECT_EQ(outer->tableList()[0].schema, "");
  EXPECT_EQ(outer->tableList()[0].table, "");

  // derivedTableList is a single entry pointing at the moved-in plan.
  ASSERT_EQ(outer->derivedTableList().size(), 1u);
  EXPECT_EQ(outer->derivedTableList()[0].get(), orig.get());

  // Flags/residual state cleared.
  EXPECT_FALSE(outer->distinct());
  EXPECT_EQ(outer->filters(), nullptr);
  EXPECT_EQ(outer->having(), nullptr);
  EXPECT_TRUE(outer->returnedCols().empty());
  EXPECT_TRUE(outer->groupByCols().empty());
}

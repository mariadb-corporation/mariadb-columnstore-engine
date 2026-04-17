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
#include <boost/shared_ptr.hpp>

#include <dbcon/execplan/aggregatecolumn.h>
#include <dbcon/execplan/calpontsystemcatalog.h>
#include <dbcon/execplan/returnedcolumn.h>
#include <dbcon/execplan/simplecolumn.h>
#include <dbcon/rbo/lib/derived_column.h>

using namespace optimizer::lib;

namespace
{

execplan::CalpontSystemCatalog::ColType bigintType()
{
  execplan::CalpontSystemCatalog::ColType t;
  t.colDataType = execplan::CalpontSystemCatalog::BIGINT;
  t.colWidth = 8;
  return t;
}

// Fresh SimpleColumn with a deterministic name/alias so we can observe field
// mutations unambiguously.
boost::shared_ptr<execplan::SimpleColumn> newBasicSC(const std::string& colName = "col")
{
  auto sc = boost::make_shared<execplan::SimpleColumn>();
  sc->columnName(colName);
  sc->alias("original_alias");
  sc->schemaName("srcdb");
  sc->tableName("srctable");
  sc->tableAlias("src_a");
  sc->oid(1234);
  sc->resultType(bigintType());
  sc->timeZone(0);
  return sc;
}

}  // namespace

// ---------------------------------------------------------------------------
// bindSCToDerivedProjectionCore
// ---------------------------------------------------------------------------

TEST(DerivedColumnTest, CoreSetsUniversalTriple)
{
  auto sc = newBasicSC();
  bindSCToDerivedProjectionCore(sc.get(), "derived1", 7);

  EXPECT_EQ(sc->tableAlias(), "derived1");
  EXPECT_EQ(sc->derivedTable(), "derived1");
  EXPECT_EQ(sc->colPosition(), 7u);
  // Fields outside the core must remain untouched.
  EXPECT_EQ(sc->schemaName(), "srcdb");
  EXPECT_EQ(sc->tableName(), "srctable");
  EXPECT_EQ(sc->oid(), 1234u);
}

// ---------------------------------------------------------------------------
// cloneAsSimpleColumn (rewrite_distinct flavour)
// ---------------------------------------------------------------------------

TEST(DerivedColumnTest, CloneAsSimpleColumnSetsFullFieldSet)
{
  auto src = newBasicSC("customer_id");
  src->charsetNumber(33u);
  execplan::SRCP rc = src;  // upcast to SRCP

  execplan::SRCP cloned = cloneAsSimpleColumn(rc, "derived_alias", 3u);

  auto* out = dynamic_cast<execplan::SimpleColumn*>(cloned.get());
  ASSERT_NE(out, nullptr);

  // Core fields.
  EXPECT_EQ(out->tableAlias(), "derived_alias");
  EXPECT_EQ(out->derivedTable(), "derived_alias");
  EXPECT_EQ(out->colPosition(), 3u);

  // Rewrite_distinct-specific resets.  Note that SimpleColumn::data() is a
  // computed property that falls back to a "`schema`.`table`.`col`" form
  // when the underlying fData is empty, so checking `data() == ""` would be
  // incorrect.  The contract is that fData was cleared (no stale raw string
  // survives the clone); we verify this indirectly via the synthesized form.
  EXPECT_EQ(out->schemaName(), "");
  EXPECT_EQ(out->tableName(), "derived_alias");
  EXPECT_EQ(out->oid(), 0u);
  EXPECT_EQ(out->data(), "``.`derived_alias`.`" + out->columnName() + "`");

  // Name derivation: empty-alias source falls back to column-name-based
  // synthesis performed by execplan::getSimpleColumnAlias.
  EXPECT_FALSE(out->columnName().empty());
  EXPECT_EQ(out->alias(), "`derived_alias`." + out->columnName());

  // Copied fields.
  EXPECT_EQ(out->charsetNumber(), 33u);
  EXPECT_EQ(out->colSource(), 0u);

  // derivedRefCol is set to the source (fresh SC, no prior derivedRefCol).
  EXPECT_EQ(out->derivedRefCol(), src.get());
}

TEST(DerivedColumnTest, CloneAsSimpleColumnNestsDerivedRefCol)
{
  // Simulate a "column already points at a derived ref": cloning must not
  // chain another indirection; it must link directly to the innermost ref.
  auto innermost = newBasicSC("inner");
  auto middle = newBasicSC("middle");
  middle->derivedRefCol(innermost.get());
  execplan::SRCP rc = middle;

  execplan::SRCP cloned = cloneAsSimpleColumn(rc, "d", 0);

  auto* out = dynamic_cast<execplan::SimpleColumn*>(cloned.get());
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(out->derivedRefCol(), innermost.get());
}

// ---------------------------------------------------------------------------
// makeDerivedColumnRef (decorrelate flavour)
// ---------------------------------------------------------------------------

TEST(DerivedColumnTest, MakeDerivedColumnRefUsesAliasAsColumnName)
{
  auto refCol = newBasicSC("physical_name");
  refCol->alias("aliased_group_0");

  std::unique_ptr<execplan::SimpleColumn> out(
      makeDerivedColumnRef(refCol.get(), "dec_sub", 2, /*timeZone=*/42));

  ASSERT_NE(out.get(), nullptr);
  EXPECT_EQ(out->tableAlias(), "dec_sub");
  EXPECT_EQ(out->derivedTable(), "dec_sub");
  EXPECT_EQ(out->colPosition(), 2u);
  EXPECT_EQ(out->sequence(), 2u);
  EXPECT_EQ(out->columnName(), "aliased_group_0");
  EXPECT_EQ(out->derivedRefCol(), refCol.get());
  EXPECT_EQ(out->timeZone(), 42);

  // Fields NOT set by this entry point remain at default-constructed values
  // (matches historical behaviour of decorrelate's makeDerivedColumnRef).
  EXPECT_EQ(out->schemaName(), "");
  EXPECT_EQ(out->tableName(), "");
}

// ---------------------------------------------------------------------------
// rebindSCToDerivedInPlace (parallel_ces flavour)
// ---------------------------------------------------------------------------

TEST(DerivedColumnTest, RebindInPlaceUpdatesBindingsAndKeepsColumnName)
{
  auto sc = newBasicSC("measure");
  rebindSCToDerivedInPlace(sc.get(), "par_d", 5);

  // Core + parallel_ces-specific fields.
  EXPECT_EQ(sc->tableAlias(), "par_d");
  EXPECT_EQ(sc->derivedTable(), "par_d");
  EXPECT_EQ(sc->colPosition(), 5u);
  EXPECT_EQ(sc->schemaName(), "");
  EXPECT_EQ(sc->tableName(), "par_d");
  EXPECT_EQ(sc->oid(), 0u);
  EXPECT_EQ(sc->data(), "``.`par_d`.`measure`");
  EXPECT_TRUE(sc->isColumnStore());

  // columnName is intentionally preserved.
  EXPECT_EQ(sc->columnName(), "measure");
  // alias untouched when scAlias is nullopt.
  EXPECT_EQ(sc->alias(), "original_alias");
}

TEST(DerivedColumnTest, RebindInPlaceAppliesOptionalAlias)
{
  auto sc = newBasicSC("m");
  rebindSCToDerivedInPlace(sc.get(), "par", 1, std::string("custom_alias"));
  EXPECT_EQ(sc->alias(), "custom_alias");
}

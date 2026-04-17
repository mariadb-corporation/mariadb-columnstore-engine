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

#include "derived_column.h"

#include <boost/make_shared.hpp>

#include "execplan/aggregatecolumn.h"
#include "execplan/functioncolumn.h"
#include "execplan/simplecolumn.h"
#include "execplan/windowfunctioncolumn.h"

namespace optimizer
{
namespace lib
{

void bindSCToDerivedProjectionCore(execplan::SimpleColumn* sc, const std::string& derivedAlias,
                                   int64_t colPos)
{
  sc->tableAlias(derivedAlias);
  sc->derivedTable(derivedAlias);
  sc->colPosition(static_cast<int>(colPos));
}

namespace
{

// Dispatches dynamic_cast lookup to copy timeZone from the concrete RC type.
// Returns 0 when the RC subtype does not carry a per-column timeZone.
long timeZoneOf(const execplan::ReturnedColumn* rc)
{
  if (const auto* rcsc = dynamic_cast<const execplan::SimpleColumn*>(rc))
    return rcsc->timeZone();
  if (const auto* rcfc = dynamic_cast<const execplan::FunctionColumn*>(rc))
    return rcfc->timeZone();
  if (const auto* rcac = dynamic_cast<const execplan::AggregateColumn*>(rc))
    return rcac->timeZone();
  if (const auto* rcwc = dynamic_cast<const execplan::WindowFunctionColumn*>(rc))
    return rcwc->timeZone();
  return 0;
}

}  // namespace

execplan::SRCP cloneAsSimpleColumn(const execplan::SRCP& rc, const std::string& tableAlias,
                                   int64_t colPos)
{
  auto rcCloned = boost::make_shared<execplan::SimpleColumn>(*rc);

  // Rewrite_distinct-specific fields that go beyond the universal core.
  rcCloned->schemaName("");
  rcCloned->tableName(tableAlias);
  rcCloned->oid(0);
  rcCloned->data("");
  rcCloned->charsetNumber(rc->charsetNumber());

  // Universal core: tableAlias/derivedTable/colPosition.
  bindSCToDerivedProjectionCore(rcCloned.get(), tableAlias, colPos);

  // derivedRefCol with nesting: if rc already is a derived reference, reuse
  // its origin rather than chaining derivedRefCol through the fresh SC.
  if (auto* rcRef = rc->derivedRefCol())
  {
    rcCloned->derivedRefCol(rcRef);
    rcRef->incRefCount();
  }
  else
  {
    rcCloned->derivedRefCol(rc.get());
    rc->incRefCount();
  }

  rcCloned->resultType(rc->resultType());
  rcCloned->operationType(rc->operationType());
  rcCloned->timeZone(timeZoneOf(rc.get()));

  auto colName = execplan::getSimpleColumnAlias(*rc, colPos);
  rcCloned->columnName(colName);
  rcCloned->alias("`" + tableAlias + "`." + colName);
  rcCloned->colSource(0);

  return rcCloned;
}

execplan::SimpleColumn* makeDerivedColumnRef(execplan::ReturnedColumn* refCol,
                                             const std::string& derivedAlias,
                                             int64_t colPos,
                                             long timeZone)
{
  // Default-constructed SimpleColumn already has oid=0, empty schemaName and
  // empty tableName, matching the historical impl which never touched those.
  auto* sc = new execplan::SimpleColumn();

  // Universal core: tableAlias/derivedTable/colPosition.
  bindSCToDerivedProjectionCore(sc, derivedAlias, colPos);

  // Decorrelate-specific fields.
  sc->columnName(refCol->alias());
  sc->resultType(refCol->resultType());
  sc->timeZone(timeZone);
  sc->sequence(static_cast<int>(colPos));
  sc->derivedRefCol(refCol);
  refCol->incRefCount();
  return sc;
}

void rebindSCToDerivedInPlace(execplan::SimpleColumn* sc, const std::string& derivedAlias,
                              int64_t colPos, std::optional<std::string> scAlias)
{
  // Parallel_ces-specific fields that go beyond the universal core.
  sc->oid(0);
  sc->schemaName("");
  sc->tableName(derivedAlias);
  sc->data("``.`" + derivedAlias + "`.`" + sc->columnName() + "`");
  sc->isColumnStore(true);

  // Universal core: tableAlias/derivedTable/colPosition.
  bindSCToDerivedProjectionCore(sc, derivedAlias, colPos);

  if (scAlias)
  {
    sc->alias(scAlias.value());
  }
}

}  // namespace lib
}  // namespace optimizer

/* Copyright (C) 2025 MariaDB Corporation

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

#include "execplan/calpontselectexecutionplan.h"
#include "execplan/simplecolumn.h"
#include "rulebased_optimizer.h"

namespace optimizer {

bool optimizeCSEPWithRules(execplan::CalpontSelectExecutionPlan& root, const std::vector<Rule>& rules) {

  bool changed = false;
  for (const auto& rule : rules)
  {
    changed |= rule.apply(root);
  }
  return changed;
}

bool optimizeCSEP(execplan::CalpontSelectExecutionPlan& root)
{
  optimizer::Rule parallelCES{"parallelCES", optimizer::matchParallelCES, optimizer::applyParallelCES};
  
  std::vector<Rule> rules = {parallelCES};

  return optimizeCSEPWithRules(root, rules);
}

// DFS walk
bool Rule::apply(execplan::CalpontSelectExecutionPlan& csep) const
{
  bool rewrite = false;

  for (auto& table : csep.derivedTableList())
  {
    auto* csepPtr = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(table.get());
    if (!csepPtr)
    {
      continue;
    }

    auto& csepLocal = *csepPtr;
    rewrite |= apply(csepLocal);
  }

  for (auto& unionUnit : csep.unionVec())
  {
    auto* unionUnitPtr = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(unionUnit.get());
    if (!unionUnitPtr)
    {
      continue;
    }

    auto& unionUnitLocal = *unionUnitPtr;
    rewrite |= apply(unionUnitLocal);
  }

  if (matchRule(csep))
  {
    applyRule(csep);
    rewrite = true;
  }

  return rewrite;
}


bool tableIsInUnion(const execplan::CalpontSystemCatalog::TableAliasName& table, execplan::CalpontSelectExecutionPlan& csep)
{
  return std::any_of(csep.unionVec().begin(), csep.unionVec().end(), 
  [&table](const auto& unionUnit) {
    execplan::CalpontSelectExecutionPlan* unionUnitLocal = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(unionUnit.get());
    bool tableIsPresented = std::any_of(unionUnitLocal->tableList().begin(), unionUnitLocal->tableList().end(), 
    [&table](const auto& unionTable) {
      return unionTable == table;
    });
    return tableIsPresented;
  });
}

bool matchParallelCES(execplan::CalpontSelectExecutionPlan& csep)
{
  auto tables = csep.tableList();
  // This is leaf and there are no other tables at this level.
  // WIP filter out CSEPs with orderBy, groupBy, having
  // WIP filter out CSEPs with nonSimpleColumns in projection
  return tables.size() == 1 && !tables[0].isColumnstore() && !tableIsInUnion(tables[0], csep);
}

execplan::CalpontSelectExecutionPlan::SelectList makeUnionFromTable(const size_t numberOfLegs,
                                          execplan::CalpontSelectExecutionPlan& csep)
{
  execplan::CalpontSelectExecutionPlan::SelectList unionVec;
  unionVec.reserve(numberOfLegs);
  for (size_t i = 0; i < numberOfLegs; ++i)
  {
    unionVec.push_back(csep.cloneWORecursiveSelects());
  }

  return unionVec;
}

void applyParallelCES(execplan::CalpontSelectExecutionPlan& csep)
{
  auto tables = csep.tableList();
  execplan::CalpontSelectExecutionPlan::TableList newTableList;
  execplan::CalpontSelectExecutionPlan::SelectList newDerivedTableList;
  static const std::string aliasPrefix = "$sub_";

  // ATM Must be only 1 table
  for (auto& table: tables)
  {
    if (!table.isColumnstore())
    {
      auto derivedSCEP = csep.cloneWORecursiveSelects();
      // need to intro a level
      std::string alias = aliasPrefix + table.schema + "_" + table.table;

      derivedSCEP->location(execplan::CalpontSelectExecutionPlan::FROM);
      derivedSCEP->subType(execplan::CalpontSelectExecutionPlan::FROM_SUBS);
      derivedSCEP->derivedTbAlias(alias);

      size_t parallelFactor = 2;
      auto additionalUnionVec = makeUnionFromTable(parallelFactor, csep);
      derivedSCEP->unionVec().insert(derivedSCEP->unionVec().end(), additionalUnionVec.begin(), additionalUnionVec.end());

      // change parent to derived table columns
      for (auto& rc : csep.returnedCols())
      {
        auto* sc = dynamic_cast<execplan::SimpleColumn*>(rc.get());
        if (sc)
        {
          sc->tableName("");
          sc->schemaName("");
          sc->tableAlias(alias);
          sc->colPosition(0);
        }
      }

      // WIP need to work with existing derived tables
      newDerivedTableList.push_back(derivedSCEP);
      // WIP
      execplan::CalpontSystemCatalog::TableAliasName tn = execplan::make_aliasview("", "", alias, "");
      newTableList.push_back(tn);
    }
  }

  // SimpleColumn* sc = new SimpleColumn("test", "i1", "i", false, csep.sessionID());
  // string alias(table->alias.c_ptr());
  // sc->timeZone(csep.timeZone());
  // sc->partitions(getPartitions(table));
  // boost::shared_ptr<SimpleColumn> spsc(sc);

  // csep.columnMap().insert({"`test`.`i1`.`i`", spsc});

  csep.derivedTableList(newDerivedTableList);
  csep.tableList(newTableList);
}
}


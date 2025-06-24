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

static const std::string RewrittenSubTableAliasPrefix = "$added_sub_";

// Apply a list of rules to a CSEP
bool optimizeCSEPWithRules(execplan::CalpontSelectExecutionPlan& root, const std::vector<Rule>& rules) {

  bool changed = false;
  for (const auto& rule : rules)
  {
    changed |= rule.apply(root);
  }
  return changed;
}

// high level API call for optimizer
bool optimizeCSEP(execplan::CalpontSelectExecutionPlan& root)
{
  optimizer::Rule parallelCES{"parallelCES", optimizer::matchParallelCES, optimizer::applyParallelCES};
  
  std::vector<Rule> rules = {parallelCES};

  return optimizeCSEPWithRules(root, rules);
}

// Apply iteratively until CSEP is converged by rule
bool Rule::apply(execplan::CalpontSelectExecutionPlan& root) const
{
  bool changedThisRound = false;
  bool hasBeenApplied = false;
  do
  {
    changedThisRound = walk(root);
    hasBeenApplied |= changedThisRound;
  } while (changedThisRound && !applyOnlyOnce);

  return hasBeenApplied;
}

// DFS walk to match CSEP and apply rules if match
bool Rule::walk(execplan::CalpontSelectExecutionPlan& csep) const
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
    rewrite |= walk(csepLocal);
  }

  for (auto& unionUnit : csep.unionVec())
  {
    auto* unionUnitPtr = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(unionUnit.get());
    if (!unionUnitPtr)
    {
      continue;
    }

    auto& unionUnitLocal = *unionUnitPtr;
    rewrite |= walk(unionUnitLocal);
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
  // This is leaf and there are no other tables at this level in neither UNION, nor derived table.
  // WIP filter out CSEPs with orderBy, groupBy, having
  // Filter out tables that were re-written.
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
  execplan::CalpontSelectExecutionPlan::ReturnedColumnList newReturnedColumns;

  // ATM Must be only 1 table
  for (auto& table: tables)
  {
    if (!table.isColumnstore())
    {
      auto derivedSCEP = csep.cloneWORecursiveSelects();
      // need to add a level here
      std::string tableAlias = RewrittenSubTableAliasPrefix + table.schema + "_" + table.table;

      derivedSCEP->location(execplan::CalpontSelectExecutionPlan::FROM);
      derivedSCEP->subType(execplan::CalpontSelectExecutionPlan::FROM_SUBS);
      derivedSCEP->derivedTbAlias(tableAlias);

      // TODO: hardcoded for now
      size_t parallelFactor = 2;
      auto additionalUnionVec = makeUnionFromTable(parallelFactor, csep);
      derivedSCEP->unionVec().insert(derivedSCEP->unionVec().end(), additionalUnionVec.begin(), additionalUnionVec.end());

      size_t colPosition = 0;
      // change parent to derived table columns
      for (auto& rc : csep.returnedCols())
      {
        auto rc_ = boost::make_shared<execplan::SimpleColumn>(*rc); 
        // TODO timezone and result type are not copied
        // TODO add specific ctor for this functionality
        rc_->tableName("");
        rc_->schemaName("");
        rc_->tableAlias(tableAlias);
        rc_->colPosition(colPosition++);
        rc_->resultType(rc->resultType());

        newReturnedColumns.push_back(rc_);
      }

      newDerivedTableList.push_back(derivedSCEP);
      execplan::CalpontSystemCatalog::TableAliasName tn = execplan::make_aliasview("", "", tableAlias, "");
      newTableList.push_back(tn);
    }
  }

  // There must be no derived at this point.
  csep.derivedTableList(newDerivedTableList);
  // Replace table list with new table list populated with union units
  csep.tableList(newTableList);
  csep.returnedCols(newReturnedColumns);
}

}


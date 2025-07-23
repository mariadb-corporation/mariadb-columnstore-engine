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

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "rulebased_optimizer.h"

#include "constantcolumn.h"
#include "execplan/calpontselectexecutionplan.h"
#include "execplan/simplecolumn.h"
#include "logicoperator.h"
#include "operator.h"
#include "predicateoperator.h"
#include "rbo_apply_parallel_ces.h"
#include "simplefilter.h"

namespace optimizer
{

void applyParallelCES_exists(execplan::CalpontSelectExecutionPlan& csep, const size_t id);

static const std::string RewrittenSubTableAliasPrefix = "$added_sub_";
static const size_t MaxParallelFactor = 16;

bool tableAliasEqual(const execplan::CalpontSystemCatalog::TableAliasName& lhs,
  const execplan::CalpontSystemCatalog::TableAliasName& rhs)
{
return (lhs.schema == rhs.schema && lhs.table == rhs.table && lhs.alias == rhs.alias &&
lhs.fisColumnStore == rhs.fisColumnStore);
}

bool tableIsInUnion(const execplan::CalpontSystemCatalog::TableAliasName& table,
                    execplan::CalpontSelectExecutionPlan& csep)
{
  return std::any_of(csep.unionVec().begin(), csep.unionVec().end(),
                     [&table](const auto& unionUnit)
                     {
                       execplan::CalpontSelectExecutionPlan* unionUnitLocal =
                           dynamic_cast<execplan::CalpontSelectExecutionPlan*>(unionUnit.get());
                       bool tableIsPresented =
                           std::any_of(unionUnitLocal->tableList().begin(), unionUnitLocal->tableList().end(),
                                       [&table](const auto& unionTable) { return unionTable == table; });
                       return tableIsPresented;
                     });
}

bool someAreForeignTables(execplan::CalpontSelectExecutionPlan& csep)
{
  return std::any_of(csep.tableList().begin(), csep.tableList().end(),
                     [](const auto& table) { return !table.isColumnstore(); });
}

bool matchParallelCES(execplan::CalpontSelectExecutionPlan& csep)
{
  auto tables = csep.tableList();
  // This is leaf and there are no other tables at this level in neither UNION, nor derived table.
  // TODO filter out CSEPs with orderBy, groupBy, having
  // Filter out tables that were re-written.
  // return tables.size() == 1 && !tables[0].isColumnstore() && !tableIsInUnion(tables[0], csep);
  return someAreForeignTables(csep);
}

// This routine produces a new ParseTree that is AND(lowerBand <= column, column <= upperBand)
// TODO add engine-independent statistics-derived ranges
execplan::ParseTree* filtersWithNewRangeAddedIfNeeded(execplan::SCSEP& csep, execplan::SimpleColumn& column,
                                                      std::pair<uint64_t, uint64_t>& bound)
{
  auto tableKeyColumnLeftOp = new execplan::SimpleColumn(column);
  tableKeyColumnLeftOp->resultType(column.resultType());

  // TODO Nobody owns this allocation and cleanup only depends on delete in ParseTree nodes' dtors.
  auto* filterColLeftOp = new execplan::ConstantColumnUInt(bound.second, 0, 0);
  // set TZ
  // There is a question with ownership of the const column
  // TODO here we lost upper bound value if predicate is not changed to weak lt
  execplan::SOP ltOp = boost::make_shared<execplan::Operator>(execplan::PredicateOperator("<"));
  ltOp->setOpType(filterColLeftOp->resultType(), tableKeyColumnLeftOp->resultType());
  ltOp->resultType(ltOp->operationType());

  auto* sfr = new execplan::SimpleFilter(ltOp, tableKeyColumnLeftOp, filterColLeftOp);
  auto tableKeyColumnRightOp = new execplan::SimpleColumn(column);
  tableKeyColumnRightOp->resultType(column.resultType());
  // TODO hardcoded column type and value
  auto* filterColRightOp = new execplan::ConstantColumnUInt(bound.first, 0, 0);

  execplan::SOP gtOp = boost::make_shared<execplan::Operator>(execplan::PredicateOperator(">="));
  gtOp->setOpType(filterColRightOp->resultType(), tableKeyColumnRightOp->resultType());
  gtOp->resultType(gtOp->operationType());

  auto* sfl = new execplan::SimpleFilter(gtOp, tableKeyColumnRightOp, filterColRightOp);

  execplan::ParseTree* ptp = new execplan::ParseTree(new execplan::LogicOperator("and"));
  ptp->right(sfr);
  ptp->left(sfl);

  auto* currentFilters = csep->filters();
  if (currentFilters)
  {
    execplan::ParseTree* andWithExistingFilters =
        new execplan::ParseTree(new execplan::LogicOperator("and"), currentFilters, ptp);
    return andWithExistingFilters;
  }

  return ptp;
}

// Looking for a projected column that comes first in an available index and has EI statistics
// INV nullptr signifies that no suitable column was found
execplan::SimpleColumn* findSuitableKeyColumn(execplan::CalpontSelectExecutionPlan& csep,
                                              execplan::CalpontSystemCatalog::TableAliasName& targetTable,
                                              optimizer::RBOptimizerContext& ctx)
{
  // TODO supply a list of suitable columns from a higher level
  for (auto& rc : csep.returnedCols())
  {
    // TODO extract SC from RC
    auto* simpleColumn = dynamic_cast<execplan::SimpleColumn*>(rc.get());
    if (simpleColumn)
    {
      execplan::CalpontSystemCatalog::TableAliasName rcTable(
          simpleColumn->schemaName(), simpleColumn->tableName(), simpleColumn->tableAlias(), "", false);
      if (!tableAliasEqual(targetTable, rcTable))
      {
        continue;
      }
      cal_impl_if::SchemaAndTableName schemaAndTableName = {simpleColumn->schemaName(),
                                                            simpleColumn->tableName()};

      auto columnStatistics = ctx.gwi.findStatisticsForATable(schemaAndTableName);
      if (!columnStatistics)
      {
        continue;
      }
      auto columnStatisticsIt = columnStatistics->find(simpleColumn->columnName());
      if (columnStatisticsIt != columnStatistics->end())
      {
        return simpleColumn;
      }
    }
  }

  return nullptr;
}

// TODO char and other numerical types support
execplan::CalpontSelectExecutionPlan::SelectList makeUnionFromTable(
    execplan::CalpontSelectExecutionPlan& csep, execplan::CalpontSystemCatalog::TableAliasName& table,
    optimizer::RBOptimizerContext& ctx)
{
  execplan::CalpontSelectExecutionPlan::SelectList unionVec;

  // SC type controls an integral type used to produce suitable filters. The continuation of this function
  // should become a template function based on SC type.
  execplan::SimpleColumn* keyColumn = findSuitableKeyColumn(csep, table, ctx);
  if (!keyColumn)
  {
    return unionVec;
  }

  // TODO char and other numerical types support
  std::vector<std::pair<uint64_t, uint64_t>> bounds;
  {
    cal_impl_if::SchemaAndTableName schemaAndTableName = {keyColumn->schemaName(), keyColumn->tableName()};
    auto tableColumnsStatisticsIt = ctx.gwi.tableStatisticsMap.find(schemaAndTableName);
    if (tableColumnsStatisticsIt == ctx.gwi.tableStatisticsMap.end())
    {
      return unionVec;
    }

    auto columnStatisticsIt = tableColumnsStatisticsIt->second.find(keyColumn->columnName());
    if (columnStatisticsIt == tableColumnsStatisticsIt->second.end())
    {
      return unionVec;
    }

    auto columnStatistics = columnStatisticsIt->second;

    // TODO configurable parallel factor via session variable
    // NB now histogram size is the way to control parallel factor with 16 being the maximum
    size_t numberOfUnionUnits = std::min(columnStatistics.get_json_histogram().size(), MaxParallelFactor);
    size_t numberOfBucketsPerUnionUnit = columnStatistics.get_json_histogram().size() / numberOfUnionUnits;

    // Loop over buckets to produce filter ranges
    for (size_t i = 0; i < numberOfUnionUnits - 1; ++i)
    {
      auto bucket = columnStatistics.get_json_histogram().begin() + i * numberOfBucketsPerUnionUnit;
      auto endBucket = columnStatistics.get_json_histogram().begin() + (i + 1) * numberOfBucketsPerUnionUnit;
      uint64_t currentLowerBound = *(uint32_t*)bucket->start_value.data();
      uint64_t currentUpperBound = *(uint32_t*)endBucket->start_value.data();
      bounds.push_back({currentLowerBound, currentUpperBound});
    }

    // Add last range
    // NB despite the fact that currently Histogram_json_hb has the last bucket that has end as its start
    auto lastBucket = columnStatistics.get_json_histogram().begin() +
                      (numberOfUnionUnits - 1) * numberOfBucketsPerUnionUnit;
    uint64_t currentLowerBound = *(uint32_t*)lastBucket->start_value.data();
    uint64_t currentUpperBound = *(uint32_t*)columnStatistics.get_last_bucket_end_endp().data();
    bounds.push_back({currentLowerBound, currentUpperBound});
  }

  for (auto& bound : bounds)
  {
    auto clonedCSEP = csep.cloneWORecursiveSelects();
    // Add BETWEEN based on key column range
    clonedCSEP->filters(filtersWithNewRangeAddedIfNeeded(clonedCSEP, *keyColumn, bound));
    unionVec.push_back(clonedCSEP);
  }

  return unionVec;
}
void applyParallelCES(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& ctx)
{
  auto tables = csep.tableList();
  execplan::CalpontSelectExecutionPlan::TableList newTableList;
  execplan::CalpontSelectExecutionPlan::SelectList newDerivedTableList;
  TableAliasMap tableAliasMap;

  for (auto& table : tables)
  {
    cal_impl_if::SchemaAndTableName schemaAndTableName = {table.schema, table.table};
    std::cout << "Processing table schema " << schemaAndTableName.schema << " table "
              << schemaAndTableName.table << " alias " << table.alias << std::endl;
    auto columnStatistics = ctx.gwi.findStatisticsForATable(schemaAndTableName);
    // TODO add column statistics check to the corresponding match
    if (!table.isColumnstore() && columnStatistics)
    {
      auto derivedSCEP = csep.cloneWORecursiveSelects();
      // need to add a level here
      std::string tableAlias = RewrittenSubTableAliasPrefix + table.schema + "_" + table.table + "_" +
                               std::to_string(ctx.uniqueId);
      // TODO add original alias to support multiple same name tables
      tableAliasMap.insert({table, {tableAlias, 0}});
      derivedSCEP->location(execplan::CalpontSelectExecutionPlan::FROM);
      derivedSCEP->subType(execplan::CalpontSelectExecutionPlan::FROM_SUBS);
      derivedSCEP->derivedTbAlias(tableAlias);

      // Create a copy of the current leaf CSEP with additional filters to partition the key column
      auto additionalUnionVec = makeUnionFromTable(csep, table, ctx);
      derivedSCEP->unionVec().insert(derivedSCEP->unionVec().end(), additionalUnionVec.begin(),
                                     additionalUnionVec.end());

      newDerivedTableList.push_back(derivedSCEP);
      execplan::CalpontSystemCatalog::TableAliasName tn = execplan::make_aliasview("", "", tableAlias, "");
      newTableList.push_back(tn);
      // Remove the filters as they were pushed down to union units
      // This is inappropriate for EXISTS filter and join conditions
      derivedSCEP->filters(nullptr);
    }
  }

  execplan::CalpontSelectExecutionPlan::ReturnedColumnList newReturnedColumns;
  // [[maybe_unused]] size_t colPosition = 0;
  // replace parent CSEP RCs with derived table RCs using ScheamAndTableName -> tableAlias map
  for (auto& rc : csep.returnedCols())
  {
    // TODO support expressions
    // Find SC for the RC
    auto rcCloned = boost::make_shared<execplan::SimpleColumn>(*rc);
    // TODO timezone and result type are not copied
    // TODO add specific ctor for this functionality
    // If there is an alias in the map then it is a new derived table
    auto sc = dynamic_cast<execplan::SimpleColumn*>(rc.get());
    std::vector<execplan::SimpleColumn*> scs;
    // execplan::ParseTree pt(rc.get());
    // pt.walk(execplan::getSimpleCols, &scs);

    // auto sc = scs[0];
    std::cout << "Processing RC schema " << sc->schemaName() << " table " << sc->tableName() << " alias "
              << sc->tableAlias() << std::endl;
    auto newTableAliasAndColPositionCounter =
        tableAliasMap.find({sc->schemaName(), sc->tableName(), sc->tableAlias(), "", false});
    if (newTableAliasAndColPositionCounter == tableAliasMap.end())
    {
      std::cout << "The RC doesn't belong to any of the derived tables, so leave it intact" << std::endl;
      continue;
    }
    sc->tableName("");
    sc->schemaName("");
    auto& [newTableAlias, colPosition] = newTableAliasAndColPositionCounter->second;
    sc->tableAlias(newTableAlias);
    // WIP Not needed according with CSEP output
    // sc->isColumnStore(true);
    sc->colPosition(colPosition++);
    // rcCloned->colPosition(colPosition++);
    // rcCloned->resultType(rc->resultType());
    // newReturnedColumns.push_back(rcCloned);
  }
  // Remove the filters if necessary using csep.filters(nullptr) as they were pushed down to union units
  // But this is inappropriate for EXISTS filter and join conditions
  // There must be no derived at this point, so we can replace it with the new derived table list
  csep.derivedTableList(newDerivedTableList);
  // Replace table list with new table list populated with union units
  csep.tableList(newTableList);
  // csep.returnedCols(newReturnedColumns);
}

}  // namespace optimizer

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
#include <limits>
#include <optional>
#include <vector>

#include "rulebased_optimizer.h"

#include "constantcolumn.h"
#include "execplan/calpontselectexecutionplan.h"
#include "execplan/simplecolumn.h"
#include "logicoperator.h"
#include "operator.h"
#include "predicateoperator.h"
#include "rbo_apply_parallel_ces.h"
#include "simplefilter.h"
#include "aggregatecolumn.h"
#include "arithmeticcolumn.h"
#include "functioncolumn.h"

namespace optimizer
{

template <typename T>
using FilterRangeBounds = std::vector<std::pair<T, T>>;

using SimpleColumnSet = std::set<execplan::SimpleColumn*>;
// Column position mapping: maps (schema, table, column) to (derived_table_alias, column_position)
using ColumnPositionMap =
    std::map<std::tuple<std::string, std::string, std::string>, std::pair<std::string, uint32_t>>;

void updateSimpleColumnReference(execplan::SimpleColumn* simpleCol, const std::string& newTableAlias,
                                 uint32_t colPosition)
{
  simpleCol->tableName("");
  simpleCol->schemaName("");
  simpleCol->tableAlias(newTableAlias);
  simpleCol->colPosition(colPosition);
}

boost::shared_ptr<execplan::SimpleColumn> createUpdatedSimpleColumn(execplan::SimpleColumn* originalCol,
                                                                    const std::string& newTableAlias,
                                                                    uint32_t colPosition)
{
  auto newSimpleCol = boost::make_shared<execplan::SimpleColumn>(*originalCol, originalCol->sessionID());
  updateSimpleColumnReference(newSimpleCol.get(), newTableAlias, colPosition);
  return newSimpleCol;
}

std::optional<std::pair<std::string, uint32_t>> lookupColumnPosition(
    execplan::SimpleColumn* simpleCol, const ColumnPositionMap& columnPositionMap,
    const TableAliasMap& tableAliasMap)
{
  // Try column-specific position mapping first
  std::tuple<std::string, std::string, std::string> columnKey =
      std::make_tuple(simpleCol->schemaName(), simpleCol->tableName(), simpleCol->columnName());

  auto columnPosIt = columnPositionMap.find(columnKey);
  if (columnPosIt != columnPositionMap.end())
  {
    return columnPosIt->second;
  }

  // Fallback to table-level mapping
  auto colTable = simpleCol->singleTable();
  if (colTable)
  {
    auto tableAliasIt = tableAliasMap.find(*colTable);
    if (tableAliasIt != tableAliasMap.end())
    {
      // Convert size_t to uint32_t for consistency
      return std::make_pair(tableAliasIt->second.first, static_cast<uint32_t>(tableAliasIt->second.second));
    }
  }

  return std::nullopt;
}

void applyParallelCES_exists(execplan::CalpontSelectExecutionPlan& csep, const size_t id);

static const std::string RewrittenSubTableAliasPrefix = "$added_sub_";
static const size_t MaxParallelFactor = 16;

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

bool someForeignTablesHasStatisticsAndMbIndex(execplan::CalpontSelectExecutionPlan& csep,
                                              optimizer::RBOptimizerContext& ctx)
{
  return std::any_of(
      csep.tableList().begin(), csep.tableList().end(),
      [&ctx](const auto& table)
      {
        cal_impl_if::SchemaAndTableName schemaAndTableName = {table.schema, table.table};
        return (!table.isColumnstore() &&
                ctx.gwi.tableStatisticsMap.find(schemaAndTableName) != ctx.gwi.tableStatisticsMap.end());
      });
}

bool matchParallelCES(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx)
{
  // TODO filter out CSEPs with orderBy, groupBy, having || or clean up OB,GB,HAVING cloning CSEP
  // Filter out tables that were re-written.
  return someAreForeignTables(csep) && someForeignTablesHasStatisticsAndMbIndex(csep, ctx);
}

// This routine produces a new ParseTree that is AND(lowerBand <= column, column <= upperBand)
// TODO add engine-independent statistics-derived ranges
execplan::ParseTree* filtersWithNewRange(execplan::SCSEP& csep, execplan::SimpleColumn& column,
                                         std::pair<uint64_t, uint64_t>& bound, bool isLast)
{
  auto tableKeyColumnLeftOp = new execplan::SimpleColumn(column);
  tableKeyColumnLeftOp->resultType(column.resultType());

  // TODO Nobody owns this allocation and cleanup only depends on delete in ParseTree nodes' dtors.
  auto* filterColLeftOp = new execplan::ConstantColumnUInt(bound.second, 0, 0);
  // set TZ
  // There is a question with ownership of the const column
  // TODO here we lost upper bound value if predicate is not changed to weak lt
  execplan::SOP ltOp = (isLast) ? boost::make_shared<execplan::Operator>(execplan::PredicateOperator("<="))
                                : boost::make_shared<execplan::Operator>(execplan::PredicateOperator("<"));
  ltOp->setOpType(filterColLeftOp->resultType(), tableKeyColumnLeftOp->resultType());
  ltOp->resultType(ltOp->operationType());

  auto* sfr = new execplan::SimpleFilter(ltOp, tableKeyColumnLeftOp, filterColLeftOp);
  // TODO new
  // TODO remove new and re-use tableKeyColumnLeftOp
  auto tableKeyColumnRightOp = new execplan::SimpleColumn(column);
  tableKeyColumnRightOp->resultType(column.resultType());
  // TODO hardcoded column type and value
  auto* filterColRightOp = new execplan::ConstantColumnUInt(bound.first, 0, 0);

  execplan::SOP gtOp = boost::make_shared<execplan::Operator>(execplan::PredicateOperator(">="));
  gtOp->setOpType(filterColRightOp->resultType(), tableKeyColumnRightOp->resultType());
  gtOp->resultType(gtOp->operationType());

  // TODO new
  auto* sfl = new execplan::SimpleFilter(gtOp, tableKeyColumnRightOp, filterColRightOp);

  // TODO new
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
      if (!targetTable.weakerEq(rcTable))
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

// TBD
Histogram_json_hb* chooseStatisticsToUse(std::vector<Histogram_json_hb*>& columnStatisticsVec)
{
  return columnStatisticsVec.front();
}

// Looking for a projected column that comes first in an available index and has EI statistics
// INV nullptr signifies that no suitable column was found
std::optional<std::pair<execplan::SimpleColumn&, Histogram_json_hb*>> chooseKeyColumnAndStatistics(
    execplan::CalpontSystemCatalog::TableAliasName& targetTable, optimizer::RBOptimizerContext& ctx)
{
  cal_impl_if::SchemaAndTableName schemaAndTableName = {targetTable.schema, targetTable.table};

  auto tableColumnsStatisticsIt = ctx.gwi.tableStatisticsMap.find(schemaAndTableName);
  if (tableColumnsStatisticsIt == ctx.gwi.tableStatisticsMap.end() ||
      tableColumnsStatisticsIt->second.empty())
  {
    return std::nullopt;
  }

  // TODO take some column and some stats for it!!!
  for (auto& [columnName, scAndStatisticsVec] : tableColumnsStatisticsIt->second)
  {
    auto& [sc, columnStatisticsVec] = scAndStatisticsVec;
    auto* columnStatistics = chooseStatisticsToUse(columnStatisticsVec);
    return {{sc, columnStatistics}};
  }

  return std::nullopt;
}

// Populates range bounds based on column statistics
// Returns optional with bounds if successful, nullopt otherwise
template <typename T>
std::optional<FilterRangeBounds<T>> populateRangeBounds(Histogram_json_hb* columnStatistics)
{
  FilterRangeBounds<T> bounds;

  // TODO configurable parallel factor via session variable
  // NB now histogram size is the way to control parallel factor with 16 being the maximum
  std::cout << "populateRangeBounds() columnStatistics->buckets.size() "
            << columnStatistics->get_json_histogram().size() << std::endl;
  size_t numberOfUnionUnits = std::min(columnStatistics->get_json_histogram().size(), MaxParallelFactor);
  size_t numberOfBucketsPerUnionUnit = columnStatistics->get_json_histogram().size() / numberOfUnionUnits;

  std::cout << "Number of union units: " << numberOfUnionUnits << std::endl;
  std::cout << "Number of buckets per union unit: " << numberOfBucketsPerUnionUnit << std::endl;

  // Loop over buckets to produce filter ranges
  // NB Currently Histogram_json_hb has the last bucket that has end as its start
  for (size_t i = 0; i < numberOfUnionUnits - 1; ++i)
  {
    auto bucket = columnStatistics->get_json_histogram().begin() + i * numberOfBucketsPerUnionUnit;
    auto endBucket = columnStatistics->get_json_histogram().begin() + (i + 1) * numberOfBucketsPerUnionUnit;
    T currentLowerBound = *(uint32_t*)bucket->start_value.data();
    T currentUpperBound = *(uint32_t*)endBucket->start_value.data();
    bounds.push_back({currentLowerBound, currentUpperBound});
  }

  // This covers from the last bucket start to the maximum value
  if (numberOfUnionUnits > 0)
  {
    auto lastBucket = columnStatistics->get_json_histogram().begin() +
                      (numberOfUnionUnits - 1) * numberOfBucketsPerUnionUnit;
    T lastLowerBound = *(uint32_t*)lastBucket->start_value.data();
    T lastUpperBound = std::numeric_limits<T>::max();

    if (!columnStatistics->get_last_bucket_end_endp().empty())
    {
      lastUpperBound = *(uint32_t*)columnStatistics->get_last_bucket_end_endp().data();
    }

    bounds.push_back({lastLowerBound, lastUpperBound});
    std::cout << "Added last range: [" << lastLowerBound << ", " << lastUpperBound << "]" << std::endl;
  }

  for (auto& bound : bounds)
  {
    std::cout << "Bound: " << bound.first << " " << bound.second << std::endl;
  }

  return bounds;
}

// Forward declarations for aggregate support functions
SimpleColumnSet extractColumnsFromAggregates(execplan::CalpontSelectExecutionPlan& csep);
SimpleColumnSet extractColumnsFromGroupBy(execplan::CalpontSelectExecutionPlan& csep);
SimpleColumnSet extractColumnsFromOrderBy(execplan::CalpontSelectExecutionPlan& csep);
void addColumnsToSubqueries(execplan::CalpontSelectExecutionPlan& subqueryCsep,
                            const SimpleColumnSet& requiredColumns,
                            const execplan::CalpontSystemCatalog::TableAliasName& targetTable);

using SimpleColumnSet = std::set<execplan::SimpleColumn*>;

// Column position mapping: maps (schema, table, column) to (derived_table_alias, column_position)
using ColumnPositionMap =
    std::map<std::tuple<std::string, std::string, std::string>, std::pair<std::string, uint32_t>>;

// TODO char and other numerical types support
execplan::CalpontSelectExecutionPlan::SelectList makeUnionFromTable(
    execplan::CalpontSelectExecutionPlan& csep, execplan::CalpontSystemCatalog::TableAliasName& table,
    optimizer::RBOptimizerContext& ctx)
{
  execplan::CalpontSelectExecutionPlan::SelectList unionVec;

  // SC type controls an integral type used to produce suitable filters. The continuation of this function
  // should become a template function based on SC type.
  auto keyColumnAndStatistics = chooseKeyColumnAndStatistics(table, ctx);
  if (!keyColumnAndStatistics)
  {
    return unionVec;
  }

  auto& [keyColumn, columnStatistics] = keyColumnAndStatistics.value();

  std::cout << "makeUnionFromTable keyColumn " << keyColumn.toString() << std::endl;
  std::cout << "makeUnionFromTable RC front " << csep.returnedCols().front()->toString() << std::endl;

  // TODO char and other numerical types support
  auto boundsOpt = populateRangeBounds<uint64_t>(columnStatistics);
  if (!boundsOpt.has_value())
  {
    return unionVec;
  }

  auto& bounds = boundsOpt.value();

  // These bounds produce low <= col < high
  if (bounds.size() > 1)
  {
    for (size_t i = 0; i <= bounds.size() - 2; ++i)
    {
      auto clonedCSEP = csep.cloneForTableWORecursiveSelectsGbObHaving(table);
      // Add BETWEEN based on key column range
      auto filter = filtersWithNewRange(clonedCSEP, keyColumn, bounds[i], false);
      clonedCSEP->filters(filter);
      // To create CES filter we need to have a column in the column map
      clonedCSEP->columnMap().insert({keyColumn.columnName(), execplan::SRCP(keyColumn.clone())});
      unionVec.push_back(clonedCSEP);
    }
  }
  // This last bound produces low <= col <= high
  // TODO add NULLs into filter of the last step
  if (!bounds.empty())
  {
    auto clonedCSEP = csep.cloneForTableWORecursiveSelectsGbObHaving(table);
    auto filter = filtersWithNewRange(clonedCSEP, keyColumn, bounds.back(), true);
    clonedCSEP->columnMap().insert({keyColumn.columnName(), execplan::SRCP(keyColumn.clone())});
    clonedCSEP->filters(filter);
    unionVec.push_back(clonedCSEP);
  }

  return unionVec;
}

execplan::SCSEP createDerivedTableFromTable(execplan::CalpontSelectExecutionPlan& csep,
                                            const execplan::CalpontSystemCatalog::TableAliasName& table,
                                            const std::string& tableAlias, optimizer::RBOptimizerContext& ctx,
                                            const SimpleColumnSet& requiredColumns = SimpleColumnSet())
{
  // Don't copy filters for this
  auto derivedSCEP = csep.cloneForTableWORecursiveSelectsGbObHaving(table, false);
  // Remove the filters as they were pushed down to union units
  // This is inappropriate for EXISTS filter and join conditions
  // WIP replace with filters applied to filters, so that only relevant filters are left
  // WIP Ugly hack to avoid leaks
  auto* derivedCSEP = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(derivedSCEP.get());
  // TODO more rigorous error handling.
  if (!derivedCSEP)
  {
    return execplan::SCSEP();
  }

  // Add required columns for aggregates and GROUP BY to the subquery
  addColumnsToSubqueries(*derivedCSEP, requiredColumns, table);

  auto additionalUnionVec = makeUnionFromTable(
      *derivedCSEP, const_cast<execplan::CalpontSystemCatalog::TableAliasName&>(table), ctx);

  // TODO add original alias to support multiple same name tables
  derivedSCEP->location(execplan::CalpontSelectExecutionPlan::FROM);
  derivedSCEP->subType(execplan::CalpontSelectExecutionPlan::FROM_SUBS);
  derivedSCEP->derivedTbAlias(tableAlias);

  derivedSCEP->unionVec().insert(derivedSCEP->unionVec().end(), additionalUnionVec.begin(),
                                 additionalUnionVec.end());

  return derivedSCEP;
}

SimpleColumnSet extractColumnsFromAggregates(execplan::CalpontSelectExecutionPlan& csep)
{
  SimpleColumnSet aggregateColumns;

  for (const auto& rc : csep.returnedCols())
  {
    if (!rc)
      continue;

    if (auto* aggCol = dynamic_cast<execplan::AggregateColumn*>(rc.get()))
    {
      // Extract columns from aggregate parameters
      for (const auto& parm : aggCol->aggParms())
      {
        if (!parm)
          continue;

        if (auto* simpleCol = dynamic_cast<execplan::SimpleColumn*>(parm.get()))
        {
          aggregateColumns.insert(simpleCol);
        }
        else
        {
          // Handle complex expressions by extracting SimpleColumns
          parm->setSimpleColumnList();
          for (auto* sc : parm->simpleColumnList())
          {
            if (sc)
              aggregateColumns.insert(sc);
          }
        }
      }
    }
    else
    {
      // Handle complex returned columns that might contain aggregates
      rc->setSimpleColumnList();
      for (auto* sc : rc->simpleColumnList())
      {
        if (sc)
          aggregateColumns.insert(sc);
      }
    }
  }

  return aggregateColumns;
}

SimpleColumnSet extractColumnsFromGroupBy(execplan::CalpontSelectExecutionPlan& csep)
{
  SimpleColumnSet groupByColumns;

  for (const auto& gbCol : csep.groupByCols())
  {
    if (!gbCol)
      continue;

    if (auto* simpleCol = dynamic_cast<execplan::SimpleColumn*>(gbCol.get()))
    {
      groupByColumns.insert(simpleCol);
    }
    else
    {
      // Handle complex GROUP BY expressions
      gbCol->setSimpleColumnList();
      for (auto* sc : gbCol->simpleColumnList())
      {
        if (sc)
          groupByColumns.insert(sc);
      }
    }
  }

  return groupByColumns;
}

SimpleColumnSet extractColumnsFromOrderBy(execplan::CalpontSelectExecutionPlan& csep)
{
  SimpleColumnSet orderByColumns;

  for (const auto& obCol : csep.orderByCols())
  {
    if (!obCol)
      continue;

    if (auto* simpleCol = dynamic_cast<execplan::SimpleColumn*>(obCol.get()))
    {
      // Only include ORDER BY columns that reference actual table columns
      // Skip columns that might be aliases (they don't have table references)
      if (simpleCol->singleTable())
      {
        orderByColumns.insert(simpleCol);
      }
    }
    else
    {
      // Handle complex ORDER BY expressions
      obCol->setSimpleColumnList();
      for (auto* sc : obCol->simpleColumnList())
      {
        if (sc && sc->singleTable())
          orderByColumns.insert(sc);
      }
    }
  }

  return orderByColumns;
}

/**
 * This function creates a mapping from (schema, table, column) to (derived_table_alias, position)
 * based on the order of columns in the derived table's returned columns list.
 */
ColumnPositionMap buildColumnPositionMapping(
    execplan::CalpontSelectExecutionPlan& derivedTable, const std::string& derivedTableAlias,
    const execplan::CalpontSystemCatalog::TableAliasName& originalTable)
{
  ColumnPositionMap columnPositionMap;
  uint32_t position = 0;

  for (auto& rc : derivedTable.returnedCols())
  {
    if (!rc)
      continue;

    auto* simpleCol = dynamic_cast<execplan::SimpleColumn*>(rc.get());
    if (simpleCol)
    {
      // Create mapping from (schema, table, column) to (derived_table_alias, position)
      std::tuple<std::string, std::string, std::string> columnKey =
          std::make_tuple(simpleCol->schemaName(), simpleCol->tableName(), simpleCol->columnName());

      columnPositionMap[columnKey] = std::make_pair(derivedTableAlias, position);

      std::cout << "Column mapping: " << simpleCol->schemaName() << "." << simpleCol->tableName() << "."
                << simpleCol->columnName() << " -> " << derivedTableAlias << "[" << position << "]"
                << std::endl;
    }
    position++;
  }

  return columnPositionMap;
}

void addColumnsToSubqueries(execplan::CalpontSelectExecutionPlan& subqueryCsep,
                            const SimpleColumnSet& requiredColumns,
                            const execplan::CalpontSystemCatalog::TableAliasName& targetTable)
{
  if (requiredColumns.empty())
    return;

  execplan::CalpontSelectExecutionPlan::ReturnedColumnList newReturnedCols = subqueryCsep.returnedCols();

  // Track which columns are already present to avoid duplicates
  std::set<std::string> existingColumns;
  for (const auto& rc : newReturnedCols)
  {
    if (rc)
    {
      existingColumns.insert(rc->data());
    }
  }

  // Add required columns that aren't already present
  for (auto* reqCol : requiredColumns)
  {
    if (!reqCol)
      continue;

    // Check if this column belongs to the target table
    auto colTable = reqCol->singleTable();
    if (!colTable || !targetTable.weakerEq(*colTable))
      continue;

    // Check if column is already present
    if (existingColumns.find(reqCol->data()) != existingColumns.end())
      continue;

    // Clone the column and add it to the subquery
    auto newCol = boost::make_shared<execplan::SimpleColumn>(*reqCol, reqCol->sessionID());
    newReturnedCols.push_back(newCol);
    existingColumns.insert(reqCol->data());
  }

  subqueryCsep.returnedCols(newReturnedCols);
}

void applyParallelCES(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx)
{
  // Extract columns needed for aggregates, GROUP BY, and ORDER BY before processing tables
  auto aggregateColumns = extractColumnsFromAggregates(csep);
  auto groupByColumns = extractColumnsFromGroupBy(csep);
  auto orderByColumns = extractColumnsFromOrderBy(csep);

  // Combine all required columns
  SimpleColumnSet requiredColumns;
  requiredColumns.insert(aggregateColumns.begin(), aggregateColumns.end());
  requiredColumns.insert(groupByColumns.begin(), groupByColumns.end());
  requiredColumns.insert(orderByColumns.begin(), orderByColumns.end());

  bool hasGroupBy = !groupByColumns.empty();

  std::cout << "Found " << aggregateColumns.size() << " aggregate columns, " << groupByColumns.size()
            << " GROUP BY columns, and " << orderByColumns.size() << " ORDER BY table columns" << std::endl;
  std::cout << "Total ORDER BY columns: " << csep.orderByCols().size() << std::endl;

  auto tables = csep.tableList();
  auto existingDerivedTables = csep.derivedTableList();

  std::cout << "Found " << tables.size() << " base tables and " << existingDerivedTables.size()
            << " existing derived tables" << std::endl;

  execplan::CalpontSelectExecutionPlan::TableList newTableList;
  execplan::CalpontSelectExecutionPlan::SelectList newDerivedTableList;
  optimizer::TableAliasMap tableAliasMap;  // Keep for backward compatibility
  ColumnPositionMap columnPositionMap;     // New column-specific position mapping

  // First, preserve existing derived tables (subqueries) as they are
  // These represent subqueries in the FROM clause that should not be modified by parallel rewrite
  for (auto& derivedTable : existingDerivedTables)
  {
    std::cout << "Preserving existing derived table (subquery)" << std::endl;
    newDerivedTableList.push_back(derivedTable);

    // Note: We don't add existing derived tables to tableAliasMap because they should
    // maintain their original column references and not be rewritten for parallel execution
  }

  for (auto& table : tables)
  {
    cal_impl_if::SchemaAndTableName schemaAndTableName = {table.schema, table.table};
    std::cout << "Processing table schema " << schemaAndTableName.schema << " table "
              << schemaAndTableName.table << " alias " << table.alias << std::endl;
    auto anyColumnStatistics = ctx.gwi.findStatisticsForATable(schemaAndTableName);
    std::cout << "Column statistics: " << anyColumnStatistics.has_value() << std::endl;
    // TODO add column statistics check to the corresponding match
    if (!table.isColumnstore() && anyColumnStatistics)
    {
      std::string tableAlias = optimizer::RewrittenSubTableAliasPrefix + table.schema + "_" + table.table +
                               "_" + std::to_string(ctx.uniqueId);
      tableAliasMap.insert({table, {tableAlias, 0}});
      execplan::CalpontSystemCatalog::TableAliasName tn = execplan::make_aliasview("", "", tableAlias, "");
      newTableList.push_back(tn);

      auto derivedSCEP = createDerivedTableFromTable(csep, table, tableAlias, ctx, requiredColumns);

      // Build column position mapping for this derived table
      auto* derivedCSEP = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(derivedSCEP.get());
      if (derivedCSEP)
      {
        auto tableColumnMapping = buildColumnPositionMapping(*derivedCSEP, tableAlias, table);
        columnPositionMap.insert(tableColumnMapping.begin(), tableColumnMapping.end());
      }

      newDerivedTableList.push_back(std::move(derivedSCEP));
    }
    else
    {
      newTableList.push_back(table);
    }
  }

  // Process GROUP BY columns FIRST to ensure consistency with returned columns
  execplan::CalpontSelectExecutionPlan::GroupByColumnList newGroupByCols;
  if (hasGroupBy && !csep.groupByCols().empty() && !newDerivedTableList.empty())
  {
    std::cout << "Updating GROUP BY columns (before returned columns)" << std::endl;

    for (auto& gbCol : csep.groupByCols())
    {
      if (!gbCol)
        continue;

      auto* simpleCol = dynamic_cast<execplan::SimpleColumn*>(gbCol.get());
      if (simpleCol)
      {
        // Use helper function to look up column position mapping
        auto positionMapping = lookupColumnPosition(simpleCol, columnPositionMap, tableAliasMap);
        if (positionMapping)
        {
          auto& [newTableAlias, colPosition] = *positionMapping;
          auto newSimpleCol = createUpdatedSimpleColumn(simpleCol, newTableAlias, colPosition);
          newGroupByCols.push_back(newSimpleCol);
          std::cout << "Updated GROUP BY column: " << simpleCol->columnName() << " -> " << newTableAlias
                    << "[" << colPosition << "]" << std::endl;
        }
        else
        {
          newGroupByCols.push_back(gbCol);
        }
      }
      else
      {
        // Handle complex GROUP BY expressions
        auto clonedGbCol = execplan::SRCP(gbCol->clone());
        clonedGbCol->setSimpleColumnList();
        for (auto* sc : clonedGbCol->simpleColumnList())
        {
          if (sc)
          {
            auto colTable = sc->singleTable();
            if (colTable)
            {
              auto tableAliasIt = tableAliasMap.find(*colTable);
              if (tableAliasIt != tableAliasMap.end())
              {
                auto& [newTableAlias, colPosition] = tableAliasIt->second;
                updateSimpleColumnReference(sc, newTableAlias, colPosition);
              }
            }
          }
        }
        newGroupByCols.push_back(clonedGbCol);
      }
    }

    // Update the CSEP with new GROUP BY columns
    csep.groupByCols(newGroupByCols);
  }

  execplan::CalpontSelectExecutionPlan::ReturnedColumnList newReturnedColumns;
  // replace parent CSEP RCs with derived table RCs using ScheamAndTableName -> tableAlias map
  if (!newDerivedTableList.empty())
  {
    std::cout << "Iterating over RCs" << std::endl;
    for (auto& rc : csep.returnedCols())
    {
      // Check if this is an AggregateColumn - these need special handling
      auto* aggCol = dynamic_cast<execplan::AggregateColumn*>(rc.get());
      if (aggCol)
      {
        std::cout << "Processing AggregateColumn: " << aggCol->data() << std::endl;
        // For aggregate columns, we need to update the table references in their parameters
        // but keep the aggregate function at the top level
        auto newAggCol = boost::make_shared<execplan::AggregateColumn>(*aggCol, aggCol->sessionID());

        // Update table references in aggregate parameters
        execplan::AggParms newParms;
        for (const auto& parm : aggCol->aggParms())
        {
          if (!parm)
            continue;

          auto* simpleCol = dynamic_cast<execplan::SimpleColumn*>(parm.get());
          if (simpleCol)
          {
            // Use helper function to look up column position mapping
            auto positionMapping = lookupColumnPosition(simpleCol, columnPositionMap, tableAliasMap);
            if (positionMapping)
            {
              auto& [newTableAlias, colPosition] = *positionMapping;
              auto newSimpleCol = createUpdatedSimpleColumn(simpleCol, newTableAlias, colPosition);
              newParms.push_back(newSimpleCol);

              std::cout << "Updated aggregate parameter: " << simpleCol->columnName() << " -> "
                        << newTableAlias << "[" << colPosition << "]" << std::endl;
            }
            else
            {
              newParms.push_back(parm);
            }
          }
          else
          {
            // Handle complex expressions (ArithmeticColumn, etc.) that may contain nested SimpleColumns
            auto clonedParm = execplan::SRCP(parm->clone());

            // Recursively update nested SimpleColumns in the expression
            clonedParm->setSimpleColumnList();
            for (auto* sc : clonedParm->simpleColumnList())
            {
              if (sc)
              {
                // Use helper function to look up and update column position mapping
                auto positionMapping = lookupColumnPosition(sc, columnPositionMap, tableAliasMap);
                if (positionMapping)
                {
                  auto& [newTableAlias, colPosition] = *positionMapping;
                  updateSimpleColumnReference(sc, newTableAlias, colPosition);
                  std::cout << "Updated nested column in aggregate expression: " << sc->columnName() << " -> "
                            << newTableAlias << "[" << colPosition << "]" << std::endl;
                }
              }
            }
            newParms.push_back(clonedParm);
          }
        }

        newAggCol->aggParms(newParms);
        newReturnedColumns.push_back(newAggCol);
      }
      else
      {
        auto sameTableAliasOpt = rc->singleTable();
        // Same table so RC was pushed into UNION units and can be replaced with new derived table SC
        if (sameTableAliasOpt)
        {
          std::cout << "RC table schema " << sameTableAliasOpt->schema << " table "
                    << sameTableAliasOpt->table << " alias " << sameTableAliasOpt->alias << std::endl;
          auto tableAliasIt = tableAliasMap.find(*sameTableAliasOpt);
          if (tableAliasIt != tableAliasMap.end())
          {
            std::cout << "Replacing RC with new SC" << std::endl;

            // Check if this column is in GROUP BY - if so, we need to ensure consistency
            bool isInGroupBy = false;
            execplan::SRCP matchingGroupByCol;

            if (hasGroupBy && !csep.groupByCols().empty())
            {
              for (auto& gbCol : csep.groupByCols())
              {
                if (gbCol)
                {
                  auto* gbSimpleCol = dynamic_cast<execplan::SimpleColumn*>(gbCol.get());
                  auto* rcSimpleCol = dynamic_cast<execplan::SimpleColumn*>(rc.get());
                  if (gbSimpleCol && rcSimpleCol && gbSimpleCol->columnName() == rcSimpleCol->columnName())
                  {
                    // Since GROUP BY columns are already updated, we can directly compare
                    isInGroupBy = true;
                    matchingGroupByCol = gbCol;
                    std::cout << "Column " << rcSimpleCol->columnName()
                              << " found in GROUP BY (already updated)" << std::endl;
                    break;
                  }
                }
              }
            }

            // If this column is in GROUP BY, use the GROUP BY column reference to ensure consistency
            if (isInGroupBy && matchingGroupByCol)
            {
              std::cout << "Using GROUP BY column reference for consistency" << std::endl;
              auto clonedGroupByCol = execplan::SRCP(matchingGroupByCol->clone());
              clonedGroupByCol->alias(rc->alias());  // Preserve original alias
              newReturnedColumns.push_back(clonedGroupByCol);
            }
            else
            {
              // add new SC
              auto& [newTableAlias, colPosition] = tableAliasIt->second;
              auto newSC = boost::make_shared<execplan::SimpleColumn>(*rc, rc->sessionID());
              newSC->tableName("");
              newSC->schemaName("");
              newSC->tableAlias(newTableAlias);
              newSC->colPosition(colPosition++);
              newSC->alias(rc->alias());
              newReturnedColumns.push_back(newSC);
            }
          }
          // RC doesn't belong to any of the new derived tables
          else
          {
            std::cout << "RC doesn't belong to any of the new derived tables" << std::endl;
            newReturnedColumns.push_back(rc);
          }
        }
        // if SCs belong to different tables
        else
        {
          rc->setSimpleColumnList();
          for (auto* sc : rc->simpleColumnList())
          {
            // TODO add method to SC to get table alias
            // auto scTableAliasOpt = sc->singleTable();
            auto tableAliasIt = tableAliasMap.find(*sc->singleTable());
            // Need a method to replace original SCs in the SClist
            if (tableAliasIt != tableAliasMap.end())
            {
              auto& [newTableAlias, colPosition] = tableAliasIt->second;
              updateSimpleColumnReference(sc, newTableAlias, colPosition++);
            }
            // do nothing with this SC
          }
          newReturnedColumns.push_back(rc);
        }
      }
    }
    // Remove the filters that are not necessary as they were pushed down to union units.
    // But this is inappropriate for some EXISTS filter and join conditions

    // WIP hardcoded query with lhs,rhs being simple columns
    if (csep.filters() && csep.filters()->data())
    {
      auto* left = dynamic_cast<execplan::SimpleFilter*>(csep.filters()->data());
      if (left)
      {
        auto* lhs = left->lhs()->clone();
        if (lhs)
        {
          auto* lhsSC = dynamic_cast<execplan::SimpleColumn*>(lhs);
          if (lhsSC)
          {
            // Use helper function to look up and update column position mapping
            auto positionMapping = lookupColumnPosition(lhsSC, columnPositionMap, tableAliasMap);
            if (positionMapping)
            {
              auto& [newTableAlias, colPosition] = *positionMapping;
              std::cout << "Updating WHERE clause filter column: " << lhsSC->columnName() << " -> "
                        << newTableAlias << "[" << colPosition << "]" << std::endl;
              updateSimpleColumnReference(lhsSC, newTableAlias, colPosition);
              left->lhs(lhs);
            }
          }
        }
      }
    }

    // GROUP BY columns are already processed earlier for consistency

    // Helper function to recursively update SimpleColumns in any ReturnedColumn
    std::function<void(execplan::ReturnedColumn*)> updateNestedColumns =
        [&](execplan::ReturnedColumn* col) -> void
    {
      if (!col)
        return;

      std::cout << "Recursively processing column of type: " << typeid(*col).name() << std::endl;

      // Handle SimpleColumn directly
      auto* simpleCol = dynamic_cast<execplan::SimpleColumn*>(col);
      if (simpleCol)
      {
        std::cout << "Found SimpleColumn: " << simpleCol->columnName() << std::endl;

        // Use helper function to look up and update column position mapping
        auto positionMapping = lookupColumnPosition(simpleCol, columnPositionMap, tableAliasMap);
        if (positionMapping)
        {
          auto& [newTableAlias, colPosition] = *positionMapping;
          std::cout << "Updating ORDER BY column: " << simpleCol->columnName() << " -> " << newTableAlias
                    << "[" << colPosition << "]" << std::endl;
          updateSimpleColumnReference(simpleCol, newTableAlias, colPosition);
        }
        else
        {
          std::cout << "No position mapping found for column: " << simpleCol->columnName() << std::endl;
        }
        return;
      }

      // Handle AggregateColumn
      auto* aggCol = dynamic_cast<execplan::AggregateColumn*>(col);
      if (aggCol)
      {
        std::cout << "Found AggregateColumn, processing aggregated parameters" << std::endl;
        // Update the aggregated column if it exists
        if (aggCol->aggParms().size() > 0)
        {
          std::cout << "AggregateColumn has " << aggCol->aggParms().size() << " parameters" << std::endl;
          updateNestedColumns(aggCol->aggParms()[0].get());
        }
        else
        {
          std::cout << "AggregateColumn has no parameters" << std::endl;
        }
        return;
      }

      // Handle ArithmeticColumn
      auto* arithCol = dynamic_cast<execplan::ArithmeticColumn*>(col);
      if (arithCol)
      {
        std::cout << "Found ArithmeticColumn, processing expression tree" << std::endl;
        // For ArithmeticColumn, we need to traverse the ParseTree
        // But since ParseTree traversal is complex, let's use simpleColumnList as fallback
        arithCol->setSimpleColumnList();
        auto simpleColumns = arithCol->simpleColumnList();
        std::cout << "ArithmeticColumn has " << simpleColumns.size() << " simple columns" << std::endl;
        for (auto* sc : simpleColumns)
        {
          updateNestedColumns(sc);
        }
        return;
      }

      std::cout << "Unknown column type, cannot process further" << std::endl;
    };

    // Update ORDER BY columns to reference derived tables
    if (!csep.orderByCols().empty())
    {
      std::cout << "Processing " << csep.orderByCols().size() << " ORDER BY columns" << std::endl;
      execplan::CalpontSelectExecutionPlan::OrderByColumnList newOrderByCols;
      for (size_t i = 0; i < csep.orderByCols().size(); ++i)
      {
        auto& obCol = csep.orderByCols()[i];
        if (!obCol)
        {
          std::cout << "ORDER BY column " << i << " is null, skipping" << std::endl;
          continue;
        }

        std::cout << "Processing ORDER BY column " << i << " of type: " << typeid(*obCol).name() << std::endl;

        // Clone the ORDER BY column to avoid modifying the original
        auto clonedObCol = execplan::SRCP(obCol->clone());

        // Recursively update all nested SimpleColumns
        updateNestedColumns(clonedObCol.get());

        newOrderByCols.push_back(clonedObCol);
      }

      csep.orderByCols(newOrderByCols);
      std::cout << "Finished updating ORDER BY columns" << std::endl;
    }
    else
    {
      std::cout << "No ORDER BY columns to process" << std::endl;
    }

    csep.derivedTableList(newDerivedTableList);
    // Replace table list with new table list populated with union units
    csep.tableList(newTableList);
    csep.returnedCols(newReturnedColumns);
  }
}

}  // namespace optimizer

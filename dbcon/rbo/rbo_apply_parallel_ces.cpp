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
#include <memory>
#include <optional>
#include <set>
#include <vector>

#include "rulebased_optimizer.h"

#include "constantcolumn.h"
#include "execplan/calpontselectexecutionplan.h"
#include "execplan/simplecolumn.h"
#include "logicoperator.h"
#include "operator.h"
#include "predicateoperator.h"
#include "lib/filter_builders.h"
#include "lib/parse_tree_ops.h"
#include "rbo_apply_parallel_ces.h"
#include "returnedcolumn.h"
#include "simplefilter.h"
#include "existsfilter.h"
#include "outerjoinonfilter.h"
#include "selectfilter.h"
#include "simplescalarfilter.h"

namespace optimizer
{

template <typename T>
using FilterRangeBounds = std::vector<std::pair<T, T>>;
using ExtraSRRC = std::vector<std::unique_ptr<execplan::SimpleColumn>>;
using SCAndItsProjectionPosition = std::pair<execplan::SimpleColumn*, uint32_t>;
using SCsAndTheirProjectionPositions = std::vector<SCAndItsProjectionPosition>;

namespace details
{

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
  return std::any_of(csep.tableList().begin(), csep.tableList().end(),
                     [&ctx](const auto& table)
                     {
                       cal_impl_if::SchemaAndTableName schemaAndTableName = {table.schema, table.table};
                       return (!table.isColumnstore() &&
                               ctx.getGwi().tableStatistics.findStatisticsForATable(schemaAndTableName));
                     });
}

// Helper: fresh SimpleColumn from `column` prototype, with resultType mirrored.
static execplan::SimpleColumn* cloneKeyColumn(const execplan::SimpleColumn& column)
{
  auto* sc = new execplan::SimpleColumn(column);
  sc->resultType(column.resultType());
  return sc;
}

// This routine produces a new ParseTree that is AND(low <= column, column <(=) high)
// with an OR IS NULL distribution appended for the final range.
// TODO add engine-independent statistics-derived ranges
execplan::ParseTree* filtersWithNewRange(execplan::SCSEP& csep, execplan::SimpleColumn& column,
                                         std::pair<uint64_t, uint64_t>& bound, bool isLast)
{
  const execplan::CalpontSystemCatalog::ColType colType = column.resultType();

  // col >= bound.first
  auto* lowConst = optimizer::lib::makeConstUInt(bound.first);
  lowConst->resultType(colType);
  execplan::ParseTree* lowerBound =
      optimizer::lib::makeCmpFilter(cloneKeyColumn(column), ">=", lowConst);

  // col <(=) bound.second
  auto* highConst = optimizer::lib::makeConstUInt(bound.second);
  highConst->resultType(colType);
  execplan::ParseTree* upperBound =
      optimizer::lib::makeCmpFilter(cloneKeyColumn(column), isLast ? "<=" : "<", highConst);

  execplan::ParseTree* range;
  if (isLast)
  {
    // For the last range distribute OR IS NULL over both sides so NULLs are
    // retained in the final UNION unit: (lower OR col IS NULL) AND (upper OR col IS NULL).
    execplan::ParseTree* nullFilter1 = optimizer::lib::makeIsNullFilter(cloneKeyColumn(column));
    execplan::ParseTree* nullFilter2 = optimizer::lib::makeIsNullFilter(cloneKeyColumn(column));
    range = optimizer::lib::andWith(optimizer::lib::orWith(lowerBound, nullFilter1),
                                    optimizer::lib::orWith(upperBound, nullFilter2));
  }
  else
  {
    range = optimizer::lib::andWith(lowerBound, upperBound);
  }

  return optimizer::lib::andWith(csep->filters(), range);
}

// Looking for a projected column that comes first in an available index and has EI statistics
// INV nullptr signifies that no suitable column was found
std::optional<cal_impl_if::ColumnStatistics*> chooseKeyColumnAndStatistics(
    execplan::CalpontSystemCatalog::TableAliasName& targetTable, optimizer::RBOptimizerContext& ctx)
{
  cal_impl_if::SchemaAndTableName schemaAndTableName = {targetTable.schema, targetTable.table};

  auto tableColumnsStatisticsOpt = ctx.getGwi().tableStatistics.findStatisticsForATable(schemaAndTableName);
  if (!tableColumnsStatisticsOpt)
  {
    return std::nullopt;
  }

  auto tableColumnsStatistics = tableColumnsStatisticsOpt.value();

  // TODO this algo now returns the first column and stats
  // for it but it should consider all columns available
  for (auto& [columnName, columnStatistics] : *tableColumnsStatistics)
  {
    return {&columnStatistics};
  }

  return std::nullopt;
}
}  // namespace details

using namespace details;

bool parallelCESFilter(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx)
{
  // TODO filter out CSEPs with orderBy, groupBy, having || or clean up OB,GB,HAVING cloning CSEP
  // Filter out tables that were re-written.
  bool someFT = someAreForeignTables(csep);
  bool someFTSI = someForeignTablesHasStatisticsAndMbIndex(csep, ctx);
  return someFT && someFTSI;
}

uint64_t decodeU64(const std::string& bytes)
{
  uint64_t v = 0;
  const size_t n = std::min<size_t>(bytes.size(), sizeof(uint64_t));
  if (n)
    std::memcpy(&v, bytes.data(), n);
  return v;
}

// Populates range bounds based on histogram.
// INV histogram != nullptr && histogram->get_json_histogram().empty() is enforced in the caller.
template <typename T>
std::optional<details::FilterRangeBounds<T>> populateRangeBoundsFromHistogram(
    cal_impl_if::ColumnStatistics& columnStatistics, size_t maxParallelFactor)
{
  details::FilterRangeBounds<T> bounds;
  auto* histogram = columnStatistics.getHistogram();

  // Get parallel factor from context
  // TODO These calls are abstraction leak from MDB so better replace with own structs.
  size_t numberOfUnionUnits = std::min(histogram->get_json_histogram().size(), maxParallelFactor);
  size_t numberOfBucketsPerUnionUnit = histogram->get_json_histogram().size() / numberOfUnionUnits;

  // Loop over buckets to produce filter ranges
  // NB Currently Histogram_json_hb has the last bucket that has end as its start
  for (size_t i = 0; i < numberOfUnionUnits - 1; ++i)
  {
    auto bucket = histogram->get_json_histogram().begin() + i * numberOfBucketsPerUnionUnit;
    auto endBucket = histogram->get_json_histogram().begin() + (i + 1) * numberOfBucketsPerUnionUnit;
    T currentLowerBound = static_cast<T>(decodeU64(bucket->start_value));
    T currentUpperBound = static_cast<T>(decodeU64(endBucket->start_value));
    bounds.push_back({currentLowerBound, currentUpperBound});
  }

  // Final segment: from the start of the last chunk to the histogram's last end endpoint
  if (numberOfUnionUnits >= 1)
  {
    auto lastChunkIndex = (numberOfUnionUnits - 1) * numberOfBucketsPerUnionUnit;
    if (lastChunkIndex < histogram->get_json_histogram().size())
    {
      auto lastStartBucket = histogram->get_json_histogram().begin() + lastChunkIndex;
      T finalLowerBound = static_cast<T>(decodeU64(lastStartBucket->start_value));

      T finalUpperBound = std::numeric_limits<T>::max();
      if (!histogram->get_last_bucket_end_endp().empty())
      {
        finalUpperBound = static_cast<T>(decodeU64(histogram->get_last_bucket_end_endp()));
      }
      bounds.push_back({finalLowerBound, finalUpperBound});
    }
  }

  // Ensure the first bound starts from the minimal representable value to avoid dropping values
  if (!bounds.empty())
  {
    bounds.front().first = std::numeric_limits<T>::lowest();
  }

  return bounds;
}

// Populates range bounds based on min/max assuming that the column values are uniformly distributed.
// This statistics is used for PK columns in engine-independent stats in MDB.
// NB The current version supports only numeric columns up to BIGNT.
template <typename T>
std::optional<details::FilterRangeBounds<T>> populateRangeBoundsFromEquallyDistributedRange(
    cal_impl_if::ColumnStatistics& columnStatistics, size_t maxParallelFactor)
{
  // TODOThis should be protected by constexpr checks on types, mb concepts.
  T minValue = 0;
  T maxValue = 0;

  // TODO consider to move into a ColumnStatistics method.
  if constexpr (std::is_integral_v<T> && std::is_unsigned_v<T>)
  {
    minValue = columnStatistics.getUIntMinValue().value();
    maxValue = columnStatistics.getUIntMaxValue().value();
  }
  else if constexpr (std::is_integral_v<T> && std::is_signed_v<T>)
  {
    minValue = columnStatistics.getIntMinValue().value();
    maxValue = columnStatistics.getIntMaxValue().value();
  }

  if (minValue >= maxValue)
  {
    return std::nullopt;
  }

  auto distance = maxValue - minValue;
  auto step = distance / maxParallelFactor;

  details::FilterRangeBounds<T> bounds;
  for (size_t i = 0; i < maxParallelFactor; ++i)
  {
    bounds.push_back({minValue + i * step, minValue + (i + 1) * step});
  }

  if (!bounds.empty())
  {
    bounds.front().first = std::numeric_limits<T>::lowest();
    bounds.back().second = maxValue;
  }

  return bounds;
}

// Populates range bounds based on column statistics
// Returns optional with bounds if successful, nullopt otherwise
template <typename T>
std::optional<details::FilterRangeBounds<T>> populateRangeBounds(
    cal_impl_if::ColumnStatistics& columnStatistics, size_t& maxParallelFactor)
{
  // Guard: empty histogram or no min/max values
  if (columnStatistics.hasNonEmptyHistogram())
  {
    return populateRangeBoundsFromHistogram<T>(columnStatistics, maxParallelFactor);
  }

  if (columnStatistics.hasMinAndMaxRangeValues())
  {
    return populateRangeBoundsFromEquallyDistributedRange<T>(columnStatistics, maxParallelFactor);
  }

  return std::nullopt;
}

// TODO char and other numerical types support
execplan::CalpontSelectExecutionPlan::SelectList makeUnionFromTable(
    execplan::CalpontSelectExecutionPlan& csep, execplan::CalpontSystemCatalog::TableAliasName& table,
    optimizer::RBOptimizerContext& ctx)
{
  execplan::CalpontSelectExecutionPlan::SelectList unionVec;

  // SC type controls an integral type used to produce suitable filters. The continuation of this function
  // should become a template function based on SC type.
  auto columnStatisticsOpt = chooseKeyColumnAndStatistics(table, ctx);
  if (!columnStatisticsOpt)
  {
    return unionVec;
  }

  auto& columnStatistics = *columnStatisticsOpt.value();

  execplan::SimpleColumn keyColumn(columnStatistics.getColumn()); // copying because we have to modify it.

  keyColumn.tableAlias(table.alias); // assigns correct alias
  keyColumn.data("");                // force recomputation of data() - SQL representation (for foreign query)

  size_t configuredMaxParallelFactor = ctx.getCesOptimizationParallelFactor();

  // TODO char and other numerical types support
  // TODO signed numerical types support
  using SCIntegralType = uint64_t;
  auto boundsOpt = populateRangeBounds<SCIntegralType>(columnStatistics, configuredMaxParallelFactor);
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
                                            SCToPosCounterMap& sCsAndTheirPositions)
{
  auto newAliasedTable(table);
  newAliasedTable.alias = tableAlias;
  auto derivedSCEP = csep.cloneForTableWORecursiveSelectsGbObHaving(newAliasedTable, false);
  // update returned columns using SC -> position map.
  std::vector<execplan::SimpleColumn*> projectionSCs(sCsAndTheirPositions.size(), nullptr);
  for (auto [sc, colPosition] : sCsAndTheirPositions)
  {
    projectionSCs[colPosition] = sc->clone();
  }

  std::vector<boost::shared_ptr<execplan::ReturnedColumn>> derivedProjection;
  derivedProjection.reserve(projectionSCs.size());

  for (auto sc : projectionSCs)
  {
    derivedProjection.push_back(execplan::SRCP(sc));
    auto it = derivedSCEP->columnMap().find(sc->columnName());
    if (it == derivedSCEP->columnMap().end())
    {
      derivedSCEP->columnMap().insert({sc->columnName(), execplan::SRCP(sc->clone())});
    }
  }

  derivedSCEP->returnedCols(std::move(derivedProjection));

  // At this point CSEP contains all SCs from original projection, GB and OB that belongs to the target table.

  auto* derivedCSEP = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(derivedSCEP.get());
  // TODO more rigorous error handling.
  if (!derivedCSEP)
  {
    return execplan::SCSEP();
  }

  {
    derivedCSEP->tableAlias(tableAlias, true);

    auto additionalUnionVec = makeUnionFromTable(
        *derivedCSEP, const_cast<execplan::CalpontSystemCatalog::TableAliasName&>(table), ctx);

    // TODO add original alias to support multiple same name tables
    derivedSCEP->location(execplan::CalpontSelectExecutionPlan::FROM);
    derivedSCEP->subType(execplan::CalpontSelectExecutionPlan::FROM_SUBS);
    derivedSCEP->derivedTbAlias(tableAlias);

    derivedSCEP->unionVec().insert(derivedSCEP->unionVec().end(), additionalUnionVec.begin(),
                                   additionalUnionVec.end());
  }

  return derivedSCEP;
}

void updateScToUseRewrittenDerived(execplan::SimpleColumn* sc, const std::string& newTableAlias,
                                   const uint32_t colPosition, std::optional<std::string> scAlias)
{
  sc->oid(0);
  sc->schemaName("");
  // For derived tables, set tableName/tableAlias/derivedTable to the new alias
  sc->tableName(newTableAlias);
  sc->tableAlias(newTableAlias);
  sc->derivedTable(newTableAlias);
  sc->data("``.`" + newTableAlias + "`.`" + sc->columnName() + "`");

  sc->colPosition(colPosition);
  sc->isColumnStore(true);

  if (scAlias)
  {
    sc->alias(scAlias.value());
  }
}

std::pair<uint32_t, bool> findOrInsertColumnPosition(execplan::SimpleColumn* sc,
                                                     SCToPosCounterMap& SCToPosCounterMap,
                                                     const uint32_t colPosition)
{
  auto it = SCToPosCounterMap.find(sc);
  if (it == SCToPosCounterMap.end())
  {
    SCToPosCounterMap.insert({sc, colPosition});
    return {colPosition, true};
  }
  return {it->second, false};
}

// MCOL-6148 If SC has execplan::execplan::JOIN_CORRELATED set this will create an additional ghost table in
// uniqTupleKey in PP.
execplan::SimpleColumn* cloneSCForDerivedProjection(execplan::SimpleColumn* sc)
{
  auto clone = sc->clone();
  clone->joinInfo(execplan::NO_JOIN);
  return clone;
}

void tryToUpdateScToUseRewrittenDerived(
    execplan::SimpleColumn* sc, optimizer::TableAliasToNewAliasAndSCPositionsMap& tableAliasToSCPositionsMap)
{
  auto singleTable = sc->singleTable();
  if (!singleTable)
    return;

  auto tableAliasToSCPositionsIt = tableAliasToSCPositionsMap.find(*singleTable);

  if (tableAliasToSCPositionsIt != tableAliasToSCPositionsMap.end())
  {
    auto& [newTableAlias, SCToPosCounterMap, currentColPositionCursorValue] =
        tableAliasToSCPositionsIt->second;

    // Adds a new column to the map if it doesn't exist
    // TODO use unique
    auto originalSC = cloneSCForDerivedProjection(sc);
    auto [colPosition, isNewColumn] =
        findOrInsertColumnPosition(originalSC, SCToPosCounterMap, currentColPositionCursorValue);
    if (isNewColumn)
    {
      ++currentColPositionCursorValue;
    }
    updateScToUseRewrittenDerived(sc, newTableAlias, colPosition, std::nullopt);
  }
}

void updateSCsUsingIteration(optimizer::TableAliasToNewAliasAndSCPositionsMap& tableAliasToSCPositionsMap,
                             std::vector<execplan::SRCP>& rcs)
{
  for (auto& rc : rcs)
  {
    rc->setSimpleColumnListExtended();
    for (auto* sc : rc->simpleColumnListExtended())
    {
      tryToUpdateScToUseRewrittenDerived(sc, tableAliasToSCPositionsMap);
    }
  }
}

void updateSCsUsingWalkers(optimizer::TableAliasToNewAliasAndSCPositionsMap& tableAliasToSCPositionsMap,
                           execplan::ParseTree* pt)
{
  std::vector<execplan::SimpleColumn*> simpleColumns;
  pt->walk(execplan::getSimpleColsExtended, &simpleColumns);
  for (auto* sc : simpleColumns)
  {
    tryToUpdateScToUseRewrittenDerived(sc, tableAliasToSCPositionsMap);
  }
}

// This routine takes tableAliasToSCPositionsMap and extraSCs and correlate extraSCs with positions.
SCsAndTheirProjectionPositions findPositionsForExtraSCs(
    optimizer::TableAliasToNewAliasAndSCPositionsMap& tableAliasToSCPositionsMap, ExtraSRRC& extraSCs)
{
  SCsAndTheirProjectionPositions scsAndTheirProjectionPositions;
  for (auto& extraSC : extraSCs)
  {
    auto tableAliasToSCPositionsIt = tableAliasToSCPositionsMap.find(*extraSC->singleTable());
    if (tableAliasToSCPositionsIt != tableAliasToSCPositionsMap.end())
    {
      auto& [newTableAlias, SCToPosCounterMap, unused] = tableAliasToSCPositionsIt->second;
      // INV there must be a position for all SCs from extraSCs
      auto colPosition = SCToPosCounterMap.at(extraSC.get());
      scsAndTheirProjectionPositions.push_back({extraSC.get(), colPosition});
    }
  }

  return scsAndTheirProjectionPositions;
}

bool applyParallelCES(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx)
{
  auto tables = csep.tableList();

  execplan::CalpontSelectExecutionPlan::TableList newTableList;
  // TODO support CSEPs with derived tables
  execplan::CalpontSelectExecutionPlan::SelectList newDerivedTableList;
  bool ruleMustBeApplied = false;

  // Get reference to accumulated map from outer queries - used for updating correlated columns
  // that reference tables from outer query
  optimizer::TableAliasToNewAliasAndSCPositionsMap& accumulatedMap = ctx.getAccumulatedTableAliasMap();

  // Local map for THIS CSEP's tables - subquery creates its OWN derived tables
  // We insert local tables here AND into accumulatedMap
  // Column updates will modify entries in accumulatedMap (which includes local tables)
  // Then we copy back local table entries for derived table creation

  // 1st pass over tables to create derived tables placeholders to collect
  // SCs to be updated - each CSEP creates its OWN derived tables
  std::vector<execplan::CalpontSystemCatalog::TableAliasName> localTables;
  // Local map for THIS CSEP's tables only - separate from accumulated map
  optimizer::TableAliasToNewAliasAndSCPositionsMap localTableMap;

  for (auto& table : tables)
  {
    cal_impl_if::SchemaAndTableName schemaAndTableName = {table.schema, table.table};
    auto anyColumnStatistics = ctx.getGwi().findStatisticsForATable(schemaAndTableName);
    if (!table.isColumnstore() && anyColumnStatistics)
    {
      // Create NEW derived table for THIS CSEP's table
      // Each CSEP (including subqueries) creates its OWN derived tables
      std::string tableAlias = getRewrittenSubTableAlias(table, ctx);
      // Add to LOCAL map - this CSEP's own derived tables
      localTableMap.insert({table, {tableAlias, {}, 0}});
      localTables.push_back(table);
      execplan::CalpontSystemCatalog::TableAliasName tn = execplan::make_aliasview("", "", tableAlias, "");
      newTableList.push_back(tn);
      ruleMustBeApplied = true;
    }
    else
    {
      newTableList.push_back(table);
    }
  }

  // Create a WORKING map for this CSEP's column updates
  // Start with local tables, then add outer query tables (local takes priority)
  // This ensures:
  // 1. Local tables use THIS CSEP's derived tables
  // 2. Correlated columns from outer query use outer query's derived tables
  // 3. We don't modify accumulatedMap (which would affect outer query)
  optimizer::TableAliasToNewAliasAndSCPositionsMap workingMap = localTableMap;

  // Add outer query mappings for correlated columns (only if not already in local map)
  for (auto& [k, v] : accumulatedMap)
  {
    workingMap.insert({k, v});  // insert() won't overwrite existing local entries
  }

  // Use workingMap for all column updates in this CSEP
  optimizer::TableAliasToNewAliasAndSCPositionsMap& tableAliasToSCPositionsMap = workingMap;

  // 2nd pass over RCs to update RCs with derived table SCs in projection
  execplan::CalpontSelectExecutionPlan::ReturnedColumnList newReturnedColumns;
  // replace parent CSEP RCs with derived table RCs using ScheamAndTableName -> tableAlias map
  if (ruleMustBeApplied)
  {
    for (auto& rc : csep.returnedCols())
    {
      updateSCsUsingIteration(tableAliasToSCPositionsMap, csep.returnedCols());
      newReturnedColumns.push_back(rc);
    }

    // OB and GB might use SCs that are not listed in projection.
    // Collect extra SCs into a vector to add them to the new derived table.
    // The lifetime of this vector must be at least until the end of the block that creates derived tables and
    // UNION units.
    // 3d pass over GROUP BY columns
    if (!csep.groupByCols().empty())
    {
      updateSCsUsingIteration(tableAliasToSCPositionsMap, csep.groupByCols());
    }

    // 4th pass over ORDER BY columns
    if (!csep.orderByCols().empty())
    {
      updateSCsUsingIteration(tableAliasToSCPositionsMap, csep.orderByCols());
    }

    // 5th pass over filters to use derived table SCs in filters
    auto filters = csep.filters();
    if (filters)
    {
      updateSCsUsingWalkers(tableAliasToSCPositionsMap, filters);
    }

    // 6th pass over filters to use derived table SCs in filters
    auto having = csep.having();
    if (having)
    {
      updateSCsUsingWalkers(tableAliasToSCPositionsMap, having);
    }

    // 6.5 pass: update correlated columns inside EXISTS subqueries
    // Walk filter/having trees, find ExistsFilter nodes and update correlated outer SCs within sub-CSEPs
    auto updateExistsCorrelated = [&tableAliasToSCPositionsMap](const execplan::ParseTree* root)
    {
      if (!root)
        return;
      // Walker to process ExistsFilter, SimpleScalarFilter, and SelectFilter nodes
      auto walker = [](const execplan::ParseTree* n, void* obj)
      {
        auto* ef = dynamic_cast<execplan::ExistsFilter*>(n->data());
        auto* ssf = dynamic_cast<execplan::SimpleScalarFilter*>(n->data());
        auto* sf = dynamic_cast<execplan::SelectFilter*>(n->data());
        if (!ef && !ssf && !sf)
          return;

        auto* mapPtr = static_cast<optimizer::TableAliasToNewAliasAndSCPositionsMap*>(obj);
        auto& map = *mapPtr;

        // Get the subquery
        auto sub = ef ? ef->sub() : (ssf ? ssf->sub() : sf->sub());

        // Build a set of subquery's own table aliases to distinguish outer query columns
        std::set<std::string> subqueryTableAliases;
        if (sub)
        {
          for (auto& t : sub->tableList())
          {
            subqueryTableAliases.insert(t.alias);
          }
        }

        // Helper to check if a column belongs to outer query (not subquery's own tables)
        auto isOuterQueryColumn = [&subqueryTableAliases](execplan::SimpleColumn* sc) -> bool
        {
          const std::string& alias = sc->tableAlias();
          // Skip already rewritten columns
          if (alias.find("$added_sub_") != std::string::npos)
            return false;
          // Column is from outer query if it's NOT in subquery's table list
          return subqueryTableAliases.count(alias) == 0;
        };

        // For ExistsFilter (used for NOT IN), correlated outer query columns are in sub->filters()
        // We need to rewrite ONLY outer query columns, not subquery's own columns
        if (ef && sub)
        {
          if (auto subFilters = sub->filters())
          {
            std::vector<execplan::SimpleColumn*> subSCs;
            subFilters->walk(execplan::getSimpleColsExtended, &subSCs);
            for (auto* sc : subSCs)
            {
              if (sc && isOuterQueryColumn(sc))
              {
                tryToUpdateScToUseRewrittenDerived(sc, map);
              }
            }
          }
        }

        // For SelectFilter, update the outer query columns being compared
        if (sf)
        {
          for (const auto& col : sf->cols())
          {
            col->setSimpleColumnListExtended();
            for (auto* sc : col->simpleColumnListExtended())
            {
              if (sc)
              {
                tryToUpdateScToUseRewrittenDerived(sc, map);
              }
            }
          }
        }
      };
      root->walk(walker, &tableAliasToSCPositionsMap);
    };

    if (filters)
      updateExistsCorrelated(filters);
    if (having)
      updateExistsCorrelated(having);

    // 7th pass over LOCAL tables to create derived CSEP with the collected SCs
    // Create derived tables only for LOCAL tables (this CSEP's own tables)
    for (auto& table : localTables)
    {
      // Get the mapping from accumulatedMap (where column updates happened)
      auto tableIt = tableAliasToSCPositionsMap.find(table);
      if (tableIt != tableAliasToSCPositionsMap.end())
      {
        auto& [newTableAlias, SCToPosCounterMap, unused] = tableIt->second;
        auto derivedSCEP = createDerivedTableFromTable(csep, table, newTableAlias, ctx, SCToPosCounterMap);
        newDerivedTableList.push_back(std::move(derivedSCEP));
      }
    }

    csep.derivedTableList(newDerivedTableList);
    // Replace table list with new table list populated with union units
    csep.tableList(newTableList);
    csep.returnedCols(newReturnedColumns);

    // Update accumulatedMap with this CSEP's local tables for subqueries to use
    // This must happen AFTER column updates so subqueries see the correct mappings
    for (auto& table : localTables)
    {
      auto tableIt = workingMap.find(table);
      if (tableIt != workingMap.end())
      {
        accumulatedMap.insert_or_assign(table, tableIt->second);
      }
    }
  }
  return ruleMustBeApplied;
}

}  // namespace optimizer

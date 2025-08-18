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
#include <vector>

#include "rulebased_optimizer.h"

#include "constantcolumn.h"
#include "execplan/calpontselectexecutionplan.h"
#include "execplan/simplecolumn.h"
#include "logicoperator.h"
#include "operator.h"
#include "predicateoperator.h"
#include "rbo_apply_parallel_ces.h"
#include "returnedcolumn.h"
#include "simplefilter.h"
#include "existsfilter.h"

namespace optimizer
{

template <typename T>
using FilterRangeBounds = std::vector<std::pair<T, T>>;
using ExtraSRRC = std::vector<std::unique_ptr<execplan::SimpleColumn>>;
using SCAndItsProjectionPosition = std::pair<execplan::SimpleColumn*, uint32_t>;
using SCsAndTheirProjectionPositions = std::vector<SCAndItsProjectionPosition>;

static const std::string RewrittenSubTableAliasPrefix = "$added_sub_";

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
  return std::any_of(
      csep.tableList().begin(), csep.tableList().end(),
      [&ctx](const auto& table)
      {
        cal_impl_if::SchemaAndTableName schemaAndTableName = {table.schema, table.table};
        return (!table.isColumnstore() &&
                ctx.getGwi().tableStatisticsMap.find(schemaAndTableName) != ctx.getGwi().tableStatisticsMap.end());
      });
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
  // Use exclusive upper bound for intermediate bounds; inclusive for the final bound
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

  // For the last range, add OR IS NULL to handle NULL values
  if (isLast)
  {
    // Create IS NULL filter: column IS NULL
    auto* nullCheckColumn1 = new execplan::SimpleColumn(column);
    nullCheckColumn1->resultType(column.resultType());
    auto* nullCheckColumn2 = new execplan::SimpleColumn(column);
    nullCheckColumn2->resultType(column.resultType());

    auto* nullFilter1 = new execplan::SimpleFilter();
    execplan::SOP isNullOp1 = boost::make_shared<execplan::Operator>(execplan::PredicateOperator("isnull"));
    isNullOp1->setOpType(nullCheckColumn1->resultType(), nullCheckColumn1->resultType());
    isNullOp1->resultType(isNullOp1->operationType());
    nullFilter1->op(isNullOp1);
    nullFilter1->lhs(nullCheckColumn1);
    auto* nullConstant1 = new execplan::ConstantColumnNull();
    nullConstant1->resultType(nullCheckColumn1->resultType());
    nullFilter1->rhs(nullConstant1);

    auto* nullFilter2 = new execplan::SimpleFilter();
    execplan::SOP isNullOp2 = boost::make_shared<execplan::Operator>(execplan::PredicateOperator("isnull"));
    isNullOp2->setOpType(nullCheckColumn2->resultType(), nullCheckColumn2->resultType());
    isNullOp2->resultType(isNullOp2->operationType());
    nullFilter2->op(isNullOp2);
    nullFilter2->lhs(nullCheckColumn2);
    auto* nullConstant2 = new execplan::ConstantColumnNull();
    nullConstant2->resultType(nullCheckColumn2->resultType());
    nullFilter2->rhs(nullConstant2);

    // Transform (A AND B) OR C to (A OR C) AND (B OR C)
    // Left side of original AND: sfl (col >= X)
    execplan::ParseTree* leftOrNull = new execplan::ParseTree(new execplan::LogicOperator("or"));
    leftOrNull->left(new execplan::ParseTree(sfl));
    leftOrNull->right(new execplan::ParseTree(nullFilter1));

    // Right side of original AND: sfr (col <= Y)
    execplan::ParseTree* rightOrNull = new execplan::ParseTree(new execplan::LogicOperator("or"));
    rightOrNull->left(new execplan::ParseTree(sfr));
    rightOrNull->right(new execplan::ParseTree(nullFilter2));

    // Final: (A OR C) AND (B OR C)
    execplan::ParseTree* finalAnd = new execplan::ParseTree(new execplan::LogicOperator("and"));
    finalAnd->left(leftOrNull);
    finalAnd->right(rightOrNull);
    ptp = finalAnd;
  }

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

      auto columnStatistics = ctx.getGwi().findStatisticsForATable(schemaAndTableName);
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

  auto tableColumnsStatisticsIt = ctx.getGwi().tableStatisticsMap.find(schemaAndTableName);
  if (tableColumnsStatisticsIt == ctx.getGwi().tableStatisticsMap.end() ||
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
}  // namespace details

using namespace details;

bool parallelCESFilter(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx)
{
  // TODO filter out CSEPs with orderBy, groupBy, having || or clean up OB,GB,HAVING cloning CSEP
  // Filter out tables that were re-written.
  return someAreForeignTables(csep) && someForeignTablesHasStatisticsAndMbIndex(csep, ctx);
}

// Populates range bounds based on column statistics
// Returns optional with bounds if successful, nullopt otherwise
template <typename T>
std::optional<details::FilterRangeBounds<T>> populateRangeBounds(Histogram_json_hb* columnStatistics,
                                                                 optimizer::RBOptimizerContext& ctx)
{
  details::FilterRangeBounds<T> bounds;

  // Guard: empty histogram
  if (!columnStatistics || columnStatistics->get_json_histogram().empty())
    return std::nullopt;

  auto decodeU64 = [](const std::string& bytes) -> uint64_t
  {
    uint64_t v = 0;
    const size_t n = std::min<size_t>(bytes.size(), sizeof(uint64_t));
    if (n)
      std::memcpy(&v, bytes.data(), n);
    return v;
  };

  // Get parallel factor from context
  size_t maxParallelFactor = ctx.getCesOptimizationParallelFactor();
  std::cout << "populateRangeBounds() columnStatistics->buckets.size() "
            << columnStatistics->get_json_histogram().size() << std::endl;
  std::cout << "Session ces_optimization_parallel_factor: " << maxParallelFactor << std::endl;
  size_t numberOfUnionUnits = std::min(columnStatistics->get_json_histogram().size(), maxParallelFactor);
  size_t numberOfBucketsPerUnionUnit = columnStatistics->get_json_histogram().size() / numberOfUnionUnits;

  std::cout << "Number of union units: " << numberOfUnionUnits << std::endl;
  std::cout << "Number of buckets per union unit: " << numberOfBucketsPerUnionUnit << std::endl;

  // Loop over buckets to produce filter ranges
  // NB Currently Histogram_json_hb has the last bucket that has end as its start
  for (size_t i = 0; i < numberOfUnionUnits - 1; ++i)
  {
    auto bucket = columnStatistics->get_json_histogram().begin() + i * numberOfBucketsPerUnionUnit;
    auto endBucket = columnStatistics->get_json_histogram().begin() + (i + 1) * numberOfBucketsPerUnionUnit;
    T currentLowerBound = static_cast<T>(decodeU64(bucket->start_value));
    T currentUpperBound = static_cast<T>(decodeU64(endBucket->start_value));
    bounds.push_back({currentLowerBound, currentUpperBound});
  }

  // Final segment: from the start of the last chunk to the histogram's last end endpoint
  if (numberOfUnionUnits >= 1)
  {
    auto lastChunkIndex = (numberOfUnionUnits - 1) * numberOfBucketsPerUnionUnit;
    if (lastChunkIndex < columnStatistics->get_json_histogram().size())
    {
      auto lastStartBucket = columnStatistics->get_json_histogram().begin() + lastChunkIndex;
      T finalLowerBound = static_cast<T>(decodeU64(lastStartBucket->start_value));

      T finalUpperBound = std::numeric_limits<T>::max();
      if (!columnStatistics->get_last_bucket_end_endp().empty())
      {
        finalUpperBound = static_cast<T>(decodeU64(columnStatistics->get_last_bucket_end_endp()));
      }
      bounds.push_back({finalLowerBound, finalUpperBound});
    }
  }

  // Ensure the first bound starts from the minimal representable value to avoid dropping values
  if (!bounds.empty())
  {
    T originalFirstLower = bounds.front().first;
    bounds.front().first = std::numeric_limits<T>::lowest();
    std::cout << "Adjusted first bound lower from " << originalFirstLower << " to " << bounds.front().first
              << std::endl;
  }

  for (auto& bucket : columnStatistics->get_json_histogram())
  {
    T currentLowerBound = static_cast<T>(decodeU64(bucket.start_value));
    std::cout << "Bucket: " << currentLowerBound << std::endl;
  }
  // Note: last bound now uses histogram's last end endpoint to cover the tail.

  for (auto& bound : bounds)
  {
    std::cout << "Bound: " << bound.first << " " << bound.second << std::endl;
  }

  return bounds;
}

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
  auto boundsOpt = populateRangeBounds<uint64_t>(columnStatistics, ctx);
  if (!boundsOpt.has_value())
  {
    return unionVec;
  }

  auto& bounds = boundsOpt.value();
  std::cout << "Bounds generated: " << bounds.size() << std::endl;

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
  std::cout << "Union units created: " << unionVec.size() << std::endl;

  return unionVec;
}

execplan::SCSEP createDerivedTableFromTable(execplan::CalpontSelectExecutionPlan& csep,
                                            const execplan::CalpontSystemCatalog::TableAliasName& table,
                                            const std::string& tableAlias, optimizer::RBOptimizerContext& ctx,
                                            SCToPosCounterMap& sCsAndTheirPositions)
{
  auto derivedSCEP = csep.cloneForTableWORecursiveSelectsGbObHaving(table, false);
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
  sc->schemaName("");
  // For derived tables, leave tableName empty; use tableAlias/derivedTable to reference it
  sc->tableName("");
  sc->tableAlias(newTableAlias);
  sc->derivedTable(newTableAlias);

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
  auto tableAliasToSCPositionsIt = tableAliasToSCPositionsMap.find(*sc->singleTable());

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

void extractExtraSCsFromGBOrOB(
    optimizer::TableAliasToNewAliasAndSCPositionsMap& tableAliasToSCPositionsMap,
    const execplan::CalpontSelectExecutionPlan::GroupByColumnList& groupByOrOrderByCols)
{
  for (auto& rc : groupByOrOrderByCols)
  {
    rc->setSimpleColumnList();
    for (auto* sc : rc->simpleColumnList())
    {
      tryToUpdateScToUseRewrittenDerived(sc, tableAliasToSCPositionsMap);
    }
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
  optimizer::TableAliasToNewAliasAndSCPositionsMap tableAliasToSCPositionsMap;

  // 1st pass over tables to create derived tables placeholders to collect
  // SCs to be updated
  for (auto& table : tables)
  {
    cal_impl_if::SchemaAndTableName schemaAndTableName = {table.schema, table.table};
    auto anyColumnStatistics = ctx.getGwi().findStatisticsForATable(schemaAndTableName);
    if (!table.isColumnstore() && anyColumnStatistics)
    {
      std::string tableAlias = optimizer::RewrittenSubTableAliasPrefix + table.schema + "_" + table.table +
                               "_" + std::to_string(ctx.getUniqueId());
      tableAliasToSCPositionsMap.insert({table, {tableAlias, {}, 0}});
      execplan::CalpontSystemCatalog::TableAliasName tn = execplan::make_aliasview("", "", tableAlias, "");
      newTableList.push_back(tn);
      ruleMustBeApplied = true;
    }
    else
    {
      newTableList.push_back(table);
    }
  }

  // 2nd pass over RCs to update RCs with derived table SCs in projection
  execplan::CalpontSelectExecutionPlan::ReturnedColumnList newReturnedColumns;
  // replace parent CSEP RCs with derived table RCs using ScheamAndTableName -> tableAlias map
  if (ruleMustBeApplied)
  {
    for (auto& rc : csep.returnedCols())
    {
      rc->setSimpleColumnList();
      for (auto* sc : rc->simpleColumnList())
      {
        tryToUpdateScToUseRewrittenDerived(sc, tableAliasToSCPositionsMap);
      }
      newReturnedColumns.push_back(rc);
    }

    // OB and GB might use SCs that are not listed in projection.
    // Collect extra SCs into a vector to add them to the new derived table.
    // The lifetime of this vector must be at least until the end of the block that creates derived tables and
    // UNION units.
    // 3d pass over GROUP BY columns
    if (!csep.groupByCols().empty())
    {
      extractExtraSCsFromGBOrOB(tableAliasToSCPositionsMap, csep.groupByCols());
    }

    // 4th pass over ORDER BY columns
    if (!csep.orderByCols().empty())
    {
      extractExtraSCsFromGBOrOB(tableAliasToSCPositionsMap, csep.orderByCols());
    }

    // 5th pass over filters to use derived table SCs in filters
    auto filters = csep.filters();
    if (filters)
    {
      std::vector<execplan::SimpleColumn*> simpleColumns;
      filters->walk(execplan::getSimpleCols, &simpleColumns);
      for (auto* sc : simpleColumns)
      {
        tryToUpdateScToUseRewrittenDerived(sc, tableAliasToSCPositionsMap);
      }
    }

    // 6th pass over filters to use derived table SCs in filters
    auto having = csep.having();
    if (having)
    {
      std::vector<execplan::SimpleColumn*> simpleColumns;
      having->walk(execplan::getSimpleCols, &simpleColumns);
      for (auto* sc : simpleColumns)
      {
        tryToUpdateScToUseRewrittenDerived(sc, tableAliasToSCPositionsMap);
      }
    }

    // 6.5 pass: update correlated columns inside EXISTS subqueries
    // Walk filter/having trees, find ExistsFilter nodes and update correlated outer SCs within sub-CSEPs
    auto updateExistsCorrelated = [&tableAliasToSCPositionsMap](const execplan::ParseTree* root)
    {
      if (!root)
        return;
      // Walker to process ExistsFilter nodes
      auto walker = [](const execplan::ParseTree* n, void* obj)
      {
        auto* ef = dynamic_cast<execplan::ExistsFilter*>(n->data());
        if (!ef)
          return;
        auto* mapPtr = static_cast<optimizer::TableAliasToNewAliasAndSCPositionsMap*>(obj);
        auto& map = *mapPtr;
        auto sub = ef->sub();
        if (sub)
        {
          if (auto subFilters = sub->filters())
          {
            std::vector<execplan::SimpleColumn*> subSCs;
            subFilters->walk(execplan::getSimpleCols, &subSCs);
            for (auto* sc : subSCs)
            {
              if (sc)
                tryToUpdateScToUseRewrittenDerived(sc, map);
            }
          }
          if (auto subHaving = sub->having())
          {
            std::vector<execplan::SimpleColumn*> subSCs;
            subHaving->walk(execplan::getSimpleCols, &subSCs);
            for (auto* sc : subSCs)
            {
              if (sc)
                tryToUpdateScToUseRewrittenDerived(sc, map);
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

    // 7th pass over tables to create derived CSEP with the collected SCs
    for (auto& table : tables)
    {
      cal_impl_if::SchemaAndTableName schemaAndTableName = {table.schema, table.table};
      if (!table.isColumnstore())
      {
        auto produceDerivedTableIt = tableAliasToSCPositionsMap.find(table);
        if (produceDerivedTableIt != tableAliasToSCPositionsMap.end())
        {
          auto& [newTableAlias, SCToPosCounterMap, unused] = produceDerivedTableIt->second;
          auto derivedSCEP = createDerivedTableFromTable(csep, table, newTableAlias, ctx, SCToPosCounterMap);
          newDerivedTableList.push_back(std::move(derivedSCEP));
        }
      }
      else
      {
        newTableList.push_back(table);
      }
    }

    csep.derivedTableList(newDerivedTableList);
    // Replace table list with new table list populated with union units
    csep.tableList(newTableList);
    csep.returnedCols(newReturnedColumns);
  }
  return ruleMustBeApplied;
}

}  // namespace optimizer

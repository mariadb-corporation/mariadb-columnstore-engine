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

#include "rulebased_optimizer.h"

#include "constantcolumn.h"
#include "execplan/calpontselectexecutionplan.h"
#include "execplan/simplecolumn.h"
#include "existsfilter.h"
#include "logicoperator.h"
#include "operator.h"
#include "predicateoperator.h"
#include "simplefilter.h"

namespace optimizer
{

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

bool matchParallelCES(execplan::CalpontSelectExecutionPlan& csep)
{
  auto tables = csep.tableList();
  // This is leaf and there are no other tables at this level in neither UNION, nor derived table.
  // WIP filter out CSEPs with orderBy, groupBy, having
  // Filter out tables that were re-written.
  return tables.size() == 1 && !tables[0].isColumnstore() && !tableIsInUnion(tables[0], csep);
}

// This routine produces a new ParseTree that is AND(lowerBand <= column, column <= upperBand)
// TODO add engine-independent statistics-derived ranges
execplan::ParseTree* filtersWithNewRangeAddedIfNeeded(execplan::SCSEP& csep,
                                                      std::pair<uint64_t, uint64_t>& bound)
{
  // INV this is SimpleColumn we supply as an argument
  // TODO find the suitable column using EI statistics.
  auto* column = dynamic_cast<execplan::SimpleColumn*>(csep->returnedCols().front().get());
  assert(column);

  auto tableKeyColumnLeftOp = new execplan::SimpleColumn(*column);
  tableKeyColumnLeftOp->resultType(column->resultType());

  // TODO Nobody owns this allocation and cleanup only depends on delete in ParseTree nodes' dtors.
  auto* filterColLeftOp = new execplan::ConstantColumnUInt(bound.second, 0, 0);
  // set TZ
  // There is a question with ownership of the const column
  // WIP here we lost upper bound value if predicate is not changed to weak lt
  execplan::SOP ltOp = boost::make_shared<execplan::Operator>(execplan::PredicateOperator("<"));
  ltOp->setOpType(filterColLeftOp->resultType(), tableKeyColumnLeftOp->resultType());
  ltOp->resultType(ltOp->operationType());

  auto* sfr = new execplan::SimpleFilter(ltOp, tableKeyColumnLeftOp, filterColLeftOp);
  // auto tableKeyColumn = derivedSCEP->returnedCols().front();
  auto tableKeyColumnRightOp = new execplan::SimpleColumn(*column);
  tableKeyColumnRightOp->resultType(column->resultType());
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
execplan::SimpleColumn* findSuitableKeyColumn(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx)
{
  for (auto& rc : csep.returnedCols())
  {
    auto* simpleColumn = dynamic_cast<execplan::SimpleColumn*>(rc.get());
    if (simpleColumn)
    {
      std::cout << "Found simple column " << simpleColumn->columnName() << std::endl;
      cal_impl_if::SchemaAndTableName schemaAndTableNam = {simpleColumn->tableName(), simpleColumn->columnName()};
      auto columnStatistics = ctx.gwi.findStatisticsForATable(schemaAndTableNam);

      return simpleColumn;
    }
  }

  return nullptr;
}

execplan::CalpontSelectExecutionPlan::SelectList makeUnionFromTable(
    execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx)
{
  execplan::CalpontSelectExecutionPlan::SelectList unionVec;

  // SC type controls an integral type used to produce suitable filters. The continuation of this function
  // should become a template function based on SC type.
  execplan::SimpleColumn* keyColumn = findSuitableKeyColumn(csep, ctx);
  if (!keyColumn)
  {
    return unionVec;
  }

  std::cout << "looking for " << keyColumn->columnName() << " in ctx.gwi.tableStatisticsMap "
            << " with size " << ctx.gwi.tableStatisticsMap.size() << std::endl;
  for (auto& [k, v] : ctx.gwi.tableStatisticsMap)
  {
    std::cout << "SchemaAndTableName " << k.schema << "." << k.table << " column map size " << v.size() << std::endl;
  }

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
  std::cout << "Histogram_json_hb histogram size " << columnStatistics.get_json_histogram().size() << std::endl;

  // TODO configurable parallel factor
  // NB now histogram size is the way to control parallel factor with 16 being the maximum
  size_t numberOfUnionUnits = std::min(columnStatistics.get_json_histogram().size(), MaxParallelFactor);
  size_t numberOfBucketsPerUnionUnit = columnStatistics.get_json_histogram().size() / numberOfUnionUnits;

  // TODO char and other numerical types support
  std::vector<std::pair<uint64_t, uint64_t>> bounds;

  // TODO need to process tail if number of buckets is not divisible by number of union units
  // TODO non-overlapping buckets if it is a problem at all
  for (size_t i = 0; i < numberOfUnionUnits - 1; ++i)
  {
    auto bucket = columnStatistics.get_json_histogram().begin() + i * numberOfBucketsPerUnionUnit;
    auto endBucket = columnStatistics.get_json_histogram().begin() + (i + 1) * numberOfBucketsPerUnionUnit;
    uint64_t currentLowerBound = *(uint32_t*)bucket->start_value.data();
    uint64_t currentUpperBound = *(uint32_t*)endBucket->start_value.data();

    std::cout << "currentLowerBound " << currentLowerBound << " currentUpperBound " << currentUpperBound
              << std::endl;
    bounds.push_back(std::make_pair(currentLowerBound, currentUpperBound));
  }

  // Add last range
  auto lastBucket = columnStatistics.get_json_histogram().begin() + (numberOfUnionUnits - 1) * numberOfBucketsPerUnionUnit;
  uint64_t currentLowerBound = *(uint32_t*)lastBucket->start_value.data();
  uint64_t currentUpperBound = *(uint32_t*)columnStatistics.get_last_bucket_end_endp().data();

  std::cout << "last currentLowerBound " << currentLowerBound << " last currentUpperBound " << currentUpperBound
            << std::endl;
  bounds.push_back(std::make_pair(currentLowerBound, currentUpperBound));

  for (auto& bound : bounds)
  {
    auto clonedCSEP = csep.cloneWORecursiveSelects();
    // Add BETWEEN based on key column range
    clonedCSEP->filters(filtersWithNewRangeAddedIfNeeded(clonedCSEP, bound));
    unionVec.push_back(clonedCSEP);
  }

  return unionVec;
}
void applyParallelCES(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& ctx)
{
  auto tables = csep.tableList();
  execplan::CalpontSelectExecutionPlan::TableList newTableList;
  execplan::CalpontSelectExecutionPlan::SelectList newDerivedTableList;
  execplan::CalpontSelectExecutionPlan::ReturnedColumnList newReturnedColumns;

  // ATM Must be only 1 table
  for (auto& table : tables)
  {
    if (!table.isColumnstore())
    {
      auto derivedSCEP = csep.cloneWORecursiveSelects();
      // need to add a level here
      std::string tableAlias = RewrittenSubTableAliasPrefix + table.schema + "_" + table.table + "_" +
                               std::to_string(ctx.uniqueId);

      derivedSCEP->location(execplan::CalpontSelectExecutionPlan::FROM);
      derivedSCEP->subType(execplan::CalpontSelectExecutionPlan::FROM_SUBS);
      derivedSCEP->derivedTbAlias(tableAlias);

      // Create a copy of the current leaf CSEP with additional filters to partition the key column
      auto additionalUnionVec = makeUnionFromTable(csep, ctx);
      derivedSCEP->unionVec().insert(derivedSCEP->unionVec().end(), additionalUnionVec.begin(),
                                     additionalUnionVec.end());

      size_t colPosition = 0;
      // change parent to derived table columns
      for (auto& rc : csep.returnedCols())
      {
        auto rcCloned = boost::make_shared<execplan::SimpleColumn>(*rc);
        // TODO timezone and result type are not copied
        // TODO add specific ctor for this functionality
        rcCloned->tableName("");
        rcCloned->schemaName("");
        rcCloned->tableAlias(tableAlias);
        rcCloned->colPosition(colPosition++);
        rcCloned->resultType(rc->resultType());

        newReturnedColumns.push_back(rcCloned);
      }

      newDerivedTableList.push_back(derivedSCEP);
      execplan::CalpontSystemCatalog::TableAliasName tn = execplan::make_aliasview("", "", tableAlias, "");
      newTableList.push_back(tn);
      // Remove the filters as they were pushed down to union units
      // This is inappropriate for EXISTS filter and join conditions
      derivedSCEP->filters(nullptr);
    }
  }
  // Remove the filters as they were pushed down to union units
  // This is inappropriate for EXISTS filter and join conditions
  // csep.filters(nullptr);
  // There must be no derived at this point.
  csep.derivedTableList(newDerivedTableList);
  // Replace table list with new table list populated with union units
  csep.tableList(newTableList);
  csep.returnedCols(newReturnedColumns);
}

execplan::CalpontSelectExecutionPlan::SelectList makeUnionFromTable_exists(
    const size_t numberOfLegs, execplan::CalpontSelectExecutionPlan& csep)
{
  execplan::CalpontSelectExecutionPlan::SelectList unionVec;
  unionVec.reserve(numberOfLegs);
  std::vector<std::pair<uint64_t, uint64_t>> bounds(
      {{0, 3000961}, {3000961, std::numeric_limits<uint64_t>::max()}});
  for (auto bound : bounds)
  {
    auto clonedCSEP = csep.cloneWORecursiveSelects();
    clonedCSEP->filters(nullptr);
    // Add BETWEEN based on key column range
    clonedCSEP->filters(filtersWithNewRangeAddedIfNeeded(clonedCSEP, bound));
    unionVec.push_back(clonedCSEP);
  }

  return unionVec;
}

// TODO: remove applyParallelCES_exists
void applyParallelCES_exists(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& ctx)
{
  auto tables = csep.tableList();
  execplan::CalpontSelectExecutionPlan::TableList newTableList;
  execplan::CalpontSelectExecutionPlan::SelectList newDerivedTableList;
  execplan::CalpontSelectExecutionPlan::ReturnedColumnList newReturnedColumns;

  // ATM Must be only 1 table
  for (auto& table : tables)
  {
    if (!table.isColumnstore())
    {
      auto derivedSCEP = csep.cloneWORecursiveSelects();
      // need to add a level here
      std::string tableAlias = RewrittenSubTableAliasPrefix + table.schema + "_" + table.table + "_" +
                               std::to_string(ctx.uniqueId);

      derivedSCEP->location(execplan::CalpontSelectExecutionPlan::FROM);
      derivedSCEP->subType(execplan::CalpontSelectExecutionPlan::FROM_SUBS);
      derivedSCEP->derivedTbAlias(tableAlias);

      // TODO: hardcoded for now
      size_t parallelFactor = 2;
      // Create a copy of the current leaf CSEP with additional filters to partition the key column
      auto additionalUnionVec = makeUnionFromTable_exists(parallelFactor, csep);
      derivedSCEP->unionVec().insert(derivedSCEP->unionVec().end(), additionalUnionVec.begin(),
                                     additionalUnionVec.end());

      size_t colPosition = 0;
      // change parent to derived table columns
      for (auto& rc : csep.returnedCols())
      {
        auto rcCloned = boost::make_shared<execplan::SimpleColumn>(*rc);
        // TODO timezone and result type are not copied
        // TODO add specific ctor for this functionality
        rcCloned->tableName("");
        rcCloned->schemaName("");
        rcCloned->tableAlias(tableAlias);
        rcCloned->colPosition(colPosition++);
        rcCloned->resultType(rc->resultType());

        newReturnedColumns.push_back(rcCloned);
      }

      newDerivedTableList.push_back(derivedSCEP);
      execplan::CalpontSystemCatalog::TableAliasName tn = execplan::make_aliasview("", "", tableAlias, "");
      newTableList.push_back(tn);
      // Remove the filters as they were pushed down to union units
      // This is inappropriate for EXISTS filter and join conditions
      // TODO if needed
      derivedSCEP->filters(nullptr);
    }
  }
  // Remove the filters as they were pushed down to union units
  // This is inappropriate for EXISTS filter and join conditions
  // csep.filters(nullptr);
  // There must be no derived at this point.
  csep.derivedTableList(newDerivedTableList);
  // Replace table list with new table list populated with union units
  csep.tableList(newTableList);
  csep.returnedCols(newReturnedColumns);
}

}  // namespace optimizer

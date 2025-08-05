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

namespace optimizer
{

template <typename T>
using FilterRangeBounds = std::vector<std::pair<T, T>>;

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
  auto tables = csep.tableList();
  // This is leaf and there are no other tables at this level in neither UNION, nor derived table.
  // TODO filter out CSEPs with orderBy, groupBy, having
  // Filter out tables that were re-written.
  // return tables.size() == 1 && !tables[0].isColumnstore() && !tableIsInUnion(tables[0], csep);
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
  std::cout << "populateRangeBounds() columnStatistics->buckets.size() " << columnStatistics->get_json_histogram().size()
            << std::endl;
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
  for (auto& bucket : columnStatistics->get_json_histogram())
  {
    T currentLowerBound = *(uint32_t*)bucket.start_value.data();
    std::cout << "Bucket: " << currentLowerBound << std::endl;
  }
  // auto penultimateBucket = columnStatistics.get_json_histogram().begin() + numberOfUnionUnits *
  // numberOfBucketsPerUnionUnit; T currentLowerBound = *(uint32_t*)penultimateBucket->start_value.data(); T
  // currentUpperBound = *(uint32_t*)columnStatistics.get_last_bucket_end_endp().data();
  // bounds.push_back({currentLowerBound, currentUpperBound});


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
      auto clonedCSEP = csep.cloneForTableWORecursiveSelects(table);
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
    auto clonedCSEP = csep.cloneForTableWORecursiveSelects(table);
    auto filter = filtersWithNewRange(clonedCSEP, keyColumn, bounds.back(), true);
    clonedCSEP->columnMap().insert({keyColumn.columnName(), execplan::SRCP(keyColumn.clone())});
    clonedCSEP->filters(filter);
    unionVec.push_back(clonedCSEP);
  }
  
  return unionVec;
}
void applyParallelCES(execplan::CalpontSelectExecutionPlan& csep, RBOptimizerContext& ctx)
{
  auto tables = csep.tableList();
  execplan::CalpontSelectExecutionPlan::TableList newTableList;
  // TODO support CSEPs with derived tables
  execplan::CalpontSelectExecutionPlan::SelectList newDerivedTableList;
  TableAliasMap tableAliasMap;

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
      // Don't copy filters for this
      auto derivedSCEP = csep.cloneForTableWORecursiveSelects(table);
      // Remove the filters as they were pushed down to union units
      // This is inappropriate for EXISTS filter and join conditions
      // WIP replace with filters applied to filters, so that only relevant filters are left
      // WIP Ugly hack to avoid leaks
      auto unusedFilters = derivedSCEP->filters();
      delete unusedFilters;
      derivedSCEP->filters(nullptr);
      auto* derivedCSEP = dynamic_cast<execplan::CalpontSelectExecutionPlan*>(derivedSCEP.get());
      if (!derivedCSEP)
      {
        continue;
      }
      auto additionalUnionVec = makeUnionFromTable(*derivedCSEP, table, ctx);

      // need to add a level here
      std::string tableAlias = RewrittenSubTableAliasPrefix + table.schema + "_" + table.table + "_" +
                               std::to_string(ctx.uniqueId);
      // TODO add original alias to support multiple same name tables
      tableAliasMap.insert({table, {tableAlias, 0}});
      derivedSCEP->location(execplan::CalpontSelectExecutionPlan::FROM);
      derivedSCEP->subType(execplan::CalpontSelectExecutionPlan::FROM_SUBS);
      derivedSCEP->derivedTbAlias(tableAlias);

      derivedSCEP->unionVec().insert(derivedSCEP->unionVec().end(), additionalUnionVec.begin(),
                                     additionalUnionVec.end());

      newDerivedTableList.push_back(derivedSCEP);
      execplan::CalpontSystemCatalog::TableAliasName tn = execplan::make_aliasview("", "", tableAlias, "");
      newTableList.push_back(tn);
    }
    else
    {
      newTableList.push_back(table);
    }
  }

  execplan::CalpontSelectExecutionPlan::ReturnedColumnList newReturnedColumns;
  // replace parent CSEP RCs with derived table RCs using ScheamAndTableName -> tableAlias map
  if (!newDerivedTableList.empty())
  {
    std::cout << "Iterating over RCs" << std::endl;
    for (auto& rc : csep.returnedCols())
    {
      auto sameTableAliasOpt = rc->singleTable();
      // Same table so RC was pushed into UNION units and can be replaced with new derived table SC
      if (sameTableAliasOpt)
      {
        std::cout << "RC table schema " << sameTableAliasOpt->schema << " table " << sameTableAliasOpt->table
                  << " alias " << sameTableAliasOpt->alias << std::endl;
        auto tableAliasIt = tableAliasMap.find(*sameTableAliasOpt);
        if (tableAliasIt != tableAliasMap.end())
        {
          std::cout << "Replacing RC with new SC" << std::endl;
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
            sc->tableName("");
            sc->schemaName("");
            sc->tableAlias(newTableAlias);
            sc->colPosition(colPosition++);
          }
          // do nothing with this SC
        }
        newReturnedColumns.push_back(rc);
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
            auto newTableAlias =
                tableAliasMap.find({lhsSC->schemaName(), lhsSC->tableName(), lhsSC->tableAlias(), "", false});
            // WIP Leak loosing previous lhs
            if (newTableAlias != tableAliasMap.end())
            {
              lhsSC->tableName("");
              lhsSC->schemaName("");
              lhsSC->tableAlias(newTableAlias->second.first);
              lhsSC->colPosition(0);
              left->lhs(lhs);
            }
          }
        }
      }
    }

    csep.derivedTableList(newDerivedTableList);
    // Replace table list with new table list populated with union units
    csep.tableList(newTableList);
    csep.returnedCols(newReturnedColumns);
  }
}

}  // namespace optimizer

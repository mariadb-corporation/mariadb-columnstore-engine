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

#pragma once

#define PREFER_MY_CONFIG_H
#include <my_config.h>
#include <dbcon/mysql/idb_mysql.h>

#include <optional>
#include <vector>

#include "execplan/calpontselectexecutionplan.h"
#include "execplan/simplecolumn.h"
#include "rulebased_optimizer.h"

namespace optimizer
{
struct TableAliasLessThan
{
  bool operator()(const execplan::CalpontSystemCatalog::TableAliasName& lhs,
                  const execplan::CalpontSystemCatalog::TableAliasName& rhs) const
  {
    if (lhs.schema < rhs.schema)
    {
      return true;
    }
    else if (lhs.schema == rhs.schema)
    {
      if (lhs.table < rhs.table)
      {
        return true;
      }
      else if (lhs.table == rhs.table)
      {
        if (lhs.alias < rhs.alias)
        {
          return true;
        }
      }
    }

    return false;
  }
};

struct SimpleColumnLessThan
{
  bool operator()(const execplan::SimpleColumn* lhs, const execplan::SimpleColumn* rhs) const
  {
    return lhs->columnName() < rhs->columnName();
  }
};

using NewTableAliasAndColumnPosCounter = std::pair<std::string, size_t>;
using SCToPosCounterMap = std::map<execplan::SimpleColumn*, size_t, SimpleColumnLessThan>;
using TableAliasToNewAliasAndSCPositionsMap =
    std::map<execplan::CalpontSystemCatalog::TableAliasName,
             std::tuple<std::string, SCToPosCounterMap, size_t>, TableAliasLessThan>;

// Helper functions in details namespace
namespace details
{

template <typename T>
using FilterRangeBounds = std::vector<std::pair<T, T>>;

bool tableIsInUnion(const execplan::CalpontSystemCatalog::TableAliasName& table,
                    execplan::CalpontSelectExecutionPlan& csep);

bool someAreForeignTables(execplan::CalpontSelectExecutionPlan& csep);

bool someForeignTablesHasStatisticsAndMbIndex(execplan::CalpontSelectExecutionPlan& csep,
                                              optimizer::RBOptimizerContext& ctx);

execplan::SimpleColumn* findSuitableKeyColumn(execplan::CalpontSelectExecutionPlan& csep,
                                              execplan::CalpontSystemCatalog::TableAliasName& targetTable,
                                              optimizer::RBOptimizerContext& ctx);

std::optional<std::pair<execplan::SimpleColumn&, Histogram_json_hb*>> chooseKeyColumnAndStatistics(
    execplan::CalpontSystemCatalog::TableAliasName& targetTable, optimizer::RBOptimizerContext& ctx);

Histogram_json_hb* chooseStatisticsToUse(const std::vector<Histogram_json_hb*>& statisticsVec);

}  // namespace details

// Main functions
bool parallelCESFilter(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx);
bool applyParallelCES(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx);
}  // namespace optimizer
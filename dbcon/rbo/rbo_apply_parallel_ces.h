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

#include <vector>

#include "execplan/calpontselectexecutionplan.h"
#include "execplan/simplecolumn.h"
#include "rulebased_optimizer.h"

namespace optimizer
{
// TableAliasLessThan, SimpleColumnLessThan, SCToPosCounterMap, and TableAliasToNewAliasAndSCPositionsMap
// are now defined in rulebased_optimizer.h

using NewTableAliasAndColumnPosCounter = std::pair<std::string, size_t>;

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

}  // namespace details

// Main functions
bool parallelCESFilter(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx);
bool applyParallelCES(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx);
}  // namespace optimizer
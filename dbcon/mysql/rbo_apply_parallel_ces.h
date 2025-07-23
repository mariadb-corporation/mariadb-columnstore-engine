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
#include "idb_mysql.h"

#include "execplan/calpontselectexecutionplan.h"
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

using NewTableAliasAndColumnPosCounter = std::pair<string, size_t>;
using TableAliasMap = std::map<execplan::CalpontSystemCatalog::TableAliasName,
                               NewTableAliasAndColumnPosCounter, TableAliasLessThan>;

bool matchParallelCES(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx);
void applyParallelCES(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx);
}  // namespace optimizer
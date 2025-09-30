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

#include "execplan/calpontselectexecutionplan.h"
#include "rulebased_optimizer.h"

namespace optimizer {
  bool predicatePushdownFilter(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx);
  bool applyPredicatePushdown(execplan::CalpontSelectExecutionPlan& csep, optimizer::RBOptimizerContext& ctx);
}
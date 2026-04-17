/* Copyright (C) 2026 MariaDB Corporation

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

#include "agg_wrap.h"

#include "execplan/aggregatecolumn.h"

namespace optimizer
{
namespace lib
{

uint32_t AggExprDedup::assignId(execplan::AggregateColumn* ac)
{
  for (size_t i = 0; i < entries.size(); ++i)
  {
    if (*ac == *entries[i].first)
    {
      ac->expressionId(entries[i].second);
      return entries[i].second;
    }
  }
  const uint32_t id = nextId++;
  ac->expressionId(id);
  entries.emplace_back(ac, id);
  return id;
}

}  // namespace lib
}  // namespace optimizer

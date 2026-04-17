/* Copyright (C) 2022 MariaDB Corporation

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

#include "check_filters_limit.h"

#include <algorithm>

#include "constantfilter.h"
#include "operator.h"
#include "parsetree.h"

namespace execplan
{

bool checkFiltersLimit(execplan::ParseTree* tree, uint64_t limit)
{
  uint64_t maxLimit = 0;
  auto walker = [](const execplan::ParseTree* node, void* maxLimit)
  {
    auto maybe_cf = dynamic_cast<ConstantFilter*>(node->data());
    if (maybe_cf != nullptr &&
        (maybe_cf->op()->op() == OpType::OP_OR || maybe_cf->op()->op() == OpType::OP_IN))
    {
      *((uint64_t*)maxLimit) = std::max(maybe_cf->filterList().size(), *((uint64_t*)maxLimit));
    }
  };
  tree->walk(walker, &maxLimit);
  return maxLimit <= limit;
}

}  // namespace execplan

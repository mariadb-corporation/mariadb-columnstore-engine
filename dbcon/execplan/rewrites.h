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

#pragma once

#include <parsetree.h>
#include <treenode.h>
#include <simplefilter.h>

namespace execplan
{

#define debug_rewrites false

execplan::OpType oppositeOperator(execplan::OpType op);

struct NodeSemanticComparator
{
  bool operator()(execplan::ParseTree* left, execplan::ParseTree* right) const;
};

// Walk the tree and find out common conjuctions
template<bool stableSort = false>
execplan::ParseTree* extractCommonLeafConjunctionsToRoot(execplan::ParseTree* tree);
bool checkFiltersLimit(execplan::ParseTree* tree, uint64_t limit);
}  // namespace execplan

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

#include <gtest/gtest.h>


#include <algorithm>
#include <memory>

#include "rewrites.h"

#include "bytestream.h"
#include "objectreader.h"


#include "query19_init.h"
#include "query19_fixed.h"

//using TreePtr = std::unique_ptr<execplan::ParseTree, decltype([](auto* t){execplan::ParseTree::destroyTree(t);})>;
using TreePtr = std::unique_ptr<execplan::ParseTree>;

TEST(Stub, Check)
{
  messageqcpp::ByteStream stream;
  stream.load(__query19_tree_init, sizeof(__query19_tree_init));

  // query from https://jira.mariadb.org/browse/MCOL-4530
  TreePtr tpch_query_19_tree;
  tpch_query_19_tree.reset(execplan::ObjectReader::createParseTree(stream));
  tpch_query_19_tree->drawTree("/tmp/treeq19-init.dot");

  // dummyRewrittenQuery
  auto tpch_query_19_rewritten = extractCommonLeafConjunctionsToRoot(tpch_query_19_tree.get());;
  tpch_query_19_rewritten->drawTree("/tmp/treeq19-rewritten.dot");

  // manualy rewritten query
  stream.load(__query19_tree_fixed, sizeof(__query19_tree_fixed));
  TreePtr tpch_rewritten_query_19_tree;
  tpch_rewritten_query_19_tree.reset(execplan::ObjectReader::createParseTree(stream));
  tpch_rewritten_query_19_tree->drawTree("/tmp/treeq19-fixed.dot");

  EXPECT_EQ(tpch_query_19_rewritten->toString(), tpch_rewritten_query_19_tree->toString() );
}

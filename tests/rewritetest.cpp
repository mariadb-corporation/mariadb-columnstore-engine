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

#include "bytestream.h"
#include "objectreader.h"
#include <parsetree.h>

#include "query19_init.h"
#include "query19_fixed.h"

auto nodeComparator= [](const execplan::TreeNode* left, const execplan::TreeNode*right) {
    return left->data() < right->data();
};
using Comparator = decltype(nodeComparator);
using CommonContainer = std::set<execplan::TreeNode*, Comparator>;


//Walk the tree and find out common conjuctions
void dummmyCommonConjunction(execplan::ParseTree* root, CommonContainer& accumulator, int level = 0) {
  if (root == nullptr) {
    accumulator.clear();
    return;
  }
  auto sep = std::string (level * 4, '-');
  std::cerr << sep << ": " <<  root->data()->data() << std::endl;

  if (root->left() == nullptr && root->left() == nullptr)
  {
    accumulator.insert(root->data());
    return;
  }

  if (root->data()->data() == "or") {
    CommonContainer leftAcc;
    CommonContainer rightAcc;
    dummmyCommonConjunction(root->left(), leftAcc, ++level);
    dummmyCommonConjunction(root->right(), rightAcc, ++level);
    CommonContainer intersection;
    std::set_intersection(leftAcc.begin(), leftAcc.end(),
                   rightAcc.begin(), rightAcc.end(),
                   std::inserter(intersection, intersection.begin()), nodeComparator);

    accumulator = intersection;
    return;
  }

  if (root->data()->data() == "and")
  {
    dummmyCommonConjunction(root->left(), accumulator, ++level);
    dummmyCommonConjunction(root->right(), accumulator, ++level);
    return;
  }

  if (root->left() == nullptr)
  {
    dummmyCommonConjunction(root->right(), accumulator, ++level);
    return;
  }

  dummmyCommonConjunction(root->left(), accumulator, ++level);
  return;
}

execplan::ParseTree* appendToRoot(execplan::ParseTree* tree, const CommonContainer& common)
{
    execplan::Operator* op = new execplan::Operator();
    op->data("and");
    execplan::ParseTree* result = new execplan::ParseTree(op);

    auto current = result;
    for (auto treenode = common.begin(); treenode != common.end();)
    {
      execplan::Operator* op = new execplan::Operator();
      op->data("and");
      execplan::ParseTree* andOp = new execplan::ParseTree(op);
      execplan::ParseTree* andCondition = new execplan::ParseTree(*treenode);

      current->right(andCondition);
      ++treenode;

      if (treenode != common.end())
      {
        current->left(andOp);
        current = andOp;
      }
    }
    current->left(tree);

    return result;
}

execplan::ParseTree* removeFromTree(execplan::ParseTree* tree, const CommonContainer& common)
{
    return tree;
}



execplan::ParseTree* dummyRewrite(execplan::ParseTree* tree, const CommonContainer& common)
{
  removeFromTree(tree, common);
  return appendToRoot(tree, common);
}

//using TreePtr = std::unique_ptr<execplan::ParseTree, decltype([](auto* t){execplan::ParseTree::destroyTree(t);})>;
using TreePtr = std::unique_ptr<execplan::ParseTree>;

template<typename T>
void printContainer(std::ostream& os, const T& container, const std::string& delimiter,
                    std::function<std::string(const typename T::value_type&)> printer, const std::string& preambule = {})
{
    os << preambule << "\n";
    for (auto i = container.begin(); i != container.end();)
    {
      os << printer(*i);
      ++i;
      if (i!= container.end())
        os << delimiter;
    }
    os << std::endl;
}

TEST(Stub, Check)
{
  messageqcpp::ByteStream stream;
  stream.load(__query19_tree_init, sizeof(__query19_tree_init));


  // query from https://jira.mariadb.org/browse/MCOL-4530
  TreePtr tcph_query_19_tree;
  tcph_query_19_tree.reset(execplan::ObjectReader::createParseTree(stream));
  tcph_query_19_tree->drawTree("/tmp/treeq19-init.dot");

  CommonContainer common;
  dummmyCommonConjunction(tcph_query_19_tree.get(), common);
  printContainer(std::cerr, common, "\n", [](auto treenode){ return treenode->data();}, "Common Leaf Conjunctions:");

  // dummyRewrittenQuery
  auto tpch_query_19_rewritten = dummyRewrite(tcph_query_19_tree.get(), common);
  tpch_query_19_rewritten->drawTree("/tmp/treeq19-rewritten.dot");


  // manualy rewritten query
  stream.load(__query19_tree_fixed, sizeof(__query19_tree_fixed));
  TreePtr tcph_rewritten_query_19_tree;
  tcph_rewritten_query_19_tree.reset(execplan::ObjectReader::createParseTree(stream));
  tcph_rewritten_query_19_tree->drawTree("/tmp/treeq19-fixed.dot");

  EXPECT_TRUE(1 == 1);
}

/* Copyright (C) 2022 MariaDB Corporation
 This program is free software; you can redistribute it and/or modify it under the terms of the GNU General
 Public License as published by the Free Software Foundation; version 2 of the License. This program is
 distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 You should have received a copy of the GNU General Public License along with this program; if not, write to
 the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA. */
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include "rewrites.h"
#include "bytestream.h"
#include "objectreader.h"
#include "unitqueries.h"

using TreePtr = std::unique_ptr<execplan::ParseTree>;


bool treeEqual(execplan::ParseTree* fst, execplan::ParseTree* snd)
{
  if (fst == nullptr)
  {
    return snd == nullptr;
  }
  if (snd == nullptr)
  {
    return fst == nullptr;
  }
  return fst->data() == snd->data() &&
         ((treeEqual(fst->left(), snd->left()) && treeEqual(fst->right(), snd->right())) ||
          (treeEqual(fst->left(), snd->right()) && treeEqual(fst->right(), snd->left())));
}

#define REWRITE_TREE_TEST_DEBUG true;


void printTrees(const std::string& queryName, execplan::ParseTree* initial, execplan::ParseTree* rewritten)
{
#ifdef REWRITE_TREE_TEST_DEBUG
  std::string dotPath = std::string("/tmp/") + queryName;
  std::string initialDot = dotPath + ".initial.dot";
  std::string rewrittenDot = dotPath + ".rewritten.dot";

  rewritten->drawTree(rewrittenDot);
  initial->drawTree(initialDot);

  std::string dotInvoke = "dot -Tpng ";

  std::string convertInitial = dotInvoke + initialDot + " -o " +  initialDot + ".png";
  std::string convertRewritten = dotInvoke + rewrittenDot + " -o " +  rewrittenDot + ".png";
  std::cerr << convertRewritten << std::endl;
  system(convertInitial.c_str());
  system(convertRewritten.c_str());
#endif
}

struct ParseTreeTestParam
{
  std::string queryName;
  std::vector<unsigned char>* query = nullptr;
  std::vector<unsigned char>* manually_rewritten_query = nullptr;

  friend std::ostream& operator<<(std::ostream& os, const ParseTreeTestParam& bar)
  {
    return os << bar.queryName;
  }
};

class ParseTreeTest : public testing::TestWithParam<::ParseTreeTestParam> {};

TEST_P(ParseTreeTest, Rewrite)
{
  messageqcpp::ByteStream stream;
  stream.load(GetParam().query->data(), GetParam().query->size());
  execplan::ParseTree* initialTree = execplan::ObjectReader::createParseTree(stream);

  TreePtr rewrittenTree;
  rewrittenTree.reset(execplan::extractCommonLeafConjunctionsToRoot(initialTree));

  printTrees(GetParam().queryName, initialTree, rewrittenTree.get());

  if (GetParam().manually_rewritten_query)
  {
    stream.load(GetParam().manually_rewritten_query->data(), GetParam().manually_rewritten_query->size());
    TreePtr manuallyRewrittenTree;
    manuallyRewrittenTree.reset(execplan::ObjectReader::createParseTree(stream));
    EXPECT_TRUE(treeEqual(manuallyRewrittenTree.get(), rewrittenTree.get()));
  }
  else
  {
    EXPECT_TRUE(treeEqual(initialTree, rewrittenTree.get()));
  }
}

INSTANTIATE_TEST_SUITE_P(TreeRewrites, ParseTreeTest, testing::Values(
  /*
  select t1.posname, t2.posname from t1,t2
  where
  (
  t1.id = t2.id
  and t1.pos + t2.pos < 1000
  )
  or
  (
  t1.id = t2.id
  and t1.pos + t2.pos > 15000
  );
  */
  ParseTreeTestParam{"Query_1", &__test_query_1},


  /*
  select t1.posname, t2.posname
  from t1,t2
  where
  t1.id = t2.id
  and (t1.pos + t2.pos < 1000);
  */
  ParseTreeTestParam{"Query_2", &__test_query_2},


 /*

  select t1.posname, t2.posname
  from t1,t2
  where
  (t1.pos + t2.pos < 1000)
  or
  (t1.pos + t2.pos > 16000)
  or
  (t1.posname < dcba);

  */
  ParseTreeTestParam{"Query_3", &__test_query_3}
),
  [](const ::testing::TestParamInfo<ParseTreeTest::ParamType>& info) {
      return info.param.queryName;
  }
);



#if 0

TEST(Simple, Q1_test)
{
  messageqcpp::ByteStream stream;

  /*

  select t1.posname, t2.posname from t1,t2
  where
  (
  t1.id = t2.id
  and t1.pos + t2.pos < 1000
  )
  or
  (
  t1.id = t2.id
  and t1.pos + t2.pos > 15000
  );

  */
  stream.load(__test_query_1, sizeof(__test_query_1));

  TreePtr query_1_tree;
  query_1_tree.reset(execplan::ObjectReader::createParseTree(stream));
  auto query_1_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_1_tree.get());

  printTrees(query_1_tree.get(), query_1_tree_rewritten);
  EXPECT_TRUE(treeEqual(query_1_tree.get(), query_1_tree_rewritten));
}

TEST(Simple, Q10_test)

{
  messageqcpp::ByteStream stream;

  /*

  select * from t1
  where
  (
  posname > 'qwer'
  and id < 30
  )
  or
  (
  pos > 5000
  and place > 'abcdefghij'
  );

  */
  stream.load(__test_query_10, sizeof(__test_query_10));

  TreePtr query_10_tree;
  query_10_tree.reset(execplan::ObjectReader::createParseTree(stream));
  auto query_10_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_10_tree.get());

  printTrees(query_10_tree.get(), query_10_tree_rewritten);

  EXPECT_TRUE(treeEqual(query_10_tree.get(), query_10_tree_rewritten));
}

TEST(Simple, Q11_test)

{
  messageqcpp::ByteStream stream;

  /*

  select *
  from t1
  where
  (
  pos > 5000
  and id < 30
  )
  or
  (
  pos > 5000
  and id < 30
  );

  */
  stream.load(__test_query_11, sizeof(__test_query_11));

  TreePtr query_11_tree;
  query_11_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_11_tree->drawTree("/tmp/treeq11-init.dot");

  auto query_11_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_11_tree.get());
  query_11_tree_rewritten->drawTree("/tmp/treeq11-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_11_tree.get(), query_11_tree_rewritten));
}

TEST(Simple, Q12_test)

{
  messageqcpp::ByteStream stream;

  /*

  select *
  from t1
  where
  (
  pos > 5000
  and id < 30
  )
  and
  (
  pos > 5000
  and id < 30
  );

  */
  stream.load(__test_query_12, sizeof(__test_query_12));

  TreePtr query_12_tree;
  query_12_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_12_tree->drawTree("/tmp/treeq12-init.dot");

  auto query_12_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_12_tree.get());
  query_12_tree_rewritten->drawTree("/tmp/treeq12-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_12_tree.get(), query_12_tree_rewritten));
}

TEST(Simple, Q13_test)

{
  messageqcpp::ByteStream stream;

  /*

  select *
  from t1
  where
  (
  pos > 5000
  or id < 30
  )
  or
  (
  pos > 5000
  or id < 30
  );

  */
  stream.load(__test_query_13, sizeof(__test_query_13));

  TreePtr query_13_tree;
  query_13_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_13_tree->drawTree("/tmp/treeq13-init.dot");

  auto query_13_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_13_tree.get());
  query_13_tree_rewritten->drawTree("/tmp/treeq13-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_13_tree.get(), query_13_tree_rewritten));
}

TEST(Simple, Q14_test)

{
  messageqcpp::ByteStream stream;

  /*

  select *
  from t1
  where
  (
  id in
  (
  select id
  from t2
  where posname > 'qwer'
  and rid > 10
  )
  )
  and
  (
  pos > 5000
  or id < 30
  );

  */
  stream.load(__test_query_14, sizeof(__test_query_14));

  TreePtr query_14_tree;
  query_14_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_14_tree->drawTree("/tmp/treeq14-init.dot");

  auto query_14_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_14_tree.get());
  query_14_tree_rewritten->drawTree("/tmp/treeq14-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_14_tree.get(), query_14_tree_rewritten));
}

TEST(Simple, Q15_test)

{
  messageqcpp::ByteStream stream;

  /*

  select *
  from t1
  where
  (
  id in
  (
  select id
  from t2
  where
  (
  posname > 'qwer'
  and rid < 10
  )
  or
  (
  posname > 'qwer'
  and rid > 40
  )
  )
  )
  and
  (
  pos > 5000
  or id < 30);

  */
  stream.load(__test_query_15, sizeof(__test_query_15));

  TreePtr query_15_tree;
  query_15_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_15_tree->drawTree("/tmp/treeq15-init.dot");

  auto query_15_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_15_tree.get());
  query_15_tree_rewritten->drawTree("/tmp/treeq15-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_15_tree.get(), query_15_tree_rewritten));
}

TEST(Simple, Q16_test)

{
  messageqcpp::ByteStream stream;

  /*

  select *
  from t1
  where
  (
  id in
  (
  select id
  from t2
  where
  posname > 'qwer'
  and
  (
  rid < 10
  or rid > 40
  )
  )
  )
  and
  (
  pos > 5000
  or id < 30
  );

  */
  stream.load(__test_query_16, sizeof(__test_query_16));

  TreePtr query_16_tree;
  query_16_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_16_tree->drawTree("/tmp/treeq16-init.dot");

  auto query_16_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_16_tree.get());
  query_16_tree_rewritten->drawTree("/tmp/treeq16-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_16_tree.get(), query_16_tree_rewritten));
}

TEST(Simple, Q2_test)

{
  messageqcpp::ByteStream stream;

  /*

  select t1.posname, t2.posname
  from t1,t2
  where
  t1.id = t2.id
  and (t1.pos + t2.pos < 1000);

  */
  stream.load(__test_query_2, sizeof(__test_query_2));

  TreePtr query_2_tree;
  query_2_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_2_tree->drawTree("/tmp/treeq2-init.dot");

  auto query_2_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_2_tree.get());
  query_2_tree_rewritten->drawTree("/tmp/treeq2-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_2_tree.get(), query_2_tree_rewritten));
}

TEST(Simple, Q3_test)

{
  messageqcpp::ByteStream stream;

  /*

  select t1.posname, t2.posname
  from t1,t2
  where
  (t1.pos + t2.pos < 1000)
  or
  (t1.pos + t2.pos > 16000)
  or
  (t1.posname < dcba);

  */
  stream.load(__test_query_3, sizeof(__test_query_3));

  TreePtr query_3_tree;
  query_3_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_3_tree->drawTree("/tmp/treeq3-init.dot");

  auto query_3_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_3_tree.get());
  query_3_tree_rewritten->drawTree("/tmp/treeq3-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_3_tree.get(), query_3_tree_rewritten));
}

TEST(Simple, Q4_test)

{
  messageqcpp::ByteStream stream;

  /*

  select t1.posname, t2.posname
  from t1,t2
  where
  (t1.pos > 20)
  or
  (t2.posname in (select t1.posname from t1 where t1.pos > 20));

  */
  stream.load(__test_query_4, sizeof(__test_query_4));

  TreePtr query_4_tree;
  query_4_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_4_tree->drawTree("/tmp/treeq4-init.dot");

  auto query_4_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_4_tree.get());
  query_4_tree_rewritten->drawTree("/tmp/treeq4-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_4_tree.get(), query_4_tree_rewritten));
}

TEST(Simple, Q5_test)

{
  messageqcpp::ByteStream stream;

  /*

  select t1.posname, t2.posname from t1,t2
  where
  (
  t1.id = t2.id
  or t1.pos + t2.pos < 1000
  )
  and
  (
  t1.id = t2.id
  or t1.pos + t2.pos > 15000
  );

  */
  stream.load(__test_query_5, sizeof(__test_query_5));

  TreePtr query_5_tree;
  query_5_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_5_tree->drawTree("/tmp/treeq5-init.dot");

  auto query_5_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_5_tree.get());
  query_5_tree_rewritten->drawTree("/tmp/treeq5-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_5_tree.get(), query_5_tree_rewritten));
}

TEST(Simple, Q6_test)

{
  messageqcpp::ByteStream stream;

  /*

  select t1.posname, t2.posname from t1,t2
  where
  (
  t1.id = t2.rid
  or t1.pos + t2.pos < 1000
  )
  and
  (
  t1.id = t2.id
  or t1.pos + t2.pos > 15000
  );

  */
  stream.load(__test_query_6, sizeof(__test_query_6));

  TreePtr query_6_tree;
  query_6_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_6_tree->drawTree("/tmp/treeq6-init.dot");

  auto query_6_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_6_tree.get());
  query_6_tree_rewritten->drawTree("/tmp/treeq6-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_6_tree.get(), query_6_tree_rewritten));
}

TEST(Simple, Q7_test)

{
  messageqcpp::ByteStream stream;

  /*

  select t1.posname
  from t1
  where
  t1.posname in
  (
  select t1.posname
  from t1
  where posname > 'qwer'
  and
  id < 30
  );

  */
  stream.load(__test_query_7, sizeof(__test_query_7));

  TreePtr query_7_tree;
  query_7_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_7_tree->drawTree("/tmp/treeq7-init.dot");

  auto query_7_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_7_tree.get());
  query_7_tree_rewritten->drawTree("/tmp/treeq7-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_7_tree.get(), query_7_tree_rewritten));
}

TEST(Simple, Q8_test)

{
  messageqcpp::ByteStream stream;

  /*

  select t1.posname, t2.posname
  from t1,t2
  where t1.posname in
  (
  select t1.posname
  from t1
  where posname > 'qwer'
  and id < 30
  )
  and t1.id = t2.id;

  */
  stream.load(__test_query_8, sizeof(__test_query_8));

  TreePtr query_8_tree;
  query_8_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_8_tree->drawTree("/tmp/treeq8-init.dot");

  auto query_8_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_8_tree.get());
  query_8_tree_rewritten->drawTree("/tmp/treeq8-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_8_tree.get(), query_8_tree_rewritten));
}

TEST(Simple, Q9_test)

{
  messageqcpp::ByteStream stream;

  /*

  select t1.posname, t2.posname
  from t1,t2
  where t1.posname in
  (
  select t1.posname
  from t1
  where posname > 'qwer'
  and id < 30
  ) and
  (
  t1.id = t2.id
  and t1.id = t2.rid
  );

  */
  stream.load(__test_query_9, sizeof(__test_query_9));

  TreePtr query_9_tree;
  query_9_tree.reset(execplan::ObjectReader::createParseTree(stream));
  query_9_tree->drawTree("/tmp/treeq9-init.dot");

  auto query_9_tree_rewritten = execplan::extractCommonLeafConjunctionsToRoot(query_9_tree.get());
  query_9_tree_rewritten->drawTree("/tmp/treeq9-rewritten.dot");

  EXPECT_TRUE(treeEqual(query_9_tree.get(), query_9_tree_rewritten));
}

#endif
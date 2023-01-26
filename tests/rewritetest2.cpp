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
#include "unitqueries_before.h"
#include "unitqueries_after.h"

using TreePtr = std::unique_ptr<execplan::ParseTree>;


bool treeEqual(execplan::ParseTree* fst, execplan::ParseTree* snd, int depth = 0)
{
  if (fst == nullptr)
  {
    return snd == nullptr;
  }
  if (snd == nullptr)
  {
    return fst == nullptr;
  }
  if (fst->data()->data() != snd->data()->data()) {
    std::cerr << "Data " << fst->data()->data() << " differs from " << snd->data()->data() << " at level " << depth << '\n';
    return false;
  }
  return (treeEqual(fst->left(), snd->left(), depth + 1) && treeEqual(fst->right(), snd->right(), depth + 1)) ||
         (treeEqual(fst->left(), snd->right(), depth + 1) && treeEqual(fst->right(), snd->left(), depth + 1));
}

#define REWRITE_TREE_TEST_DEBUG true;


void printTrees(const std::string& queryName, execplan::ParseTree* initial, execplan::ParseTree* rewritten, execplan::ParseTree* reference = nullptr)
{
#ifdef REWRITE_TREE_TEST_DEBUG
  std::string dotPath = std::string("/tmp/") + queryName;
  std::string initialDot = dotPath + ".initial.dot";
  std::string rewrittenDot = dotPath + ".rewritten.dot";
  std::string referenceDot = dotPath + ".reference.dot";

  rewritten->drawTree(rewrittenDot);
  initial->drawTree(initialDot);
  if (reference)
    reference->drawTree(referenceDot);

  std::string dotInvoke = "dot -Tpng ";

  std::string convertInitial = dotInvoke + initialDot + " -o " +  initialDot + ".png";
  std::string convertRewritten = dotInvoke + rewrittenDot + " -o " +  rewrittenDot + ".png";
  std::string convertReference = dotInvoke + referenceDot + " -o " +  referenceDot + ".png";

  [[maybe_unused]] auto _ = std::system(convertInitial.c_str());
  _ = system(convertRewritten.c_str());
  _ = system(convertReference.c_str());
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


  if (GetParam().manually_rewritten_query)
  {
    stream.load(GetParam().manually_rewritten_query->data(), GetParam().manually_rewritten_query->size());
    TreePtr manuallyRewrittenTree;
    manuallyRewrittenTree.reset(execplan::ObjectReader::createParseTree(stream));
    bool result = treeEqual(manuallyRewrittenTree.get(), rewrittenTree.get());
    if (!result)
      printTrees(GetParam().queryName, initialTree, rewrittenTree.get(), manuallyRewrittenTree.get());

    EXPECT_TRUE(result);
  }
  else
  {
    bool result = treeEqual(initialTree, rewrittenTree.get());
    if (!result)
      printTrees(GetParam().queryName, initialTree, rewrittenTree.get());

    EXPECT_TRUE(result);
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

  ParseTreeTestParam{"Query_1", &__test_query_before_1, &__test_query_after_1},

  /*
  select t1.posname, t2.posname
  from t1,t2
  where
  t1.id = t2.id
  and (t1.pos + t2.pos < 1000);
  */
  ParseTreeTestParam{"Query_2", &__test_query_before_2},

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
  ParseTreeTestParam{"Query_3", &__test_query_before_3},

  /*
  select t1.posname, t2.posname
from t1,t2
where
(t1.pos > 20)
or
(t2.posname in (select t1.posname from t1 where t1.pos > 20));
   */


  ParseTreeTestParam{"Query_4", &__test_query_before_4},

/*select t1.posname, t2.posname from t1,t2
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
  ParseTreeTestParam{"Query_5", &__test_query_before_5},

/*select t1.posname, t2.posname from t1,t2
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
  ParseTreeTestParam{"Query_6", &__test_query_before_6},

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
  ParseTreeTestParam{"Query_7", &__test_query_before_7},

/*select t1.posname, t2.posname
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
  ParseTreeTestParam{"Query_8", &__test_query_before_8},

/*select t1.posname, t2.posname
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
  ParseTreeTestParam{"Query_9", &__test_query_before_9},

/*select * from t1
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
  ParseTreeTestParam{"Query_10", &__test_query_before_10},

/*select *
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
  ParseTreeTestParam{"Query_11", &__test_query_before_11, &__test_query_after_11},

/*select *
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
  ParseTreeTestParam{"Query_12", &__test_query_before_12},

/*select *
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
  ParseTreeTestParam{"Query_13", &__test_query_before_13},

/*select *
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
  ParseTreeTestParam{"Query_14", &__test_query_before_14},

/*select *
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
  ParseTreeTestParam{"Query_15", &__test_query_before_15, &__test_query_after_15},

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
)
or
(
pos > 5000
and id < 30
);
*/
  ParseTreeTestParam{"Query_16", &__test_query_before_16, &__test_query_after_16}


),
  [](const ::testing::TestParamInfo<ParseTreeTest::ParamType>& info) {
      return info.param.queryName;
  }
);

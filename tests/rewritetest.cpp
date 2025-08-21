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
#include "unitqueries_tree_before.h"
#include "unitqueries_tree_after.h"

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
  auto comp = execplan::NodeSemanticComparator();
  if (comp(fst, snd) || comp(fst, snd))
  {
    std::cerr << "Data " << fst->data()->data() << " differs from " << snd->data()->data() << " at level "
              << depth << '\n';
    return false;
  }
  return (treeEqual(fst->left(), snd->left(), depth + 1) &&
          treeEqual(fst->right(), snd->right(), depth + 1)) ||
         (treeEqual(fst->left(), snd->right(), depth + 1) && treeEqual(fst->right(), snd->left(), depth + 1));
}

#define REWRITE_TREE_TEST_DEBUG false

void printTree([[maybe_unused]] const std::string& queryName, [[maybe_unused]] execplan::ParseTree* tree,
               [[maybe_unused]] const std::string& treeName)
{
#if REWRITE_TREE_TEST_DEBUG
  std::string dotPath = std::string("/tmp/") + queryName;
  std::string dotFile = dotPath + "." + treeName + ".dot";

  tree->drawTree(dotFile);

  std::string dotInvoke = "dot -Tpng ";
  std::string convertCommand = dotInvoke + dotFile + " -o " + dotFile + ".png";

  [[maybe_unused]] auto _ = std::system(convertCommand.c_str());
#endif
}

struct ParseTreeTestParam
{
  std::string queryName;
  execplan::ParseTree* query = nullptr;
  execplan::ParseTree* manually_rewritten_query = nullptr;

  friend std::ostream& operator<<(std::ostream& os, const ParseTreeTestParam& bar)
  {
    return os << bar.queryName;
  }
};

class ParseTreeTest : public testing::TestWithParam<::ParseTreeTestParam>
{
};

TEST_P(ParseTreeTest, Rewrite)
{
  execplan::ParseTree* initialTree = GetParam().query;
  printTree(GetParam().queryName, initialTree, "initial");

  TreePtr rewrittenTree;
  rewrittenTree.reset(execplan::extractCommonLeafConjunctionsToRoot<true>(initialTree));

  if (GetParam().manually_rewritten_query)
  {
    TreePtr manuallyRewrittenTree;
    manuallyRewrittenTree.reset(GetParam().manually_rewritten_query);
    bool result = treeEqual(manuallyRewrittenTree.get(), rewrittenTree.get());
    printTree(GetParam().queryName, rewrittenTree.get(), "rewritten");
    printTree(GetParam().queryName, manuallyRewrittenTree.get(), "reference");
    EXPECT_TRUE(result);
  }
  else
  {
    bool result = treeEqual(initialTree, rewrittenTree.get());
    printTree(GetParam().queryName, rewrittenTree.get(), "rewritten");

    EXPECT_TRUE(result);
  }
}

INSTANTIATE_TEST_SUITE_P(
    TreeRewrites, ParseTreeTest,
    testing::Values(
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

        ParseTreeTestParam{"Query_1", execplan::initial_Query_1, execplan::reference_Query_1},

        /*
        select t1.posname, t2.posname
        from t1,t2
        where
        t1.id = t2.id
        and (t1.pos + t2.pos < 1000);
        */
        ParseTreeTestParam{"Query_2", execplan::initial_Query_2},

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
        ParseTreeTestParam{"Query_3", execplan::initial_Query_3},

        /*
        select t1.posname, t2.posname
      from t1,t2
      where
      (t1.pos > 20)
      or
      (t2.posname in (select t1.posname from t1 where t1.pos > 20));
         */

        ParseTreeTestParam{"Query_4", execplan::initial_Query_4},

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
        ParseTreeTestParam{"Query_5", execplan::initial_Query_5},

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
        ParseTreeTestParam{"Query_6", execplan::initial_Query_6},

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
        ParseTreeTestParam{"Query_7", execplan::initial_Query_7},

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
        ParseTreeTestParam{"Query_8", execplan::initial_Query_8},

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
        ParseTreeTestParam{"Query_9", execplan::initial_Query_9},

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
        ParseTreeTestParam{"Query_10", execplan::initial_Query_10},

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
        ParseTreeTestParam{"Query_11", execplan::initial_Query_11, execplan::reference_Query_11},

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
        ParseTreeTestParam{"Query_12", execplan::initial_Query_12},

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
        ParseTreeTestParam{"Query_13", execplan::initial_Query_13},

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
        ParseTreeTestParam{"Query_14", execplan::initial_Query_14},

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
        ParseTreeTestParam{"Query_15", execplan::initial_Query_15, execplan::reference_Query_15},
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
        ParseTreeTestParam{"Query_16", execplan::initial_Query_16, execplan::reference_Query_16},
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
        )
        or
        (
        pos > 5000
        and id < 30
        );
         */
        ParseTreeTestParam{"Query_17", execplan::initial_Query_17, execplan::reference_Query_17},
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
        ParseTreeTestParam{"Query_18", execplan::initial_Query_18, execplan::reference_Query_18},
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
        )
        or
        (
        pos > 5000
        and id < 30
        );

         */
        ParseTreeTestParam{"Query_19", execplan::initial_Query_19, execplan::reference_Query_19},
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
        posname > 'qwer'
        and
        id < 30
        and
        place > 'abcdefghij'
        );

         */
        ParseTreeTestParam{"Query_20", execplan::initial_Query_20, execplan::reference_Query_20},
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
        posname > 'qwer'
        and
        id < 30
        and
        place > 'abcdefghij'
        )
        or
        (
        id < 30
        and
        place < 'zyxqwertyu'
        );

         */
        ParseTreeTestParam{"Query_21", execplan::initial_Query_21, execplan::reference_Query_21},
        /*
        select *
        from t1
        where
        (pos > 5000 and id < 30)
        or
        (posname > 'qwer' and id < 30 and place > 'abcdefghij' and pos > 5000)
        or
        (id < 30 and place < 'zyxqwertyu' and pos > 5000)
        or
        (pos > 5000 and id < 30);

         */
        ParseTreeTestParam{"Query_22", execplan::initial_Query_22, execplan::reference_Query_22},

        /*
          select *
        from t1
        where
        (5000 < pos and id < 30)
        or
        (posname > 'qwer' and id < 30 and place > 'abcdefghij' and  5000 < pos)
        or
        (30 > id and place < 'zyxqwertyu' and pos > 5000)
        or
        (pos > 5000 and id < 30);
         */

        ParseTreeTestParam{"Query_23", execplan::initial_Query_23, execplan::reference_Query_23},
        /*
        select *
        from t1
        where
        (pos > 5000 and id < 30 and rid > 20)
        or
        (posname > 'qwer' and id < 30 and place > 'abcdefghij' and pos > 5000 and rid > 20)
        or
        (id < 30 and place < 'zyxqwertyu' and pos > 5000 and rid > 20)
        or
        (pos > 5000 and id < 30 and rid > 20)
        or
        (pos > 5000 and id < 30 and place < 'zyxqwertyu' and rid > 20);
         */

        ParseTreeTestParam{"Query_27", execplan::initial_Query_27, execplan::reference_Query_27},
        /*
        select *
        from t1
        where
        (pos > 5000 and id < 30 and rid > 20 and place < 'zyxqwertyu')
        or
        (posname > 'qwer' and id < 30 and place > 'abcdefghij' and place < 'zyxqwertyu' and pos > 5000 and rid
        > 20) or (id < 30 and place < 'zyxqwertyu' and pos > 5000 and rid > 20) or (pos > 5000 and id < 30 and
        rid > 20 and place < 'zyxqwertyu' and place < 'zyxqwertyu');
         */

        ParseTreeTestParam{"Query_28", execplan::initial_Query_28, execplan::reference_Query_28},
        ParseTreeTestParam{"TPCH_19", execplan::initial_TPCH_19, execplan::reference_TPCH_19}),
    [](const ::testing::TestParamInfo<ParseTreeTest::ParamType>& info) { return info.param.queryName; });

struct ComparatorTestParam
{
  std::string queryName;
  std::string filter;
  std::vector<std::string> existingFilters;
  bool contains;

  friend std::ostream& operator<<(std::ostream& os, const ComparatorTestParam& bar)
  {
    return os << bar.queryName;
  }
};

class ParseTreeComparatorTest : public testing::TestWithParam<ComparatorTestParam>
{
};

struct TestComparator
{
  bool operator()(std::unique_ptr<execplan::ParseTree> const& left,
                  std::unique_ptr<execplan::ParseTree> const& right) const
  {
    execplan::NodeSemanticComparator comp;
    return comp(left.get(), right.get());
  }
};

TEST_P(ParseTreeComparatorTest, CompareContains)
{
  std::set<std::unique_ptr<execplan::ParseTree>, TestComparator> container;
  for (auto const& f : GetParam().existingFilters)
  {
    container.insert(std::make_unique<execplan::ParseTree>(
        new execplan::SimpleFilter(f, execplan::SimpleFilter::ForTestPurposesWithoutColumnsOIDS{})));
  }
  auto filter = std::make_unique<execplan::ParseTree>(new execplan::SimpleFilter(
      GetParam().filter, execplan::SimpleFilter::ForTestPurposesWithoutColumnsOIDS{}));

  ASSERT_EQ(GetParam().contains, container.count(filter) != 0);
}

INSTANTIATE_TEST_SUITE_P(
    Comparator, ParseTreeComparatorTest,
    testing::Values(ComparatorTestParam{"SimpleInverse1", "a=b", {"b=a", "a=a"}, true},
                    ComparatorTestParam{"SimpleInverse2", "acb=bdd", {"b>a", "a=b", "bdd=acb"}, true},
                    ComparatorTestParam{"SimpleInverseOpposite", "a<b", {"b>a"}, true},
                    ComparatorTestParam{"SimpleInverseOpposite2", "a<b", {"a=c", "d=e", "b>a", "a<=b"}, true},
                    ComparatorTestParam{"SimpleContains", "a<b", {"a<b", "a=b", "acb=bdd"}, true},
                    ComparatorTestParam{"SimpleNotContains", "a<b", {"a<b1", "a=b", "acb=bdd"}, false}),
    [](const ::testing::TestParamInfo<ParseTreeComparatorTest::ParamType>& info)
    { return info.param.queryName; });

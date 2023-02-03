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
#include "query19_init.h"
#include "query19_fixed.h"


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


void printTree(const std::string& queryName, execplan::ParseTree* tree, const std::string& treeName)
{
#ifdef REWRITE_TREE_TEST_DEBUG
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
  printTree(GetParam().queryName, initialTree, "initial");

  TreePtr rewrittenTree;
  rewrittenTree.reset(execplan::extractCommonLeafConjunctionsToRoot(initialTree));


  if (GetParam().manually_rewritten_query)
  {
    stream.load(GetParam().manually_rewritten_query->data(), GetParam().manually_rewritten_query->size());
    TreePtr manuallyRewrittenTree;
    manuallyRewrittenTree.reset(execplan::ObjectReader::createParseTree(stream));
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
  ParseTreeTestParam{"Query_16", &__test_query_before_16, &__test_query_after_16},
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
  ParseTreeTestParam{"Query_17", &__test_query_before_17, &__test_query_after_17},
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
  ParseTreeTestParam{"Query_18", &__test_query_before_18, &__test_query_after_18},
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
  ParseTreeTestParam{"Query_19", &__test_query_before_19, &__test_query_after_19},
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
  ParseTreeTestParam{"Query_20", &__test_query_before_20, &__test_query_after_20},
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
  ParseTreeTestParam{"Query_21", &__test_query_before_21, &__test_query_after_21},
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
  ParseTreeTestParam{"Query_22", &__test_query_before_22, &__test_query_after_22},

  ParseTreeTestParam{"Query_27", &__test_query_before_27, &__test_query_after_27},
  ParseTreeTestParam{"TPCH_19", &__query19_tree_init, &__query19_tree_fixed}
),
  [](const ::testing::TestParamInfo<ParseTreeTest::ParamType>& info) {
      return info.param.queryName;
  }
);


struct ComparatorTestParam
{
  std::string queryName;
  std::string filter;
  std::vector<std::string> existingFilters;
  bool contains;

  friend std::ostream& operator<<(std::ostream& os, const  ComparatorTestParam& bar)
  {
    return os << bar.queryName;
  }
};


class ParseTreeComparatorTest : public testing::TestWithParam<ComparatorTestParam> {};


class SimpleFilterMock : public execplan::SimpleFilter
{
 public:

  SimpleFilterMock(std::string const& sql)
  {
    parse(sql);
  }
  void parse(std::string const& sql)
  {
    string delimiter[7] = {">=", "<=", "<>", "!=", "=", "<", ">"};
    string::size_type pos;

    for (int i = 0; i < 7; i++)
    {
      pos = sql.find(delimiter[i], 0);

      if (pos == string::npos)
        continue;

    fOp.reset(new execplan::Operator(delimiter[i]));
    string lhs = sql.substr(0, pos);

    if (lhs.at(0) == ' ')
      lhs = lhs.substr(1, pos);

    if (lhs.at(lhs.length() - 1) == ' ')
      lhs = lhs.substr(0, pos - 1);

    fLhs = lhs;

    pos = pos + delimiter[i].length();
    string rhs = sql.substr(pos, sql.length());

    if (rhs.at(0) == ' ')
      rhs = rhs.substr(1, pos);

    if (rhs.at(rhs.length() - 1) == ' ')
      rhs = rhs.substr(0, pos - 1);

    fRhs = rhs;
    break;
    }

    if (fLhs.empty() || fRhs.empty())
    throw runtime_error("invalid sql for simple filter\n");
  }

  std::string const& lhs() const
  {
    return fLhs;
  }

  std::string const& rhs() const
  {
    return fRhs;
  }

  execplan::SOP const& op() const
  {
    return fOp;
  }

  void op(const execplan::SOP& op)
  {
    fOp = op;
  }

  const std::string data() const
  {
    return fLhs + fOp->data() + fRhs;
  }

  void data(const std::string sql)
  {
    parse(sql);
  }

 private:
  execplan::SOP fOp;
  std::string fLhs;
  std::string fRhs;
};


std::string normalizeNode(std::string const & left, std::string const & right, execplan::Operator* op)
{
  if (left < right)
    return left + op->data() + right;

  std::unique_ptr<execplan::Operator> opposite (op->opposite());

  return right + opposite->data() + left;
}

bool mockFiltersCmp(const SimpleFilterMock* left, const SimpleFilterMock* right)
{
  auto leftN = normalizeNode(left->lhs(), left->rhs(), left->op().get());
  std::cerr << "Left Normalized: " << leftN << std::endl;
  auto rightN = normalizeNode(right->lhs(), right->rhs(), right->op().get());
  std::cerr << "Right Normalized: " << rightN << std::endl;
  return leftN < rightN;

}

struct MockNodeSemanticComparator
{
  bool operator()(std::unique_ptr<execplan::ParseTree> const& left,
                  std::unique_ptr<execplan::ParseTree> const& right) const
  {
    auto filterLeft = dynamic_cast<SimpleFilterMock*>(left->data());
    auto filterRight = dynamic_cast<SimpleFilterMock*>(right->data());

    if (filterLeft && filterRight)
      return mockFiltersCmp(filterLeft, filterRight);


    return left->data()->data() < right->data()->data();
  }
};


TEST_P(ParseTreeComparatorTest, CompareContains)
{
  std::set<std::unique_ptr<execplan::ParseTree>, MockNodeSemanticComparator> container;
  for (auto const& f : GetParam().existingFilters)
  {

    container.insert(std::make_unique<execplan::ParseTree>(new SimpleFilterMock(f)));
  }
  auto filter = std::make_unique<execplan::ParseTree>(new SimpleFilterMock(GetParam().filter));

  ASSERT_EQ(GetParam().contains, container.contains(filter));

}

INSTANTIATE_TEST_SUITE_P(Comparator, ParseTreeComparatorTest,
                         testing::Values(
                          ComparatorTestParam{"SimpleInverse1", "a=b", {"b=a", "a=a"}, true},
                          ComparatorTestParam{"SimpleInverse2", "acb=bdd", {"b>a", "a=b","acb=bdd" }, true},
                          ComparatorTestParam{"SimpleInverseOpposite", "a<b", {"b>a", "a=b","acb=bdd" }, true},
                          ComparatorTestParam{"SimpleContains", "a<b", {"a<b", "a=b","acb=bdd" }, true},
                          ComparatorTestParam{"SimpleNotContains", "a<b", {"a<b1", "a=b","acb=bdd" }, false}
  ),
     [](const ::testing::TestParamInfo<ParseTreeComparatorTest::ParamType>& info)
     { return info.param.queryName; }
 );

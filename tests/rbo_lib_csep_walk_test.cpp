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

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <vector>

#include <boost/make_shared.hpp>

#include <dbcon/execplan/calpontselectexecutionplan.h>
#include <dbcon/execplan/filter.h>
#include <dbcon/execplan/outerjoinonfilter.h>
#include <dbcon/execplan/parsetree.h>
#include <dbcon/rbo/lib/csep_walk.h>

using namespace optimizer::lib;

namespace
{

boost::shared_ptr<execplan::CalpontSelectExecutionPlan> newCSEP()
{
  return boost::make_shared<execplan::CalpontSelectExecutionPlan>();
}

// A small "marker" leaf we can recognise by label via Filter::data().
execplan::ParseTree* makeLeaf(const std::string& label)
{
  return new execplan::ParseTree(new execplan::Filter(label));
}

}  // namespace

// ---------------------------------------------------------------------------
// walkNestedCSEPs
// ---------------------------------------------------------------------------

TEST(CsepWalkTest, WalkVisitsSubSelectListDerivedAndUnion)
{
  auto root = newCSEP();
  auto subA = newCSEP();
  auto subB = newCSEP();
  auto subC = newCSEP();

  root->subSelectList({subA});
  execplan::CalpontSelectExecutionPlan::SelectList derived{subB};
  root->derivedTableList(derived);
  root->unionVec({subC});

  std::set<const execplan::CalpontSelectExecutionPlan*> seen;
  walkNestedCSEPs(*root,
                   [&seen](const execplan::CalpontSelectExecutionPlan& n) -> bool
                   {
                     seen.insert(&n);
                     return false;  // never stop
                   });

  EXPECT_EQ(seen.size(), 3u);
  EXPECT_NE(seen.find(subA.get()), seen.end());
  EXPECT_NE(seen.find(subB.get()), seen.end());
  EXPECT_NE(seen.find(subC.get()), seen.end());
  // Root is never visited by design.
  EXPECT_EQ(seen.find(root.get()), seen.end());
}

TEST(CsepWalkTest, WalkRecursesDeeply)
{
  auto root = newCSEP();
  auto l1 = newCSEP();
  auto l2 = newCSEP();  // nested in l1
  l1->subSelectList({l2});
  root->subSelectList({l1});

  std::set<const execplan::CalpontSelectExecutionPlan*> seen;
  walkNestedCSEPs(*root,
                   [&seen](const execplan::CalpontSelectExecutionPlan& n) -> bool
                   {
                     seen.insert(&n);
                     return false;
                   });
  EXPECT_EQ(seen.size(), 2u);
  EXPECT_NE(seen.find(l1.get()), seen.end());
  EXPECT_NE(seen.find(l2.get()), seen.end());
}

TEST(CsepWalkTest, WalkEarlyStopsWhenVisitorReturnsTrue)
{
  auto root = newCSEP();
  auto subA = newCSEP();
  auto subB = newCSEP();
  root->subSelectList({subA, subB});

  int visits = 0;
  const bool stopped =
      walkNestedCSEPs(*root,
                       [&visits](const execplan::CalpontSelectExecutionPlan&) -> bool
                       {
                         ++visits;
                         return true;  // stop immediately
                       });
  EXPECT_TRUE(stopped);
  EXPECT_EQ(visits, 1);
}

TEST(CsepWalkTest, WalkEmptyReturnsFalse)
{
  auto root = newCSEP();
  bool everVisited = false;
  const bool stopped = walkNestedCSEPs(
      *root, [&everVisited](const execplan::CalpontSelectExecutionPlan&) -> bool
      {
        everVisited = true;
        return true;
      });
  EXPECT_FALSE(stopped);
  EXPECT_FALSE(everVisited);
}

// ---------------------------------------------------------------------------
// collectLeavesInOuterJoinOn
// ---------------------------------------------------------------------------

TEST(CsepWalkTest, CollectLeavesFindsPredicateMatchInsideOJF)
{
  // Build AND(OJF(AND(matching, non_matching)), non_matching_outside) and
  // confirm collector finds only the matching leaf INSIDE the OJF.
  auto* matching = makeLeaf("MATCH");
  auto* nonMatchingInside = makeLeaf("NOPE_IN");
  auto* nonMatchingOutside = makeLeaf("NOPE_OUT");

  // Inner OJF's parse tree: AND(matching, nonMatchingInside)
  auto* innerAnd = new execplan::ParseTree(new execplan::Filter("AND"));
  innerAnd->left(matching);
  innerAnd->right(nonMatchingInside);

  boost::shared_ptr<execplan::ParseTree> innerAndPtr(innerAnd);
  auto* ojf = new execplan::OuterJoinOnFilter();
  ojf->pt(innerAndPtr);

  // Outer AND(OJF, nonMatchingOutside)
  std::unique_ptr<execplan::ParseTree> root(new execplan::ParseTree(new execplan::Filter("AND")));
  root->left(new execplan::ParseTree(ojf));
  root->right(nonMatchingOutside);

  std::vector<execplan::ParseTree*> out;
  collectLeavesInOuterJoinOn(
      root.get(), out,
      [](execplan::TreeNode* tn) -> bool { return tn && tn->data() == "MATCH"; });

  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0]->data()->data(), "MATCH");
}

TEST(CsepWalkTest, CollectLeavesNoOJFYieldsNothing)
{
  std::unique_ptr<execplan::ParseTree> root(makeLeaf("MATCH"));
  std::vector<execplan::ParseTree*> out;
  collectLeavesInOuterJoinOn(
      root.get(), out, [](execplan::TreeNode* tn) -> bool { return tn && tn->data() == "MATCH"; });
  EXPECT_TRUE(out.empty());
}

TEST(CsepWalkTest, CollectLeavesNullRootIsNoOp)
{
  std::vector<execplan::ParseTree*> out;
  collectLeavesInOuterJoinOn(nullptr, out, [](execplan::TreeNode*) { return true; });
  EXPECT_TRUE(out.empty());
}

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
#include <string>
#include <vector>

#include <dbcon/execplan/filter.h>
#include <dbcon/execplan/logicoperator.h>
#include <dbcon/execplan/parsetree.h>
#include <dbcon/rbo/lib/parse_tree_ops.h>

using namespace optimizer::lib;

namespace
{

// Build a leaf parse tree whose data is a Filter with the given label.
// Filter is the simplest concrete TreeNode we can construct without wiring
// up a full SimpleFilter / Operator / SimpleColumn triple.
execplan::ParseTree* makeLeaf(const std::string& label)
{
  return new execplan::ParseTree(new execplan::Filter(label));
}

// Returns `node->data()->data()` (label) or "<null>" if node is null.
std::string labelOf(const execplan::ParseTree* node)
{
  if (!node || !node->data())
    return "<null>";
  return node->data()->data();
}

}  // namespace

// ---------------------------------------------------------------------------
// Factory smoke tests
// ---------------------------------------------------------------------------

TEST(ParseTreeOpsTest, NewAndNodeCreatesAndNode)
{
  std::unique_ptr<execplan::ParseTree> n(newAndNode());
  ASSERT_NE(n.get(), nullptr);
  EXPECT_TRUE(isAnd(n.get()));
  EXPECT_FALSE(isOr(n.get()));
  EXPECT_EQ(n->left(), nullptr);
  EXPECT_EQ(n->right(), nullptr);
}

TEST(ParseTreeOpsTest, NewOrNodeCreatesOrNode)
{
  std::unique_ptr<execplan::ParseTree> n(newOrNode());
  ASSERT_NE(n.get(), nullptr);
  EXPECT_TRUE(isOr(n.get()));
  EXPECT_FALSE(isAnd(n.get()));
}

TEST(ParseTreeOpsTest, NewLogicNodeRespectsOpName)
{
  std::unique_ptr<execplan::ParseTree> n(newLogicNode("xor"));
  ASSERT_NE(n.get(), nullptr);
  // Custom ops just shouldn't be AND/OR.
  EXPECT_FALSE(isAnd(n.get()));
  EXPECT_FALSE(isOr(n.get()));
}

// ---------------------------------------------------------------------------
// andAll / orAll
// ---------------------------------------------------------------------------

TEST(ParseTreeOpsTest, AndAllEmptyReturnsNull)
{
  EXPECT_EQ(andAll({}), nullptr);
  EXPECT_EQ(orAll({}), nullptr);
}

TEST(ParseTreeOpsTest, AndAllSingletonReturnsLeafUnchanged)
{
  auto* a = makeLeaf("a");
  auto* r = andAll({a});
  EXPECT_EQ(r, a);
  delete r;
}

TEST(ParseTreeOpsTest, AndAllTwoLeavesBuildsAndNode)
{
  auto* a = makeLeaf("a");
  auto* b = makeLeaf("b");
  std::unique_ptr<execplan::ParseTree> r(andAll({a, b}));
  ASSERT_NE(r.get(), nullptr);
  EXPECT_TRUE(isAnd(r.get()));
  EXPECT_EQ(r->left(), a);
  EXPECT_EQ(r->right(), b);
}

TEST(ParseTreeOpsTest, AndAllThreeLeavesIsRightDeep)
{
  auto* a = makeLeaf("a");
  auto* b = makeLeaf("b");
  auto* c = makeLeaf("c");
  std::unique_ptr<execplan::ParseTree> r(andAll({a, b, c}));
  ASSERT_NE(r.get(), nullptr);
  EXPECT_TRUE(isAnd(r.get()));
  EXPECT_EQ(r->left(), a);
  ASSERT_NE(r->right(), nullptr);
  EXPECT_TRUE(isAnd(r->right()));
  EXPECT_EQ(r->right()->left(), b);
  EXPECT_EQ(r->right()->right(), c);
}

TEST(ParseTreeOpsTest, OrAllThreeLeavesIsRightDeep)
{
  auto* a = makeLeaf("a");
  auto* b = makeLeaf("b");
  auto* c = makeLeaf("c");
  std::unique_ptr<execplan::ParseTree> r(orAll({a, b, c}));
  ASSERT_NE(r.get(), nullptr);
  EXPECT_TRUE(isOr(r.get()));
  EXPECT_EQ(r->left(), a);
  ASSERT_NE(r->right(), nullptr);
  EXPECT_TRUE(isOr(r->right()));
  EXPECT_EQ(r->right()->left(), b);
  EXPECT_EQ(r->right()->right(), c);
}

// ---------------------------------------------------------------------------
// andWith / orWith
// ---------------------------------------------------------------------------

TEST(ParseTreeOpsTest, AndWithBothNullReturnsNull)
{
  EXPECT_EQ(andWith(nullptr, nullptr), nullptr);
}

TEST(ParseTreeOpsTest, AndWithNullLhsReturnsRhsUntouched)
{
  auto* rhs = makeLeaf("r");
  auto* r = andWith(nullptr, rhs);
  EXPECT_EQ(r, rhs);
  delete r;
}

TEST(ParseTreeOpsTest, AndWithNullRhsReturnsLhsUntouched)
{
  auto* lhs = makeLeaf("l");
  auto* r = andWith(lhs, nullptr);
  EXPECT_EQ(r, lhs);
  delete r;
}

TEST(ParseTreeOpsTest, AndWithBothSidesWrapsInAnd)
{
  auto* lhs = makeLeaf("l");
  auto* rhs = makeLeaf("r");
  std::unique_ptr<execplan::ParseTree> r(andWith(lhs, rhs));
  ASSERT_NE(r.get(), nullptr);
  EXPECT_TRUE(isAnd(r.get()));
  EXPECT_EQ(r->left(), lhs);
  EXPECT_EQ(r->right(), rhs);
}

TEST(ParseTreeOpsTest, OrWithMirrorsAndWith)
{
  auto* lhs = makeLeaf("l");
  auto* rhs = makeLeaf("r");
  std::unique_ptr<execplan::ParseTree> r(orWith(lhs, rhs));
  ASSERT_NE(r.get(), nullptr);
  EXPECT_TRUE(isOr(r.get()));
  EXPECT_EQ(r->left(), lhs);
  EXPECT_EQ(r->right(), rhs);
}

// ---------------------------------------------------------------------------
// deleteOneNode
// ---------------------------------------------------------------------------

TEST(ParseTreeOpsTest, DeleteOneNodeNullSafe)
{
  execplan::ParseTree* p = nullptr;
  deleteOneNode(&p);
  EXPECT_EQ(p, nullptr);
  deleteOneNode(nullptr);  // must not crash
}

TEST(ParseTreeOpsTest, DeleteOneNodeDoesNotTouchChildren)
{
  // Build AND(a, b).  Deleting the root via deleteOneNode must null the
  // pointer and leave a, b alive (we delete them afterwards manually).
  auto* a = makeLeaf("a");
  auto* b = makeLeaf("b");
  auto* root = newAndNode();
  root->left(a);
  root->right(b);

  deleteOneNode(&root);
  EXPECT_EQ(root, nullptr);

  // a and b are still valid.
  EXPECT_EQ(labelOf(a), "a");
  EXPECT_EQ(labelOf(b), "b");
  delete a;
  delete b;
}

// ---------------------------------------------------------------------------
// replaceInPlace
// ---------------------------------------------------------------------------

TEST(ParseTreeOpsTest, ReplaceInPlaceSwapsLeafContents)
{
  // target is a standalone leaf, src is AND(x, y).  After replaceInPlace
  // target becomes AND(x, y) and src is destroyed.
  auto* target = makeLeaf("old");
  auto* x = makeLeaf("x");
  auto* y = makeLeaf("y");
  auto* src = newAndNode();
  src->left(x);
  src->right(y);

  replaceInPlace(target, src);

  EXPECT_TRUE(isAnd(target));
  EXPECT_EQ(target->left(), x);
  EXPECT_EQ(target->right(), y);

  delete target;  // owns x, y now
}

TEST(ParseTreeOpsTest, ReplaceInPlaceNullsSafe)
{
  auto* target = makeLeaf("t");
  replaceInPlace(target, nullptr);
  EXPECT_EQ(labelOf(target), "t");
  delete target;

  replaceInPlace(nullptr, nullptr);  // must not crash
}

// ---------------------------------------------------------------------------
// collectConjuncts
// ---------------------------------------------------------------------------

TEST(ParseTreeOpsTest, CollectConjunctsNullIsEmpty)
{
  std::vector<execplan::ParseTree*> out;
  EXPECT_TRUE(collectConjuncts(nullptr, out));
  EXPECT_TRUE(out.empty());
}

TEST(ParseTreeOpsTest, CollectConjunctsSingleLeaf)
{
  std::unique_ptr<execplan::ParseTree> a(makeLeaf("a"));
  std::vector<execplan::ParseTree*> out;
  EXPECT_TRUE(collectConjuncts(a.get(), out));
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0], a.get());
}

TEST(ParseTreeOpsTest, CollectConjunctsRightDeepAndThree)
{
  auto* a = makeLeaf("a");
  auto* b = makeLeaf("b");
  auto* c = makeLeaf("c");
  std::unique_ptr<execplan::ParseTree> root(andAll({a, b, c}));

  std::vector<execplan::ParseTree*> out;
  EXPECT_TRUE(collectConjuncts(root.get(), out));
  ASSERT_EQ(out.size(), 3u);
  EXPECT_EQ(out[0], a);
  EXPECT_EQ(out[1], b);
  EXPECT_EQ(out[2], c);
}

TEST(ParseTreeOpsTest, CollectConjunctsRejectsOrInside)
{
  // AND(a, OR(b, c)) — OR inside must fail the check.
  auto* a = makeLeaf("a");
  auto* b = makeLeaf("b");
  auto* c = makeLeaf("c");
  auto* orNode = orAll({b, c});
  std::unique_ptr<execplan::ParseTree> root(andAll({a, orNode}));

  std::vector<execplan::ParseTree*> out;
  EXPECT_FALSE(collectConjuncts(root.get(), out));
}

// ---------------------------------------------------------------------------
// logicOpType / isAnd / isOr
// ---------------------------------------------------------------------------

TEST(ParseTreeOpsTest, IsAndOrHandleNullAndNonLogic)
{
  EXPECT_FALSE(isAnd(nullptr));
  EXPECT_FALSE(isOr(nullptr));
  EXPECT_EQ(logicOpType(nullptr), execplan::OP_UNKNOWN);

  std::unique_ptr<execplan::ParseTree> leaf(makeLeaf("a"));
  EXPECT_FALSE(isAnd(leaf.get()));
  EXPECT_FALSE(isOr(leaf.get()));
  EXPECT_EQ(logicOpType(leaf.get()), execplan::OP_UNKNOWN);
}

#include "rewrites.h"
#include <typeinfo>
#include "objectreader.h"
#include "parsetree.h"
#include "operator.h"
#include "simplefilter.h"
#include "logicoperator.h"
#include <boost/core/demangle.hpp>
#include <set>
#include <string>
#include <ostream>

namespace execplan
{
namespace details
{
template <typename T, typename F>
void printContainer(std::ostream& os, const T& container, const std::string& delimiter,
                    const F & printer,
                    const std::string& preambule = {})
{
  os << preambule << "\n";
  for (auto i = container.begin(); i != container.end();)
  {
    os << printer(*i);
    ++i;
    if (i != container.end())
      os << delimiter;
  }
  os << std::endl;
}

#define debug_rewrites true

// using CommonPtr = std::shared_ptr<execplan::SimpleFilter>;

SimpleFilter* castToSimpleFilter(execplan::TreeNode* node)
{
  return dynamic_cast<SimpleFilter*>(node);
}

auto nodeComparator = [](execplan::ParseTree* left, execplan::ParseTree* right)
{
  auto filterLeft = castToSimpleFilter(left->data());
  auto filterRight = castToSimpleFilter(right->data());

  if (filterLeft && filterRight &&filterLeft->semanticEq(*filterRight))
    return false;

  return left->data()->data() < right->data()->data();
};



using CommonContainer = std::pair<std::set<execplan::ParseTree*, decltype(nodeComparator)>, std::set<execplan::ParseTree*>>;

execplan::Filter* castToFilter(execplan::ParseTree* node)
{
  return dynamic_cast<execplan::Filter*>(node->data());
}

bool commonContainsSemantic(const CommonContainer & common, execplan::ParseTree* node)
{
  auto filter = castToFilter(node);
  return filter && common.first.contains(node);
}

bool commonContainsPtr(const CommonContainer & common, execplan::ParseTree* node)
{
  return common.second.contains(node);
}

OpType operatorType(execplan::ParseTree* node)
{
  auto op = dynamic_cast<Operator*>(node->data());
  if (!op)
    return OP_UNKNOWN;

  return op->op();
}

void printTreeLevel(execplan::ParseTree* root, int level)
{
#ifdef debug_rewrites
  auto sep = std::string(level * 4, '-');
  auto& node = *(root->data());
  std::cerr << sep << ": " << root->data()->data() << " "
            << boost::core::demangle(typeid(node).name()) << " " << root->data() << std::endl;
#endif
}

// Walk the tree and find out common conjuctions
void collectCommonConjuctions(execplan::ParseTree* root, CommonContainer& accumulator, int level = 0,
                              bool orMeeted = false, bool andParent = false)
{
  if (root == nullptr)
  {
    accumulator.first.clear();
    accumulator.second.clear();
    return;
  }

  printTreeLevel(root, level);

  if (root->left() == nullptr && root->right() == nullptr && orMeeted && andParent)
  {
    if (castToFilter(root))
    {
      accumulator.first.insert(root);
    }
    return;
  }

  if (operatorType(root) == OP_OR)
  {
    CommonContainer leftAcc;
    CommonContainer rightAcc;
    collectCommonConjuctions(root->left(), leftAcc, ++level, true, false);
    collectCommonConjuctions(root->right(), rightAcc, ++level, true, false);
    CommonContainer intersection;
    std::set_intersection(leftAcc.first.begin(), leftAcc.first.end(), rightAcc.first.begin(), rightAcc.first.end(),
                          std::inserter(intersection.first, intersection.first.begin()), nodeComparator);

    accumulator = intersection;
    return;
  }

  if (operatorType(root) == OP_AND)
  {
    collectCommonConjuctions(root->left(), accumulator, ++level, orMeeted, true);
    collectCommonConjuctions(root->right(), accumulator, ++level, orMeeted, true);
    return;
  }

  if (root->left() == nullptr)
  {
    collectCommonConjuctions(root->right(), accumulator, ++level, orMeeted, false);
    return;
  }

  collectCommonConjuctions(root->left(), accumulator, ++level, orMeeted, false);
  return;
}


execplan::ParseTree* newAndNode()
{
  execplan::Operator* op = new execplan::Operator();
  op->data("and");
  return new execplan::ParseTree(op);
}

execplan::ParseTree* appendToRoot(execplan::ParseTree* tree, const CommonContainer& common)
{
  if (common.first.empty())
    return tree;

  execplan::ParseTree* result = newAndNode();
  auto current = result;
  for (auto treenode = common.first.begin(); treenode != common.first.end();)
  {
    execplan::ParseTree* andOp = newAndNode();
    execplan::ParseTree* andCondition = *treenode;

    ++treenode;
    current->right(andCondition);

    if (treenode != common.first.end() && std::next(treenode) != common.first.end())
    {
      current->left(andOp);
      current = andOp;
    }
    else if (std::next(treenode) == common.first.end() && tree != nullptr)
    {
      current->left(andOp);
      current = andOp;
    }
    else if (std::next(treenode) == common.first.end() && tree == nullptr)
    {
      current->left(andCondition);
    }
  }
  if (tree)
    current->left(tree);

  return result;
}

enum class GoTo
{
  Left,
  Right,
  Up
};

struct StackFrame
{
  execplan::ParseTree** node;
  GoTo direction;
  StackFrame(execplan::ParseTree** node_, GoTo direction_) : node(node_), direction(direction_) {}
};

using DFSStack = std::vector<StackFrame>;

void deleteOneNode(execplan::ParseTree** node)
{
  if (!node || !*node)
    return;

  (*node)->nullLeft();
  (*node)->nullRight();

#ifdef debug_rewrites
  std::cerr << "   Deleting: " <<  (*node)->data()->data() << " " << boost::core::demangle(typeid(**node).name()) <<
            " " << "ptr: " << (*node)->data() << std::endl;
#endif

  delete *node;
  *node = nullptr;
}

void replaceAncestor(execplan::ParseTree* father, GoTo direction, execplan::ParseTree* ancestor, const CommonContainer & common)
{
  if (direction == GoTo::Up)
  {
    // if not in commontPtrs
    if (!commonContainsPtr(common, father->right()))
    {
      deleteOneNode(father->rightRef());
    }
    father->right(ancestor);
  }
  else
  {
    if (!commonContainsPtr(common, father->left()))
    {
      deleteOneNode(father->leftRef());
    }
    father->left(ancestor);
  }
}

void nullAncestor(execplan::ParseTree* father, GoTo direction)
{
  if (direction == GoTo::Right)
  {
    deleteOneNode(father->leftRef());
  }
  else
  {
    deleteOneNode(father->rightRef());
  }
}


void fixUpTree(execplan::ParseTree** root, execplan::ParseTree** node, DFSStack& stack, const CommonContainer& common)
{
  // stack size is always >= 3 here
  auto sz = stack.size();
  assert (sz >=3);
  auto [father, fatherflag] = stack.at(sz - 2);

  if (operatorType(*node) != OP_AND)
    return;

  bool containsLeft =  commonContainsSemantic(common, (*node)->left());
  bool containsRight = commonContainsSemantic(common, (*node)->right());

  if (containsLeft && containsRight)
  {
    nullAncestor(*father, fatherflag);
    if (fatherflag == GoTo::Right)
    {
      auto [grandfather, grandfatherflag] = stack.at(sz - 3);
      auto newfather = (*father)->right();
      stack.at(sz - 2).direction = GoTo::Left;
      replaceAncestor(*grandfather, grandfatherflag, newfather, common);
    }
    else
    {
      if ((*stack.at(sz - 2).node)->left() != nullptr)
      {
        auto [grandfather, grandfatherflag] = stack.at(sz - 3);
        auto newfather = (*stack.at(sz - 2).node)->left();
        stack.at(sz - 2).direction = GoTo::Up;
        replaceAncestor(*grandfather, grandfatherflag, newfather, common);
      }
      else
      {
        auto [ancestor, ancflag] = stack.at(sz - 3);
        nullAncestor(*ancestor, ancflag);
      }
    }
  }
  else if (containsLeft)
  {
    replaceAncestor(*father, fatherflag, (*node)->right(), common);
  }
  else if (containsRight)
  {
    replaceAncestor(*father, fatherflag, (*node)->left(), common);
  }
}

void addStackFrame(DFSStack &stack, GoTo direction, execplan::ParseTree* node)
{
  if (direction == GoTo::Left)
  {
    stack.back().direction = GoTo::Right;
    if (node->left() != nullptr)
    {
      auto left = node->leftRef();
      stack.emplace_back(left, GoTo::Left);
    }
  }
  else if (direction ==  GoTo::Right)
  {
    stack.back().direction = GoTo::Up;
    if (node->right() != nullptr)
    {
      auto right = node->rightRef();
      stack.emplace_back(right, GoTo::Left);
    }
  }
}

void removeFromTreeIterative(execplan::ParseTree** root, const CommonContainer& common)
{
  DFSStack stack;
  stack.emplace_back(root, GoTo::Left);
  while (!stack.empty())
  {
    auto [node, flag] = stack.back();
    if (flag != GoTo::Up)
    {
      addStackFrame(stack, flag, *node);
      continue;
    }

    auto sz = stack.size();
    if (sz > 2)
    {
      fixUpTree(root, node, stack, common);
    }
    else
    {
      if (operatorType(*node) == OP_AND)
      {
        bool containsLeft = commonContainsSemantic(common, (*node)->left());
        bool containsRight = commonContainsSemantic(common, (*node)->right());

        if (containsLeft && containsRight)
        {
          if (sz == 1)
          {
            deleteOneNode(root);
            return; // ->>>>>>
          }
          else
          {
            auto [father, fatherflag] = stack.at(0);
            nullAncestor(*father, fatherflag);
            if (fatherflag == GoTo::Right)
            {
              deleteOneNode((*root)->leftRef());
              *root = (*root)->right();
              stack.at(0).direction = GoTo::Left;
            }
            else if (fatherflag == GoTo::Up)
            {
              deleteOneNode((*root)->rightRef());
              *root = (*root)->left();
            }
          }
        }
        else if (containsLeft)
        {
          if (sz == 1)
          {
            auto oldRoot = root;
            *root = (*node)->right();
            deleteOneNode(oldRoot);
            stack.at(0).direction = GoTo::Left;
          }
          else //sz == 2
          {
            auto [father, fatherflag] = stack.at(0);
            if (!commonContainsPtr(common, (*node)->left()))
            {
              deleteOneNode((*node)->leftRef());
            }
            replaceAncestor(*father, fatherflag, (*node)->right(), common);
          }
        }
        else if (containsRight)
        {
          if (sz == 1)
          {
            deleteOneNode(root);
            *root = (*node)->left();
          }
          else //sz == 2
          {
            auto [father, fatherflag] = stack.at(0);
            if (!commonContainsPtr(common, (*node)->right()))
            {
              deleteOneNode((*node)->rightRef());
            }
            replaceAncestor(*father, fatherflag, (*node)->left(), common);
          }
        }
      }
    }

    stack.pop_back();
  }
}

}  // namespace details

bool treeEqual(execplan::ParseTree* fst, execplan::ParseTree* snd)
{
  if (fst == nullptr || snd == nullptr)
  {
    return fst == snd;
  }

  return fst->data() == snd->data() &&
         ((treeEqual(fst->left(), snd->left()) && treeEqual(fst->right(), snd->right())) ||
          (treeEqual(fst->left(), snd->right()) && treeEqual(fst->right(), snd->left())));
}

void dumpTreeFiles(execplan::ParseTree* filters, const std::string& name)
{
#ifdef debug_rewrites
  messageqcpp::ByteStream beforetree;
  ObjectReader::writeParseTree(filters, beforetree);
  std::ofstream before("/tmp/filters." + name + ".data");
  before << beforetree;
  std::string dotname = "/tmp/filters." + name + ".dot";
  filters->drawTree(dotname);
  std::string dotInvoke = "dot -Tpng ";
  std::string convert = dotInvoke + dotname + " -o " + dotname + ".png";
  [[maybe_unused]] auto _ = std::system(convert.c_str());
#endif
}

execplan::ParseTree* extractCommonLeafConjunctionsToRoot(execplan::ParseTree* tree, bool dumpOnly)
{
  dumpTreeFiles(tree, "0.initial");

  if (dumpOnly)
    return tree;

  details::CommonContainer common;
  details::collectCommonConjuctions(tree, common);

  //// SHIIITTT
  for (auto it = common.first.begin(); it != common.first.end(); ++it)
  {
    common.second.insert(*it);
  }


#ifdef debug_rewrites
  printContainer(
      std::cerr, common.first, "\n", [](auto treenode) { return treenode->data(); }, "Common Leaf Conjunctions:");
#endif

  details::removeFromTreeIterative(&tree, common);

  auto result = details::appendToRoot(tree, common);

  dumpTreeFiles(result, "3.final");
  return result;
}

}  // namespace execplan

#include "rewrites.h"
#include <typeinfo>
#include "objectreader.h"
#include "parsetree.h"
#include "operator.h"
#include "simplefilter.h"
#include <boost/core/demangle.hpp>
#include <set>
#include <string>
#include <ostream>



namespace execplan
{
namespace details
{
template <typename T>
void printContainer(std::ostream& os, const T& container, const std::string& delimiter,
                    std::function<std::string(const typename T::value_type&)> printer,
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


//using CommonPtr = std::shared_ptr<execplan::SimpleFilter>;

using CommonPtr = execplan::TreeNode*;
auto nodeComparator = [](const CommonPtr& left, const CommonPtr& right)
{
    // if (left->semanticEq(right.get())
    //   return false;

    return left->data() < right->data();
};


using CommonContainer = std::set<CommonPtr, decltype(nodeComparator)>;

// Walk the tree and find out common conjuctions
void collectCommonConjuctions(execplan::ParseTree* root, CommonContainer& accumulator, int level = 0, bool orMeeted = false, bool andParent = false)
{
  if (root == nullptr)
  {
    accumulator.clear();
    return;
  }

  #ifdef debug_rewrites
    auto sep = std::string(level * 4, '-');
    std::cerr << sep << ": " << root->data()->data() << " " << boost::core::demangle(typeid(*(root->data())).name()) << std::endl;
  #endif

  if (root->left() == nullptr && root->right() == nullptr && orMeeted && andParent)
  {
    accumulator.insert(root->data());
    return;
  }

  if (root->data()->data() == "or")
  {
    CommonContainer leftAcc;
    CommonContainer rightAcc;
    collectCommonConjuctions(root->left(), leftAcc, ++level, true, false);
    collectCommonConjuctions(root->right(), rightAcc, ++level, true, false);
    CommonContainer intersection;
    std::set_intersection(leftAcc.begin(), leftAcc.end(), rightAcc.begin(), rightAcc.end(),
                          std::inserter(intersection, intersection.begin()), nodeComparator);

    accumulator = intersection;
    return;
  }

  if (root->data()->data() == "and")
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

execplan::ParseTree* appendToRoot(execplan::ParseTree* tree, const CommonContainer& common)
{
  if (common.empty())
    return tree;

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

    ++treenode;
    current->right(andCondition);

    if (treenode != common.end() && std::next(treenode) != common.end())
    {
      current->left(andOp);
      current = andOp;
    }
    else if (std::next(treenode) == common.end() && tree != nullptr)
    {
      current->left(andOp);
      current = andOp;
    }
    else if (std::next(treenode) == common.end() && tree == nullptr)
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

void fixUpTree(execplan::ParseTree** node, std::vector<std::pair<execplan::ParseTree*, GoTo>>& stack,
               const CommonContainer& common)
{
  auto sz = stack.size();
  auto [father, fatherflag] = stack.at(sz - 2);

  if ((*node)->data()->data() != "and")
    return;

  bool containsLeft = !(*node)->left() || common.contains((*node)->left()->data());
  bool containsRight = common.contains((*node)->right()->data());
  if (containsLeft && containsRight)
  {
    if (fatherflag == GoTo::Right)
    {
      father->nullLeft();
    }
    else
    {
      father->nullRight();
      for (int prev = sz - 2; sz-->0;)
      {
        if (stack.at(prev).first->left() == nullptr)
        {
          if (prev == 0)
          {
            *node = nullptr;
          }
          else
          {
            auto [ancestor, ancflag] = stack.at(prev - 1);
            if (ancflag == GoTo::Right)
              ancestor->nullLeft();
            else
            {
              ancestor->nullRight();
            }
          }
        }
        else
          break;
      }
    }
  }
  else if (containsLeft)
  {
    if (fatherflag == GoTo::Up)
    {
      father->right((*node)->right());
    }
    else
      father->left((*node)->right());
  }
  else if (containsRight)
  {
    if (fatherflag == GoTo::Up)
    {
      father->right((*node)->left());
    }
    else
      father->left((*node)->left());
  }
}

void removeFromTreeIterative(execplan::ParseTree** root, const CommonContainer& common)
{
  std::vector<std::pair<execplan::ParseTree*, GoTo>> stack;
  stack.emplace_back(*root, GoTo::Left);
  while (!stack.empty())
  {
    auto [node, flag] = stack.back();
    if (node != nullptr) {
    switch (flag)
    {
      case GoTo::Left:
        stack.back().second = GoTo::Right;
        if (node->left() != nullptr)
        {
          stack.emplace_back(node->left(), GoTo::Left);
        }
        break;
      case GoTo::Right:
        stack.back().second = GoTo::Up;
        if (node->right() != nullptr)
        {
          stack.emplace_back(node->right(), GoTo::Left);
        }
        break;
      default:
        auto sz = stack.size();
        if (sz == 2)
        {
          auto [father, fatherflag] = stack.at(0);
          if (node->data()->data() == "and")
          {
            bool containsLeft = common.contains(node->left()->data());
            bool containsRight = common.contains(node->right()->data());
            if (containsLeft && containsRight)
            {
              if (fatherflag == GoTo::Right)
              {
                father->nullLeft();
              }
              else
              {
                father->nullRight();

                if (father->left() == nullptr)
                {
                  *root = nullptr;
                }
              }
            }
            else if (containsLeft)
            {
              if (fatherflag == GoTo::Up)
                father->right(node->right());
              else
                father->left(node->right());
            }
            else if (containsRight)
            {
              if (fatherflag == GoTo::Up)
                father->right(node->left());
              else
                father->left(node->left());
            }
          }
        }
        else if (sz > 2)
        {
          fixUpTree(&node, stack, common);
        }
        stack.pop_back();
        break;
    }
    }
  }
}

}  // namespace details

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


void dumpTreeFiles(execplan::ParseTree* filters, const std::string& name)
{
#ifdef debug_rewrites
  messageqcpp::ByteStream beforetree;
  ObjectReader::writeParseTree(filters, beforetree);
  std::ofstream before("/tmp/filters." + name + ".data");
  before << beforetree;
  std::string dotname = "/tmp/filters." + name +".dot";
  filters->drawTree(dotname);
  std::string dotInvoke = "dot -Tpng ";
  std::string convert = dotInvoke + dotname + " -o " +  dotname + ".png";
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

  #ifdef debug_rewrites
    printContainer(std::cerr, common, "\n", [](auto treenode) { return treenode->data(); }, "Common Leaf Conjunctions:");
  #endif

  details::removeFromTreeIterative(&tree, common);

  auto result = details::appendToRoot(tree, common);

  dumpTreeFiles(result, "3.final");
  return result;
}

}  // namespace execplan

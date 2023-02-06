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

using CommonContainer = std::pair<std::set<execplan::ParseTree*, NodeSemanticComparator>, std::set<execplan::ParseTree*>>;

execplan::Filter* castToFilter(execplan::ParseTree* node)
{
  return dynamic_cast<execplan::Filter*>(node->data());
}

SimpleFilter* castToSimpleFilter(execplan::TreeNode * node)
{
  return dynamic_cast<SimpleFilter*>(node);
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

enum class ChildType
{
  Unchain,
  Delete,
  Leave
};

bool isSimpleFilter(execplan::ParseTree* node)
{
   return !!dynamic_cast<execplan::SimpleFilter*>(node->data());
}

void printTreeLevel(execplan::ParseTree* root, int level)
{
#ifdef debug_rewrites
  auto sep = std::string(level * 4, '-');
  auto& node = *(root->data());
  std::cerr << sep << ": " << root->data()->data() << " "
            << boost::core::demangle(typeid(node).name()) << " " << root << std::endl;
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
                          std::inserter(intersection.first, intersection.first.begin()), NodeSemanticComparator{});

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

execplan::ParseTree* appendToRoot(execplan::ParseTree* tree, const CommonContainer& common_)
{
  if (common_.first.empty())
    return tree;

  // TODO: refactor to debug
  std::vector<execplan::ParseTree*> common;
  std::copy(common_.first.begin(), common_.first.end(), std::back_inserter(common));
  std::sort(common.begin(), common.end(), [](auto left, auto right) { return left->data()->data() < right->data()->data(); } );

  execplan::ParseTree* result = newAndNode();
  auto current = result;
  for (auto treenode = common.begin(); treenode != common.end();)
  {
    execplan::ParseTree* andCondition = *treenode;

    ++treenode;
    current->right(andCondition);

    if (treenode != common.end() && std::next(treenode) != common.end())
    {
      execplan::ParseTree* andOp = newAndNode();
      current->left(andOp);
      current = andOp;
    }
    else if (std::next(treenode) == common.end() && tree != nullptr)
    {
      execplan::ParseTree* andOp = newAndNode();
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

struct StackFrame
{
  execplan::ParseTree** node;
  GoTo direction;
  ChildType containsLeft;
  ChildType containsRight;
  StackFrame(execplan::ParseTree** node_, GoTo direction_)
   : node(node_)
   , direction(direction_)
   , containsLeft(ChildType::Leave)
   , containsRight(ChildType::Leave)
  {
  }
};

using DFSStack = std::vector<StackFrame>;

void deleteOneNode(execplan::ParseTree** node)
{
  if (!node || !*node)
    return;

  (*node)->nullLeft();
  (*node)->nullRight();

#ifdef debug_rewrites
  std::cerr << "   Deleting: " <<  (*node)->data() << " " << boost::core::demangle(typeid(**node).name()) <<
            " " << "ptr: " << *node << std::endl;
#endif

  delete *node;
  *node = nullptr;
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

void replaceContainsTypeFlag(StackFrame& stackframe, ChildType containsflag)
{
  if (stackframe.direction == GoTo::Right)
    stackframe.containsLeft = containsflag;
  else
    stackframe.containsRight = containsflag;
}

void removeFromTreeIterative(execplan::ParseTree** root, const CommonContainer& common)
{
  DFSStack stack;
  stack.emplace_back(root, GoTo::Left);
  while (!stack.empty())
  {
    auto [node, flag, ltype, rtype] = stack.back();
    if (flag != GoTo::Up)
    {
      addStackFrame(stack, flag, *node);
      continue;
    }

    auto sz = stack.size();
    if (castToFilter(*node) && stack.size() > 1)
    {
      if (commonContainsPtr(common, *node))
        replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Unchain);
      else if (!commonContainsPtr(common, *node) && commonContainsSemantic(common, *node))
        replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Delete);
      else
        replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Leave);

      stack.pop_back();
      continue;
    }

    if (sz == 1)
    {
      if (ltype == ChildType::Leave)
      {
        if (rtype == ChildType::Unchain)
        {
          execplan::ParseTree* oldNode = *node;
          *node = (*node)->left();
          deleteOneNode(&oldNode);
        }
        else if (rtype == ChildType::Delete)
        {
          execplan::ParseTree* oldNode = *node;
          deleteOneNode((*node)->rightRef());
          *node = (*node)->left();
          deleteOneNode(&oldNode);
        }
      }
      else if (ltype == ChildType::Unchain)
      {
        if (rtype == ChildType::Leave)
        {
          execplan::ParseTree* oldNode = *node;
          *node = (*node)->right();
          deleteOneNode(&oldNode);
        }
        else if (rtype == ChildType::Unchain)
        {
          deleteOneNode(node);
        }
        else if (rtype == ChildType::Delete)
        {
          deleteOneNode((*node)->rightRef());
          deleteOneNode(node);
        }
      }
      else if (ltype == ChildType::Delete)
      {
        deleteOneNode((*node)->leftRef());
        if (rtype == ChildType::Leave)
        {
          execplan::ParseTree* oldNode = *node;
          *node = (*node)->right();
          deleteOneNode(&oldNode);
        }
        else if (rtype == ChildType::Unchain)
        {
          deleteOneNode(node);
        }
        else if (rtype == ChildType::Delete)
        {
          deleteOneNode((*node)->rightRef());
          deleteOneNode(node);
        }
      }
    }
    else
    {
      if (ltype == ChildType::Leave)
      {
        replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Leave);
        if (rtype == ChildType::Unchain)
        {
          execplan::ParseTree* oldNode = *node;
          *node = (*node)->left();
          deleteOneNode(&oldNode);
        }
        else if (rtype == ChildType::Delete)
        {
          execplan::ParseTree* oldNode = *node;
          deleteOneNode((*node)->rightRef());
          *node = (*node)->left();
          deleteOneNode(&oldNode);
        }
      }
      else if (ltype == ChildType::Unchain)
      {
        if (rtype == ChildType::Leave)
        {
          execplan::ParseTree* oldNode = *node;
          *node = (*node)->right();
          deleteOneNode(&oldNode);
          replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Leave);
        }
        else if (rtype == ChildType::Unchain)
        {
          replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Delete);
        }
        else if (rtype == ChildType::Delete)
        {
          deleteOneNode((*node)->rightRef());
          replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Delete);
        }
      }
      else if (ltype == ChildType::Delete)
      {
        deleteOneNode((*node)->leftRef());
        if (rtype == ChildType::Leave)
        {
          execplan::ParseTree* oldNode = *node;
          *node = (*node)->right();
          deleteOneNode(&oldNode);
          replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Leave);
        }
        else if (rtype == ChildType::Unchain)
        {
          replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Delete);
        }
        else if (rtype == ChildType::Delete)
        {
          deleteOneNode((*node)->rightRef());
          replaceContainsTypeFlag(stack.at(sz - 2), ChildType::Delete);
        }
      }
    }

    stack.pop_back();
  }
}

}  // namespace details

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


std::set<execplan::ParseTree*> collectAllNodes(execplan::ParseTree* tree, std::set<execplan::ParseTree*> result = std::set<execplan::ParseTree*> {})
{

  result.insert(tree);
  if (tree->left())
  {
    collectAllNodes(tree->left(), result);
  }
  if (tree->right())
  {
    collectAllNodes(tree->right(), result);
  }
  return result;
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
  details::printContainer(
      std::cerr, common.first, "\n", [](auto treenode) { return treenode->data()->data(); }, "Common Leaf Conjunctions:");
#endif

  details::removeFromTreeIterative(&tree, common);

  auto result = details::appendToRoot(tree, common);

  dumpTreeFiles(result, "3.final");
  return result;
}

bool NodeSemanticComparator::operator()(std::unique_ptr<execplan::ParseTree> const& left,
                                        std::unique_ptr<execplan::ParseTree> const& right) const
{
  return this->operator()(left.get(), right.get());
}

bool NodeSemanticComparator::operator()(execplan::ParseTree* left, execplan::ParseTree* right) const
{
  auto filterLeft = details::castToSimpleFilter(left->data());
  auto filterRight = details::castToSimpleFilter(right->data());

  if (filterLeft && filterRight)
    return *filterLeft < *filterRight;


  return left->data()->data() < right->data()->data();
}

}  // namespace execplan

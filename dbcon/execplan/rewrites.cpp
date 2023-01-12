#include "rewrites.h"

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

auto nodeComparator = [](const execplan::TreeNode* left, const execplan::TreeNode* right)
{ return left->data() < right->data(); };

using Comparator = decltype(nodeComparator);
using CommonContainer = std::set<execplan::TreeNode*, Comparator>;

// Walk the tree and find out common conjuctions
void collectCommonConjuctions(execplan::ParseTree* root, CommonContainer& accumulator, int level = 0)
{
  if (root == nullptr)
  {
    accumulator.clear();
    return;
  }
  auto sep = std::string(level * 4, '-');
  std::cerr << sep << ": " << root->data()->data() << std::endl;

  if (root->left() == nullptr && root->left() == nullptr)
  {
    accumulator.insert(root->data());
    return;
  }

  if (root->data()->data() == "or")
  {
    CommonContainer leftAcc;
    CommonContainer rightAcc;
    collectCommonConjuctions(root->left(), leftAcc, ++level);
    collectCommonConjuctions(root->right(), rightAcc, ++level);
    CommonContainer intersection;
    std::set_intersection(leftAcc.begin(), leftAcc.end(), rightAcc.begin(), rightAcc.end(),
                          std::inserter(intersection, intersection.begin()), nodeComparator);

    accumulator = intersection;
    return;
  }

  if (root->data()->data() == "and")
  {
    collectCommonConjuctions(root->left(), accumulator, ++level);
    collectCommonConjuctions(root->right(), accumulator, ++level);
    return;
  }

  if (root->left() == nullptr)
  {
    collectCommonConjuctions(root->right(), accumulator, ++level);
    return;
  }

  collectCommonConjuctions(root->left(), accumulator, ++level);
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

// TODO: investigate when AND has two comon nodes !!!!
void removeFromAnd(execplan::ParseTree* father, execplan::ParseTree* andNode, const CommonContainer& common,
                   bool rightChild)
{
  if (!andNode || andNode->data()->data() != "and")
    return;

  if (common.contains(andNode->left()->data()))
  {
    if (rightChild)
      father->right(andNode->right());
    else
      father->left(andNode->right());
  }
  else if (common.contains(andNode->right()->data()))
  {
    if (rightChild)
      father->right(andNode->left());
    else
      father->left(andNode->left());
  }
}

void removeFromTree(execplan::ParseTree* tree, const CommonContainer& common)
{
  if (tree == nullptr)
  {
    return;
  }

  removeFromAnd(tree, tree->left(), common, false);
  removeFromAnd(tree, tree->right(), common, true);

  removeFromTree(tree->left(), common);
  removeFromTree(tree->right(), common);
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

execplan::ParseTree* extractCommonLeafConjunctionsToRoot(execplan::ParseTree* tree)
{
  details::CommonContainer common;
  details::collectCommonConjuctions(tree, common);

  printContainer(
      std::cerr, common, "\n", [](auto treenode) { return treenode->data(); }, "Common Leaf Conjunctions:");

  details::removeFromTree(tree, common);
  // HACK WORKAROUND FOR case of two common nodes
  details::removeFromTree(tree, common);
  return details::appendToRoot(tree, common);
}

}  // namespace execplan

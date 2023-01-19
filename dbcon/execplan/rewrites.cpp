#include "rewrites.h"
#include <typeinfo>
#include "objectreader.h"


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

auto nodeComparator = [](const execplan::TreeNode* left, const execplan::TreeNode* right)
{ return left->data() < right->data(); };

using Comparator = decltype(nodeComparator);
using CommonContainer = std::set<execplan::TreeNode*, Comparator>;

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
    std::cerr << sep << ": " << root->data()->data() << " " << typeid(root->data()).name() << std::endl;
  #endif

  if (root->left() == nullptr && root->left() == nullptr && orMeeted && andParent)
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

  details::removeFromTree(tree, common);
  dumpTreeFiles(tree, "1.remove");
  // HACK WORKAROUND FOR case of two common nodes
  details::removeFromTree(tree, common);
  dumpTreeFiles(tree, "2.remove");

  auto result = details::appendToRoot(tree, common);

  dumpTreeFiles(result, "3.final");
  return result;
}

}  // namespace execplan

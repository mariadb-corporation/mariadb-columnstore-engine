#include <queue>
#include <utility>

#include "compileoperator.h"
#include "returnedcolumn.h"
#include "arithmeticcolumn.h"

namespace msc_jit
{
static JIT& getJITInstance()
{
  static JIT jit;
  return jit;
}

class CompiledColumn : public execplan::ReturnedColumn
{
 private:
  CompiledOperatorINT64 compiledOperatorInt64;

 public:
  CompiledColumn()
  {
  }
  CompiledColumn(CompiledOperatorINT64 compiledOperatorInt64, ReturnedColumn* expression)
   : ReturnedColumn(*expression), compiledOperatorInt64(std::move(compiledOperatorInt64))
  {
  }

  inline CompiledColumn* clone() const override
  {
    return new CompiledColumn(*this);
  }
  bool hasWindowFunc() override
  {
    return false;
  }

 public:
  virtual int64_t getIntVal(rowgroup::Row& row, bool& isNull)
  {
    auto columnList = this->simpleColumnList();
    return 1;
  }
};
static void compileExpression(const execplan::SRCP& expression, rowgroup::Row& row, bool& isNull)
{
  boost::shared_ptr<ArithmeticColumn> alithmeticcolumn =
      boost::dynamic_pointer_cast<ArithmeticColumn>(expression);
  if (alithmeticcolumn)
  {
    ParseTree* root = alithmeticcolumn->expression();
    std::queue<ParseTree*> nodeQueue;
    nodeQueue.emplace(root);
    while (!nodeQueue.empty())
    {
      ParseTree* node = nodeQueue.front();
      nodeQueue.pop();
      if (!node->isCompilable(row))
      {
        if (node->left() && node->right())
        {
          nodeQueue.emplace(node->left());
          nodeQueue.emplace(node->right());
        }
      }
      else
      {
        auto* c_ptr = dynamic_cast<ReturnedColumn*>(node->data());
        execplan::SRCP ptr = boost::shared_ptr<ReturnedColumn>(c_ptr);
        CompiledOperatorINT64 compiledOperatorInt64 = compileOperator(getJITInstance(), ptr, row, isNull);
        auto* compiledColumn = new CompiledColumn(compiledOperatorInt64, c_ptr);
        node->data(compiledColumn);
      }
    }
  }
}
}  // namespace msc_jit
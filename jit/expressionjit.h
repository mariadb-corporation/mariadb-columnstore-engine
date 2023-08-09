#include <queue>
#include <utility>

#include "compileoperator.h"
#include "returnedcolumn.h"
#include "arithmeticcolumn.h"

using namespace execplan;

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
  CompiledOperator compiledOperator;

 public:
  CompiledColumn()
  {
  }
  CompiledColumn(CompiledOperator compiledOperator, ReturnedColumn* expression)
   : ReturnedColumn(*expression), compiledOperator(std::move(compiledOperator))
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
  int64_t getIntVal(rowgroup::Row& row, bool& isNull) override
  {
    return compiledOperator.compiled_function_int64(row.getData(), isNull);
  }
  uint64_t getUintVal(rowgroup::Row& row, bool& isNull) override
  {
    return compiledOperator.compiled_function_int64(row.getData(), isNull);
  }
  double getDoubleVal(rowgroup::Row& row, bool& isNull) override
  {
    return compiledOperator.compiled_function_double(row.getData(), isNull);
  }
  float getFloatVal(rowgroup::Row& row, bool& isNull) override
  {
    return compiledOperator.compiled_function_float(row.getData(), isNull);
  }
};
// TODO: Optimize code structure
static void compileExpression(std::vector<execplan::SRCP>& expression, rowgroup::Row& row)
{
  for (int i = expression.size() - 1; i >= 0; --i)
  {
    boost::shared_ptr<ArithmeticColumn> arithmeticcolumn =
        boost::dynamic_pointer_cast<ArithmeticColumn>(expression[i]);
    if (arithmeticcolumn)
    {
      if (arithmeticcolumn->isCompilable(row))
      {
        CompiledOperator compiledOperator = compileOperator(getJITInstance(), expression[i], row);
        expression[i] = boost::make_shared<CompiledColumn>(compiledOperator, expression[i].get());
      }
      //      else
      //      {
      //        // TODO: The child nodes have no aliases, further modifications are needed.
      //        ParseTree* root = arithmeticcolumn->expression();
      //        std::queue<ParseTree*> nodeQueue;
      //        nodeQueue.emplace(root);
      //        while (!nodeQueue.empty())
      //        {
      //          ParseTree* node = nodeQueue.front();
      //          nodeQueue.pop();
      //          if (!node->isCompilable(row))
      //          {
      //            if (node->left() && node->right())
      //            {
      //              nodeQueue.emplace(node->left());
      //              nodeQueue.emplace(node->right());
      //            }
      //          }
      //          else
      //          {
      //            auto* c_ptr = dynamic_cast<ReturnedColumn*>(node->data());
      //            execplan::SRCP ptr = boost::shared_ptr<ReturnedColumn>(c_ptr);
      //            CompiledOperatorINT64 compiledOperatorInt64 = compileOperator(getJITInstance(), ptr, row,
      //            isNull); auto* compiledColumn = new CompiledColumn(compiledOperatorInt64, c_ptr);
      //            node->data(compiledColumn);
      //          }
      //        }
      //      }
    }
    else
    {
      if (expression[i]->isCompilable(row))
      {
        CompiledOperator compiledOperator = compileOperator(getJITInstance(), expression[i], row);
        expression[i] = boost::make_shared<CompiledColumn>(compiledOperator, expression[i].get());
      }
    }
  }
}
}  // namespace msc_jit
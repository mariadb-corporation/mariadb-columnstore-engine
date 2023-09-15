#include <queue>
#include <utility>

#include "compileoperator.h"
#include "returnedcolumn.h"
#include "arithmeticcolumn.h"
#include "dataconvert.h"

using namespace execplan;

namespace msc_jit
{

static bool initialized = false;

static void init(JIT& jit)
{
  jit.registerExternalSymbol("dataconvert::DataConvert::intToDatetime",
                             reinterpret_cast<void*>(&dataconvert::DataConvert::intToDatetime));
  jit.registerExternalSymbol("dataconvert::DataConvert::timestampValueToInt",
                             reinterpret_cast<void*>(&dataconvert::DataConvert::timestampValueToInt));
  initialized = true;
}

static JIT& getJITInstance()
{
  static JIT jit;
  if (!initialized)
  {
    init(jit);
  }
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
// TODO: Optimize code structure, maybe it should be implemented using recursion.
static void compileExpression(std::vector<execplan::SRCP>& expression, rowgroup::Row& row)
{
  for (int i = expression.size() - 1; i >= 0; --i)
  {
    if (expression[i]->isCompilable(row))
    {
      CompiledOperator compiledOperator = compileOperator(getJITInstance(), expression[i], row);
      expression[i] = boost::make_shared<CompiledColumn>(compiledOperator, expression[i].get());
    }
    else
    {
      ParseTree* root = nullptr;
      if (boost::dynamic_pointer_cast<ArithmeticColumn>(expression[i]) != nullptr)
      {
        boost::shared_ptr<ArithmeticColumn> arithmeticcolumn =
            boost::dynamic_pointer_cast<ArithmeticColumn>(expression[i]);
        root = arithmeticcolumn->expression();
      }
      else
      {
        // TODO: other type
      }
      if (root)
      {
        std::queue<ParseTree*> nodeQueue;
        nodeQueue.emplace(root);
        size_t identifier = 0;
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
            ptr->alias(expression[i]->alias() + to_string(++identifier));
            CompiledOperator compiledOperator = compileOperator(getJITInstance(), ptr, row);
            auto* compiledColumn = new CompiledColumn(compiledOperator, c_ptr);
            node->data(compiledColumn);
          }
        }
      }
    }
  }
}
}  // namespace msc_jit
#include <queue>
#include <utility>

// WIP reorder
#include "compileoperator.h"
#include "mcs_data_condition.h"
#include "returnedcolumn.h"
#include "mcs_outofrange.h"
#include "dataconvert.h"
#include "rowgroup.h"

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
  // Unfortunately there are compiled and enterpreted parts of getUintVal().
  // The former calculates and the latter checks if the value is out of range
  // b/c exceptions in JIT is hard to handle.
  uint64_t getUintVal(rowgroup::Row& row, bool& isNull) override
  {
    datatypes::DataCondition::ErrorType error;
    auto result = compiledOperator.compiled_function_uint64(row.getData(), isNull, error);
    datatypes::throwOutOfRangeExceptionIfNeeded(isNull, datatypes::DataCondition::isOutOfRange(error));
    return result;
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
static void compileExpressions(std::vector<execplan::SRCP>& expressions, rowgroup::Row& row)
{
  for (auto it = expressions.rbegin(); it != expressions.rend(); ++it)
  {
    auto& expression = *it;
    if (expression->isCompilable(row))
    {
      // auto a = expression->operationType();
      // The next call must share a module to try to find the same function in the module.
      CompiledOperator compiledOperator = compileOperator(getJITInstance(), expression, row);
      expression = boost::make_shared<CompiledColumn>(compiledOperator, expression.get());
    }
    // WIP
    // The else block below tries to compile a sub-tree and replace it in the expression.
    // It is problematic and causes double free in ParseTree dtor.
    // else
    // {
    //   [[maybe_unused]] ParseTree* root = nullptr;
    //   if (boost::dynamic_pointer_cast<ArithmeticColumn>(expression) != nullptr)
    //   {
    //     boost::shared_ptr<ArithmeticColumn> arithmeticcolumn =
    //         boost::dynamic_pointer_cast<ArithmeticColumn>(expression);
    //     root = arithmeticcolumn->expression();
    //   }
    //   else
    //   {
    //     // TODO: other type
    //   }

    //   if (root)
    //   {
    //     std::queue<ParseTree*> nodeQueue;
    //     nodeQueue.emplace(root);
    //     size_t identifier = 0;
    //     while (!nodeQueue.empty())
    //     {
    //       ParseTree* node = nodeQueue.front();
    //       nodeQueue.pop();
    //       std::cout << "compileExpressions node " << std::hex << (uint64_t)(node) << std::dec << std::endl;
    //       if (!node->isCompilable(row))
    //       {
    //         if (node->left() && node->right())
    //         {
    //           nodeQueue.emplace(node->left());
    //           nodeQueue.emplace(node->right());
    //         }
    //       }
    //       else
    //       {
    //         [[maybe_unused]] auto* c_ptr = dynamic_cast<ReturnedColumn*>(node->data());
    //         execplan::SRCP ptr = boost::shared_ptr<ReturnedColumn>(c_ptr);
    //         ptr->alias(expression.alias() + to_string(++identifier));
    //         CompiledOperator compiledOperator = compileOperator(getJITInstance(), ptr, row);
    //         auto* compiledColumn = new CompiledColumn(compiledOperator);
    //         node->data(compiledColumn);
    //       }
    //     }
    //   }
    // }
  }
}
}  // namespace msc_jit
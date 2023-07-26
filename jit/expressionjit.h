#include "compileoperator.h"
#include "returnedcolumn.h"

namespace msc_jit
{
static JIT& getJITInstance()
{
  static JIT jit;
  return jit;
}

class CompiledColumn : execplan::ReturnedColumn
{
};
static CompiledOperatorINT64 compileExpression(const execplan::SRCP& expression, rowgroup::Row& row, bool& isNull)
{
  return compileOperator(getJITInstance(), expression, row, isNull);
}
}  // namespace msc_jit
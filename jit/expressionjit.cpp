#include "parsetree.h"
#include "jit.h"
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
void compileExpression(execplan::ParseTree* root)
{

}
}  // namespace msc_jit
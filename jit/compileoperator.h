#ifndef MARIADB_COMPILEOPERATOR_H
#define MARIADB_COMPILEOPERATOR_H
#include "jit.h"
#include "returnedcolumn.h"
#include "simplecolumn.h"

namespace execplan
{
using JITCompiledOperatorINT64 = int64_t (*)(uint8_t* data, bool& isNull);
struct CompiledOperatorINT64
{
  msc_jit::JIT::CompiledModule compiled_module;
  JITCompiledOperatorINT64 compiled_function;
};

CompiledOperatorINT64 compileOperator(msc_jit::JIT& jit, const execplan::SRCP& expression, rowgroup::Row& row,
                                 bool& isNull);

}
#endif  // MARIADB_COMPILEOPERATOR_H

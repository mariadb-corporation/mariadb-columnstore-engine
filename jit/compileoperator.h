#ifndef MARIADB_COMPILEOPERATOR_H
#define MARIADB_COMPILEOPERATOR_H
#include "jit.h"
#include "returnedcolumn.h"
#include "simplecolumn.h"

namespace execplan
{
template <typename t>
using JITCompiledOperator = t (*)(uint8_t* data, bool& isNull);

struct CompiledOperator
{
  msc_jit::JIT::CompiledModule compiled_module;
  JITCompiledOperator<int64_t> compiled_function_int64;
  JITCompiledOperator<uint64_t> compiled_function_uint64;
  JITCompiledOperator<double> compiled_function_double;
  JITCompiledOperator<float> compiled_function_float;
};

CompiledOperator compileOperator(msc_jit::JIT& jit, const execplan::SRCP& expression, rowgroup::Row& row,
                                 bool& isNull);

}
#endif  // MARIADB_COMPILEOPERATOR_H

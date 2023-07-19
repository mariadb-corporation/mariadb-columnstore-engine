#ifndef MARIADB_COMPILEOPERATOR_H
#define MARIADB_COMPILEOPERATOR_H
#include "jit.h"
#include "returnedcolumn.h"
#include "simplecolumn.h"

namespace execplan
{
struct CompiledOperator
{
  msc_jit::JIT::CompiledModule compiled_module;
};

CompiledOperator compileOperator(msc_jit::JIT& jit, const execplan::SRCP expression, rowgroup::Row& row,
                                 bool& isNull);

}
#endif  // MARIADB_COMPILEOPERATOR_H

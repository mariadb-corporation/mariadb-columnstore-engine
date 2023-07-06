#ifndef MARIADB_COMPILEFUNCTION_H
#define MARIADB_COMPILEFUNCTION_H

#include "jit.h"
#include "../utils/funcexp/functor.h"
namespace funcexp
{
struct CompiledFunction
{
  msc_jit::JIT::CompiledModule compiled_module;
};

CompiledFunction compileFunction(msc_jit::JIT& jit, const Func& function);

}  // namespace funcexp
#endif  // MARIADB_COMPILEFUNCTION_H

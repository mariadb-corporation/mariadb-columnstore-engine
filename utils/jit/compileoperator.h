#include "jit.h"
#include "returnedcolumn.h"
#include "simplecolumn.h"

#pragma once

namespace execplan
{
template <typename T>
using JITIntConditionedCompiledOperator = T (*)(uint8_t* data, bool& isNull, uint32_t& dataCondition);

template <typename T>
using JITCompiledOperator = T (*)(uint8_t* data, bool& isNull);

struct CompiledOperator
{
  CompiledOperator(mcs_jit::JIT& jit, const execplan::SRCP& expression, rowgroup::Row& row);

  mcs_jit::CompiledModule compiledModule_;
  JITCompiledOperator<int64_t> int64CompiledFunc = nullptr;
  JITIntConditionedCompiledOperator<uint64_t> uint64CompiledFunction = nullptr;
  JITCompiledOperator<double> doubleCompiledFunction = nullptr;
  JITCompiledOperator<float> floatCompiledFunction = nullptr;
};

// CompiledOperator compileOperator(mcs_jit::JIT& jit, const execplan::SRCP& expression, rowgroup::Row& row);

}  // namespace execplan

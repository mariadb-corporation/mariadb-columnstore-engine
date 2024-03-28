#include "jit.h"
#include "returnedcolumn.h"
#include "simplecolumn.h"

namespace execplan
{
template <typename T>
using JITIntConditionedCompiledOperator = T (*)(uint8_t* data, bool& isNull, uint32_t& dataCondition);

template <typename T>
using JITCompiledOperator = T (*)(uint8_t* data, bool& isNull);

struct CompiledOperator
{
  msc_jit::JIT::CompiledModule compiled_module;
  JITCompiledOperator<int64_t> compiled_function_int64 = nullptr;
  JITIntConditionedCompiledOperator<uint64_t> compiled_function_uint64 = nullptr;
  JITCompiledOperator<double> compiled_function_double = nullptr;
  JITCompiledOperator<float> compiled_function_float = nullptr;
};

class CompiledOperatorCache
{
 public:
  std::unordered_map<std::string, msc_jit::JIT::CompiledModule> cache;

  static CompiledOperatorCache& getInstance()
  {
    static CompiledOperatorCache instance;
    return instance;
  }

 private:
  CompiledOperatorCache() = default;
  CompiledOperatorCache(const CompiledOperatorCache&) = delete;
  CompiledOperatorCache& operator=(const CompiledOperatorCache&) = delete;
};

CompiledOperator compileOperator(msc_jit::JIT& jit, const execplan::SRCP& expression, rowgroup::Row& row);

}  // namespace execplan

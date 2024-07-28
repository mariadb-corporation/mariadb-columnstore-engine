#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#pragma once

namespace mcs_jit
{
struct CompiledModule
{
 public:
  size_t size;
  uint64_t identifier;
  std::unordered_map<std::string, void*> function_name_to_symbol;

  std::vector<std::string> getExpressionStringList() const
  {
    std::vector<std::string> functions;
    for (const auto& [name, _] : function_name_to_symbol)
    {
      functions.push_back(name);
    }
    return functions;
  }

  void* getFunctionSymbol(const std::string& function_name) const
  {
    return function_name_to_symbol.at(function_name);
  }
};

using CompiledModuleOpt = std::optional<CompiledModule>;
using ModulesVector = std::vector<CompiledModule>;

}  // namespace mcs_jit

#include <mutex>

#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Target/TargetMachine.h>

#include "modules_storage.h"

namespace mcs_jit
{
// using ModuleUPtr = std::unique_ptr<llvm::Module>;
// using ModuleSPtr = std::shared_ptr<llvm::Module>;
// using ModulesVector = std::vector<ModuleSPtr>;
// using ExprToModulesVectorId = std::unordered_map<std::string, std::size_t>;

class ModuleMemoryManager;
using ModuleIdToMemManager = std::unordered_map<uint64_t, std::unique_ptr<ModuleMemoryManager>>;
class JITSymbolResolver;
class JITCompiler;

class JIT
{
 public:
  JIT();
  ~JIT();

  mcs_jit::CompiledModule compileModule(std::function<void(llvm::Module&)> compile_function);
  mcs_jit::CompiledModule compileAndCacheModule(std::function<void(llvm::Module&)> compile_function);
  mcs_jit::CompiledModule findOrCompileModule(const std::string& expressionString,
                                              std::function<void(llvm::Module&)> compile_function);
  // void deleteCompiledModule(CompiledModule& module);
  void registerExternalSymbol(const std::string& symbol_name, void* address);
  // std::optional<std::shared_ptr<Module>> moduleByExpressionName(const std::string& expressionName) const;

 private:
  std::unique_ptr<llvm::Module> createModuleForCompilation();
  CompiledModule compileModule(std::unique_ptr<llvm::Module> module);
  // std::optional<size_t> moduleIdByExpressionName(const std::string expressionName) const;

  static std::unique_ptr<llvm::TargetMachine> getTargetMachine();
  std::string getMangledName(const std::string& name_to_mangle) const;
  void runOptimizationPassesOnModule(llvm::Module& module) const;
  llvm::LLVMContext context;
  std::unique_ptr<llvm::TargetMachine> target_machine;
  llvm::DataLayout data_layout;
  std::unique_ptr<JITCompiler> compiler;
  std::unique_ptr<JITSymbolResolver> symbol_resolver;
  size_t current_module_key = 0;
  // std::atomic<size_t> compiled_code_size;

  // ModuleIdToMemManager moduleIdToMemManager_;
  CompiledModuleStorage moduleStorage_;

  mutable std::mutex jit_lock;
};
}  // namespace mcs_jit

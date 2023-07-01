#include <unordered_map>
#include <atomic>
#include <mutex>

#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Target/TargetMachine.h>

class JITModuleMemoryManager;
class JITSymbolResolver;
class JITCompiler;
class JIT
{
 public:
  JIT();
  ~JIT();
  struct CompiledModule
  {
    size_t size;
    uint64_t identifier;
    std::unordered_map<std::string, void*> function_name_to_symbol;
  };
  CompiledModule compiledModule(std::function<void(llvm::Module&)> compile_function);
  void deleteCompiledModule(CompiledModule& module);
  void registerExternalSymbol(const std::string& symbol_name, void* address);

 private:
  std::unique_ptr<llvm::Module> createModuleForCompilation();
  CompiledModule compiledModule(std::unique_ptr<llvm::Module> module);
  static std::unique_ptr<llvm::TargetMachine> getTargetMachine();
  std::string getMangledName(const std::string& name_to_mangle) const;
  void runOptimizationPassesOnModule(llvm::Module & module) const;
  llvm::LLVMContext context;
  std::unique_ptr<llvm::TargetMachine> target_machine;
  llvm::DataLayout data_layout;
  std::unique_ptr<JITCompiler> compiler;
  std::unique_ptr<JITSymbolResolver> symbol_resolver;
  std::unordered_map<uint64_t, std::unique_ptr<JITModuleMemoryManager>> module_identifier_to_memory_manager;
  uint64_t current_module_key = 0;
  std::atomic<size_t> compiled_code_size;
  // TODO lock when compiling a module
  mutable std::mutex jit_lock;
};

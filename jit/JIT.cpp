#include "JIT.h"
#include <llvm/Analysis/TargetTransformInfo.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Mangler.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/ExecutionEngine/JITEventListener.h>
#include <llvm/MC/SubtargetFeature.h>
#include <llvm/MC/TargetRegistry.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/Support/SmallVectorMemoryBuffer.h>

class JITCompiler
{
 public:
  explicit JITCompiler(llvm::TargetMachine& target_machine_) : target_machine(target_machine_)
  {
  }

 private:
  llvm::TargetMachine& target_machine;
};

class JITSymbolResolver;

// JIT::JIT()
//  : target_machine(getTargetMachine())
//  , data_layout(target_machine->createDataLayout())
//  , compiler(std::make_unique<JITCompiler>(*target_machine))
//  , symbol_resolver(std::make_unique<JITSymbolResolver>())
//{
//   /// Define common symbols that can be generated during compilation
//   /// Necessary for valid linker symbol resolution
//   symbol_resolver->registerSymbol("memset", reinterpret_cast<void*>(&memset));
//   symbol_resolver->registerSymbol("memcpy", reinterpret_cast<void*>(&memcpy));
//   symbol_resolver->registerSymbol("memcmp", reinterpret_cast<void*>(&memcmp));
// }

//CompiledModule JIT::compiledModule(std::function<void(llvm::Module&)> compile_function)
//{
//  std::unique_ptr<llvm::Module> module = nullptr;
//  return nullptr;
//}

std::unique_ptr<llvm::TargetMachine> JIT::getTargetMachine()
{
  static std::once_flag llvm_target_machine_initialized;
  std::call_once(llvm_target_machine_initialized,
                 []()
                 {
                   llvm::InitializeNativeTarget();
                   llvm::InitializeNativeTargetAsmPrinter();
                   llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
                 });
  std::string error;
  auto cpu = llvm::sys::getHostCPUName();
  auto triple = llvm::sys::getProcessTriple();
  const auto* target = llvm::TargetRegistry::lookupTarget(triple, error);
  if (!target)
  {
    throw std::runtime_error("Failed to lookup target: " + error);
  }
  llvm::SubtargetFeatures features;
  llvm::StringMap<bool> feature_map;
  if (llvm::sys::getHostCPUFeatures(feature_map))
    for (auto& f : feature_map)
      features.AddFeature(f.first(), f.second);

  llvm::TargetOptions options;

  bool jit = true;
  auto* target_machine = target->createTargetMachine(triple, cpu, features.getString(), options, llvm::None,
                                                     llvm::None, llvm::CodeGenOpt::Aggressive, jit);

  if (!target_machine)
    throw std::runtime_error("Failed to create target machine");

  return std::unique_ptr<llvm::TargetMachine>(target_machine);
}
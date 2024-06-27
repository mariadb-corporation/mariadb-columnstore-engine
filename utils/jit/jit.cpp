#include <iostream>

#include "jit.h"

#include <sys/mman.h>
#include <boost/noncopyable.hpp>
#include <optional>
#include "llvm/Config/llvm-config.h"
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

#if LLVM_VERSION_MAJOR <= 16
#include <llvm/MC/SubtargetFeature.h>
#else
#include <llvm/TargetParser/SubtargetFeature.h>
#endif

#if LLVM_VERSION_MAJOR < 14
#include <llvm/Support/TargetRegistry.h>
#else
#include <llvm/MC/TargetRegistry.h>
#endif

#include "llvm/Config/llvm-config.h"

#if LLVM_VERSION_MAJOR < 14
#include <llvm/Support/TargetRegistry.h>
#else
#include <llvm/MC/TargetRegistry.h>
#endif

#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/TargetSelect.h>

#if LLVM_VERSION_MAJOR < 17
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/ADT/None.h>
#else
#include "llvm/Passes/PassBuilder.h"
#include "llvm/IR/PassManager.h"
#include "llvm/Pass.h"
#endif

#include <llvm/Support/SmallVectorMemoryBuffer.h>
#include <llvm/Passes/PassBuilder.h>

#include <llvm/ExecutionEngine/RTDyldMemoryManager.h>
#include <llvm/ExecutionEngine/MCJIT.h>

namespace mcs_jit
{
class JITCompiler
{
 public:
  explicit JITCompiler(llvm::TargetMachine& target_machine_) : target_machine(target_machine_)
  {
  }

  /**
   * Compile the module into machine code
   * */
  std::unique_ptr<llvm::MemoryBuffer> compile(llvm::Module& module)
  {
    auto materialize_err = module.materializeAll();
    if (materialize_err)
    {
      std::string error_message;
      handleAllErrors(std::move(materialize_err),
                      [&](const llvm::ErrorInfoBase& error_info) { error_message = error_info.message(); });
      // TODO: throw a more specific exception
      throw std::logic_error("Failed to materialize module: " + error_message);
    }
    // WIP constant size?
    llvm::SmallVector<char, 4096> buffer;
    llvm::raw_svector_ostream ostream(buffer);
    llvm::legacy::PassManager pass_manager;
    llvm::MCContext* mc_context = nullptr;
    if (target_machine.addPassesToEmitMC(pass_manager, mc_context, ostream))
    {
      // TODO: throw a more specific exception
      throw std::logic_error("Failed to create MC pass manager");
    }

    // std::cout << "compile " << std::endl;
    // module.print(llvm::outs(), nullptr);
    // std::cout.flush();

    pass_manager.run(module);

    std::unique_ptr<llvm::MemoryBuffer> compiled_memory_buffer =
        std::make_unique<llvm::SmallVectorMemoryBuffer>(
            std::move(buffer), "<in memory object compiled from " + module.getModuleIdentifier() + ">");

    return compiled_memory_buffer;
  }

  ~JITCompiler() = default;

 private:
  llvm::TargetMachine& target_machine;
};

class JITSymbolResolver : public llvm::LegacyJITSymbolResolver
{
 public:
  void registerSymbol(const std::string& symbol_name, void* symbol)
  {
    symbol_name_to_symbol_address[symbol_name] = symbol;
  }

  llvm::JITSymbol findSymbol(const std::string& symbol_name) override
  {
    auto address_it = symbol_name_to_symbol_address.find(symbol_name);
    if (address_it == symbol_name_to_symbol_address.end())
    {
      // TODO: throw a more specific exception
      throw std::logic_error("Symbol " + symbol_name + " not found");
    }

    auto symbol_address = reinterpret_cast<uint64_t>(address_it->second);
    auto symbol = llvm::JITSymbol(symbol_address, llvm::JITSymbolFlags::None);
    return symbol;
  }

  llvm::JITSymbol findSymbolInLogicalDylib(const std::string& symbol_name) override
  {
    return nullptr;
  }

 private:
  std::unordered_map<std::string, void*> symbol_name_to_symbol_address;
};

JIT::JIT()
 : target_machine(getTargetMachine())
 , data_layout(target_machine->createDataLayout())
 , compiler(std::make_unique<JITCompiler>(*target_machine))
 , symbol_resolver(std::make_unique<JITSymbolResolver>())
{
  /// Define common symbols that can be generated during compilation
  /// Necessary for valid linker symbol resolution
  symbol_resolver->registerSymbol("memset", reinterpret_cast<void*>(&memset));
  symbol_resolver->registerSymbol("memcpy", reinterpret_cast<void*>(&memcpy));
  symbol_resolver->registerSymbol("memcmp", reinterpret_cast<void*>(&memcmp));
}

JIT::~JIT()
{
}

// Needs a mutex lock
mcs_jit::CompiledModule JIT::compileModule(std::function<void(llvm::Module&)> compile_function)
{
  auto module = createModuleForCompilation();
  compile_function(*module);
  auto module_info = compileModule(std::move(module));

  ++current_module_key;
  return module_info;
}

// Needs a mutex lock
mcs_jit::CompiledModule JIT::compileAndCacheModule(std::function<void(llvm::Module&)> compile_function)
{
  auto compiledModule = compileModule(compile_function);
  moduleStorage_.add(compiledModule);
  return compiledModule;
}

mcs_jit::CompiledModule JIT::findOrCompileModule(const std::string& expressionString,
                                                 std::function<void(llvm::Module&)> compile_function)
{
  std::lock_guard<std::mutex> lock(jit_lock);

  // auto cachedCompiledModule = moduleStorage_.get(expressionString);
  // if (cachedCompiledModule)
  // {
  //   std::cout << " JIT cache hit " << expressionString << std::endl;
  //   return cachedCompiledModule.value();
  // }
  std::cout << " JIT cache miss " << expressionString << std::endl;

  return compileAndCacheModule(compile_function);
}

// #include "llvm/Support/raw_ostream.h"

// Needs a mutex lock
mcs_jit::CompiledModule JIT::compileModule(std::unique_ptr<llvm::Module> module)
{
  std::cout << "JIT::compileModule !!!!!!!!!!" << std::endl;
  runOptimizationPassesOnModule(*module);
  module->print(llvm::outs(), nullptr);

  auto buffer = compiler->compile(*module);

  llvm::Expected<std::unique_ptr<llvm::object::ObjectFile>> object_file =
      llvm::object::ObjectFile::createObjectFile(buffer->getMemBufferRef());
  if (!object_file)
  {
    std::string error_message;
    handleAllErrors(object_file.takeError(),
                    [&](const llvm::ErrorInfoBase& error_info) { error_message = error_info.message(); });
    // TODO: throw a more specific exception
    throw std::logic_error("Failed to create object file: " + error_message);
  }
  auto module_memory_manager = std::make_unique<ModuleMemoryManager>();
  llvm::RuntimeDyld runtime_dynamic_link = {*module_memory_manager, *symbol_resolver};
  auto linked_object = runtime_dynamic_link.loadObject(*object_file.get());
  runtime_dynamic_link.resolveRelocations();
  module_memory_manager->finalizeMemory(nullptr);

  mcs_jit::CompiledModule compiledModule_;
  for (const auto& function : *module)
  {
    if (function.isDeclaration())
    {
      continue;
    }
    auto function_name = function.getName().str();
    // std::cout << "function name " << function_name << std::endl;
    auto function_mangled_name = getMangledName(function_name);
    auto jit_symbol = runtime_dynamic_link.getSymbol(function_mangled_name);
    if (!jit_symbol)
    {
      // TODO throw a more specific exception
      throw std::logic_error("Failed to find symbol " + function_mangled_name);
    }
    auto* jit_symbol_address = reinterpret_cast<void*>(jit_symbol.getAddress());
    compiledModule_.function_name_to_symbol.emplace(std::move(function_name), jit_symbol_address);
  }
  compiledModule_.size = module_memory_manager->allocatedMemorySize();
  compiledModule_.identifier = current_module_key;
  moduleStorage_.addMemoryManager(current_module_key, std::move(module_memory_manager),
                                  compiledModule_.size);  // TODO here is a race

  return compiledModule_;
}

std::unique_ptr<llvm::Module> JIT::createModuleForCompilation()
{
  auto module = std::make_unique<llvm::Module>("jit_module_" + std::to_string(current_module_key), context);
  module->setDataLayout(data_layout);
  module->setTargetTriple(target_machine->getTargetTriple().str());
  return module;
}
std::string JIT::getMangledName(const std::string& name_to_mangle) const
{
  std::string mangled_name;
  llvm::raw_string_ostream ostream(mangled_name);
  llvm::Mangler::getNameWithPrefix(ostream, name_to_mangle, data_layout);
  ostream.flush();

  return mangled_name;
}
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
    throw std::logic_error("Failed to lookup target: " + error);
  }
  llvm::SubtargetFeatures features;
  llvm::StringMap<bool> feature_map;
  if (llvm::sys::getHostCPUFeatures(feature_map))
    for (auto& f : feature_map)
      features.AddFeature(f.first(), f.second);

  llvm::TargetOptions options;

  bool jit = true;
#if LLVM_VERSION_MAJOR < 17
  auto* target_machine = target->createTargetMachine(triple, cpu, features.getString(), options, llvm::None,
                                                     llvm::None, llvm::CodeGenOpt::Aggressive, jit);
#else
  auto* target_machine = target->createTargetMachine(triple, cpu, features.getString(), options, std::nullopt,
                                                     std::nullopt, llvm::CodeGenOpt::Aggressive, jit);
#endif
  if (!target_machine)
    throw std::logic_error("Failed to create target machine");

  return std::unique_ptr<llvm::TargetMachine>(target_machine);
}

void JIT::registerExternalSymbol(const std::string& symbol_name, void* address)
{
  std::lock_guard<std::mutex> lock(jit_lock);
  symbol_resolver->registerSymbol(symbol_name, address);
}

// Optimization passes are not well defined.
// Module PassManager removes the function.
#if LLVM_VERSION_MAJOR < 17
void JIT::runOptimizationPassesOnModule(llvm::Module& module) const
{
  llvm::PassManagerBuilder pass_manager_builder;
  llvm::legacy::PassManager mpm;
  llvm::legacy::FunctionPassManager fpm(&module);

  pass_manager_builder.OptLevel = 0;
  // pass_manager_builder.SLPVectorize = true;
  // pass_manager_builder.LoopVectorize = true;
  // pass_manager_builder.VerifyInput = true;
  // pass_manager_builder.VerifyOutput = true;
#if LLVM_VERSION_MAJOR < 16
  pass_manager_builder.RerollLoops = true;
  target_machine->adjustPassManager(pass_manager_builder);
#endif

  fpm.add(llvm::createTargetTransformInfoWrapperPass(target_machine->getTargetIRAnalysis()));
  mpm.add(llvm::createTargetTransformInfoWrapperPass(target_machine->getTargetIRAnalysis()));

  pass_manager_builder.populateFunctionPassManager(fpm);
  pass_manager_builder.populateModulePassManager(mpm);

  fpm.doInitialization();

  for (auto& function : module)
  {
    fpm.run(function);
  }

  fpm.doFinalization();

  // // WIP the module opt pass removes the function compiled
  // Module opt passes aggresivelly remove "unreferenced" functions from
  // a module.
  // mpm.run(module);
}
#endif

#if LLVM_VERSION_MAJOR >= 17
void JIT::runOptimizationPassesOnModule(llvm::Module& module) const
{
  llvm::LoopAnalysisManager lam;
  llvm::FunctionAnalysisManager fam;
  llvm::CGSCCAnalysisManager cgam;
  llvm::ModuleAnalysisManager mam;

  llvm::PassBuilder PassBuilder;

  PassBuilder.registerModuleAnalyses(mam);
  PassBuilder.registerCGSCCAnalyses(cgam);
  PassBuilder.registerFunctionAnalyses(fam);
  PassBuilder.registerLoopAnalyses(lam);
  PassBuilder.crossRegisterProxies(lam, fam, cgam, mam);

  // Create the pass manager.
  auto mpm = PassBuilder.buildPerModuleDefaultPipeline(llvm::OptimizationLevel::O0);
  auto fpm = PassBuilder.buildFunctionSimplificationPipeline(llvm::OptimizationLevel::O0,
                                                             llvm::ThinOrFullLTOPhase::None);

  // TODO LoopPassManager with vectorization enabled
  mpm.run(module, mam);

  for (auto& function : module)
  {
    fpm.run(function, fam);
  }
}
#endif

}  // namespace mcs_jit
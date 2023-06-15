#include "JIT.h"

#include <sys/mman.h>
#include <boost/noncopyable.hpp>
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

// TODO: implement this
class MemoryPage
{
 public:
  MemoryPage() : page_size(0)
  {
  }
  char* allocateMemory(size_t size, size_t alignment)
  {
    return nullptr;
  }
  inline size_t getAllocatedSize() const
  {
    return allocated_size;
  }

 private:
  size_t page_size = 0;
  size_t allocated_size = 0;
};

class JITModuleMemoryManager : public llvm::RTDyldMemoryManager
{
 public:
  uint8_t* allocateCodeSection(uintptr_t size, unsigned alignment, unsigned section_id,
                               llvm::StringRef section_name) override
  {
    return reinterpret_cast<uint8_t*>(exec_memory_page.allocateMemory(size, alignment));
  }
  uint8_t* allocateDataSection(uintptr_t size, unsigned alignment, unsigned, llvm::StringRef,
                               bool is_read_only) override
  {
    if (is_read_only)
    {
      return reinterpret_cast<uint8_t*>(ro_memory_page.allocateMemory(size, alignment));
    }
    else
    {
      return reinterpret_cast<uint8_t*>(rw_memory_page.allocateMemory(size, alignment));
    }
  }

  bool finalizeMemory(std::string* err_msg = nullptr) override
  {
    return false;
  }

  inline size_t allocatedMemorySize() const
  {
    return rw_memory_page.getAllocatedSize() + ro_memory_page.getAllocatedSize() +
           exec_memory_page.getAllocatedSize();
  }

 private:
  MemoryPage rw_memory_page;
  MemoryPage ro_memory_page;
  MemoryPage exec_memory_page;
};

class JITCompiler
{
 public:
  explicit JITCompiler(llvm::TargetMachine& target_machine_) : target_machine(target_machine_)
  {
  }

  /**
   * 把module编译成机器码
   * EN：Compile the module into machine code
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
      throw std::runtime_error("Failed to materialize module: " + error_message);
    }
    llvm::SmallVector<char, 4096> buffer;
    llvm::raw_svector_ostream ostream(buffer);
    llvm::legacy::PassManager pass_manager;
    llvm::MCContext* mc_context = nullptr;
    if (target_machine.addPassesToEmitMC(pass_manager, mc_context, ostream))
    {
      // TODO: throw a more specific exception
      throw std::runtime_error("Failed to create MC pass manager");
    }
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
      throw std::runtime_error("Symbol " + symbol_name + " not found");
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

JIT::CompiledModule JIT::compiledModule(std::function<void(llvm::Module&)> compile_function)
{
  auto module = createModuleForCompilation();
  compile_function(*module);
  auto module_info = compiledModule(std::move(module));

  ++current_module_key;
  return module_info;
}

JIT::CompiledModule JIT::compiledModule(std::unique_ptr<llvm::Module> module)
{
  // TODO: module optimization function
  auto buffer = compiler->compile(*module);
  llvm::Expected<std::unique_ptr<llvm::object::ObjectFile>> object_file =
      llvm::object::ObjectFile::createObjectFile(buffer->getMemBufferRef());
  if (!object_file)
  {
    std::string error_message;
    handleAllErrors(object_file.takeError(),
                    [&](const llvm::ErrorInfoBase& error_info) { error_message = error_info.message(); });
    // TODO: throw a more specific exception
    throw std::runtime_error("Failed to create object file: " + error_message);
  }
  auto module_memory_manager = std::make_unique<JITModuleMemoryManager>();
  llvm::RuntimeDyld runtime_dynamic_link = {*module_memory_manager, *symbol_resolver};
  auto linked_object = runtime_dynamic_link.loadObject(*object_file.get());
  runtime_dynamic_link.resolveRelocations();
  module_memory_manager->finalizeMemory();

  CompiledModule compiled_module;
  for (const auto& function : *module)
  {
    if (function.isDeclaration())
    {
      continue;
    }
    auto function_name = std::string(function.getName());
    auto function_mangled_name = getMangledName(function_name);
    auto jit_symbol = runtime_dynamic_link.getSymbol(function_mangled_name);
    if (!jit_symbol)
    {
      // TODO throw a more specific exception
      throw std::runtime_error("Failed to find symbol " + function_mangled_name);
    }
    auto* jit_symbol_address = reinterpret_cast<void*>(jit_symbol.getAddress());
    compiled_module.function_name_to_symbol.emplace(std::move(function_name), jit_symbol_address);
  }
  //  compiled_module.size = module_memory_manager->allocatedSize();
  compiled_module.identifier = current_module_key;
  module_identifier_to_memory_manager.emplace(current_module_key, std::move(module_memory_manager));
  compiled_code_size.fetch_add(compiled_module.size, std::memory_order_relaxed);
  return compiled_module;
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
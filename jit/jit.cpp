#include "jit.h"

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
#include <llvm/Passes/PassBuilder.h>

#include <llvm/ExecutionEngine/RTDyldMemoryManager.h>
#include <llvm/ADT/None.h>
#include <llvm/ExecutionEngine/MCJIT.h>

#include <boost/noncopyable.hpp>
namespace msc_jit
{
// Arena Memory pool for JIT
class Arena : private boost::noncopyable
{
 public:
  Arena() : page_size(8)
  {
  }
  ~Arena()
  {
    protect(PROT_READ | PROT_WRITE);

    for (auto& block : blocks)
    {
      free(block.base());
    }
  }
  /*
   * param: size: the size of the memory to be allocated
   * return: the address of the allocated memory
   * */
  char* allocateAligned(size_t size, size_t alignment)
  {
    for (size_t i = 0; i < blocks.size(); ++i)
    {
      char* result = allocateFromBlockWithIndex(size, alignment, i);
      if (result)
      {
        return result;
      }
    }
    allocateNewBlock(size);
    size_t allocated_index = blocks.size() - 1;
    char* result = allocateFromBlockWithIndex(size, alignment, allocated_index);
    assert(result);
    return result;
  }

  void protect(int flag)
  {
#if defined(__NetBSD__) && defined(PROT_MPROTECT)
    flag |= PROT_MPROTECT(PROT_READ | PROT_WRITE | PROT_EXEC);
#endif

    bool invalidate_cache = (flag & PROT_EXEC);

    for (const auto& block : blocks)
    {
#if defined(__arm__) || defined(__aarch64__)
      /// Certain ARM implementations treat icache clear instruction as a memory read,
      /// and CPU segfaults on trying to clear cache on !PROT_READ page.
      /// Therefore we need to temporarily add PROT_READ for the sake of flushing the instruction caches.
      if (invalidate_cache && !(flag & PROT_READ))
      {
        int res = mprotect(block.base(), block.blockSize(), flag | PROT_READ);
        if (res != 0)
          throw std::runtime_error("Cannot mprotect memory region");

        llvm::sys::Memory::InvalidateInstructionCache(block.base(), block.blockSize());
        invalidate_cache = false;
      }
#endif
      int res = mprotect(block.base(), block.blockSize(), flag);
      if (res != 0)
        throw std::runtime_error("Cannot mprotect memory region");

      if (invalidate_cache)
        llvm::sys::Memory::InvalidateInstructionCache(block.base(), block.blockSize());
    }
  }

  inline size_t getAllocatedSize() const
  {
    return allocated_size;
  }

  inline size_t getPageSize() const
  {
    return page_size;
  }

 private:
  size_t page_size = 0;
  size_t allocated_size = 0;
  struct Block
  {
   public:
    Block(void* pages_base_, size_t pages_size_, size_t page_size_)
     : pages_base(pages_base_), pages_size(pages_size_), page_size(page_size_)
    {
    }
    inline void* base() const
    {
      return pages_base;
    }
    inline size_t pagesSize() const
    {
      return pages_size;
    }
    inline size_t pageSize() const
    {
      return page_size;
    }
    inline size_t blockSize() const
    {
      return pages_size * page_size;
    }

   private:
    void* pages_base;
    size_t pages_size;
    size_t page_size;
  };
  std::vector<Block> blocks;
  std::vector<size_t> blocks_allocated_size;

  char* allocateFromBlockWithIndex(size_t size, size_t alignment, size_t block_index)
  {
    assert(block_index < blocks.size());
    auto& block = blocks[block_index];

    size_t block_size = block.blockSize();
    size_t& block_allocated_size = blocks_allocated_size[block_index];
    size_t block_free_size = block_size - block_allocated_size;

    uint8_t* block_base = static_cast<uint8_t*>(block.base());
    void* offset = block_base + block_allocated_size;
    auto* result = std::align(alignment, size, offset, block_free_size);
    if (result)
    {
      block_allocated_size = reinterpret_cast<uint8_t*>(result) - block_base;
      block_allocated_size += size;
      return static_cast<char*>(result);
    }
    return nullptr;
  }
  void allocateNewBlock(size_t size)
  {
    size_t allocate_page_num = ((size / page_size) + 1) * 2;
    size_t allocate_size = page_size * allocate_page_num;

    void* buf = nullptr;
    int res = posix_memalign(&buf, page_size, allocate_size);
    if (res != 0)
    {
      throw std::runtime_error("Cannot allocate memory");
    }
    blocks.emplace_back(buf, allocate_page_num, page_size);
    blocks_allocated_size.emplace_back(0);

    allocated_size += allocate_size;
  }
};

class JITModuleMemoryManager : public llvm::RTDyldMemoryManager
{
 public:
  uint8_t* allocateCodeSection(uintptr_t size, unsigned alignment, unsigned section_id,
                               llvm::StringRef section_name) override
  {
    return reinterpret_cast<uint8_t*>(exec_memory_page.allocateAligned(size, alignment));
  }
  uint8_t* allocateDataSection(uintptr_t size, unsigned alignment, unsigned, llvm::StringRef,
                               bool is_read_only) override
  {
    if (is_read_only)
    {
      return reinterpret_cast<uint8_t*>(ro_memory_page.allocateAligned(size, alignment));
    }
    else
    {
      return reinterpret_cast<uint8_t*>(rw_memory_page.allocateAligned(size, alignment));
    }
  }

  bool finalizeMemory(std::string* err_msg) override
  {
    ro_memory_page.protect(PROT_READ);
    exec_memory_page.protect(PROT_READ | PROT_EXEC);
    return true;
  }

  inline size_t allocatedMemorySize() const
  {
    return rw_memory_page.getAllocatedSize() + ro_memory_page.getAllocatedSize() +
           exec_memory_page.getAllocatedSize();
  }

 private:
  Arena rw_memory_page;
  Arena ro_memory_page;
  Arena exec_memory_page;
};

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

JIT::CompiledModule JIT::compileModule(std::function<void(llvm::Module&)> compile_function)
{
  auto module = createModuleForCompilation();
  compile_function(*module);
  auto module_info = compileModule(std::move(module));

  ++current_module_key;
  return module_info;
}

JIT::CompiledModule JIT::compileModule(std::unique_ptr<llvm::Module> module)
{
  runOptimizationPassesOnModule(*module);

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
  module_memory_manager->finalizeMemory(nullptr);

  CompiledModule compiled_module;
  for (const auto& function : *module)
  {
    if (function.isDeclaration())
    {
      continue;
    }
    auto function_name = function.getName().str();
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
  compiled_module.size = module_memory_manager->allocatedMemorySize();
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

void JIT::deleteCompiledModule(JIT::CompiledModule& module)
{
  std::lock_guard<std::mutex> lock(jit_lock);
  auto module_memory_manager_it = module_identifier_to_memory_manager.find(module.identifier);
  if (module_memory_manager_it == module_identifier_to_memory_manager.end())
  {
    throw std::runtime_error("There is no compiled module with identifier");
  }
  module_identifier_to_memory_manager.erase(module_memory_manager_it);
  compiled_code_size.fetch_sub(module.size, std::memory_order_relaxed);
}

void JIT::registerExternalSymbol(const std::string& symbol_name, void* address)
{
  std::lock_guard<std::mutex> lock(jit_lock);
  symbol_resolver->registerSymbol(symbol_name, address);
}

void JIT::runOptimizationPassesOnModule(llvm::Module& module) const
{
  llvm::PassManagerBuilder pass_manager_builder;
  llvm::legacy::PassManager mpm;
  llvm::legacy::FunctionPassManager fpm(&module);
  pass_manager_builder.OptLevel = 3;
  pass_manager_builder.SLPVectorize = true;
  pass_manager_builder.LoopVectorize = true;
  pass_manager_builder.RerollLoops = true;
  pass_manager_builder.VerifyInput = true;
  pass_manager_builder.VerifyOutput = true;
  target_machine->adjustPassManager(pass_manager_builder);

  fpm.add(llvm::createTargetTransformInfoWrapperPass(target_machine->getTargetIRAnalysis()));
  mpm.add(llvm::createTargetTransformInfoWrapperPass(target_machine->getTargetIRAnalysis()));

  pass_manager_builder.populateFunctionPassManager(fpm);
  pass_manager_builder.populateModulePassManager(mpm);

  fpm.doInitialization();
  for (auto& function : module)
    fpm.run(function);
  fpm.doFinalization();

  mpm.run(module);
}
}  // namespace msc_jit
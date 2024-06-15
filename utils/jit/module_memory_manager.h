#include <sys/mman.h>
#include <boost/noncopyable.hpp>

#include <llvm/ExecutionEngine/SectionMemoryManager.h>

#include <llvm/Support/DynamicLibrary.h>

#pragma once

namespace mcs_jit
{

const constexpr size_t JITPageSize = 4096;

// Arena Memory pool for JIT
class Arena : private boost::noncopyable
{
 public:
  Arena() : page_size(JITPageSize)
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
          throw std::logic_error("Cannot mprotect memory region");

        llvm::sys::Memory::InvalidateInstructionCache(block.base(), block.blockSize());
        invalidate_cache = false;
      }
#endif
      int res = mprotect(block.base(), block.blockSize(), flag);
      if (res != 0)
        throw std::logic_error("Cannot mprotect memory region");

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
      throw std::logic_error("Cannot allocate memory");
    }
    blocks.emplace_back(buf, allocate_page_num, page_size);
    blocks_allocated_size.emplace_back(0);

    allocated_size += allocate_size;
  }
};

class ModuleMemoryManager : public llvm::RTDyldMemoryManager
{
 public:
  uint8_t* allocateCodeSection(uintptr_t size, unsigned alignment, unsigned section_id,
                               llvm::StringRef section_name) override
  {
    return reinterpret_cast<uint8_t*>(ex_memory_page.allocateAligned(size, alignment));
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
    ex_memory_page.protect(PROT_READ | PROT_EXEC);
    return true;
  }

  inline size_t allocatedMemorySize() const
  {
    return rw_memory_page.getAllocatedSize() + ro_memory_page.getAllocatedSize() +
           ex_memory_page.getAllocatedSize();
  }

 private:
  Arena rw_memory_page;
  Arena ro_memory_page;
  Arena ex_memory_page;
};

}  // namespace mcs_jit

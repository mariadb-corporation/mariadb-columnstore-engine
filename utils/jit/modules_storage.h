/* Copyright (C) 2024 MariaDB Corporation

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License
  as published by the Free Software Foundation; version 2 of
  the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
  MA 02110-1301, USA. */

#pragma once

#include <cstddef>
#include <mutex>
#include <optional>
#include <unordered_map>

#include <llvm/IR/Module.h>

#include "LRU.h"
#include "compiled_module.h"
#include "module_memory_manager.h"

namespace mcs_jit
{

using ExprToModulesVectorId = std::unordered_map<std::string, std::size_t>;
class ModuleMemoryManager;
using ModuleIdToMemManager = std::unordered_map<uint64_t, std::unique_ptr<ModuleMemoryManager>>;
const constexpr size_t MaxLRUCacheSize = 8192;

class CompiledModuleStorageIface
{
 public:
  virtual ~CompiledModuleStorageIface() = default;
  virtual void add(const CompiledModule& module) = 0;
  virtual CompiledModuleOpt get(const std::string&) = 0;
  virtual void clear() = 0;
  virtual std::size_t size() = 0;
  virtual bool empty() = 0;
};

// Need to take Modules MemManager_ into.
class CompiledModuleStorage : public CompiledModuleStorageIface
{
 public:
  CompiledModuleStorage(const size_t maxSize = MaxLRUCacheSize);
  void add(const CompiledModule& module) override;
  std::optional<CompiledModule> get(const std::string& expressionString) override;
  void addMemoryManager(const size_t moduleKey, std::unique_ptr<ModuleMemoryManager> manager,
                        const size_t moduleSize);
  void clear() override;
  std::size_t size() override;
  bool empty() override;
  ModuleIdToMemManager& getModuleIdToMemManager()
  {
    return moduleIdToMemManager_;
  }

 private:
  void deleteCompiledModuleMemory(const CompiledModule& module);
  std::mutex modulesMutex_;  // This mutex protection might overlap with JIT::jit_lock
  ModulesVector modules_;
  ExprToModulesVectorId exprToModules_;
  std::unique_ptr<mcs_lru::LRUIface> lru_;
  size_t maxSize_;
  size_t compiledCodeSize = 0;
  ModuleIdToMemManager moduleIdToMemManager_;
};
}  // namespace mcs_jit

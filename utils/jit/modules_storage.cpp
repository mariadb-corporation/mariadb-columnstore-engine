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

#include "modules_storage.h"
#include <mutex>
#include "LRU.h"

namespace mcs_jit
{

CompiledModuleStorage::CompiledModuleStorage(const size_t maxSize) : maxSize_(maxSize)
{
  lru_ = std::unique_ptr<mcs_lru::LRUIface>(new mcs_lru::LRU());
}

void CompiledModuleStorage::add(CompiledModule module)
{
  std::lock_guard<std::mutex> lg(modulesMutex_);

  if (modules_.size() >= maxSize_)
  {
    const auto leastUsedId = *lru_->end();
    lru_->remove(leastUsedId);
    // remove all functions from the map
    for (auto& expressionString : modules_[leastUsedId].getExpressionStringList())
    {
      exprToModules_.erase(expressionString);
    }

    modules_[leastUsedId] = std::move(module);
    lru_->add(leastUsedId);

    return;
  }

  // TODO needs sync mech here
  const auto id = modules_.size();

  for (auto& expressionString : module.getExpressionStringList())
  {
    exprToModules_[expressionString] = id;
  }

  // TODO check if copy ctor is fine here
  modules_.emplace_back(module);
}

CompiledModuleOpt CompiledModuleStorage::get(const std::string& expressionString)
{
  std::lock_guard<std::mutex> lg(modulesMutex_);

  auto it = exprToModules_.find(expressionString);
  if (it == exprToModules_.end())
  {
    return std::nullopt;
  }
  const auto id = it->second;
  return {modules_[id]};
}

void CompiledModuleStorage::addMemoryManager(const size_t moduleKey,
                                             std::unique_ptr<ModuleMemoryManager> manager,
                                             const size_t moduleSize)
{
  std::lock_guard<std::mutex> lock(modulesMutex_);

  moduleIdToMemManager_.emplace(moduleKey, std::move(manager));
  compiledCodeSize += moduleSize;
}

void CompiledModuleStorage::deleteCompiledModuleMemory(CompiledModule& module)
{
  std::lock_guard<std::mutex> lock(modulesMutex_);
  auto module_memory_manager_it = moduleIdToMemManager_.find(module.identifier);
  if (module_memory_manager_it == moduleIdToMemManager_.end())
  {
    throw std::logic_error("There is no compiled module with identifier");
  }
  moduleIdToMemManager_.erase(module_memory_manager_it);
  compiledCodeSize -= module.size;
}

void CompiledModuleStorage::clear()
{
  modules_.clear();
  exprToModules_.clear();
  lru_->clear();
}

std::size_t CompiledModuleStorage::size() const
{
  return modules_.size();
}

bool CompiledModuleStorage::empty() const
{
  return modules_.empty();
}

}  // namespace mcs_jit
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

// TODO remove
#include <iostream>
#include <mutex>

#include "modules_storage.h"
#include "LRU.h"

namespace mcs_jit
{

CompiledModuleStorage::CompiledModuleStorage(const size_t maxSize) : maxSize_(maxSize)
{
  lru_ = std::unique_ptr<mcs_lru::LRUIface>(new mcs_lru::LRU());
}

// TODO refactor
void CompiledModuleStorage::add(const CompiledModule& module)
{
  std::lock_guard<std::mutex> lg(modulesMutex_);

  if (modules_.size() >= maxSize_)
  {
    const auto leastUsedId = *lru_->rbegin();
    lru_->remove(leastUsedId);

    std::cout << "Replace the LRU element of the cache " << leastUsedId << " with modules_.size() "
              << modules_.size() << std::endl;

    // assert(leastUsedId < modules_.size() && "LRU cache is corrupted");
    const auto& lruModule = modules_[leastUsedId];

    // remove the pre-empted module memory
    deleteCompiledModuleMemory(lruModule);

    // remove all functions from the map
    for (auto& expressionString : modules_[leastUsedId].getExpressionStringList())
    {
      exprToModules_.erase(expressionString);
    }

    for (auto& expressionString : module.getExpressionStringList())
    {
      exprToModules_[expressionString] = leastUsedId;
    }

    // swap module with the least used one
    modules_[leastUsedId] = module;
    // TODO need to register funcs added to the map
    lru_->add(leastUsedId);

    return;
  }

  // TODO needs sync mech here
  const auto id = modules_.size();
  lru_->add(id);

  for (auto& expressionString : module.getExpressionStringList())
  {
    exprToModules_[expressionString] = id;
  }

  // TODO check if copy ctor is fine here
  modules_.emplace_back(module);

  std::cout << "Cache miss add to LRU id " << id << " with modules_.size() " << modules_.size() << std::endl;
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

// Needs modulesMutex_ mutex
void CompiledModuleStorage::deleteCompiledModuleMemory(const CompiledModule& module)
{
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
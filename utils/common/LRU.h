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

#include <cstdint>
#include <list>

#include "branchpred.h"
#include "robin_hood.h"

namespace mcs_lru
{

/** @brief NoOP interface to LRU-cache used by RowGroupStorage & HashStorage
 */

struct LRUIface
{
  using List = std::list<uint64_t>;

  virtual ~LRUIface() = default;
  /** @brief Put an ID to cache or set it as last used */
  virtual void add(uint64_t)
  {
  }
  /** @brief Remove an ID from cache */
  virtual void remove(uint64_t)
  {
  }
  /** @brief Get iterator of the most recently used ID */
  virtual List::const_reverse_iterator begin() const
  {
    return List::const_reverse_iterator();
  }
  /** @brief Get iterator after the latest ID */
  virtual List::const_reverse_iterator end() const
  {
    return List::const_reverse_iterator();
  }
  /** @brief Get iterator of the latest ID */
  virtual List::const_iterator rbegin() const
  {
    return {};
  }
  /** @brief Get iterator after the most recently used ID */
  virtual List::const_iterator rend() const
  {
    return {};
  }

  virtual void clear()
  {
  }
  virtual std::size_t size() const
  {
    return 0;
  }
  virtual bool empty() const
  {
    return true;
  }
  virtual LRUIface* clone() const
  {
    return new LRUIface();
  }
};

struct LRU : public LRUIface
{
  ~LRU() override
  {
    fMap.clear();
    fList.clear();
  }
  inline void add(uint64_t rgid) final
  {
    auto it = fMap.find(rgid);
    if (it != fMap.end())
    {
      fList.erase(it->second);
    }
    fMap[rgid] = fList.insert(fList.end(), rgid);
  }

  inline void remove(uint64_t rgid) final
  {
    auto it = fMap.find(rgid);
    if (UNLIKELY(it != fMap.end()))
    {
      fList.erase(it->second);
      fMap.erase(it);
    }
  }

  inline List::const_reverse_iterator begin() const final
  {
    return fList.crbegin();
  }
  inline List::const_reverse_iterator end() const final
  {
    return fList.crend();
  }
  inline List::const_iterator rbegin() const final
  {
    return fList.cbegin();
  }
  inline List::const_iterator rend() const final
  {
    return fList.cend();
  }
  inline void clear() final
  {
    fMap.clear();
    fList.clear();
  }

  size_t size() const final
  {
    return fMap.size();
  }
  bool empty() const final
  {
    return fList.empty();
  }

  LRUIface* clone() const final
  {
    return new LRU();
  }

  robin_hood::unordered_flat_map<uint64_t, List::iterator> fMap;
  List fList;
};

}  // namespace mcs_lru
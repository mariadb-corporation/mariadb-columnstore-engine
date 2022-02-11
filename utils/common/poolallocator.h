/* Copyright (C) 2014 InfiniDB, Inc.

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

/******************************************************************************************
 * $Id$
 *
 ******************************************************************************************/

/* This allocator is an attempt to consolidate small allocations and
   deallocations to boost performance and reduce mem fragmentation. */

#ifndef POOLALLOCATOR_H_
#define POOLALLOCATOR_H_

#include <unistd.h>
#include <stdint.h>
#include <vector>
#include <map>
#include <boost/shared_array.hpp>
#include <atomic>

namespace utils
{
class PoolAllocator
{
 public:
  static const unsigned DEFAULT_WINDOW_SIZE = 4096 * 40;  // should be an integral # of pages

  explicit PoolAllocator(unsigned windowSize = DEFAULT_WINDOW_SIZE, bool isTmpSpace = false,
                         bool _useLock = false)
   : allocSize(windowSize)
   , tmpSpace(isTmpSpace)
   , capacityRemaining(0)
   , memUsage(0)
   , nextAlloc(0)
   , useLock(_useLock)
   , lock(false)
  {
  }
  PoolAllocator(const PoolAllocator& p)
   : allocSize(p.allocSize)
   , tmpSpace(p.tmpSpace)
   , capacityRemaining(0)
   , memUsage(0)
   , nextAlloc(0)
   , useLock(p.useLock)
   , lock(false)
  {
  }
  virtual ~PoolAllocator()
  {
  }

  PoolAllocator& operator=(const PoolAllocator&);

  void* allocate(uint64_t size);
  void deallocate(void* p);
  void deallocateAll();

  inline uint64_t getMemUsage() const
  {
    return memUsage;
  }
  unsigned getWindowSize() const
  {
    return allocSize;
  }

  void setUseLock(bool ul)
  {
    useLock = ul;
  }

 private:
  void newBlock();
  void* allocOOB(uint64_t size);

  unsigned allocSize;
  std::vector<boost::shared_array<uint8_t> > mem;
  bool tmpSpace;
  unsigned capacityRemaining;
  uint64_t memUsage;
  uint8_t* nextAlloc;
  bool useLock;
  std::atomic<bool> lock;

  struct OOBMemInfo
  {
    boost::shared_array<uint8_t> mem;
    uint64_t size;
  };
  typedef std::map<void*, OOBMemInfo> OutOfBandMap;
  OutOfBandMap oob;  // for mem chunks bigger than the window size; these can be dealloc'd
};

inline void* PoolAllocator::allocate(uint64_t size)
{
  void* ret;
  bool _false = false;

  if (useLock)
    while (!lock.compare_exchange_weak(_false, true, std::memory_order_acquire))
      _false = false;

  if (size > allocSize)
  {
    ret = allocOOB(size);
    if (useLock)
      lock.store(false, std::memory_order_release);
    return ret;
  }

  if (size > capacityRemaining)
    newBlock();

  ret = (void*)nextAlloc;
  nextAlloc += size;
  capacityRemaining -= size;
  memUsage += size;
  if (useLock)
    lock.store(false, std::memory_order_release);
  return ret;
}

}  // namespace utils

#endif

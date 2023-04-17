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

/* This allocator is for frequent small allocations that all get deallocated at once.
   It allocates large blocks of memory from the system and distributes 'allocsize'
   units to the caller.  When the large allocation is used up, it will allocate
   more unless the 'tmpspace' flag is set.  If it is, it will reuse the memory it
   already allocated.  This is useful for short-lived vars that are guaranteed to be
   out of scope by the time the allocator wraps around.

   TODO: make it STL and boost compliant...
*/

#pragma once

#include <stdint.h>

#include <vector>
#include <limits>
#include <unistd.h>
#include <atomic>
#include "spinlock.h"

#define EXPORT

namespace utils
{
class FixedAllocator
{
 public:
  EXPORT static const unsigned long DEFAULT_NUM_ELEMENTS = (4096 * 4);  // should be a multiple of pagesize

  EXPORT FixedAllocator()
   : capacityRemaining(0)
   , elementCount(DEFAULT_NUM_ELEMENTS)
   , elementSize(0)
   , currentlyStored(0)
   , tmpSpace(false)
   , nextAlloc(0)
   , useLock(false)
   , lock(false)
  {
  }
  EXPORT explicit FixedAllocator(unsigned long allocSize, bool isTmpSpace = false,
                                 unsigned long numElements = DEFAULT_NUM_ELEMENTS)
   : capacityRemaining(0)
   , elementCount(numElements)
   , elementSize(allocSize)
   , currentlyStored(0)
   , tmpSpace(isTmpSpace)
   , nextAlloc(0)
   , useLock(false)
   , lock(false)
  {
  }
  EXPORT FixedAllocator(const FixedAllocator&);
  EXPORT FixedAllocator& operator=(const FixedAllocator&);
  virtual ~FixedAllocator()
  {
  }

  EXPORT void* allocate();
  EXPORT void* allocate(
      uint32_t len);  // a hack to make it work more like a pool allocator (use PoolAllocator instead)
  EXPORT void truncateBy(uint32_t amt);  // returns a portion of mem just allocated; use with caution
  void deallocate()
  {
  }                             // does nothing
  EXPORT void deallocateAll();  // drops all memory in use
  EXPORT uint64_t getMemUsage() const;
  void setUseLock(bool);
  void setAllocSize(uint);

 private:
  void newBlock();

  std::vector<std::shared_ptr<uint8_t[]>> mem;
  unsigned long capacityRemaining;
  uint64_t elementCount;
  unsigned long elementSize;
  uint64_t currentlyStored;
  bool tmpSpace;
  uint8_t* nextAlloc;
  bool useLock;
  std::atomic<bool> lock;
};

inline void* FixedAllocator::allocate()
{
  void* ret;

  if (useLock)
    getSpinlock(lock);
  if (capacityRemaining < elementSize)
    newBlock();

  ret = nextAlloc;
  nextAlloc += elementSize;
  capacityRemaining -= elementSize;
  currentlyStored += elementSize;
  if (useLock)
    releaseSpinlock(lock);
  return ret;
}

inline void* FixedAllocator::allocate(uint32_t len)
{
  void* ret;

  if (useLock)
    getSpinlock(lock);
  if (capacityRemaining < len)
    newBlock();

  ret = nextAlloc;
  nextAlloc += len;
  capacityRemaining -= len;
  currentlyStored += len;
  if (useLock)
    releaseSpinlock(lock);
  return ret;
}

#undef EXPORT
}  // namespace utils

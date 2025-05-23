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

//#define NDEBUG
#include <cassert>
#include <boost/smart_ptr/allocate_shared_array.hpp>
#include <boost/smart_ptr/make_shared_array.hpp>

#include "poolallocator.h"

using namespace std;


namespace utils
{
PoolAllocator& PoolAllocator::operator=(const PoolAllocator& v)
{
  allocSize = v.allocSize;
  tmpSpace = v.tmpSpace;
  useLock = v.useLock;
  alloc = v.alloc;
  deallocateAll();
  return *this;
}

void PoolAllocator::deallocateAll()
{
  capacityRemaining = 0;
  nextAlloc = NULL;
  memUsage = 0;
  mem.clear();
  oob.clear();
}

void PoolAllocator::newBlock()
{
  capacityRemaining = allocSize;
  if (!tmpSpace || mem.size() == 0)
  {
    if (alloc)
    {
      mem.emplace_back(boost::allocate_shared<PoolAllocatorBufType>(*alloc, allocSize)); 
    }
    else 
    {
      mem.emplace_back(boost::make_shared<PoolAllocatorBufType>(allocSize));
    }
    nextAlloc = mem.back().get();
  }
  else
    nextAlloc = mem.front().get();
}

void* PoolAllocator::allocOOB(uint64_t size)
{
  OOBMemInfo memInfo;

  memUsage += size;
  if (alloc)
  {
    memInfo.mem = boost::allocate_shared<PoolAllocatorBufType>(*alloc, size);
  }
  else 
  {
    memInfo.mem = boost::make_shared<PoolAllocatorBufType>(size);
  }
  memInfo.size = size;
  void* ret = (void*)memInfo.mem.get();
  oob[ret] = memInfo;
  return ret;
}

void PoolAllocator::deallocate(void* p)
{
  bool _false = false;
  if (useLock)
    while (!lock.compare_exchange_weak(_false, true, std::memory_order_acquire))
      _false = false;
  OutOfBandMap::iterator it = oob.find(p);

  if (it == oob.end())
  {
    if (useLock)
      lock.store(false, std::memory_order_release);
    return;
  }

  memUsage -= it->second.size;
  oob.erase(it);
  if (useLock)
    lock.store(false, std::memory_order_release);
}

}  // namespace utils

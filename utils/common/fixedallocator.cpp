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

// This is one of the first files we compile, check the compiler...
#include <stdint.h>

#include "fixedallocator.h"

#include <boost/smart_ptr/allocate_shared_array.hpp>
#include <boost/smart_ptr/make_shared_array.hpp>


using namespace std;

namespace utils
{
FixedAllocator::FixedAllocator(const FixedAllocator& f)
{
  elementCount = f.elementCount;
  elementSize = f.elementSize;
  tmpSpace = f.tmpSpace;
  capacityRemaining = 0;
  currentlyStored = 0;
  useLock = f.useLock;
  lock = false;
  alloc = f.alloc;
}

FixedAllocator& FixedAllocator::operator=(const FixedAllocator& f)
{
  elementCount = f.elementCount;
  elementSize = f.elementSize;
  tmpSpace = f.tmpSpace;
  useLock = f.useLock;
  lock = false;
  alloc = f.alloc;
  deallocateAll();
  return *this;
}

void FixedAllocator::setUseLock(bool useIt)
{
  useLock = useIt;
}

void FixedAllocator::setAllocSize(uint allocSize)
{
  elementSize = allocSize;
}

void FixedAllocator::newBlock()
{
  // boost::shared_ptr<FixedAllocatorBufType> next;

  capacityRemaining = elementCount * elementSize;

  if (!tmpSpace || mem.size() == 0)
  {
    if (alloc)
    {
      mem.emplace_back(boost::allocate_shared<FixedAllocatorBufType>(*alloc, elementCount * elementSize)); 
    }
    else 
    {
      mem.emplace_back(boost::make_shared<FixedAllocatorBufType>(elementCount * elementSize));
    }
    // next.reset(new uint8_t[elementCount * elementSize]);
    // mem.push_back(next);
    // nextAlloc = next.get();
    nextAlloc = mem.back().get();
  }
  else
  {
    currentlyStored = 0;
    nextAlloc = mem.front().get();
  }
}

void FixedAllocator::truncateBy(uint32_t amt)
{
  if (useLock)
    getSpinlock(lock);
  nextAlloc -= amt;
  capacityRemaining += amt;
  currentlyStored -= amt;
  if (useLock)
    releaseSpinlock(lock);
}

void FixedAllocator::deallocateAll()
{
  mem.clear();
  currentlyStored = 0;
  capacityRemaining = 0;
}

uint64_t FixedAllocator::getMemUsage() const
{
  return (mem.size() * elementCount * elementSize);
}

}  // namespace utils

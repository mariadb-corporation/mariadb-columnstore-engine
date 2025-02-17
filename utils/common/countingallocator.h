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

#include <cassert>
#include <cstdint>
#include <atomic>
#include <cstddef>
#include <cstdlib>

namespace allocators
{

// This is an aggregating custom allocator that tracks the memory usage using
// a globally unique atomic counter.
// It is supposed to recv a ptr to an atomic from a singleton entity, e.g. ResourceManager.
// NB The atomic provides an upper hard limit for the memory usage and not the usage counter.
// The allocator's model counts allocated size locally and to sync allocated size difference
// every CheckPointStepSize(100MB by default) both allocating and deallocating.
// When a sync op hits MemoryLimitLowerBound trying to allocate more memory, it throws.
// SQL operators or TBPS runtime must catch the exception and act acordingly.

const constexpr int64_t MemoryLimitLowerBound = 500 * 1024 * 1024;  // WIP
const constexpr int64_t CheckPointStepSize = 100 * 1024 * 1024;     // WIP

// Custom Allocator that tracks allocated memory using an atomic counter
template <typename T>
class CountingAllocator
{
 public:
  using value_type = T;

  bool needCheckPoint(const int64_t sizeChange, const int64_t diffSinceLastCheckPoint,
                      const int64_t checkPointStepSize)
  {
    return std::llabs(sizeChange + diffSinceLastCheckPoint) > checkPointStepSize;
  }

  int64_t int_distance(const int64_t x, const int64_t y)
  {
    return (x > y) ? x - y : y - x;
  }

  // INVARIANT: sizeChange > 0
  void changeLocalAndGlobalMemoryLimits(const int64_t sizeChange)
  {
    // This routine must be used for mem allocation accounting path only!
    // The case CurrentCheckpoint > LastCheckpoint(we deallocated mem since the last checkpoint), sizeIncrease is
    // negative b/c we now move into the opposite direction. The case Last > Current (we allocated
    // mem since the last checkpoint), sizeIncrease is positive
    int64_t sizeChangeWDirection =
        (currentLocalMemoryUsage_ <= lastMemoryLimitCheckpoint_) ? -sizeChange : sizeChange;
    int64_t diffSinceLastCheckPoint = int_distance(currentLocalMemoryUsage_, lastMemoryLimitCheckpoint_);
    if (needCheckPoint(sizeChangeWDirection, diffSinceLastCheckPoint, checkPointStepSize_))
    {
      int64_t lastMemoryLimitCheckpointDiff = (currentLocalMemoryUsage_ <= lastMemoryLimitCheckpoint_)
                                                  ? sizeChange - diffSinceLastCheckPoint
                                                  : sizeChange + diffSinceLastCheckPoint;
      assert(lastMemoryLimitCheckpointDiff > 0);

      auto currentGlobalMemoryLimit =
          memoryLimit_->fetch_sub(lastMemoryLimitCheckpointDiff, std::memory_order_relaxed);
      if (currentGlobalMemoryLimit < memoryLimitLowerBound_)
      {
        memoryLimit_->fetch_add(lastMemoryLimitCheckpointDiff, std::memory_order_relaxed);
        throw std::bad_alloc();
      }
      lastMemoryLimitCheckpoint_ += lastMemoryLimitCheckpointDiff;
    }

    currentLocalMemoryUsage_ += sizeChange;
  }

  // Constructor accepting a reference to an atomic counter
  explicit CountingAllocator(std::atomic<int64_t>* memoryLimit,
                             const int64_t checkPointStepSize = CheckPointStepSize,
                             const int64_t lowerBound = MemoryLimitLowerBound) noexcept
   : memoryLimit_(memoryLimit), memoryLimitLowerBound_(lowerBound), checkPointStepSize_(checkPointStepSize)
  {
  }

  // Copy constructor (template to allow conversion between different types)
  template <typename U>
  CountingAllocator(const CountingAllocator<U>& other) noexcept
   : memoryLimit_(other.memoryLimit_)
   , memoryLimitLowerBound_(other.memoryLimitLowerBound_)
   , checkPointStepSize_(other.checkPointStepSize_)
  {
  }

  // Allocate memory for n objects of type T
  template <typename U = T>
  typename std::enable_if<!std::is_array<U>::value, U*>::type allocate(std::size_t n)
  {
    auto sizeAllocated = n * sizeof(T);

    changeLocalAndGlobalMemoryLimits(sizeAllocated);

    T* ptr = static_cast<T*>(::operator new(sizeAllocated));
    return ptr;
  }

  template <typename U = T>
  typename std::enable_if<std::is_array<U>::value, typename std::remove_extent<U>::type*>::type allocate(
      std::size_t n)
  {
    auto sizeAllocated = n * sizeof(T);

    changeLocalAndGlobalMemoryLimits(sizeAllocated);

    T ptr = static_cast<T>(::operator new[](n));
    return ptr;
  }

  // Deallocate memory for n objects of type T
  void deallocate(T* ptr, std::size_t n) noexcept
  {
    ::operator delete(ptr);

    int64_t sizeToDeallocate = n * sizeof(T);

    int64_t sizeChangeWDirection =
        (currentLocalMemoryUsage_ >= lastMemoryLimitCheckpoint_) ? -sizeToDeallocate : sizeToDeallocate;
    int64_t diffSinceLastCheckPoint = int_distance(currentLocalMemoryUsage_, lastMemoryLimitCheckpoint_);

    if (needCheckPoint(sizeChangeWDirection, diffSinceLastCheckPoint, checkPointStepSize_))
    {
      // Invariant is lastMemoryLimitCheckpoint_ >= currentLocalMemoryUsage_ - sizeToDeallocate
      int64_t lastMemoryLimitCheckpointDiff =
          (currentLocalMemoryUsage_ >= lastMemoryLimitCheckpoint_)
              ? sizeToDeallocate - (currentLocalMemoryUsage_ - lastMemoryLimitCheckpoint_)
              : diffSinceLastCheckPoint + sizeToDeallocate;

      assert(lastMemoryLimitCheckpointDiff > 0);
      memoryLimit_->fetch_add(lastMemoryLimitCheckpointDiff, std::memory_order_relaxed);

      lastMemoryLimitCheckpoint_ -= (lastMemoryLimitCheckpoint_ == 0) ? 0 : lastMemoryLimitCheckpointDiff;
    }
    currentLocalMemoryUsage_ = currentLocalMemoryUsage_ - sizeToDeallocate;
  }

  // Equality operators (allocators are equal if they share the same counter)
  template <typename U>
  bool operator==(const CountingAllocator<U>& other) const noexcept
  {
    return memoryLimit_ == other.memoryLimit_;
  }

  template <typename U>
  bool operator!=(const CountingAllocator<U>& other) const noexcept
  {
    return !(*this == other);
  }

  int64_t getMemoryLimitLowerBound() const noexcept
  {
    return memoryLimitLowerBound_;
  }

  int64_t getlastMemoryLimitCheckpoint() const noexcept
  {
    return lastMemoryLimitCheckpoint_;
  }

  int64_t getCurrentLocalMemoryUsage() const noexcept
  {
    return currentLocalMemoryUsage_;
  }

 private:
  std::atomic<int64_t>* memoryLimit_ = nullptr;
  int64_t memoryLimitLowerBound_ = MemoryLimitLowerBound;
  int64_t checkPointStepSize_ = CheckPointStepSize;
  int64_t lastMemoryLimitCheckpoint_ = 0;
  int64_t currentLocalMemoryUsage_ = 0;

  // Grant access to other instances of CountingAllocator with different types
  template <typename U>
  friend class CountingAllocator;
};

}  // namespace allocators
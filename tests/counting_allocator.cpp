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
#include <gtest/gtest.h>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>

#include <boost/smart_ptr/allocate_shared_array.hpp>

#include "countingallocator.h"

using namespace allocators;

// Example class to be managed by the allocator
struct TestClass
{
  int value[1024];

  TestClass(int val) : value(val)
  {
  }
};

static const constexpr int64_t MemoryAllowance = 1 * 1024 * 1024;
static const constexpr int64_t MemoryLimitStep = MemoryAllowance / 100;

// Test Fixture for AtomicCounterAllocator
class CountingAllocatorTest : public ::testing::Test
{
 protected:
  // Atomic counter to track allocated memory
  std::atomic<int64_t> allocatedMemory{MemoryAllowance};

  // Custom allocator instance
  CountingAllocator<TestClass> allocator;

  // Constructor
  CountingAllocatorTest()
   : allocatedMemory(MemoryAllowance)
   , allocator(&allocatedMemory, MemoryAllowance / 100, MemoryAllowance / 100)
  {
  }

  // Destructor
  ~CountingAllocatorTest() override = default;
};

// Test 1: Allocation increases the counter correctly
TEST_F(CountingAllocatorTest, Allocation)
{
  const std::size_t numObjects = 5;
  TestClass* ptr = allocator.allocate(numObjects);
  EXPECT_NE(ptr, nullptr);
  if (MemoryLimitStep > numObjects * static_cast<int64_t>(sizeof(TestClass)))
  {
    EXPECT_EQ(allocatedMemory.load() - allocator.getCurrentLocalMemoryUsage(),
              MemoryAllowance - numObjects * static_cast<int64_t>(sizeof(TestClass)));
  }
  else
  {
    EXPECT_EQ(allocatedMemory.load(), MemoryAllowance - numObjects * static_cast<int64_t>(sizeof(TestClass)));
  }
  allocator.deallocate(ptr, numObjects);
}

// Test 2: Deallocation decreases the counter correctly
TEST_F(CountingAllocatorTest, Deallocation)
{
  const std::size_t numObjects = 3;
  TestClass* ptr = allocator.allocate(numObjects);
  if (MemoryLimitStep > numObjects * static_cast<int64_t>(sizeof(TestClass)))
  {
    EXPECT_EQ(allocatedMemory.load() - allocator.getCurrentLocalMemoryUsage(),
              MemoryAllowance - numObjects * static_cast<int64_t>(sizeof(TestClass)));
  }
  else
  {
    EXPECT_EQ(allocatedMemory.load(), MemoryAllowance - numObjects * static_cast<int64_t>(sizeof(TestClass)));
  }
  allocator.deallocate(ptr, numObjects);
  EXPECT_EQ(allocatedMemory.load(), MemoryAllowance);
}

// Test 3: Allocator equality based on shared counter
TEST_F(CountingAllocatorTest, AllocatorEquality)
{
  CountingAllocator<TestClass> allocator1(&allocatedMemory);
  CountingAllocator<TestClass> allocator2(&allocatedMemory);
  EXPECT_TRUE(allocator1 == allocator2);

  std::atomic<int64_t> anotherCounter(0);
  CountingAllocator<TestClass> allocator3(&anotherCounter);
  EXPECT_FALSE(allocator1 == allocator3);
}

// Test 4: Using allocator with std::allocate_shared
TEST_F(CountingAllocatorTest, AllocateSharedUsesAllocator)
{
  // Create a shared_ptr using allocate_shared with the custom allocator
  CountingAllocator<TestClass> allocatorSmallerStep(&allocatedMemory,
                                                    MemoryAllowance / 1000, MemoryAllowance / 100);
  std::shared_ptr<TestClass> ptr1 = std::allocate_shared<TestClass>(allocatorSmallerStep, 100);
  std::shared_ptr<TestClass> ptr2 = std::allocate_shared<TestClass>(allocatorSmallerStep, 100);
  std::shared_ptr<TestClass> ptr3 = std::allocate_shared<TestClass>(allocatorSmallerStep, 100);

  // Check that the counter has increased by the size of TestClass plus control block
  // Exact size depends on the implementation, so we verify it's at least sizeof(TestClass)
  EXPECT_LE(allocatedMemory.load(), MemoryAllowance - 3 * static_cast<int64_t>(sizeof(TestClass)));

  // Reset the shared_ptr and check that the counter decreases appropriately
  ptr1.reset();
  ptr2.reset();
  ptr3.reset();
  // After deallocation, the counter should return to zero
  EXPECT_EQ(allocatedMemory.load(), MemoryAllowance);

  // auto deleter = [&allocatorSmallerStep](TestClass* ptr) { allocatorSmallerStep.deallocate(ptr, 1); };
  // ptr1.reset(allocatorSmallerStep.allocate(3), deleter);
  // EXPECT_LE(allocatedMemory.load(), MemoryAllowance - static_cast<int64_t>(sizeof(TestClass)));

  ptr1.reset();
  EXPECT_EQ(allocatedMemory.load(), MemoryAllowance);

  using RGDataBufType = uint8_t[];
  size_t allocSize = 16ULL * 8192;
  auto buf = boost::allocate_shared<RGDataBufType>(allocator, allocSize);

  EXPECT_LE(allocatedMemory.load(), MemoryAllowance - allocSize);

  buf.reset();
  EXPECT_EQ(allocatedMemory.load(), MemoryAllowance);

  CountingAllocator<RGDataBufType> allocator1(&allocatedMemory, MemoryAllowance / 100, MemoryAllowance / 100);
  std::optional<CountingAllocator<RGDataBufType>> allocator2(allocator1);
  auto buf1 = boost::allocate_shared<RGDataBufType>(*allocator2, allocSize);
  EXPECT_LE(allocatedMemory.load(), MemoryAllowance - allocSize);

  buf1.reset();
  EXPECT_EQ(allocatedMemory.load(), MemoryAllowance);
}

// Test 5: Thread Safety - Concurrent Allocations and Deallocations
TEST_F(CountingAllocatorTest, ThreadSafety)
{
  const std::size_t numThreads = 100;
  const std::size_t allocationsPerThread = 3;

  auto worker = [this]()
  {
    std::vector<TestClass*> ptrs;
    CountingAllocator<TestClass> allocatorLocal(&allocatedMemory, MemoryAllowance / 1000,
                                                MemoryAllowance / 100);
    for (std::size_t i = 0; i < allocationsPerThread; ++i)
    {
      ptrs.push_back(allocatorLocal.allocate(1));
    }

    int64_t usedMemory = allocationsPerThread * sizeof(TestClass);
    EXPECT_EQ(allocatorLocal.getCurrentLocalMemoryUsage(), allocationsPerThread * sizeof(TestClass));
    EXPECT_GE(usedMemory - allocatorLocal.getlastMemoryLimitCheckpoint(), 0LL);
    EXPECT_LE(allocatedMemory.load(), MemoryAllowance - allocatorLocal.getlastMemoryLimitCheckpoint());

    for (auto* ptr : ptrs)
    {
      allocatorLocal.deallocate(ptr, 1);
    }

    EXPECT_EQ(allocatorLocal.getCurrentLocalMemoryUsage(), 0);
    EXPECT_EQ(allocatorLocal.getlastMemoryLimitCheckpoint(), 0);
    EXPECT_GE(allocatedMemory.load(), allocationsPerThread * sizeof(TestClass));
  };

  std::vector<std::thread> threads;
  // Launch multiple threads performing allocations and deallocations
  for (std::size_t i = 0; i < numThreads; ++i)
  {
    threads.emplace_back(worker);
  }

  // Wait for all threads to finish
  for (auto& th : threads)
  {
    th.join();
  }

  // After all allocations and deallocations, the counter should be zero minus the remainder
  EXPECT_EQ(allocatedMemory.load(), MemoryAllowance);
}

// Test 6: Allocating zero objects should not change the counter
TEST_F(CountingAllocatorTest, AllocateZeroObjects)
{
  TestClass* ptr = allocator.allocate(0);
  EXPECT_NE(ptr, nullptr);
  EXPECT_EQ(allocatedMemory.load(), MemoryAllowance);
  allocator.deallocate(ptr, 0);
  EXPECT_EQ(allocatedMemory.load(), MemoryAllowance);
}

TEST_F(CountingAllocatorTest, CopyAssignable)
{
  CountingAllocator<TestClass> allocator1(&allocatedMemory);
  CountingAllocator<TestClass> allocator2(&allocatedMemory);
  allocator1 = allocator2;
  EXPECT_EQ(allocator1, allocator2);
}
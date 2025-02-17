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
#include <sys/types.h>
#include <atomic>
#include <cstddef>
#include <memory>
#include <thread>

#include "countingallocator.h"
#include "poolallocator.h"

using namespace allocators;
using namespace utils;

/**
 * Тест явного задания windowSize при создании:
 */
TEST(PoolAllocatorTest, CustomWindowSize)
{
  const unsigned CUSTOM_SIZE = 1024;
  PoolAllocator pa(CUSTOM_SIZE);
  EXPECT_EQ(pa.getWindowSize(), CUSTOM_SIZE);
  EXPECT_EQ(pa.getMemUsage(), 0ULL);
}

/**
 * Тест базового выделения небольшого блока памяти:
 *  - Выделяем блок меньше, чем windowSize.
 *  - Проверяем, что memUsage увеличился на размер выделенного блока.
 *  - Указатель не должен быть равен nullptr.
 */
TEST(PoolAllocatorTest, AllocateSmallBlock)
{
  PoolAllocator pa;
  uint64_t initialUsage = pa.getMemUsage();
  const uint64_t ALLOC_SIZE = 128;

  void* ptr = pa.allocate(ALLOC_SIZE);

  ASSERT_NE(ptr, nullptr);
  EXPECT_EQ(pa.getMemUsage(), initialUsage + ALLOC_SIZE);
}

/**
 * Тест выделения блока памяти больше, чем windowSize (Out-Of-Band - OOB):
 *  - Проверяем, что memUsage увеличился на нужное количество байт.
 *  - Указатель не nullptr.
 */
TEST(PoolAllocatorTest, AllocateOOBBlock)
{
  // Выбираем размер гарантированно больше, чем окно по умолчанию
  const uint64_t BIG_BLOCK_SIZE = PoolAllocator::DEFAULT_WINDOW_SIZE + 1024;
  PoolAllocator pa;
  uint64_t initialUsage = pa.getMemUsage();

  void* ptr = pa.allocate(BIG_BLOCK_SIZE);
  ASSERT_NE(ptr, nullptr);
  EXPECT_EQ(pa.getMemUsage(), initialUsage + BIG_BLOCK_SIZE);
}

/**
 * Тест деаллокации (deallocate) Out-Of-Band блока:
 *  - Убеждаемся, что после deallocate memUsage возвращается к исходному значению.
 */
TEST(PoolAllocatorTest, DeallocateOOBBlock)
{
  PoolAllocator pa;
  // Блок больше windowSize
  const uint64_t BIG_BLOCK_SIZE = pa.getWindowSize() + 1024;

  uint64_t initialUsage = pa.getMemUsage();
  void* ptr = pa.allocate(BIG_BLOCK_SIZE);
  ASSERT_NE(ptr, nullptr);
  EXPECT_EQ(pa.getMemUsage(), initialUsage + BIG_BLOCK_SIZE);

  pa.deallocate(ptr);
  EXPECT_EQ(pa.getMemUsage(), initialUsage);
}

/**
 * Тест деаллокации блока, который был выделен внутри "windowSize".
 * По текущей логике PoolAllocator::deallocate для "маленьких" блоков ничего не делает.
 * Основная проверка – что код не падает и не меняет memUsage.
 */
TEST(PoolAllocatorTest, DeallocateSmallBlock)
{
  PoolAllocator pa;
  const uint64_t ALLOC_SIZE = 128;

  uint64_t initialUsage = pa.getMemUsage();
  void* ptr = pa.allocate(ALLOC_SIZE);
  ASSERT_NE(ptr, nullptr);
  EXPECT_EQ(pa.getMemUsage(), initialUsage + ALLOC_SIZE);

  // Попытка деаллокации "маленького" блока – в текущей реализации
  // код его не возвращает в пул, следовательно memUsage не уменьшится.
  pa.deallocate(ptr);
  EXPECT_EQ(pa.getMemUsage(), initialUsage + ALLOC_SIZE);
}

/**
 * Тест полного освобождения памяти (deallocateAll):
 *  - Выделяем несколько блоков: и маленький, и большой.
 *  - После вызова deallocateAll всё должно освободиться, memUsage вернётся к 0.
 */
TEST(PoolAllocatorTest, DeallocateAll)
{
  PoolAllocator pa;
  // Блок в пределах windowSize
  const uint64_t SMALL_BLOCK = 256;
  // Блок Out-Of-Band
  const uint64_t LARGE_BLOCK = pa.getWindowSize() + 1024;

  pa.allocate(SMALL_BLOCK);
  pa.allocate(LARGE_BLOCK);
  // Убедимся, что memUsage > 0
  EXPECT_GT(pa.getMemUsage(), 0ULL);

  // Освобождаем всё
  pa.deallocateAll();
  EXPECT_EQ(pa.getMemUsage(), 0ULL);
}

/**
 * Тест копирующего оператора присваивания:
 *  - Проверяем, что параметры (allocSize, tmpSpace, useLock) копируются.
 *  - Однако выделенная память не копируется (т.к. после operator= вызывается deallocateAll).
 */
TEST(PoolAllocatorTest, AssignmentOperator)
{
  PoolAllocator pa1(2048, true, true);  // windowSize=2048, tmpSpace=true, useLock=true
  // Выделяем немного памяти
  pa1.allocate(100);
  pa1.allocate(200);

  EXPECT_EQ(pa1.getWindowSize(), 2048U);
  EXPECT_TRUE(pa1.getMemUsage() > 0);

  // С помощью оператора присваивания: pa2 = pa1
  PoolAllocator pa2;
  pa2 = pa1;  // После этого deallocateAll() вызывается внутри operator= (в нашем коде)

  // Проверяем скопированные поля:
  EXPECT_EQ(pa2.getWindowSize(), 2048U);
  // tmpSpace и useLock также должны совпасть
  // (В данном коде напрямую нет геттеров для них,
  //  но, если нужно, можете добавить соответствующие методы или рефлексировать код.)

  // Проверяем, что у pa2 memUsage == 0 после deallocateAll
  EXPECT_EQ(pa2.getMemUsage(), 0ULL);
  // А у pa1 осталась прежняя статистика использования памяти,
  // т.к. operator= сделал deallocateAll только внутри pa2.
  EXPECT_TRUE(pa1.getMemUsage() > 0);
}

TEST(PoolAllocatorTest, MultithreadedAllocationWithLock)
{
  PoolAllocator pa(PoolAllocator::DEFAULT_WINDOW_SIZE, false, true);
  // useLock = true

  const int THREAD_COUNT = 4;
  const uint64_t ALLOC_PER_THREAD = 1024;
  std::vector<std::thread> threads;

  // Стартовое значение
  uint64_t initialUsage = pa.getMemUsage();

  // Запускаем несколько потоков, каждый сделает небольшое кол-во аллокаций
  for (int i = 0; i < THREAD_COUNT; i++)
  {
    threads.emplace_back(
        [&pa]()
        {
          for (int j = 0; j < 10; j++)
          {
            pa.allocate(ALLOC_PER_THREAD);
          }
        });
  }

  for (auto& th : threads)
    th.join();

  uint64_t expected = initialUsage + THREAD_COUNT * 10ULL * ALLOC_PER_THREAD;
  EXPECT_GE(pa.getMemUsage(), expected);
}

static const constexpr int64_t MemoryAllowance = 1 * 1024 * 1024;

// Test Fixture for AtomicCounterAllocator
class PoolallocatorTest : public ::testing::Test
{
 protected:
  // Atomic counter to track allocated memory
  std::atomic<int64_t> allocatedMemory{MemoryAllowance};

  // Custom allocator instance
  CountingAllocator<PoolAllocatorBufType> allocator;

  // Constructor
  PoolallocatorTest()
   : allocatedMemory(MemoryAllowance)
   , allocator(&allocatedMemory, MemoryAllowance / 1000, MemoryAllowance / 100)
  {
  }

  // Destructor
  ~PoolallocatorTest() override = default;
};

// Тест для проверки учёта потребления памяти в PoolAllocator.
TEST_F(PoolallocatorTest, AllocationWithAccounting)
{
  int bufSize = 512;
  const unsigned CUSTOM_SIZE = 1024;
  PoolAllocator pa(allocator, CUSTOM_SIZE, false, true);
  EXPECT_EQ(pa.getWindowSize(), CUSTOM_SIZE);
  EXPECT_EQ(pa.getMemUsage(), 0ULL);
  auto* ptr = pa.allocate(bufSize);

  EXPECT_NE(ptr, nullptr);
  EXPECT_LE(allocatedMemory.load(), MemoryAllowance - bufSize);
  EXPECT_LE(allocatedMemory.load(), MemoryAllowance - CUSTOM_SIZE);
  pa.deallocate(ptr);
  // B/c this PoolAllocator frees memory only when it's destroyed.
  EXPECT_LE(allocatedMemory.load(), MemoryAllowance - bufSize);
  EXPECT_LE(allocatedMemory.load(), MemoryAllowance - CUSTOM_SIZE);

  bufSize = 64536;
  auto* ptr1 = pa.allocate(bufSize);

  EXPECT_NE(ptr1, nullptr);
  EXPECT_LE(allocatedMemory.load(), MemoryAllowance - bufSize);

  pa.deallocate(ptr1);
  EXPECT_LE(allocatedMemory.load(), MemoryAllowance - CUSTOM_SIZE);
  EXPECT_GE(allocatedMemory.load(), MemoryAllowance - bufSize);
}

TEST_F(PoolallocatorTest, MultithreadedAccountedAllocationWithLock)
{
  const unsigned CUSTOM_SIZE = 1024;
  PoolAllocator pa(allocator, CUSTOM_SIZE, false, true);

  const int THREAD_COUNT = 4;
  const uint64_t ALLOC_PER_THREAD = 1024;
  const uint64_t NUM_ALLOCS_PER_THREAD = 10;
  std::vector<std::thread> threads;

  // Стартовое значение
  uint64_t initialUsage = pa.getMemUsage();

  // Запускаем несколько потоков, каждый сделает небольшое кол-во аллокаций
  for (int i = 0; i < THREAD_COUNT; i++)
  {
    threads.emplace_back(
        [&pa]()
        {
          for (uint64_t j = 0; j < NUM_ALLOCS_PER_THREAD; j++)
          {
            pa.allocate(ALLOC_PER_THREAD);
          }
        });
  }

  for (auto& th : threads)
    th.join();

  uint64_t expected = initialUsage + THREAD_COUNT * 10ULL * ALLOC_PER_THREAD;
  EXPECT_GE(pa.getMemUsage(), expected);
  // 2 * CUSTOM_SIZE semantics is structs allocation overhead.
  EXPECT_GE(allocatedMemory.load(),
            MemoryAllowance - (THREAD_COUNT * ALLOC_PER_THREAD * NUM_ALLOCS_PER_THREAD) - 3 * CUSTOM_SIZE);
}
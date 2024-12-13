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
#include <limits>
#include <memory>
#include <atomic>
#include <cstddef>
#include <iostream>
#include <utility>

namespace allocators 
{

// const constexpr std::uint64_t CounterUpdateUnitSize = 4 * 1024 * 1024; 
const constexpr std::int64_t MemoryLimitLowerBound = 100 * 1024 * 1024;  // WIP

// Custom Allocator that tracks allocated memory using an atomic counter
template <typename T>
class CountingAllocator {
public:
    using value_type = T;

    // Constructor accepting a reference to an atomic counter
    explicit CountingAllocator(std::atomic<int64_t>* memoryLimit, const uint64_t lowerBound = MemoryLimitLowerBound) noexcept
        : memoryLimit_(memoryLimit), memoryLimitLowerBound(lowerBound) {}

    // Copy constructor (template to allow conversion between different types)
    template <typename U>
    CountingAllocator(const CountingAllocator<U>& other) noexcept
        : memoryLimit_(other.memoryLimit_),  memoryLimitLowerBound(other.memoryLimitLowerBound) {}

    // Allocate memory for n objects of type T
    template <typename U = T>
    typename std::enable_if<!std::is_array<U>::value, U*>::type
    allocate(std::size_t n) 
    {
        auto memCounted = memoryLimit_->fetch_sub(n * sizeof(T), std::memory_order_relaxed);
        if (memCounted < memoryLimitLowerBound) {
            memoryLimit_->fetch_add(n * sizeof(T), std::memory_order_relaxed);
            throw std::bad_alloc();
        }
        
        T* ptr = static_cast<T*>(::operator new(n * sizeof(T)));
        // std::cout << "[Allocate] " << n * sizeof(T) << " bytes at " << static_cast<void*>(ptr)
        //           << ". current timit: " << std::dec << memoryLimit_.load() << std::hex << " bytes.\n";
        // std::cout << std::dec;
        return ptr;
    }

       template <typename U = T>
    typename std::enable_if<std::is_array<U>::value, typename std::remove_extent<U>::type*>::type
    allocate(std::size_t n) 
    {
        auto memCounted = memoryLimit_->fetch_sub(n * sizeof(T), std::memory_order_relaxed);
        if (memCounted < memoryLimitLowerBound) {
            memoryLimit_->fetch_add(n * sizeof(T), std::memory_order_relaxed);
            throw std::bad_alloc();
        }
        
        T ptr = static_cast<T>(::operator new[](n));
        // std::cout << "[Allocate] " << n * sizeof(T) << " bytes at " << static_cast<void*>(ptr)
        //           << ". current timit: " << std::dec << memoryLimit_.load() << std::hex << " bytes.\n";
        return ptr;
    }

    // Deallocate memory for n objects of type T
    void deallocate(T* ptr, std::size_t n) noexcept 
    {
        ::operator delete(ptr);
        memoryLimit_->fetch_add(n * sizeof(T), std::memory_order_relaxed);
        // std::cout << "[Deallocate] " << n * sizeof(T) << " bytes from " << static_cast<void*>(ptr)
        //           << ". current timit: " << std::dec << memoryLimit_.load() << std::hex << " bytes.\n";
        // std::cout << std::dec;
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

private:
    std::atomic<int64_t>* memoryLimit_ = nullptr;
    int64_t memoryLimitLowerBound = 0;

    // Grant access to other instances of CountingAllocator with different types
    template <typename U>
    friend class CountingAllocator;
};

}  // namespace allocators
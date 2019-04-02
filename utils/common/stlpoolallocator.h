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

/* Makes PoolAllocator STL-compliant */
#pragma once

#include <memory>
#include <tbb/memory_pool.h>
#include <boost/shared_ptr.hpp>
#include "poolallocator.h"

#undef min
#undef max


namespace utils {

/* This is an STL-compliant wrapper for PoolAllocator + an optimization for containers
 * that aren't entirely node based (ex: vectors and hash tables)
 */
  template<class T>
  class STLPoolAllocator : public tbb::memory_pool_allocator<T> {
  public:
    using _super_t = tbb::memory_pool_allocator<T>;
    using value_type = T;
    using pool_t = tbb::memory_pool<tbb::scalable_allocator<value_type>>;
    using pointer = typename _super_t::pointer;
    using size_type = typename _super_t::size_type;

    static pool_t &get_pool() {
      static pool_t oPool;
      return oPool;
    }


    static STLPoolAllocator &get() {
      static std::unique_ptr<STLPoolAllocator> oAllocator(new STLPoolAllocator);
      return *oAllocator;
    }

    ~STLPoolAllocator() = default;

    STLPoolAllocator(STLPoolAllocator &&src) : _super_t(std::move(src)) {}

    STLPoolAllocator &operator=(STLPoolAllocator &&src) {
      return _super_t::operator=(std::move(src));
    }

    STLPoolAllocator(const STLPoolAllocator &) = delete;

    STLPoolAllocator &operator=(const STLPoolAllocator &) = delete;

    pointer allocate(size_type n, const void * /*hint*/ = 0) {
      count()++;
      return _super_t::allocate(n);
    }

    void deallocate(pointer p, size_type) {
      count()--;
      _super_t::deallocate(p);
    }


    size_t getMemUsage() const {
      return count().load() * sizeof(T);
    }


  private:
    static std::atomic<size_t>& count(){
      static std::atomic<size_t> oCount;
      return oCount;
    }

    STLPoolAllocator() : _super_t(get_pool()) {
      count().store(0);
    }
  };
}
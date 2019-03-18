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
//https://software.intel.com/en-us/blogs/2011/12/19/scalable-memory-pools-community-preview-feature
#define TBB_PREVIEW_MEMORY_POOL 1
#include <tbb/memory_pool.h>
#include <atomic>
#include <memory>
#include <boost/shared_ptr.hpp>
#include "poolallocator.h"

#undef min
#undef max

#ifndef STLPOOLALLOCATOR_H_
#define STLPOOLALLOCATOR_H_

namespace utils {

/* If using the pool allocator with a boost smart ptr, use an instance of this
as the deleter. */
  struct BoostPoolDeallocator {
    inline void operator()(void *ptr) {};
  };

//wrapper around tbb::memory_pool_allocator to keep track of live allocations
  template<typename _ty, typename P=tbb::interface6::internal::pool_base>
  struct STLPoolAllocator : tbb::memory_pool_allocator<_ty> {

    using _super_t    = tbb::memory_pool_allocator<_ty>;
    using pointer      = typename _super_t::pointer;
    using size_type    = typename _super_t::size_type;

    ~STLPoolAllocator() = default;

    explicit STLPoolAllocator(typename _super_t::pool_type &src) : _super_t{src} {
      _live_count.store(0);
    }

    STLPoolAllocator(const STLPoolAllocator &src) : _super_t{src} {
      _live_count.store(src._live_count.load());
    }

    template<typename U>
    STLPoolAllocator(const STLPoolAllocator<U, P> &src) : _super_t{src} {
      _live_count.store(src._live_count.load());
    }

    static STLPoolAllocator& get(){
      static tbb::memory_pool<tbb::scalable_allocator<_ty>> oPool;
      static STLPoolAllocator oAllocator(oPool);
      return oAllocator;
    }

    pointer allocate(size_type n, const void *hint = nullptr) {
      auto pRet = _super_t::allocate(n);
      _live_count.fetch_add(n);
      return pRet;
    }

    void deallocate(pointer p, size_type) {
      _super_t::deallocate(p, 1);
      --_live_count;
    }

    size_t getMemUsage() const { return _live_count.load() * sizeof(_ty);}

  protected:
    std::atomic<size_t> _live_count;

  };


}

#endif

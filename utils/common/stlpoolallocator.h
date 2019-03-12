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

#include <memory>
#include <boost/shared_ptr.hpp>
#include "concurrent_stack.hpp"

#undef min
#undef max

#ifndef STLPOOLALLOCATOR_H_
#define STLPOOLALLOCATOR_H_

namespace utils {

/* If using the pool allocator with a boost smart ptr, use an instance of this
as the deleter. */
  struct BoostPoolDeallocator
  {
    inline void operator()(void *ptr) { };
  };

/* This is an STL-compliant wrapper for PoolAllocator + an optimization for containers
* that aren't entirely node based (ex: vectors and hash tables)
*/
  template<class T>
  class STLPoolAllocator
  {
  public:

    using size_type = size_t ;
    using difference_type = ptrdiff_t ;
    using pointer = T *;
    using const_pointer = const T *;
    using reference = T& ;
    using const_reference = const T& ;
    using value_type = T;
    using stack_type = concurrent::stack<uint8_t*>;

    template<class U> struct rebind { typedef STLPoolAllocator<U> other; };

    ~STLPoolAllocator() {
      if (1==instances().fetch_add(1)){
        typename stack_type::value_type pItem;
        while (!instances().load() && stack().try_pop(pItem)){
          delete [] pItem;
        }
      }
    }

    STLPoolAllocator() throw() {  instances().fetch_add(1); }

    STLPoolAllocator(STLPoolAllocator&& ) {  instances().fetch_add(1); }

    template<class U> STLPoolAllocator(const STLPoolAllocator<U> &) {  instances().fetch_add(1); }

    STLPoolAllocator(uint32_t capacity) throw(){
      if (!instances().fetch_add(1)){
        for (auto i = capacity ; i ; --i){
          stack().push(new uint8_t[sizeof(T)]);
        }
      }
    }

    STLPoolAllocator& operator=(const STLPoolAllocator &) = default;

    const stack_type& get_stack() const { return stack(); };

    size_t getMemUsage() const { return sizeof(stack_type::value_type)  * stack().unsafe_count(); }


    pointer allocate(size_type, const void *hint = nullptr){
      typename stack_type::value_type pRet;
      if (!stack().try_pop(pRet)) pRet = new uint8_t[sizeof(T)];
      return reinterpret_cast<pointer>(pRet);
    }

    void deallocate(pointer p, size_type n){
      auto ptr = reinterpret_cast<typename stack_type::value_type>(p);
      stack().push(ptr);
    }

    void construct(pointer p, const T& val){
      new(reinterpret_cast<void*>(p)) T(val);
    }


    void destroy(pointer p){
      p->T::~T();
    }


  private:

    static stack_type& stack() {
      static stack_type _stack;
      return _stack;
    }
    static std::atomic_uint32_t& instances() {
      static std::atomic_uint32_t _instances;
      return _instances;
    }

  };




  template<typename T>
  bool operator==(const STLPoolAllocator<T> &, const STLPoolAllocator<T> &)
  {
    return true;
  }

  template<typename T>
  bool operator!=(const STLPoolAllocator<T> &, const STLPoolAllocator<T> &)
  {
    return false;
  }

}

#endif

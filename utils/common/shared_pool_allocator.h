/* Copyright (C) 2014 InfiniDB, Inc.zzzzzzzzzzz

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


/* Makes PoolAllocator STL-compliant */

#pragma once

#include <stdlib.h>
#include <algorithm>
#include <boost/weak_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/atomic.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lock_guard.hpp>


namespace utils {


  struct iAllocMemUse {
    virtual size_t getMemUsage() const = 0;
  };

  namespace _ {

    template <uint32_t _val> struct next_pow2 {
      static const uint32_t _val0 = _val - 1;
      static const uint32_t _val1 =  _val0 | (_val0 >> 1);
      static const uint32_t _val2 = _val1 | (_val1 >> 2);
      static const uint32_t _val3 = _val2 | (_val2 >> 4);
      static const uint32_t _val4 = _val3 | (_val3 >> 8);
      static const uint32_t _val5 = _val4 | (_val4 >> 16);
      static const uint32_t value = 1 + _val5;
    };

    template <size_t _len>
    union alloc_node
    {
      alloc_node * _next;
      uint8_t _buff[_len];
    };

    template <size_t> struct alloc_stack;
    
    template <> struct alloc_stack<0x8000000000000000>{
      void * pop(size_t){
        throw std::bad_alloc();
      }
      void push(void*, size_t){
        throw std::bad_alloc();
      }
      size_t getMemUsage() const{ return 0; }
      
      typedef boost::shared_ptr<alloc_stack> ptr;
    
      static ptr get() {
        static ptr oRet(new alloc_stack);
        return oRet;
      }
    };
    
    template <size_t _bufflen>
      struct alloc_stack : iAllocMemUse {
      typedef boost::shared_ptr<alloc_stack> ptr;
      typedef alloc_node <_bufflen> node_type;
      typedef alloc_stack<_bufflen * 2> next_stack_type;
      typedef boost::shared_ptr<next_stack_type> next_stack_ptr_type;
      static const size_t alloc_node_size = (sizeof(void*) > _bufflen ? sizeof(void*) : _bufflen);

        /** returns a shared_ptr<alloc_stack> but maintains a weak_ptr<alloc_stack>
         ** so consecutive callers will recieve the same instance until all callers are finished
         ***/
      static ptr get(size_t prealloc=0) {
        static boost::weak_ptr<alloc_stack> oStack;
        static boost::mutex mux;
        ptr oRet;
        bool bCreated = false;
        {
          boost::lock_guard<boost::mutex> oLock(mux);
          oRet = oStack.lock();
          if (!oRet) {
            oRet = ptr(new alloc_stack(prealloc));
            oStack = oRet;
            bCreated = true;
          }
        }       
        if (bCreated && prealloc) {
          boost::thread(prepopulate).detach();
        }
        return oRet;

      }

      void * pop(size_t len) {
        if (len > alloc_node_size) return next_stack()->pop(len);
        for (;;) {                  
          node_type * pRet = _root.load();
          if (!pRet) {
            void * pVoid = NULL;
            if (posix_memalign(&pVoid, sizeof(void*), alloc_node_size) || !pVoid) throw std::bad_alloc();
            pRet = reinterpret_cast<node_type*>(pVoid);
            ++_live_count;
            return pRet;
          }
          if (_root.compare_exchange_strong(pRet, pRet->_next)) return pRet;
        }
      }

      void push(void* pBuff, size_t len){
        if (len > alloc_node_size){ next_stack()->push(pBuff, len); return; }

        node_type * pNode = reinterpret_cast<node_type*>(pBuff);
        for (;;){
          pNode->_next = _root.load();
          if (_root.compare_exchange_strong(pNode->_next, pNode)) break;
        }
      }

      ~alloc_stack(){
        for (;;){
          node_type * pNode = _root.load();
          if (!pNode) return;
          if (!_root.compare_exchange_strong(pNode, pNode->_next)) continue;
          free(pNode);
        }
      }

			virtual size_t getMemUsage() const { return (_live_count.load() * alloc_node_size) + (_next_stack_ptr ? _next_stack_ptr->getMemUsage() : 0); }


    private:
      template <size_t> friend struct alloc_stack;
      next_stack_ptr_type _next_stack_ptr;
      alloc_stack(size_t prealloc = 0) : _prealloc(prealloc) {
        _root.store(NULL);
        _live_count.store(0);
      }
      alloc_stack(alloc_stack&){}
        
      next_stack_ptr_type& next_stack(){
        if(!_next_stack_ptr) _next_stack_ptr = next_stack_type::get();
        return _next_stack_ptr;
      }
      static void prepopulate(){
        ptr pThis = get();
        for (size_t i = 0; i < pThis->_prealloc; i += alloc_node_size) {
          void * pVoid = NULL;
          if (!posix_memalign(&pVoid, sizeof(void*), alloc_node_size) && pVoid) pThis->push(pVoid, alloc_node_size);
        }
      }
      boost::atomic <node_type*> _root;
			boost::atomic<size_t> _live_count;
      size_t _prealloc;
      
    };

  }


  
  template<class T>
    class shared_pool_allocator
    {
      typedef _::alloc_stack < _::next_pow2<sizeof(T)>::value > alloc_stack_type;
    public:
      typedef size_t size_type;
      typedef ptrdiff_t difference_type;
      typedef T *pointer;
      typedef const T *const_pointer;
      typedef T& reference;
      typedef const T& const_reference;
      typedef T value_type;
      template<class U> struct rebind { typedef shared_pool_allocator<U> other; };

      shared_pool_allocator() throw() : _stack(alloc_stack_type::get()) {}
      shared_pool_allocator(size_t prealloc) throw() : _stack(alloc_stack_type::get(prealloc)) {}
      shared_pool_allocator(const shared_pool_allocator & src) throw() : _stack(src._stack) {}
      template<class U> shared_pool_allocator(const shared_pool_allocator<U> & src) throw() : _stack(alloc_stack_type::get()) {}
      ~shared_pool_allocator() { }

      shared_pool_allocator<T>& operator=(const shared_pool_allocator<T> & src){
        _stack = src._stack;
        return *this;
      }

      pointer allocate(size_type size, const void *hint = 0) {
        void * pRet = _stack->pop((size * sizeof(T)));
        return reinterpret_cast<pointer>(pRet);
      }
      void deallocate(pointer p, size_type n) {
        _stack->push(p, (n * sizeof(T)));
      }
      size_type max_size() const throw() { return -1; }


      void construct(pointer p, const T& val){  new((void *)p) T(val); }
      void destroy(pointer p) { p->T::~T(); }

      iAllocMemUse * getPoolAllocator() const { return alloc_stack_type::get().get(); }
      template<class U> bool operator==(const shared_pool_allocator<U>&) const { return true; }
      template<class U> bool operator!=(const shared_pool_allocator<U>&) const { return false; }

  	private:
      typename alloc_stack_type::ptr _stack;

    };


}


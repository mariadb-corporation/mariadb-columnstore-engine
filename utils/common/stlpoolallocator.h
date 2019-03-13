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

namespace utils
{

/* If using the pool allocator with a boost smart ptr, use an instance of this
as the deleter. */
<<<<<<< HEAD
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
=======
struct BoostPoolDeallocator
{
    inline void operator()(void* ptr) { };
};

/* This is an STL-compliant wrapper for PoolAllocator + an optimization for containers
 * that aren't entirely node based (ex: vectors and hash tables)
 */
template<class T>
class STLPoolAllocator
{
public:
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;
    typedef T* pointer;
    typedef const T* const_pointer;
    typedef T& reference;
    typedef const T& const_reference;
    typedef T value_type;
    template<class U> struct rebind
    {
        typedef STLPoolAllocator<U> other;
    };

    STLPoolAllocator() throw();
    STLPoolAllocator(const STLPoolAllocator&) throw();
    STLPoolAllocator(uint32_t capacity) throw();
    template<class U> STLPoolAllocator(const STLPoolAllocator<U>&) throw();
    ~STLPoolAllocator();

    STLPoolAllocator<T>& operator=(const STLPoolAllocator<T>&);

    void usePoolAllocator(boost::shared_ptr<PoolAllocator> b);
    boost::shared_ptr<utils::PoolAllocator> getPoolAllocator();

    pointer allocate(size_type, const void* hint = 0);
    void deallocate(pointer p, size_type n);
    size_type max_size() const throw();
    inline uint64_t getMemUsage() const
    {
        return pa->getMemUsage();
    }

    void construct(pointer p, const T& val);
    void destroy(pointer p);

    static const uint32_t DEFAULT_SIZE = 4096 * sizeof(T);

    boost::shared_ptr<utils::PoolAllocator> pa;
};

template<class T>
STLPoolAllocator<T>::STLPoolAllocator() throw()
{
    pa.reset(new PoolAllocator(DEFAULT_SIZE));
}

template<class T>
STLPoolAllocator<T>::STLPoolAllocator(const STLPoolAllocator<T>& s) throw()
{
    pa = s.pa;
}

template<class T>
STLPoolAllocator<T>::STLPoolAllocator(uint32_t capacity) throw()
{
    pa.reset(new PoolAllocator(capacity));
}

template<class T>
template<class U>
STLPoolAllocator<T>::STLPoolAllocator(const STLPoolAllocator<U>& s) throw()
{
    pa = s.pa;
}
>>>>>>> 453850ee1a36bac96b99faa32c51541a631b1cc7

    STLPoolAllocator() throw() {  instances().fetch_add(1); }

<<<<<<< HEAD
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
=======
template<class T>
void STLPoolAllocator<T>::usePoolAllocator(boost::shared_ptr<PoolAllocator> p)
{
    pa = p;
}
template<class T>
boost::shared_ptr<utils::PoolAllocator> STLPoolAllocator<T>::getPoolAllocator()
{
    return pa;
}

template<class T>
typename STLPoolAllocator<T>::pointer
STLPoolAllocator<T>::allocate(typename STLPoolAllocator<T>::size_type s,
                              typename std::allocator<void>::const_pointer hint)
{
    return (pointer) pa->allocate(s * sizeof(T));
}

template<class T>
void STLPoolAllocator<T>::deallocate(typename STLPoolAllocator<T>::pointer p,
                                     typename STLPoolAllocator<T>::size_type n)
{
    pa->deallocate((void*) p);
}

template<class T>
typename STLPoolAllocator<T>::size_type STLPoolAllocator<T>::max_size() const throw()
{
    return std::numeric_limits<uint64_t>::max() / sizeof(T);
}

template<class T>
void STLPoolAllocator<T>::construct(typename STLPoolAllocator<T>::pointer p, const T& val)
{
    new ((void*)p) T(val);
}

template<class T>
void STLPoolAllocator<T>::destroy(typename STLPoolAllocator<T>::pointer p)
{
    p->T::~T();
}

template<class T>
STLPoolAllocator<T>& STLPoolAllocator<T>::operator=(const STLPoolAllocator<T>& c)
{
    pa = c.pa;
    return *this;
}

template<typename T>
bool operator==(const STLPoolAllocator<T>&, const STLPoolAllocator<T>&)
{
    return true;
}

template<typename T>
bool operator!=(const STLPoolAllocator<T>&, const STLPoolAllocator<T>&)
{
    return false;
}
>>>>>>> 453850ee1a36bac96b99faa32c51541a631b1cc7

}

#endif

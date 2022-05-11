/* Copyright (C) 2020 MariaDB Corporation

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

#include <cstddef>
#include <stdexcept>
#include <type_traits>


namespace utils
{
template <typename T, int SIZE = 64>
class VLArray
{
 public:
  VLArray(int sz) : sz(sz), stack_storage(NULL), dyn_storage(NULL), ptr(NULL)
  {
    if (sz > SIZE)
    {
      dyn_storage = new T[sz];
      ptr = dyn_storage;
    }
    else
    {
      stack_storage = new (stack) T[sz];
      ptr = stack_storage;
    }
  }

  VLArray(int sz, const T& initval) : VLArray(sz)
  {
    for (int i = 0; i < sz; ++i)
      ptr[i] = initval;
  }

  VLArray(const VLArray&) = delete;
  VLArray(VLArray&&) = delete;
  VLArray& operator=(const VLArray&) = delete;
  VLArray& operator=(VLArray&&) = delete;

  ~VLArray()
  {
    if (dyn_storage)
    {
      delete[] dyn_storage;
    }
    else
    {
      // we cannot use `delete [] stack_storage` here so call d-tors explicitly
      if (!std::is_trivially_destructible<T>::value)
      {
        for (int i = 0; i < sz; ++i)
          stack_storage[i].~T();
      }
    }
  }

  int size() const
  {
    return sz;
  }
  const T* data() const
  {
    return ptr;
  }
  T* data()
  {
    return ptr;
  }

  const T& operator[](int i) const
  {
    return ptr[i];
  }
  T& operator[](int i)
  {
    return ptr[i];
  }

  const T& at(int i) const
  {
    if (i >= sz)
    {
      throw std::out_of_range("index out of range: " + std::to_string(i) + " >= size " + std::to_string(sz));
    }
    return ptr[i];
  }

  T& at(int i)
  {
    if (i >= sz)
    {
      throw std::out_of_range("index out of range: " + std::to_string(i) + " >= size " + std::to_string(sz));
    }
    return ptr[i];
  }

  operator const T*() const
  {
    return ptr;
  }
  operator T*()
  {
    return ptr;
  }

 private:
  const int sz;
  alignas(T) char stack[SIZE * sizeof(T)];
  T* stack_storage;
  T* dyn_storage;
  T* ptr;
};

}  // namespace utils

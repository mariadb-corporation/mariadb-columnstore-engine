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

/** @file */

#pragma once

#include <unistd.h>
#include <stdint.h>
#include <sched.h>

/*
This is an attempt to wrap the differneces between Windows and Linux around atomic ops.
Boost has something in interprocess::ipcdetail, but it doesn't have 64-bit API's.
*/

namespace atomicops
{
// Returns the resulting, incremented value
template <typename T>
inline T atomicInc(volatile T* mem)
{
  return __sync_add_and_fetch(mem, 1);
}

// decrements, but returns the pre-decrement value
template <typename T>
inline T atomicDec(volatile T* mem)
{
  return __sync_fetch_and_add(mem, -1);
}

// Returns the resulting value (but doesn't need to yet)
template <typename T>
inline T atomicAdd(volatile T* mem, T val)
{
  return __sync_add_and_fetch(mem, val);
}

// Returns the resulting value
template <typename T>
inline T atomicSub(volatile T* mem, T val)
{
  return __sync_sub_and_fetch(mem, val);
}

// Implements a memory barrier
inline void atomicMb()
{
  __sync_synchronize();
}

// Returns true iff the CAS took place, that is
// if (*mem == comp) {
//   *mem = swap;
//   return true;
// } else {
//   return false;
// }
template <typename T>
inline bool atomicCAS(volatile T* mem, T comp, T swap)
{
  // If the current value of *mem is comp, then write swap into *comp. Return true if the comparison is
  // successful and swap was written.
  return __sync_bool_compare_and_swap(mem, comp, swap);
}

// implements a zero out of a variable
template <typename T>
inline void atomicZero(volatile T* mem)
{
  __sync_xor_and_fetch(mem, *mem);
}

// Implements a scheduler yield
inline void atomicYield()
{
  sched_yield();
}

}  // namespace atomicops

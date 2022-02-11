/*
   Copyright (C) 2020 MariaDB Corporation

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
   MA 02110-1301, USA.
*/
#ifndef MCS_INT128_H_INCLUDED
#define MCS_INT128_H_INCLUDED

#include <cfloat>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <string>
#include "mcs_float128.h"

// Inline asm has three argument lists: output, input and clobber list
#ifdef __aarch64__
#define MACRO_VALUE_PTR_128(dst, dst_restrictions, src, src_restrictions, clobb) \
  ::memcpy((dst), &(src), sizeof(int128_t));
#define MACRO_PTR_PTR_128(dst, dst_restrictions, src, src_restrictions, clobb) \
  ::memcpy((dst), (src), sizeof(int128_t));
#elif defined(__GNUC__) && (__GNUC___ > 7) || defined(__clang__)
#define MACRO_VALUE_PTR_128(dst, dst_restrictions, src, src_restrictions, clobb) \
  __asm__ volatile("movups %1,%0" : dst_restrictions(*(dst)) : src_restrictions((src)) : clobb);
#define MACRO_PTR_PTR_128(dst, dst_restrictions, src, src_restrictions, clobb) \
  ::memcpy((dst), (src), sizeof(int128_t));

#else
#define MACRO_VALUE_PTR_128(dst, dst_restrictions, src, src_restrictions, clobb) \
  __asm__ volatile("movups %1,%0" : dst_restrictions(*(dst)) : src_restrictions((src)) : clobb);
#define MACRO_PTR_PTR_128(dst, dst_restrictions, src, src_restrictions, clobb) \
  __asm__ volatile(                                                            \
      "movdqu %1,%%xmm0;"                                                      \
      "movups %%xmm0,%0;"                                                      \
      : dst_restrictions(*(dst))                                               \
      : src_restrictions(*(src))                                               \
      : "memory", clobb);
#endif

namespace datatypes
{
using int128_t = __int128;
using uint128_t = unsigned __int128;

//    Type traits
template <typename T>
struct is_allowed_numeric
{
  static const bool value = false;
};

template <>
struct is_allowed_numeric<int>
{
  static const bool value = true;
};

template <>
struct is_allowed_numeric<int128_t>
{
  static const bool value = true;
};

template <typename T>
struct is_int128_t
{
  static const bool value = false;
};

template <>
struct is_int128_t<int128_t>
{
  static const bool value = true;
};

template <typename T>
struct is_uint128_t
{
  static const bool value = false;
};

template <>
struct is_uint128_t<uint128_t>
{
  static const bool value = true;
};

inline int128_t abs(int128_t x)
{
  return (x >= 0) ? x : -x;
}

class TSInt128
{
 public:
  static constexpr uint8_t MAXLENGTH16BYTES = 42;
  static constexpr int128_t NullValue = int128_t(0x8000000000000000LL) << 64;
  static constexpr int128_t EmptyValue = (int128_t(0x8000000000000000LL) << 64) + 1;

  //  A variety of ctors for aligned and unaligned arguments
  TSInt128() : s128Value(0)
  {
  }

  // Copy ctor
  TSInt128(const TSInt128& other) : s128Value(other.s128Value)
  {
  }

  TSInt128& operator=(const TSInt128& other)
  {
    s128Value = other.s128Value;
    return *this;
  }

  //    aligned argument
  explicit TSInt128(const int128_t& x)
  {
    s128Value = x;
  }

  //    unaligned argument
  explicit TSInt128(const int128_t* x)
  {
    assignPtrPtr(&s128Value, x);
  }

  //    unaligned argument
  explicit TSInt128(const unsigned char* x)
  {
    assignPtrPtr(&s128Value, x);
  }

  //    Method returns max length of a string representation
  static constexpr uint8_t maxLength()
  {
    return TSInt128::MAXLENGTH16BYTES;
  }

  //    Checks if the value is NULL
  inline bool isNull() const
  {
    return s128Value == NullValue;
  }

  //    Checks if the value is Empty
  inline bool isEmpty() const
  {
    return s128Value == EmptyValue;
  }

  //     The method copies 16 bytes from one memory cell
  //     into another using memcpy or SIMD.
  //     memcpy in gcc >= 7 is replaced with SIMD instructions
  template <typename D, typename S>
  static inline void assignPtrPtr(D* dst, const S* src)
  {
    MACRO_PTR_PTR_128(dst, "=m", src, "m", "xmm0")
  }

  template <typename D>
  static inline void storeUnaligned(D* dst, const int128_t& src)
  {
    MACRO_VALUE_PTR_128(dst, "=m", src, "x", "memory")
  }

  // operators
  template <typename T, typename = std::enable_if<is_allowed_numeric<T>::value> >
  inline bool operator<(const T& x) const
  {
    return s128Value < static_cast<int128_t>(x);
  }

  template <typename T, typename = std::enable_if<is_allowed_numeric<T>::value> >
  inline bool operator==(const T& x) const
  {
    return s128Value == static_cast<int128_t>(x);
  }

  inline operator double() const
  {
    return toDouble();
  }

  inline double toDouble() const
  {
    return static_cast<double>(s128Value);
  }

  inline operator long double() const
  {
    return toLongDouble();
  }

  inline long double toLongDouble() const
  {
    return static_cast<long double>(s128Value);
  }

  inline operator int32_t() const
  {
    if (s128Value > static_cast<int128_t>(INT32_MAX))
      return INT32_MAX;
    if (s128Value < static_cast<int128_t>(INT32_MIN))
      return INT32_MIN;

    return static_cast<int32_t>(s128Value);
  }

  inline operator uint32_t() const
  {
    if (s128Value > static_cast<int128_t>(UINT32_MAX))
      return UINT32_MAX;
    if (s128Value < 0)
      return 0;

    return static_cast<uint32_t>(s128Value);
  }

  inline operator int64_t() const
  {
    if (s128Value > static_cast<int128_t>(INT64_MAX))
      return INT64_MAX;
    if (s128Value < static_cast<int128_t>(INT64_MIN))
      return INT64_MIN;

    return static_cast<int64_t>(s128Value);
  }

  inline operator uint64_t() const
  {
    if (s128Value > static_cast<int128_t>(UINT64_MAX))
      return UINT64_MAX;
    if (s128Value < 0)
      return 0;

    return static_cast<uint64_t>(s128Value);
  }

  inline operator TFloat128() const
  {
    return toTFloat128();
  }

  //    unaligned argument
  inline TSInt128& operator=(const int128_t* x)
  {
    assignPtrPtr(&s128Value, x);
    return *this;
  }

  inline TSInt128 operator%(const int64_t& rhs) const
  {
    return TSInt128(s128Value % rhs);
  }

  inline TSInt128 operator%(const int128_t& rhs) const
  {
    return TSInt128(s128Value % rhs);
  }

  inline TSInt128 operator*(const TSInt128& rhs) const
  {
    return TSInt128(s128Value * rhs.s128Value);
  }

  inline TSInt128 operator+(const TSInt128& rhs) const
  {
    return TSInt128(s128Value + rhs.s128Value);
  }

  inline bool operator>(const TSInt128& rhs) const
  {
    return s128Value > rhs.s128Value;
  }

  inline bool operator<(const TSInt128& rhs) const
  {
    return s128Value < rhs.s128Value;
  }

  inline bool operator!=(const TSInt128& rhs) const
  {
    return s128Value != rhs.getValue();
  }

  inline TFloat128 toTFloat128() const
  {
    return TFloat128(s128Value);
  }

  inline const int128_t& getValue() const
  {
    return s128Value;
  }

  //    print int128_t parts represented as PODs
  uint8_t printPodParts(char* buf, const int128_t& high, const int128_t& mid, const int128_t& low) const;

  //    writes integer part of dec into a buffer
  uint8_t writeIntPart(const int128_t& x, char* buf, const uint8_t buflen) const;

  //    string representation of TSInt128
  std::string toString() const;

  friend std::ostream& operator<<(std::ostream& os, const TSInt128& x);

  int128_t s128Value;
};  // end of class

}  // end of namespace datatypes

#endif  // MCS_TSINT128_H_INCLUDED
// vim:ts=2 sw=2:

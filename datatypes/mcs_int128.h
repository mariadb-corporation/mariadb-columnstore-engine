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

// Inline asm has three argument lists: output, input and clobber list
#if defined(__GNUC__) && (__GNUC___ > 7)
    #define MACRO_VALUE_PTR_128(dst, \
                                dst_restrictions, \
                                src, \
                                src_restrictions, \
                                clobb) \
        __asm__ volatile("movups %1,%0" \
            :dst_restrictions ( *(dst) ) \
            :src_restrictions ( (src) ) \
            :clobb \
        );
    #define MACRO_PTR_PTR_128(dst, \
                              dst_restrictions, \
                              src, \
                              src_restrictions, \
                              clobb) \
        ::memcpy((dst), (src), sizeof(int128_t));

#else
    #define MACRO_VALUE_PTR_128(dst, \
                                dst_restrictions, \
                                src, \
                                src_restrictions, \
                                clobb) \
        __asm__ volatile("movups %1,%0" \
            :dst_restrictions ( *(dst) ) \
            :src_restrictions ( (src) ) \
            :clobb \
        );
    #define MACRO_PTR_PTR_128(dst, \
                              dst_restrictions, \
                              src, \
                              src_restrictions, \
                              clobb) \
        __asm__ volatile("movdqu %1,%%xmm0;" \
                         "movups %%xmm0,%0;" \
            :dst_restrictions ( *(dst) ) \
            :src_restrictions ( *(src) ) \
            :"memory", clobb \
        );
#endif

namespace datatypes
{

using int128_t = __int128;


//    Type traits
template <typename T>
struct is_allowed_numeric {
  static const bool value = false;
};

template <>
struct is_allowed_numeric<int> {
  static const bool value = true;
};

template <>
struct is_allowed_numeric<int128_t> {
  static const bool value = true;
};

//     The method converts a __float128 s128Value to a double.
static inline double getDoubleFromFloat128(const __float128& value)
{
    if (value > static_cast<__float128>(DBL_MAX))
        return DBL_MAX;
    else if (value < -static_cast<__float128>(DBL_MAX))
        return -DBL_MAX;

    return static_cast<double>(value);
}


//     The method converts a __float128 value to a long double.
static inline long double getLongDoubleFromFloat128(const __float128& value)
{
    if (value > static_cast<__float128>(LDBL_MAX))
        return LDBL_MAX;
    else if (value < -static_cast<__float128>(LDBL_MAX))
        return -LDBL_MAX;

    return static_cast<long double>(value);
}


class TSInt128
{
  public:
    static constexpr uint8_t MAXLENGTH16BYTES = 42;
    static constexpr int128_t NullValue = int128_t(0x8000000000000000LL) << 64;
    static constexpr int128_t EmptyValue = (int128_t(0x8000000000000000LL) << 64) + 1;


    //  A variety of ctors for aligned and unaligned arguments
    TSInt128(): s128Value(0) { }

    // Copy ctor
    TSInt128(const TSInt128& other): s128Value(other.s128Value) { }

    //    aligned argument
    TSInt128(const int128_t& x) { s128Value = x; }

    //    unaligned argument
    TSInt128(const int128_t* x) { assignPtrPtr(&s128Value, x); }

    //    unaligned argument
    TSInt128(const unsigned char* x) { assignPtrPtr(&s128Value, x); }

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
    template<typename T,
             typename = std::enable_if<is_allowed_numeric<T>::value> >
    inline bool operator<(const T& x) const
    {
      return s128Value < static_cast<int128_t>(x);
    }

    template<typename T,
             typename = std::enable_if<is_allowed_numeric<T>::value> >
    inline bool operator==(const T& x) const
    {
      return s128Value == static_cast<int128_t>(x);
    }

    //    print int128_t parts represented as PODs
    uint8_t printPodParts(char* buf,
                          const int128_t& high,
                          const int128_t& mid,
                          const int128_t& low) const;

    //    writes integer part of dec into a buffer
    uint8_t writeIntPart(const int128_t& x,
                         char* buf,
                         const uint8_t buflen) const;

    //    string representation of TSInt128
    std::string toString() const;

    friend std::ostream& operator<<(std::ostream& os, const TSInt128& x);

    //     The method converts a wide decimal s128Value to a double.
    inline double getDoubleFromWideDecimal();

    //     The method converts a wide decimal s128Value to a long double.
    inline long double getLongDoubleFromWideDecimal();

    //     The method converts a wide decimal s128Value to an int64_t,
    //    saturating the s128Value if necessary.
    inline int64_t getInt64FromWideDecimal();

    //     The method converts a wide decimal s128Value to an uint32_t.
    inline uint32_t getUInt32FromWideDecimal();

    //     The method converts a wide decimal s128Value to an uint64_t.
    inline uint64_t getUInt64FromWideDecimal();

    //     The method converts a wide decimal s128Value to an int32_t.
    inline int32_t getInt32FromWideDecimal();

    int128_t s128Value;
  }; // end of class
    

} //end of namespace datatypes

#endif // MCS_TSINT128_H_INCLUDED
// vim:ts=2 sw=2:

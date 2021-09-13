/* Copyright (C) 2021 Mariadb Corporation.

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

#ifndef UTILS_SIMD_SSE_H
#define UTILS_SIMD_SSE_H

#include <cstdint>
#include <type_traits>

#ifdef __OPTIMIZE__
  #include <smmintrin.h>
  #include <emmintrin.h>
  #define MCS_FORCE_INLINE __attribute__((__always_inline__))
#else
  #define __OPTIMIZE__
  #include <smmintrin.h>
  #include <emmintrin.h>
  #undef __OPTIMIZE__
  #define MCS_FORCE_INLINE inline
#endif

namespace simd
{
  using vi128_t = __m128i;
  using msk128_t = uint16_t;
  struct vi128_wr
  {
    __m128i v;
  };
/*
  // Load value
  MCS_FORCE_INLINE vi128_t load8BitsValue(const char fill)
  {
    return _mm_set1_epi8(fill);
  }

  MCS_FORCE_INLINE vi128_t load16BitsValue(const int16_t fill)
  {
    return _mm_set1_epi16(fill);
  }

  MCS_FORCE_INLINE vi128_t load32BitsValue(const int32_t fill)
  {
    return _mm_set1_epi32(fill);
  }

  MCS_FORCE_INLINE vi128_t load64BitsValue(const int64_t fill)
  {
    return _mm_set1_epi32(fill);
  }

  // Load from
  MCS_FORCE_INLINE vi128_t load8BitsFrom(const uint8_t* from)
  {
    return _mm_loadu_si128 (reinterpret_cast<const vi128_t*>(from));
  }

  // Compare EQ
  MCS_FORCE_INLINE vi128_t cmpGt8Bits(vi128_t& x, vi128_t& y)
  {
    return _mm_cmpeq_epi8(x, y);
  }

  MCS_FORCE_INLINE vi128_t cmpGt16Bits(vi128_t& x, vi128_t& y)
  {
    return _mm_cmpeq_epi16(x, y);
  }

  MCS_FORCE_INLINE vi128_t cmpGt32Bits(vi128_t& x, vi128_t& y)
  {
    return _mm_cmpeq_epi32(x, y);
  }

  MCS_FORCE_INLINE vi128_t cmpGt64Bits(vi128_t& x, vi128_t& y)
  {
    return _mm_cmpeq_epi64(x, y);
  }

  // Compare GT
  MCS_FORCE_INLINE vi128_t cmpEq8Bits(vi128_t& x, vi128_t& y)
  {
    return _mm_cmpgt_epi8(x, y);
  }

  MCS_FORCE_INLINE vi128_t cmpEq16Bits(vi128_t& x, vi128_t& y)
  {
    return _mm_cmpgt_epi16(x, y);
  }

  MCS_FORCE_INLINE vi128_t cmpEq32Bits(vi128_t& x, vi128_t& y)
  {
    return _mm_cmpgt_epi32(x, y);
  }

  MCS_FORCE_INLINE vi128_t cmpEq64Bits(vi128_t& x, vi128_t& y)
  {
    return _mm_cmpgt_epi64(x, y);
  }

  // misc
  MCS_FORCE_INLINE uint16_t convertVectorToBitMask(vi128_t& vmask)
  {
    return _mm_movemask_epi8(vmask);
  }
*/
  template<typename T>
  class SimdFilterProcessor
  {
/*
     // Load value
    MCS_FORCE_INLINE T load8BitsValue(const char fill)
    {
      return _mm_set1_epi8(fill);
    }

    MCS_FORCE_INLINE T load16BitsValue(const int16_t fill)
    {
      return _mm_set1_epi16(fill);
    }

    MCS_FORCE_INLINE T load32BitsValue(const int32_t fill)
    {
      return _mm_set1_epi32(fill);
    }

    MCS_FORCE_INLINE T load64BitsValue(const int64_t fill)
    {
      return _mm_set1_epi32(fill);
    }

    // Load from
    MCS_FORCE_INLINE T load8BitsFrom(const uint8_t* from)
    {
      return _mm_loadu_si128 (reinterpret_cast<const T*>(from));
    }

    // Compare EQ
    MCS_FORCE_INLINE T cmpGt8Bits(T& x, T& y)
    {
      return _mm_cmpeq_epi8(x, y);
    }

    MCS_FORCE_INLINE T cmpGt16Bits(T& x, T& y)
    {
      return _mm_cmpeq_epi16(x, y);
    }

    MCS_FORCE_INLINE T cmpGt32Bits(T& x, T& y)
    {
      return _mm_cmpeq_epi32(x, y);
    }

    MCS_FORCE_INLINE T cmpGt64Bits(T& x, T& y)
    {
      return _mm_cmpeq_epi64(x, y);
    }

    // Compare GT
    MCS_FORCE_INLINE T cmpEq8Bits(T& x, T& y)
    {
      return _mm_cmpgt_epi8(x, y);
    }

    MCS_FORCE_INLINE T cmpEq16Bits(T& x, T& y)
    {
      return _mm_cmpgt_epi16(x, y);
    }

    MCS_FORCE_INLINE T cmpEq32Bits(T& x, T& y)
    {
      return _mm_cmpgt_epi32(x, y);
    }

    MCS_FORCE_INLINE T cmpEq64Bits(T& x, T& y)
    {
      return _mm_cmpgt_epi64(x, y);
    }

    // misc
    MCS_FORCE_INLINE uint16_t convertVectorToBitMask(T& vmask)
    {
      return _mm_movemask_epi8(vmask);
    }
*/
  };

  template<>
  class SimdFilterProcessor<vi128_wr>
  {
   public:
    constexpr static const uint16_t vecBitSize = 128U;
    // Load value
    MCS_FORCE_INLINE vi128_t load8BitsValue(const char fill)
    {
      return _mm_set1_epi8(fill);
    }

    MCS_FORCE_INLINE vi128_t load16BitsValue(const int16_t fill)
    {
      return _mm_set1_epi16(fill);
    }

    MCS_FORCE_INLINE vi128_t load32BitsValue(const int32_t fill)
    {
      return _mm_set1_epi32(fill);
    }

    MCS_FORCE_INLINE vi128_t load64BitsValue(const int64_t fill)
    {
      return _mm_set1_epi32(fill);
    }

    // Load from
    MCS_FORCE_INLINE vi128_t load8BitsFrom(const char* from)
    {
      return _mm_loadu_si128 (reinterpret_cast<const vi128_t*>(from));
    }

    // Compare EQ
    MCS_FORCE_INLINE vi128_t cmpGt8Bits(vi128_t& x, vi128_t& y)
    {
      return _mm_cmpeq_epi8(x, y);
    }

    MCS_FORCE_INLINE vi128_t cmpGt16Bits(vi128_t& x, vi128_t& y)
    {
      return _mm_cmpeq_epi16(x, y);
    }

    MCS_FORCE_INLINE vi128_t cmpGt32Bits(vi128_t& x, vi128_t& y)
    {
      return _mm_cmpeq_epi32(x, y);
    }

    MCS_FORCE_INLINE vi128_t cmpGt64Bits(vi128_t& x, vi128_t& y)
    {
      return _mm_cmpeq_epi64(x, y);
    }

    // Compare GT
    MCS_FORCE_INLINE vi128_t cmpEq8Bits(vi128_t& x, vi128_t& y)
    {
      return _mm_cmpgt_epi8(x, y);
    }

    MCS_FORCE_INLINE vi128_t cmpEq16Bits(vi128_t& x, vi128_t& y)
    {
      return _mm_cmpgt_epi16(x, y);
    }

    MCS_FORCE_INLINE vi128_t cmpEq32Bits(vi128_t& x, vi128_t& y)
    {
      return _mm_cmpgt_epi32(x, y);
    }

    MCS_FORCE_INLINE vi128_t cmpEq64Bits(vi128_t& x, vi128_t& y)
    {
      return _mm_cmpgt_epi64(x, y);
    }

    // permute
/* Available in AVX-512
    MCS_FORCE_INLINE vi128_t perm8Bits(vi128_t& x, vi128_t& idx)
    {
      return _mm_permutexvar_epi8(x, idx);
    }
*/
    // misc
    MCS_FORCE_INLINE uint16_t convertVectorToBitMask(vi128_t& vmask)
    {
      return _mm_movemask_epi8(vmask);
    }

    MCS_FORCE_INLINE vi128_t setToZero()
    {
      return _mm_setzero_si128();
    }

    // store
    MCS_FORCE_INLINE void storeWMask(vi128_t& x, vi128_t& vmask, char* dst)
    {
      _mm_maskmoveu_si128(x, vmask, dst);
    }

    MCS_FORCE_INLINE void store(char* dst, vi128_t& x)
    {
      _mm_storeu_si128(reinterpret_cast<vi128_t*>(dst), x);
    }
};

} // end of simd

#endif
// vim:ts=2 sw=2:

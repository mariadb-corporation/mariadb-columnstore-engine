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

#if defined(__x86_64__)

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

#include <mcs_datatype.h>

namespace simd
{
  using vi128_t = __m128i;
  using vi128f_t = __m128;
  using vi128d_t = __m128d;
  using int128_t = __int128;
  using MT = uint16_t;
  // These ugly wrappers are used to allow to use __m128* as template class parameter argument
  struct vi128_wr
  {
    __m128i v;
  };

  struct vi128f_wr
  {
    __m128 v;
  };

  struct vi128d_wr
  {
    __m128d v;
  };

  template<typename T>
  struct _IntegralToVectorWrapper
  {
      using type = T;
  };

  template<typename T>
  struct IntegralToVectorWrapper: _IntegralToVectorWrapper<T> { };

  template<>
  struct IntegralToVectorWrapper<int128_t>: _IntegralToVectorWrapper<vi128_wr> { };

  template<>
  struct IntegralToVectorWrapper<uint64_t>: _IntegralToVectorWrapper<vi128_wr> { };

  template<>
  struct IntegralToVectorWrapper<int64_t>: _IntegralToVectorWrapper<vi128_wr> { };

  template<>
  struct IntegralToVectorWrapper<uint32_t>: _IntegralToVectorWrapper<vi128_wr> { };

  template<>
  struct IntegralToVectorWrapper<int32_t>: _IntegralToVectorWrapper<vi128_wr> { };

  template<>
  struct IntegralToVectorWrapper<uint16_t>: _IntegralToVectorWrapper<vi128_wr> { };

  template<>
  struct IntegralToVectorWrapper<int16_t>: _IntegralToVectorWrapper<vi128_wr> { };

  template<>
  struct IntegralToVectorWrapper<uint8_t>: _IntegralToVectorWrapper<vi128_wr> { };

  template<>
  struct IntegralToVectorWrapper<int8_t>: _IntegralToVectorWrapper<vi128_wr> { };

  template<>
  struct IntegralToVectorWrapper<double>: _IntegralToVectorWrapper<vi128d_wr> { };

  template<>
  struct IntegralToVectorWrapper<float>: _IntegralToVectorWrapper<vi128f_wr> { };

  template <typename VT, typename T, typename ENABLE = void>
  class SimdFilterProcessor;

  template <typename VT, typename CHECK_T>
  class SimdFilterProcessor<VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, vi128_wr>::value && sizeof(CHECK_T) == 16>::type>
  {
   // This is a dummy class that is not currently used.
   public:
    constexpr static const uint16_t vecByteSize = 16U;
    constexpr static const uint16_t vecBitSize = 128U;
    using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
    using SIMD_WRAPPER_TYPE = simd::vi128_wr;
    using SIMD_TYPE = simd::vi128_t;
    // Load value
    MCS_FORCE_INLINE vi128_t loadValue(const T fill)
    {
      return _mm_loadu_si128(reinterpret_cast<const vi128_t*>(&fill));
    }

    // Load from
    MCS_FORCE_INLINE vi128_t loadFrom(const char* from)
    {
      return _mm_loadu_si128(reinterpret_cast<const vi128_t*>(from));
    }

    MCS_FORCE_INLINE MT cmpDummy(vi128_t& x, vi128_t& y)
    {
      return 0xFFFF;
    }
    // Compare
    MCS_FORCE_INLINE MT cmpEq(vi128_t& x, vi128_t& y)
    {
      return cmpDummy(x, y);
    }

    MCS_FORCE_INLINE MT cmpGe(vi128_t& x, vi128_t& y)
    {
      return cmpDummy(x, y);
    }

    MCS_FORCE_INLINE MT cmpGt(vi128_t& x, vi128_t& y)
    {
      return cmpDummy(x, y);
    }

    MCS_FORCE_INLINE MT cmpLt(vi128_t& x, vi128_t& y)
    {
      return cmpDummy(x, y);
    }

    MCS_FORCE_INLINE MT cmpLe(vi128_t& x, vi128_t& y)
    {
      return cmpDummy(x, y);
    }

    MCS_FORCE_INLINE MT cmpNe(vi128_t& x, vi128_t& y)
    {
      return cmpDummy(x, y);
    }

    MCS_FORCE_INLINE MT cmpAlwaysFalse(vi128_t& x, vi128_t& y)
    {
      return 0;
    }

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

  template <typename VT, typename T>
  class SimdFilterProcessor<VT, T,
    typename std::enable_if<std::is_same<VT, vi128d_wr>::value && std::is_same<T, double>::value>::type>
  {
   public:
    constexpr static const uint16_t vecByteSize = 16U;
    constexpr static const uint16_t vecBitSize = 128U;
    using SIMD_WRAPPER_TYPE = simd::vi128d_wr;
    using SIMD_TYPE = simd::vi128d_t;
    // Load value
    MCS_FORCE_INLINE SIMD_TYPE loadValue(const T fill)
    {
      return _mm_set1_pd(fill);
    }

    // Load from
    MCS_FORCE_INLINE SIMD_TYPE loadFrom(const char* from)
    {
      return _mm_loadu_pd(reinterpret_cast<const T*>(from));
    }

    // Compare
    MCS_FORCE_INLINE MT cmpEq(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return _mm_movemask_pd(_mm_cmpeq_pd(x, y));
    }

    MCS_FORCE_INLINE MT cmpGe(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return _mm_movemask_pd( _mm_cmpge_pd(x,y));
    }

    MCS_FORCE_INLINE MT cmpGt(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return _mm_movemask_pd(_mm_cmpgt_pd(x, y));
    }

    MCS_FORCE_INLINE MT cmpLe(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return _mm_movemask_pd(_mm_cmple_pd(x, y));
    }

    MCS_FORCE_INLINE MT cmpLt(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return _mm_movemask_pd(_mm_cmplt_pd(x, y));
    }

    MCS_FORCE_INLINE MT cmpNe(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return _mm_movemask_pd(_mm_cmpneq_pd(x, y));
    }

    MCS_FORCE_INLINE MT cmpAlwaysFalse(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return 0;
    }

    // misc
    MCS_FORCE_INLINE MT convertVectorToBitMask(SIMD_TYPE& vmask)
    {
      return _mm_movemask_pd(vmask);
    }

    MCS_FORCE_INLINE SIMD_TYPE setToZero()
    {
      return _mm_setzero_pd();
    }

    MCS_FORCE_INLINE void store(char* dst, SIMD_TYPE& x)
    {
      _mm_storeu_pd(reinterpret_cast<T*>(dst), x);
    }
  };

  template <typename VT, typename T>
  class SimdFilterProcessor<VT, T,
    typename std::enable_if<std::is_same<VT, vi128_wr>::value && std::is_same<T, float>::value>::type>
  {
   public:
    constexpr static const uint16_t vecByteSize = 16U;
    constexpr static const uint16_t vecBitSize = 128U;
    using SIMD_WRAPPER_TYPE = simd::vi128f_wr;
    using SIMD_TYPE = simd::vi128f_t;
    // Load value
    MCS_FORCE_INLINE SIMD_TYPE loadValue(const T fill)
    {
      return _mm_set1_ps(fill);
    }

    // Load from
    MCS_FORCE_INLINE SIMD_TYPE loadFrom(const char* from)
    {
      return _mm_loadu_ps(reinterpret_cast<const T*>(from));
    }

    // Compare
    MCS_FORCE_INLINE MT cmpEq(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return _mm_movemask_ps(_mm_cmpeq_ps(x, y));
    }

    MCS_FORCE_INLINE MT cmpGe(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return _mm_movemask_ps( _mm_cmpge_ps(x,y));
    }

    MCS_FORCE_INLINE MT cmpGt(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return _mm_movemask_ps(_mm_cmpgt_ps(x, y));
    }

    MCS_FORCE_INLINE MT cmpLe(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return _mm_movemask_ps(_mm_cmple_ps(x, y));
    }

    MCS_FORCE_INLINE MT cmpLt(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return _mm_movemask_ps(_mm_cmplt_ps(x, y));
    }

    MCS_FORCE_INLINE MT cmpNe(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return _mm_movemask_ps(_mm_cmpneq_ps(x, y));
    }

    MCS_FORCE_INLINE MT cmpAlwaysFalse(SIMD_TYPE& x, SIMD_TYPE& y)
    {
      return 0;
    }

    // misc
    MCS_FORCE_INLINE MT convertVectorToBitMask(SIMD_TYPE& vmask)
    {
      return _mm_movemask_ps(vmask);
    }

    MCS_FORCE_INLINE SIMD_TYPE setToZero()
    {
      return _mm_setzero_ps();
    }

    MCS_FORCE_INLINE void store(char* dst, SIMD_TYPE& x)
    {
      _mm_storeu_ps(reinterpret_cast<T*>(dst), x);
    }
  };

  template <typename VT, typename CHECK_T>
  class SimdFilterProcessor<VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, vi128_wr>::value &&
                            sizeof(CHECK_T) == 8 && !std::is_same<CHECK_T, double>::value>::type>
  {
   public:
    constexpr static const uint16_t vecByteSize = 16U;
    constexpr static const uint16_t vecBitSize = 128U;
    using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
    using SIMD_WRAPPER_TYPE = simd::vi128_wr;
    using SIMD_TYPE = simd::vi128_t;
    // Load value
    MCS_FORCE_INLINE vi128_t loadValue(const T fill)
    {
      return _mm_set_epi64x(fill, fill);
    }

    // Load from
    MCS_FORCE_INLINE vi128_t loadFrom(const char* from)
    {
      return _mm_loadu_si128(reinterpret_cast<const vi128_t*>(from));
    }

    // Compare
    MCS_FORCE_INLINE MT cmpGe(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_or_si128(_mm_cmpgt_epi64(x, y),_mm_cmpeq_epi64(x, y)));
    }

    MCS_FORCE_INLINE MT cmpGt(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmpgt_epi64(x, y));
    }

    MCS_FORCE_INLINE MT cmpEq(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmpeq_epi64(x, y));
    }

    MCS_FORCE_INLINE MT cmpLe(vi128_t& x, vi128_t& y)
    {
      return cmpGt(x, y) ^ 0xFFFF;
    }

    MCS_FORCE_INLINE MT cmpLt(vi128_t& x, vi128_t& y)
    {
      return cmpNe(x, y) ^ cmpGt(x, y);
    }

    MCS_FORCE_INLINE MT cmpNe(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmpeq_epi64(x, y)) ^ 0xFFFF;
    }

    MCS_FORCE_INLINE MT cmpAlwaysFalse(vi128_t& x, vi128_t& y)
    {
      return 0;
    }

    // misc
    MCS_FORCE_INLINE MT convertVectorToBitMask(vi128_t& vmask)
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

  template <typename VT, typename CHECK_T>
  class SimdFilterProcessor<VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, vi128_wr>::value && 
                            sizeof(CHECK_T) == 4 && !std::is_same<CHECK_T, float>::value>::type>
  {
   public:
    constexpr static const uint16_t vecByteSize = 16U;
    constexpr static const uint16_t vecBitSize = 128U;
    using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
    using SIMD_WRAPPER_TYPE = simd::vi128_wr;
    using SIMD_TYPE = simd::vi128_t;
    // Load value
    MCS_FORCE_INLINE vi128_t loadValue(const T fill)
    {
      return _mm_set1_epi32(fill);
    }

    // Load from
    MCS_FORCE_INLINE vi128_t loadFrom(const char* from)
    {
      return _mm_loadu_si128(reinterpret_cast<const vi128_t*>(from));
    }

    // Compare
    MCS_FORCE_INLINE MT cmpEq(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmpeq_epi32(x, y));
    }

    MCS_FORCE_INLINE MT cmpGe(vi128_t& x, vi128_t& y)
    {
      return cmpLt(x, y) ^ 0xFFFF;
    }

    MCS_FORCE_INLINE MT cmpGt(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmpgt_epi32(x, y));
    }

    MCS_FORCE_INLINE MT cmpLe(vi128_t& x, vi128_t& y)
    {
      return cmpGt(x, y) ^ 0xFFFF;
    }

    MCS_FORCE_INLINE MT cmpLt(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmplt_epi32(x, y));
    }

    MCS_FORCE_INLINE MT cmpNe(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmpeq_epi32(x, y)) ^ 0xFFFF;
    }

    MCS_FORCE_INLINE MT cmpAlwaysFalse(vi128_t& x, vi128_t& y)
    {
      return 0;
    }

    // misc
    MCS_FORCE_INLINE MT convertVectorToBitMask(vi128_t& vmask)
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

  template <typename VT, typename CHECK_T>
  class SimdFilterProcessor<VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, vi128_wr>::value && sizeof(CHECK_T) == 2>::type>
  {
   public:
    constexpr static const uint16_t vecByteSize = 16U;
    constexpr static const uint16_t vecBitSize = 128U;
    using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
    using SIMD_WRAPPER_TYPE = simd::vi128_wr;
    using SIMD_TYPE = simd::vi128_t;
    // Load value
    MCS_FORCE_INLINE vi128_t loadValue(const T fill)
    {
      return _mm_set1_epi16(fill);
    }

    // Load from
    MCS_FORCE_INLINE vi128_t loadFrom(const char* from)
    {
      return _mm_loadu_si128(reinterpret_cast<const vi128_t*>(from));
    }

    // Compare
    MCS_FORCE_INLINE MT cmpEq(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmpeq_epi16(x, y));
    }

    MCS_FORCE_INLINE MT cmpGe(vi128_t& x, vi128_t& y)
    {
      return cmpLt(x, y) ^ 0xFFFF;
    }

    MCS_FORCE_INLINE MT cmpGt(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmpgt_epi16(x, y));
    }

    MCS_FORCE_INLINE MT cmpLe(vi128_t& x, vi128_t& y)
    {
      return cmpGt(x, y) ^ 0xFFFF;
    }

    MCS_FORCE_INLINE MT cmpLt(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmplt_epi16(x, y));
    }

    MCS_FORCE_INLINE MT cmpNe(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmpeq_epi16(x, y)) ^ 0xFFFF;
    }

    MCS_FORCE_INLINE MT cmpAlwaysFalse(vi128_t& x, vi128_t& y)
    {
      return 0;
    }

    // misc
    MCS_FORCE_INLINE MT convertVectorToBitMask(vi128_t& vmask)
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

  template <typename VT, typename CHECK_T>
  class SimdFilterProcessor<VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, vi128_wr>::value && sizeof(CHECK_T) == 1>::type>
  {
   public:
    constexpr static const uint16_t vecByteSize = 16U;
    constexpr static const uint16_t vecBitSize = 128U;
    using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
    using SIMD_WRAPPER_TYPE = simd::vi128_wr;
    using SIMD_TYPE = simd::vi128_t;
    // Load value
    MCS_FORCE_INLINE vi128_t loadValue(const T fill)
    {
      return _mm_set1_epi8(fill);
    }

    // Load from
    MCS_FORCE_INLINE vi128_t loadFrom(const char* from)
    {
      return _mm_loadu_si128(reinterpret_cast<const vi128_t*>(from));
    }

    // Compare
    MCS_FORCE_INLINE MT cmpEq(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmpeq_epi8(x, y));
    }

    MCS_FORCE_INLINE MT cmpGe(vi128_t& x, vi128_t& y)
    {
      return cmpLt(x, y) ^ 0xFFFF;
    }

    MCS_FORCE_INLINE MT cmpGt(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmpgt_epi8(x, y));
    }

    MCS_FORCE_INLINE MT cmpLe(vi128_t& x, vi128_t& y)
    {
      return cmpGt(x, y) ^ 0xFFFF;
    }

    MCS_FORCE_INLINE MT cmpLt(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmplt_epi8(x, y));
    }

    MCS_FORCE_INLINE MT cmpNe(vi128_t& x, vi128_t& y)
    {
      return _mm_movemask_epi8(_mm_cmpeq_epi8(x, y)) ^ 0xFFFF;
    }

    MCS_FORCE_INLINE MT cmpAlwaysFalse(vi128_t& x, vi128_t& y)
    {
      return 0;
    }

    // permute
/* TODO Available in AVX-512
    MCS_FORCE_INLINE vi128_t perm8Bits(vi128_t& x, vi128_t& idx)
    {
      return _mm_permutexvar_epi8(x, idx);
    }
*/
    // misc
    MCS_FORCE_INLINE MT convertVectorToBitMask(vi128_t& vmask)
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

#endif // if defined(__x86_64__ )

#endif
// vim:ts=2 sw=2:

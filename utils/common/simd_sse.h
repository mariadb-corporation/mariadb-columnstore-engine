/* Copyright (C) 2021-2022 Mariadb Corporation.

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

// Column filtering is dispatched 4-way based on the column type,
// which defines implementation of comparison operations for the column values
enum ENUM_KIND
{
  KIND_DEFAULT,   // compared as signed integers
  KIND_UNSIGNED,  // compared as unsigned integers
  KIND_FLOAT,     // compared as floating-point numbers
  KIND_TEXT
};  // whitespace-trimmed and then compared as signed integers


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

template <typename T, ENUM_KIND KIND, typename ENABLE = void>
struct IntegralToSIMD;

template <typename T, ENUM_KIND KIND>
struct IntegralToSIMD<T, KIND,
                      typename std::enable_if<KIND == KIND_FLOAT && sizeof(double) == sizeof(T)>::type>
{
  using type = vi128d_wr;
};

template <typename T, ENUM_KIND KIND>
struct IntegralToSIMD<T, KIND,
                      typename std::enable_if<KIND == KIND_FLOAT && sizeof(float) == sizeof(T)>::type>
{
  using type = vi128f_wr;
};

template <typename T, ENUM_KIND KIND>
struct IntegralToSIMD<T, KIND, typename std::enable_if<KIND != KIND_FLOAT>::type>
{
  using type = vi128_wr;
};

template <typename T, ENUM_KIND KIND, typename ENABLE = void>
struct StorageToFiltering;

template <typename T, ENUM_KIND KIND>
struct StorageToFiltering<T, KIND,
                          typename std::enable_if<KIND == KIND_FLOAT && sizeof(double) == sizeof(T)>::type>
{
  using type = double;
};

template <typename T, ENUM_KIND KIND>
struct StorageToFiltering<T, KIND,
                          typename std::enable_if<KIND == KIND_FLOAT && sizeof(float) == sizeof(T)>::type>
{
  using type = float;
};

template <typename T, ENUM_KIND KIND>
struct StorageToFiltering<T, KIND, typename std::enable_if<KIND != KIND_FLOAT>::type>
{
  using type = T;
};

template <int i0, int i1, int i2, int i3>
static inline vi128_t constant4i() {
    static const union {
        int     i[4];
        vi128_t xmm;
    } u = {{i0,i1,i2,i3}};
    return u.xmm;
}

template <typename VT, typename T, typename ENABLE = void>
class SimdFilterProcessor;

// Dummy class that captures all impossible cases, e.g. integer vector as VT and flot as CHECK_T.
template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T,
    typename std::enable_if<(std::is_same<VT, vi128_wr>::value && sizeof(CHECK_T) == 16) ||
                            (std::is_same<VT, vi128f_wr>::value && !std::is_same<CHECK_T, float>::value &&
                             !std::is_same<CHECK_T, double>::value)>::type>
{
  // This is a dummy class that is not currently used.
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = vi128_wr;
  using SimdType = vi128_t;
  using FilterType = T;
  using StorageType = T;
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return _mm_loadu_si128(reinterpret_cast<const SimdType*>(&fill));
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return _mm_loadu_si128(reinterpret_cast<const SimdType*>(from));
  }

  MCS_FORCE_INLINE MT cmpDummy(SimdType& x, SimdType& y)
  {
    return 0xFFFF;
  }
  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType& x, SimdType& y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType& x, SimdType& y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType& x, SimdType& y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType& x, SimdType& y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType& x, SimdType& y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType& x, SimdType& y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType& x, SimdType& y)
  {
    return 0;
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType& x, SimdType& y)
  {
    return 0xFFFF;
  }

  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType& vmask)
  {
    return _mm_movemask_epi8(vmask);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType& x, SimdType& y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType& x, SimdType& y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return _mm_setzero_si128();
  }

  // store
  MCS_FORCE_INLINE void storeWMask(SimdType& x, SimdType& vmask, char* dst)
  {
    _mm_maskmoveu_si128(x, vmask, dst);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType& x)
  {
    _mm_storeu_si128(reinterpret_cast<SimdType*>(dst), x);
  }

  MCS_FORCE_INLINE SimdType min(SimdType& x, SimdType& y)
  {
    return reinterpret_cast<SimdType>(std::min(reinterpret_cast<int128_t>(x), reinterpret_cast<int128_t>(y)));
  }

  MCS_FORCE_INLINE SimdType max(SimdType& x, SimdType& y)
  {
    return reinterpret_cast<SimdType>(std::max(reinterpret_cast<int128_t>(x), reinterpret_cast<int128_t>(y)));
  }


};

template <typename VT, typename T>
class SimdFilterProcessor<
    VT, T,
    typename std::enable_if<std::is_same<VT, vi128d_wr>::value && std::is_same<T, double>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using FilterType = T;
  using NullEmptySimdType = vi128_t;
  using SimdWrapperType = simd::vi128d_wr;
  using SimdType = simd::vi128d_t;
  using StorageSimdType = simd::vi128_t;
  using StorageType = typename datatypes::WidthToSIntegralType<sizeof(T)>::type;
  using StorageVecProcType = SimdFilterProcessor<simd::vi128_wr, StorageType>;
  // Mask calculation for int and float types differs.
  // See corresponding intrinsics algos for details.
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    StorageVecProcType nullEmptyProcessor;
    // This spec borrows the expr from u-/int64 based proceesor class.
    return (SimdType)nullEmptyProcessor.loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return _mm_set1_pd(fill);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return _mm_loadu_pd(reinterpret_cast<const T*>(from));
  }

  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8((StorageSimdType)_mm_cmpeq_pd(x, y));
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8((StorageSimdType)_mm_cmpge_pd(x, y));
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8((StorageSimdType)_mm_cmpgt_pd(x, y));
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8((StorageSimdType)_mm_cmple_pd(x, y));
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8((StorageSimdType)_mm_cmplt_pd(x, y));
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8((StorageSimdType)_mm_cmpneq_pd(x, y));
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType& x, SimdType& y)
  {
    return 0;
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType& x, SimdType& y)
  {
    return 0xFFFF;
  }

  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType& vmask)
  {
    return _mm_movemask_pd(vmask);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType& x, SimdType& y)
  {
    StorageVecProcType nullEmptyProcessor;
    NullEmptySimdType* xAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&x);
    NullEmptySimdType* yAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&y);
    // This spec borrows the expr from u-/int64 based proceesor class.
    return nullEmptyProcessor.cmpNe(*xAsIntVecPtr, *yAsIntVecPtr);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType& x, SimdType& y)
  {
    StorageVecProcType nullEmptyProcessor;

    NullEmptySimdType* xAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&x);
    NullEmptySimdType* yAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&y);
    // This spec borrows the expr from u-/int64 based proceesor class.
    return nullEmptyProcessor.cmpEq(*xAsIntVecPtr, *yAsIntVecPtr);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return _mm_setzero_pd();
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType& x)
  {
    _mm_storeu_pd(reinterpret_cast<T*>(dst), x);
  }

  MCS_FORCE_INLINE SimdType min(SimdType& x, SimdType& y)
  {
    return _mm_min_pd(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType& x, SimdType& y)
  {
    return _mm_max_pd(x, y);
  }
};

template <typename VT, typename T>
class SimdFilterProcessor<
    VT, T, typename std::enable_if<std::is_same<VT, vi128f_wr>::value && std::is_same<T, float>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using FilterType = T;
  using NullEmptySimdType = vi128_t;
  using SimdWrapperType = vi128f_wr;
  using SimdType = vi128f_t;
  using StorageSimdType = simd::vi128_t;
  using StorageType = typename datatypes::WidthToSIntegralType<sizeof(T)>::type;
  using StorageVecProcType = SimdFilterProcessor<simd::vi128_wr, StorageType>;
  // Mask calculation for int and float types differs.
  // See corresponding intrinsics algos for details.
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    StorageVecProcType nullEmptyProcessor;
    // This spec borrows the expr from u-/int64 based proceesor class.
    return (SimdType)nullEmptyProcessor.loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return _mm_set1_ps(fill);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return _mm_loadu_ps(reinterpret_cast<const T*>(from));
  }

  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8((StorageSimdType)_mm_cmpeq_ps(x, y));
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8((StorageSimdType)_mm_cmpge_ps(x, y));
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8((StorageSimdType)_mm_cmpgt_ps(x, y));
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8((StorageSimdType)_mm_cmple_ps(x, y));
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8((StorageSimdType)_mm_cmplt_ps(x, y));
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8((StorageSimdType)_mm_cmpneq_ps(x, y));
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType& x, SimdType& y)
  {
    return 0;
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType& x, SimdType& y)
  {
    return 0xFFFF;
  }

  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType& vmask)
  {
    return _mm_movemask_ps(vmask);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType& x, SimdType& y)
  {
    StorageVecProcType nullEmptyProcessor;

    NullEmptySimdType* xAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&x);
    NullEmptySimdType* yAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&y);
    // This spec borrows the expr from u-/int64 based proceesor class.
    return nullEmptyProcessor.cmpNe(*xAsIntVecPtr, *yAsIntVecPtr);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType& x, SimdType& y)
  {
    StorageVecProcType nullEmptyProcessor;

    NullEmptySimdType* xAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&x);
    NullEmptySimdType* yAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&y);
    // This spec borrows the expr from u-/int64 based proceesor class.
    return nullEmptyProcessor.cmpEq(*xAsIntVecPtr, *yAsIntVecPtr);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return _mm_setzero_ps();
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType& x)
  {
    _mm_storeu_ps(reinterpret_cast<T*>(dst), x);
  }

  MCS_FORCE_INLINE SimdType min(SimdType& x, SimdType& y)
  {
    return _mm_min_ps(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType& x, SimdType& y)
  {
    return _mm_max_ps(x, y);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<VT, CHECK_T,
                          typename std::enable_if<std::is_same<VT, vi128_wr>::value && std::is_same<CHECK_T, int64_t>::value &&
                                                  !std::is_same<CHECK_T, double>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = vi128_wr;
  using SimdType = vi128_t;
  using FilterType = T;
  using StorageType = T;
  // Mask calculation for int and float types differs.
  // See corresponding intrinsics algos for details.
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return _mm_set_epi64x(fill, fill);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return _mm_loadu_si128(reinterpret_cast<const SimdType*>(from));
  }

  // Compare
  MCS_FORCE_INLINE MT cmpGe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_or_si128(_mm_cmpgt_epi64(x, y), _mm_cmpeq_epi64(x, y)));
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpgt_epi64(x, y));
  }

  MCS_FORCE_INLINE MT cmpEq(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi64(x, y));
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType& x, SimdType& y)
  {
    return cmpGt(x, y) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType& x, SimdType& y)
  {
    return cmpNe(x, y) ^ cmpGt(x, y);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi64(x, y)) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType& x, SimdType& y)
  {
    return 0;
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType& x, SimdType& y)
  {
    return 0xFFFF;
  }

  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType& vmask)
  {
    return _mm_movemask_epi8(vmask);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return _mm_setzero_si128();
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType& x, SimdType& y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType& x, SimdType& y)
  {
    return cmpEq(x, y);
  }

  // store
  MCS_FORCE_INLINE void storeWMask(SimdType& x, SimdType& vmask, char* dst)
  {
    _mm_maskmoveu_si128(x, vmask, dst);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType& x)
  {
    _mm_storeu_si128(reinterpret_cast<SimdType*>(dst), x);
  }

  MCS_FORCE_INLINE SimdType min(SimdType& x, SimdType& y)
  {
    return _mm_blendv_epi8(x, y, _mm_cmpgt_epi64(x,y));
  }

  MCS_FORCE_INLINE SimdType max(SimdType& x, SimdType& y)
  {
    return _mm_blendv_epi8(x, y, _mm_cmpgt_epi64(y,x));
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<VT, CHECK_T,
                          typename std::enable_if<std::is_same<VT, vi128_wr>::value && std::is_same<CHECK_T, uint64_t>::value &&
                                                  !std::is_same<CHECK_T, double>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = vi128_wr;
  using SimdType = vi128_t;
  using FilterType = T;
  using StorageType = T;
  // Mask calculation for int and float types differs.
  // See corresponding intrinsics algos for details.
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return _mm_set_epi64x(fill, fill);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return _mm_loadu_si128(reinterpret_cast<const SimdType*>(from));
  }

  // Compare
  MCS_FORCE_INLINE MT cmpGe(SimdType& x, SimdType& y)
  {
    return cmpGt(y, x) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType& x, SimdType& y)
  {
    SimdType signVec = constant4i<0,(int32_t)0x80000000,0,(int32_t)0x80000000>();
    SimdType xFlip = _mm_xor_si128(x, signVec);
    SimdType yFlip = _mm_xor_si128(y, signVec);
    return _mm_movemask_epi8(_mm_cmpgt_epi64(xFlip, yFlip));
  }

  MCS_FORCE_INLINE MT cmpEq(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi64(x, y));
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType& x, SimdType& y)
  {
    return cmpGt(x, y) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType& x, SimdType& y)
  {
    return cmpGt(y, x);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi64(x, y)) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType& x, SimdType& y)
  {
    return 0;
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType& x, SimdType& y)
  {
    return 0xFFFF;
  }

  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType& vmask)
  {
    return _mm_movemask_epi8(vmask);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return _mm_setzero_si128();
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType& x, SimdType& y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType& x, SimdType& y)
  {
    return cmpEq(x, y);
  }

  // store
  MCS_FORCE_INLINE void storeWMask(SimdType& x, SimdType& vmask, char* dst)
  {
    _mm_maskmoveu_si128(x, vmask, dst);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType& x)
  {
    _mm_storeu_si128(reinterpret_cast<SimdType*>(dst), x);
  }

  MCS_FORCE_INLINE SimdType min(SimdType& x, SimdType& y)
  {
    return _mm_blendv_epi8(x, y, _mm_cmpgt_epi64(x,y));
  }

  MCS_FORCE_INLINE SimdType max(SimdType& x, SimdType& y)
  {
    return _mm_blendv_epi8(x, y, _mm_cmpgt_epi64(y,x));
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<VT, CHECK_T,
                          typename std::enable_if<std::is_same<VT, vi128_wr>::value && std::is_same<CHECK_T, int32_t>::value &&
                                                  !std::is_same<CHECK_T, float>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = vi128_wr;
  using SimdType = vi128_t;
  using FilterType = T;
  using StorageType = T;
  // Mask calculation for int and float types differs.
  // See corresponding intrinsics algos for details.
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return _mm_set1_epi32(fill);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return _mm_loadu_si128(reinterpret_cast<const SimdType*>(from));
  }

  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi32(x, y));
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType& x, SimdType& y)
  {
    return cmpLt(x, y) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpgt_epi32(x, y));
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType& x, SimdType& y)
  {
    return cmpGt(x, y) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmplt_epi32(x, y));
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi32(x, y)) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType& x, SimdType& y)
  {
    return 0;
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType& x, SimdType& y)
  {
    return 0xFFFF;
  }

  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType& vmask)
  {
    return _mm_movemask_epi8(vmask);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType& x, SimdType& y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType& x, SimdType& y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return _mm_setzero_si128();
  }

  // store
  MCS_FORCE_INLINE void storeWMask(SimdType& x, SimdType& vmask, char* dst)
  {
    _mm_maskmoveu_si128(x, vmask, dst);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType& x)
  {
    _mm_storeu_si128(reinterpret_cast<SimdType*>(dst), x);
  }

  MCS_FORCE_INLINE SimdType min(SimdType& x, SimdType& y)
  {
    return _mm_min_epi32(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType& x, SimdType& y)
  {
    return _mm_max_epi32(x, y);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<VT, CHECK_T,
                          typename std::enable_if<std::is_same<VT, vi128_wr>::value && std::is_same<CHECK_T, uint32_t>::value &&
                                                  !std::is_same<CHECK_T, float>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = vi128_wr;
  using SimdType = vi128_t;
  using FilterType = T;
  using StorageType = T;
  // Mask calculation for int and float types differs.
  // See corresponding intrinsics algos for details.
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return _mm_set1_epi32(fill);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return _mm_loadu_si128(reinterpret_cast<const SimdType*>(from));
  }

  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi32(x, y));
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType& x, SimdType& y)
  {
    return cmpGt(y, x) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType& x, SimdType& y)
  {
    SimdType signVec = constant4i<(int32_t)0x80000000,(int32_t)0x80000000,(int32_t)0x80000000,(int32_t)0x80000000>();
    SimdType xFlip = _mm_xor_si128(x, signVec);
    SimdType yFlip = _mm_xor_si128(y, signVec);
    return _mm_movemask_epi8(_mm_cmpgt_epi32(xFlip, yFlip));
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType& x, SimdType& y)
  {
    return cmpGt(x, y) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType& x, SimdType& y)
  {
    return cmpGt(y, x);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi32(x, y)) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType& x, SimdType& y)
  {
    return 0;
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType& x, SimdType& y)
  {
    return 0xFFFF;
  }

  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType& vmask)
  {
    return _mm_movemask_epi8(vmask);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType& x, SimdType& y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType& x, SimdType& y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return _mm_setzero_si128();
  }

  // store
  MCS_FORCE_INLINE void storeWMask(SimdType& x, SimdType& vmask, char* dst)
  {
    _mm_maskmoveu_si128(x, vmask, dst);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType& x)
  {
    _mm_storeu_si128(reinterpret_cast<SimdType*>(dst), x);
  }

  MCS_FORCE_INLINE SimdType min(SimdType& x, SimdType& y)
  {
    return _mm_min_epu32(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType& x, SimdType& y)
  {
    return _mm_max_epu32(x, y);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T, typename std::enable_if<std::is_same<VT, vi128_wr>::value && std::is_same<CHECK_T, int16_t>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = simd::vi128_wr;
  using SimdType = simd::vi128_t;
  using FilterType = T;
  using StorageType = T;
  // Mask calculation for int and float types differs.
  // See corresponding intrinsics algos for details.
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return _mm_set1_epi16(fill);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return _mm_loadu_si128(reinterpret_cast<const SimdType*>(from));
  }

  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi16(x, y));
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType& x, SimdType& y)
  {
    return cmpLt(x, y) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpgt_epi16(x, y));
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType& x, SimdType& y)
  {
    return cmpGt(x, y) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmplt_epi16(x, y));
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi16(x, y)) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType& x, SimdType& y)
  {
    return 0;
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType& x, SimdType& y)
  {
    return 0xFFFF;
  }

  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType& vmask)
  {
    return _mm_movemask_epi8(vmask);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType& x, SimdType& y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType& x, SimdType& y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return _mm_setzero_si128();
  }

  // store
  MCS_FORCE_INLINE void storeWMask(SimdType& x, SimdType& vmask, char* dst)
  {
    _mm_maskmoveu_si128(x, vmask, dst);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType& x)
  {
    _mm_storeu_si128(reinterpret_cast<SimdType*>(dst), x);
  }

  MCS_FORCE_INLINE SimdType min(SimdType& x, SimdType& y)
  {
    return _mm_min_epi16(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType& x, SimdType& y)
  {
    return _mm_max_epi16(x, y);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T, typename std::enable_if<std::is_same<VT, vi128_wr>::value && std::is_same<CHECK_T, uint16_t>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = simd::vi128_wr;
  using SimdType = simd::vi128_t;
  using FilterType = T;
  using StorageType = T;
  // Mask calculation for int and float types differs.
  // See corresponding intrinsics algos for details.
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return _mm_set1_epi16(fill);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return _mm_loadu_si128(reinterpret_cast<const SimdType*>(from));
  }

  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi16(x, y));
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType& x, SimdType& y)
  {
    SimdType maxOfTwo = _mm_max_epu16(x, y); // max(x, y), unsigned
    return _mm_movemask_epi8(_mm_cmpeq_epi16(x, maxOfTwo));
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType& x, SimdType& y)
  {
    return cmpGe(y, x) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType& x, SimdType& y)
  {
    return cmpGe(y, x);
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType& x, SimdType& y)
  {
    return cmpGe(x, y) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi16(x, y)) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType& x, SimdType& y)
  {
    return 0;
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType& x, SimdType& y)
  {
    return 0xFFFF;
  }

  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType& vmask)
  {
    return _mm_movemask_epi8(vmask);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType& x, SimdType& y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType& x, SimdType& y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return _mm_setzero_si128();
  }

  // store
  MCS_FORCE_INLINE void storeWMask(SimdType& x, SimdType& vmask, char* dst)
  {
    _mm_maskmoveu_si128(x, vmask, dst);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType& x)
  {
    _mm_storeu_si128(reinterpret_cast<SimdType*>(dst), x);
  }

  MCS_FORCE_INLINE SimdType min(SimdType& x, SimdType& y)
  {
    return _mm_min_epu16(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType& x, SimdType& y)
  {
    return _mm_max_epu16(x, y);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T, typename std::enable_if<std::is_same<VT, vi128_wr>::value && std::is_same<CHECK_T, int8_t>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = vi128_wr;
  using SimdType = vi128_t;
  using FilterType = T;
  using StorageType = T;
  // Mask calculation for int and float types differs.
  // See corresponding intrinsics algos for details.
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return _mm_set1_epi8(fill);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return _mm_loadu_si128(reinterpret_cast<const SimdType*>(from));
  }

  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi8(x, y));
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType& x, SimdType& y)
  {
    return cmpLt(x, y) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpgt_epi8(x, y));
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType& x, SimdType& y)
  {
    return cmpGt(x, y) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmplt_epi8(x, y));
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi8(x, y)) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType& x, SimdType& y)
  {
    return 0;
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType& x, SimdType& y)
  {
    return 0xFFFF;
  }

  // permute
  /* TODO Available in AVX-512
      MCS_FORCE_INLINE SimdType perm8Bits(SimdType& x, SimdType& idx)
      {
        return _mm_permutexvar_epi8(x, idx);
      }
  */
  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType& vmask)
  {
    return _mm_movemask_epi8(vmask);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType& x, SimdType& y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType& x, SimdType& y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return _mm_setzero_si128();
  }

  // store
  MCS_FORCE_INLINE void storeWMask(SimdType& x, SimdType& vmask, char* dst)
  {
    _mm_maskmoveu_si128(x, vmask, dst);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType& x)
  {
    _mm_storeu_si128(reinterpret_cast<SimdType*>(dst), x);
  }

  MCS_FORCE_INLINE SimdType min(SimdType& x, SimdType& y)
  {
    return _mm_min_epi8(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType& x, SimdType& y)
  {
    return _mm_max_epi8(x, y);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T, typename std::enable_if<std::is_same<VT, vi128_wr>::value && std::is_same<CHECK_T, uint8_t>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = vi128_wr;
  using SimdType = vi128_t;
  using FilterType = T;
  using StorageType = T;
  // Mask calculation for int and float types differs.
  // See corresponding intrinsics algos for details.
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return _mm_set1_epi8(fill);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return _mm_loadu_si128(reinterpret_cast<const SimdType*>(from));
  }

  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi8(x, y));
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType& x, SimdType& y)
  {
    return cmpLt(x, y) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpgt_epi8(x, y));
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType& x, SimdType& y)
  {
    return cmpGt(x, y) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmplt_epi8(x, y));
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType& x, SimdType& y)
  {
    return _mm_movemask_epi8(_mm_cmpeq_epi8(x, y)) ^ 0xFFFF;
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType& x, SimdType& y)
  {
    return 0;
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType& x, SimdType& y)
  {
    return 0xFFFF;
  }

  // permute
  /* TODO Available in AVX-512
      MCS_FORCE_INLINE SimdType perm8Bits(SimdType& x, SimdType& idx)
      {
        return _mm_permutexvar_epi8(x, idx);
      }
  */
  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType& vmask)
  {
    return _mm_movemask_epi8(vmask);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType& x, SimdType& y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType& x, SimdType& y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return _mm_setzero_si128();
  }

  // store
  MCS_FORCE_INLINE void storeWMask(SimdType& x, SimdType& vmask, char* dst)
  {
    _mm_maskmoveu_si128(x, vmask, dst);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType& x)
  {
    _mm_storeu_si128(reinterpret_cast<SimdType*>(dst), x);
  }

  MCS_FORCE_INLINE SimdType min(SimdType& x, SimdType& y)
  {
    return _mm_min_epu8(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType& x, SimdType& y)
  {
    return _mm_max_epu8(x, y);
  }
};

}  // namespace simd

#endif  // if defined(__x86_64__ )

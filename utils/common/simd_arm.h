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

#include "simd_sse.h"  // ENUM_KIND
#ifdef __aarch64__
#include "arm_neon.h"
#include <cstdint>
#include <type_traits>
#ifdef __OPTIMIZE__
#define MCS_FORCE_INLINE __attribute__((__always_inline__))
#else
#define MCS_FORCE_INLINE inline
#endif

#include "mcs_datatype.h"

namespace simd
{
// the type is decided by the basic type
using vi1_t = int8x16_t;
using vi2_t = int16x8_t;
using vi4_t = int32x4_t;
using vi8_t = int64x2_t;
using vi1u_t = uint8x16_t;
using vi2u_t = uint16x8_t;
using vi4u_t = uint32x4_t;
using vi8u_t = uint64x2_t;
using vi128f_t = float32x4_t;
using vi128d_t = float64x2_t;
using int128_t = __int128;
using MaskSimdType = vi1u_t;
// wrapper types
struct vi1_wr
{
  int8x16_t v;
};
struct vi2_wr
{
  int16x8_t v;
};
struct vi4_wr
{
  int32x4_t v;
};
struct vi8_wr
{
  int64x2_t v;
};
struct vi16_wr
{
  int128_t v;
};
struct viu1_wr
{
  uint8x16_t v;
};
struct viu2_wr
{
  uint16x8_t v;
};
struct viu4_wr
{
  uint32x4_t v;
};
struct viu8_wr
{
  uint64x2_t v;
};

struct vi128f_wr
{
  float32x4_t v;
};
struct vi128d_wr
{
  float64x2_t v;
};

template <int W>
struct WidthToSVecWrapperType;

template <>
struct WidthToSVecWrapperType<1>
{
  using Vectype = int8x16_t;
  using WrapperType = struct vi1_wr;
};

template <>
struct WidthToSVecWrapperType<2>
{
  using Vectype = int16x8_t;
  using WrapperType = struct vi2_wr;
};

template <>
struct WidthToSVecWrapperType<4>
{
  using Vectype = int32x4_t;
  using WrapperType = struct vi4_wr;
};

template <>
struct WidthToSVecWrapperType<8>
{
  using Vectype = int64x2_t;
  using WrapperType = struct vi8_wr;
};
template <>
struct WidthToSVecWrapperType<16>
{
  using Vectype = int128_t;
  using WrapperType = struct vi16_wr;
};
template <int W>
struct WidthToVecWrapperType;

template <>
struct WidthToVecWrapperType<1>
{
  using Vectype = uint8x16_t;
  using WrapperType = struct viu1_wr;
};

template <>
struct WidthToVecWrapperType<2>
{
  using Vectype = uint16x8_t;
  using WrapperType = struct viu2_wr;
};

template <>
struct WidthToVecWrapperType<4>
{
  using Vectype = uint32x4_t;
  using WrapperType = struct viu4_wr;
};

template <>
struct WidthToVecWrapperType<8>
{
  using Vectype = uint64x2_t;
  using WrapperType = struct viu8_wr;
};

// We get the simd and wrapper type of basic type by TypeToVecWrapperType.
template <typename T, typename ENABLE = void>
struct TypeToVecWrapperType;

template <typename T>
struct TypeToVecWrapperType<T, typename std::enable_if<std::is_same<T, __int128>::value>::type>
 : WidthToSVecWrapperType<sizeof(__int128)>
{
};
template <typename T>
struct TypeToVecWrapperType<T, typename std::enable_if<std::is_same_v<float, T>>::type>
{
  using Vectype = vi128f_t;
  using WrapperType = vi128f_wr;
};
template <typename T>
struct TypeToVecWrapperType<T, typename std::enable_if<std::is_same_v<double, T>>::type>
{
  using Vectype = vi128d_t;
  using WrapperType = vi128d_wr;
};
template <typename T>
struct TypeToVecWrapperType<T, typename std::enable_if<std::is_unsigned_v<T>>::type>
 : WidthToVecWrapperType<sizeof(T)>
{
};

template <typename T>
struct TypeToVecWrapperType<T, typename std::enable_if<std::is_signed_v<T> && !is_floating_point_v<T>>::type>
 : WidthToSVecWrapperType<sizeof(T)>
{
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
  using type = typename TypeToVecWrapperType<T>::WrapperType;
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

template <typename VT, typename T, typename ENABLE = void>
class SimdFilterProcessor;

// Dummy class that captures all impossible cases, e.g. integer vector as VT and flot as CHECK_T.we use
// int32_t to do operations
template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T,
    typename std::enable_if<sizeof(CHECK_T) == 16 ||
                            (std::is_same<VT, vi128f_wr>::value && !std::is_same<CHECK_T, float>::value &&
                             !std::is_same<CHECK_T, double>::value)>::type>
{
  // This is a dummy class that is not currently used.
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = int32_t;
  using SimdWrapperType = vi4_wr;
  using SimdType = int32x4_t;
  using FilterType = T;
  using StorageType = T;
  using MT = uint32x4_t;
  using MaskType = MT;
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return vdupq_n_s32(fill);
  }
  MCS_FORCE_INLINE SimdType blend(SimdType x, SimdType y, MT mask) const
  {
    return vbslq_s32(mask, y, x);
  }
  MCS_FORCE_INLINE SimdType cmpGtSimdType(SimdType x, SimdType y) const
  {
    return (SimdType)vcgtq_s32(x, y);
  }
  MCS_FORCE_INLINE T maxScalar(SimdType x)
  {
    return vmaxvq_s32(x);
  }
  MCS_FORCE_INLINE SimdType bwAnd(SimdType x, SimdType y) const
  {
    return vandq_s32(x, y);
  }
  MCS_FORCE_INLINE T minScalar(SimdType x)
  {
    return vminvq_s32(x);
  }
  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return vld1q_s32(reinterpret_cast<const int32_t*>(from));
  }

  MCS_FORCE_INLINE MT cmpDummy(SimdType x, SimdType y)
  {
    return vdupq_n_u32(0xFFFFFFFF);
  }
  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType x, SimdType y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType x, SimdType y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType x, SimdType y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType x, SimdType y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType x, SimdType y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType x, SimdType y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType x, SimdType y)
  {
    return vdupq_n_u32(0);
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType x, SimdType y)
  {
    return vdupq_n_u32(0xFFFFFFFF);
  }
  MCS_FORCE_INLINE MT falseMask()
  {
    return vdupq_n_u32(0);
  }

  MCS_FORCE_INLINE MT trueMask()
  {
    return vdupq_n_u32(0xFFFFFFFF);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType x, SimdType y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE SimdType min(SimdType x, SimdType y)
  {
    return reinterpret_cast<SimdType>(std::min(reinterpret_cast<int128_t>(x), reinterpret_cast<int128_t>(y)));
  }

  MCS_FORCE_INLINE SimdType max(SimdType x, SimdType y)
  {
    return reinterpret_cast<SimdType>(std::max(reinterpret_cast<int128_t>(x), reinterpret_cast<int128_t>(y)));
  }
  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType x, SimdType y)
  {
    return cmpDummy(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return vdupq_n_s32(0);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType x)
  {
    vst1q_s32(reinterpret_cast<int32_t*>(dst), x);
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
  using NullEmptySimdType = typename WidthToSVecWrapperType<sizeof(T)>::Vectype;
  using SimdWrapperType = simd::vi128d_wr;
  using SimdType = simd::vi128d_t;
  using StorageSimdType = typename WidthToSVecWrapperType<sizeof(T)>::Vectype;
  using StorageType = typename datatypes::WidthToSIntegralType<sizeof(T)>::type;
  using StorageWrapperTypeType = typename WidthToSVecWrapperType<sizeof(T)>::WrapperType;
  using StorageVecProcType = SimdFilterProcessor<StorageWrapperTypeType, StorageType>;
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  using MT = uint64x2_t;
  using MaskType = MT;
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    StorageVecProcType nullEmptyProcessor;
    // This spec borrows the expr from u-/int64 based proceesor class.
    return (SimdType)nullEmptyProcessor.loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return vdupq_n_f64(fill);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return vld1q_f64(reinterpret_cast<const T*>(from));
  }
  MCS_FORCE_INLINE SimdType blend(SimdType x, SimdType y, MT mask) const
  {
    return vbslq_f64(mask, y, x);
  }

  MCS_FORCE_INLINE SimdType bwAnd(SimdType x, SimdType y) const
  {
    return (SimdType)vandq_s64((StorageSimdType)x, (StorageSimdType)y);
  }
  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType x, SimdType y)
  {
    return vceqq_f64(x, y);
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType x, SimdType y)
  {
    return vcgeq_f64(x, y);
  }
  MCS_FORCE_INLINE T maxScalar(SimdType x)
  {
    return vmaxvq_f64(x);
  }
  MCS_FORCE_INLINE T minScalar(SimdType x)
  {
    return vminvq_f64(x);
  }
  MCS_FORCE_INLINE SimdType cmpGtSimdType(SimdType x, SimdType y) const
  {
    return (SimdType)vcgtq_f64(x, y);
  }
  MCS_FORCE_INLINE MT cmpGt(SimdType x, SimdType y)
  {
    return vcgtq_f64(x, y);
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType x, SimdType y)
  {
    return vcleq_f64(x, y);
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType x, SimdType y)
  {
    return vcltq_f64(x, y);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType x, SimdType y)
  {
    return vreinterpretq_u64_u32(vmvnq_u32(vreinterpretq_u32_u64(cmpEq(x, y))));
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType x, SimdType y)
  {
    return vdupq_n_u64(0);
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType x, SimdType y)
  {
    return vdupq_n_u64(0xFFFFFFFFFFFFFFFF);
  }
  MCS_FORCE_INLINE MT falseMask()
  {
    return vdupq_n_u64(0);
  }

  MCS_FORCE_INLINE MT trueMask()
  {
    return vdupq_n_u64(0xFFFFFFFFFFFFFFFF);
  }
  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType x, SimdType y)
  {
    StorageVecProcType nullEmptyProcessor;
    NullEmptySimdType* xAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&x);
    NullEmptySimdType* yAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&y);
    // This spec borrows the expr from u-/int64 based proceesor class.
    return nullEmptyProcessor.cmpNe(*xAsIntVecPtr, *yAsIntVecPtr);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType x, SimdType y)
  {
    StorageVecProcType nullEmptyProcessor;

    NullEmptySimdType* xAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&x);
    NullEmptySimdType* yAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&y);
    // This spec borrows the expr from u-/int64 based proceesor class.
    return nullEmptyProcessor.cmpEq(*xAsIntVecPtr, *yAsIntVecPtr);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return vdupq_n_f64(0);
  }
  MCS_FORCE_INLINE SimdType min(SimdType x, SimdType y)
  {
    return vminq_f64(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType x, SimdType y)
  {
    return vmaxq_f64(x, y);
  }
  MCS_FORCE_INLINE void store(char* dst, SimdType x)
  {
    vst1q_f64(reinterpret_cast<T*>(dst), x);
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
  using NullEmptySimdType = typename WidthToSVecWrapperType<sizeof(T)>::Vectype;
  using SimdWrapperType = vi128f_wr;
  using SimdType = vi128f_t;
  using StorageSimdType = typename WidthToSVecWrapperType<sizeof(T)>::Vectype;
  using StorageType = typename datatypes::WidthToSIntegralType<sizeof(T)>::type;
  using StorageWrapperTypeType = typename WidthToSVecWrapperType<sizeof(T)>::WrapperType;
  using StorageVecProcType = SimdFilterProcessor<StorageWrapperTypeType, StorageType>;
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  using MT = uint32x4_t;
  using MaskType = MT;
  MCS_FORCE_INLINE MaskType maskCtor(const char* inputArray)
  {
    // These masks are valid for little-endian archs.
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(reinterpret_cast<const uint32_t*>(inputArray));
    return uint32x4_t{ptr[0], ptr[1], ptr[2], ptr[3]};
  }
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    StorageVecProcType nullEmptyProcessor;
    // This spec borrows the expr from u-/int64 based proceesor class.
    return (SimdType)nullEmptyProcessor.loadValue(fill);
  }
  MCS_FORCE_INLINE SimdType blend(SimdType x, SimdType y, MT mask) const
  {
    return vbslq_f32(mask, y, x);
  }
  MCS_FORCE_INLINE SimdType cmpGtSimdType(SimdType x, SimdType y) const
  {
    return (SimdType)vcgtq_f32(x, y);
  }
  MCS_FORCE_INLINE SimdType bwAnd(SimdType x, SimdType y) const
  {
    return (SimdType)vandq_s32((StorageSimdType)x, (StorageSimdType)y);
  }
  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return vdupq_n_f32(fill);
  }
  MCS_FORCE_INLINE T maxScalar(SimdType x)
  {
    return vmaxvq_f32(x);
  }
  MCS_FORCE_INLINE T minScalar(SimdType x)
  {
    return vminvq_f32(x);
  }
  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return vld1q_f32(reinterpret_cast<const T*>(from));
  }

  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType x, SimdType y)
  {
    return vceqq_f32(x, y);
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType x, SimdType y)
  {
    return vcgeq_f32(x, y);
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType x, SimdType y)
  {
    return vcgtq_f32(x, y);
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType x, SimdType y)
  {
    return vcleq_f32(x, y);
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType x, SimdType y)
  {
    return vcltq_f32(x, y);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType x, SimdType y)
  {
    return vmvnq_u32(vceqq_f32(x, y));
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType x, SimdType y)
  {
    return vdupq_n_u32(0);
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType x, SimdType y)
  {
    return vdupq_n_u32(0xFFFFFFFF);
  }
  MCS_FORCE_INLINE MT falseMask()
  {
    return vdupq_n_u32(0);
  }

  MCS_FORCE_INLINE MT trueMask()
  {
    return vdupq_n_u32(0xFFFFFFFF);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType x, SimdType y)
  {
    StorageVecProcType nullEmptyProcessor;

    NullEmptySimdType* xAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&x);
    NullEmptySimdType* yAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&y);
    // This spec borrows the expr from u-/int64 based proceesor class.
    return nullEmptyProcessor.cmpNe(*xAsIntVecPtr, *yAsIntVecPtr);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType x, SimdType y)
  {
    StorageVecProcType nullEmptyProcessor;

    NullEmptySimdType* xAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&x);
    NullEmptySimdType* yAsIntVecPtr = reinterpret_cast<NullEmptySimdType*>(&y);
    // This spec borrows the expr from u-/int64 based proceesor class.
    return nullEmptyProcessor.cmpEq(*xAsIntVecPtr, *yAsIntVecPtr);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return vdupq_n_f32(0);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType x)
  {
    vst1q_f32(reinterpret_cast<T*>(dst), x);
  }
  MCS_FORCE_INLINE SimdType min(SimdType x, SimdType y)
  {
    return vminq_f32(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType x, SimdType y)
  {
    return vmaxq_f32(x, y);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, vi8_wr>::value && std::is_same<CHECK_T, int64_t>::value &&
                            !std::is_same<CHECK_T, double>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = typename WidthToSVecWrapperType<sizeof(T)>::WrapperType;
  using SimdType = typename WidthToSVecWrapperType<sizeof(T)>::Vectype;
  using FilterType = T;
  using StorageType = T;
  using MT = uint64x2_t;
  using MaskType = MT;
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return vdupq_n_s64(fill);
  }
  MCS_FORCE_INLINE T maxScalar(SimdType x)
  {
    return std::max(((int64_t*)(&x))[0], ((int64_t*)(&x))[1]);
  }
  MCS_FORCE_INLINE T minScalar(SimdType x)
  {
    return std::min(((int64_t*)(&x))[0], ((int64_t*)(&x))[1]);
  }
  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return vld1q_s64(reinterpret_cast<const int64_t*>(from));
  }
  MCS_FORCE_INLINE SimdType blend(SimdType x, SimdType y, MT mask) const
  {
    return vbslq_s64(mask, y, x);
  }

  MCS_FORCE_INLINE SimdType bwAnd(SimdType x, SimdType y) const
  {
    return vandq_s64(x, y);
  }
  MCS_FORCE_INLINE SimdType cmpGtSimdType(SimdType x, SimdType y) const
  {
    return (SimdType)vcgtq_s64(x, y);
  }

  // Compare
  MCS_FORCE_INLINE MT cmpGe(SimdType x, SimdType y)
  {
    return vcgeq_s64(x, y);
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType x, SimdType y)
  {
    return vcgtq_s64(x, y);
  }

  MCS_FORCE_INLINE MT cmpEq(SimdType x, SimdType y)
  {
    return vceqq_s64(x, y);
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType x, SimdType y)
  {
    return vcleq_s64(x, y);
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType x, SimdType y)
  {
    return vcltq_s64(x, y);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType x, SimdType y)
  {
    return vreinterpretq_u64_u32(vmvnq_u32(vreinterpretq_u32_u64(cmpEq(x, y))));
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType x, SimdType y)
  {
    return vdupq_n_u64(0);
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType x, SimdType y)
  {
    return vdupq_n_u64(0xFFFFFFFFFFFFFFFF);
  }
  MCS_FORCE_INLINE MT falseMask()
  {
    return vdupq_n_u64(0);
  }

  MCS_FORCE_INLINE MT trueMask()
  {
    return vdupq_n_u64(0xFFFFFFFFFFFFFFFF);
  }
  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType vmask)
  {
    return vmask;
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return vdupq_n_s64(0);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType x, SimdType y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(MaskType x, MaskType y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType x, SimdType y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType min(SimdType x, SimdType y)
  {
    return vbslq_s64(vcgtq_s64(y, x), x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType x, SimdType y)
  {
    return vbslq_s64(vcgtq_s64(x, y), x, y);
  }
  MCS_FORCE_INLINE void store(char* dst, SimdType x)
  {
    vst1q_s64(reinterpret_cast<int64_t*>(dst), x);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, viu8_wr>::value && std::is_same<CHECK_T, uint64_t>::value &&
                            !std::is_same<CHECK_T, double>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = uint64_t;
  using SimdWrapperType = typename WidthToVecWrapperType<sizeof(T)>::WrapperType;
  using SimdType = typename WidthToVecWrapperType<sizeof(T)>::Vectype;
  using FilterType = T;
  using StorageType = T;
  using MT = uint64x2_t;
  using MaskType = MT;
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return vdupq_n_u64(fill);
  }
  MCS_FORCE_INLINE SimdType blend(SimdType x, SimdType y, MT mask) const
  {
    return vbslq_u64(mask, y, x);
  }
  MCS_FORCE_INLINE SimdType cmpGtSimdType(SimdType x, SimdType y) const
  {
    return (SimdType)vcgtq_u64(x, y);
  }
  MCS_FORCE_INLINE SimdType bwAnd(SimdType x, SimdType y) const
  {
    return vandq_u64(x, y);
  }
  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return vld1q_u64(reinterpret_cast<const uint64_t*>(from));
  }
  // Compare
  MCS_FORCE_INLINE MT cmpGe(SimdType x, SimdType y)
  {
    return vcgeq_u64(x, y);
  }
  MCS_FORCE_INLINE T maxScalar(SimdType x)
  {
    return std::max(((uint64_t*)(&x))[0], ((uint64_t*)(&x))[1]);
  }
  MCS_FORCE_INLINE T minScalar(SimdType x)
  {
    return std::min(((uint64_t*)(&x))[0], ((uint64_t*)(&x))[1]);
  }
  MCS_FORCE_INLINE MT cmpGt(SimdType x, SimdType y)
  {
    return vcgtq_u64(x, y);
  }
  MCS_FORCE_INLINE SimdType min(SimdType x, SimdType y)
  {
    return vbslq_u64(vcgtq_u64(y, x), x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType x, SimdType y)
  {
    return vbslq_u64(vcgtq_u64(x, y), x, y);
  }
  MCS_FORCE_INLINE MT cmpEq(SimdType x, SimdType y)
  {
    return vceqq_u64(x, y);
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType x, SimdType y)
  {
    return vcleq_u64(x, y);
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType x, SimdType y)
  {
    return cmpGt(y, x);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType x, SimdType y)
  {
    return vreinterpretq_u64_u32(vmvnq_u32(vreinterpretq_u32_u64(vceqq_u64(x, y))));
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType x, SimdType y)
  {
    return vdupq_n_u64(0);
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType x, SimdType y)
  {
    return vdupq_n_u64(0xFFFFFFFFFFFFFFFF);
  }

  MCS_FORCE_INLINE MT falseMask()
  {
    return vdupq_n_u64(0);
  }

  MCS_FORCE_INLINE MT trueMask()
  {
    return vdupq_n_u64(0xFFFFFFFFFFFFFFFF);
  }
  MCS_FORCE_INLINE SimdType setToZero()
  {
    return vdupq_n_u64(0);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType x, SimdType y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType x, SimdType y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType x)
  {
    vst1q_u64(reinterpret_cast<uint64_t*>(dst), x);
  }
};
template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, vi4_wr>::value && std::is_same<CHECK_T, int32_t>::value &&
                            !std::is_same<CHECK_T, float>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = typename WidthToSVecWrapperType<sizeof(T)>::WrapperType;
  using SimdType = typename WidthToSVecWrapperType<sizeof(T)>::Vectype;
  using FilterType = T;
  using StorageType = T;
  using MT = uint32x4_t;
  using MaskType = MT;
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  MCS_FORCE_INLINE MaskType maskCtor(const char* inputArray)
  {
    // These masks are valid for little-endian archs.
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(reinterpret_cast<const uint32_t*>(inputArray));
    return uint32x4_t{ptr[0], ptr[1], ptr[2], ptr[3]};
  }
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return vdupq_n_s32(fill);
  }
  MCS_FORCE_INLINE T maxScalar(SimdType x)
  {
    return vmaxvq_s32(x);
  }
  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return vld1q_s32(reinterpret_cast<const int32_t*>(from));
  }
  MCS_FORCE_INLINE SimdType blend(SimdType x, SimdType y, MT mask) const
  {
    return vbslq_s32(mask, y, x);
  }
  MCS_FORCE_INLINE SimdType cmpGtSimdType(SimdType x, SimdType y) const
  {
    return (SimdType)vcgtq_s32(x, y);
  }
  MCS_FORCE_INLINE SimdType bwAnd(SimdType x, SimdType y) const
  {
    return vandq_s32(x, y);
  }
  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType x, SimdType y)
  {
    return vceqq_s32(x, y);
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType x, SimdType y)
  {
    return vcgeq_s32(x, y);
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType x, SimdType y)
  {
    return vcgtq_s32(x, y);
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType x, SimdType y)
  {
    return vcleq_s32(x, y);
  }
  MCS_FORCE_INLINE T minScalar(SimdType x)
  {
    return vminvq_s32(x);
  }
  MCS_FORCE_INLINE SimdType min(SimdType x, SimdType y)
  {
    return vminq_s32(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType x, SimdType y)
  {
    return vmaxq_s32(x, y);
  }
  MCS_FORCE_INLINE MT cmpLt(SimdType x, SimdType y)
  {
    return vcltq_s32(x, y);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType x, SimdType y)
  {
    return vmvnq_u32(vceqq_s32(x, y));
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType x, SimdType y)
  {
    return vdupq_n_u32(0);
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType x, SimdType y)
  {
    return vdupq_n_u32(0xFFFFFFFF);
  }
  MCS_FORCE_INLINE MT falseMask()
  {
    return vdupq_n_u32(0);
  }

  MCS_FORCE_INLINE MT trueMask()
  {
    return vdupq_n_u32(0xFFFFFFFF);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType x, SimdType y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType x, SimdType y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return vdupq_n_s32(0);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType x)
  {
    vst1q_s32(reinterpret_cast<int32_t*>(dst), x);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, viu4_wr>::value && std::is_same<CHECK_T, uint32_t>::value &&
                            !std::is_same<CHECK_T, float>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = uint32_t;
  using SimdWrapperType = typename WidthToVecWrapperType<sizeof(T)>::WrapperType;
  using SimdType = typename WidthToVecWrapperType<sizeof(T)>::Vectype;
  using FilterType = T;
  using StorageType = T;
  using MT = uint32x4_t;
  using MaskType = MT;
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  MCS_FORCE_INLINE MaskType maskCtor(const char* inputArray)
  {
    // These masks are valid for little-endian archs.
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(reinterpret_cast<const uint32_t*>(inputArray));
    return uint32x4_t{ptr[0], ptr[1], ptr[2], ptr[3]};
  }
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return vdupq_n_u32(fill);
  }
  MCS_FORCE_INLINE T maxScalar(SimdType x)
  {
    return vmaxvq_u32(x);
  }
  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return vld1q_u32(reinterpret_cast<const uint32_t*>(from));
  }
  MCS_FORCE_INLINE SimdType blend(SimdType x, SimdType y, MT mask) const
  {
    return vbslq_u32(mask, y, x);
  }
  MCS_FORCE_INLINE SimdType cmpGtSimdType(SimdType x, SimdType y) const
  {
    return (SimdType)vcgtq_u32(x, y);
  }
  MCS_FORCE_INLINE SimdType bwAnd(SimdType x, SimdType y) const
  {
    return vandq_u32(x, y);
  }
  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType x, SimdType y)
  {
    return vceqq_u32(x, y);
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType x, SimdType y)
  {
    return vcgeq_u32(x, y);
  }
  MCS_FORCE_INLINE SimdType min(SimdType x, SimdType y)
  {
    return vminq_u32(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType x, SimdType y)
  {
    return vmaxq_u32(x, y);
  }
  MCS_FORCE_INLINE MT cmpGt(SimdType x, SimdType y)
  {
    return vcgtq_u32(x, y);
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType x, SimdType y)
  {
    return vcleq_u32(x, y);
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType x, SimdType y)
  {
    return cmpGt(y, x);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType x, SimdType y)
  {
    return vmvnq_u32(vceqq_u32(x, y));
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType x, SimdType y)
  {
    return vdupq_n_u32(0);
  }
  MCS_FORCE_INLINE T minScalar(SimdType x)
  {
    return vminvq_u32(x);
  }
  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType x, SimdType y)
  {
    return vdupq_n_u32(0xFFFFFFFF);
  }
  MCS_FORCE_INLINE MT falseMask()
  {
    return vdupq_n_u32(0);
  }

  MCS_FORCE_INLINE MT trueMask()
  {
    return vdupq_n_u32(0xFFFFFFFF);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType x, SimdType y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType x, SimdType y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return vdupq_n_u32(0);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType x)
  {
    vst1q_u32(reinterpret_cast<uint32_t*>(dst), x);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, vi2_wr>::value && std::is_same<CHECK_T, int16_t>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = typename WidthToSVecWrapperType<sizeof(T)>::WrapperType;
  using SimdType = typename WidthToSVecWrapperType<sizeof(T)>::Vectype;
  using FilterType = T;
  using StorageType = T;
  using MT = uint16x8_t;
  using MaskType = MT;
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  MCS_FORCE_INLINE MaskType maskCtor(const char* inputArray)
  {
    // These masks are valid for little-endian archs.
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(reinterpret_cast<const uint64_t*>(inputArray));
    return uint16x8_t{ptr[0], ptr[1], ptr[2], ptr[3], ptr[4], ptr[5], ptr[6], ptr[7]};
  }

  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return vdupq_n_s16(fill);
  }
  MCS_FORCE_INLINE T maxScalar(SimdType x)
  {
    return vmaxvq_s16(x);
  }
  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return vld1q_s16(reinterpret_cast<const int16_t*>(from));
  }

  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType x, SimdType y)
  {
    return vceqq_s16(x, y);
  }
  MCS_FORCE_INLINE SimdType blend(SimdType x, SimdType y, MT mask) const
  {
    return vbslq_s16(mask, y, x);
  }

  MCS_FORCE_INLINE SimdType bwAnd(SimdType x, SimdType y) const
  {
    return vandq_s16(x, y);
  }
  MCS_FORCE_INLINE SimdType cmpGtSimdType(SimdType x, SimdType y) const
  {
    return (SimdType)vcgtq_s16(x, y);
  }
  MCS_FORCE_INLINE T minScalar(SimdType x)
  {
    return vminvq_s16(x);
  }
  MCS_FORCE_INLINE MT cmpGe(SimdType x, SimdType y)
  {
    return vcgeq_s16(x, y);
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType x, SimdType y)
  {
    return vcgtq_s16(x, y);
  }
  MCS_FORCE_INLINE SimdType min(SimdType x, SimdType y)
  {
    return vminq_s16(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType x, SimdType y)
  {
    return vmaxq_s16(x, y);
  }
  MCS_FORCE_INLINE MT cmpLe(SimdType x, SimdType y)
  {
    return vcleq_s16(x, y);
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType x, SimdType y)
  {
    return vcltq_s16(x, y);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType x, SimdType y)
  {
    return vmvnq_u16(cmpEq(x, y));
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType x, SimdType y)
  {
    return vdupq_n_u16(0);
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType x, SimdType y)
  {
    return vdupq_n_u16(0xFFFF);
  }
  MCS_FORCE_INLINE MT falseMask()
  {
    return vdupq_n_u16(0);
  }

  MCS_FORCE_INLINE MT trueMask()
  {
    return vdupq_n_u16(0xFFFF);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType x, SimdType y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType x, SimdType y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return vdupq_n_s16(0);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType x)
  {
    vst1q_s16(reinterpret_cast<int16_t*>(dst), x);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, viu2_wr>::value && std::is_same<CHECK_T, uint16_t>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = uint16_t;
  using SimdWrapperType = typename WidthToVecWrapperType<sizeof(T)>::WrapperType;
  using SimdType = typename WidthToVecWrapperType<sizeof(T)>::Vectype;
  using FilterType = T;
  using StorageType = T;
  using MT = uint16x8_t;
  using MaskType = MT;
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  MCS_FORCE_INLINE MaskType maskCtor(const char* inputArray)
  {
    // These masks are valid for little-endian archs.
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(reinterpret_cast<const uint64_t*>(inputArray));
    return uint16x8_t{ptr[0], ptr[1], ptr[2], ptr[3], ptr[4], ptr[5], ptr[6], ptr[7]};
  }
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return vdupq_n_u16(fill);
  }
  MCS_FORCE_INLINE SimdType cmpGtSimdType(SimdType x, SimdType y) const
  {
    return (SimdType)vcgtq_u16(x, y);
  }
  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return vld1q_u16(reinterpret_cast<const uint16_t*>(from));
  }
  MCS_FORCE_INLINE SimdType min(SimdType x, SimdType y)
  {
    return vminq_u16(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType x, SimdType y)
  {
    return vmaxq_u16(x, y);
  }
  MCS_FORCE_INLINE T maxScalar(SimdType x)
  {
    return vmaxvq_u16(x);
  }
  MCS_FORCE_INLINE T minScalar(SimdType x)
  {
    return vminvq_u16(x);
  }
  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType x, SimdType y)
  {
    return vceqq_u16(x, y);
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType x, SimdType y)
  {
    return vcgeq_u16(x, y);
  }
  MCS_FORCE_INLINE SimdType blend(SimdType x, SimdType y, MT mask) const
  {
    return vbslq_u16(mask, y, x);
  }

  MCS_FORCE_INLINE SimdType bwAnd(SimdType x, SimdType y) const
  {
    return vandq_u16(x, y);
  }
  MCS_FORCE_INLINE MT cmpGt(SimdType x, SimdType y)
  {
    return vcgtq_u16(x, y);
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType x, SimdType y)
  {
    return cmpGe(y, x);
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType x, SimdType y)
  {
    return vcltq_u16(x, y);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType x, SimdType y)
  {
    return vmvnq_u16(vceqq_u16(x, y));
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType x, SimdType y)
  {
    return vdupq_n_u16(0);
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType x, SimdType y)
  {
    return vdupq_n_u16(0xFFFF);
  }
  MCS_FORCE_INLINE MT falseMask()
  {
    return vdupq_n_u16(0);
  }

  MCS_FORCE_INLINE MT trueMask()
  {
    return vdupq_n_u16(0xFFFF);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType x, SimdType y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType x, SimdType y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return vdupq_n_u16(0);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType x)
  {
    vst1q_u16(reinterpret_cast<uint16_t*>(dst), x);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, vi1_wr>::value && std::is_same<CHECK_T, int8_t>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = typename datatypes::WidthToSIntegralType<sizeof(CHECK_T)>::type;
  using SimdWrapperType = typename WidthToSVecWrapperType<sizeof(T)>::WrapperType;
  using SimdType = typename WidthToSVecWrapperType<sizeof(T)>::Vectype;
  using FilterType = T;
  using StorageType = T;
  using MT = uint8x16_t;
  using MaskType = MT;

  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }
  MCS_FORCE_INLINE SimdType min(SimdType x, SimdType y)
  {
    return vminq_s8(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType x, SimdType y)
  {
    return vmaxq_s8(x, y);
  }
  MCS_FORCE_INLINE T maxScalar(SimdType x)
  {
    return vmaxvq_s8(x);
  }
  MCS_FORCE_INLINE SimdType cmpGtSimdType(SimdType x, SimdType y) const
  {
    return (SimdType)vcgtq_s8(x, y);
  }
  MCS_FORCE_INLINE T minScalar(SimdType x)
  {
    return vminvq_s8(x);
  }
  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return vdupq_n_s8(fill);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return vld1q_s8(reinterpret_cast<const int8_t*>(from));
  }
  MCS_FORCE_INLINE SimdType blend(SimdType x, SimdType y, MT mask) const
  {
    return vbslq_s8(mask, y, x);
  }

  MCS_FORCE_INLINE SimdType bwAnd(SimdType x, SimdType y) const
  {
    return vandq_s8(x, y);
  }
  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType x, SimdType y)
  {
    return vceqq_s8(x, y);
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType x, SimdType y)
  {
    return vcgeq_s8(x, y);
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType x, SimdType y)
  {
    return vcgtq_s8(x, y);
  }

  MCS_FORCE_INLINE MT cmpLe(SimdType x, SimdType y)
  {
    return vcleq_s8(x, y);
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType x, SimdType y)
  {
    return vcltq_s8(x, y);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType x, SimdType y)
  {
    return vmvnq_u8(vceqq_s8(x, y));
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType x, SimdType y)
  {
    return vdupq_n_u8(0);
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType x, SimdType y)
  {
    return vdupq_n_u8(0xff);
  }
  MCS_FORCE_INLINE MT falseMask()
  {
    return vdupq_n_u8(0);
  }

  MCS_FORCE_INLINE MT trueMask()
  {
    return vdupq_n_u8(0xff);
  }
  // misc
  MCS_FORCE_INLINE MT convertVectorToBitMask(SimdType vmask)
  {
    return vmask;
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType x, SimdType y)
  {
    return cmpNe(x, y);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType x, SimdType y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return vdupq_n_s8(0);
  }

  MCS_FORCE_INLINE void store(char* dst, SimdType x)
  {
    vst1q_s8(reinterpret_cast<int8_t*>(dst), x);
  }
};

template <typename VT, typename CHECK_T>
class SimdFilterProcessor<
    VT, CHECK_T,
    typename std::enable_if<std::is_same<VT, viu1_wr>::value && std::is_same<CHECK_T, uint8_t>::value>::type>
{
 public:
  constexpr static const uint16_t vecByteSize = 16U;
  constexpr static const uint16_t vecBitSize = 128U;
  using T = uint8_t;
  using SimdWrapperType = typename WidthToVecWrapperType<sizeof(T)>::WrapperType;
  using SimdType = typename WidthToVecWrapperType<sizeof(T)>::Vectype;
  using FilterType = T;
  using StorageType = T;
  using MT = uint8x16_t;
  using MaskType = MT;
  constexpr static const uint16_t FilterMaskStep = sizeof(T);
  // Load value
  MCS_FORCE_INLINE SimdType emptyNullLoadValue(const T fill)
  {
    return loadValue(fill);
  }

  MCS_FORCE_INLINE SimdType loadValue(const T fill)
  {
    return vdupq_n_u8(fill);
  }
  MCS_FORCE_INLINE T maxScalar(SimdType x)
  {
    return vmaxvq_u8(x);
  }
  MCS_FORCE_INLINE T minScalar(SimdType x)
  {
    return vminvq_u8(x);
  }

  // Load from
  MCS_FORCE_INLINE SimdType loadFrom(const char* from)
  {
    return vld1q_u8(reinterpret_cast<const uint8_t*>(from));
  }
  MCS_FORCE_INLINE SimdType blend(SimdType x, SimdType y, MT mask) const
  {
    return vbslq_u8(mask, y, x);
  }

  MCS_FORCE_INLINE SimdType bwAnd(SimdType x, SimdType y) const
  {
    return vandq_u8(x, y);
  }
  MCS_FORCE_INLINE SimdType cmpGtSimdType(SimdType x, SimdType y) const
  {
    return (SimdType)vcgtq_u8(x, y);
  }
  // Compare
  MCS_FORCE_INLINE MT cmpEq(SimdType x, SimdType y)
  {
    return vceqq_u8(x, y);
  }

  MCS_FORCE_INLINE MT cmpGe(SimdType x, SimdType y)
  {
    return vcgeq_u8(x, y);
  }

  MCS_FORCE_INLINE MT cmpGt(SimdType x, SimdType y)
  {
    return vcgtq_u8(x, y);
  }
  MCS_FORCE_INLINE SimdType min(SimdType x, SimdType y)
  {
    return vminq_u8(x, y);
  }

  MCS_FORCE_INLINE SimdType max(SimdType x, SimdType y)
  {
    return vmaxq_u8(x, y);
  }
  MCS_FORCE_INLINE MT cmpLe(SimdType x, SimdType y)
  {
    return vcleq_u8(x, y);
  }

  MCS_FORCE_INLINE MT cmpLt(SimdType x, SimdType y)
  {
    return vcltq_u8(x, y);
  }

  MCS_FORCE_INLINE MT cmpNe(SimdType x, SimdType y)
  {
    return vmvnq_u8(vceqq_u8(x, y));
  }

  MCS_FORCE_INLINE MT cmpAlwaysFalse(SimdType x, SimdType y)
  {
    return vdupq_n_u8(0);
  }

  MCS_FORCE_INLINE MT cmpAlwaysTrue(SimdType x, SimdType y)
  {
    return vdupq_n_u8(0xff);
  }

  MCS_FORCE_INLINE MT falseMask()
  {
    return vdupq_n_u8(0);
  }

  MCS_FORCE_INLINE MT trueMask()
  {
    return vdupq_n_u8(0xff);
  }

  MCS_FORCE_INLINE MT nullEmptyCmpNe(SimdType x, SimdType y)
  {
    return cmpNe(x, y);
  }

  // MCS_FORCE_INLINE MaskType nullEmptyCmpNe(MaskType x, MaskType y)
  // {
  //   return cmpNe(x, y);
  // }

  MCS_FORCE_INLINE MT nullEmptyCmpEq(SimdType x, SimdType y)
  {
    return cmpEq(x, y);
  }

  MCS_FORCE_INLINE SimdType setToZero()
  {
    return vdupq_n_u8(0);
  }
  MCS_FORCE_INLINE void store(char* dst, SimdType x)
  {
    vst1q_u8(reinterpret_cast<uint8_t*>(dst), x);
  }
};

};  // namespace simd

#endif

/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016-2021 MariaDB Corporation

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

#include <iostream>
#include <sstream>
//#define NDEBUG
#include <cassert>
#include <cmath>
#include <functional>
#ifndef _MSC_VER
#include <pthread.h>
#else
#endif
using namespace std;

#include <boost/scoped_array.hpp>
using namespace boost;

#include "primitiveprocessor.h"
#include "messagelog.h"
#include "messageobj.h"
#include "we_type.h"
#include "stats.h"
#include "primproc.h"
#include "dataconvert.h"
#include "mcs_decimal.h"
#include "simd_sse.h"
#include "utils/common/columnwidth.h"

using namespace logging;
using namespace dbbc;
using namespace primitives;
using namespace primitiveprocessor;
using namespace execplan;

namespace
{
using MT = uint16_t;

inline uint64_t order_swap(uint64_t x)
{
  uint64_t ret = (x >> 56) | ((x << 40) & 0x00FF000000000000ULL) | ((x << 24) & 0x0000FF0000000000ULL) |
                 ((x << 8) & 0x000000FF00000000ULL) | ((x >> 8) & 0x00000000FF000000ULL) |
                 ((x >> 24) & 0x0000000000FF0000ULL) | ((x >> 40) & 0x000000000000FF00ULL) | (x << 56);
  return ret;
}

template <class T>
inline int compareBlock(const void* a, const void* b)
{
  return ((*(T*)a) - (*(T*)b));
}

// this function is out-of-band, we don't need to inline it
void logIt(int mid, int arg1, const string& arg2 = string())
{
  MessageLog logger(LoggingID(28));
  logging::Message::Args args;
  Message msg(mid);

  args.add(arg1);

  if (arg2.length() > 0)
    args.add(arg2);

  msg.format(args);
  logger.logErrorMessage(msg);
}

template <class T>
inline bool colCompare_(const T& val1, const T& val2, uint8_t COP)
{
  switch (COP)
  {
    case COMPARE_NIL: return false;

    case COMPARE_LT: return val1 < val2;

    case COMPARE_EQ: return val1 == val2;

    case COMPARE_LE: return val1 <= val2;

    case COMPARE_GT: return val1 > val2;

    case COMPARE_NE: return val1 != val2;

    case COMPARE_GE: return val1 >= val2;

    default: logIt(34, COP, "colCompare_"); return false;  // throw an exception here?
  }
}

inline bool colCompareStr(const ColRequestHeaderDataType& type, uint8_t COP, const utils::ConstString& val1,
                          const utils::ConstString& val2)
{
  int error = 0;
  bool rc = primitives::StringComparator(type).op(&error, COP, val1, val2);
  if (error)
  {
    logIt(34, COP, "colCompareStr");
    return false;  // throw an exception here?
  }
  return rc;
}

template <class T>
inline bool colCompare_(const T& val1, const T& val2, uint8_t COP, uint8_t rf)
{
  switch (COP)
  {
    case COMPARE_NIL: return false;

    case COMPARE_LT: return val1 < val2 || (val1 == val2 && (rf & 0x01));

    case COMPARE_LE: return val1 < val2 || (val1 == val2 && rf ^ 0x80);

    case COMPARE_EQ: return val1 == val2 && rf == 0;

    case COMPARE_NE: return val1 != val2 || rf != 0;

    case COMPARE_GE: return val1 > val2 || (val1 == val2 && rf ^ 0x01);

    case COMPARE_GT: return val1 > val2 || (val1 == val2 && (rf & 0x80));

    default: logIt(34, COP, "colCompare_"); return false;  // throw an exception here?
  }
}

//@bug 1828  Like must be a string compare.
inline bool colStrCompare_(uint64_t val1, uint64_t val2, uint8_t COP, uint8_t rf)
{
  switch (COP)
  {
    case COMPARE_NIL: return false;

    case COMPARE_LT: return val1 < val2 || (val1 == val2 && rf != 0);

    case COMPARE_LE: return val1 <= val2;

    case COMPARE_EQ: return val1 == val2 && rf == 0;

    case COMPARE_NE: return val1 != val2 || rf != 0;

    case COMPARE_GE: return val1 > val2 || (val1 == val2 && rf == 0);

    case COMPARE_GT: return val1 > val2;

    case COMPARE_LIKE:
    case COMPARE_NLIKE:
    default: logIt(34, COP, "colStrCompare_"); return false;  // throw an exception here?
  }
}

// Set the minimum and maximum in the return header if we will be doing a block scan and
// we are dealing with a type that is comparable as a 64 bit integer.  Subsequent calls can then
// skip this block if the value being searched is outside of the Min/Max range.
inline bool isMinMaxValid(const NewColRequestHeader* in)
{
  if (in->NVALS != 0)
  {
    return false;
  }
  else
  {
    switch (in->colType.DataType)
    {
      case CalpontSystemCatalog::CHAR: return (in->colType.DataSize < 9);

      case CalpontSystemCatalog::VARCHAR:
      case CalpontSystemCatalog::BLOB:
      case CalpontSystemCatalog::TEXT: return (in->colType.DataSize < 8);

      case CalpontSystemCatalog::TINYINT:
      case CalpontSystemCatalog::SMALLINT:
      case CalpontSystemCatalog::MEDINT:
      case CalpontSystemCatalog::INT:
      case CalpontSystemCatalog::DATE:
      case CalpontSystemCatalog::BIGINT:
      case CalpontSystemCatalog::DATETIME:
      case CalpontSystemCatalog::TIME:
      case CalpontSystemCatalog::TIMESTAMP:
      case CalpontSystemCatalog::UTINYINT:
      case CalpontSystemCatalog::USMALLINT:
      case CalpontSystemCatalog::UMEDINT:
      case CalpontSystemCatalog::UINT:
      case CalpontSystemCatalog::UBIGINT: return true;

      case CalpontSystemCatalog::DECIMAL:
      case CalpontSystemCatalog::UDECIMAL: return (in->colType.DataSize <= datatypes::MAXDECIMALWIDTH);

      default: return false;
    }
  }
}

template <ENUM_KIND KIND, int COL_WIDTH, bool IS_NULL, typename T1, typename T2,
          typename std::enable_if<COL_WIDTH == sizeof(int32_t) && KIND == KIND_FLOAT && !IS_NULL, T1>::type* =
              nullptr>
inline bool colCompareDispatcherT(T1 columnValue, T2 filterValue, uint8_t cop, uint8_t rf,
                                  const ColRequestHeaderDataType& typeHolder, bool isVal2Null)
{
  float dVal1 = *((float*)&columnValue);
  float dVal2 = *((float*)&filterValue);
  return colCompare_(dVal1, dVal2, cop);
}

template <ENUM_KIND KIND, int COL_WIDTH, bool IS_NULL, typename T1, typename T2,
          typename std::enable_if<COL_WIDTH == sizeof(int64_t) && KIND == KIND_FLOAT && !IS_NULL, T1>::type* =
              nullptr>
inline bool colCompareDispatcherT(T1 columnValue, T2 filterValue, uint8_t cop, uint8_t rf,
                                  const ColRequestHeaderDataType& typeHolder, bool isVal2Null)
{
  double dVal1 = *((double*)&columnValue);
  double dVal2 = *((double*)&filterValue);
  return colCompare_(dVal1, dVal2, cop);
}

template <ENUM_KIND KIND, int COL_WIDTH, bool IS_NULL, typename T1, typename T2,
          typename std::enable_if<KIND == KIND_TEXT && !IS_NULL, T1>::type* = nullptr>
inline bool colCompareDispatcherT(T1 columnValue, T2 filterValue, uint8_t cop, uint8_t rf,
                                  const ColRequestHeaderDataType& typeHolder, bool isVal2Null)
{
  if (cop & COMPARE_LIKE)  // LIKE and NOT LIKE
  {
    utils::ConstString subject{reinterpret_cast<const char*>(&columnValue), COL_WIDTH};
    utils::ConstString pattern{reinterpret_cast<const char*>(&filterValue), COL_WIDTH};
    return typeHolder.like(cop & COMPARE_NOT, subject.rtrimZero(), pattern.rtrimZero());
  }

  if (!rf)
  {
    // A temporary hack for xxx_nopad_bin collations
    // TODO: MCOL-4534 Improve comparison performance in 8bit nopad_bin collations
    if ((typeHolder.getCharset().state & (MY_CS_BINSORT | MY_CS_NOPAD)) == (MY_CS_BINSORT | MY_CS_NOPAD))
      return colCompare_(order_swap(columnValue), order_swap(filterValue), cop);
    utils::ConstString s1{reinterpret_cast<const char*>(&columnValue), COL_WIDTH};
    utils::ConstString s2{reinterpret_cast<const char*>(&filterValue), COL_WIDTH};
    return colCompareStr(typeHolder, cop, s1.rtrimZero(), s2.rtrimZero());
  }
  else
    return colStrCompare_(order_swap(columnValue), order_swap(filterValue), cop, rf);
}

// This template where IS_NULL = true is used only comparing filter predicate
// values with column NULL so I left branching here.
template <ENUM_KIND KIND, int COL_WIDTH, bool IS_NULL, typename T1, typename T2,
          typename std::enable_if<IS_NULL, T1>::type* = nullptr>
inline bool colCompareDispatcherT(T1 columnValue, T2 filterValue, uint8_t cop, uint8_t rf,
                                  const ColRequestHeaderDataType& typeHolder, bool isVal2Null)
{
  if (IS_NULL == isVal2Null || (isVal2Null && cop == COMPARE_NE))
  {
    if (KIND_UNSIGNED == KIND)
    {
      // Ugly hack to convert all to the biggest type b/w T1 and T2.
      // I presume that sizeof(T2) AKA a filter predicate type is GEQ sizeof(T1) AKA col type.
      using UT2 = typename datatypes::make_unsigned<T2>::type;
      UT2 ucolumnValue = columnValue;
      UT2 ufilterValue = filterValue;
      return colCompare_(ucolumnValue, ufilterValue, cop, rf);
    }
    else
    {
      // Ugly hack to convert all to the biggest type b/w T1 and T2.
      // I presume that sizeof(T2) AKA a filter predicate type is GEQ sizeof(T1) AKA col type.
      T2 tempVal1 = columnValue;
      return colCompare_(tempVal1, filterValue, cop, rf);
    }
  }
  else
    return false;
}

template <ENUM_KIND KIND, int COL_WIDTH, bool IS_NULL, typename T1, typename T2,
          typename std::enable_if<KIND == KIND_UNSIGNED && !IS_NULL, T1>::type* = nullptr>
inline bool colCompareDispatcherT(T1 columnValue, T2 filterValue, uint8_t cop, uint8_t rf,
                                  const ColRequestHeaderDataType& typeHolder, bool isVal2Null)
{
  if (IS_NULL == isVal2Null || (isVal2Null && cop == COMPARE_NE))
  {
    // Ugly hack to convert all to the biggest type b/w T1 and T2.
    // I presume that sizeof(T2)(a filter predicate type) is GEQ T1(col type).
    using UT2 = typename datatypes::make_unsigned<T2>::type;
    UT2 ucolumnValue = columnValue;
    UT2 ufilterValue = filterValue;
    return colCompare_(ucolumnValue, ufilterValue, cop, rf);
  }
  else
    return false;
}

template <ENUM_KIND KIND, int COL_WIDTH, bool IS_NULL, typename T1, typename T2,
          typename std::enable_if<KIND == KIND_DEFAULT && !IS_NULL, T1>::type* = nullptr>
inline bool colCompareDispatcherT(T1 columnValue, T2 filterValue, uint8_t cop, uint8_t rf,
                                  const ColRequestHeaderDataType& typeHolder, bool isVal2Null)
{
  if (IS_NULL == isVal2Null || (isVal2Null && cop == COMPARE_NE))
  {
    // Ugly hack to convert all to the biggest type b/w T1 and T2.
    // I presume that sizeof(T2)(a filter predicate type) is GEQ T1(col type).
    T2 tempVal1 = columnValue;
    return colCompare_(tempVal1, filterValue, cop, rf);
  }
  else
    return false;
}

// Compare two column values using given comparison operation,
// taking into account all rules about NULL values, string trimming and so on
template <ENUM_KIND KIND, int COL_WIDTH, bool IS_NULL = false, typename T1, typename T2>
inline bool colCompare(T1 columnValue, T2 filterValue, uint8_t cop, uint8_t rf,
                       const ColRequestHeaderDataType& typeHolder, bool isVal2Null = false)
{
  // 	cout << "comparing " << hex << columnValue << " to " << filterValue << endl;
  if (COMPARE_NIL == cop)
    return false;

  return colCompareDispatcherT<KIND, COL_WIDTH, IS_NULL, T1, T2>(columnValue, filterValue, cop, rf,
                                                                 typeHolder, isVal2Null);
}

/*****************************************************************************
 *** NULL/EMPTY VALUES FOR EVERY COLUMN TYPE/WIDTH ***************************
 *****************************************************************************/
// Bit pattern representing EMPTY value for given column type/width
// TBD Use typeHandler
template <typename T, typename std::enable_if<sizeof(T) == sizeof(int128_t), T>::type* = nullptr>
T getEmptyValue(uint8_t type)
{
  return datatypes::Decimal128Empty;
}

template <typename T, typename std::enable_if<sizeof(T) == sizeof(int64_t), T>::type* = nullptr>
T getEmptyValue(uint8_t type)
{
  switch (type)
  {
    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: return joblist::DOUBLEEMPTYROW;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME:
    case CalpontSystemCatalog::VARBINARY:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT: return joblist::CHAR8EMPTYROW;

    case CalpontSystemCatalog::UBIGINT: return joblist::UBIGINTEMPTYROW;

    default: return joblist::BIGINTEMPTYROW;
  }
}

template <typename T, typename std::enable_if<sizeof(T) == sizeof(int32_t), T>::type* = nullptr>
T getEmptyValue(uint8_t type)
{
  switch (type)
  {
    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: return joblist::FLOATEMPTYROW;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: return joblist::CHAR4EMPTYROW;

    case CalpontSystemCatalog::UINT:
    case CalpontSystemCatalog::UMEDINT: return joblist::UINTEMPTYROW;

    default: return joblist::INTEMPTYROW;
  }
}

template <typename T, typename std::enable_if<sizeof(T) == sizeof(int16_t), T>::type* = nullptr>
T getEmptyValue(uint8_t type)
{
  switch (type)
  {
    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: return joblist::CHAR2EMPTYROW;

    case CalpontSystemCatalog::USMALLINT: return joblist::USMALLINTEMPTYROW;

    default: return joblist::SMALLINTEMPTYROW;
  }
}

template <typename T, typename std::enable_if<sizeof(T) == sizeof(int8_t), T>::type* = nullptr>
T getEmptyValue(uint8_t type)
{
  switch (type)
  {
    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: return joblist::CHAR1EMPTYROW;

    case CalpontSystemCatalog::UTINYINT: return joblist::UTINYINTEMPTYROW;

    default: return joblist::TINYINTEMPTYROW;
  }
}

// Bit pattern representing NULL value for given column type/width
// TBD Use TypeHandler
template <typename T, typename std::enable_if<sizeof(T) == sizeof(int128_t), T>::type* = nullptr>
T getNullValue(uint8_t type)
{
  return datatypes::Decimal128Null;
}

template <typename T, typename std::enable_if<sizeof(T) == sizeof(int64_t), T>::type* = nullptr>
T getNullValue(uint8_t type)
{
  switch (type)
  {
    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE: return joblist::DOUBLENULL;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME:
    case CalpontSystemCatalog::VARBINARY:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT: return joblist::CHAR8NULL;

    case CalpontSystemCatalog::UBIGINT: return joblist::UBIGINTNULL;

    default: return joblist::BIGINTNULL;
  }
}

template <typename T, typename std::enable_if<sizeof(T) == sizeof(int32_t), T>::type* = nullptr>
T getNullValue(uint8_t type)
{
  switch (type)
  {
    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT: return joblist::FLOATNULL;

    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT: return joblist::CHAR4NULL;

    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: return joblist::DATENULL;

    case CalpontSystemCatalog::UINT:
    case CalpontSystemCatalog::UMEDINT: return joblist::UINTNULL;

    default: return joblist::INTNULL;
  }
}

template <typename T, typename std::enable_if<sizeof(T) == sizeof(int16_t), T>::type* = nullptr>
T getNullValue(uint8_t type)
{
  switch (type)
  {
    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: return joblist::CHAR2NULL;

    case CalpontSystemCatalog::USMALLINT: return joblist::USMALLINTNULL;

    default: return joblist::SMALLINTNULL;
  }
}

template <typename T, typename std::enable_if<sizeof(T) == sizeof(int8_t), T>::type* = nullptr>
T getNullValue(uint8_t type)
{
  switch (type)
  {
    case CalpontSystemCatalog::CHAR:
    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT:
    case CalpontSystemCatalog::DATE:
    case CalpontSystemCatalog::DATETIME:
    case CalpontSystemCatalog::TIMESTAMP:
    case CalpontSystemCatalog::TIME: return joblist::CHAR1NULL;

    case CalpontSystemCatalog::UTINYINT: return joblist::UTINYINTNULL;

    default: return joblist::TINYINTNULL;
  }
}

// Check whether val is NULL (or alternative NULL bit pattern for 64-bit string types)
template <ENUM_KIND KIND, typename T>
inline bool isNullValue(const T val, const T NULL_VALUE)
{
  return val == NULL_VALUE;
}

template <>
inline bool isNullValue<KIND_TEXT, int64_t>(const int64_t val, const int64_t NULL_VALUE)
{
  //@bug 339 might be a token here
  // TODO: what's up with the alternative NULL here?
  constexpr const int64_t ALT_NULL_VALUE = 0xFFFFFFFFFFFFFFFELL;

  return (val == NULL_VALUE || val == ALT_NULL_VALUE);
}

//
// FILTER A COLUMN VALUE
//

template <bool IS_NULL, typename T, typename FT, typename std::enable_if<IS_NULL == true, T>::type* = nullptr>
inline bool noneValuesInArray(const T curValue, const FT* filterValues, const uint32_t filterCount)
{
  // ignore NULLs in the array and in the column data
  return false;
}

template <bool IS_NULL, typename T, typename FT,
          typename std::enable_if<IS_NULL == false, T>::type* = nullptr>
inline bool noneValuesInArray(const T curValue, const FT* filterValues, const uint32_t filterCount)
{
  for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
  {
    if (curValue == static_cast<T>(filterValues[argIndex]))
      return false;
  }

  return true;
}

template <bool IS_NULL, typename T, typename ST, typename std::enable_if<IS_NULL == true, T>::type* = nullptr>
inline bool noneValuesInSet(const T curValue, const ST* filterSet)
{
  // bug 1920: ignore NULLs in the set and in the column data
  return false;
}

template <bool IS_NULL, typename T, typename ST,
          typename std::enable_if<IS_NULL == false, T>::type* = nullptr>
inline bool noneValuesInSet(const T curValue, const ST* filterSet)
{
  bool found = (filterSet->find(curValue) != filterSet->end());
  return !found;
}

// The routine is used to test the value from a block against filters
// according with columnFilterMode(see the corresponding enum for details).
// Returns true if the curValue matches the filter.
template <ENUM_KIND KIND, int COL_WIDTH, bool IS_NULL = false, typename T, typename FT, typename ST>
inline bool matchingColValue(
    const T curValue, const ColumnFilterMode columnFilterMode,
    const ST* filterSet,  // Set of values for simple filters (any of values / none of them)
    const uint32_t
        filterCount,  // Number of filter elements, each described by one entry in the following arrays:
    const uint8_t* filterCOPs,  //   comparison operation
    const FT* filterValues,     //   value to compare to
    const uint8_t* filterRFs,   // reverse byte order flags
    const ColRequestHeaderDataType& typeHolder,
    const T NULL_VALUE)  // Bit pattern representing NULL value for this column type/width
{
  /* In order to make filtering as fast as possible, we replaced the single generic algorithm
     with several algorithms, better tailored for more specific cases:
     empty filter, single comparison, and/or/xor comparison results, one/none of small/large set of values
  */
  switch (columnFilterMode)
  {
    // Empty filter is always true
    case ALWAYS_TRUE: return true;

    // Filter consisting of exactly one comparison operation
    case SINGLE_COMPARISON:
    {
      auto filterValue = filterValues[0];
      // This can be future optimized checking if a filterValue is NULL or not
      bool cmp =
          colCompare<KIND, COL_WIDTH, IS_NULL>(curValue, filterValue, filterCOPs[0], filterRFs[0], typeHolder,
                                               isNullValue<KIND, T>(filterValue, NULL_VALUE));
      return cmp;
    }

    // Filter is true if ANY comparison is true (BOP_OR)
    case ANY_COMPARISON_TRUE:
    {
      for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
      {
        auto filterValue = filterValues[argIndex];
        // This can be future optimized checking if a filterValues are NULLs or not before the higher level
        // loop.
        bool cmp = colCompare<KIND, COL_WIDTH, IS_NULL>(curValue, filterValue, filterCOPs[argIndex],
                                                        filterRFs[argIndex], typeHolder,
                                                        isNullValue<KIND, T>(filterValue, NULL_VALUE));

        // Short-circuit the filter evaluation - true || ... == true
        if (cmp == true)
          return true;
      }

      // We can get here only if all filters returned false
      return false;
    }

    // Filter is true only if ALL comparisons are true (BOP_AND)
    case ALL_COMPARISONS_TRUE:
    {
      for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
      {
        auto filterValue = filterValues[argIndex];
        // This can be future optimized checking if a filterValues are NULLs or not before the higher level
        // loop.
        bool cmp = colCompare<KIND, COL_WIDTH, IS_NULL>(curValue, filterValue, filterCOPs[argIndex],
                                                        filterRFs[argIndex], typeHolder,
                                                        isNullValue<KIND, T>(filterValue, NULL_VALUE));

        // Short-circuit the filter evaluation - false && ... = false
        if (cmp == false)
          return false;
      }

      // We can get here only if all filters returned true
      return true;
    }

    // XORing results of comparisons (BOP_XOR)
    case XOR_COMPARISONS:
    {
      bool result = false;

      for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
      {
        auto filterValue = filterValues[argIndex];
        // This can be future optimized checking if a filterValues are NULLs or not before the higher level
        // loop.
        bool cmp = colCompare<KIND, COL_WIDTH, IS_NULL>(curValue, filterValue, filterCOPs[argIndex],
                                                        filterRFs[argIndex], typeHolder,
                                                        isNullValue<KIND, T>(filterValue, NULL_VALUE));
        result ^= cmp;
      }

      return result;
    }

    // ONE of the values in the small set represented by an array (BOP_OR + all COMPARE_EQ)
    case ONE_OF_VALUES_IN_ARRAY:
    {
      for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
      {
        if (curValue == static_cast<T>(filterValues[argIndex]))
          return true;
      }

      return false;
    }

    // NONE of the values in the small set represented by an array (BOP_AND + all COMPARE_NE)
    case NONE_OF_VALUES_IN_ARRAY:
      return noneValuesInArray<IS_NULL, T, FT>(curValue, filterValues, filterCount);

    // ONE of the values in the set is equal to the value checked (BOP_OR + all COMPARE_EQ)
    case ONE_OF_VALUES_IN_SET:
    {
      bool found = (filterSet->find(curValue) != filterSet->end());
      return found;
    }

    // NONE of the values in the set is equal to the value checked (BOP_AND + all COMPARE_NE)
    case NONE_OF_VALUES_IN_SET: return noneValuesInSet<IS_NULL, T, ST>(curValue, filterSet);

    default: idbassert(0); return true;
  }
}

/*****************************************************************************
 *** MISC FUNCS **************************************************************
 *****************************************************************************/
// These two are templates update min/max values in the loop iterating the values in filterColumnData.
template <ENUM_KIND KIND, typename T, typename std::enable_if<KIND == KIND_TEXT, T>::type* = nullptr>
inline void updateMinMax(T& Min, T& Max, const T curValue, NewColRequestHeader* in)
{
  constexpr int COL_WIDTH = sizeof(T);
  if (colCompare<KIND_TEXT, COL_WIDTH>(Min, curValue, COMPARE_GT, false, in->colType))
    Min = curValue;

  if (colCompare<KIND_TEXT, COL_WIDTH>(Max, curValue, COMPARE_LT, false, in->colType))
    Max = curValue;
}

template <ENUM_KIND KIND, typename T, typename std::enable_if<KIND != KIND_TEXT, T>::type* = nullptr>
inline void updateMinMax(T& Min, T& Max, const T curValue, NewColRequestHeader* in)
{
  if (Min > curValue)
    Min = curValue;

  if (Max < curValue)
    Max = curValue;
}

// The next templates group sets initial Min/Max values in filterColumnData.
template <ENUM_KIND KIND, typename T, typename std::enable_if<KIND == KIND_TEXT, T>::type* = nullptr>
T getInitialMin(NewColRequestHeader* in)
{
  const CHARSET_INFO& cs = in->colType.getCharset();
  T Min = 0;
  cs.max_str((uchar*)&Min, sizeof(Min), sizeof(Min));
  return Min;
}

template <ENUM_KIND KIND, typename T, typename std::enable_if<KIND != KIND_TEXT, T>::type* = nullptr>
T getInitialMin(NewColRequestHeader* in)
{
  return datatypes::numeric_limits<T>::max();
}

template <ENUM_KIND KIND, typename T,
          typename std::enable_if<KIND != KIND_TEXT && KIND != KIND_UNSIGNED, T>::type* = nullptr>
T getInitialMax(NewColRequestHeader* in)
{
  return datatypes::numeric_limits<T>::min();
}

template <ENUM_KIND KIND, typename T, typename std::enable_if<KIND == KIND_UNSIGNED, T>::type* = nullptr>
T getInitialMax(NewColRequestHeader* in)
{
  return 0;
}

template <ENUM_KIND KIND, typename T, typename std::enable_if<KIND == KIND_TEXT, T>::type* = nullptr>
T getInitialMax(NewColRequestHeader* in)
{
  const CHARSET_INFO& cs = in->colType.getCharset();
  T Max = 0;
  cs.min_str((uchar*)&Max, sizeof(Max), sizeof(Max));
  return Max;
}

/*****************************************************************************
 *** READ COLUMN VALUES ******************************************************
 *****************************************************************************/

// Read one ColValue from the input block.
// Return true on success, false on End of Block.
// Values are read from srcArray either in natural order or in the order defined by ridArray.
// Empty values are skipped, unless ridArray==0 && !(OutputType & OT_RID).
template <typename T, int COL_WIDTH>
inline bool nextColValue(
    T& result,      // Place for the value returned
    bool* isEmpty,  // ... and flag whether it's EMPTY
    uint32_t*
        index,  // Successive index either in srcArray (going from 0 to srcSize-1) or ridArray (0..ridSize-1)
    uint16_t* rid,             // Index in srcArray of the value returned
    const T* srcArray,         // Input array
    const uint32_t srcSize,    // ... and its size
    const uint16_t* ridArray,  // Optional array of indexes into srcArray, that defines the read order
    const uint16_t ridSize,    // ... and its size
    const uint8_t OutputType,  // Used to decide whether to skip EMPTY values
    T EMPTY_VALUE)
{
  auto i = *index;  // local copy of *index to speed up loops
  T value;          // value to be written into *result, local for the same reason

  if (ridArray)
  {
    // Read next non-empty value in the order defined by ridArray
    for (;; i++)
    {
      if (UNLIKELY(i >= ridSize))
        return false;

      value = srcArray[ridArray[i]];

      if (value != EMPTY_VALUE)
        break;
    }

    *rid = ridArray[i];
    *isEmpty = false;
  }
  else if (OutputType & OT_RID)  // TODO: check correctness of this condition for SKIP_EMPTY_VALUES
  {
    // Read next non-empty value in the natural order
    for (;; i++)
    {
      if (UNLIKELY(i >= srcSize))
        return false;

      value = srcArray[i];

      if (value != EMPTY_VALUE)
        break;
    }

    *rid = i;
    *isEmpty = false;
  }
  else
  {
    // Read next value in the natural order
    if (UNLIKELY(i >= srcSize))
      return false;

    *rid = i;
    value = srcArray[i];
    *isEmpty = (value == EMPTY_VALUE);
  }

  *index = i + 1;
  result = value;
  return true;
}

///
/// WRITE COLUMN VALUES
///

// Write the value index in srcArray and/or the value itself, depending on bits in OutputType,
// into the output buffer and update the output pointer.
// TODO Introduce another dispatching layer based on OutputType.
template <typename T>
inline void writeColValue(uint8_t OutputType, ColResultHeader* out, uint16_t rid, const T* srcArray)
{
  // TODO move base ptr calculation one level up.
  uint8_t* outPtr = reinterpret_cast<uint8_t*>(&out[1]);
  auto idx = out->NVALS++;
  if (OutputType & OT_RID)
  {
    auto* outPos = getRIDArrayPosition(outPtr, idx);
    *outPos = rid;
    out->RidFlags |= (1 << (rid >> 9));  // set the (row/512)'th bit
  }

  if (OutputType & (OT_TOKEN | OT_DATAVALUE))
  {
    // TODO move base ptr calculation one level up.
    T* outPos = getValuesArrayPosition<T>(primitives::getFirstValueArrayPosition(out), idx);
    // TODO check bytecode for the 16 byte type
    *outPos = srcArray[rid];
  }
}

#if defined(__x86_64__)
template <typename T, ENUM_KIND KIND, bool HAS_INPUT_RIDS,
          typename std::enable_if<HAS_INPUT_RIDS == false, T>::type* = nullptr>
inline void vectUpdateMinMax(const bool validMinMax, const bool isNonNullOrEmpty, T& Min, T& Max, T curValue,
                             NewColRequestHeader* in)
{
  if (validMinMax && isNonNullOrEmpty)
    updateMinMax<KIND>(Min, Max, curValue, in);
}

// MCS won't update Min/Max for a block if it doesn't read all values in a block.
// This happens if in->NVALS > 0(HAS_INPUT_RIDS is set).
template <typename T, ENUM_KIND KIND, bool HAS_INPUT_RIDS,
          typename std::enable_if<HAS_INPUT_RIDS == true, T>::type* = nullptr>
inline void vectUpdateMinMax(const bool validMinMax, const bool isNonNullOrEmpty, T& Min, T& Max, T curValue,
                             NewColRequestHeader* in)
{
  //
}

template <typename T, bool HAS_INPUT_RIDS,
          typename std::enable_if<HAS_INPUT_RIDS == false, T>::type* = nullptr>
void vectWriteColValuesLoopRIDAsignment(primitives::RIDType* ridDstArray, ColResultHeader* out,
                                        const primitives::RIDType calculatedRID,
                                        const primitives::RIDType* ridSrcArray, const uint32_t srcRIDIdx)
{
  *ridDstArray = calculatedRID;
  out->RidFlags |= (1 << (calculatedRID >> 9));  // set the (row/512)'th bit
}

template <typename T, bool HAS_INPUT_RIDS,
          typename std::enable_if<HAS_INPUT_RIDS == true, T>::type* = nullptr>
void vectWriteColValuesLoopRIDAsignment(primitives::RIDType* ridDstArray, ColResultHeader* out,
                                        const primitives::RIDType calculatedRID,
                                        const primitives::RIDType* ridSrcArray, const uint32_t srcRIDIdx)
{
  *ridDstArray = ridSrcArray[srcRIDIdx];
  out->RidFlags |= (1 << (ridSrcArray[srcRIDIdx] >> 9));  // set the (row/512)'th bit
}

// The set of SFINAE templates are used to write values/RID into the output buffer based on
// a number of template parameters
// No RIDs only values
template <typename T, typename VT, int OUTPUT_TYPE, ENUM_KIND KIND, bool HAS_INPUT_RIDS,
          typename std::enable_if<OUTPUT_TYPE&(OT_TOKEN | OT_DATAVALUE) && !(OUTPUT_TYPE & OT_RID),
                                  T>::type* = nullptr>
inline uint16_t vectWriteColValues(
    VT& simdProcessor,                    // SIMD processor
    const MT writeMask,                   // SIMD intrinsics bitmask for values to write
    const MT nonNullOrEmptyMask,          // SIMD intrinsics inverce bitmask for NULL/EMPTY values
    const bool validMinMax,               // The flag to update Min/Max for a block or not
    const primitives::RIDType ridOffset,  // The first RID value of the dataVecTPtr
    T* dataVecTPtr,                       // Typed SIMD vector from the input block
    char* dstArray,                       // the actual char dst array ptr to start writing values
    T& Min, T& Max,                       // Min/Max of the extent
    NewColRequestHeader* in,              // Proto message
    ColResultHeader* out,                 // Proto message
    primitives::RIDType* ridDstArray,     // The actual dst arrray ptr to start writing RIDs
    primitives::RIDType* ridSrcArray)     // The actual src array ptr to read RIDs
{
  constexpr const uint16_t FilterMaskStep = VT::FilterMaskStep;
  using SimdType = typename VT::SimdType;
  SimdType tmpStorageVector;
  T* tmpDstVecTPtr = reinterpret_cast<T*>(&tmpStorageVector);
  // Saving values based on writeMask into tmp vec.
  // Min/Max processing.
  // The mask is 16 bit long and it describes N elements.
  // N = sizeof(vector type) / WIDTH.
  uint32_t j = 0;
  for (uint32_t it = 0; it < VT::vecByteSize; ++j, it += FilterMaskStep)
  {
    MT bitMapPosition = 1 << it;
    if (writeMask & bitMapPosition)
    {
      *tmpDstVecTPtr = dataVecTPtr[j];
      ++tmpDstVecTPtr;
    }

    vectUpdateMinMax<T, KIND, HAS_INPUT_RIDS>(validMinMax, nonNullOrEmptyMask & bitMapPosition, Min, Max,
                                              dataVecTPtr[j], in);
  }
  // Store the whole vector however one level up the stack
  // vectorizedFiltering() increases the dstArray by a number of
  // actual values written that is the result of this function.
  simdProcessor.store(dstArray, tmpStorageVector);

  return tmpDstVecTPtr - reinterpret_cast<T*>(&tmpStorageVector);
}

// RIDs no values
template <typename T, typename VT, int OUTPUT_TYPE, ENUM_KIND KIND, bool HAS_INPUT_RIDS,
          typename std::enable_if<OUTPUT_TYPE & OT_RID && !(OUTPUT_TYPE & OT_TOKEN), T>::type* = nullptr>
inline uint16_t vectWriteColValues(
    VT& simdProcessor,                    // SIMD processor
    const MT writeMask,                   // SIMD intrinsics bitmask for values to write
    const MT nonNullOrEmptyMask,          // SIMD intrinsics inverce bitmask for NULL/EMPTY values
    const bool validMinMax,               // The flag to update Min/Max for a block or not
    const primitives::RIDType ridOffset,  // The first RID value of the dataVecTPtr
    T* dataVecTPtr,                       // Typed SIMD vector from the input block
    char* dstArray,                       // the actual char dst array ptr to start writing values
    T& Min, T& Max,                       // Min/Max of the extent
    NewColRequestHeader* in,              // Proto message
    ColResultHeader* out,                 // Proto message
    primitives::RIDType* ridDstArray,     // The actual dst arrray ptr to start writing RIDs
    primitives::RIDType* ridSrcArray)     // The actual src array ptr to read RIDs
{
  return 0;
}

// Both RIDs and values
template <typename T, typename VT, int OUTPUT_TYPE, ENUM_KIND KIND, bool HAS_INPUT_RIDS,
          typename std::enable_if<OUTPUT_TYPE == OT_BOTH, T>::type* = nullptr>
inline uint16_t vectWriteColValues(
    VT& simdProcessor,                    // SIMD processor
    const MT writeMask,                   // SIMD intrinsics bitmask for values to write
    const MT nonNullOrEmptyMask,          // SIMD intrinsics inverce bitmask for NULL/EMPTY values
    const bool validMinMax,               // The flag to update Min/Max for a block or not
    const primitives::RIDType ridOffset,  // The first RID value of the dataVecTPtr
    T* dataVecTPtr,                       // Typed SIMD vector from the input block
    char* dstArray,                       // the actual char dst array ptr to start writing values
    T& Min, T& Max,                       // Min/Max of the extent
    NewColRequestHeader* in,              // Proto message
    ColResultHeader* out,                 // Proto message
    primitives::RIDType* ridDstArray,     // The actual dst arrray ptr to start writing RIDs
    primitives::RIDType* ridSrcArray)     // The actual src array ptr to read RIDs
{
  constexpr const uint16_t FilterMaskStep = VT::FilterMaskStep;
  using SimdType = typename VT::SimdType;
  SimdType tmpStorageVector;
  T* tmpDstVecTPtr = reinterpret_cast<T*>(&tmpStorageVector);
  // Saving values based on writeMask into tmp vec.
  // Min/Max processing.
  // The mask is 16 bit long and it describes N elements.
  // N = sizeof(vector type) / WIDTH.
  uint32_t j = 0;
  for (uint32_t it = 0; it < VT::vecByteSize; ++j, it += FilterMaskStep)
  {
    MT bitMapPosition = 1 << it;
    if (writeMask & bitMapPosition)
    {
      *tmpDstVecTPtr = dataVecTPtr[j];
      ++tmpDstVecTPtr;
      vectWriteColValuesLoopRIDAsignment<T, HAS_INPUT_RIDS>(ridDstArray, out, ridOffset + j, ridSrcArray, j);
      ++ridDstArray;
    }
    vectUpdateMinMax<T, KIND, HAS_INPUT_RIDS>(validMinMax, nonNullOrEmptyMask & bitMapPosition, Min, Max,
                                              dataVecTPtr[j], in);
  }
  // Store the whole vector however one level up the stack
  // vectorizedFiltering() increases the dstArray by a number of
  // actual values written that is the result of this function.
  simdProcessor.store(dstArray, tmpStorageVector);

  return tmpDstVecTPtr - reinterpret_cast<T*>(&tmpStorageVector);
}

// RIDs no values
template <typename T, typename VT, int OUTPUT_TYPE, ENUM_KIND KIND, bool HAS_INPUT_RIDS,
          typename std::enable_if<!(OUTPUT_TYPE & (OT_TOKEN | OT_DATAVALUE)) && OUTPUT_TYPE & OT_RID,
                                  T>::type* = nullptr>
inline uint16_t vectWriteRIDValues(
    VT& processor,                        // SIMD processor
    const uint16_t valuesWritten,         // The number of values written to in certain SFINAE cases
    const bool validMinMax,               // The flag to update Min/Max for a block or not
    const primitives::RIDType ridOffset,  // The first RID value of the dataVecTPtr
    T* dataVecTPtr,                       // Typed SIMD vector from the input block
    primitives::RIDType* ridDstArray,     // The actual dst arrray ptr to start writing RIDs
    MT writeMask,                         // SIMD intrinsics bitmask for values to write
    T& Min, T& Max,                       // Min/Max of the extent
    NewColRequestHeader* in,              // Proto message
    ColResultHeader* out,                 // Proto message
    MT nonNullOrEmptyMask,                // SIMD intrinsics inverce bitmask for NULL/EMPTY values
    primitives::RIDType* ridSrcArray)     // The actual src array ptr to read RIDs
{
  constexpr const uint16_t FilterMaskStep = VT::FilterMaskStep;
  primitives::RIDType* origRIDDstArray = ridDstArray;
  // Saving values based on writeMask into tmp vec.
  // Min/Max processing.
  // The mask is 16 bit long and it describes N elements where N = sizeof(vector type) / WIDTH.
  uint16_t j = 0;
  for (uint32_t it = 0; it < VT::vecByteSize; ++j, it += FilterMaskStep)
  {
    MT bitMapPosition = 1 << it;
    if (writeMask & (1 << it))
    {
      vectWriteColValuesLoopRIDAsignment<T, HAS_INPUT_RIDS>(ridDstArray, out, ridOffset + j, ridSrcArray, j);
      ++ridDstArray;
    }
    vectUpdateMinMax<T, KIND, HAS_INPUT_RIDS>(validMinMax, nonNullOrEmptyMask & bitMapPosition, Min, Max,
                                              dataVecTPtr[j], in);
  }
  return ridDstArray - origRIDDstArray;
}

// Both RIDs and values
// vectWriteColValues writes RIDs traversing the writeMask.
template <typename T, typename VT, int OUTPUT_TYPE, ENUM_KIND KIND, bool HAS_INPUT_RIDS,
          typename std::enable_if<OUTPUT_TYPE == OT_BOTH, T>::type* = nullptr>
inline uint16_t vectWriteRIDValues(
    VT& processor,                        // SIMD processor
    const uint16_t valuesWritten,         // The number of values written to in certain SFINAE cases
    const bool validMinMax,               // The flag to update Min/Max for a block or not
    const primitives::RIDType ridOffset,  // The first RID value of the dataVecTPtr
    T* dataVecTPtr,                       // Typed SIMD vector from the input block
    primitives::RIDType* ridDstArray,     // The actual dst arrray ptr to start writing RIDs
    MT writeMask,                         // SIMD intrinsics bitmask for values to write
    T& Min, T& Max,                       // Min/Max of the extent
    NewColRequestHeader* in,              // Proto message
    ColResultHeader* out,                 // Proto message
    MT nonNullOrEmptyMask,                // SIMD intrinsics inverce bitmask for NULL/EMPTY values
    primitives::RIDType* ridSrcArray)     // The actual src array ptr to read RIDs
{
  return valuesWritten;
}

// No RIDs only values
template <typename T, typename VT, int OUTPUT_TYPE, ENUM_KIND KIND, bool HAS_INPUT_RIDS,
          typename std::enable_if<OUTPUT_TYPE&(OT_TOKEN | OT_DATAVALUE) && !(OUTPUT_TYPE & OT_RID),
                                  T>::type* = nullptr>
inline uint16_t vectWriteRIDValues(
    VT& processor,                        // SIMD processor
    const uint16_t valuesWritten,         // The number of values written to in certain SFINAE cases
    const bool validMinMax,               // The flag to update Min/Max for a block or not
    const primitives::RIDType ridOffset,  // The first RID value of the dataVecTPtr
    T* dataVecTPtr,                       // Typed SIMD vector from the input block
    primitives::RIDType* ridDstArray,     // The actual dst arrray ptr to start writing RIDs
    MT writeMask,                         // SIMD intrinsics bitmask for values to write
    T& Min, T& Max,                       // Min/Max of the extent
    NewColRequestHeader* in,              // Proto message
    ColResultHeader* out,                 // Proto message
    MT nonNullOrEmptyMask,                // SIMD intrinsics inverce bitmask for NULL/EMPTY values
    primitives::RIDType* ridSrcArray)     // The actual src array ptr to read RIDs
{
  return valuesWritten;
}
#endif

/*****************************************************************************
 *** RUN DATA THROUGH A COLUMN FILTER ****************************************
 *****************************************************************************/
// TODO turn columnFilterMode into template param to use it in matchingColValue
// This routine filters values in a columnar block processing one scalar at a time.
template <typename T, typename FT, typename ST, ENUM_KIND KIND>
void scalarFiltering(
    NewColRequestHeader* in, ColResultHeader* out, const ColumnFilterMode columnFilterMode,
    const ST* filterSet,  // Set of values for simple filters (any of values / none of them)
    const uint32_t
        filterCount,  // Number of filter elements, each described by one entry in the following arrays:
    const uint8_t* filterCOPs,  //   comparison operation
    const FT* filterValues,     //   value to compare to
    const uint8_t* filterRFs,
    const ColRequestHeaderDataType& typeHolder,  // TypeHolder to use collation-aware ops for char/text.
    const T* srcArray,                           // Input array
    const uint32_t srcSize,                      // ... and its size
    const uint16_t* ridArray,   // Optional array of indexes into srcArray, that defines the read order
    const uint16_t ridSize,     // ... and its size
    const uint32_t initialRID,  // The input block idx to start scanning/filter at.
    const uint8_t outputType,   // Used to decide whether to skip EMPTY values
    const bool validMinMax,     // The flag to store min/max
    T emptyValue,               // Deduced empty value magic
    T nullValue,                // Deduced null value magic
    T Min, T Max, const bool isNullValueMatches)
{
  constexpr int WIDTH = sizeof(T);
  // Loop-local variables
  T curValue = 0;
  primitives::RIDType rid = 0;
  bool isEmpty = false;

  // Loop over the column values, storing those matching the filter, and updating the min..max range
  for (uint32_t i = initialRID; nextColValue<T, WIDTH>(curValue, &isEmpty, &i, &rid, srcArray, srcSize,
                                                       ridArray, ridSize, outputType, emptyValue);)
  {
    if (isEmpty)
      continue;
    else if (isNullValue<KIND, T>(curValue, nullValue))
    {
      // If NULL values match the filter, write curValue to the output buffer
      if (isNullValueMatches)
        writeColValue<T>(outputType, out, rid, srcArray);
    }
    else
    {
      // If curValue matches the filter, write it to the output buffer
      if (matchingColValue<KIND, WIDTH, false>(curValue, columnFilterMode, filterSet, filterCount, filterCOPs,
                                               filterValues, filterRFs, in->colType, nullValue))
      {
        writeColValue<T>(outputType, out, rid, srcArray);
      }

      // Update Min and Max if necessary.  EMPTY/NULL values are processed in other branches.
      if (validMinMax)
        updateMinMax<KIND>(Min, Max, curValue, in);
    }
  }

  // Write captured Min/Max values to *out
  out->ValidMinMax = validMinMax;
  if (validMinMax)
  {
    out->Min = Min;
    out->Max = Max;
  }
}

#if defined(__x86_64__)
template <typename VT, typename SIMD_WRAPPER_TYPE, bool HAS_INPUT_RIDS, typename T,
          typename std::enable_if<HAS_INPUT_RIDS == false, T>::type* = nullptr>
inline SIMD_WRAPPER_TYPE simdDataLoadTemplate(VT& processor, const T* srcArray, const T* origSrcArray,
                                              const primitives::RIDType* ridArray, const uint16_t iter)
{
  return {processor.loadFrom(reinterpret_cast<const char*>(srcArray))};
}

// Scatter-gather implementation
// TODO Move the logic into simd namespace class methods and use intrinsics
template <typename VT, typename SIMD_WRAPPER_TYPE, bool HAS_INPUT_RIDS, typename T,
          typename std::enable_if<HAS_INPUT_RIDS == true, T>::type* = nullptr>
inline SIMD_WRAPPER_TYPE simdDataLoadTemplate(VT& processor, const T* srcArray, const T* origSrcArray,
                                              const primitives::RIDType* ridArray, const uint16_t iter)
{
  constexpr const uint16_t WIDTH = sizeof(T);
  constexpr const uint16_t VECTOR_SIZE = VT::vecByteSize / WIDTH;
  using SimdType = typename VT::SimdType;
  SimdType result;
  T* resultTypedPtr = reinterpret_cast<T*>(&result);
  for (uint32_t i = 0; i < VECTOR_SIZE; ++i)
  {
    resultTypedPtr[i] = origSrcArray[ridArray[i]];
  }

  return {result};
}

// This routine filters input block in a vectorized manner.
// It supports all output types, all input types.
// It doesn't support KIND==TEXT so upper layers filters this KIND out beforehand.
// It doesn't support KIND==FLOAT yet also.
// To reduce branching it first compiles the filter to produce a vector of
// vector processing class methods(actual filters) pointers and a logical function pointer
// to glue the masks produced by actual filters.
// Then it takes a vector of data, run filters and logical function using pointers.
// See the corresponding dispatcher to get more details on vector processing class.
template <typename T, typename VT, bool HAS_INPUT_RIDS, int OUTPUT_TYPE, ENUM_KIND KIND, typename FT,
          typename ST>
void vectorizedFiltering(NewColRequestHeader* in, ColResultHeader* out, const T* srcArray,
                         const uint32_t srcSize, primitives::RIDType* ridArray, const uint16_t ridSize,
                         ParsedColumnFilter* parsedColumnFilter, const bool validMinMax, const T emptyValue,
                         const T nullValue, T Min, T Max, const bool isNullValueMatches)
{
  constexpr const uint16_t WIDTH = sizeof(T);
  using SimdType = typename VT::SimdType;
  using SimdWrapperType = typename VT::SimdWrapperType;
  using FilterType = typename VT::FilterType;
  VT simdProcessor;
  SimdType dataVec;
  SimdType emptyFilterArgVec = simdProcessor.emptyNullLoadValue(emptyValue);
  SimdType nullFilterArgVec = simdProcessor.emptyNullLoadValue(nullValue);
  MT writeMask, nonEmptyMask, nonNullMask, nonNullOrEmptyMask;
  MT initFilterMask = 0xFFFF;
  primitives::RIDType rid = 0;
  primitives::RIDType* origRidArray = ridArray;
  uint16_t totalValuesWritten = 0;
  char* dstArray = reinterpret_cast<char*>(primitives::getFirstValueArrayPosition(out));
  primitives::RIDType* ridDstArray = reinterpret_cast<primitives::RIDType*>(getFirstRIDArrayPosition(out));
  const T* origSrcArray = srcArray;
  const FT* filterValues = nullptr;
  const ParsedColumnFilter::CopsType* filterCOPs = nullptr;
  ColumnFilterMode columnFilterMode = ALWAYS_TRUE;
  const ST* filterSet = nullptr;
  const ParsedColumnFilter::RFsType* filterRFs = nullptr;
  uint8_t outputType = in->OutputType;
  constexpr uint16_t VECTOR_SIZE = VT::vecByteSize / WIDTH;
  // If there are RIDs use its number to get a number of vectorized iterations.
  uint16_t iterNumber = HAS_INPUT_RIDS ? ridSize / VECTOR_SIZE : srcSize / VECTOR_SIZE;
  uint32_t filterCount = 0;
  // These pragmas are to silence GCC warnings
  // warning: ignoring attributes on template argument
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-attributes"
  std::vector<SimdType> filterArgsVectors;
  auto ptrA = std::mem_fn(&VT::cmpEq);
  using COPType = decltype(ptrA);
  std::vector<COPType> copFunctorVec;
#pragma GCC diagnostic pop
  using BOPType = std::function<MT(MT, MT)>;
  BOPType bopFunctor;
  // filter comparators and logical function compilation.
  if (parsedColumnFilter != nullptr)
  {
    filterValues = parsedColumnFilter->getFilterVals<FT>();
    filterCOPs = parsedColumnFilter->prestored_cops.get();
    columnFilterMode = parsedColumnFilter->columnFilterMode;
    filterSet = parsedColumnFilter->getFilterSet<ST>();
    filterRFs = parsedColumnFilter->prestored_rfs.get();
    filterCount = parsedColumnFilter->getFilterCount();
    if (iterNumber > 0)
    {
      copFunctorVec.reserve(filterCount);
      switch (parsedColumnFilter->getBOP())
      {
        case BOP_OR:
          bopFunctor = std::bit_or<MT>();
          initFilterMask = 0;
          break;
        case BOP_AND: bopFunctor = std::bit_and<MT>(); break;
        case BOP_XOR:
          bopFunctor = std::bit_or<MT>();
          initFilterMask = 0;
          break;
        case BOP_NONE:
          // According with the comments in linux-port/primitiveprocessor.h
          // there can't be BOP_NONE with filterCount > 0
          bopFunctor = std::bit_and<MT>();
          break;
        default: idbassert(false);
      }
      filterArgsVectors.reserve(filterCount);
      for (uint32_t j = 0; j < filterCount; ++j)
      {
        // Preload filter argument values only once.
        filterArgsVectors.push_back(simdProcessor.loadValue(*((FilterType*)&filterValues[j])));
        switch (filterCOPs[j])
        {
          case (COMPARE_EQ):
            // Skipping extra filter pass generated by IS NULL
            if (memcmp(&filterValues[j], &nullValue, sizeof(nullValue)) == 0)
              copFunctorVec.push_back(std::mem_fn(&VT::nullEmptyCmpEq));
            else
              copFunctorVec.push_back(std::mem_fn(&VT::cmpEq));
            break;
          case (COMPARE_GE): copFunctorVec.push_back(std::mem_fn(&VT::cmpGe)); break;

          case (COMPARE_GT): copFunctorVec.push_back(std::mem_fn(&VT::cmpGt)); break;
          case (COMPARE_LE): copFunctorVec.push_back(std::mem_fn(&VT::cmpLe)); break;
          case (COMPARE_LT): copFunctorVec.push_back(std::mem_fn(&VT::cmpLt)); break;
          case (COMPARE_NE): copFunctorVec.push_back(std::mem_fn(&VT::cmpNe)); break;
          case (COMPARE_NIL):
            copFunctorVec.push_back(std::mem_fn(&VT::cmpAlwaysFalse));
            break;
            // There are couple other COP, e.g. COMPARE_NOT however they can't be met here
            // b/c MCS 6.x uses COMPARE_NOT for strings with OP_LIKE only. See op2num() for
            // details.

          default: idbassert(false);
        }
      }
    }
  }

  // main loop
  // writeMask tells which values must get into the result. Includes values that matches filters. Can have
  // NULLs. nonEmptyMask tells which vector coords are not EMPTY magics. nonNullMask tells which vector coords
  // are not NULL magics.
  for (uint16_t i = 0; i < iterNumber; ++i)
  {
    primitives::RIDType ridOffset = i * VECTOR_SIZE;
    assert(!HAS_INPUT_RIDS || (HAS_INPUT_RIDS && ridSize >= ridOffset));
    dataVec = simdDataLoadTemplate<VT, SimdWrapperType, HAS_INPUT_RIDS, T>(simdProcessor, srcArray,
                                                                           origSrcArray, ridArray, i)
                  .v;
    nonEmptyMask = simdProcessor.nullEmptyCmpNe(dataVec, emptyFilterArgVec);
    writeMask = nonEmptyMask;
    // NULL check
    nonNullMask = simdProcessor.nullEmptyCmpNe(dataVec, nullFilterArgVec);
    // Exclude NULLs from the resulting set if NULL doesn't match the filters.
    writeMask = isNullValueMatches ? writeMask : writeMask & nonNullMask;
    nonNullOrEmptyMask = nonNullMask & nonEmptyMask;
    // filters
    MT prevFilterMask = initFilterMask;
    // TODO name this mask literal
    MT filterMask = 0xFFFF;
    for (uint32_t j = 0; j < filterCount; ++j)
    {
      // filter using compiled filter and preloaded filter argument
      filterMask = copFunctorVec[j](simdProcessor, dataVec, filterArgsVectors[j]);
      filterMask = bopFunctor(prevFilterMask, filterMask);
      prevFilterMask = filterMask;
    }
    writeMask = writeMask & filterMask;

    T* dataVecTPtr = reinterpret_cast<T*>(&dataVec);

    // vectWriteColValues iterates over the values in the source vec
    // to store values/RIDs into dstArray/ridDstArray.
    // It also sets Min/Max values for the block if eligible.
    // !!! vectWriteColValues increases ridDstArray internally but it doesn't go
    // outside the scope of the memory allocated to out msg.
    // vectWriteColValues is empty if outputMode == OT_RID.
    uint16_t valuesWritten = vectWriteColValues<T, VT, OUTPUT_TYPE, KIND, HAS_INPUT_RIDS>(
        simdProcessor, writeMask, nonNullOrEmptyMask, validMinMax, ridOffset, dataVecTPtr, dstArray, Min, Max,
        in, out, ridDstArray, ridArray);
    // Some outputType modes saves RIDs also. vectWriteRIDValues is empty for
    // OT_DATAVALUE, OT_BOTH(vectWriteColValues takes care about RIDs).
    valuesWritten = vectWriteRIDValues<T, VT, OUTPUT_TYPE, KIND, HAS_INPUT_RIDS>(
        simdProcessor, valuesWritten, validMinMax, ridOffset, dataVecTPtr, ridDstArray, writeMask, Min, Max,
        in, out, nonNullOrEmptyMask, ridArray);

    // Calculate bytes written
    uint16_t bytesWritten = valuesWritten * WIDTH;
    totalValuesWritten += valuesWritten;
    ridDstArray += valuesWritten;
    dstArray += bytesWritten;
    rid += VECTOR_SIZE;
    srcArray += VECTOR_SIZE;
    ridArray += VECTOR_SIZE;
  }

  // Set the number of output values here b/c tail processing can skip this operation.
  out->NVALS = totalValuesWritten;
  // WIP Remove this block
  // Write captured Min/Max values to *out
  out->ValidMinMax = validMinMax;
  if (validMinMax)
  {
    out->Min = Min;
    out->Max = Max;
  }

  // process the tail. scalarFiltering changes out contents, e.g. Min/Max, NVALS, RIDs and values array
  // This tail also sets out::Min/Max, out::validMinMax if validMinMax is set.
  uint32_t processedSoFar = rid;
  scalarFiltering<T, FT, ST, KIND>(in, out, columnFilterMode, filterSet, filterCount, filterCOPs,
                                   filterValues, filterRFs, in->colType, origSrcArray, srcSize, origRidArray,
                                   ridSize, processedSoFar, outputType, validMinMax, emptyValue, nullValue,
                                   Min, Max, isNullValueMatches);
}

// This routine dispatches template function calls to reduce branching.
template <typename STORAGE_TYPE, ENUM_KIND KIND, typename FT, typename ST>
void vectorizedFilteringDispatcher(NewColRequestHeader* in, ColResultHeader* out,
                                   const STORAGE_TYPE* srcArray, const uint32_t srcSize, uint16_t* ridArray,
                                   const uint16_t ridSize, ParsedColumnFilter* parsedColumnFilter,
                                   const bool validMinMax, const STORAGE_TYPE emptyValue,
                                   const STORAGE_TYPE nullValue, STORAGE_TYPE Min, STORAGE_TYPE Max,
                                   const bool isNullValueMatches)
{
  // Using struct to dispatch SIMD type based on integral type T.
  using SimdType = typename simd::IntegralToSIMD<STORAGE_TYPE, KIND>::type;
  using FilterType = typename simd::StorageToFiltering<STORAGE_TYPE, KIND>::type;
  using VT = typename simd::SimdFilterProcessor<SimdType, FilterType>;
  bool hasInputRIDs = (in->NVALS > 0) ? true : false;
  if (hasInputRIDs)
  {
    const bool hasInput = true;
    switch (in->OutputType)
    {
      case OT_RID:
        vectorizedFiltering<STORAGE_TYPE, VT, hasInput, OT_RID, KIND, FT, ST>(
            in, out, srcArray, srcSize, ridArray, ridSize, parsedColumnFilter, validMinMax, emptyValue,
            nullValue, Min, Max, isNullValueMatches);
        break;
      case OT_BOTH:
        vectorizedFiltering<STORAGE_TYPE, VT, hasInput, OT_BOTH, KIND, FT, ST>(
            in, out, srcArray, srcSize, ridArray, ridSize, parsedColumnFilter, validMinMax, emptyValue,
            nullValue, Min, Max, isNullValueMatches);
        break;
      case OT_TOKEN:
        vectorizedFiltering<STORAGE_TYPE, VT, hasInput, OT_TOKEN, KIND, FT, ST>(
            in, out, srcArray, srcSize, ridArray, ridSize, parsedColumnFilter, validMinMax, emptyValue,
            nullValue, Min, Max, isNullValueMatches);
        break;
      case OT_DATAVALUE:
        vectorizedFiltering<STORAGE_TYPE, VT, hasInput, OT_DATAVALUE, KIND, FT, ST>(
            in, out, srcArray, srcSize, ridArray, ridSize, parsedColumnFilter, validMinMax, emptyValue,
            nullValue, Min, Max, isNullValueMatches);
        break;
    }
  }
  else
  {
    const bool hasInput = false;
    switch (in->OutputType)
    {
      case OT_RID:
        vectorizedFiltering<STORAGE_TYPE, VT, hasInput, OT_RID, KIND, FT, ST>(
            in, out, srcArray, srcSize, ridArray, ridSize, parsedColumnFilter, validMinMax, emptyValue,
            nullValue, Min, Max, isNullValueMatches);
        break;
      case OT_BOTH:
        vectorizedFiltering<STORAGE_TYPE, VT, hasInput, OT_BOTH, KIND, FT, ST>(
            in, out, srcArray, srcSize, ridArray, ridSize, parsedColumnFilter, validMinMax, emptyValue,
            nullValue, Min, Max, isNullValueMatches);
        break;
      case OT_TOKEN:
        vectorizedFiltering<STORAGE_TYPE, VT, hasInput, OT_TOKEN, KIND, FT, ST>(
            in, out, srcArray, srcSize, ridArray, ridSize, parsedColumnFilter, validMinMax, emptyValue,
            nullValue, Min, Max, isNullValueMatches);
        break;
      case OT_DATAVALUE:
        vectorizedFiltering<STORAGE_TYPE, VT, hasInput, OT_DATAVALUE, KIND, FT, ST>(
            in, out, srcArray, srcSize, ridArray, ridSize, parsedColumnFilter, validMinMax, emptyValue,
            nullValue, Min, Max, isNullValueMatches);
        break;
    }
  }
}
#endif

// TBD Make changes in Command class ancestors to threat BPP::values as buffer.
// TBD this will allow to copy values only once from BPP::blockData to the destination.
// This template contains the main scanning/filtering loop.
// Copy data matching parsedColumnFilter from input to output.
// Input is srcArray[srcSize], optionally accessed in the order defined by ridArray[ridSize].
// Output is buf: ColResponseHeader, RIDType[BLOCK_SIZE], T[BLOCK_SIZE].
template <typename T, ENUM_KIND KIND>
void filterColumnData(NewColRequestHeader* in, ColResultHeader* out, uint16_t* ridArray,
                      const uint16_t ridSize,  // Number of values in ridArray
                      int* srcArray16, const uint32_t srcSize,
                      boost::shared_ptr<ParsedColumnFilter> parsedColumnFilter)
{
  using FT = typename IntegralTypeToFilterType<T>::type;
  using ST = typename IntegralTypeToFilterSetType<T>::type;
  constexpr int WIDTH = sizeof(T);
  const T* srcArray = reinterpret_cast<const T*>(srcArray16);

  // Cache some structure fields in local vars
  auto dataType = (CalpontSystemCatalog::ColDataType)in->colType.DataType;  // Column datatype
  uint32_t filterCount = in->NOPS;  // Number of elements in the filter
  uint8_t outputType = in->OutputType;

  // If no pre-parsed column filter is set, parse the filter in the message
  if (parsedColumnFilter.get() == nullptr && filterCount > 0)
    parsedColumnFilter = _parseColumnFilter<T>(in->getFilterStringPtr(), dataType, filterCount, in->BOP);

  // Cache parsedColumnFilter fields in local vars
  auto columnFilterMode = filterCount == 0 ? ALWAYS_TRUE : parsedColumnFilter->columnFilterMode;
  FT* filterValues = filterCount == 0 ? nullptr : parsedColumnFilter->getFilterVals<FT>();
  auto filterCOPs = filterCount == 0 ? nullptr : parsedColumnFilter->prestored_cops.get();
  auto filterRFs = filterCount == 0 ? nullptr : parsedColumnFilter->prestored_rfs.get();
  ST* filterSet = filterCount == 0 ? nullptr : parsedColumnFilter->getFilterSet<ST>();

  // Bit patterns in srcArray[i] representing EMPTY and NULL values
  T emptyValue = getEmptyValue<T>(dataType);
  T nullValue = getNullValue<T>(dataType);

  // Precompute filter results for NULL values
  bool isNullValueMatches =
      matchingColValue<KIND, WIDTH, true>(nullValue, columnFilterMode, filterSet, filterCount, filterCOPs,
                                          filterValues, filterRFs, in->colType, nullValue);

  // ###########################
  // Boolean indicating whether to capture the min and max values
  bool validMinMax = isMinMaxValid(in);
  T Min = getInitialMin<KIND, T>(in);
  T Max = getInitialMax<KIND, T>(in);

  // Vectorized scanning/filtering for all numerics except float/double types.
  // If the total number of input values can't fill a vector the vector path
  // applies scalar filtering.
  // Syscat queries mustn't follow vectorized processing path b/c PP must return
  // all values w/o any filter(even empty values filter) applied.

#if defined(__x86_64__)
  // Don't use vectorized filtering for text based data types.
  if (KIND <= KIND_FLOAT && WIDTH < 16)
  {
    bool canUseFastFiltering = true;
    for (uint32_t i = 0; i < filterCount; ++i)
      if (filterRFs[i] != 0)
        canUseFastFiltering = false;

    if (canUseFastFiltering)
    {
      vectorizedFilteringDispatcher<T, KIND, FT, ST>(in, out, srcArray, srcSize, ridArray, ridSize,
                                                     parsedColumnFilter.get(), validMinMax, emptyValue,
                                                     nullValue, Min, Max, isNullValueMatches);
      return;
    }
  }
#endif
  uint32_t initialRID = 0;
  scalarFiltering<T, FT, ST, KIND>(in, out, columnFilterMode, filterSet, filterCount, filterCOPs,
                                   filterValues, filterRFs, in->colType, srcArray, srcSize, ridArray, ridSize,
                                   initialRID, outputType, validMinMax, emptyValue, nullValue, Min, Max,
                                   isNullValueMatches);
}  // end of filterColumnData

}  // namespace

namespace primitives
{
// The routine used to dispatch CHAR|VARCHAR|TEXT|BLOB scan.
inline bool isDictTokenScan(NewColRequestHeader* in)
{
  switch (in->colType.DataType)
  {
    case CalpontSystemCatalog::CHAR: return (in->colType.DataSize > 8);

    case CalpontSystemCatalog::VARCHAR:
    case CalpontSystemCatalog::BLOB:
    case CalpontSystemCatalog::TEXT: return (in->colType.DataSize > 7);
    default: return false;
  }
}

// A set of dispatchers for different column widths/integral types.
template <typename T,
// Remove this ugly preprocessor macrosses when RHEL7 reaches EOL.
// This ugly preprocessor if is here b/c of templated class method parameter default value syntax diff b/w gcc
// versions.
#ifdef __GNUC__
#if ___GNUC__ >= 5
          typename std::enable_if<sizeof(T) == sizeof(int32_t), T>::type* = nullptr>  // gcc >= 5
#else
          typename std::enable_if<sizeof(T) == sizeof(int32_t), T>::type*>  // gcc 4.8.5
#endif
#else
          typename std::enable_if<sizeof(T) == sizeof(int32_t), T>::type* = nullptr>
#endif
void PrimitiveProcessor::scanAndFilterTypeDispatcher(NewColRequestHeader* in, ColResultHeader* out)
{
  constexpr int W = sizeof(T);
  auto dataType = (execplan::CalpontSystemCatalog::ColDataType)in->colType.DataType;
  if (dataType == execplan::CalpontSystemCatalog::FLOAT)
  {
    const uint16_t ridSize = in->NVALS;
    uint16_t* ridArray = in->getRIDArrayPtr(W);
    const uint32_t itemsPerBlock = logicalBlockMode ? BLOCK_SIZE : BLOCK_SIZE / W;
    filterColumnData<T, KIND_FLOAT>(in, out, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);
    return;
  }
  _scanAndFilterTypeDispatcher<T>(in, out);
}

template <typename T,
#ifdef __GNUC__
#if ___GNUC__ >= 5
          typename std::enable_if<sizeof(T) == sizeof(int64_t), T>::type* = nullptr>  // gcc >= 5
#else
          typename std::enable_if<sizeof(T) == sizeof(int64_t), T>::type*>  // gcc 4.8.5
#endif
#else
          typename std::enable_if<sizeof(T) == sizeof(int64_t), T>::type* = nullptr>
#endif
void PrimitiveProcessor::scanAndFilterTypeDispatcher(NewColRequestHeader* in, ColResultHeader* out)
{
  constexpr int W = sizeof(T);
  auto dataType = (execplan::CalpontSystemCatalog::ColDataType)in->colType.DataType;
  if (dataType == execplan::CalpontSystemCatalog::DOUBLE)
  {
    const uint16_t ridSize = in->NVALS;
    uint16_t* ridArray = in->getRIDArrayPtr(W);
    const uint32_t itemsPerBlock = logicalBlockMode ? BLOCK_SIZE : BLOCK_SIZE / W;
    filterColumnData<T, KIND_FLOAT>(in, out, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);
    return;
  }
  _scanAndFilterTypeDispatcher<T>(in, out);
}

template <typename T, typename std::enable_if<sizeof(T) == sizeof(int8_t) || sizeof(T) == sizeof(int16_t) ||
#ifdef __GNUC__
#if ___GNUC__ >= 5
                                                  sizeof(T) == sizeof(int128_t),
                                              T>::type* = nullptr>  // gcc >= 5
#else
                                                  sizeof(T) == sizeof(int128_t),
                                              T>::type*>                     // gcc 4.8.5
#endif
#else
                                                  sizeof(T) == sizeof(int128_t),
                                              T>::type* = nullptr>
#endif
void PrimitiveProcessor::scanAndFilterTypeDispatcher(NewColRequestHeader* in, ColResultHeader* out)
{
  _scanAndFilterTypeDispatcher<T>(in, out);
}

template <typename T,
#ifdef __GNUC__
#if ___GNUC__ >= 5
          typename std::enable_if<sizeof(T) == sizeof(int128_t), T>::type* = nullptr>  // gcc >= 5
#else
          typename std::enable_if<sizeof(T) == sizeof(int128_t), T>::type*>  // gcc 4.8.5
#endif
#else
          typename std::enable_if<sizeof(T) == sizeof(int128_t), T>::type* = nullptr>
#endif
void PrimitiveProcessor::_scanAndFilterTypeDispatcher(NewColRequestHeader* in, ColResultHeader* out)
{
  constexpr int W = sizeof(T);
  const uint16_t ridSize = in->NVALS;
  uint16_t* ridArray = in->getRIDArrayPtr(W);
  const uint32_t itemsPerBlock = logicalBlockMode ? BLOCK_SIZE : BLOCK_SIZE / W;

  filterColumnData<T, KIND_DEFAULT>(in, out, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);
}

template <typename T,
#ifdef __GNUC__
#if ___GNUC__ >= 5
          typename std::enable_if<sizeof(T) <= sizeof(int64_t), T>::type* = nullptr>  // gcc >= 5
#else
          typename std::enable_if<sizeof(T) <= sizeof(int64_t), T>::type*>   // gcc 4.8.5
#endif
#else
          typename std::enable_if<sizeof(T) <= sizeof(int64_t), T>::type* = nullptr>
#endif
void PrimitiveProcessor::_scanAndFilterTypeDispatcher(NewColRequestHeader* in, ColResultHeader* out)
{
  constexpr int W = sizeof(T);
  const uint16_t ridSize = in->NVALS;
  uint16_t* ridArray = in->getRIDArrayPtr(W);
  const uint32_t itemsPerBlock = logicalBlockMode ? BLOCK_SIZE : BLOCK_SIZE / W;

  auto dataType = (execplan::CalpontSystemCatalog::ColDataType)in->colType.DataType;
  if ((dataType == execplan::CalpontSystemCatalog::CHAR ||
       dataType == execplan::CalpontSystemCatalog::VARCHAR ||
       dataType == execplan::CalpontSystemCatalog::TEXT) &&
      !isDictTokenScan(in))
  {
    filterColumnData<T, KIND_TEXT>(in, out, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);
    return;
  }

  if (datatypes::isUnsigned(dataType))
  {
    using UT = typename std::conditional<std::is_unsigned<T>::value || datatypes::is_uint128_t<T>::value, T,
                                         typename datatypes::make_unsigned<T>::type>::type;
    filterColumnData<UT, KIND_UNSIGNED>(in, out, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);
    return;
  }
  filterColumnData<T, KIND_DEFAULT>(in, out, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);
}

// The entrypoint for block scanning and filtering.
// The block is in in msg, out msg is used to store values|RIDs matched.
template <typename T>
void PrimitiveProcessor::columnScanAndFilter(NewColRequestHeader* in, ColResultHeader* out)
{
#ifdef PRIM_DEBUG
  auto markEvent = [&](char eventChar)
  {
    if (fStatsPtr)
      fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, eventChar);
  };
#endif
  constexpr int W = sizeof(T);

  void* outp = static_cast<void*>(out);
  memcpy(outp, in, sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
  out->NVALS = 0;
  out->LBID = in->LBID;
  out->ism.Command = COL_RESULTS;
  out->OutputType = in->OutputType;
  out->RidFlags = 0;
  //...Initialize I/O counts;
  out->CacheIO = 0;
  out->PhysicalIO = 0;

#if 0
    // short-circuit the actual block scan for testing
    if (out->LBID >= 802816)
    {
        out->ValidMinMax = false;
        out->Min = 0;
        out->Max = 0;
        return;
    }
#endif

#ifdef PRIM_DEBUG
  markEvent('B');
#endif

  // Sort ridArray (the row index array) if there are RIDs with this in msg
  in->sortRIDArrayIfNeeded(W);
  scanAndFilterTypeDispatcher<T>(in, out);
#ifdef PRIM_DEBUG
  markEvent('C');
#endif
}

template void primitives::PrimitiveProcessor::columnScanAndFilter<int8_t>(NewColRequestHeader*,
                                                                          ColResultHeader*);
template void primitives::PrimitiveProcessor::columnScanAndFilter<int16_t>(NewColRequestHeader*,
                                                                           ColResultHeader*);
template void primitives::PrimitiveProcessor::columnScanAndFilter<int32_t>(NewColRequestHeader*,
                                                                           ColResultHeader*);
template void primitives::PrimitiveProcessor::columnScanAndFilter<int64_t>(NewColRequestHeader*,
                                                                           ColResultHeader*);
template void primitives::PrimitiveProcessor::columnScanAndFilter<int128_t>(NewColRequestHeader*,
                                                                            ColResultHeader*);

}  // namespace primitives
// vim:ts=4 sw=4:

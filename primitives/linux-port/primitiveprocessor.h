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

/******************************************************************************
 * $Id: primitiveprocessor.h 2035 2013-01-21 14:12:19Z rdempsey $
 *
 *****************************************************************************/

/** @file */

#ifndef PRIMITIVEPROCESSOR_H_
#define PRIMITIVEPROCESSOR_H_

#include <stdexcept>
#include <vector>
#ifndef _MSC_VER
#include <tr1/unordered_set>
#else
#include <unordered_set>
#endif

#ifdef __linux__
#define POSIX_REGEX
#endif

#ifdef POSIX_REGEX
#include <regex.h>
#else
#include <boost/regex.hpp>
#endif
#include <cstddef>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>

#include "primitivemsg.h"
#include "calpontsystemcatalog.h"
#include "stats.h"
#include "primproc.h"
#include "hasher.h"

class PrimTest;

namespace primitives
{
enum ColumnFilterMode
{
  ALWAYS_TRUE,              // empty filter is always true
  SINGLE_COMPARISON,        // exactly one comparison operation
  ANY_COMPARISON_TRUE,      // ANY comparison is true (BOP_OR)
  ALL_COMPARISONS_TRUE,     // ALL comparisons are true (BOP_AND)
  XOR_COMPARISONS,          // XORing results of comparisons (BOP_XOR)
  ONE_OF_VALUES_IN_SET,     // ONE of the values in the set is equal to the value checked (BOP_OR + all
                            // COMPARE_EQ)
  NONE_OF_VALUES_IN_SET,    // NONE of the values in the set is equal to the value checked (BOP_AND + all
                            // COMPARE_NE)
  ONE_OF_VALUES_IN_ARRAY,   // ONE of the values in the small set represented by an array (BOP_OR + all
                            // COMPARE_EQ)
  NONE_OF_VALUES_IN_ARRAY,  // NONE of the values in the small set represented by an array (BOP_AND + all
                            // COMPARE_NE)
};

// TBD Test if avalance makes lookup in the hash maps based on this hashers faster.
class pcfHasher
{
 public:
  inline size_t operator()(const int64_t i) const
  {
    return i;
  }
};

class pcfEqual
{
 public:
  inline size_t operator()(const int64_t f1, const int64_t f2) const
  {
    return f1 == f2;
  }
};

class pcfHasher128
{
 public:
  inline size_t operator()(const int128_t i) const
  {
    return *reinterpret_cast<const uint64_t*>(&i);
  }
};

class pcfEqual128
{
 public:
  inline bool operator()(const int128_t f1, const int128_t f2) const
  {
    return f1 == f2;
  }
};

// TBD Test robinhood as tr1 set replacement.
typedef std::tr1::unordered_set<int64_t, pcfHasher, pcfEqual> prestored_set_t;
typedef std::tr1::unordered_set<int128_t, pcfHasher128, pcfEqual128> prestored_set_t_128;

class DictEqualityFilter : public std::tr1::unordered_set<std::string, datatypes::CollationAwareHasher,
                                                          datatypes::CollationAwareComparator>
{
 public:
  DictEqualityFilter(const datatypes::Charset& cs)
   : std::tr1::unordered_set<std::string, datatypes::CollationAwareHasher,
                             datatypes::CollationAwareComparator>(10, datatypes::CollationAwareHasher(cs),
                                                                  datatypes::CollationAwareComparator(cs))
  {
  }
  CHARSET_INFO& getCharset() const
  {
    idbassert(&_M_h1.getCharset() == &_M_eq.getCharset());
    return _M_h1.getCharset();
  }
};

// Not the safest way b/c it doesn't cover uint128_t but the type
// isn't used now and the approach saves some space.
template <typename T, typename D = void>
struct IntegralTypeToFilterType
{
  using type = int64_t;
};

template <>
struct IntegralTypeToFilterType<int128_t>
{
  using type = int128_t;
};

template <typename T, typename D = void>
struct IntegralTypeToFilterSetType
{
  using type = prestored_set_t;
};

template <>
struct IntegralTypeToFilterSetType<int128_t>
{
  using type = prestored_set_t_128;
};

// DRRTuy shared_arrays and shared_ptr looks redundant here
// given that the higher level code uses shared_ptr<ParsedColumnFilter>
// thus runtime calls ParsedColumnFilter dtor in the end.
class ParsedColumnFilter
{
 public:
  using CopsType = uint8_t;
  using RFsType = uint8_t;
  static constexpr uint32_t noSetFilterThreshold = 8;
  ColumnFilterMode columnFilterMode;
  // Very unfortunately prestored_argVals can also be used to store double/float values.
  boost::shared_array<int64_t> prestored_argVals;
  boost::shared_array<int128_t> prestored_argVals128;
  boost::shared_array<CopsType> prestored_cops;
  boost::shared_array<uint8_t> prestored_rfs;
  boost::shared_ptr<prestored_set_t> prestored_set;
  boost::shared_ptr<prestored_set_t_128> prestored_set_128;

  ParsedColumnFilter();
  ParsedColumnFilter(const uint32_t aFilterCount, const int BOP);
  ~ParsedColumnFilter();

  template <typename T, typename std::enable_if<std::is_same<T, int64_t>::value, T>::type* = nullptr>
  T* getFilterVals()
  {
    return reinterpret_cast<T*>(prestored_argVals.get());
  }

  template <typename T, typename std::enable_if<std::is_same<T, int128_t>::value, T>::type* = nullptr>
  T* getFilterVals()
  {
    return prestored_argVals128.get();
  }

  template <typename T, typename std::enable_if<std::is_same<T, prestored_set_t>::value, T>::type* = nullptr>
  T* getFilterSet()
  {
    return prestored_set.get();
  }

  template <typename T,
            typename std::enable_if<std::is_same<T, prestored_set_t_128>::value, T>::type* = nullptr>
  T* getFilterSet()
  {
    return prestored_set_128.get();
  }

  template <typename T, typename std::enable_if<sizeof(T) <= sizeof(int64_t), T>::type* = nullptr>
  void storeFilterArg(const uint32_t argIndex, const T* argValPtr)
  {
    prestored_argVals[argIndex] = *argValPtr;
  }

  template <typename WT, typename std::enable_if<sizeof(WT) == sizeof(int128_t), WT>::type* = nullptr>
  void storeFilterArg(const uint32_t argIndex, const WT* argValPtr)
  {
    datatypes::TSInt128::assignPtrPtr(&(prestored_argVals128[argIndex]), argValPtr);
  }

  template <typename T, typename std::enable_if<sizeof(T) <= sizeof(int64_t), T>::type* = nullptr>
  void allocateSpaceForFilterArgs()
  {
    prestored_argVals.reset(new int64_t[mFilterCount]);
  }

  template <typename WT, typename std::enable_if<sizeof(WT) == sizeof(int128_t), WT>::type* = nullptr>
  void allocateSpaceForFilterArgs()
  {
    prestored_argVals128.reset(new int128_t[mFilterCount]);
  }

  template <typename WT, typename std::enable_if<sizeof(WT) == sizeof(int128_t), WT>::type* = nullptr>
  void populatePrestoredSet()
  {
    prestored_set_128.reset(new prestored_set_t_128());

    // @bug 2584, use COMPARE_NIL for "= null" to allow "is null" in OR expression
    for (uint32_t argIndex = 0; argIndex < mFilterCount; ++argIndex)
      if (prestored_rfs[argIndex] == 0)
        prestored_set_128->insert(prestored_argVals128[argIndex]);
  }

  template <typename T, typename std::enable_if<sizeof(T) <= sizeof(int64_t), T>::type* = nullptr>
  void populatePrestoredSet()
  {
    prestored_set.reset(new prestored_set_t());

    // @bug 2584, use COMPARE_NIL for "= null" to allow "is null" in OR expression
    for (uint32_t argIndex = 0; argIndex < mFilterCount; argIndex++)
      if (prestored_rfs[argIndex] == 0)
        prestored_set->insert(prestored_argVals[argIndex]);
  }

  inline int getBOP() const
  {
    return mBOP;
  }

  inline int getFilterCount() const
  {
    return mFilterCount;
  }

 private:
  uint32_t mFilterCount;
  int mBOP;
};

//@bug 1828 These need to be public so that column operations can use it for 'like'
struct p_DataValue
{
  int len;
  const uint8_t* data;
};

/** @brief This class encapsulates the primitive processing functionality of the system.
 *
 *  This class encapsulates the primitive processing functionality of the system.
 */
class PrimitiveProcessor
{
 public:
  PrimitiveProcessor(int debugLevel = 0);
  virtual ~PrimitiveProcessor();

  /** @brief Sets the block to operate on
   *
   * The primitive processing functions operate on one block at a time.  The caller
   * sets which block to operate on next with this function.
   */
  void setBlockPtr(int* data)
  {
    block = data;
  }
  void setPMStatsPtr(dbbc::Stats* p)
  {
    fStatsPtr = p;
  }

  /** @brief The interface to Mark's NIOS primitive processing code.
   *
   * The interface to Mark's NIOS primitive processing code.  Instead of reading
   * and writing to a bus, it will read/write to buffers specified by inBuf
   * and outBuf.  The primitives implemented this way are:
   * - p_Col and p_ColAggregate
   * - p_GetSignature
   *
   * @param inBuf (in) The buffer containing a command to execute
   * @param inLength (in) The size of inBuf in 4-byte words
   * @param outBuf (in) The buffer to store the output in
   * @param outLength (in) The size of outBuf in 4-byte words
   * @param written (out) The number of bytes written to outBuf.
   * @note Throws logic_error if the output buffer is too small for the result.
   */
  void processBuffer(int* inBuf, unsigned inLength, int* outBuf, unsigned outLength, unsigned* written);

  /* Patrick */

  /** @brief The p_TokenByScan primitive processor
   *
   * The p_TokenByScan primitive processor.  It relies on the caller setting
   * the block to operate on with setBlockPtr().  It assumes the continuation
   * pointer is not used.
   * @param t (in) The arguments to the primitive
   * @param out (out) This must point to memory of some currently unknown max size
   * @param outSize (in) The size of the output buffer in bytes.
   * @note Throws logic_error if the output buffer is too small for the result.
   */
  void p_TokenByScan(const TokenByScanRequestHeader* t, TokenByScanResultHeader* out, unsigned outSize,
                     boost::shared_ptr<DictEqualityFilter> eqFilter);

  /** @brief The p_IdxWalk primitive processor
   *
   * The p_IdxWalk primitive processor.  The caller must set the block to operate
   * on with setBlockPtr().  This primitive can return intermediate results.
   * All results returned will have an different LBID than the input.  They can
   * also be in varying states of completion.  A result is final when
   * Shift >= SSlen, otherwise it is intermediate and needs to be reissued with
   * the specified LBID loaded.
   * @note If in->NVALS > 2, new vectors may be returned in the result set, which
   * will have to be deleted by the caller.  The test to use right now is
   * ({element}->NVALS > 2 && {element}->State == 0).  If that condition is true,
   * delete the vector, otherwise don't.  This kludginess is for efficiency's sake
   * and may go away for the sake of sanity later.
   * @note It is safe to delete any vector passed in after the call.
   * @param out The caller should pass in an empty vector.  The results
   * will be returned as elements of this vector.
   */
  void p_IdxWalk(const IndexWalkHeader* in, std::vector<IndexWalkHeader*>* out) throw();

  /** @brief The p_IdxList primitive processor.
   *
   * The p_IdxList primitive processor.  The caller must set the block to operate
   * on with setBlockPtr().  This primitive can return one intermediate result
   * for every call made.  If there is an intermediate result returned, it will
   * be the first element, distinguished by its type field.  If the
   * first element has a type == RID (3) , there is no intermediate result.  If
   * the first element had a type == LLP_SUBBLK (4) or type == LLP_BLK (5),
   * that element is the intermediate result.  Its value field will be a pointer
   * to the next section of the list.
   *
   * @param rqst (in) The request header followed by NVALS IndexWalkParams
   * @param rslt (out) The caller passes in a buffer which will be filled
   * by the primitive on return.  It will consist of an IndexListHeader,
   * followed by NVALS IndexListEntrys.
   * @param mode (optional, in) 0 specifies old behavior (the last entry of a block might
   * be a pointer).  1 specifies new behavior (the last entry should be ignored).
   */
  void p_IdxList(const IndexListHeader* rqst, IndexListHeader* rslt, int mode = 1);

  /** @brief The p_Col primitive processor.
   *
   * The p_Col primitive processor.  It operates on a column block specified using setBlockPtr().
   * @param in The buffer containing the command parameters.
   * 		The buffer should begin with a NewColRequestHeader structure, followed by
   * 		an array of 'NOPS' defining the filter to apply (optional),
   * 		followed by an array of RIDs to apply the filter to (optional).
   * @param out The buffer that will contain the results.  On return, it will start with
   * a ColResultHeader, followed by the output type specified by in->OutputType.
   * \li If OT_RID, it will be an array of RIDs
   * \li If OT_DATAVALUE, it will be an array of matching data values stored in the column
   * \li If OT_BOTH, it will be an array of <RID, DataValue> pairs
   * @param outSize The size of the output buffer in bytes.
   * @param written (out parameter) A pointer to 1 int, which will contain the
   * number of bytes written to out.
   * @note See PrimitiveMsg.h for the type definitions.
   */
  void p_Col(NewColRequestHeader* in, ColResultHeader* out, unsigned outSize, unsigned* written);

  template <typename T, typename std::enable_if<sizeof(T) == sizeof(int8_t) || sizeof(T) == sizeof(int16_t) ||
                                                    sizeof(T) == sizeof(int128_t),
                                                T>::type* = nullptr>
  void scanAndFilterTypeDispatcher(NewColRequestHeader* in, ColResultHeader* out);

  template <typename T, typename std::enable_if<sizeof(T) == sizeof(int32_t), T>::type* = nullptr>
  void scanAndFilterTypeDispatcher(NewColRequestHeader* in, ColResultHeader* out);
  template <typename T, typename std::enable_if<sizeof(T) == sizeof(int64_t), T>::type* = nullptr>
  void scanAndFilterTypeDispatcher(NewColRequestHeader* in, ColResultHeader* out);
  template <typename T, typename std::enable_if<sizeof(T) <= sizeof(int64_t), T>::type* = nullptr>
  void _scanAndFilterTypeDispatcher(NewColRequestHeader* in, ColResultHeader* out);

  template <typename T, typename std::enable_if<sizeof(T) == sizeof(int128_t), T>::type* = nullptr>
  void _scanAndFilterTypeDispatcher(NewColRequestHeader* in, ColResultHeader* out);

  template <typename T>
  void columnScanAndFilter(NewColRequestHeader* in, ColResultHeader* out);

  boost::shared_ptr<ParsedColumnFilter> parseColumnFilter(const uint8_t* filterString, uint32_t colWidth,
                                                          uint32_t colType, uint32_t filterCount,
                                                          uint32_t BOP);
  void setParsedColumnFilter(boost::shared_ptr<ParsedColumnFilter>);

  /** @brief The p_ColAggregate primitive processor.
   *
   * The p_ColAggregate primitive processor.  It operates on a column block
   * specified using setBlockPtr().
   * @param in The buffer containing the command parameters.  The buffer should begin
   *		with a NewColAggRequestHeader, followed by an array of RIDs to generate
   * 		the data for (optional).
   * @param out The buffer to put the result in.  On return, it will contain a
   * NewCollAggResultHeader.
   * @note See PrimitiveMsg.h for the type definitions.
   */
  //	void p_ColAggregate(const NewColAggRequestHeader *in, NewColAggResultHeader *out);

  void p_Dictionary(const DictInput* in, std::vector<uint8_t>* out, bool skipNulls, uint32_t charsetNumber,
                    boost::shared_ptr<DictEqualityFilter> eqFilter, uint8_t eqOp);

  inline void setLogicalBlockMode(bool b)
  {
    logicalBlockMode = b;
  }

 private:
  PrimitiveProcessor(const PrimitiveProcessor& rhs);
  PrimitiveProcessor& operator=(const PrimitiveProcessor& rhs);

  int* block;

  bool compare(const datatypes::Charset& cs, uint8_t COP, const char* str1, size_t length1, const char* str2,
               size_t length2) throw();
  int compare(int val1, int val2, uint8_t COP, bool lastStage) throw();
  void indexWalk_1(const IndexWalkHeader* in, std::vector<IndexWalkHeader*>* out) throw();
  void indexWalk_2(const IndexWalkHeader* in, std::vector<IndexWalkHeader*>* out) throw();
  void indexWalk_many(const IndexWalkHeader* in, std::vector<IndexWalkHeader*>* out) throw();
  void grabSubTree(const IndexWalkHeader* in, std::vector<IndexWalkHeader*>* out) throw();

  void nextSig(int NVALS, const PrimToken* tokens, p_DataValue* ret, uint8_t outputFlags = 0,
               bool oldGetSigBehavior = false, bool skipNulls = false) throw();

  uint64_t masks[11];
  int dict_OffsetIndex, currentOffsetIndex;  // used by p_dictionary
  int fDebugLevel;
  dbbc::Stats* fStatsPtr;  // pointer for pmstats
  bool logicalBlockMode;

  boost::shared_ptr<ParsedColumnFilter> parsedColumnFilter;

  friend class ::PrimTest;
};

//
// COMPILE A COLUMN FILTER
//
// Compile column filter from BLOB into structure optimized for fast filtering.
// Return a shared_ptr for the compiled filter.
template <typename T>  // C++ integer type providing storage for colType
boost::shared_ptr<ParsedColumnFilter> _parseColumnFilter(
    const uint8_t* filterString,  // Filter represented as BLOB
    uint32_t colType,             // Column datatype as ColDataType
    uint32_t filterCount,         // Number of filter elements contained in filterString
    uint32_t BOP)                 // Operation (and/or/xor/none) that combines all filter elements
{
  using UT = typename std::conditional<std::is_unsigned<T>::value || datatypes::is_uint128_t<T>::value, T,
                                       typename datatypes::make_unsigned<T>::type>::type;
  const uint32_t WIDTH = sizeof(T);  // Sizeof of the column to be filtered

  boost::shared_ptr<ParsedColumnFilter> ret;  // Place for building the value to return
  if (filterCount == 0)
    return ret;

  // Allocate the compiled filter structure with space for filterCount filters.
  // No need to init arrays since they will be filled on the fly.
  ret.reset(new ParsedColumnFilter(filterCount, BOP));
  ret->allocateSpaceForFilterArgs<T>();

  // Choose initial filter mode based on operation and number of filter elements
  if (filterCount == 1)
    ret->columnFilterMode = SINGLE_COMPARISON;
  else if (BOP == BOP_OR)
    ret->columnFilterMode = ANY_COMPARISON_TRUE;
  else if (BOP == BOP_AND)
    ret->columnFilterMode = ALL_COMPARISONS_TRUE;
  else if (BOP == BOP_XOR)
    ret->columnFilterMode = XOR_COMPARISONS;
  else
    idbassert(0);  // BOP_NONE is compatible only with filterCount <= 1

  // Size of single filter element in filterString BLOB
  const uint32_t filterSize = sizeof(uint8_t) + sizeof(uint8_t) + WIDTH;

  // Parse the filter predicates and insert them into argVals and cops
  for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
  {
    // Pointer to ColArgs structure representing argIndex'th element in the BLOB
    auto args = reinterpret_cast<const ColArgs*>(filterString + (argIndex * filterSize));

    ret->prestored_cops[argIndex] = args->COP;
    ret->prestored_rfs[argIndex] = args->rf;

    if (datatypes::isUnsigned((execplan::CalpontSystemCatalog::ColDataType)colType))
      ret->storeFilterArg(argIndex, reinterpret_cast<const UT*>(args->val));
    else
      ret->storeFilterArg(argIndex, reinterpret_cast<const T*>(args->val));
  }

  /*  Decide which structure to use.  I think the only cases where we can use the set
      are when NOPS > 1, BOP is OR, and every COP is ==,
      and when NOPS > 1, BOP is AND, and every COP is !=.

      If there were no predicates that violate the condition for using a set,
      insert argVals into a set.
  */
  if (filterCount > 1)
  {
    // Check that all COPs are of right kind that depends on BOP
    for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
    {
      auto cop = ret->prestored_cops[argIndex];
      if (!((BOP == BOP_OR && cop == COMPARE_EQ) || (BOP == BOP_AND && cop == COMPARE_NE)))
      {
        goto skipConversion;
      }
    }

    // Now we found that conversion is possible. Let's choose between array-based search
    // and set-based search depending on the set size.
    // TODO: Tailor the threshold based on the actual search algorithms used and WIDTH/SIMD_WIDTH
    if (filterCount <= ParsedColumnFilter::noSetFilterThreshold)
    {
      // Assign filter mode of array-based filtering
      if (BOP == BOP_OR)
        ret->columnFilterMode = ONE_OF_VALUES_IN_ARRAY;
      else
        ret->columnFilterMode = NONE_OF_VALUES_IN_ARRAY;
    }
    else
    {
      // Assign filter mode of set-based filtering
      if (BOP == BOP_OR)
        ret->columnFilterMode = ONE_OF_VALUES_IN_SET;
      else
        ret->columnFilterMode = NONE_OF_VALUES_IN_SET;

      ret->populatePrestoredSet<T>();
    }

  skipConversion:;
  }

  return ret;
}

}  // namespace primitives

#endif
// vim:ts=4 sw=4:

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

/*
 * $Id: elementtype.h 9655 2013-06-25 23:08:13Z xlou $
 */
/** @file */

#pragma once

#include <iostream>
#include <utility>
#include <string>
#include <stdexcept>

#include <stdint.h>
#include <rowgroup.h>

#ifndef __GNUC__
#ifndef __attribute__
#define __attribute__(x)
#endif
#endif

namespace joblist
{
/** @brief struct ElementType
 *
 */

struct ElementType
{
  typedef uint64_t first_type;
  typedef uint64_t second_type;
  uint64_t first;
  uint64_t second;

  ElementType() : first(static_cast<uint64_t>(-1)), second(static_cast<uint64_t>(-1)){};
  ElementType(uint64_t f, uint64_t s) : first(f), second(s){};

  const char* getHashString(uint64_t mode, uint64_t* len) const
  {
    switch (mode)
    {
      case 0: *len = 8; return (char*)&first;

      case 1: *len = 8; return (char*)&second;

      default: throw std::logic_error("ElementType: invalid mode in getHashString().");
    }
  }
  inline bool operator<(const ElementType& e) const
  {
    return (first < e.first);
  }
  const std::string toString() const;
};

/** @brief struct StringElementType
 *
 */
struct StringElementType
{
  typedef uint64_t first_type;
  typedef utils::NullString second_type;

  uint64_t first;
  utils::NullString second;

  StringElementType();
  StringElementType(uint64_t f, const std::string& s);

  const char* getHashString(uint64_t mode, uint64_t* len) const
  {
    switch (mode)
    {
      case 0: *len = sizeof(first); return (char*)&first;

      case 1: *len = second.length(); return (char*)second.str();

      default: throw std::logic_error("StringElementType: invalid mode in getHashString().");
    }
  }
  inline bool operator<(const StringElementType& e) const
  {
    return (first < e.first);
  }
};

/** @brief struct DoubleElementType
 *
 */
struct DoubleElementType
{
  uint64_t first;
  double second;

  DoubleElementType();
  DoubleElementType(uint64_t f, double s);

  typedef double second_type;
  const char* getHashString(uint64_t mode, uint64_t* len) const
  {
    switch (mode)
    {
      case 0: *len = sizeof(first); return (char*)&first;

      case 1: *len = sizeof(second); return (char*)&second;

      default: throw std::logic_error("StringElementType: invalid mode in getHashString().");
    }
  }
  inline bool operator<(const DoubleElementType& e) const
  {
    return (first < e.first);
  }
};

template <typename element_t>
struct RowWrapper
{
  uint64_t count;
  static const uint64_t ElementsPerGroup = 8192;
  element_t et[8192];

  RowWrapper() : count(0)
  {
  }

  inline RowWrapper(const RowWrapper& rg) : count(rg.count)
  {
    for (uint32_t i = 0; i < count; ++i)
      et[i] = rg.et[i];
  }

  ~RowWrapper() = default;

  inline RowWrapper& operator=(const RowWrapper& rg)
  {
    count = rg.count;

    for (uint32_t i = 0; i < count; ++i)
      et[i] = rg.et[i];

    return *this;
  }
};

/** @brief struct RIDElementType
 *
 */
struct RIDElementType
{
  uint64_t first;

  RIDElementType();
  explicit RIDElementType(uint64_t f);

  const char* getHashString(uint64_t /*mode*/, uint64_t* len) const
  {
    *len = 8;
    return (char*)&first;
  }

  bool operator<(const RIDElementType& e) const
  {
    return (first < e.first);
  }
};

/** @brief struct TupleType
 *
 * first: rid
 * second: data value in unstructured format
 */

struct TupleType
{
  uint64_t first;
  char* second;
  TupleType() = default;
  TupleType(uint64_t f, char* s) : first(f), second(s)
  {
  }

  /** @brief delete a tuple
   *
   * this function should be called by the tuple user outside
   * the datalist if necessary
   */
  void deleter()
  {
    delete[] second;
  }

  /** @brief get hash string
   * @note params mode and len are ignored here. they are carried
   * just to keep a consistent interface with the other element type
   */
  const char* getHashString(uint64_t /*mode*/, uint64_t* /*len*/) const
  {
    return (char*)second;
  }

  bool operator<(const TupleType& e) const
  {
    return (first < e.first);
  }
};

typedef RowWrapper<ElementType> UintRowGroup;
typedef RowWrapper<StringElementType> StringRowGroup;
typedef RowWrapper<DoubleElementType> DoubleRowGroup;

extern std::istream& operator>>(std::istream& in, ElementType& rhs);
extern std::ostream& operator<<(std::ostream& out, const ElementType& rhs);
extern std::istream& operator>>(std::istream& in, StringElementType& rhs);
extern std::ostream& operator<<(std::ostream& out, const StringElementType& rhs);
extern std::istream& operator>>(std::istream& in, DoubleElementType& rhs);
extern std::ostream& operator<<(std::ostream& out, const DoubleElementType& rhs);
extern std::istream& operator>>(std::istream& in, RIDElementType& rhs);
extern std::ostream& operator<<(std::ostream& out, const RIDElementType& rhs);
extern std::istream& operator>>(std::istream& in, TupleType& rhs);
extern std::ostream& operator<<(std::ostream& out, const TupleType& rhs);
}  // namespace joblist

#ifndef NO_DATALISTS

// #include "bandeddl.h"
// #include "wsdl.h"
#include "fifo.h"
// #include "bucketdl.h"
// #include "constantdatalist.h"
// #include "swsdl.h"
// #include "zdl.h"
// #include "deliverywsdl.h"

namespace joblist
{
///** @brief type BandedDataList
// *
// */
// typedef BandedDL<ElementType> BandedDataList;
///** @brief type StringDataList
// *
// */
// typedef BandedDL<StringElementType> StringDataList;
/** @brief type StringFifoDataList
 *
 */
// typedef FIFO<StringElementType> StringFifoDataList;
typedef FIFO<StringRowGroup> StringFifoDataList;
///** @brief type StringBucketDataList
// *
// */
// typedef BucketDL<StringElementType> StringBucketDataList;
///** @brief type WorkingSetDataList
// *
// */
// typedef WSDL<ElementType> WorkingSetDataList;
/** @brief type FifoDataList
 *
 */
// typedef FIFO<ElementType> FifoDataList;
typedef FIFO<UintRowGroup> FifoDataList;
///** @brief type BucketDataList
// *
// */
// typedef BucketDL<ElementType> BucketDataList;
///** @brief type ConstantDataList_t
// *
// */
// typedef ConstantDataList<ElementType> ConstantDataList_t;
///** @brief type StringConstantDataList_t
// *
// */
// typedef ConstantDataList<StringElementType> StringConstantDataList_t;
/** @brief type DataList_t
 *
 */
typedef DataList<ElementType> DataList_t;
/** @brief type StrDataList
 *
 */
typedef DataList<StringElementType> StrDataList;
///** @brief type DoubleDataList
// *
// */
// typedef DataList<DoubleElementType> DoubleDataList;
///** @brief type TupleDataList
// *
// */
// typedef DataList<TupleType> TupleDataList;
///** @brief type SortedWSDL
// *
// */
// typedef SWSDL<ElementType> SortedWSDL;
///** @brief type StringSortedWSDL
// *
// */
// typedef SWSDL<StringElementType> StringSortedWSDL;
///** @brief type ZonedDL
// *
// */
// typedef ZDL<ElementType> ZonedDL;
///** @brief type StringZonedDL
// *
// */
// typedef ZDL<StringElementType> StringZonedDL;
//
///** @brief type TupleBucketDL
// *
// */
// typedef BucketDL<TupleType> TupleBucketDataList;

typedef FIFO<rowgroup::RGData> RowGroupDL;

}  // namespace joblist

#include <vector>
#include <boost/shared_ptr.hpp>

namespace joblist
{
/** @brief class AnyDataList
 *
 */
class AnyDataList
{
 public:
  AnyDataList() = default;

  ~AnyDataList() = default;

  inline void rowGroupDL(boost::shared_ptr<RowGroupDL> dl)
  {
    fDatalist = dl;
  }
  inline void rowGroupDL(RowGroupDL* dl)
  {
    fDatalist.reset(dl);
  }
  inline RowGroupDL* rowGroupDL()
  {
    return fDatalist.get();
  }
  inline const RowGroupDL* rowGroupDL() const
  {
    return fDatalist.get();
  }

  enum DataListTypes
  {
    UNKNOWN_DATALIST,                  /*!<  0 Unknown DataList */
    BANDED_DATALIST,                   /*!<  1 Banded DataList */
    WORKING_SET_DATALIST,              /*!<  2 WSDL */
    FIFO_DATALIST,                     /*!<  3 FIFO */
    BUCKET_DATALIST,                   /*!<  4 Bucket */
    CONSTANT_DATALIST,                 /*!<  5 Constant */
    STRING_DATALIST,                   /*!<  6 String */
    DOUBLE_DATALIST,                   /*!<  7 Double */
    STRINGFIFO_DATALIST,               /*!<  8 String FIFO */
    STRINGBANDED_DATALIST,             /*!<  9 String Banded */
    STRINGBUCKET_DATALIST,             /*!< 10 String Bucket */
    STRINGCONSTANT_DATALIST,           /*!< 11 String Constant */
    SORTED_WORKING_SET_DATALIST,       /*!< 12 Sorted WSDL */
    STRINGSORTED_WORKING_SET_DATALIST, /*!< 13 String Sorted WSDL */
    ZONED_DATALIST,                    /*!< 14 Zoned Datalist */
    STRINGZONED_DATALIST,              /*!< 15 String Zoned Datalist */
    TUPLEBUCKET_DATALIST,              /*!< 16 Tuple Bucket Datalist */
    TUPLE_DATALIST,                    /*!< 17 Tuple Datalist */
    DELIVERYWSDL,                      /*!< 18 Delivery WSDL */
    ROWGROUP_DATALIST
  };

  uint32_t getNumConsumers()
  {
    if (fDatalist)
      return 1;
    return 0;
  }

  // There is no operator==() because 2 AnyDataList's are equal if they point to the same DL, but the only way
  // that could be is if they are the _same_ AnyDatalist, since, by convention, AnyDataList's are only
  // moved around as shared_ptr's (AnyDataListSPtr). Indeed, it is an error if two different AnyDataList
  // objects point to the same DL.
  // bool operator==(const AnyDataList& rhs);

 private:
  AnyDataList(const AnyDataList& rhs) = delete;
  AnyDataList& operator=(const AnyDataList& rhs) = delete;
  boost::shared_ptr<RowGroupDL> fDatalist;
};

/** @brief type AnyDataListSPtr
 *
 */
typedef boost::shared_ptr<AnyDataList> AnyDataListSPtr;
/** @brief type DataListVec
 *
 */
typedef std::vector<AnyDataListSPtr> DataListVec;

extern std::ostream& operator<<(std::ostream& os, const AnyDataListSPtr& dl);

//
//...Manipulators for controlling the inclusion of the datalist's
//...OID in the AnyDataListSPtr's output stream operator.
//
extern std::ostream& showOidInDL(std::ostream& strm);
extern std::ostream& omitOidInDL(std::ostream& strm);

}  // namespace joblist

#endif

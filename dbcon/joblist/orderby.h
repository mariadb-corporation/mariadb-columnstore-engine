/* Copyright (C) 2022 MariaDB Corporation, Inc.

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

//  $Id: FlatOrderBy.h 9414 2013-04-22 22:18:30Z xlou $

/** @file */

#pragma once

#include <string>
#include <vector>
#include "resourcemanager.h"
#include "rowgroup.h"
#include "jlf_common.h"

namespace sorting
{
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
    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE: return joblist::DOUBLENULL;

    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::DATE:
    case execplan::CalpontSystemCatalog::DATETIME:
    case execplan::CalpontSystemCatalog::TIMESTAMP:
    case execplan::CalpontSystemCatalog::TIME:
    case execplan::CalpontSystemCatalog::VARBINARY:
    case execplan::CalpontSystemCatalog::BLOB:
    case execplan::CalpontSystemCatalog::TEXT: return joblist::CHAR8NULL;

    case execplan::CalpontSystemCatalog::UBIGINT: return joblist::UBIGINTNULL;

    default: return joblist::BIGINTNULL;
  }
}

template <typename T, typename std::enable_if<sizeof(T) == sizeof(int32_t), T>::type* = nullptr>
T getNullValue(uint8_t type)
{
  switch (type)
  {
    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT: return joblist::FLOATNULL;

    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::BLOB:
    case execplan::CalpontSystemCatalog::TEXT: return joblist::CHAR4NULL;

    case execplan::CalpontSystemCatalog::DATE:
    case execplan::CalpontSystemCatalog::DATETIME:
    case execplan::CalpontSystemCatalog::TIMESTAMP:
    case execplan::CalpontSystemCatalog::TIME: return joblist::DATENULL;

    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UMEDINT: return joblist::UINTNULL;

    default: return joblist::INTNULL;
  }
}

template <typename T, typename std::enable_if<sizeof(T) == sizeof(int16_t), T>::type* = nullptr>
T getNullValue(uint8_t type)
{
  switch (type)
  {
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::BLOB:
    case execplan::CalpontSystemCatalog::TEXT:
    case execplan::CalpontSystemCatalog::DATE:
    case execplan::CalpontSystemCatalog::DATETIME:
    case execplan::CalpontSystemCatalog::TIMESTAMP:
    case execplan::CalpontSystemCatalog::TIME: return joblist::CHAR2NULL;

    case execplan::CalpontSystemCatalog::USMALLINT: return joblist::USMALLINTNULL;

    default: return joblist::SMALLINTNULL;
  }
}

template <typename T, typename std::enable_if<sizeof(T) == sizeof(int8_t), T>::type* = nullptr>
T getNullValue(uint8_t type)
{
  switch (type)
  {
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::BLOB:
    case execplan::CalpontSystemCatalog::TEXT:
    case execplan::CalpontSystemCatalog::DATE:
    case execplan::CalpontSystemCatalog::DATETIME:
    case execplan::CalpontSystemCatalog::TIMESTAMP:
    case execplan::CalpontSystemCatalog::TIME: return joblist::CHAR1NULL;

    case execplan::CalpontSystemCatalog::UTINYINT: return joblist::UTINYINTNULL;

    default: return joblist::TINYINTNULL;
  }
}
}  // namespace sorting

namespace joblist
{
// forward reference
struct JobInfo;

// ORDER BY with LIMIT class
// This version is for subqueries, limit the result set to fit in memory,
// use ORDER BY to make the results consistent.
// The actual output are the first or last # of rows, which are NOT ordered.

// There is an important invariant that the code of this class must hold,
// namely rg_ must be init-ed only once.
class FlatOrderBy
{
  using RGDataOrRowIDType = uint32_t;
  struct PermutationType
  {
    uint64_t rgdataID : 32 {}, rowID : 24 {}, flags : 8 {};
  };
  using PermutationVec = std::vector<PermutationType>;
  using PermutationVecIter = std::vector<PermutationType>::iterator;
  using IterDiffT = std::iterator_traits<PermutationVecIter>::difference_type;
  using Ranges2SortQueue = std::queue<std::pair<IterDiffT, IterDiffT>>;

 public:
  FlatOrderBy();
  ~FlatOrderBy();
  void initialize(const rowgroup::RowGroup&, const JobInfo&, bool invertRules = false,
                  bool isMultiThreded = false);
  void processRow(const rowgroup::Row&);
  bool addBatch(rowgroup::RGData& rgData);
  bool sort();
  bool sortByColumn(const uint32_t columnId, const bool sortDirection);
  bool getData(rowgroup::RGData& data);
  template <datatypes::SystemCatalog::ColDataType, typename StorageType, typename EncodedKeyType>
  bool exchangeSortByColumn_(const uint32_t columnId, const bool sortDirection);
  template <datatypes::SystemCatalog::ColDataType ColType, typename StorageType, typename EncodedKeyType>
  void initialPermutationKeysNulls(const uint32_t columnID, std::vector<EncodedKeyType>& keys,
                                   std::vector<PermutationType>& nulls);
  template <datatypes::SystemCatalog::ColDataType ColType, typename StorageType, typename EncodedKeyType>
  void loopIterKeysNullsPerm(const uint32_t columnID, std::vector<EncodedKeyType>& keys,
                             PermutationVec& nulls, PermutationVec& permutation, PermutationVecIter begin,
                             PermutationVecIter end);
  template <typename EncodedKeyType>
  Ranges2SortQueue populateRanges(const IterDiffT beginOffset,
                                  typename std::vector<EncodedKeyType>::const_iterator begin,
                                  typename std::vector<EncodedKeyType>::const_iterator end);
  // template <enum datatypes::SystemCatalog::ColDataType, typename StorageType, typename EncodedKeyType>
  // bool distributionSortByColumn_(const uint32_t columnId);

  uint64_t getKeyLength() const;
  uint64_t getLimitCount() const
  {
    return count_;
  }
  const std::string toString() const;

  void finalize();

 protected:
  uint64_t start_;
  uint64_t count_;
  uint64_t uncommitedMemory_;
  static const uint64_t maxUncommited_;
  joblist::OrderByKeysType jobListorderByRGColumnIDs_;
  rowgroup::RowGroup rg_;
  std::vector<rowgroup::RGData> rgDatas_;
  std::vector<PermutationType> permutation_;
  std::unique_ptr<joblist::MemManager> mm_;
  IterDiffT flatCurPermutationDiff_ = 0;
  Ranges2SortQueue ranges2Sort_;
  // Scratch desk
  rowgroup::RGData data_;
  // It is possible to use rg_ member only but there is a potential to shoot in the foot
  // setting different RGDatas in two points in the code of this class methods.
  rowgroup::RowGroup rgOut_;
  rowgroup::Row inRow_;
  rowgroup::Row outRow_;
};

}  // namespace joblist

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

#include <cstdint>
#include <string>
#include <vector>
#include "conststring.h"
#include "mcs_int128.h"
#include "resourcemanager.h"
#include "rowgroup.h"
#include "jlf_common.h"

namespace sorting
{
template <typename T, typename EncodedKeyType>
  requires(std::is_same<T, datatypes::TSInt128>::value &&
           !(std::is_same<EncodedKeyType, utils::ConstString>::value ||
             std::is_same<EncodedKeyType, utils::ShortConstString>::value))
T getNullValue(uint8_t type)
{
  return datatypes::TSInt128(datatypes::Decimal128Null);
}

template <typename T, typename EncodedKeyType>
  requires std::is_same<T, double>::value
uint64_t getNullValue(uint8_t type)
{
  return joblist::DOUBLENULL;
}

template <typename T, typename EncodedKeyType>
  requires std::is_same<T, float>::value
uint32_t getNullValue(uint8_t type)
{
  return joblist::FLOATNULL;
}

// WIP Align all datatypes, e.g. TIME has its own NULL.
template <typename T, typename EncodedKeyType>
  requires(sizeof(T) == sizeof(int64_t) && !std::is_same<T, double>::value &&
           !(std::is_same<EncodedKeyType, utils::ConstString>::value ||
             std::is_same<EncodedKeyType, utils::ShortConstString>::value))
T getNullValue(uint8_t type)
{
  switch (type)
  {
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::VARBINARY:
    case execplan::CalpontSystemCatalog::BLOB:
    case execplan::CalpontSystemCatalog::TEXT: return joblist::CHAR8NULL;
    case execplan::CalpontSystemCatalog::TIME: return joblist::TIMENULL;
    case execplan::CalpontSystemCatalog::TIMESTAMP: return joblist::TIMESTAMPNULL;
    case execplan::CalpontSystemCatalog::DATETIME: return joblist::DATETIMENULL;
    case execplan::CalpontSystemCatalog::DATE: return joblist::DATENULL;

    case execplan::CalpontSystemCatalog::UBIGINT: return joblist::UBIGINTNULL;

    default: return joblist::BIGINTNULL;
  }
}

template <typename T, typename EncodedKeyType>
  requires(sizeof(T) == sizeof(int32_t) && !std::is_same<T, float>::value &&
           !(std::is_same<EncodedKeyType, utils::ConstString>::value ||
             std::is_same<EncodedKeyType, utils::ShortConstString>::value))
T getNullValue(uint8_t type)
{
  switch (type)
  {
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

template <typename T, typename EncodedKeyType>
  requires(sizeof(T) == sizeof(int16_t) && !(std::is_same<EncodedKeyType, utils::ConstString>::value ||
                                             std::is_same<EncodedKeyType, utils::ShortConstString>::value))
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

template <typename T, typename EncodedKeyType>
  requires(sizeof(T) == sizeof(int8_t) && !(std::is_same<EncodedKeyType, utils::ConstString>::value ||
                                            std::is_same<EncodedKeyType, utils::ShortConstString>::value))
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

template <typename StorageType, typename EncodedKeyType>
  requires((std::is_same<EncodedKeyType, utils::ConstString>::value ||
            std::is_same<EncodedKeyType, utils::ShortConstString>::value) &&
           !(std::is_same<StorageType, utils::ShortConstString>::value &&
             std::is_same<EncodedKeyType, utils::ShortConstString>::value))
EncodedKeyType getNullValue(uint8_t type)
{
  const char* n = nullptr;
  return utils::ConstString(n, 0);
}

// This stub is needed to allow templates to compile.
template <typename StorageType, typename EncodedKeyType>
  requires(std::is_same<StorageType, utils::ShortConstString>::value &&
           std::is_same<EncodedKeyType, utils::ShortConstString>::value)
EncodedKeyType getNullValue(uint8_t type)
{
  return {};
}

template <typename EncodedKeyType>
concept IsConstString = requires {
                          requires std::is_same<EncodedKeyType, utils::ConstString>::value ||
                                       std::is_same<EncodedKeyType, utils::ShortConstString>::value;
                        };
template <typename EncodedKeyType>
concept NotConstString = requires {
                           requires !(std::is_same<EncodedKeyType, utils::ConstString>::value ||
                                      std::is_same<EncodedKeyType, utils::ShortConstString>::value);
                         };

bool isDictColumn(datatypes::SystemCatalog::ColDataType colType, auto columnWidth)
{
  switch (colType)
  {
    case execplan::CalpontSystemCatalog::CHAR: return (columnWidth > 8);

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::BLOB:
    case execplan::CalpontSystemCatalog::TEXT: return (columnWidth > 7);
    default: return false;
  }
}

}  // namespace sorting

namespace joblist
{
// forward reference
struct JobInfo;

template <bool TrueCheck>
concept IsTrue = requires { requires TrueCheck == true; };

template <bool FalseCheck>
concept IsFalse = requires { requires FalseCheck == false; };

class FlatOrderBy;
using SortingThreads = std::vector<std::unique_ptr<FlatOrderBy>>;

// There is an important invariant that the code of this class must hold,
// namely rg_ must be init-ed only once.
class FlatOrderBy
{
 public:
  // Bit lengths for rowID and threadID are flexible
  struct PermutationType
  {
    uint64_t rgdataID : 32 {}, rowID : 24 {}, threadID : 8 {};
  };
  using PermutationVecIter = std::vector<PermutationType>::iterator;
  using IterDiffT = std::iterator_traits<PermutationVecIter>::difference_type;
  using PermutationVec = std::vector<PermutationType>;
  using Ranges2SortQueue = std::queue<std::pair<IterDiffT, IterDiffT>>;

 private:
  using RGDataOrRowIDType = uint32_t;

 public:
  FlatOrderBy();
  ~FlatOrderBy();
  void initialize(const rowgroup::RowGroup&, const JobInfo&, bool invertRules = false,
                  bool isMultiThreded = false);
  void processRow(const rowgroup::Row&);
  bool addBatch(rowgroup::RGData& rgData);
  bool sortCF(const uint32_t id);
  template <bool IsFirst>
  bool sortByColumnCF(const uint32_t id, joblist::OrderByKeysType columns, PermutationVec&& permutation,
                      Ranges2SortQueue&& ranges2Sort, const SortingThreads& prevPhaseThreads);

  bool getData(rowgroup::RGData& data, const SortingThreads& prevPhaseThreads);

  template <bool IsFirst, datatypes::SystemCatalog::ColDataType, typename StorageType,
            typename EncodedKeyType>
    requires IsFalse<IsFirst> bool
  exchangeSortByColumnCF_(const uint32_t id, const uint32_t columnId, const bool sortDirection,
                          joblist::OrderByKeysType columns, PermutationVec&& permutation,
                          Ranges2SortQueue&& ranges2Sort, const SortingThreads& prevPhaseThreads);
  template <bool IsFirst, datatypes::SystemCatalog::ColDataType, typename StorageType,
            typename EncodedKeyType>
    requires IsTrue<IsFirst> bool
  exchangeSortByColumnCF_(const uint32_t id, const uint32_t columnId, const bool sortDirection,
                          joblist::OrderByKeysType columns, PermutationVec&& permutation,
                          Ranges2SortQueue&& ranges2Sort, const SortingThreads& prevPhaseThreads);
  template <datatypes::SystemCatalog::ColDataType ColType, typename StorageType, typename EncodedKeyType>
  void initialPermutationKeysNulls(const uint32_t id, const uint32_t columnID, const bool nullsFirst,
                                   std::vector<EncodedKeyType>& keys, PermutationVec& permutation,
                                   PermutationVec& nulls);
  template <datatypes::SystemCatalog::ColDataType ColType, typename StorageType, typename EncodedKeyType>
  auto loopIterKeysNullsPerm(const uint32_t columnID, const bool nullsFirst, PermutationVecIter begin,
                             PermutationVecIter end, const SortingThreads& prevPhaseThreads);
  template <typename EncodedKeyType>
  Ranges2SortQueue populateRanges(const uint32_t columnID, const IterDiffT beginOffset,
                                  typename std::vector<EncodedKeyType>::const_iterator begin,
                                  typename std::vector<EncodedKeyType>::const_iterator end);

  uint64_t getKeyLength() const;
  uint64_t getLimitCount() const
  {
    return count_;
  }
  const std::string toString() const;

  void finalize();

  const PermutationVec& getPermutation() const
  {
    return permutation_;
  }
  rowgroup::RDGataVector& getRGDatas()
  {
    return rgDatas_;
  }
  joblist::OrderByKeysType getSortingColumns() const
  {
    return jobListorderByRGColumnIDs_;
  }

 private:
  template <typename EncodedKeyType, typename StorageType>
    requires(!(std::is_integral<StorageType>::value || std::is_floating_point<StorageType>::value) &&
             sorting::IsConstString<EncodedKeyType>) bool
  isNull(const EncodedKeyType value, const EncodedKeyType null, const StorageType storageNull) const
  {
    return value.isNull();
  }

  template <typename EncodedKeyType, typename StorageType>
    requires((std::is_integral<EncodedKeyType>::value ||
              std::is_same<EncodedKeyType, datatypes::TSInt128>::value) &&
             (std::is_integral<StorageType>::value ||
              std::is_same<StorageType, datatypes::TSInt128>::value)) bool
  isNull(const EncodedKeyType value, const EncodedKeyType null, const StorageType storageNull) const
  {
    return value == null;
  }

  template <typename EncodedKeyType, typename StorageType>
    requires(std::is_floating_point<StorageType>::value && std::is_same<StorageType, double>::value) bool
  isNull(const EncodedKeyType value, const uint64_t null, const uint64_t storageNull) const
  {
    return std::memcmp(&value, &null, sizeof(uint64_t)) == 0;
  }

  template <typename EncodedKeyType, typename StorageType>
    requires(std::is_floating_point<StorageType>::value && std::is_same<StorageType, float>::value) bool
  isNull(const EncodedKeyType value, const uint32_t null, const uint32_t storageNull) const
  {
    return std::memcmp(&value, &null, sizeof(uint32_t)) == 0;
  }

  template <typename EncodedKeyType, typename StorageType>
    requires(sorting::IsConstString<EncodedKeyType> && std::is_integral<StorageType>::value) bool
  isNull(const EncodedKeyType value, const EncodedKeyType null, const StorageType storageNull) const
  {
    const StorageType v = *reinterpret_cast<const StorageType*>(value.str());
    return v == storageNull;
  }

 protected:
  uint64_t start_;
  uint64_t count_;
  uint64_t uncommitedMemory_;
  static const uint64_t maxUncommited_;
  joblist::OrderByKeysType jobListorderByRGColumnIDs_;
  rowgroup::RowGroup rg_;
  rowgroup::RDGataVector rgDatas_;
  PermutationVec permutation_;
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

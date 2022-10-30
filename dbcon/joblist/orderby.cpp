/* Copyright (C) 2022 MariaDB Corporation, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITH`OUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

//  $Id: FlatOrderBy.cpp 9581 2013-05-31 13:46:14Z pleblanc $

#include <iostream>
// #define NDEBUG
#include <cassert>
#include <string>
#include "calpontsystemcatalog.h"
using namespace std;

#include <boost/shared_array.hpp>
using namespace boost;

#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "rowgroup.h"
using namespace rowgroup;

#include "jlf_common.h"
#include "orderby.h"
#include "utils/common/modified_pdqsort.h"

using namespace ordering;

namespace joblist
{
// const uint64_t FlatOrderBy::fMaxUncommited = 102400;  // 100KiB - make it configurable?

// FlatOrderBy class implementation
FlatOrderBy::FlatOrderBy() : start_(0), count_(-1)
{
  // fRule.fIdbCompare = this;
}

FlatOrderBy::~FlatOrderBy()
{
}

void FlatOrderBy::initialize(const RowGroup& rg, const JobInfo& jobInfo, bool invertRules,
                             bool isMultiThreaded)
{
  //   fRm = jobInfo.rm;
  // Watch out and re-check the defaults !!!
  mm_.reset(new joblist::RMMemManager(jobInfo.rm, jobInfo.umMemLimit));

  start_ = jobInfo.limitStart;
  count_ = jobInfo.limitCount;
  rg_ = rg;
  rgOut_ = rg;
  data_.reinit(rg_, 2);
  rg_.setData(&data_);
  rg_.resetRowGroup(0);
  rg_.initRow(&inRow_);
  rg_.initRow(&outRow_);
  rg_.getRow(0, &inRow_);
  rg_.getRow(1, &outRow_);

  map<uint32_t, uint32_t> keyToIndexMap;
  auto& keys = rg.getKeys();
  for (uint64_t i = 0; i < keys.size(); ++i)
  {
    if (keyToIndexMap.find(keys[i]) == keyToIndexMap.end())
      keyToIndexMap.insert(make_pair(keys[i], i));
  }

  // TupleAnnexStep execute sends duplicates b/c it has access to RowGroup
  // but ORDER BY columns are described in JobList::orderByColVec.
  for (auto [key, sortDirection] : jobInfo.orderByColVec)
  {
    auto j = keyToIndexMap.find(key);
    idbassert(j != keyToIndexMap.end());
    auto [keyId, rgColumnID] = *j;
    jobListorderByRGColumnIDs_.push_back({rgColumnID, sortDirection});
  }

  //   IdbOrderBy::initialize(rg);
}

// This must return a proper number of key columns and
// not just a column count.
uint64_t FlatOrderBy::getKeyLength() const
{
  return rg_.getColumnCount();
  //   return fRow0.getColumnCount();
}

bool FlatOrderBy::addBatch(rowgroup::RGData& rgData)
{
  bool isFailure = false;
  rg_.setData(&rgData);
  auto rowCount = rg_.getRowCount();
  auto bytes = rg_.getSizeWithStrings(rowCount);
  if (!mm_->acquire(bytes))
  {
    cerr << IDBErrorInfo::instance()->errorMsg(ERR_LIMIT_TOO_BIG) << " @" << __FILE__ << ":" << __LINE__;
    throw IDBExcept(ERR_LIMIT_TOO_BIG);
  }

  rgDatas_.push_back(rgData);

  return isFailure;
}

bool FlatOrderBy::sortCF()
{
  bool isFailure = false;
  constexpr const bool isFirst = true;
  // std::cout << "sortCF jobListorderByRGColumnIDs_ size " << jobListorderByRGColumnIDs_.size() << std::endl;
  if (sortByColumnCF<isFirst>(jobListorderByRGColumnIDs_))
  {
    isFailure = true;
    return isFailure;
  }

  return isFailure;
}

template <bool IsFirst>
bool FlatOrderBy::sortByColumnCF(joblist::OrderByKeysType columns)
{
  const bool isFailure = false;
  // std::cout << "sortByColumnCF columns size " << columns.size() << std::endl;

  const auto [columnId, sortDirection] = columns.front();
  auto columnType = rg_.getColType(columnId);
  switch (columnType)
  {
    case execplan::CalpontSystemCatalog::TINYINT:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::TINYINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::TINYINT, StorageType,
                                     EncodedKeyType>(columnId, sortDirection, columns);
    }
    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    {
      // WIP !!!!!!!!!!! There are three variants typed cols,
      // inline short strings, out-of-band long strings
      // inline short strings are not supported yet
      auto columnWidth = rg_.getColumnWidth(columnId);
      bool forceInline = rg_.getForceInline()[columnId];
      if (sorting::isDictColumn(columnType, columnWidth) && columnWidth >= rg_.getStringTableThreshold() &&
          !forceInline)
      {
        using StorageType = utils::ConstString;
        using EncodedKeyType = utils::ConstString;
        // No difference b/w VARCHAR, CHAR and TEXT types yet
        return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::VARCHAR, StorageType,
                                       EncodedKeyType>(columnId, sortDirection, columns);
      }
      else if (sorting::isDictColumn(columnType, columnWidth) &&
               (columnWidth < rg_.getStringTableThreshold() || forceInline))
      {
        using EncodedKeyType = utils::ConstString;
        using StorageType = utils::ShortConstString;
        // No difference b/w VARCHAR, CHAR and TEXT types yet
        return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::VARCHAR, StorageType,
                                       EncodedKeyType>(columnId, sortDirection, columns);
      }
      else if (!sorting::isDictColumn(columnType, columnWidth))
      {
        using EncodedKeyType = utils::ConstString;

        switch (columnWidth)
        {
          case 1:
          {
            using StorageType =
                datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::TINYINT>::type;
            return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::VARCHAR, StorageType,
                                           EncodedKeyType>(columnId, sortDirection, columns);
          }

          case 2:
          {
            using StorageType =
                datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::SMALLINT>::type;
            return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::VARCHAR, StorageType,
                                           EncodedKeyType>(columnId, sortDirection, columns);
          };

          case 4:
          {
            using StorageType =
                datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::INT>::type;
            return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::VARCHAR, StorageType,
                                           EncodedKeyType>(columnId, sortDirection, columns);
          };

          case 8:
          {
            using StorageType =
                datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::BIGINT>::type;
            return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::VARCHAR, StorageType,
                                           EncodedKeyType>(columnId, sortDirection, columns);
          };
          default: idbassert(0);
        }
      }
      break;
    }
    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      using StorageType =
          datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::SMALLINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::SMALLINT, StorageType,
                                     EncodedKeyType>(columnId, sortDirection, columns);
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::DOUBLE>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::DOUBLE, StorageType,
                                     EncodedKeyType>(columnId, sortDirection, columns);
    }

    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::INT:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::INT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::INT, StorageType,
                                     EncodedKeyType>(columnId, sortDirection, columns);
    }

    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::FLOAT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::FLOAT, StorageType,
                                     EncodedKeyType>(columnId, sortDirection, columns);
    }

    case execplan::CalpontSystemCatalog::DATE:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::BIGINT:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::BIGINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::BIGINT, StorageType,
                                     EncodedKeyType>(columnId, sortDirection, columns);
    }
    case execplan::CalpontSystemCatalog::DATETIME:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::UTINYINT:
    {
      using StorageType =
          datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::UTINYINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::UTINYINT, StorageType,
                                     EncodedKeyType>(columnId, sortDirection, columns);
    }

    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      using StorageType =
          datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::USMALLINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::USMALLINT, StorageType,
                                     EncodedKeyType>(columnId, sortDirection, columns);
    }

    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UINT:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::UINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::UINT, StorageType,
                                     EncodedKeyType>(columnId, sortDirection, columns);
    }

    case execplan::CalpontSystemCatalog::UBIGINT:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::UBIGINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::UBIGINT, StorageType,
                                     EncodedKeyType>(columnId, sortDirection, columns);
    }

    default: break;
  }
  return isFailure;
}

template <datatypes::SystemCatalog::ColDataType ColType, typename StorageType, typename EncodedKeyType>
void FlatOrderBy::initialPermutationKeysNulls(const uint32_t columnID, const bool nullsFirst,
                                              std::vector<EncodedKeyType>& keys, PermutationVec& permutation,
                                              PermutationVec& nulls)
{
  rowgroup::Row r;
  // Replace with a constexpr
  auto nullValue = sorting::getNullValue<StorageType, EncodedKeyType>(ColType);
  // Used by getColumnValue to detect NULLs when a string is less than 8 bytes.
  [[maybe_unused]] auto storageNull = sorting::getNullValue<StorageType, StorageType>(ColType);
  RGDataOrRowIDType rgDataId = 0;
  permutation.reserve(rgDatas_.size() * rowgroup::rgCommonSize);
  keys.reserve(rgDatas_.size() * rowgroup::rgCommonSize);
  rg_.initRow(&r);
  for (auto& rgData : rgDatas_)
  {
    // set rgdata
    rg_.setData(&rgData);
    // Move this outside the loop    // Row iterator call seems unreasonably costly here
    rg_.getRow(0, &r);  // get first row
    auto rowCount = rg_.getRowCount();

    for (decltype(rowCount) i = 0; i < rowCount; ++i)
    {
      EncodedKeyType value = rg_.getColumnValue<ColType, StorageType, EncodedKeyType>(columnID, i);
      PermutationType permute = {rgDataId, i, 0};
      if (!isNull<EncodedKeyType, StorageType>(value, nullValue, storageNull))
      {
        keys.push_back(value);
        permutation.push_back(permute);
      }
      else
      {
        nulls.push_back(permute);
      }
    }
    ++rgDataId;
  }
  if (nullsFirst)
  {
    permutation.insert(permutation.begin(), nulls.begin(), nulls.end());
  }
  else
  {
    permutation.insert(permutation.end(), nulls.begin(), nulls.end());
  }

  // rg_.setData(&rgDatas_[0]);
  // std::cout << "initialPermutationKeysNulls " << rg_.toString() << std::endl;
}

template <datatypes::SystemCatalog::ColDataType ColType, typename StorageType, typename EncodedKeyType>
void FlatOrderBy::loopIterKeysNullsPerm(const uint32_t columnID, const bool nullsFirst,
                                        std::vector<EncodedKeyType>& keys, PermutationVec& nulls,
                                        PermutationVec& permutation, PermutationVecIter begin,
                                        PermutationVecIter end)
{
  rowgroup::Row r;
  // Replace with a constexpr
  auto nullValue = sorting::getNullValue<StorageType, EncodedKeyType>(ColType);
  [[maybe_unused]] auto storageNull = sorting::getNullValue<StorageType, StorageType>(ColType);
  rg_.initRow(&r);  // Row iterator call seems unreasonably costly here
  auto permSrcBegin = begin;
  auto permSrcEnd = end;
  keys.reserve(std::distance(permSrcBegin, permSrcEnd));
  permutation.reserve(std::distance(permSrcBegin, permSrcEnd));
  // Проверить актуальность итераторов
  for (auto p = permSrcBegin; p != permSrcEnd; ++p)
  {
    // set rgdata
    rg_.setData(&rgDatas_[p->rgdataID]);  // WIP costly thing
    EncodedKeyType value = rg_.getColumnValue<ColType, StorageType, EncodedKeyType>(columnID, p->rowID);
    if (!isNull<EncodedKeyType, StorageType>(value, nullValue, storageNull))
    {
      keys.push_back(value);
      permutation.push_back(*p);
    }
    else
    {
      nulls.push_back(*p);
    }
  }
}

template <typename EncodedKeyType>
FlatOrderBy::Ranges2SortQueue FlatOrderBy::populateRanges(
    [[maybe_unused]] const uint32_t columnID, const IterDiffT beginOffset,
    typename std::vector<EncodedKeyType>::const_iterator begin,
    typename std::vector<EncodedKeyType>::const_iterator end)
{
  // Single column only sorting
  if (jobListorderByRGColumnIDs_.size() == 1)
    return {};
  if (std::distance(begin, end) <= 1)
    return {};

  FlatOrderBy::Ranges2SortQueue ranges;
  auto rangeItLeft = begin;
  auto rangeItRight = begin + 1;
  if constexpr (sorting::IsConstString<EncodedKeyType>)
  {
    datatypes::Charset cs(rg_.getCharset(columnID));
    for (; rangeItRight != end; ++rangeItRight)
    {
      if (!cs.strnncollsp(*rangeItLeft, *rangeItRight))
        continue;
      // left it value doesn't match right it value
      auto dist = std::distance(rangeItLeft, rangeItRight);
      if (dist > 1)
      {
        ranges.push({beginOffset + std::distance(begin, rangeItLeft),
                     beginOffset + std::distance(begin, rangeItRight)});
      }
      rangeItLeft = rangeItRight;
    }
  }
  else
  {
    for (; rangeItRight != end; ++rangeItRight)
    {
      if (*rangeItLeft == *rangeItRight)
        continue;
      // left it value doesn't match right it value
      auto dist = std::distance(rangeItLeft, rangeItRight);
      if (dist > 1)
      {
        ranges.push({beginOffset + std::distance(begin, rangeItLeft),
                     beginOffset + std::distance(begin, rangeItRight)});
      }
      rangeItLeft = rangeItRight;
    }
  }

  // rangeItRight == end here thus reducing it by one to stay in range
  auto dist = std::distance(rangeItLeft, rangeItRight);
  if (dist > 1)
  {
    {
      ranges.push({beginOffset + std::distance(begin, rangeItLeft),
                   beginOffset + std::distance(begin, rangeItRight)});
    }
  }

  return ranges;
}

// template <bool isAscendingSort, datatypes::SystemCatalog::ColDataType ColType, typename EncodedKeyType>

template <bool IsFirst, datatypes::SystemCatalog::ColDataType ColType, typename StorageType,
          typename EncodedKeyType>
  requires IsTrue<IsFirst> bool
FlatOrderBy::exchangeSortByColumnCF_(const uint32_t columnID, const bool isAscDirection,
                                     joblist::OrderByKeysType columns)
{
  bool isFailure = false;
  // ASC = true, DESC = false
  // MCS finally reads records from TAS in opposite to nullsFirst value,
  // if nullsFirst is true the NULLS will be in the end and vice versa.
  const bool nullsFirst = !isAscDirection;
  auto bytes = (sizeof(EncodedKeyType) + sizeof(FlatOrderBy::PermutationType)) * rgDatas_.size() *
               rowgroup::rgCommonSize;
  {
    std::vector<EncodedKeyType> keys;
    PermutationVec nulls;
    PermutationVec permutation;
    // Grab RAM  keys + permutation. NULLS are counted also.
    if (!mm_->acquire(bytes))
    {
      cerr << IDBErrorInfo::instance()->errorMsg(ERR_LIMIT_TOO_BIG) << " @" << __FILE__ << ":" << __LINE__;
      throw IDBExcept(ERR_LIMIT_TOO_BIG);
    }

    initialPermutationKeysNulls<ColType, StorageType, EncodedKeyType>(columnID, nullsFirst, keys, permutation,
                                                                      nulls);

    // This is the first column sorting and keys, nulls were already filled up previously.
    // permutation = std::move(permutation_);

    auto permBegin = (nullsFirst) ? permutation.begin() + nulls.size() : permutation.begin();
    auto permEnd = (nullsFirst) ? permutation.end() : permutation.end() - nulls.size();
    assert(std::distance(keys.begin(), keys.end()) == std::distance(permBegin, permEnd));
    // assert(permutation.size() == keys.size() + ((nullsFirst) ? 0 : nulls.size()));
    // sortDirection is true = ASC

    if constexpr (std::is_same<EncodedKeyType, utils::ConstString>::value)
    {
      datatypes::Charset cs(rg_.getCharset(columnID));
      if (isAscDirection)
      {
        auto cmp = [&cs](EncodedKeyType x, EncodedKeyType y) { return cs.strnncollsp(x, y) > 0; };
        sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, cmp);
      }
      else
      {
        auto cmp = [&cs](EncodedKeyType x, EncodedKeyType y) { return cs.strnncollsp(x, y) < 0; };
        sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, cmp);
      }
    }
    else
    {
      if (isAscDirection)
      {
        sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, std::greater<EncodedKeyType>());
      }
      else
      {
        sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, std::less<EncodedKeyType>());
      }
    }

    // Use && here
    FlatOrderBy::Ranges2SortQueue ranges4Sort;
    if (columns.size() > 1)
    {
      ranges4Sort = populateRanges<EncodedKeyType>(columnID, (nullsFirst) ? nulls.size() : 0ULL, keys.begin(),
                                                   keys.end());

      // Adding NULLs range into ranges
      if (nulls.size() > 1)
      {
        if (isAscDirection)
        {
          ranges4Sort.push({keys.size(), permutation.size()});
        }
        else
        {
          ranges4Sort.push({0, nulls.size()});
        }
      }
    }

    permutation_ = std::move(permutation);

    // Check if copy ellision works here.
    // Set a stack of equal values ranges. Comp complexity is O(N), mem complexity is O(N)
    ranges2Sort_ = std::move(ranges4Sort);
  }
  if (columns.size() > 1)
  {
    // !!! FREE RAM
    mm_->release(bytes);
    columns.erase(columns.begin());
    return sortByColumnCF<false>(columns);
  }
  return isFailure;
}

template <bool IsFirst, datatypes::SystemCatalog::ColDataType ColType, typename StorageType,
          typename EncodedKeyType>
  requires IsFalse<IsFirst> bool
FlatOrderBy::exchangeSortByColumnCF_(const uint32_t columnID, const bool sortDirection,
                                     joblist::OrderByKeysType columns)
{
  // std::cout << " sortByColumnCF_  1 columns.size() " << columns.size() << std::endl;

  bool isFailure = false;
  // ASC = true, DESC = false
  // MCS finally reads records from TAS in opposite to nullsFirst value,
  // if nullsFirst is true the NULLS will be in the end and vice versa.
  const bool nullsFirst = !sortDirection;

  // Add stopped check into this and other loops
  Ranges2SortQueue ranges4Sort;
  while (!ranges2Sort_.empty())
  {
    auto [sameValuesRangeBeginDist, sameValuesRangeEndDist] = ranges2Sort_.front();
    // for (auto p : permutation_)
    // {
    //   std::cout << " perm " << p.rowID << " ";
    // }
    // std::cout << std::endl;
    // std::cout << "The next column iter left " << sameValuesRangeBeginDist << " right "
    //           << sameValuesRangeEndDist << std::endl;
    auto sameValuesRangeBegin = permutation_.begin() + sameValuesRangeBeginDist;
    auto sameValuesRangeEnd = permutation_.begin() + sameValuesRangeEndDist;
    auto permCurRangeBeginEndDist = sameValuesRangeEndDist - sameValuesRangeBeginDist;
    // auto permutationBeginEndDist = std::distance(permutation_.begin(), permutation_.end());
    ranges2Sort_.pop();
    auto bytes = (sizeof(EncodedKeyType) + sizeof(FlatOrderBy::PermutationType)) * permCurRangeBeginEndDist;
    if (!mm_->acquire(bytes))
    {
      cerr << IDBErrorInfo::instance()->errorMsg(ERR_LIMIT_TOO_BIG) << " @" << __FILE__ << ":" << __LINE__;
      throw IDBExcept(ERR_LIMIT_TOO_BIG);
    }

    std::vector<EncodedKeyType> keys;
    PermutationVec nulls;
    PermutationVec permutation;
    // This is the first column sorting and keys, nulls were already filled up previously.
    // !!! it might worth to move nulls_.empty() check into a separate condition
    {
      // Sorting the full original set. This is either the first iteration or all key values are equal.
      // if (permCurRangeBeginEndDist == permutationBeginEndDist)
      // {
      //   permutation = std::move(permutation_);
      //   // Full set here is either all NULLs or no NULLs.
      //   loopIterKeysNullsPerm<ColType, StorageType>(columnID, keys, nulls, permutation,
      //   permutation.begin(),
      //                                               permutation.end());
      // }
      // else
      {
        loopIterKeysNullsPerm<ColType, StorageType>(columnID, nullsFirst, keys, nulls, permutation,
                                                    sameValuesRangeBegin, sameValuesRangeEnd);
      }
    }

    // auto permBegin = permutation_.begin() + (std::distance(permutation_.begin(), sameValuesRangeBegin));
    // auto permEnd = permutation_.begin() + (std::distance(permutation_.begin(), sameValuesRangeEnd));
    auto permBegin = permutation.begin();
    auto permEnd = permutation.end();
    assert(std::distance(keys.begin(), keys.end()) == std::distance(permBegin, permEnd));
    assert(permutation.size() == keys.size());
    if constexpr (std::is_same<EncodedKeyType, utils::ConstString>::value)
    {
      datatypes::Charset cs(rg_.getCharset(columnID));
      if (sortDirection)
      {
        auto cmp = [&cs](EncodedKeyType& x, EncodedKeyType& y) { return -cs.strnncollsp(x, y); };
        sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, cmp);
      }
      else
      {
        auto cmp = [&cs](EncodedKeyType& x, EncodedKeyType& y) { return cs.strnncollsp(x, y); };
        sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, cmp);
      }
    }
    else
    {
      if (sortDirection)
      {
        sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, std::greater<EncodedKeyType>());
        // sorting::pdqsort(keys.begin(), keys.end(), std::greater<EncodedKeyType>());
      }
      else
      {
        sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, std::less<EncodedKeyType>());
        // sorting::pdqsort(keys.begin(), keys.end(), std::less<EncodedKeyType>());
      }
    }

    // Use && here
    if (columns.size() > 1)
    {
      // std::cout << "nulls size " << nulls.size() << std::endl;
      auto ranges = populateRanges<EncodedKeyType>(
          columnID, (nullsFirst) ? sameValuesRangeBeginDist + nulls.size() : sameValuesRangeBeginDist,
          keys.begin(), keys.end());
      while (!ranges.empty())
      {
        auto range = ranges.front();
        ranges.pop();
        ranges4Sort.push(range);
      }
      if (nulls.size() > 1)
      {
        if (sortDirection)
        {
          ranges4Sort.push({sameValuesRangeBeginDist + keys.size(),
                            sameValuesRangeBeginDist + keys.size() + nulls.size()});
        }
        else
        {
          ranges4Sort.push({sameValuesRangeBeginDist, sameValuesRangeBeginDist + nulls.size()});
        }
      }
    }

    assert(sameValuesRangeBegin + permutation.size() + nulls.size() != permutation_.end() + 1);
    {
      auto rangeBegin = sameValuesRangeBegin;
      if (nullsFirst)
      {
        for (auto it = nulls.begin(); it != nulls.end(); ++it)
        {
          *rangeBegin = *it;
          ++rangeBegin;
        }
      }
      else
      {
        // Add NULLs from back or front that depends on a sorting direction used for the column
        permutation.insert(permutation.end(), nulls.begin(), nulls.end());
      }

      for (auto it = permutation.begin(); it != permutation.end(); ++it)
      {
        *rangeBegin = *it;
        ++rangeBegin;
      }
    }
    // !!! FREE RAM
    mm_->release(bytes);
  }
  // Check if copy ellision works here.
  // Set a stack of equal values ranges. Comp complexity is O(N), mem complexity is O(N)
  ranges2Sort_ = std::move(ranges4Sort);
  // std::cout << " sortByColumnCF_  2 columns.size() " << columns.size() << std::endl;
  if (columns.size() > 1)
  {
    columns.erase(columns.begin());
    return sortByColumnCF<false>(columns);
  }

  return isFailure;
}

void FlatOrderBy::processRow(const rowgroup::Row& row)
{
}

// This method calculates the final permutation offset to start with
// taking OFFSET and LIMIT into account.
void FlatOrderBy::finalize()
{
  // Signal getData() to return false if perm is empty. Impossible case though.
  if (permutation_.size() == 0 || start_ > permutation_.size())
  {
    flatCurPermutationDiff_ = -1;
    return;
  }

  flatCurPermutationDiff_ = std::min(count_, permutation_.size() - start_);
}

// returns false when finishes
// WIP try the forward direction looping over permutation_.
bool FlatOrderBy::getData(rowgroup::RGData& data)
{
  static constexpr IterDiffT rgMaxSize = rowgroup::rgCommonSize;
  auto rowsToReturn = std::min(rgMaxSize, flatCurPermutationDiff_);
  // This negation is to convert a number of rows into array idx.
  if (rowsToReturn - 1 < 0)
  {
    return false;
  }

  data.reinit(rgOut_, rowsToReturn);
  rgOut_.setData(&data);
  rgOut_.resetRowGroup(0);
  rgOut_.initRow(&outRow_);
  rgOut_.getRow(0, &outRow_);
  auto cols = std::min(inRow_.getColumnCount(), rgOut_.getColumnCount());
  auto i = flatCurPermutationDiff_ - 1;
  auto stopAt = flatCurPermutationDiff_ - rowsToReturn;
  for (; i >= stopAt; --i)
  {
    // find src row, copy into dst row
    // RGData::getRow but need to init an arg row first
    assert(static_cast<size_t>(i) < permutation_.size() && permutation_[i].rgdataID < rgDatas_.size());
    // Get RowPointer
    rgDatas_[permutation_[i].rgdataID].getRow(permutation_[i].rowID, &inRow_);
    // std::cout << "inRow i " << i << inRow_.toString() << std::endl;
    rowgroup::copyRowM(inRow_, &outRow_, cols);
    outRow_.nextRow();  // I don't like this way of iterating the rows
    // std::cout << "outRow i " << i << outRow_.toString() << std::endl;
  }
  rgOut_.setRowCount(rowsToReturn);
  // This reduces the global row counter by the number of rows returned by this getData() call.
  flatCurPermutationDiff_ -= rowsToReturn;

  // std::cout << rgOut_.toString() << std::endl;
  return true;
}

const string FlatOrderBy::toString() const
{
  ostringstream oss;
  oss << "FlatOrderBy   cols: ";
  for (auto [rgColumnID, sortDirection] : jobListorderByRGColumnIDs_)
  {
    oss << "(" << rgColumnID << "," << ((sortDirection) ? "Asc" : "Desc)");
  }
  oss << " offset-" << start_ << " count-" << count_;

  // if (fDistinct)
  //   oss << " distinct";

  oss << endl;

  return oss.str();
}

}  // namespace joblist

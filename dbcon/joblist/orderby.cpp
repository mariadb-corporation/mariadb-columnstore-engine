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
//#define NDEBUG
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

  //   fErrorCode = ERR_LIMIT_TOO_BIG;

  // locate column position in the rowgroup
  //   map<uint32_t, uint32_t> keyToIndexMap;

  //   for (uint64_t i = 0; i < rg.getKeys().size(); ++i)
  //   {
  //     if (keyToIndexMap.find(rg.getKeys()[i]) == keyToIndexMap.end())
  //       keyToIndexMap.insert(make_pair(rg.getKeys()[i], i));
  //   }

  //   vector<pair<uint32_t, bool> >::const_iterator i = jobInfo.orderByColVec.begin();

  //   for (; i != jobInfo.orderByColVec.end(); i++)
  //   {
  //     map<uint32_t, uint32_t>::iterator j = keyToIndexMap.find(i->first);
  //     idbassert(j != keyToIndexMap.end());

  //     // fOrderByCond.push_back(IdbSortSpec(j->second, i->second ^ invertRules));
  //   }

  // limit row count info
  //   if (isMultiThreaded)
  //   {
  //     // CS can't apply offset at the first stage
  //     // otherwise it looses records.
  //     start_ = 0;
  //     count_ = jobInfo.limitStart + jobInfo.limitCount;
  //   }
  //   else
  {
    start_ = jobInfo.limitStart;
    count_ = jobInfo.limitCount;
  }
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

bool FlatOrderBy::sort()
{
  bool isFailure = false;
  for (auto [rgColumnID, sortDirection] : jobListorderByRGColumnIDs_)
  {
    if (sortByColumn(rgColumnID, sortDirection))
    {
      isFailure = true;
      return isFailure;
    }
  }
  return isFailure;
}

bool FlatOrderBy::sortByColumn(const uint32_t columnId, const bool sortDirection)
{
  bool isFailure = false;

  switch (rg_.getColType(columnId))
  {
    case execplan::CalpontSystemCatalog::TINYINT:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::CHAR:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::SMALLINT:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::INT:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::DATE:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::BIGINT:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::BIGINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumn_<execplan::CalpontSystemCatalog::BIGINT, StorageType, EncodedKeyType>(
          columnId, sortDirection);
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
      break;
    }

    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UINT:
    {
      break;
    }

    case execplan::CalpontSystemCatalog::UBIGINT:
    {
      break;
    }

    default: break;
  }
  return isFailure;
}

template <datatypes::SystemCatalog::ColDataType ColType, typename StorageType, typename EncodedKeyType>
void FlatOrderBy::initialPermutationKeysNulls(const uint32_t columnID, std::vector<EncodedKeyType>& keys,
                                              std::vector<PermutationType>& nulls)
{
  rowgroup::Row r;
  // Replace with a constexpr
  auto nullValue = sorting::getNullValue<StorageType>(ColType);
  RGDataOrRowIDType rgDataId = 0;
  permutation_.reserve(rgDatas_.size() * 8192);  // WIP hardcode
  for (auto& rgData : rgDatas_)
  {
    // set rgdata
    rg_.setData(&rgData);
    rg_.initRow(&r);    // Move this outside the loop    // Row iterator call seems unreasonably costly here
    rg_.getRow(0, &r);  // get first row
    auto rowCount = rg_.getRowCount();
    auto bytes = (sizeof(EncodedKeyType) + sizeof(FlatOrderBy::PermutationType)) * rowCount;
    if (!mm_->acquire(bytes))
    {
      cerr << IDBErrorInfo::instance()->errorMsg(ERR_LIMIT_TOO_BIG) << " @" << __FILE__ << ":" << __LINE__;
      throw IDBExcept(ERR_LIMIT_TOO_BIG);
    }

    for (decltype(rowCount) i = 0; i < rowCount; ++i)
    {
      EncodedKeyType value = rg_.getColumnValue<ColType, StorageType>(columnID, i);
      if (value != nullValue)
      {
        keys.push_back(value);
        permutation_.push_back({rgDataId, i, 0});
      }
      else
      {
        nulls.push_back({rgDataId, i, 0});
      }
    }
    ++rgDataId;
  }
}

template <datatypes::SystemCatalog::ColDataType ColType, typename StorageType, typename EncodedKeyType>
void FlatOrderBy::loopIterKeysNullsPerm(const uint32_t columnID, std::vector<EncodedKeyType>& keys,
                                        PermutationVec& nulls, PermutationVec& permutation,
                                        PermutationVecIter begin, PermutationVecIter end)
{
  rowgroup::Row r;
  // Replace with a constexpr
  auto nullValue = sorting::getNullValue<StorageType>(ColType);
  // RGDataOrRowIDType rgDataId = 0;
  rg_.initRow(&r);  // Row iterator call seems unreasonably costly here
  // !!!!!!!!!! data set sizes don't match here.
  // Длина keys не равна длине диапазона permutation_ - надо как-то учесть или выбросить
  // null-ы.
  auto permSrcBegin = (permutation.empty()) ? begin : permutation.begin();
  auto permSrcEnd = (permutation.empty()) ? end : permutation.end();

  // Проверить актуальность итераторов
  for (auto p = permSrcBegin; p != permSrcEnd; ++p)
  {
    // set rgdata
    rg_.setData(&rgDatas_[p->rgdataID]);
    // rg_.getRow(permutation.rowID, &r);  // get first row
    EncodedKeyType value = rg_.getColumnValue<ColType, StorageType>(columnID, p->rowID);
    if (value != nullValue)
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
    const IterDiffT beginOffset, typename std::vector<EncodedKeyType>::const_iterator begin,
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

// For numeric datatypes only
// add numerics only concept check
// This function uses iterators extensively. Be careful to not alter iterator after it is assigned.
template <datatypes::SystemCatalog::ColDataType ColType, typename StorageType, typename EncodedKeyType>
bool FlatOrderBy::exchangeSortByColumn_(const uint32_t columnID, const bool sortDirection)
{
  bool isFailure = false;
  std::vector<EncodedKeyType> keys_;
  PermutationVec nulls_;
  if (permutation_.empty())
  {
    initialPermutationKeysNulls<ColType, StorageType, EncodedKeyType>(columnID, keys_, nulls_);
    ranges2Sort_.push({0, permutation_.size()});
  }

  // count sorting for types with width < 8 bytes.
  // use sorting class
  // sorting must move permute members when keys are moved
  // Comp complexity O(logN*N), mem compl O(N)
  // Use iterators from the stack
  // while (!sameValuesRanges_.empty())
  // {
  //   auto [sameValuesRangeBegin, sameValuesRangeEnd] = sameValuesRanges_.top();
  //   sameValuesRanges_.pop();
  //   if (sortDirection)
  //   {
  //     sorting::mod_pdqsort(keys.begin(), keys.end(), permutation_.begin(), permutation_.end(),
  //                          std::less<EncodedKeyType>());
  //   }
  //   else
  //   {
  //     sorting::mod_pdqsort(keys.begin(), keys.end(), permutation_.begin(), permutation_.end(),
  //                          std::greater<EncodedKeyType>());
  //   }
  // }

  // Add stopped check into this and other loops
  Ranges2SortQueue ranges2Sort;
  while (!ranges2Sort_.empty())
  {
    auto [sameValuesRangeBeginDist, sameValuesRangeEndDist] = ranges2Sort_.front();
    auto sameValuesRangeBegin = permutation_.begin() + sameValuesRangeBeginDist;
    auto sameValuesRangeEnd = permutation_.begin() + sameValuesRangeEndDist;
    auto permCurRangeBeginEndDist = sameValuesRangeEndDist - sameValuesRangeBeginDist;
    auto permutationBeginEndDist = std::distance(permutation_.begin(), permutation_.end());
    ranges2Sort_.pop();
    // Grab RAM  keys + permutation. NULLS are not accounted
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
    if (!keys_.empty() || !nulls_.empty())
    {
      keys = std::move(keys_);
      nulls = std::move(nulls_);
      permutation = std::move(permutation_);
    }
    else
    {
      // Sorting the full original set. This is either the first iteration or all key values are equal.
      if (permCurRangeBeginEndDist == permutationBeginEndDist)
      {
        permutation = std::move(permutation_);
        loopIterKeysNullsPerm<ColType, StorageType>(columnID, keys, nulls, permutation, permutation.begin(),
                                                    permutation.end());
      }
      else
      {
        loopIterKeysNullsPerm<ColType, StorageType>(columnID, keys, nulls, permutation, sameValuesRangeBegin,
                                                    sameValuesRangeEnd);
      }
    }

    // auto permBegin = permutation_.begin() + (std::distance(permutation_.begin(), sameValuesRangeBegin));
    // auto permEnd = permutation_.begin() + (std::distance(permutation_.begin(), sameValuesRangeEnd));
    auto permBegin = permutation.begin();
    auto permEnd = permutation.end();
    assert(std::distance(keys.begin(), keys.end()) == std::distance(permBegin, permEnd));
    // большой логический косяк - диапазон permutation_ потенциально содержит null-ы в середине, а с краю
    // могут быть индексы не NULL ключей. Решение в лоб - копия диапазона перестановки.fg
    if (sortDirection)
    {
      sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, std::greater<EncodedKeyType>());
    }
    else
    {
      sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, std::less<EncodedKeyType>());
    }
    // Use && here
    auto ranges = populateRanges<EncodedKeyType>(sameValuesRangeBeginDist, keys.begin(), keys.end());
    while (!ranges.empty())
    {
      auto range = ranges.front();
      ranges.pop();
      ranges2Sort.push(range);
    }

    // Add NULL from the back
    permutation.insert(permutation.end(), nulls.begin(), nulls.end());
    // Adding NULLs range into ranges
    if (nulls.size() > 1)
    {
      // left = offset + keys.size, right = offset + keys.size + nulls.size
      auto left = sameValuesRangeBeginDist + std::distance(keys.begin(), keys.end());
      auto right = sameValuesRangeBeginDist +
                   std::distance(keys.begin(), keys.end() + std::distance(nulls.begin(), nulls.end()));
      ranges2Sort.push({left, right});
    }
    // Populating permutation_ with a sorted run produced by this iteration.
    if (permCurRangeBeginEndDist == permutationBeginEndDist)
    {
      permutation_ = std::move(permutation);
    }
    else
    {
      assert(sameValuesRangeBegin + std::distance(permutation.begin(), permutation.end()) !=
             permutation_.end() + 1);
      for (auto it = permutation.begin(); it != permutation.end(); ++it)
      {
        *sameValuesRangeBegin = *it;
        ++sameValuesRangeBegin;
      }
    }
    // !!! FREE RAM
    mm_->release(bytes);
  }
  // Check if copy ellision works here.
  // Set a stack of equal values ranges. Comp complexity is O(N), mem complexity is O(N)
  ranges2Sort_ = std::move(ranges2Sort);

  return isFailure;
}

// template <enum datatypes::SystemCatalog::ColDataType, typename StorageType, typename EncodedKeyType>
// bool FlatOrderBy::distributionSortByColumn_(const uint32_t columnId)
// {
//   bool isFailure = false;
//   return isFailure;
// }

void FlatOrderBy::processRow(const rowgroup::Row& row)
{
}

void FlatOrderBy::finalize()
{
  // Signal getData() to return false if perm is empty. Impossible case though.
  flatCurPermutationDiff_ = (permutation_.size() > 0) ? permutation_.size() : -1;
}

// returns false when finishes
bool FlatOrderBy::getData(rowgroup::RGData& data)
{
  static constexpr IterDiffT rgMaxSize = rowgroup::rgCommonSize;
  auto rowsToReturn = std::min(rgMaxSize, flatCurPermutationDiff_);
  // This reduces the global row counter by the number of rows returned by this getData() call.
  flatCurPermutationDiff_ -= rowsToReturn;
  // This negation is to convert a number of rows into array idx.
  auto i = rowsToReturn - 1;
  if (i < 0)
  {
    return false;
  }

  data.reinit(rgOut_, rowsToReturn);
  rgOut_.setData(&data);
  rgOut_.resetRowGroup(0);
  rgOut_.initRow(&outRow_);
  rgOut_.getRow(0, &outRow_);

  for (; i >= 0; --i)
  {
    // find src row, copy into dst row
    // RGData::getRow but need to init an arg row first
    assert(static_cast<size_t>(i) < permutation_.size() && permutation_[i].rgdataID < rgDatas_.size());
    // Get RowPointer
    rgDatas_[permutation_[i].rgdataID].getRow(permutation_[i].rowID, &inRow_);
    // std::cout << "inRow i " << i << inRow_.toString() << std::endl;
    copyRow(inRow_, &outRow_);
    outRow_.nextRow();  // I don't like this way of iterating the rows
    // std::cout << "outRow i " << i << outRow_.toString() << std::endl;
  }
  rgOut_.setRowCount(rowsToReturn);
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

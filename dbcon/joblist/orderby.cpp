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
  // std::cout << "addBatch " << rowCount << std::endl;
  auto bytes = rg_.getSizeWithStrings(rowCount);
  if (!mm_->acquire(bytes))
  {
    cerr << IDBErrorInfo::instance()->errorMsg(ERR_LIMIT_TOO_BIG) << " @" << __FILE__ << ":" << __LINE__;
    throw IDBExcept(ERR_LIMIT_TOO_BIG);
  }

  // WIP what if copy-move happens here? It is a costly op
  rgDatas_.push_back(rgData);

  return isFailure;
}

bool FlatOrderBy::sortCF(const uint32_t id)
{
  bool isFailure = false;
  constexpr const bool isFirst = true;
  PermutationVec emptyPermutation;
  Ranges2SortQueue emptyRanges2Sort;
  assert(!jobListorderByRGColumnIDs_.empty());
  SortingThreads emptyVec;
  if (sortByColumnCF<isFirst>(id, jobListorderByRGColumnIDs_, std::move(emptyPermutation),
                              std::move(emptyRanges2Sort), emptyVec))
  {
    isFailure = true;
    return isFailure;
  }

  return isFailure;
}

bool FlatOrderBy::sortByColumnCFNoPerm(const uint32_t id, joblist::OrderByKeysType columns,
                                       PermutationVec&& permutation, Ranges2SortQueue&& ranges2Sort,
                                       const SortingThreads& prevPhaseThreads)
{
  auto perm = permutation;
  auto ranges = ranges2Sort;
  return sortByColumnCF<false>(id, columns, std::move(perm), std::move(ranges), prevPhaseThreads);
}

template <bool IsFirst>
bool FlatOrderBy::sortByColumnCF(const uint32_t id, joblist::OrderByKeysType columns,
                                 PermutationVec&& permutation, Ranges2SortQueue&& ranges2Sort,
                                 const SortingThreads& prevPhaseThreads)
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
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
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
                                       EncodedKeyType>(id, columnId, sortDirection, columns,
                                                       std::move(permutation), std::move(ranges2Sort),
                                                       prevPhaseThreads);
      }
      else if (sorting::isDictColumn(columnType, columnWidth) &&
               (columnWidth < rg_.getStringTableThreshold() || forceInline))
      {
        using EncodedKeyType = utils::ConstString;
        using StorageType = utils::ShortConstString;
        // No difference b/w VARCHAR, CHAR and TEXT types yet
        return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::VARCHAR, StorageType,
                                       EncodedKeyType>(id, columnId, sortDirection, columns,
                                                       std::move(permutation), std::move(ranges2Sort),
                                                       prevPhaseThreads);
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
                                           EncodedKeyType>(id, columnId, sortDirection, columns,
                                                           std::move(permutation), std::move(ranges2Sort),
                                                           prevPhaseThreads);
          }

          case 2:
          {
            using StorageType =
                datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::SMALLINT>::type;
            return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::VARCHAR, StorageType,
                                           EncodedKeyType>(id, columnId, sortDirection, columns,
                                                           std::move(permutation), std::move(ranges2Sort),
                                                           prevPhaseThreads);
          };

          case 4:
          {
            using StorageType =
                datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::INT>::type;
            return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::VARCHAR, StorageType,
                                           EncodedKeyType>(id, columnId, sortDirection, columns,
                                                           std::move(permutation), std::move(ranges2Sort),
                                                           prevPhaseThreads);
          };

          case 8:
          {
            using StorageType =
                datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::BIGINT>::type;
            return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::VARCHAR, StorageType,
                                           EncodedKeyType>(id, columnId, sortDirection, columns,
                                                           std::move(permutation), std::move(ranges2Sort),
                                                           prevPhaseThreads);
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
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }

    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
    {
      auto columnWidth = rg_.getColumnWidth(columnId);
      if (columnWidth == datatypes::DECIMAL128WIDTH)
      {
        using StorageType = datatypes::TSInt128;
        using EncodedKeyType = datatypes::TSInt128;
        return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::DECIMAL, StorageType,
                                       EncodedKeyType>(id, columnId, sortDirection, columns,
                                                       std::move(permutation), std::move(ranges2Sort),
                                                       prevPhaseThreads);
      }
      else if (columnWidth == datatypes::DECIMAL64WIDTH)
      {
        using StorageType = datatypes::WidthToSIntegralType<datatypes::DECIMAL64WIDTH>::type;
        using EncodedKeyType = StorageType;
        return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::DECIMAL, StorageType,
                                       EncodedKeyType>(id, columnId, sortDirection, columns,
                                                       std::move(permutation), std::move(ranges2Sort),
                                                       prevPhaseThreads);
      }
      else
      {
        throw logging::NotImplementedExcept("sortByColumnCF(): U-/DECIMAL has an unexpected width" +
                                            std::to_string(columnWidth));
      }
    }

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::DOUBLE>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::DOUBLE, StorageType,
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }

    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::INT:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::INT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::INT, StorageType,
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }

    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::FLOAT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::FLOAT, StorageType,
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }

    case execplan::CalpontSystemCatalog::DATE:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::DATE>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::DATE, StorageType,
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }

    case execplan::CalpontSystemCatalog::BIGINT:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::BIGINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::BIGINT, StorageType,
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }
    case execplan::CalpontSystemCatalog::DATETIME:
    {
      using StorageType =
          datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::DATETIME>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::DATETIME, StorageType,
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }
    case execplan::CalpontSystemCatalog::TIME:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::TIME>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::TIME, StorageType,
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }
    case execplan::CalpontSystemCatalog::TIMESTAMP:
    {
      using StorageType =
          datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::TIMESTAMP>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::TIMESTAMP, StorageType,
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }

    case execplan::CalpontSystemCatalog::UTINYINT:
    {
      using StorageType =
          datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::UTINYINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::UTINYINT, StorageType,
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }

    case execplan::CalpontSystemCatalog::USMALLINT:
    {
      using StorageType =
          datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::USMALLINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::USMALLINT, StorageType,
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }

    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UINT:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::UINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::UINT, StorageType,
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }

    case execplan::CalpontSystemCatalog::UBIGINT:
    {
      using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::UBIGINT>::type;
      using EncodedKeyType = StorageType;
      return exchangeSortByColumnCF_<IsFirst, execplan::CalpontSystemCatalog::UBIGINT, StorageType,
                                     EncodedKeyType>(id, columnId, sortDirection, columns,
                                                     std::move(permutation), std::move(ranges2Sort),
                                                     prevPhaseThreads);
    }

    default: break;
  }
  return isFailure;
}

template <datatypes::SystemCatalog::ColDataType ColType, typename StorageType, typename EncodedKeyType>
void FlatOrderBy::initialPermutationKeysNulls(const uint32_t id, const uint32_t columnID,
                                              const bool nullsFirst, std::vector<EncodedKeyType>& keys,
                                              PermutationVec& permutation, PermutationVec& nulls)
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
      PermutationType permute = {rgDataId, i, id};
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

  // std::cout << "initialPermutationKeysNulls " << rg_.toString() << std::endl;
}

template <datatypes::SystemCatalog::ColDataType ColType, typename StorageType, typename EncodedKeyType>
auto FlatOrderBy::loopIterKeysNullsPerm(const uint32_t columnID, const bool nullsFirst,
                                        PermutationVecIter begin, PermutationVecIter end,
                                        const SortingThreads& prevPhaseThreads)
{
  std::vector<EncodedKeyType> keys;
  PermutationVec nulls;
  PermutationVec permutation;
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
    if (prevPhaseThreads.empty())
    {
      rg_.setData(&rgDatas_[p->rgdataID]);  // WIP costly thing
    }
    else
    {
      rg_.setData(&(prevPhaseThreads[p->threadID]->getRGDatas()[p->rgdataID]));
    }
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
  return std::tuple{std::move(keys), std::move(nulls), std::move(permutation)};
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

template <bool IsFirst, datatypes::SystemCatalog::ColDataType ColType, typename StorageType,
          typename EncodedKeyType>
  requires IsTrue<IsFirst> bool
FlatOrderBy::exchangeSortByColumnCF_(const uint32_t id, const uint32_t columnID, const bool isAscDirection,
                                     joblist::OrderByKeysType columns, PermutationVec&& permutationA,
                                     Ranges2SortQueue&& ranges2SortA, const SortingThreads& prevPhaseThreads)
{
  bool isFailure = false;
  // ASC = true, DESC = false
  // MCS finally reads records from TAS in opposite to nullsFirst value,
  // if nullsFirst is true the NULLS will be in the end and vice versa.
  const bool nullsFirst = !isAscDirection;
  auto bytes = (sizeof(EncodedKeyType) + sizeof(FlatOrderBy::PermutationType)) * rgDatas_.size() *
               rowgroup::rgCommonSize;
  Ranges2SortQueue ranges2Sort;
  PermutationVec permutation;
  {
    std::vector<EncodedKeyType> keys;
    PermutationVec nulls;
    // Grab RAM  keys + permutation. NULLS are counted also.
    if (!mm_->acquire(bytes))
    {
      cerr << IDBErrorInfo::instance()->errorMsg(ERR_LIMIT_TOO_BIG) << " @" << __FILE__ << ":" << __LINE__;
      throw IDBExcept(ERR_LIMIT_TOO_BIG);
    }
    auto start = std::chrono::steady_clock::now();

    initialPermutationKeysNulls<ColType, StorageType, EncodedKeyType>(id, columnID, nullsFirst, keys,
                                                                      permutation, nulls);

    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout << "keys " + std::to_string(id) + " elapsed time: " << elapsed_seconds.count() << "s\n";

    auto permBegin = (nullsFirst) ? permutation.begin() + nulls.size() : permutation.begin();
    auto permEnd = (nullsFirst) ? permutation.end() : permutation.end() - nulls.size();
    assert(std::distance(keys.begin(), keys.end()) == std::distance(permBegin, permEnd));
    // assert(permutation.size() == keys.size() + ((nullsFirst) ? 0 : nulls.size()));
    // sortDirection is true = ASC
    start = std::chrono::steady_clock::now();

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
    end = std::chrono::steady_clock::now();
    elapsed_seconds = end - start;
    std::cout << "sort " + std::to_string(id) + " elapsed time: " << elapsed_seconds.count() << "s\n";

    // Use && here
    if (columns.size() > 1)
    {
      ranges2Sort = populateRanges<EncodedKeyType>(columnID, (nullsFirst) ? nulls.size() : 0ULL, keys.begin(),
                                                   keys.end());

      // Adding NULLs range into ranges
      if (nulls.size() > 1)
      {
        if (isAscDirection)
        {
          ranges2Sort.push({keys.size(), permutation.size()});
        }
        else
        {
          ranges2Sort.push({0, nulls.size()});
        }
      }
    }

    // Check if copy ellision works here.
    // Set a stack of equal values ranges. Comp complexity is O(N), mem complexity is O(N)
  }
  // std::cout << " exchangeSortByColumnCF_ perm size " << permutation.size() << std::endl;
  if (columns.size() > 1)
  {
    // !!! FREE RAM
    mm_->release(bytes);
    columns.erase(columns.begin());
    return sortByColumnCF<false>(id, columns, std::move(permutation), std::move(ranges2Sort),
                                 prevPhaseThreads);
  }
  else
  {
    permutation_ = std::move(permutation);
    // WIP In a single thread ranges2Sort is empty.
    ranges2Sort_ = std::move(ranges2Sort);
  }
  return isFailure;
}

template <bool IsFirst, datatypes::SystemCatalog::ColDataType ColType, typename StorageType,
          typename EncodedKeyType>
  requires IsFalse<IsFirst> bool
FlatOrderBy::exchangeSortByColumnCF_(const uint32_t id, const uint32_t columnID, const bool sortDirection,
                                     joblist::OrderByKeysType columns, PermutationVec&& permutationA,
                                     Ranges2SortQueue&& ranges2SortA, const SortingThreads& prevPhaseThreads)
{
  // std::cout << " sortByColumnCF_  1 columns.size() " << columns.size() << std::endl;

  bool isFailure = false;
  // ASC = true, DESC = false
  // MCS finally reads records from TAS in opposite to nullsFirst value,
  // if nullsFirst is true the NULLS will be in the end and vice versa.
  const bool isAscDirection = !sortDirection;

  // Add stopped check into this and other loops
  PermutationVec permutation = permutationA;
  Ranges2SortQueue ranges2Sort = ranges2SortA;
  Ranges2SortQueue ranges2SortNext;

  while (!ranges2Sort.empty())
  {
    const auto [sameValuesRangeBeginDist, sameValuesRangeEndDist] = ranges2Sort.front();
    // for (auto p : permutation_)
    // {
    //   std::cout << " permutation_ " << p.rowID << " ";
    // }
    // std::cout << std::endl;
    // std::cout << "The next column iter left " << sameValuesRangeBeginDist << " right "
    //           << sameValuesRangeEndDist << std::endl;
    const auto sameValuesRangeBegin = permutation.begin() + sameValuesRangeBeginDist;
    const auto sameValuesRangeEnd = permutation.begin() + sameValuesRangeEndDist;
    const auto permCurRangeBeginEndDist = sameValuesRangeEndDist - sameValuesRangeBeginDist;
    ranges2Sort.pop();
    auto bytes = (sizeof(EncodedKeyType) + sizeof(FlatOrderBy::PermutationType)) * permCurRangeBeginEndDist;
    if (!mm_->acquire(bytes))
    {
      cerr << IDBErrorInfo::instance()->errorMsg(ERR_LIMIT_TOO_BIG) << " @" << __FILE__ << ":" << __LINE__;
      throw IDBExcept(ERR_LIMIT_TOO_BIG);
    }

    // std::vector<EncodedKeyType> keys;
    // PermutationVec nulls;
    // PermutationVec permutation;
    auto [keys, nulls, permutation] = loopIterKeysNullsPerm<ColType, StorageType, EncodedKeyType>(
        columnID, isAscDirection, sameValuesRangeBegin, sameValuesRangeEnd, prevPhaseThreads);

    auto permBegin = permutation.begin();
    auto permEnd = permutation.end();
    assert(std::distance(keys.begin(), keys.end()) == std::distance(permBegin, permEnd));
    assert(permutation.size() == keys.size());
    if constexpr (std::is_same<EncodedKeyType, utils::ConstString>::value)
    {
      datatypes::Charset cs(rg_.getCharset(columnID));
      if (isAscDirection)
      {
        auto cmp = [&cs](EncodedKeyType x, EncodedKeyType y) { return cs.strnncollsp(x, y) < 0; };
        sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, cmp);
      }
      else
      {
        auto cmp = [&cs](EncodedKeyType x, EncodedKeyType y) { return cs.strnncollsp(x, y) > 0; };
        sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, cmp);
      }
    }
    else
    {
      if (isAscDirection)
      {
        sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, std::less<EncodedKeyType>());
      }
      else
      {
        sorting::mod_pdqsort(keys.begin(), keys.end(), permBegin, permEnd, std::greater<EncodedKeyType>());
      }
    }
    // for (auto p : permutation)
    // {
    //   std::cout << " permutation after the sorting " << p.rowID << " " << std::endl;
    // }
    // std::cout << std::endl;

    // Use && here
    if (columns.size() > 1)
    {
      // std::cout << "nulls size " << nulls.size() << std::endl;
      // WIP Should do copy ellision
      auto ranges = populateRanges<EncodedKeyType>(
          columnID, (isAscDirection) ? sameValuesRangeBeginDist + nulls.size() : sameValuesRangeBeginDist,
          keys.begin(), keys.end());
      while (!ranges.empty())
      {
        auto range = ranges.front();
        ranges.pop();
        ranges2SortNext.push(range);
      }
      if (nulls.size() > 1)
      {
        if (sortDirection)
        {
          ranges2SortNext.push({sameValuesRangeBeginDist + keys.size(),
                                sameValuesRangeBeginDist + keys.size() + nulls.size()});
        }
        else
        {
          ranges2SortNext.push({sameValuesRangeBeginDist, sameValuesRangeBeginDist + nulls.size()});
        }
      }
    }

    // Copy back the sorted run into the permutation.
    assert(sameValuesRangeBegin + permutation.size() + nulls.size() != permutation.end() + 1);
    {
      auto rangeBegin = sameValuesRangeBegin;
      if (isAscDirection)
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
    // for (auto p : permutation_)
    // {
    //   std::cout << " permutation_ after the mergin " << p.rowID << " " << std::endl;
    // }

    // !!! FREE RAM
    mm_->release(bytes);
  }
  // Set a stack of equal values ranges. Comp complexity is O(N), mem complexity is O(N)
  // std::cout << " sortByColumnCF_  2 columns.size() " << columns.size() << std::endl;
  if (columns.size() > 1)
  {
    columns.erase(columns.begin());
    return sortByColumnCF<false>(id, columns, std::move(permutation), std::move(ranges2SortNext),
                                 prevPhaseThreads);
  }
  else
  {
    permutation_ = std::move(permutation);
    ranges2Sort_ = std::move(ranges2SortNext);
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
bool FlatOrderBy::getData(rowgroup::RGData& data, const SortingThreads& prevPhaseThreads)
{
  static constexpr IterDiffT rgMaxSize = rowgroup::rgCommonSize;
  auto rowsToReturn = std::min(rgMaxSize, flatCurPermutationDiff_);
  // This negation is to convert a number of rows into array idx.
  if (rowsToReturn - 1 < 0)
  {
    return false;
  }
  // for (auto p : permutation_)
  // {
  //   std::cout << " permutation_ getData " << p.rowID << " " << std::endl;
  // }
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
    // std::cout << "getData i " << i << " row " << permutation_[i].rowID << std::endl;
    // find src row, copy into dst row
    // RGData::getRow but need to init an arg row first
    assert(static_cast<size_t>(i) < permutation_.size());
    assert((prevPhaseThreads.empty() && permutation_[i].rgdataID < rgDatas_.size()) ||
           (!prevPhaseThreads.empty() &&
            permutation_[i].rgdataID < prevPhaseThreads[permutation_[i].threadID]->getRGDatas().size()));
    auto p = permutation_[i];
    // Get RowPointer
    if (prevPhaseThreads.empty())
    {
      rgDatas_[p.rgdataID].getRow(p.rowID, &inRow_);
    }
    else
    {
      prevPhaseThreads[p.threadID]->getRGDatas()[p.rgdataID].getRow(p.rowID, &inRow_);
    }

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

  oss << endl;

  return oss.str();
}

}  // namespace joblist

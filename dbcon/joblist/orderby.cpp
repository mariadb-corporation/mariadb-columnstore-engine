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
    if (sortByColumn(rgColumnID))
    {
      isFailure = true;
      return isFailure;
    }
  }
  return isFailure;
}

bool FlatOrderBy::sortByColumn(const uint32_t columnId)
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
          columnId);
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

// For numeric datatypes only
// add numerics only concept check
template <datatypes::SystemCatalog::ColDataType ColType, typename StorageType, typename EncodedKeyType>
bool FlatOrderBy::exchangeSortByColumn_(const uint32_t columnID)
{
  bool isFailure = false;
  std::vector<EncodedKeyType> keys;
  std::vector<PermutationType> nulls;
  rowgroup::Row r;
  // Replace with a constexpr
  auto nullValue = sorting::getNullValue<StorageType>(ColType);

  RGDataOrRowIDType rgDataId = 0;
  for (auto& rgData : rgDatas_)
  {
    // set rgdata
    rg_.setData(&rgData);
    rg_.initRow(&r);    // Row iterator call seems unreasonably costly here
    rg_.getRow(0, &r);  // get first row
    auto rowCount = rg_.getRowCount();
    auto bytes = (sizeof(EncodedKeyType) + sizeof(FlatOrderBy::PermutationType)) * rowCount;
    if (!mm_->acquire(bytes))
    {
      cerr << IDBErrorInfo::instance()->errorMsg(ERR_LIMIT_TOO_BIG) << " @" << __FILE__ << ":" << __LINE__;
      throw IDBExcept(ERR_LIMIT_TOO_BIG);
    }

    permutation_.reserve(permutation_.size() + rowCount);

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
  // count sorting for types with width < 8 bytes.
  // use sorting class
  // sorting must move permute members when keys are moved
  sorting::mod_pdqsort(keys.begin(), keys.end(), permutation_.begin(), permutation_.end(),
                       std::greater<EncodedKeyType>());

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
  //   queue<RGData> tempQueue;

  // WIP check how it is this
  //   if (fRowGroup.getRowCount() > 0)
  //     fDataQueue.push(fData);

  //   if (fOrderByQueue.size() > 0)
  //   {
  //     // *DRRTUY Very memory intensive. CS needs to account active
  //     // memory only and release memory if needed.
  //     uint64_t memSizeInc = fRowGroup.getSizeWithStrings() - fRowGroup.getHeaderSize();

  //     if (!fRm->getMemory(memSizeInc, fSessionMemLimit))
  //     {
  //       cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode) << " @" << __FILE__ << ":" << __LINE__;
  //       throw IDBExcept(fErrorCode);
  //     }
  //     fMemSize += memSizeInc;

  //     uint64_t offset = 0;
  //     uint64_t i = 0;
  //     // Reduce queue size by an offset value if it applicable.
  //     uint64_t queueSizeWoOffset = fOrderByQueue.size() > start_ ? fOrderByQueue.size() - start_ : 0;
  //     list<RGData> tempRGDataList;

  //     if (count_ <= queueSizeWoOffset)
  //     {
  //       offset = count_ % fRowsPerRG;
  //       if (!offset && count_ > 0)
  //         offset = fRowsPerRG;
  //     }
  //     else
  //     {
  //       offset = queueSizeWoOffset % fRowsPerRG;
  //       if (!offset && queueSizeWoOffset > 0)
  //         offset = fRowsPerRG;
  //     }

  //     list<RGData>::iterator tempListIter = tempRGDataList.begin();

  //     i = 0;
  //     uint32_t rSize = fRow0.getSize();
  //     uint64_t preLastRowNumb = fRowsPerRG - 1;
  //     fData.reinit(fRowGroup, fRowsPerRG);
  //     fRowGroup.setData(&fData);
  //     fRowGroup.resetRowGroup(0);
  //     // *DRRTUY This approach won't work with
  //     // OFSET > fRowsPerRG
  //     offset = offset != 0 ? offset - 1 : offset;
  //     fRowGroup.getRow(offset, &fRow0);

  //     while ((fOrderByQueue.size() > start_) && (i++ < count_))
  //     {
  //       const OrderByRow& topRow = fOrderByQueue.top();
  //       row1.setData(topRow.fData);
  //       copyRow(row1, &fRow0);
  //       fRowGroup.incRowCount();
  //       offset--;
  //       fRow0.prevRow(rSize);
  //       fOrderByQueue.pop();

  //       // if RG has fRowsPerRG rows
  //       if (offset == (uint64_t)-1)
  //       {
  //         tempRGDataList.push_front(fData);

  //         if (!fRm->getMemory(memSizeInc, fSessionMemLimit))
  //         {
  //           cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode) << " @" << __FILE__ << ":" << __LINE__;
  //           throw IDBExcept(fErrorCode);
  //         }
  //         fMemSize += memSizeInc;

  //         fData.reinit(fRowGroup, fRowsPerRG);
  //         fRowGroup.setData(&fData);
  //         fRowGroup.resetRowGroup(0);  // ?
  //         fRowGroup.getRow(preLastRowNumb, &fRow0);
  //         offset = preLastRowNumb;
  //       }
  //     }
  //     // Push the last/only group into the queue.
  //     if (fRowGroup.getRowCount() > 0)
  //       tempRGDataList.push_front(fData);

  //     for (tempListIter = tempRGDataList.begin(); tempListIter != tempRGDataList.end(); tempListIter++)
  //       tempQueue.push(*tempListIter);

  //     fDataQueue = tempQueue;
  //   }
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
    std::cout << "inRow i " << i << inRow_.toString() << std::endl;
    copyRow(inRow_, &outRow_);
    outRow_.nextRow();  // I don't like this way of iterating the rows
    std::cout << "outRow i " << i << outRow_.toString() << std::endl;
  }
  rgOut_.setRowCount(rowsToReturn);
  std::cout << rgOut_.toString() << std::endl;
  return true;
}

const string FlatOrderBy::toString() const
{
  ostringstream oss;
  oss << "FlatOrderBy   cols: ";
  //   vector<IdbSortSpec>::const_iterator i = fOrderByCond.begin();

  //   for (; i != fOrderByCond.end(); i++)
  //     oss << "(" << i->fIndex << "," << ((i->fAsc) ? "Asc" : "Desc") << ","
  //         << ((i->fNf) ? "null first" : "null last") << ") ";

  //   oss << " start-" << start_ << " count-" << count_;

  //   if (fDistinct)
  //     oss << " distinct";

  //   oss << endl;

  return oss.str();
}

}  // namespace joblist

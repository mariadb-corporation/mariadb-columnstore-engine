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

#include <netinet/in.h>
#include <algorithm>

#include "heaporderby.h"
#include "conststring.h"
#include "pdqorderby.h"
#include "vlarray.h"

namespace sorting
{
KeyType::KeyType(rowgroup::RowGroup& rg, const joblist::OrderByKeysType& colsAndDirection,
                 const sorting::PermutationType p, uint8_t* buf)
{
  key_ = buf;
  uint8_t* pos = key_;
  for (auto [columnId, isDsc] : colsAndDirection)
  {
    uint32_t columnWidth = rg.getColumnWidth(columnId);
    auto columnType = rg.getColType(columnId);
    switch (columnType)
    {
      case execplan::CalpontSystemCatalog::TINYINT:
      {
        using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::INT>::type;
        const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::TINYINTNULL);
        bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
        *pos++ = static_cast<uint8_t>(isNotNull);
        std::memcpy(pos, valueBuf, columnWidth);
        StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
        *valPtr ^= 0x80;
        *valPtr = (isDsc) ? ~*valPtr : *valPtr;
        pos += columnWidth;
        break;
      }

      case execplan::CalpontSystemCatalog::SMALLINT:
      {
        using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::INT>::type;
        const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::SMALLINTNULL);
        bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
        *pos++ = static_cast<uint8_t>(isNotNull);
        std::memcpy(pos, valueBuf, columnWidth);
        StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
        *valPtr ^= 0x8000;
        *valPtr = (isDsc) ? ~htons(*valPtr) : htons(*valPtr);
        pos += columnWidth;
        break;
      }
      case execplan::CalpontSystemCatalog::INT:
      {
        using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::INT>::type;
        const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::INTNULL);
        bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
        *pos++ = static_cast<uint8_t>(isNotNull);
        std::memcpy(pos, valueBuf, columnWidth);
        StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
        *valPtr ^= 0x80000000;
        *valPtr = (isDsc) ? ~htonl(*valPtr) : htonl(*valPtr);
        pos += columnWidth;
        break;
      }
      case execplan::CalpontSystemCatalog::BIGINT:
      {
        using StorageType =
            datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::BIGINT>::type;
        const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::BIGINTNULL);
        bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
        *pos++ = static_cast<uint8_t>(isNotNull);
        std::memcpy(pos, valueBuf, columnWidth);
        StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
        *valPtr ^= 0x8000000000000000;
        *valPtr = (isDsc) ? ~htonll(*valPtr) : htonll(*valPtr);
        pos += columnWidth;
        break;
      }
      case execplan::CalpontSystemCatalog::VARCHAR:
      case execplan::CalpontSystemCatalog::CHAR:
      case execplan::CalpontSystemCatalog::TEXT:
      {
        using EncodedKeyType = utils::ConstString;
        EncodedKeyType value(nullptr, 0);
        bool forceInline = rg.getForceInline()[columnId];
        auto cs = datatypes::Charset(rg.getCharset(columnId));
        uint flags = (rg.getCharset(columnId)->state & MY_CS_NOPAD) ? 0 : cs.getDefaultFlags();
        size_t keyWidth = columnWidth;
        if (rg.isLongString(columnId) && columnWidth >= rg.getStringTableThreshold() && !forceInline)
        {
          using StorageType = utils::ConstString;
          // No difference b/w VARCHAR, CHAR and TEXT types yet
          value = rg.getColumnValue<execplan::CalpontSystemCatalog::VARCHAR, StorageType, EncodedKeyType>(
              columnId, p.rowID);
          *pos++ = 1;
          keyWidth = rg.getStringTableThreshold();
          // std::memset(pos, 0, rg.getStringTableThreshold());
          // std::cout << "KeyType ctor flags " << flags << std::endl;
          // // WIP Hardcode
          // [[maybe_unused]] size_t nActualWeights =
          //     cs.strnxfrm(pos, rg.getStringTableThreshold(), value.length(),
          //                 reinterpret_cast<const uchar*>(value.str()), value.length(), flags);
          // pos += rg.getStringTableThreshold();
        }
        else if (sorting::isDictColumn(columnType, columnWidth) &&
                 (columnWidth <= rg.getStringTableThreshold() || forceInline))
        {
          using StorageType = utils::ShortConstString;
          // No difference b/w VARCHAR, CHAR and TEXT types yet
          value = rg.getColumnValue<execplan::CalpontSystemCatalog::VARCHAR, StorageType, EncodedKeyType>(
              columnId, p.rowID);
          *pos++ = 1;
          // std::memset(pos, 0, columnWidth);
          // // std::cout << "KeyType ctor flags " << flags << std::endl;
          // // WIP Hardcode
          // [[maybe_unused]] size_t nActualWeights =
          //     cs.strnxfrm(pos, columnWidth, value.length(), reinterpret_cast<const uchar*>(value.str()),
          //                 value.length(), flags);
          // pos += columnWidth;
        }
        // WIP
        else if (!sorting::isDictColumn(columnType, columnWidth))
        {
          switch (columnWidth)
          {
            case 1:
            {
              using StorageType =
                  datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::TINYINT>::type;
              value = rg.getColumnValue<execplan::CalpontSystemCatalog::VARCHAR, StorageType, EncodedKeyType>(
                  columnId, p.rowID);
              *pos++ = 1;
              // std::memset(pos, 0, columnWidth);
              // // std::cout << "KeyType ctor flags " << flags << std::endl;
              // // WIP Hardcode
              // [[maybe_unused]] size_t nActualWeights =
              //     cs.strnxfrm(pos, columnWidth, value.length(), reinterpret_cast<const
              //     uchar*>(value.str()),
              //                 value.length(), flags);
              // pos += columnWidth;
              break;
            }

            case 2:
            {
              using StorageType =
                  datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::SMALLINT>::type;
              value = rg.getColumnValue<execplan::CalpontSystemCatalog::VARCHAR, StorageType, EncodedKeyType>(
                            columnId, p.rowID)
                          .rtrimZero();
              value.rtrimZero();
              *pos++ = 1;
              break;
            };

            case 4:
            {
              using StorageType =
                  datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::INT>::type;
              value = rg.getColumnValue<execplan::CalpontSystemCatalog::VARCHAR, StorageType, EncodedKeyType>(
                            columnId, p.rowID)
                          .rtrimZero();
              value.rtrimZero();
              *pos++ = 1;
              // std::memset(pos, 0, columnWidth);
              // // std::cout << "KeyType ctor flags " << fls.rtrimZero()ags << std::endl;
              // // WIP Hardcode
              // [[maybe_unused]] size_t nActualWeights =
              //     cs.strnxfrm(pos, columnWidth, value.length(), reinterpret_cast<const
              //     uchar*>(value.str()),
              //                 value.length(), flags);
              break;
            };

            case 8:
            {
              using StorageType =
                  datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::BIGINT>::type;
              value = rg.getColumnValue<execplan::CalpontSystemCatalog::VARCHAR, StorageType, EncodedKeyType>(
                            columnId, p.rowID)
                          .rtrimZero();
              *pos++ = 1;
              // std::memset(pos, 0, keyWidth);
              // [[maybe_unused]] size_t nActualWeights =
              //     cs.strnxfrm(pos, keyWidth, keyWidth, reinterpret_cast<const uchar*>(value.str()),
              //                 value.length(), flags);
              // pos += keyWidth;
              break;
            };
            default: idbassert(0);
          }
        }
        std::memset(pos, 0, keyWidth);
        [[maybe_unused]] size_t nActualWeights = cs.strnxfrm(
            pos, keyWidth, keyWidth, reinterpret_cast<const uchar*>(value.str()), value.length(), flags);
        uint8_t* end = pos + nActualWeights;
        if (isDsc)
        {
          for (; pos < end; ++pos)
          {
            *pos = ~*pos;
          }
        }
        else
        {
          pos += nActualWeights;
        }
      }
      default: break;
    }
  }
}
bool KeyType::less(const KeyType& r, const rowgroup::RowGroup& rg,
                   const joblist::OrderByKeysType& colsAndDirection) const
{
  assert(rg.getColumnCount() >= 1);
  auto& widths = rg.getColWidths();
  rowgroup::OffsetType l_offset = 0;
  rowgroup::OffsetType r_offset = 0;
  size_t colsNumber = colsAndDirection.size();
  int32_t cmpResult = 0;
  for (size_t i = 0; !cmpResult && i <= colsNumber; ++i)
  {
    if (i < colsNumber)
    {
      auto [colIdx, isAsc] = colsAndDirection[i];
      ++r_offset;
      if (widths[colIdx] <= rg.getStringTableThreshold())
      {
        r_offset += widths[colIdx];
        continue;
      }
      else
      {
        r_offset += rg.getStringTableThreshold();
      }
    }
    // std::cout << "less pos left " << std::hex << (uint64_t)(key_ + l_offset) << " pos right "
    //           << (uint64_t)(r.key_ + l_offset) << std::endl;

    cmpResult = memcmp(key_ + l_offset, r.key_ + l_offset, r_offset - l_offset);
    l_offset = r_offset;
  }

  return cmpResult < 0;
}

bool KeyType::less(const KeyType& r, rowgroup::RowGroup& rg, const joblist::OrderByKeysType& colsAndDirection,
                   const PermutationType leftP, const PermutationType rightP,
                   const sorting::SortingThreads& prevPhaseSorting) const
{
  assert(rg.getColumnCount() >= 1);
  auto& widths = rg.getColWidths();
  // auto& colTypes = rg.getColTypes();
  rowgroup::OffsetType l_offset = 0;
  rowgroup::OffsetType r_offset = 0;
  size_t colsNumber = colsAndDirection.size();
  // int8_t normKeyCmpResult = 0;
  [[maybe_unused]] int32_t unsizedCmpResult = 0;
  int32_t cmpResult = 0;
  const uint32_t stringThreshold = rg.getStringTableThreshold();
  bool longString = false;
  bool isDsc = false;
  for (size_t i = 0; !cmpResult && i <= colsNumber; ++i)
  {
    if (i < colsNumber)
    {
      auto [columnId, isDscLocal] = colsAndDirection[i];
      ++r_offset;
      if (widths[columnId] <= stringThreshold)
      {
        r_offset += widths[columnId];
        longString = false;
        continue;
      }
      else
      {
        r_offset += stringThreshold;
        longString = true;
        isDsc = isDscLocal;
      }
    }
    // std::cout << "less pos left " << std::hex << (uint64_t)(key_ + l_offset) << " pos right "
    //           << (uint64_t)(r.key_ + l_offset) << std::endl;

    cmpResult = memcmp(key_ + l_offset, r.key_ + l_offset, r_offset - l_offset);
    if (!cmpResult && longString)
    {
      // std::cout << "less on long strings " << std::endl;
      using StorageType = utils::ConstString;
      using EncodedKeyType = StorageType;
      auto [columnId, isAsc] = colsAndDirection[i];
      rg.setData(&(prevPhaseSorting[leftP.threadID]->getRGDatas()[leftP.rgdataID]));
      auto leftV = rg.getColumnValue<execplan::CalpontSystemCatalog::VARCHAR, StorageType, EncodedKeyType>(
          columnId, leftP.rowID);
      rg.setData(&(prevPhaseSorting[rightP.threadID]->getRGDatas()[rightP.rgdataID]));
      auto rightV = rg.getColumnValue<execplan::CalpontSystemCatalog::VARCHAR, StorageType, EncodedKeyType>(
          columnId, rightP.rowID);
      auto cs = datatypes::Charset(rg.getCharset(columnId));
      cmpResult = cs.strnncollsp(leftV, rightV);
      cmpResult = (isDsc) ? -cmpResult : cmpResult;
      longString = false;
    }
    // std::cout << "result " << std::dec << cmpResult << " left " << *((int64_t*)(key_ + 1)) << " left "
    //           << std::hex << *((int64_t*)(key_ + 1)) << " right " << std::dec << *((int64_t*)(r.key_ +
    //           1))
    //           << std::hex << " right " << *((int64_t*)(r.key_ + 1)) << std::endl;
    l_offset = r_offset;
  }

  return cmpResult < 0;
}

HeapOrderBy::HeapOrderBy(const rowgroup::RowGroup& rg, const joblist::OrderByKeysType& sortingKeyCols,
                         const size_t limitStart, const size_t limitCount, joblist::MemManager* mm,
                         const uint32_t threadId, const sorting::SortingThreads& prevPhaseSorting,
                         const uint32_t threadNum, const sorting::ValueRangesVector& ranges)
 : start_(limitStart)
 , count_(limitCount)
 , recordsLeftInRanges_(0)
 , jobListorderByRGColumnIDs_(sortingKeyCols)
 , rg_(rg)
 , rgOut_(rg)
{
  mm_.reset(mm);
  data_.reinit(rg_, 2);
  rg_.setData(&data_);
  rg_.resetRowGroup(0);
  rg_.initRow(&inRow_);
  rg_.initRow(&outRow_);
  rg_.getRow(0, &inRow_);
  rg_.getRow(1, &outRow_);

  size_t heapSize = threadNum << 1;
  // WIP doesn't take varchars prefixes into account
  // keyBytesSize_ = rg.getOffsets().back() - 2 + sortingKeyCols.size();
  keyBytesSize_ = sortingKeyCols.size();
  for_each(jobListorderByRGColumnIDs_.begin(), jobListorderByRGColumnIDs_.end(),
           [this](auto p)
           {
             keyBytesSize_ +=
                 (rg_.isLongString(p.first)) ? rg_.getStringTableThreshold() : rg_.getColumnWidth(p.first);
           });
  mm_->acquire((heapSize + 1) * keyBytesSize_);
  keyBuf_.reset(new uint8_t[(heapSize + 1) * keyBytesSize_]);

  // Make a permutations struct
  for (auto range = ranges.begin(); auto& prevPhaseSortingThread : prevPhaseSorting)
  {
    auto [rangeLeft, rangeRight] = *range;
    ++range;
    if (rangeLeft == rangeRight)
    {
      continue;
    }
    // std::cout << "HeapOrderBy ctor perm size " << prevPhaseSortingThread->getPermutation().size()
    //           << std::endl;
    auto srcPermBegin = prevPhaseSortingThread->getPermutation().begin() + rangeLeft;
    auto srcPermEnd = prevPhaseSortingThread->getPermutation().begin() + rangeRight;
    auto rangeSize = rangeRight - rangeLeft + 1;
    recordsLeftInRanges_ += rangeSize;
    ranges_.push_back({});
    ranges_.back().reserve(rangeSize);
    ranges_.back().push_back(ImpossiblePermute);
    ranges_.back().insert(ranges_.back().end(), srcPermBegin, srcPermEnd);
  }

  // Build a heap
  auto half = threadNum;
  for (size_t i = 0; i < heapSize; ++i)
  {
    heap_.push_back({KeyType(rg_, keyBuf_.get() + (i * keyBytesSize_)), {0, 0, i % half}});
  }

  size_t prevHeapIdx = heap_.size();
  for (size_t heapIdx = threadNum; heapIdx >= 1; heapIdx >>= 1, prevHeapIdx >>= 1)  // level
  {
    for (size_t i = heapIdx; i < prevHeapIdx; ++i)  // all level items
    {
      size_t bestChildIdx = i;
      size_t prevBestChildIdx = bestChildIdx;
      for (; bestChildIdx < half;)
      {
        prevBestChildIdx = bestChildIdx;
        // get min of left and right.
        bestChildIdx = idxOfMin(heap_, prevBestChildIdx << 1, (prevBestChildIdx << 1) + 1, sortingKeyCols);
        assert(bestChildIdx >= heapIdx && bestChildIdx <= (prevBestChildIdx << 1) + 1 &&
               bestChildIdx < heap_.size());
        // swap the best and the current
        std::swap(heap_[prevBestChildIdx], heap_[bestChildIdx]);
      }

      auto threadId = heap_[prevBestChildIdx].second.threadID;
      auto p = ranges_[threadId].back();
      rg_.setData(&(prevPhaseSorting[p.threadID]->getRGDatas()[p.rgdataID]));
      auto buf = heap_[bestChildIdx].first.key();
      auto key = KeyType(rg_, sortingKeyCols, p, buf);
      // WIP the simplified key inversion is ~
      heap_[bestChildIdx] = {key, p};
      ranges_[threadId].pop_back();
    }
  }
  recordsLeftInRanges_ -= heap_.size() - 1;
}

size_t HeapOrderBy::idxOfMin(Heap& heap, const size_t left, const size_t right,
                             const joblist::OrderByKeysType& colsAndDirection)
{
  if (heap[left].first.less(heap[right].first, rg_, colsAndDirection))
    return left;
  return right;
}

PermutationType HeapOrderBy::getTopPermuteFromHeap(std::vector<HeapUnit>& heap,
                                                   const SortingThreads& prevPhaseSortingThread)
{
  auto half = heap.size() >> 1;
  PermutationType topPermute = heap[1].second;
  // Use the buffer of the top.
  auto buf = heap_[1].first.key();
  size_t bestChildIdx = 1;
  size_t heapIdx = 1;
  // WIP remove IP comparison
  for (; heapIdx < half && heap[bestChildIdx].second != ImpossiblePermute; heapIdx = bestChildIdx)
  {
    // get min of left and right.
    bestChildIdx = idxOfMin(heap, heapIdx << 1, (heapIdx << 1) + 1, jobListorderByRGColumnIDs_);
    std::swap(heap[heapIdx], heap[bestChildIdx]);
  }

  heapIdx = (heapIdx == 1) ? 1 : heapIdx >> 1;
  if (heap[heapIdx >> 1].second == ImpossiblePermute || heap[heapIdx].second == ImpossiblePermute)
  {
    return topPermute;
  }
  // Use the previously swapped threadId and key buffer;
  auto threadId = heap[heapIdx].second.threadID;
  auto p = ranges_[threadId].back();
  ranges_[threadId].pop_back();
  if (p == ImpossiblePermute)
  {
    *buf = 2;  // impossible key WIP
    heap_[bestChildIdx] = {KeyType(rg_, buf), ImpossiblePermute};
  }
  else
  {
    assert(p.threadID < prevPhaseSortingThread.size() &&
           p.rgdataID < prevPhaseSortingThread[p.threadID]->getRGDatas().size());
    rg_.setData(&(prevPhaseSortingThread[p.threadID]->getRGDatas()[p.rgdataID]));
    // WIP the simplified key inversion is ~
    heap_[bestChildIdx] = {KeyType(rg_, jobListorderByRGColumnIDs_, p, buf), p};
  }

  return topPermute;
}

size_t HeapOrderBy::getData(rowgroup::RGData& data, const SortingThreads& prevPhaseThreads)
{
  static constexpr size_t rgMaxSize = rowgroup::rgCommonSize;
  auto rowsToReturnEstimate = std::min(rgMaxSize, recordsLeftInRanges_ + heap_.size() - 1);
  // There is impossible top permute so nothing to return.
  if (heap_[1].second == ImpossiblePermute)
  {
    return 0;
  }
  // rgData cleanup should be done upper the call stack
  // Simplify the boilerplate
  data.reinit(rgOut_, rowsToReturnEstimate);
  rgOut_.setData(&data);
  rgOut_.resetRowGroup(0);
  rgOut_.initRow(&outRow_);
  rgOut_.getRow(0, &outRow_);
  auto cols = rgOut_.getColumnCount();
  size_t rowsToReturn = 0;
  for (; rowsToReturn < rowsToReturnEstimate; ++rowsToReturn)
  {
    auto p = getTopPermuteFromHeap(heap_, prevPhaseThreads);
    // WIP
    if (p == ImpossiblePermute)
    {
      break;
    }
    prevPhaseThreads[p.threadID]->getRGDatas()[p.rgdataID].getRow(p.rowID, &inRow_);
    rowgroup::copyRowM(inRow_, &outRow_, cols);
    outRow_.nextRow();  // I don't like this way of iterating the rows
  }
  rgOut_.setRowCount(rowsToReturn);
  recordsLeftInRanges_ -= rowsToReturn;
  return rowsToReturn;
}

const string HeapOrderBy::toString() const
{
  ostringstream oss;
  oss << "HeapOrderBy   cols: ";
  for (auto [rgColumnID, sortDirection] : jobListorderByRGColumnIDs_)
  {
    oss << "(" << rgColumnID << "," << ((sortDirection) ? "Asc" : "Desc)");
  }
  oss << " offset-" << start_ << " count-" << count_;

  oss << endl;

  return oss.str();
}

}  // namespace sorting
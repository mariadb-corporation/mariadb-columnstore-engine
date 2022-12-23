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

#include "heaporderby.h"
#include "conststring.h"
#include "pdqorderby.h"
#include "vlarray.h"

namespace sorting
{
KeyType::KeyType(rowgroup::RowGroup& rg, const joblist::OrderByKeysType& colsAndDirection,
                 const sorting::PermutationType p)
//  : key_(utils::VLArray<uint8_t, 16>(9))
{
  key_ = new uint8_t(rg.getOffsets().back() - 2 + colsAndDirection.size());
  uint8_t* pos = key_;
  const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::BIGINTNULL);
  for (auto [colId, isAsc] : colsAndDirection)
  {
    uint32_t colWidth = rg.getColumnWidth(colId);
    const uint8_t* valueBuf = rg.getColumnValueBuf(colId, p.rowID);
    // std::cout << "KeyType width " << colWidth << " " << *((int64_t*)valueBuf) << " "
    //           << *((int64_t*)nullValuePtr) << std::endl;
    if (memcmp(nullValuePtr, valueBuf, colWidth))
    {
      *pos++ = 1;
      std::memcpy(pos, valueBuf, colWidth);
      pos += colWidth;
    }
    else
    {
      *pos++ = 0;
    }
  }

  values_ = {};
}

HeapOrderBy::HeapOrderBy(const rowgroup::RowGroup& rg, const joblist::OrderByKeysType& sortingKeyCols,
                         const size_t limitStart, const size_t limitCount, joblist::RMMemManager* mm,
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

  // Make a permutations struct
  // ranges_ = std::vector(threadNum, PermutationVec());
  // auto prevPhaseSortingThread = prevPhaseSorting.begin();
  // auto range = ranges.begin();
  for (auto range = ranges.begin(); auto& prevPhaseSortingThread : prevPhaseSorting)
  // prevPhaseSortingThread != prevPhaseSorting.end() && range != ranges.end();
  //   ++prevPhaseSortingThread,
  {
    auto [rangeLeft, rangeRight] = *range;
    ++range;
    if (rangeLeft == rangeRight)
    {
      continue;
    }
    auto srcPermBegin = prevPhaseSortingThread->getPermutation().begin() + rangeLeft;
    auto srcPermEnd = prevPhaseSortingThread->getPermutation().begin() + rangeRight;
    auto rangeSize = rangeRight - rangeLeft + 1;
    recordsLeftInRanges_ += rangeSize;
    ranges_.push_back({});
    ranges_.back().reserve(rangeSize);
    ranges_.back().push_back(impossiblePermute);
    ranges_.back().insert(ranges_.back().end(), srcPermBegin, srcPermEnd);
  }

  // Build a heap
  size_t heapSize = threadNum << 1;
  auto half = threadNum;
  // heap_ = std::vector<HeapUnit>(heapSize, {KeyType(), {0, 0, 0}});
  for (size_t i = 0; i < heapSize; ++i)
  {
    heap_.push_back({KeyType(rg_), {0, 0, i % half}});
  }

  size_t prevHeapIdx = heap_.size();
  for (size_t heapIdx = threadNum; heapIdx >= 1; heapIdx >>= 1, prevHeapIdx >>= 1)  // level
  {
    for (size_t i = heapIdx; i < prevHeapIdx; ++i)  // all level items
    {
      size_t bestChildIdx = i;
      size_t prevBestChildIdx = bestChildIdx;
      for (; bestChildIdx < half; prevBestChildIdx = bestChildIdx)
      {
        // get min of left and right.
        bestChildIdx =
            idxOfMin<KeyType>(heap_, prevBestChildIdx << 1, (prevBestChildIdx << 1) + 1, sortingKeyCols);
        assert(bestChildIdx >= heapIdx && bestChildIdx <= (prevBestChildIdx << 1) + 1 &&
               bestChildIdx < heap_.size());
        // swap the best and the current
        heap_[prevBestChildIdx] = heap_[bestChildIdx];
      }

      auto threadId = heap_[bestChildIdx].second.threadID;
      auto p = ranges_[threadId].back();
      rg_.setData(&(prevPhaseSorting[p.threadID]->getRGDatas()[p.rgdataID]));
      auto key = KeyType(rg_, sortingKeyCols, p);
      // // WIP
      // KeyType v = rg_.getColumnValue<datatypes::SystemCatalog::BIGINT, KeyType, KeyType>(0, p.rowID);
      // WIP the simplified key inversion is ~
      heap_[bestChildIdx] = {key, p};
      ranges_[threadId].pop_back();
    }
  }
  recordsLeftInRanges_ -= heap_.size() - 1;
}

template <typename T>
size_t HeapOrderBy::idxOfMin(Heap& heap, const size_t left, const size_t right,
                             const joblist::OrderByKeysType& colsAndDirection)
{
  // if (heap[right].second == impossiblePermute || heap[left].first < heap[right].first)
  if (heap[right].second == impossiblePermute ||
      heap[left].first.less(heap[right].first, rg_, colsAndDirection))
    return left;
  return right;
}

template <typename T>
PermutationType HeapOrderBy::getTopPermuteFromHeap(std::vector<HeapUnit>& heap,
                                                   const SortingThreads& prevPhaseSortingThread)
{
  auto half = heap.size() >> 1;
  PermutationType topPermute = heap[1].second;
  size_t bestChildIdx = 0;
  for (size_t heapIdx = 1; heapIdx < half && heap[bestChildIdx].second != impossiblePermute;
       heapIdx = bestChildIdx)
  {
    // get min of left and right.
    bestChildIdx = idxOfMin<KeyType>(heap, heapIdx << 1, (heapIdx << 1) + 1, jobListorderByRGColumnIDs_);
    heap[heapIdx] = heap[bestChildIdx];
  }

  if (heap[bestChildIdx].second == impossiblePermute)
  {
    return topPermute;
  }
  auto threadId = heap[bestChildIdx].second.threadID;
  auto p = ranges_[threadId].back();
  ranges_[threadId].pop_back();
  if (p == impossiblePermute)
  {
    heap_[bestChildIdx].second = impossiblePermute;
  }
  else
  {
    rg_.setData(&(prevPhaseSortingThread[p.threadID]->getRGDatas()[p.rgdataID]));
    // WIP column id
    // KeyType v = rg_.getColumnValue<datatypes::SystemCatalog::BIGINT, KeyType, KeyType>(0, p.rowID);
    // WIP the simplified key inversion is ~
    heap_[bestChildIdx] = {KeyType(rg_, jobListorderByRGColumnIDs_, p), p};
  }

  return topPermute;
}

size_t HeapOrderBy::getData(rowgroup::RGData& data, const SortingThreads& prevPhaseThreads)
{
  static constexpr size_t rgMaxSize = rowgroup::rgCommonSize;
  auto rowsToReturnEstimate = std::min(rgMaxSize, recordsLeftInRanges_ + heap_.size() - 1);
  // There is impossible top permute so nothing to return.
  if (heap_[1].second == impossiblePermute)
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
    auto p = getTopPermuteFromHeap<KeyType>(heap_, prevPhaseThreads);
    // WIP
    if (p == impossiblePermute)
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
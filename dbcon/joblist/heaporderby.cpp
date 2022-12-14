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

namespace sorting
{
HeapOrderBy::HeapOrderBy(const rowgroup::RowGroup& rg, const joblist::JobInfo& jobInfo,
                         const uint32_t threadId, const sorting::SortingThreads& prevPhaseSorting,
                         const uint32_t threadNum, const sorting::ValueRangesVector& ranges,
                         const joblist::OrderByKeysType& sortingKeyCols)
 : start_(jobInfo.limitStart)
 , count_(jobInfo.limitCount)
 , recordsLeftInRanges_(0)
 , jobListorderByRGColumnIDs_(sortingKeyCols)
 , rg_(rg)
 , rgOut_(rg)
{
  mm_.reset(new joblist::RMMemManager(jobInfo.rm, jobInfo.umMemLimit));
  data_.reinit(rg_, 2);
  rg_.setData(&data_);
  rg_.resetRowGroup(0);
  rg_.initRow(&inRow_);
  rg_.initRow(&outRow_);
  rg_.getRow(0, &inRow_);
  rg_.getRow(1, &outRow_);

  // Make a permutations struct
  ranges_ = std::vector(threadNum, PermutationVec());
  auto prevPhaseSortingThread = prevPhaseSorting.begin();
  auto range = ranges[threadId].begin();
  for (size_t i = 0; prevPhaseSortingThread != prevPhaseSorting.end() && range != ranges[threadId].end();
       ++prevPhaseSortingThread, ++range, ++i)
  {
    auto [rangeLeft, rangeRight] = *range;
    if (rangeLeft == rangeRight)
    {
      continue;
    }
    auto srcPermBegin = (*prevPhaseSortingThread)->getPermutation().begin() + rangeLeft;
    auto srcPermEnd = (*prevPhaseSortingThread)->getPermutation().begin() + rangeRight;
    ranges_[i].reserve(rangeRight - rangeLeft + 1);
    ranges_[i].push_back(impossiblePermute);
    ranges_[i].insert(ranges_[i].end(), srcPermBegin, srcPermEnd);
  }

  // Build a heap
  size_t heapSize = threadNum << 1;
  heap_ = std::vector<HeapUnit>(heapSize, {KeyType(), {0, 0, 0}});
  for (size_t i = 0; i < threadNum; ++i)
  {
    // The last value prevents pop_back() on an empty vector.
    assert(!ranges[i].empty());
    auto p = ranges_[i].back();
    rg_.setData(&(prevPhaseSorting[p.threadID]->getRGDatas()[p.rgdataID]));
    // WIP Take bigint value from the first row
    KeyType v = rg_.getColumnValue<datatypes::SystemCatalog::BIGINT, KeyType, KeyType>(0, p.rowID);
    // WIP the simplified key inversion is ~
    heap_[i + threadNum] = {v, p};
    ranges_[i].pop_back();
  }
  size_t prevHeapIdx = threadNum;
  for (size_t heapIdx = threadNum >> 1; heapIdx >= 1; heapIdx >>= 1, prevHeapIdx >>= 1)
  {
    for (size_t i = heapIdx; i < prevHeapIdx; ++i)
    {
      // get min of left and right.
      size_t bestChildIdx = idxOfMin<KeyType>(heap_, i << 1, (i << 1) + 1);
      assert(bestChildIdx >= heapIdx && bestChildIdx < prevHeapIdx);
      heap_[i] = heap_[bestChildIdx];
      auto threadId = heap_[i].second.threadID;
      auto p = ranges_[threadId].back();
      rg_.setData(&(prevPhaseSortingThread[p.threadID]->getRGDatas()[p.rgdataID]));
      KeyType v = rg_.getColumnValue<datatypes::SystemCatalog::BIGINT, KeyType, KeyType>(0, p.rowID);
      // WIP the simplified key inversion is ~
      heap_[bestChildIdx] = {v, p};
      ranges_[threadId].pop_back();
    }
  }
  for (auto& range : ranges)
  {
    recordsLeftInRanges_ += range.size();
  }
}

template <typename T>
size_t HeapOrderBy::idxOfMin(Heap& heap, const size_t left, const size_t right)
{
  // WIP impossible permutation
  if (heap[left].first < heap[right].first)
    return left;
  return right;
}

template <typename T>
PermutationType HeapOrderBy::getTopPermuteFromHeap(std::vector<HeapUnit>& heap,
                                                   const SortingThreads& prevPhaseSortingThread)
{
  auto maxIdx = heap.size() >> 1;
  PermutationType topPermute = heap[1].second;
  size_t bestChildIdx = 0;
  for (size_t heapIdx = 1; heapIdx < maxIdx;)
  {
    // get min of left and right.
    bestChildIdx = idxOfMin<KeyType>(heap, heapIdx << 1, (heapIdx << 1) + 1);
    heap[heapIdx] = heap[bestChildIdx];
    heapIdx = bestChildIdx;
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
    KeyType v = rg_.getColumnValue<datatypes::SystemCatalog::BIGINT, KeyType, KeyType>(0, p.rowID);
    // WIP the simplified key inversion is ~
    heap_[bestChildIdx] = {v, p};
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
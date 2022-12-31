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
#include <netinet/in.h>
#include "conststring.h"
#include "pdqorderby.h"
#include "vlarray.h"

namespace sorting
{
KeyType::KeyType(rowgroup::RowGroup& rg, const joblist::OrderByKeysType& colsAndDirection,
                 const sorting::PermutationType p, uint8_t* buf)
//  : key_(utils::VLArray<uint8_t, 16>(9))
{
  // key_ = new uint8_t(rg.getOffsets().back() - 2 + colsAndDirection.size());
  key_ = buf;
  uint8_t* pos = key_;
  const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::BIGINTNULL);
  for (auto [colId, isAsc] : colsAndDirection)
  {
    uint32_t colWidth = rg.getColumnWidth(colId);
    const uint8_t* valueBuf = rg.getColumnValueBuf(colId, p.rowID);
    // std::cout << "KeyType width " << std::dec << colWidth << " " << *((int64_t*)valueBuf) << " "
    //           << *((int64_t*)nullValuePtr) << std::endl;
    // std::cout << "KeyType pos 1 " << std::hex << (uint64_t)pos << std::endl;
    if (memcmp(nullValuePtr, valueBuf, colWidth))
    {
      *pos++ = 1;
      std::memcpy(pos, valueBuf, colWidth);
      // std::cout << "KeyType val 2 " << std::dec << *((int64_t*)pos) << " " << std::hex << *((int64_t*)pos)
      //           << std::endl;
      // std::cout << "KeyType pos 2 " << std::hex << (uint64_t)pos << std::endl;
      int64_t* valPtr = reinterpret_cast<int64_t*>(pos);
      *valPtr &= 0x7FFFFFFFFFFFFFFF;
      *valPtr = htonll(*valPtr);
      pos += colWidth;
    }
    else
    {
      *pos++ = 0;
    }
  }

  // values_ = {};
}

bool KeyType::less(const KeyType& r, const rowgroup::RowGroup& rg,
                   const joblist::OrderByKeysType& colsAndDirection) const
{
  assert(rg.getColumnCount() >= 1);
  auto& widths = rg.getColWidths();
  // auto& colTypes = rg.getColTypes();
  rowgroup::OffsetType l_offset = 0;
  rowgroup::OffsetType r_offset = 1;
  size_t colsNumber = colsAndDirection.size();
  // int8_t normKeyCmpResult = 0;
  [[maybe_unused]] int32_t unsizedCmpResult = 0;
  int32_t cmpResult = 0;
  for (size_t i = 0; !cmpResult && i <= colsNumber; ++i)
  {
    if (i < colsNumber)
    {
      auto [colIdx, isAsc] = colsAndDirection[i];
      if (widths[colIdx] <= 8)
      {
        r_offset += widths[colIdx] + 1;
        continue;
      }
    }
    // std::cout << "less pos left " << std::hex << (uint64_t)(key_ + l_offset) << " pos right "
    //           << (uint64_t)(r.key_ + l_offset) << std::endl;

    // while ((cmpResult = memcmp(key_ + l_offset, r.key_ + l_offset, r_offset - l_offset)) == 0 /*&&
    //      (unsizedCmpResult = CHARSET::cmp(unsizedKeys[i - 1], unsizedKeys[i])) == 0*/)
    cmpResult = memcmp(key_ + l_offset, r.key_ + l_offset, r_offset - l_offset);
    // std::cout << "result " << std::dec << cmpResult << " left " << *((int64_t*)(key_ + 1)) << " left "
    //           << std::hex << *((int64_t*)(key_ + 1)) << " right " << std::dec << *((int64_t*)(r.key_ + 1))
    //           << std::hex << " right " << *((int64_t*)(r.key_ + 1)) << std::endl;
    l_offset = r_offset;
  }

  return cmpResult < 0;  //|| (cmpResult == 0 && unsizedCmpResult < 0);
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
  keyBytesSize_ = rg.getOffsets().back() - 2 + sortingKeyCols.size();
  mm_->acquire((heapSize + 1) * keyBytesSize_);
  keyBuf_.reset(new uint8_t[(heapSize + 1) * keyBytesSize_]);

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
    // std::cout << "HeapOrderBy ctor perm size " << prevPhaseSortingThread->getPermutation().size()
    //           << std::endl;
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
        bestChildIdx =
            idxOfMin<KeyType>(heap_, prevBestChildIdx << 1, (prevBestChildIdx << 1) + 1, sortingKeyCols);
        assert(bestChildIdx >= heapIdx && bestChildIdx <= (prevBestChildIdx << 1) + 1 &&
               bestChildIdx < heap_.size());
        // swap the best and the current
        std::swap(heap_[prevBestChildIdx], heap_[bestChildIdx]);
      }

      auto threadId = heap_[prevBestChildIdx].second.threadID;
      auto p = ranges_[threadId].back();
      rg_.setData(&(prevPhaseSorting[p.threadID]->getRGDatas()[p.rgdataID]));
      auto buf = heap_[prevBestChildIdx].first.key();
      auto key = KeyType(rg_, sortingKeyCols, p, buf);
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
  size_t bestChildIdx = 1;
  size_t heapIdx = 1;
  for (; heapIdx < half && heap[bestChildIdx].second != impossiblePermute; heapIdx = bestChildIdx)
  {
    // get min of left and right.
    bestChildIdx = idxOfMin<KeyType>(heap, heapIdx << 1, (heapIdx << 1) + 1, jobListorderByRGColumnIDs_);
    std::swap(heap[heapIdx], heap[bestChildIdx]);
  }

  if (heap[bestChildIdx].second == impossiblePermute)
  {
    return topPermute;
  }
  heapIdx = (heapIdx == 1) ? 1 : heapIdx >> 1;
  // Use the previously swapped threadId and key buffer;
  auto threadId = heap[heapIdx].second.threadID;
  auto buf = heap_[heapIdx].first.key();
  auto p = ranges_[threadId].back();
  ranges_[threadId].pop_back();
  if (p == impossiblePermute)
  {
    heap_[bestChildIdx].second = impossiblePermute;
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
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

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "conststring.h"
#include "jlf_common.h"
#include "pdqorderby.h"
#include "resourcemanager.h"
#include "rowgroup.h"
#include "utils/common/vlarray.h"

namespace sorting
{
using HeapPermutations = std::vector<PermutationVec>;
using ColumnsValues = std::vector<utils::ConstString>;
struct KeyType
{
  KeyType(rowgroup::RowGroup& rg, uint8_t* keyBuf)
  {
    key_ = keyBuf;
  };
  KeyType(rowgroup::RowGroup& rg, const joblist::OrderByKeysType& colsAndDirection,
          const sorting::PermutationType p, uint8_t* buf);
  ~KeyType(){};

  const uint8_t* key() const
  {
    return key_;
  }
  uint8_t* key()
  {
    return key_;
  }
  // const ColumnsValues& values() const
  // {
  //   return values_;
  // }
  bool less(const KeyType& r, const rowgroup::RowGroup& rg,
            const joblist::OrderByKeysType& colsAndDirection) const;

 private:
  uint8_t* key_;
  // ColumnsValues values_;
};

class HeapOrderBy
{
 private:
  using RGDataOrRowIDType = uint32_t;
  using HeapUnit = std::pair<KeyType, PermutationType>;
  using Heap = std::vector<HeapUnit>;

 public:
  static constexpr PermutationType ImpossiblePermute = {0xFFFFFFFF, 0xFFFFFF, 0xFF};
  HeapOrderBy(const rowgroup::RowGroup& rg, const joblist::OrderByKeysType& sortingKeyCols,
              const size_t limitStart, const size_t limitCount, joblist::MemManager* mm,
              const uint32_t threadId, const sorting::SortingThreads& prevPhaseSorting,
              const uint32_t threadNum, const sorting::ValueRangesVector& ranges);
  ~HeapOrderBy()
  {
    mm_->release((heap_.size() + 1) * keyBytesSize_);
  };
  size_t getData(rowgroup::RGData& data, const SortingThreads& prevPhaseThreads);
  size_t idxOfMin(std::vector<HeapUnit>& heap, const size_t left, const size_t right,
                  const joblist::OrderByKeysType& colsAndDirection);
  PermutationType getTopPermuteFromHeap(std::vector<HeapUnit>& heap, const SortingThreads& prevPhaseThreads);
  uint64_t getLimitCount() const
  {
    return count_;
  }
  const std::string toString() const;
  //   void finalize();
  const Heap& heap() const
  {
    return heap_;
  }
  Heap& heapMut()
  {
    return heap_;
  }

 protected:
  uint64_t start_;
  uint64_t count_;
  size_t keyBytesSize_;
  size_t recordsLeftInRanges_;
  joblist::OrderByKeysType jobListorderByRGColumnIDs_;
  rowgroup::RowGroup rg_;
  std::unique_ptr<joblist::MemManager> mm_;
  std::unique_ptr<uint8_t> keyBuf_;
  Heap heap_;
  HeapPermutations ranges_;
  // Scratch desk
  rowgroup::RGData data_;
  // It is possible to use rg_ member only but there is a potential to shoot in the foot
  // setting different RGDatas in two points in the code of this class methods.
  rowgroup::RowGroup rgOut_;
  rowgroup::Row inRow_;
  rowgroup::Row outRow_;
};
}  // namespace sorting
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

#include "pdqorderby.h"
#include "resourcemanager.h"
#include "rowgroup.h"

namespace sorting
{
using HeapPermutations = std::vector<PermutationVec>;
class HeapOrderBy
{
 private:
  using RGDataOrRowIDType = uint32_t;
  using KeyType = int64_t;
  using HeapUnit = std::pair<KeyType, PermutationType>;
  using Heap = std::vector<HeapUnit>;
  static constexpr PermutationType impossiblePermute = {0xFFFFFFFF, 0xFFFFFF, 0xFF};

 public:
  HeapOrderBy(const rowgroup::RowGroup& rg, const size_t limitStart, const size_t limitCount,
              joblist::RMMemManager* mm, const uint32_t threadId,
              const sorting::SortingThreads& prevPhaseSorting, const uint32_t threadNum,
              const sorting::ValueRangesVector& ranges, const joblist::OrderByKeysType& sortingKeyCols);
  ~HeapOrderBy() = default;
  size_t getData(rowgroup::RGData& data, const SortingThreads& prevPhaseThreads);
  template <typename T>
  size_t idxOfMin(std::vector<HeapUnit>& heap, const size_t left, const size_t right);
  template <typename T>
  PermutationType getTopPermuteFromHeap(std::vector<HeapUnit>& heap, const SortingThreads& prevPhaseThreads);
  uint64_t getLimitCount() const
  {
    return count_;
  }
  const std::string toString() const;
  //   void finalize();

 protected:
  uint64_t start_;
  uint64_t count_;
  size_t recordsLeftInRanges_;
  joblist::OrderByKeysType jobListorderByRGColumnIDs_;
  rowgroup::RowGroup rg_;
  std::unique_ptr<joblist::RMMemManager> mm_;
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
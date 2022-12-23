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
  KeyType(rowgroup::RowGroup& rg)
  {
    key_ = new uint8_t(rg.getOffsets().back());
  };
  KeyType(rowgroup::RowGroup& rg, const joblist::OrderByKeysType& colsAndDirection,
          const sorting::PermutationType p);
  ~KeyType(){};

  const uint8_t* key() const
  {
    return key_;
  }
  const ColumnsValues& values() const
  {
    return values_;
  }
  bool less(const KeyType& r, const rowgroup::RowGroup& rg,
            const joblist::OrderByKeysType& colsAndDirection) const
  {
    assert(rg.getColumnCount() >= 1);
    auto& widths = rg.getColWidths();
    // auto& colTypes = rg.getColTypes();
    rowgroup::OffsetType l_offset = 0;
    rowgroup::OffsetType r_offset = widths.front() + 1;
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

      // while ((cmpResult = memcmp(key_ + l_offset, r.key_ + l_offset, r_offset - l_offset)) == 0 /*&&
      //      (unsizedCmpResult = CHARSET::cmp(unsizedKeys[i - 1], unsizedKeys[i])) == 0*/)

      cmpResult = memcmp(key_ + l_offset, r.key_ + l_offset, r_offset - l_offset);
      l_offset = r_offset;
    }
    //   if ()
    // cmpResult = memcmp(key_ + l_offset, r.key_ + l_offset, r_offset - l_offset)) == 0

    return cmpResult < 0;  //|| (cmpResult == 0 && unsizedCmpResult < 0);
  }

 private:
  // KeyType(const uint8_t* k, ColumnsValues& values)
  //  : key_(utils::VLArray<uint8_t, 16>(sizeof(int64_t))), values_(values){};
  // utils::VLArray<uint8_t, 16> key_;
  uint8_t* key_;
  ColumnsValues values_;
};

class HeapOrderBy
{
 private:
  using RGDataOrRowIDType = uint32_t;
  // using KeyType = int64_t;
  using HeapUnit = std::pair<KeyType, PermutationType>;
  using Heap = std::vector<HeapUnit>;
  static constexpr PermutationType impossiblePermute = {0xFFFFFFFF, 0xFFFFFF, 0xFF};

 public:
  HeapOrderBy(const rowgroup::RowGroup& rg, const joblist::OrderByKeysType& sortingKeyCols,
              const size_t limitStart, const size_t limitCount, joblist::RMMemManager* mm,
              const uint32_t threadId, const sorting::SortingThreads& prevPhaseSorting,
              const uint32_t threadNum, const sorting::ValueRangesVector& ranges);
  ~HeapOrderBy() = default;
  size_t getData(rowgroup::RGData& data, const SortingThreads& prevPhaseThreads);
  template <typename T>
  size_t idxOfMin(std::vector<HeapUnit>& heap, const size_t left, const size_t right,
                  const joblist::OrderByKeysType& colsAndDirection);
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
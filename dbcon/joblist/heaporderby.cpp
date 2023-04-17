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
#include "mcs_int128.h"
#include "numeric_functions.h"
#include "pdqorderby.h"
#include "vlarray.h"

namespace sorting
{
KeyType::KeyType(rowgroup::RowGroup& rg, const joblist::OrderByKeysType& colsAndDirection,
                 const sorting::PermutationType p, uint8_t* buf)
{
  key_ = buf;
  uint8_t* pos = key_;
  for (auto [columnId, isAsc] : colsAndDirection)
  {
    uint32_t columnWidth = rg.getColumnWidth(columnId);
    auto columnType = rg.getColType(columnId);
    switch (columnType)
    {
      case execplan::CalpontSystemCatalog::DECIMAL:
      case execplan::CalpontSystemCatalog::UDECIMAL:
      {
        switch (columnWidth)
        {
          case datatypes::MAXDECIMALWIDTH:
          {
            const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
            const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&datatypes::TSInt128::NullValue);
            bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
            *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
            memcpy(pos, valueBuf, 16);
            int64_t* wDecimaAsInt64 = reinterpret_cast<int64_t*>(pos);
            int64_t lower = 0;
            if (!isAsc)
            {
              lower = ~htonll(wDecimaAsInt64[0]);
              wDecimaAsInt64[0] = ~htonll(wDecimaAsInt64[1] ^ 0x8000000000000000);
            }
            else
            {
              lower = htonll(wDecimaAsInt64[0]);
              wDecimaAsInt64[0] = htonll(wDecimaAsInt64[1] ^ 0x8000000000000000);
            }
            wDecimaAsInt64[1] = lower;
            break;
          }
          case 8:
          {
            using StorageType =
                datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::BIGINT>::type;
            const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
            const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::BIGINTNULL);
            bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
            *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
            std::memcpy(pos, valueBuf, sizeof(StorageType));
            StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
            *valPtr ^= 0x8000000000000000;
            *valPtr = (!isAsc) ? ~htonll(*valPtr) : htonll(*valPtr);
            pos += columnWidth;
            break;
          }
          case 1:
          {
            using StorageType =
                datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::UTINYINT>::type;
            const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
            const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::TINYINTNULL);
            bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
            *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
            std::memcpy(pos, valueBuf, sizeof(StorageType));
            StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
            *valPtr ^= 0x80;
            *valPtr = (!isAsc) ? ~*valPtr : *valPtr;
            pos += columnWidth;
            break;
          }
          case 2:
          {
            using StorageType =
                datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::SMALLINT>::type;
            const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
            const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::SMALLINTNULL);
            bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
            *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
            std::memcpy(pos, valueBuf, sizeof(StorageType));
            StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
            *valPtr ^= 0x8000;
            *valPtr = (!isAsc) ? ~htons(*valPtr) : htons(*valPtr);
            pos += columnWidth;
            break;
          }
          case 4:
          {
            using StorageType =
                datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::INT>::type;
            const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
            const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::INTNULL);
            bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
            *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
            std::memcpy(pos, valueBuf, sizeof(StorageType));
            StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
            *valPtr ^= 0x80000000;
            *valPtr = (!isAsc) ? ~htonl(*valPtr) : htonl(*valPtr);
            pos += columnWidth;
            break;
          }
        }
        break;
      }
      case execplan::CalpontSystemCatalog::DOUBLE:
      {
        double v =
            rg.getColumnValue<execplan::CalpontSystemCatalog::DOUBLE, double, double>(columnId, p.rowID);
        const uint8_t* valueBuf = reinterpret_cast<const uint8_t*>(&v);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::DOUBLENULL);
        bool isNeitherNullOrSpecial =
            (memcmp(nullValuePtr, valueBuf, columnWidth) != 0) && !isnan(v) && !isinf(v);
        *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNeitherNullOrSpecial)
                          : static_cast<uint8_t>(isNeitherNullOrSpecial);

        int64_t doubleAsInt = 0;
        std::memcpy(&doubleAsInt, valueBuf, columnWidth);
        int64_t s = (doubleAsInt & 0x8000000000000000) ^ 0x8000000000000000;  // sign bit
        int64_t e = (doubleAsInt >> 52) & 0x07FF;                             // exponent part
        int64_t m =
            (s) ? doubleAsInt & 0x07FFFFFFFFFFFF : (~doubleAsInt) & 0x07FFFFFFFFFFFF;  // fraction part
        m = (e) ? m | 0x8000000000000
                : m << 1;  // set an additional 52th bit if exp != 0 or move the value left 1 bit
        m = (s) ? m : m & 0xFFF7FFFFFFFFFFFF;  // if negative takes the faction or all but extra bit instead
        e = ((s) ? e : ~e & 0x07FF) << 51;
        int64_t key = m;
        key ^= s;
        key |= e;
        key = (!isAsc) ? ~htonll(key) : htonll(key);
        std::memcpy(pos, &key, sizeof(double));
        pos += columnWidth;
        break;
      }
      case execplan::CalpontSystemCatalog::FLOAT:
      {
        float v = rg.getColumnValue<execplan::CalpontSystemCatalog::FLOAT, float, float>(columnId, p.rowID);
        const uint8_t* valueBuf = reinterpret_cast<const uint8_t*>(&v);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::FLOATNULL);
        bool isNeitherNullOrSpecial =
            (memcmp(nullValuePtr, valueBuf, columnWidth) != 0) && !isnan(v) && !isinf(v);
        *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNeitherNullOrSpecial)
                          : static_cast<uint8_t>(isNeitherNullOrSpecial);

        int floatAsInt = 0;
        std::memcpy(&floatAsInt, valueBuf, sizeof(float));
        int32_t s = (floatAsInt & 0x80000000) ^ 0x80000000;
        int32_t e = (floatAsInt >> 23) & 0xFF;
        int m = (s) ? floatAsInt & 0x7FFFFF : (~(floatAsInt)) & 0x7FFFFF;
        m = (e) ? m | 0x800000 : m << 1;
        m = (s) ? m : m & 0xFF7FFFFF;
        e = ((s) ? e : ~e & 0xFF) << 23;
        int32_t key = m;
        key ^= s;
        key |= e;
        key = (!isAsc) ? ~htonl(key) : htonl(key);
        std::memcpy(pos, &key, sizeof(float));
        pos += columnWidth;
        break;
      }
      case execplan::CalpontSystemCatalog::TINYINT:
      {
        using StorageType =
            datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::UTINYINT>::type;
        const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::TINYINTNULL);
        bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
        *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
        std::memcpy(pos, valueBuf, sizeof(StorageType));
        StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
        *valPtr ^= 0x80;
        *valPtr = (!isAsc) ? ~*valPtr : *valPtr;
        pos += columnWidth;
        break;
      }
      case execplan::CalpontSystemCatalog::SMALLINT:
      {
        using StorageType =
            datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::SMALLINT>::type;
        const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::SMALLINTNULL);
        bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
        *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
        std::memcpy(pos, valueBuf, sizeof(StorageType));
        StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
        *valPtr ^= 0x8000;
        *valPtr = (!isAsc) ? ~htons(*valPtr) : htons(*valPtr);
        pos += columnWidth;
        break;
      }
      case execplan::CalpontSystemCatalog::INT:
      {
        using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::INT>::type;
        const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::INTNULL);
        bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
        *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
        std::memcpy(pos, valueBuf, sizeof(StorageType));
        StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
        *valPtr ^= 0x80000000;
        *valPtr = (!isAsc) ? ~htonl(*valPtr) : htonl(*valPtr);
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
        *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
        std::memcpy(pos, valueBuf, sizeof(StorageType));
        StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
        *valPtr ^= 0x8000000000000000;
        *valPtr = (!isAsc) ? ~htonll(*valPtr) : htonll(*valPtr);
        pos += columnWidth;
        break;
      }
      case execplan::CalpontSystemCatalog::UTINYINT:
      {
        using StorageType =
            datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::UTINYINT>::type;
        const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::UTINYINTNULL);
        bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
        *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
        std::memcpy(pos, valueBuf, sizeof(StorageType));
        StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
        *valPtr ^= 0x80;
        *valPtr = (!isAsc) ? ~*valPtr : *valPtr;
        pos += columnWidth;
        break;
      }

      case execplan::CalpontSystemCatalog::USMALLINT:
      {
        using StorageType =
            datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::USMALLINT>::type;
        const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::USMALLINTNULL);
        bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
        *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
        std::memcpy(pos, valueBuf, sizeof(StorageType));
        StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
        *valPtr ^= 0x8000;
        *valPtr = (!isAsc) ? ~htons(*valPtr) : htons(*valPtr);
        pos += columnWidth;
        break;
      }
      case execplan::CalpontSystemCatalog::UINT:
      {
        using StorageType = datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::UINT>::type;
        const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::UINTNULL);
        bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
        *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
        std::memcpy(pos, valueBuf, sizeof(StorageType));
        StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
        *valPtr ^= 0x80000000;
        *valPtr = (!isAsc) ? ~htonl(*valPtr) : htonl(*valPtr);
        pos += columnWidth;
        break;
      }
      case execplan::CalpontSystemCatalog::UBIGINT:
      {
        using StorageType =
            datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::UBIGINT>::type;
        const uint8_t* valueBuf = rg.getColumnValueBuf(columnId, p.rowID);
        const uint8_t* nullValuePtr = reinterpret_cast<const uint8_t*>(&joblist::UBIGINTNULL);
        bool isNotNull = memcmp(nullValuePtr, valueBuf, columnWidth) != 0;
        *pos++ = (!isAsc) ? static_cast<uint8_t>(!isNotNull) : static_cast<uint8_t>(isNotNull);
        std::memcpy(pos, valueBuf, sizeof(StorageType));
        StorageType* valPtr = reinterpret_cast<StorageType*>(pos);
        *valPtr ^= 0x8000000000000000;
        *valPtr = (!isAsc) ? ~htonll(*valPtr) : htonll(*valPtr);
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
          *pos++ = (!isAsc) ? 0 : 1;
          keyWidth = rg.getStringTableThreshold();
        }
        else if (sorting::isDictColumn(columnType, columnWidth) &&
                 (columnWidth <= rg.getStringTableThreshold() || forceInline))
        {
          using StorageType = utils::ShortConstString;
          // No difference b/w VARCHAR, CHAR and TEXT types yet
          value = rg.getColumnValue<execplan::CalpontSystemCatalog::VARCHAR, StorageType, EncodedKeyType>(
              columnId, p.rowID);
          *pos++ = (!isAsc) ? 0 : 1;
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
              *pos++ = (!isAsc) ? 0 : 1;
              break;
            }

            case 2:
            {
              using StorageType =
                  datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::SMALLINT>::type;
              value = rg.getColumnValue<execplan::CalpontSystemCatalog::VARCHAR, StorageType, EncodedKeyType>(
                            columnId, p.rowID)
                          .rtrimZero();
              *pos++ = (!isAsc) ? 0 : 1;
              break;
            };

            case 4:
            {
              using StorageType =
                  datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::INT>::type;
              value = rg.getColumnValue<execplan::CalpontSystemCatalog::VARCHAR, StorageType, EncodedKeyType>(
                            columnId, p.rowID)
                          .rtrimZero();
              *pos++ = (!isAsc) ? 0 : 1;
              break;
            };

            case 8:
            {
              using StorageType =
                  datatypes::ColDataTypeToIntegralType<execplan::CalpontSystemCatalog::BIGINT>::type;
              value = rg.getColumnValue<execplan::CalpontSystemCatalog::VARCHAR, StorageType, EncodedKeyType>(
                            columnId, p.rowID)
                          .rtrimZero();
              *pos++ = (!isAsc) ? 0 : 1;
              break;
            };
            default: idbassert(0);
          }
        }
        std::memset(pos, 0, keyWidth);
        [[maybe_unused]] size_t nActualWeights = cs.strnxfrm(
            pos, keyWidth, keyWidth, reinterpret_cast<const uchar*>(value.str()), value.length(), flags);
        uint8_t* end = pos + nActualWeights;
        if (!isAsc)
        {
          for (; pos < end; ++pos)
          {
            *pos = ~*pos;
          }
        }
        else
        {
          pos = end;
        }
      }
      default: break;
    }
  }
}

bool KeyType::less(const KeyType& r, rowgroup::RowGroup& rg, const joblist::OrderByKeysType& colsAndDirection,
                   const PermutationType leftP, const PermutationType rightP,
                   const sorting::SortingThreads& prevPhaseSorting) const
{
  assert(rg.getColumnCount() >= 1);
  auto& widths = rg.getColWidths();
  rowgroup::OffsetType l_offset = 0;
  rowgroup::OffsetType r_offset = 0;
  size_t colsNumber = colsAndDirection.size();
  [[maybe_unused]] int32_t unsizedCmpResult = 0;
  int32_t cmpResult = 0;
  const uint32_t stringThreshold = rg.getStringTableThreshold();
  bool longString = false;
  for (size_t i = 0; !cmpResult && i <= colsNumber; ++i)
  {
    if (i < colsNumber)
    {
      auto [columnId, isAscLocal] = colsAndDirection[i];
      ++r_offset;  // NULL byte
      if (widths[columnId] <= stringThreshold)
      {
        r_offset += widths[columnId];
        continue;
      }
      r_offset += stringThreshold;
      longString = true;
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
      cmpResult = (isAsc) ? cmpResult : -cmpResult;
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
                         const uint32_t threadId, const sorting::SortingThreads& prevPhaseSortingThreads,
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

  size_t nextPowOf2 = common::nextPowOf2(threadNum);
  size_t heapSize = nextPowOf2 << 1;
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
  for (auto range = ranges.begin(); auto& prevPhaseSortingThread : prevPhaseSortingThreads)
  {
    auto [rangeLeft, rangeRight] = *range;
    ++range;
    auto srcPermBegin = prevPhaseSortingThread->getPermutation().begin() + rangeLeft;
    auto srcPermEnd = prevPhaseSortingThread->getPermutation().begin() + rangeRight;
    auto rangeSize = rangeRight - rangeLeft + ((rangeLeft != rangeRight) ? 1 : 0);
    recordsLeftInRanges_ += rangeSize;
    ranges_.push_back({ImpossiblePermute});
    ranges_.back().reserve(rangeSize);
    if (rangeSize)
    {
      ranges_.back().insert(ranges_.back().end(), srcPermBegin, srcPermEnd);
    }
  }

  assert(heapSize < 256);  // WIP
  if (heapSize > prevPhaseSortingThreads.size())
  {
    for (size_t i = 0; i < nextPowOf2 - prevPhaseSortingThreads.size(); ++i)
    {
      ranges_.push_back({ImpossiblePermute});
    }
  }

  // Build a heap
  auto heapSizeHalf = heapSize >> 1;
  for (size_t i = 0; i < heapSize; ++i)
  {
    heap_.push_back({KeyType(rg_, keyBuf_.get() + (i * keyBytesSize_)), {0, 0, i % heapSizeHalf}});
  }

  size_t prevHeapIdx = heap_.size();
  for (size_t heapIdx = nextPowOf2; heapIdx >= 1; heapIdx >>= 1, prevHeapIdx >>= 1)  // level
  {
    for (size_t i = heapIdx; i < prevHeapIdx; ++i)  // all level items
    {
      size_t bestChildIdx = i;
      size_t prevBestChildIdx = bestChildIdx;
      for (; bestChildIdx < heapSizeHalf;)
      {
        prevBestChildIdx = bestChildIdx;
        bestChildIdx = findMinAndSwap(heap_, prevPhaseSortingThreads, prevBestChildIdx);
      }
      if (heap_[prevBestChildIdx].second == ImpossiblePermute)  // edge case to handle empty Sources
      {
        auto buf = heap_[bestChildIdx].first.key();
        heap_[bestChildIdx] = {KeyType(buf), ImpossiblePermute};
        continue;
      }
      auto threadId = heap_[prevBestChildIdx].second.threadID;
      assert(threadId < ranges_.size() && !ranges_[threadId].empty());
      auto p = ranges_[threadId].back();
      auto buf = heap_[bestChildIdx].first.key();
      if (p == ImpossiblePermute)  // edge case to handle empty Sources
      {
        heap_[bestChildIdx] = {KeyType(buf), p};
        continue;
      }
      rg_.setData(&(prevPhaseSortingThreads[p.threadID]->getRGDatas()[p.rgdataID]));
      auto key = KeyType(rg_, sortingKeyCols, p, buf);
      heap_[bestChildIdx] = {key, p};
      ranges_[threadId].pop_back();
    }
  }
  recordsLeftInRanges_ -= heap_.size() - 1;
}

size_t HeapOrderBy::idxOfMin(Heap& heap, const size_t left, const size_t right,
                             const joblist::OrderByKeysType& colsAndDirection, const PermutationType leftP,
                             const PermutationType rightP, const sorting::SortingThreads& prevPhaseSorting)
{
  if (leftP == ImpossiblePermute)
  {
    return right;
  }
  if (rightP == ImpossiblePermute ||
      heap[left].first.less(heap[right].first, rg_, colsAndDirection, leftP, rightP, prevPhaseSorting))
    return left;
  return right;
}

size_t HeapOrderBy::findMinAndSwap(Heap& heap, const SortingThreads& prevPhaseSortingThreads,
                                   const size_t heapIdx)
{
  size_t leftHeapIdx = heapIdx << 1;
  size_t rightHeapIdx = (heapIdx << 1) + 1;
  auto leftP = heap[leftHeapIdx].second;
  auto rightP = heap[rightHeapIdx].second;
  size_t bestChildIdx = idxOfMin(heap, leftHeapIdx, rightHeapIdx, jobListorderByRGColumnIDs_, leftP, rightP,
                                 prevPhaseSortingThreads);
  std::swap(heap[heapIdx], heap[bestChildIdx]);
  return bestChildIdx;
}

PermutationType HeapOrderBy::getTopPermuteFromHeap(std::vector<HeapUnit>& heap,
                                                   const SortingThreads& prevPhaseSortingThreads)
{
  auto half = heap.size() >> 1;
  PermutationType topPermute = heap[1].second;
  // Use the buffer of the top.
  assert(heap_.size() >= 2);
  auto buf = heap_[1].first.key();
  size_t bestChildIdx = 1;
  size_t heapIdx = 1;
  // WIP remove IP comparison
  for (; heapIdx < half && heap[bestChildIdx].second != ImpossiblePermute; heapIdx = bestChildIdx)
  {
    // get min of left and right.
    bestChildIdx = findMinAndSwap(heap, prevPhaseSortingThreads, heapIdx);
  }

  heapIdx = (heapIdx == 1) ? 1 : heapIdx >> 1;
  if (heap[heapIdx >> 1].second == ImpossiblePermute || heap[heapIdx].second == ImpossiblePermute)
  {
    return topPermute;
  }
  // Use the previously swapped threadId and key buffer;
  auto threadId = heap[heapIdx].second.threadID;
  auto p = ranges_[threadId].back();
  // std::cout << "getTopPermuteFromHeap heapIdx " << heapIdx << " threadId " << threadId << " p " <<
  // p.rgdataID
  //           << " ranges_[threadId].size " << ranges_[threadId].size() << std::endl;
  if (p == ImpossiblePermute || ranges_[threadId].empty())
  {
    *buf = 2;  // impossible key WIP
    heap_[bestChildIdx] = {KeyType(rg_, buf), ImpossiblePermute};
  }
  else
  {
    assert(p.threadID < prevPhaseSortingThreads.size() &&
           p.rgdataID < prevPhaseSortingThreads[p.threadID]->getRGDatas().size());
    rg_.setData(&(prevPhaseSortingThreads[p.threadID]->getRGDatas()[p.rgdataID]));
    heap_[bestChildIdx] = {KeyType(rg_, jobListorderByRGColumnIDs_, p, buf), p};
  }
  ranges_[threadId].pop_back();

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

const string HeapOrderBy::heapToString(const SortingThreads& prevPhaseThreads)
{
  ostringstream oss;
  oss << std::endl;
  for (size_t i = 1; i < heap_.size(); ++i)
  {
    if (!(i & (i - 1)))
    {
      oss << std::endl;
    }
    auto p = heap_[i].second;
    if (p != ImpossiblePermute)
    {
      rg_.setData(&(prevPhaseThreads[p.threadID]->getRGDatas()[p.rgdataID]));
      int64_t v = rg_.getColumnValue<execplan::CalpontSystemCatalog::BIGINT, int64_t, int64_t>(0, p.rowID);
      oss << "v [" << v << "] ";
    }

    oss << " (" << p.threadID << "," << p.rgdataID << "," << p.rowID << ") ";
  }
  oss << endl;

  return oss.str();
}

}  // namespace sorting
/* Copyright (C) 2021 MariaDB Corporation

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

#ifndef ROWSTORAGE_H
#define ROWSTORAGE_H

#include "resourcemanager.h"
#include "rowgroup.h"
#include "idbcompress.h"
#include <sys/stat.h>
#include <unistd.h>

namespace rowgroup
{
uint32_t calcNumberOfBuckets(ssize_t availMem, uint32_t numOfThreads, uint32_t numOfBuckets,
                             uint32_t groupsPerThread, uint32_t inRowSize, uint32_t outRowSize,
                             bool enabledDiskAggr);

class MemManager;
class RowPosHashStorage;
using RowPosHashStoragePtr = std::unique_ptr<RowPosHashStorage>;
class RowGroupStorage;

uint64_t hashRow(const rowgroup::Row& r, std::size_t lastCol);

class RowAggStorage
{
 public:
  RowAggStorage(const std::string& tmpDir, RowGroup* rowGroupOut, RowGroup* keysRowGroup, uint32_t keyCount,
                joblist::ResourceManager* rm = nullptr, boost::shared_ptr<int64_t> sessLimit = {},
                bool enabledDiskAgg = false, bool allowGenerations = false,
                compress::CompressInterface* compressor = nullptr);

  RowAggStorage(const std::string& tmpDir, RowGroup* rowGroupOut, uint32_t keyCount,
                joblist::ResourceManager* rm = nullptr, boost::shared_ptr<int64_t> sessLimit = {},
                bool enabledDiskAgg = false, bool allowGenerations = false,
                compress::CompressInterface* compressor = nullptr)
   : RowAggStorage(tmpDir, rowGroupOut, rowGroupOut, keyCount, rm, std::move(sessLimit), enabledDiskAgg,
                   allowGenerations, compressor)
  {
  }

  ~RowAggStorage();

  static uint16_t getMaxRows(bool enabledDiskAgg)
  {
    return (enabledDiskAgg ? 8192 : 256);
  }

  static size_t getBucketSize();

  /** @brief Find or create resulting row.
   *
   *    Create "aggregation key" row if necessary.
   *    NB! Using getTargetRow() after append() is UB!
   *
   *  @param row(in)  input row
   *  @param rowOut() row to aggregate data from input row
   *
   *  @returns true if new row created, false otherwise
   */
  bool getTargetRow(const Row& row, Row& rowOut);
  bool getTargetRow(const Row& row, uint64_t row_hash, Row& rowOut);

  /** @brief Dump some RGDatas to disk and release memory for further use.
   */
  void dump();

  /** @brief Append RGData from other RowAggStorage and clear it.
   *
   *    NB! Any operation except getNextRGData() or append() is UB!
   *
   * @param other(in) donor storage
   */
  void append(RowAggStorage& other);

  /** @brief Remove last RGData from internal RGData storage and return it.
   *
   * @returns pointer to the next RGData or nullptr if empty
   */
  std::unique_ptr<RGData> getNextRGData();

  /** @brief TODO
   *
   * @param mergeFunc
   * @param rowOut
   */
  void finalize(std::function<void(Row&)> mergeFunc, Row& rowOut);

  /** @brief Calculate maximum size of hash assuming 80% fullness.
   *
   * @param elems(in) number of elements
   * @returns calculated size
   */
  inline static size_t calcMaxSize(size_t elems) noexcept
  {
    if (LIKELY(elems <= std::numeric_limits<size_t>::max() / 100))
      return elems * 80 / 100;

    return (elems / 100) * 80;
  }

  inline static size_t calcSizeWithBuffer(size_t elems, size_t maxSize) noexcept
  {
    return elems + std::min(maxSize, 0xFFUL);
  }

  inline static size_t calcSizeWithBuffer(size_t elems) noexcept
  {
    return calcSizeWithBuffer(elems, calcMaxSize(elems));
  }

 private:
  struct Data;
  /** @brief Create new RowAggStorage with the same params and load dumped data
   *
   * @param gen(in) generation number
   * @return pointer to a new RowAggStorage
   */
  RowAggStorage* clone(uint16_t gen) const;

  /** @brief Free any internal data
   */
  void freeData();

  /** @brief Move internal data & row position inside [insIdx, startIdx] up by 1.
   *
   * @param startIdx(in) last element's index to move
   * @param insIdx(in)   first element's index to move
   */
  void shiftUp(size_t startIdx, size_t insIdx);

  /** @brief Find best position of row and save it's hash.
   *
   * @param row(in)   input row
   * @param info(out) info data
   * @param idx(out)  index computed from row hash
   * @param hash(out) row hash value
   */
  void rowToIdx(const Row& row, uint32_t& info, size_t& idx, uint64_t& hash) const;
  void rowToIdx(const Row& row, uint32_t& info, size_t& idx, uint64_t& hash, const Data* curData) const;

  /** @brief Find best position using precomputed hash
   *
   * @param h(in)     row hash
   * @param info(out) info data
   * @param idx(out)  index
   */
  inline void rowHashToIdx(uint64_t h, uint32_t& info, size_t& idx, const Data* curData) const
  {
    info = curData->fInfoInc + static_cast<uint32_t>((h & INFO_MASK) >> curData->fInfoHashShift);
    idx = (h >> INIT_INFO_BITS) & curData->fMask;
  }

  inline void rowHashToIdx(uint64_t h, uint32_t& info, size_t& idx) const
  {
    return rowHashToIdx(h, info, idx, fCurData);
  }

  /** @brief Iterate over internal info until info with less-or-equal distance
   *         from the best position was found.
   *
   * @param info(in,out) info data
   * @param idx(in,out)  index
   */
  inline void nextWhileLess(uint32_t& info, size_t& idx, const Data* curData) const noexcept
  {
    while (info < curData->fInfo[idx])
    {
      next(info, idx, curData);
    }
  }

  inline void nextWhileLess(uint32_t& info, size_t& idx) const noexcept
  {
    return nextWhileLess(info, idx, fCurData);
  }

  /** @brief Get next index and corresponding info
   */
  inline void next(uint32_t& info, size_t& idx, const Data* curData) const noexcept
  {
    ++(idx);
    info += curData->fInfoInc;
  }

  inline void next(uint32_t& info, size_t& idx) const noexcept
  {
    return next(info, idx, fCurData);
  }

  /** @brief Get index and info of the next non-empty entry
   */
  inline void nextExisting(uint32_t& info, size_t& idx) const noexcept
  {
    uint64_t n = 0;
    uint64_t data;
    while (true)
    {
      memcpy(&data, fCurData->fInfo.get() + idx, sizeof(data));
      if (data == 0)
      {
        idx += sizeof(n);
      }
      else
      {
        break;
      }
    }

#if BYTE_ORDER == BIG_ENDIAN
    n = __builtin_clzll(data) / sizeof(data);
#else
    n = __builtin_ctzll(data) / sizeof(data);
#endif
    idx += n;
    info = fCurData->fInfo[idx];
  }

  /** @brief Increase internal data size if needed
   */
  void increaseSize();

  /** @brief Increase distance capacity of info removing 1 bit of the hash.
   *
   * @returns success
   */
  bool tryIncreaseInfo();

  /** @brief Reserve space for number of elements (power of two)
   *
   *    This function performs re-insert all data
   *
   * @param elems(in)   new size
   */
  void rehashPowerOfTwo(size_t elems);

  /** @brief Move elements from old one into rehashed data.
   *
   *    It's mostly the same algo as in getTargetRow(), but returns nothing
   *    and skips some checks because it's guaranteed that there is no dups.
   *
   * @param oldIdx(in)    index of "old" data
   * @param oldHashes(in) old storage of row positions and hashes
   */
  void insertSwap(size_t oldIdx, RowPosHashStorage* oldHashes);

  /** @brief (Re)Initialize internal data of specified size.
   *
   * @param elems(in) number of elements
   */
  void initData(size_t elems, const RowPosHashStorage* oldHashes);

  /** @brief Calculate memory size of info data
   *
   * @param elems(in) number of elements
   * @returns size in bytes
   */
  inline static size_t calcBytes(size_t elems) noexcept
  {
    return elems + sizeof(uint64_t);
  }

  /** @brief Reserve place sufficient for elems
   *
   * @param elems(in) number of elements
   */
  void reserve(size_t elems);

  /** @brief Start new aggregation generation
   *
   * Dump all the data on disk, including internal info data, positions & row
   * hashes, and the rowgroups itself.
   */
  void startNewGeneration();

  /** @brief Save internal info data on disk */
  void dumpInternalData() const;

  /** @brief Load previously dumped data from disk
   *
   * @param gen(in) generation number
   */
  void loadGeneration(uint16_t gen);
  /** @brief Load previously dumped data into the tmp storage */
  void loadGeneration(uint16_t gen, size_t& size, size_t& mask, size_t& maxSize, uint32_t& infoInc,
                      uint32_t& infoHashShift, std::unique_ptr<uint8_t[]>& info);

  /** @brief Remove temporary data files */
  void cleanup();
  void cleanup(uint16_t gen);

  /** @brief Remove all temporary data files */
  void cleanupAll() noexcept;

  std::string makeDumpFilename(int32_t gen = -1) const;

 private:
  static constexpr size_t INIT_SIZE{sizeof(uint64_t)};
  static constexpr uint32_t INIT_INFO_BITS{5};
  static constexpr uint8_t INIT_INFO_INC{1U << INIT_INFO_BITS};
  static constexpr size_t INFO_MASK{INIT_INFO_INC - 1U};
  static constexpr uint8_t INIT_INFO_HASH_SHIFT{0};
  static constexpr uint16_t MAX_INMEMORY_GENS{4};

  struct Data
  {
    RowPosHashStoragePtr fHashes;
    std::unique_ptr<uint8_t[]> fInfo;
    size_t fSize{0};
    size_t fMask{0};
    size_t fMaxSize{0};
    uint32_t fInfoInc{INIT_INFO_INC};
    uint32_t fInfoHashShift{INIT_INFO_HASH_SHIFT};
  };
  std::vector<std::unique_ptr<Data>> fGens;
  Data* fCurData;
  uint32_t fMaxRows;
  const bool fExtKeys;

  std::unique_ptr<RowGroupStorage> fStorage;
  std::unique_ptr<RowGroupStorage> fRealKeysStorage;
  RowGroupStorage* fKeysStorage;
  uint32_t fLastKeyCol;

  uint16_t fGeneration{0};
  void* fUniqId;

  Row fKeyRow;

  std::unique_ptr<MemManager> fMM;
  uint32_t fNumOfInputRGPerThread;
  bool fAggregated = true;
  bool fAllowGenerations;
  bool fEnabledDiskAggregation;
  std::unique_ptr<compress::CompressInterface> fCompressor;
  std::string fTmpDir;
  bool fInitialized{false};
  rowgroup::RowGroup* fRowGroupOut;
  rowgroup::RowGroup* fKeysRowGroup;
};

}  // namespace rowgroup

#endif  // MYSQL_ROWSTORAGE_H

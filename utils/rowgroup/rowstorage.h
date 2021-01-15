//
// Created by kemm on 09.02.2021.
//

#ifndef ROWSTORAGE_H
#define ROWSTORAGE_H

#include "rowgroup.h"
#include <sys/stat.h>
#include <unistd.h>

namespace rowgroup
{

class MemManager;
class RowPosHashStorage;
class RowGroupStorage;

class RowAggStorage
{
public:
  RowAggStorage(RowGroup* rowGroupOut,
                RowGroup* keysRowGroup,
                uint32_t keyCount,
                size_t maxRows = 256,
                joblist::ResourceManager* rm = nullptr,
                boost::shared_ptr<int64_t> sessLimit = {},
                bool enabledDiskAgg = false);

  RowAggStorage(RowGroup* rowGroupOut,
                uint32_t keyCount,
                size_t maxRows = 256,
                joblist::ResourceManager* rm = nullptr,
                boost::shared_ptr<int64_t> sessLimit = {},
                bool enabledDiskAgg = false)
      : RowAggStorage(rowGroupOut, rowGroupOut, keyCount, maxRows, rm, std::move(sessLimit), enabledDiskAgg)
  {}

  ~RowAggStorage();

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

private:
  /** @brief Free any internal data
   */
  void freeData();

  /** @brief Move internal data & row position inside [insIdx, startIdx] up by 1.
   *
   * @param startIdx(in) last element's index to move
   * @param insIdx(in)   first element's index to move
   */
  void shiftUp(size_t startIdx, size_t insIdx);

  inline static size_t mixHash(size_t x) noexcept
  {
    size_t h1 = x * 0xA24BAED4963EE407ULL;
    size_t h2 = ((x >> 32U) | (x << (8U * sizeof(x) - 32U))) * 0x9FB21C651E98DF25ULL;
    size_t h3 = h1 + h2;
    size_t h  = (h3 >> 32U) | (h3 << (8U * sizeof(x) - 32U));
    return h;
  }

  /** @brief Find best position of row and save it's hash.
   *
   * @param row(in)   input row
   * @param info(out) info data
   * @param idx(out)  index computed from row hash
   * @param hash(out) row hash value
   */
  void rowToIdx(const Row& row, uint32_t& info, size_t& idx, uint64_t& hash) const;

  /** @brief Find best position using precomputed hash
   *
   * @param h(in)     row hash
   * @param info(out) info data
   * @param idx(out)  index
   */
  inline void rowHashToIdx(uint64_t h, uint32_t& info, size_t& idx) const
  {
    info = fInfoInc + static_cast<uint32_t>((h & INFO_MASK) >> fInfoHashShift);
    idx = (h >> INIT_INFO_BITS) & fMask;
  }

  /** @brief Iterate over internal info until info with less-or-equal distance
   *         from the best position was found.
   *
   * @param info(in,out) info data
   * @param idx(in,out)  index
   */
  inline void nextWhileLess(uint32_t& info, size_t& idx) const noexcept
  {
    while (info < fInfo[idx])
    {
      next(info, idx);
    }
  }

  /** @brief Get next index and corresponding info
   */
  inline void next(uint32_t& info, size_t& idx) const noexcept
  {
    ++(idx);
    info += fInfoInc;
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
  void insertSwap(size_t oldIdx, RowPosHashStorage * oldHashes);

  /** @brief (Re)Initialize internal data of specified size.
   *
   * @param elems(in) number of elements
   */
  void initData(size_t elems);

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

private:
  static constexpr size_t   INIT_SIZE{sizeof(uint64_t)};
  static constexpr uint32_t INIT_INFO_BITS{5};
  static constexpr uint8_t  INIT_INFO_INC{1U << INIT_INFO_BITS};
  static constexpr size_t   INFO_MASK{INIT_INFO_INC - 1U};
  static constexpr uint8_t  INIT_INFO_HASH_SHIFT{0};

  RowPosHashStorage* fHashes{nullptr};
  uint8_t* fInfo{nullptr};
  size_t fSize{0};
  size_t fMask{0};
  size_t fMaxSize{0};
  uint32_t fInfoInc{INIT_INFO_INC};
  uint32_t fInfoHashShift{INIT_INFO_HASH_SHIFT};
  uint32_t fMaxRows;
  const bool fExtKeys;

  std::unique_ptr<RowGroupStorage> fStorage;
  RowGroupStorage* fKeysStorage;
  uint32_t fLastKeyCol;

  Row fKeyRow;

  std::unique_ptr<MemManager> fMM;
  int64_t fInitialMemLimit{0};
  uint32_t fNumOfOuterBuckets{1};
};

} // namespace rowgroup

#endif // MYSQL_ROWSTORAGE_H

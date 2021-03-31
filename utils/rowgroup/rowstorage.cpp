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

#include <unistd.h>
#include <sys/stat.h>
#include "rowgroup.h"
#include <resourcemanager.h>
#include <fcntl.h>
#include "rowstorage.h"
#include "robin_hood.h"

namespace
{

bool writeData(int fd, const char* buf, size_t sz)
{
  if (sz == 0)
    return true;

  auto to_write = sz;
  while (to_write > 0)
  {
    auto r = write(fd, buf + sz - to_write, to_write);
    if (UNLIKELY(r < 0))
    {
      if (errno == EAGAIN)
        continue;

      return false;
    }
    assert(size_t(r) <= to_write);
    to_write -= r;
  }

  return true;
}

bool readData(int fd, char* buf, size_t sz)
{
  if (sz == 0)
    return true;

  auto to_read = sz;
  while (to_read > 0)
  {
    auto r = read(fd, buf + sz - to_read, to_read);
    if (UNLIKELY(r < 0))
    {
      if (errno == EAGAIN)
        continue;

      return false;
    }

    assert(size_t(r) <= to_read);
    to_read -= r;
  }

  return true;
}

inline uint64_t hashData(const void* ptr, uint32_t len, uint64_t x = 0ULL)
{
  static constexpr uint64_t m = 0xc6a4a7935bd1e995ULL;
  static constexpr uint64_t seed = 0xe17a1465ULL;
  static constexpr unsigned int r = 47;

  auto const* const data64 = static_cast<uint64_t const*>(ptr);
  uint64_t h = seed ^ (len * m);

  std::size_t const n_blocks = len / 8;
  if (x)
  {
    x *= m;
    x ^= x >> r;
    x *= m;
    h ^= x;
    h *= m;
  }
  for (std::size_t i = 0; i < n_blocks; ++i) {
    uint64_t k;
    memcpy(&k, data64 + i, sizeof(k));

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  auto const* const data8 = reinterpret_cast<uint8_t const*>(data64 + n_blocks);
  switch (len & 7U) {
  case 7:
    h ^= static_cast<uint64_t>(data8[6]) << 48U;
    // FALLTHROUGH
  case 6:
    h ^= static_cast<uint64_t>(data8[5]) << 40U;
    // FALLTHROUGH
  case 5:
    h ^= static_cast<uint64_t>(data8[4]) << 32U;
    // FALLTHROUGH
  case 4:
    h ^= static_cast<uint64_t>(data8[3]) << 24U;
    // FALLTHROUGH
  case 3:
    h ^= static_cast<uint64_t>(data8[2]) << 16U;
    // FALLTHROUGH
  case 2:
    h ^= static_cast<uint64_t>(data8[1]) << 8U;
    // FALLTHROUGH
  case 1:
    h ^= static_cast<uint64_t>(data8[0]);
    h *= m;
    // FALLTHROUGH
  default:
    break;
  }

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

inline uint64_t hashRow(const rowgroup::Row& r, std::size_t lastCol)
{
  uint64_t ret = 0;
  if (lastCol >= r.getColumnCount())
    return 0;

  datatypes::MariaDBHasher h;
  for (uint32_t i = 0; i <= lastCol; ++i)
  {
    switch (r.getColType(i))
    {
    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::BLOB:
    case execplan::CalpontSystemCatalog::TEXT:
      h.add(r.getCharset(i), r.getConstString(i));
      break;

    default:
      ret = hashData(r.getData() + r.getOffset(i), r.getColumnWidth(i), ret);
      break;
    }
  }

  if (UNLIKELY(h.wasUsed()))
  {
    uint64_t strhash = h.finalize();
    ret = hashData(&strhash, sizeof(strhash), ret);
  }

  return ret;
}

} // anonymous namespace

namespace rowgroup
{

/** @brief NoOP interface to LRU-cache used by RowGroupStorage & HashStorage
 */
struct LRUIface
{
  using List = std::list<uint64_t>;

  virtual ~LRUIface() = default;
  /** @brief Put an ID to cache or set it as last used */
  virtual void add(uint64_t) {}
  /** @brief Remove an ID from cache */
  virtual void remove(uint64_t) {}
  /** @brief Get iterator of the most recently used ID */
  virtual List::const_reverse_iterator begin() const { return List::const_reverse_iterator(); }
  /** @brief Get iterator after the latest ID */
  virtual List::const_reverse_iterator end() const { return List::const_reverse_iterator(); }
  /** @brief Get iterator of the latest ID */
  virtual List::const_iterator rbegin() const { return {}; }
  /** @brief Get iterator after the most recently used ID */
  virtual List::const_iterator rend() const { return {}; }

  virtual void clear() {}
  virtual std::size_t size() const { return 0; }
  virtual bool empty() const { return true; }
  virtual LRUIface* clone() const { return new LRUIface(); }
};

struct LRU : public LRUIface
{
  ~LRU() override {
    fMap.clear();
    fList.clear();
  }
  inline void add(uint64_t rgid) final {
    auto it = fMap.find(rgid);
    if (it != fMap.end()) {
      fList.erase(it->second);
    }
    fMap[rgid] = fList.insert(fList.end(), rgid);
  }

  inline void remove(uint64_t rgid) final {
    auto it = fMap.find(rgid);
    if (UNLIKELY(it != fMap.end())) {
      fList.erase(it->second);
      fMap.erase(it);
    }
  }

  inline List::const_reverse_iterator begin() const final { return fList.crbegin(); }
  inline List::const_reverse_iterator end() const final { return fList.crend(); }
  inline List::const_iterator rbegin() const final { return fList.cbegin(); }
  inline List::const_iterator rend() const final { return fList.cend(); }
  inline void clear() final {
    fMap.clear();
    fList.clear();
  }

  bool empty() const final { return fList.empty(); }

  LRUIface* clone() const final { return new LRU(); }

  robin_hood::unordered_flat_map<uint64_t, List::iterator> fMap;
  List fList;
};

/** @brief Some service wrapping around ResourceManager (or NoOP) */
class MemManager
{
public:
  MemManager() {}
  virtual ~MemManager() {
    release(fMemUsed);
  }

  bool acquire(std::size_t amount)
  {
    return acquireImpl(amount);
  }
  void release(ssize_t amount = 0)
  {
    // in some cases it tries to release more memory than acquired, ie create
    // new rowgroup, acquire maximum size (w/o strings), add some rows with
    // strings and finally release the actual size of RG with strings
    if (amount == 0 || amount > fMemUsed)
      amount = fMemUsed;
    releaseImpl(amount);
  }

  std::size_t getUsed() const { return fMemUsed; }
  virtual int64_t getFree() const
  {
    return std::numeric_limits<int64_t>::max();
  }

  virtual bool isStrict() const { return false; }

  virtual MemManager* clone() const { return new MemManager(); }

protected:
  virtual bool acquireImpl(std::size_t amount) { fMemUsed += amount; return true; }
  virtual void releaseImpl(std::size_t amount) { fMemUsed -= amount; }
  ssize_t fMemUsed = 0;
};

class RMMemManager : public MemManager
{
public:
  RMMemManager(joblist::ResourceManager* rm, boost::shared_ptr<int64_t> sl, bool wait = true, bool strict = true)
      : fRm(rm), fSessLimit(std::move(sl)), fWait(wait), fStrict(strict)
  {}

  ~RMMemManager() override
  {
    release(fMemUsed);
    fMemUsed = 0;
  }

  int64_t getFree() const final
  {
    return std::min(fRm->availableMemory(), *fSessLimit);
  }

  bool isStrict() const final { return fStrict; }

  MemManager* clone() const final
  {
    return new RMMemManager(fRm, fSessLimit, fWait, fStrict);
  }

protected:
  bool acquireImpl(size_t amount) final
  {
    MemManager::acquireImpl(amount);
    if (!fRm->getMemory(amount, fSessLimit, fWait) && fStrict)
    {
      return false;
    }

    return true;
  }
  void releaseImpl(size_t amount) override {
    MemManager::releaseImpl(amount);
    fRm->returnMemory(amount, fSessLimit);
  }

private:
  joblist::ResourceManager* fRm = nullptr;
  boost::shared_ptr<int64_t> fSessLimit;
  const bool fWait;
  const bool fStrict;
};

/** @brief Storage for RGData with LRU-cache & memory management
 */
class RowGroupStorage
{
public:
  using RGDataStorage = std::vector<std::unique_ptr<RGData>>;

public:
  /** @brief Default constructor
   *
   * @param tmpDir(in)          directory for tmp data
   * @param rowGroupOut(in,out) RowGroup metadata
   * @param maxRows(in)         number of rows per rowgroup
   * @param rm                  ResourceManager to use or nullptr if we don't
   *                            need memory accounting
   * @param sessLimit           session memory limit
   * @param wait                shall we wait a bit if we haven't enough memory
   *                            right now?
   * @param strict              true  -> throw an exception if not enough memory
   *                            false -> deal with it later
   */
  RowGroupStorage(const std::string& tmpDir,
                  RowGroup* rowGroupOut,
                  size_t maxRows,
                  joblist::ResourceManager* rm = nullptr,
                  boost::shared_ptr<int64_t> sessLimit = {},
                  bool wait = false,
                  bool strict = false)
      : fRowGroupOut(rowGroupOut)
      , fMaxRows(maxRows)
      , fRGDatas()
      , fUniqId(this)
      , fTmpDir(tmpDir)
  {
    if (rm)
    {
      fMM = std::unique_ptr<MemManager>(new RMMemManager(rm, sessLimit, wait, strict));
      if (!wait && !strict)
      {
        fLRU = std::unique_ptr<LRUIface>(new LRU());
      }
      else
      {
        fLRU = std::unique_ptr<LRUIface>(new LRUIface());
      }
    }
    else
    {
      fMM = std::unique_ptr<MemManager>(new MemManager());
      fLRU = std::unique_ptr<LRUIface>(new LRUIface());
    }
    auto* curRG = new RGData(*fRowGroupOut, fMaxRows);
    fRowGroupOut->setData(curRG);
    fRowGroupOut->resetRowGroup(0);
    fRGDatas.emplace_back(curRG);
    fMM->acquire(fRowGroupOut->getSizeWithStrings(fMaxRows));
  }

  ~RowGroupStorage() = default;

  /** @brief Take away RGDatas from another RowGroupStorage
   *
   *    If some of the RGDatas is not in the memory do not load them,
   *    just rename dump file to match new RowGroupStorage pattern
   *
   * @param o RowGroupStorage to take from
   */
  void append(std::unique_ptr<RowGroupStorage> o) { return append(o.get()); }
  void append(RowGroupStorage* o)
  {
    std::unique_ptr<RGData> rgd;
    std::string ofname;
    while (o->getNextRGData(rgd, ofname))
    {
      fRGDatas.push_back(std::move(rgd));
      uint64_t rgid = fRGDatas.size() - 1;
      if (fRGDatas[rgid])
      {
        fRowGroupOut->setData(fRGDatas[rgid].get());
        int64_t memSz = fRowGroupOut->getSizeWithStrings(fMaxRows);
        if (!fMM->acquire(memSz))
        {
          throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                       logging::ERR_AGGREGATION_TOO_BIG),
                                   logging::ERR_AGGREGATION_TOO_BIG);
        }

        fLRU->add(rgid);
      }
      else
      {
        auto r = rename(ofname.c_str(), makeRGFilename(rgid).c_str());
        if (UNLIKELY(r < 0))
        {
          throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                       logging::ERR_DISKAGG_FILEIO_ERROR),
                                   logging::ERR_DISKAGG_FILEIO_ERROR);
        }
      }
      rgd.reset();
      ofname.clear();
    }
  }

  /** @brief Returns next RGData, load it from disk if necessary.
   *
   * @returns pointer to the next RGData or empty pointer if there is nothing
   */
  std::unique_ptr<RGData> getNextRGData()
  {
    while (!fRGDatas.empty())
    {
      uint64_t rgid = fRGDatas.size() - 1;
      if (!fRGDatas[rgid])
        loadRG(rgid, fRGDatas[rgid], true);
      unlink(makeRGFilename(rgid).c_str());

      auto rgdata = std::move(fRGDatas[rgid]);
      fRGDatas.pop_back();

      fRowGroupOut->setData(rgdata.get());
      int64_t memSz = fRowGroupOut->getSizeWithStrings(fMaxRows);
      fMM->release(memSz);
      fLRU->remove(rgid);
      if (fRowGroupOut->getRowCount() == 0)
        continue;
      return rgdata;
    }

    return {};
  }

  void initRow(Row& row) const
  {
    fRowGroupOut->initRow(&row);
  }

  /** @brief Get the row at the specified position, loading corresponding RGData if needed.
   *
   * @param idx(in)  index (from 0) of the row
   * @param row(out) resulting row
   */
  void getRow(uint64_t idx, Row& row)
  {
    uint64_t rgid = idx / fMaxRows;
    uint64_t rid = idx % fMaxRows;
    if (UNLIKELY(!fRGDatas[rgid]))
    {
      loadRG(rgid);
    }
    fRGDatas[rgid]->getRow(rid, &row);
    fLRU->add(rgid);
  }

  /** @brief Return a row and an index at the first free position.
   *
   * @param idx(out) index of the row
   * @param row(out) the row itself
   */
  void putRow(uint64_t& idx, Row& row)
  {
    bool need_new = false;
    if (UNLIKELY(fRGDatas.empty()))
    {
      need_new = true;
    }
    else if (UNLIKELY(!fRGDatas[fCurRgid]))
    {
      need_new = true;
    }
    else
    {
      fRowGroupOut->setData(fRGDatas[fCurRgid].get());
      if (UNLIKELY(fRowGroupOut->getRowCount() >= fMaxRows))
        need_new = true;
    }

    if (UNLIKELY(need_new))
    {
      for (auto rgid : *fLRU)
      {
        if (LIKELY(static_cast<bool>(fRGDatas[rgid])))
        {
          fRowGroupOut->setData(fRGDatas[rgid].get());
          if (fRowGroupOut->getRowCount() < fMaxRows)
          {
            fCurRgid = rgid;
            need_new = false;
            break;
          }
        }
      }
    }

    if (UNLIKELY(need_new))
    {
      int64_t memSz = fRowGroupOut->getSizeWithStrings(fMaxRows);
      if (!fMM->acquire(memSz))
      {
        throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                     logging::ERR_AGGREGATION_TOO_BIG),
                                 logging::ERR_AGGREGATION_TOO_BIG);
      }
      auto* curRG = new RGData(*fRowGroupOut, fMaxRows);
      fRowGroupOut->setData(curRG);
      fRowGroupOut->resetRowGroup(0);
      fRGDatas.emplace_back(curRG);
      fCurRgid = fRGDatas.size() - 1;
    }

    fLRU->add(fCurRgid);
    idx = fCurRgid * fMaxRows + fRowGroupOut->getRowCount();
    fRowGroupOut->getRow(fRowGroupOut->getRowCount(), &row);
    fRowGroupOut->incRowCount();
  }

  /** @brief Create a row at the specified position.
   *
   *    Used only for key rows in case of external keys. Indexes of data row and
   *    corresponding key row are always the same.
   *
   * @param idx(in)  index to create row
   * @param row(out) row itself
   */
  void putKeyRow(uint64_t idx, Row& row)
  {
    uint64_t rgid = idx / fMaxRows;
    uint64_t rid = idx % fMaxRows;

    while (rgid >= fRGDatas.size())
    {
      int64_t memSz = fRowGroupOut->getSizeWithStrings(fMaxRows);
      if (!fMM->acquire(memSz))
      {
        throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                     logging::ERR_AGGREGATION_TOO_BIG),
                                 logging::ERR_AGGREGATION_TOO_BIG);
      }
      auto* curRG = new RGData(*fRowGroupOut, fMaxRows);
      fRowGroupOut->setData(curRG);
      fRowGroupOut->resetRowGroup(0);
      fRGDatas.emplace_back(curRG);
      fCurRgid = fRGDatas.size() - 1;
      fLRU->add(fCurRgid);
    }

    if (UNLIKELY(!fRGDatas[rgid]))
    {
      loadRG(rgid);
    }
    else
    {
      fRowGroupOut->setData(fRGDatas[rgid].get());
    }

    fLRU->add(rgid);

    assert(rid == fRowGroupOut->getRowCount());
    fRowGroupOut->getRow(fRowGroupOut->getRowCount(), &row);
    fRowGroupOut->incRowCount();
  }

  /** @brief Dump the oldest RGData to disk, freeing memory
   *
   * @returns true if any RGData was dumped
   */
  bool dump()
  {
    while (true)
    {
      auto it = fLRU->rbegin();
      if (UNLIKELY(it == fLRU->rend()))
        return false;

      uint64_t rgid = *it;
      if (UNLIKELY(!fRGDatas[rgid]))
      {
        fLRU->remove(rgid);
        continue;
      }
      fRowGroupOut->setData(fRGDatas[rgid].get());
      auto memSz = fRowGroupOut->getSizeWithStrings(fMaxRows);
      fMM->release(memSz);
      saveRG(rgid);
      fLRU->remove(rgid);
      fRGDatas[rgid].reset();
      break;
    }
    return true;
  }

  /** @brief Dump all data, clear state and start over */
  void startNewGeneration()
  {
    dumpAll();
    fLRU->clear();
    fMM->release();
    fRGDatas.clear();
    ++fGeneration;
  }

  /** @brief Save "finalized" bitmap to disk for future use */
  void dumpFinalizedInfo() const
  {
    auto fname = makeFinalizedFilename();
    int fd = open(fname.c_str(),
                  O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (UNLIKELY(fd < 0))
    {
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
          logging::ERR_DISKAGG_FILEIO_ERROR),
                               logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    uint64_t sz = fRGDatas.size();
    uint64_t finsz = fFinalizedRows.size();

    if (!writeData(fd, (const char*)&sz, sizeof(sz)) ||
        !writeData(fd, (const char*)&finsz, sizeof(finsz)) ||
        !writeData(fd, (const char*)fFinalizedRows.data(), finsz * sizeof(uint64_t)))
    {
      close(fd);
      unlink(fname.c_str());
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
          logging::ERR_DISKAGG_FILEIO_ERROR),
                               logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    close(fd);
  }

  /** @brief Load "finalized" bitmap */
  void loadFinalizedInfo()
  {
    auto fname = makeFinalizedFilename();
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0)
    {
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
          logging::ERR_DISKAGG_FILEIO_ERROR),
                               logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    uint64_t sz;
    uint64_t finsz;
    if (!readData(fd, (char*)&sz, sizeof(sz)) ||
        !readData(fd, (char*)&finsz, sizeof(finsz)))
    {
      close(fd);
      unlink(fname.c_str());
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
          logging::ERR_DISKAGG_FILEIO_ERROR),
                               logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    fRGDatas.resize(sz);
    fFinalizedRows.resize(finsz);
    if (!readData(fd, (char*)fFinalizedRows.data(), finsz * sizeof(uint64_t)))
    {
      close(fd);
      unlink(fname.c_str());
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
          logging::ERR_DISKAGG_FILEIO_ERROR),
                               logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    close(fd);
    unlink(fname.c_str());
  }

  /** @brief Save all RGData to disk */
  void dumpAll(bool dumpFin = true) const
  {
#ifdef DISK_AGG_DEBUG
    dumpMeta();
#endif
    for (uint64_t i = 0; i < fRGDatas.size(); ++i)
    {
      if (fRGDatas[i])
        saveRG(i, fRGDatas[i].get());
      else
      {
        auto fname = makeRGFilename(i);
        if (access(fname.c_str(), F_OK) != 0)
          ::abort();
      }
    }
    if (dumpFin)
      dumpFinalizedInfo();
  }

  /** @brief Create new RowGroupStorage with the save LRU, MemManager & uniq ID */
  RowGroupStorage* clone(uint16_t gen) const
  {
    auto* ret = new RowGroupStorage(fTmpDir, fRowGroupOut, fMaxRows);
    ret->fRGDatas.clear();
    ret->fLRU.reset(fLRU->clone());
    ret->fMM.reset(fMM->clone());
    ret->fUniqId = fUniqId;
    ret->fGeneration = gen;
    ret->loadFinalizedInfo();
    return ret;
  }

  /** @brief Mark row at specified index as finalized so it should be skipped
   */
  void markFinalized(uint64_t idx)
  {
    uint64_t gid = idx / 64;
    uint64_t rid = idx % 64;
    if (LIKELY(fFinalizedRows.size() <= gid))
      fFinalizedRows.resize(gid + 1, 0ULL);

    fFinalizedRows[gid] |= 1ULL << rid;
  }

  /** @brief Check if row at specified index was finalized earlier */
  bool isFinalized(uint64_t idx) const
  {
    uint64_t gid = idx / 64;
    uint64_t rid = idx % 64;
    if (LIKELY(fFinalizedRows.size() <= gid))
      return false;

    return fFinalizedRows[gid] & (1ULL << rid);
  }

private:

  /** @brief Get next available RGData and fill filename of the dump if it's
   *         not in the memory.
   *
   *   It skips finalized rows, shifting data within rowgroups
   *
   * @param rgdata(out) RGData to return
   * @param fname(out)  Filename of the dump if it's not in the memory
   * @returns true if there is available RGData
   */
  bool getNextRGData(std::unique_ptr<RGData>& rgdata, std::string& fname)
  {
    if (UNLIKELY(fRGDatas.empty()))
    {
      fMM->release();
      return false;
    }
    while (!fRGDatas.empty())
    {
      uint64_t rgid = fRGDatas.size() - 1;
      rgdata = std::move(fRGDatas[rgid]);
      fRGDatas.pop_back();

      uint64_t fgid = rgid * fMaxRows / 64;
      uint64_t tgid = fgid + fMaxRows / 64;
      if (fFinalizedRows.size() > fgid)
      {
        if (tgid >= fFinalizedRows.size())
          fFinalizedRows.resize(tgid + 1, 0ULL);

        if (!rgdata)
        {
          for (auto i = fgid; i < tgid; ++i)
          {
            if (fFinalizedRows[i] != ~0ULL)
            {
              loadRG(rgid, rgdata, true);
              break;
            }
          }
        }

        if (!rgdata)
        {
          unlink(makeRGFilename(rgid).c_str());
          continue;
        }

        uint64_t pos = 0;
        uint64_t opos = 0;
        fRowGroupOut->setData(rgdata.get());
        for (auto i = fgid; i < tgid; ++i)
        {
          if ((i - fgid) * 64 >= fRowGroupOut->getRowCount())
            break;
          uint64_t mask = ~fFinalizedRows[i];
          if ((i - fgid + 1) * 64 > fRowGroupOut->getRowCount())
          {
            mask &= (~0ULL) >> ((i - fgid + 1) * 64 - fRowGroupOut->getRowCount());
          }
          opos = (i - fgid) * 64;
          if (mask == ~0ULL)
          {
            if (LIKELY(pos != opos))
              moveRows(rgdata.get(), pos, opos, 64);
            pos += 64;
            continue;
          }

          if (mask == 0)
            continue;

          while (mask != 0)
          {
            size_t b = __builtin_ffsll(mask);
            size_t e = __builtin_ffsll(~(mask >> b)) + b;
            if (UNLIKELY(e >= 64))
              mask = 0;
            else
              mask >>= e;
            if (LIKELY(pos != opos + b - 1))
              moveRows(rgdata.get(), pos, opos + b - 1, e - b);
            pos += e - b;
            opos += e;
          }
          --opos;
        }

        if (pos == 0)
        {
          fLRU->remove(rgid);
          unlink(makeRGFilename(rgid).c_str());
          continue;
        }

        fRowGroupOut->setData(rgdata.get());
        fRowGroupOut->setRowCount(pos);
      }

      if (rgdata)
      {
        fRowGroupOut->setData(rgdata.get());
        int64_t memSz = fRowGroupOut->getSizeWithStrings(fMaxRows);
        fMM->release(memSz);
        unlink(makeRGFilename(rgid).c_str());
      }
      else
      {
        fname = makeRGFilename(rgid);
      }
      fLRU->remove(rgid);
      return true;
    }
    return false;
  }

  /** @brief Compact rowgroup with finalized rows
   *
   *   Move raw data from the next non-finalized row to replace finalized rows
   *   It is safe because pointers to long string also stored inside data
   *
   * @param rgdata(in,out) RGData to work with
   * @param to(in)         row num inside rgdata of the first finalized row
   * @param from(in)       row num of the first actual row
   * @param numRows(in)    how many rows should be moved
   */
  void moveRows(RGData* rgdata, uint64_t to, uint64_t from, size_t numRows)
  {
    const size_t rowsz = fRowGroupOut->getRowSize();
    const size_t hdrsz = RowGroup::getHeaderSize();
    uint8_t* data = rgdata->rowData.get() + hdrsz;
    memmove(data + to * rowsz,
            data + from * rowsz,
            numRows * rowsz);
  }

  /** @brief Load RGData from disk dump.
   *
   * @param rgid(in) RGData ID
   */
  void loadRG(uint64_t rgid)
  {
    if (UNLIKELY(static_cast<bool>(fRGDatas[rgid])))
    {
      fRowGroupOut->setData(fRGDatas[rgid].get());
      return;
    }

    loadRG(rgid, fRGDatas[rgid]);
  }

  void loadRG(uint64_t rgid, std::unique_ptr<RGData>& rgdata, bool unlinkDump = false)
  {
    auto fname = makeRGFilename(rgid);
    int fd = open(fname.c_str(), O_RDONLY);
    if (UNLIKELY(fd < 0))
    {
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                   logging::ERR_DISKAGG_FILEIO_ERROR),
                               logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    messageqcpp::ByteStream bs;

    try
    {
      struct stat st
      {
      };
      fstat(fd, &st);

      bs.needAtLeast(st.st_size);
      bs.restart();
      if (!readData(fd, (char*)bs.getInputPtr(), st.st_size))
      {
        close(fd);
        unlink(fname.c_str());
        throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
            logging::ERR_DISKAGG_FILEIO_ERROR),
                                 logging::ERR_DISKAGG_FILEIO_ERROR);
      }
      bs.advanceInputPtr(st.st_size);
      close(fd);
    }
    catch (...)
    {
      close(fd);
      throw;
    }

    if (unlinkDump)
      unlink(fname.c_str());
    rgdata.reset(new RGData());
    rgdata->deserialize(bs);

    fRowGroupOut->setData(rgdata.get());
    auto memSz = fRowGroupOut->getSizeWithStrings(fMaxRows);

    if (!fMM->acquire(memSz))
    {
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                   logging::ERR_AGGREGATION_TOO_BIG),
                               logging::ERR_AGGREGATION_TOO_BIG);
    }
  }

  /** @brief Dump RGData to disk.
   *
   * @param rgid(in) RGData ID
   */
  void saveRG(uint64_t rgid)
  {
    auto rgdata = std::move(fRGDatas[rgid]);
    if (!rgdata)
      return;

    fLRU->remove(rgid);
    fRowGroupOut->setData(rgdata.get());
    fMM->release(fRowGroupOut->getSizeWithStrings(fMaxRows));

    saveRG(rgid, rgdata.get());
  }

  /** @brief Dump RGData to disk.
   *
   * @param rgid(in)   RGData ID
   * @param rgdata(in) pointer to RGData itself
   */
  void saveRG(uint64_t rgid, const RGData* rgdata) const
  {
    messageqcpp::ByteStream bs;
    rgdata->serialize(bs, fRowGroupOut->getDataSize(fMaxRows));

    int fd = open(makeRGFilename(rgid).c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (UNLIKELY(fd < 0))
    {
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                   logging::ERR_DISKAGG_FILEIO_ERROR),
                               logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    if (!writeData(fd, (char*)bs.buf(), bs.length()))
    {
      close(fd);
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
          logging::ERR_DISKAGG_FILEIO_ERROR),
                               logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    close(fd);
  }

#ifdef DISK_AGG_DEBUG
  void dumpMeta() const
  {
    messageqcpp::ByteStream bs;
    fRowGroupOut->serialize(bs);

    char buf[1024];
    snprintf(buf, sizeof(buf), "/tmp/kemm/META-p%u-t%p", getpid(), fUniqPtr);
    int fd = open(buf, O_WRONLY | O_TRUNC | O_CREAT, 0644);
    assert(fd >= 0);

    auto r = write(fd, bs.buf(), bs.length());
    assert(r == bs.length());
    close(fd);
  }
#endif

  /** @brief Create dump filename.
   */
  std::string makeRGFilename(uint64_t rgid) const
  {
    char buf[PATH_MAX];
    snprintf(buf, sizeof(buf), "%s/Agg-p%u-t%p-rg%lu-g%u",
             fTmpDir.c_str(), getpid(), fUniqId, rgid, fGeneration);
    return buf;
  }

  std::string makeFinalizedFilename() const
  {
    char fname[PATH_MAX];
    snprintf(fname, sizeof(fname), "%s/AggFin-p%u-t%p-g%u",
             fTmpDir.c_str(), getpid(), fUniqId, fGeneration);
    return fname;
  }

private:
  friend class RowAggStorage;
  RowGroup* fRowGroupOut{nullptr};
  const size_t fMaxRows;
  std::unique_ptr<MemManager> fMM;
  std::unique_ptr<LRUIface> fLRU;
  RGDataStorage fRGDatas;
  const void* fUniqId;

  uint64_t fCurRgid{0};
  uint16_t fGeneration{0};
  std::vector<uint64_t> fFinalizedRows;
  std::string fTmpDir;
};

/** @brief Internal data for the hashmap */
struct RowPosHash
{
  uint64_t hash; ///< Row hash
  uint64_t idx;  ///< index in the RowGroupStorage
};

class RowPosHashStorage
{
public:
  RowPosHashStorage(const std::string& tmpDir,
                    size_t maxRows,
                    size_t size,
                    joblist::ResourceManager* rm,
                    boost::shared_ptr<int64_t> sessLimit,
                    bool enableDiskAgg)
      : fMaxRows(maxRows)
      , fUniqId(this)
      , fTmpDir(tmpDir)
  {
    if (rm)
      fMM = new RMMemManager(rm, sessLimit, !enableDiskAgg, !enableDiskAgg);
    else
      fMM = new MemManager();

    if (enableDiskAgg)
      fLRU = new LRU();
    else
      fLRU = new LRUIface();

    if (size != 0)
      init(size + 1);
  }

  ~RowPosHashStorage()
  {
    for (RowPosHash* ptr : fPosHashes)
    {
      delete[] ptr;
    }
    delete fLRU;
    delete fMM;
    if (fDumpFd >= 0)
      close(fDumpFd);
  }

  void cleanup()
  {
    unlink(makeDumpName().c_str());
  }

  RowPosHash& get(uint64_t idx)
  {
    uint64_t gid = idx / fMaxRows;
    uint64_t rid = idx % fMaxRows;
    if (fPosHashes[gid] == nullptr)
    {
      load(gid);
    }
    else
    {
      fLRU->add(gid);
    }
    return fPosHashes[gid][rid];
  }

  void set(uint64_t idx, const RowPosHash & rowData)
  {
    uint64_t gid = idx / fMaxRows;
    uint64_t rid = idx % fMaxRows;
    if (fPosHashes[gid] == nullptr)
    {
      load(gid);
    }
    else
    {
      fLRU->add(gid);
    }
    fPosHashes[gid][rid] = rowData;
  }

  bool dump()
  {
    if (!fLRU->empty())
    {
      auto gid = *fLRU->begin();
      save(gid);
      return true;
    }
    return false;
  }

  void clearData()
  {
    for (auto*& ptr : fPosHashes)
    {
      delete[] ptr;
      ptr = nullptr;
    }
    fMM->release();
    fLRU->clear();
  }

  void dropOld(uint64_t gid)
  {
    if (fPosHashes[gid])
    {
      delete[] fPosHashes[gid];
      fPosHashes[gid] = nullptr;
      fMM->release(fMaxRows * sizeof(RowPosHash));
    }
  }

  void shiftUp(uint64_t startIdx, uint64_t insIdx)
  {
    uint64_t gid = startIdx / fMaxRows;
    uint64_t rid = startIdx % fMaxRows;
    uint64_t ins_gid = insIdx / fMaxRows;
    uint64_t ins_rid = insIdx % fMaxRows;

    auto idx = startIdx;
    while (idx > insIdx)
    {
      if (UNLIKELY(fPosHashes[gid] == nullptr))
      {
        load(gid);
      }
      if (rid == 0)
      {
        if (UNLIKELY(fPosHashes[gid - 1] == nullptr))
        {
          load(gid - 1);
        }
        fPosHashes[gid][rid] = fPosHashes[gid - 1][fMaxRows - 1];
        --idx;
        if (UNLIKELY(idx == insIdx))
          break;
        --gid;
        rid = fMaxRows - 1;
      }

      uint64_t from_rid = 0;
      size_t num_rows = rid;
      if (LIKELY(ins_gid == gid))
      {
        from_rid = ins_rid;
        num_rows = rid - ins_rid;
      }

      memmove(&fPosHashes[gid][from_rid + 1],
              &fPosHashes[gid][from_rid],
              num_rows * sizeof(RowPosHash));
      idx -= num_rows;
      gid = idx / fMaxRows;
      rid = idx % fMaxRows;
    }
  }

  RowPosHashStorage* clone(size_t size, uint16_t gen)
  {
    if (UNLIKELY(fPosHashes.empty()))
    {
      fGeneration = gen;
      init(size + 1);
      return this;
    }
    auto* cloned = new RowPosHashStorage(fTmpDir, fMaxRows);
    cloned->fLRU = fLRU->clone();
    cloned->fMM = fMM->clone();
    cloned->init(size + 1);
    cloned->fUniqId = fUniqId;
    cloned->fDumpFd = -1;
    cloned->fGeneration = gen;
    return cloned;
  }

  void resetLRU()
  {
    delete fLRU;
    fLRU = new LRUIface();
  }

  void startNewGeneration()
  {
    dumpAll();
    ++fGeneration;
    close(fDumpFd);
    fDumpFd = -1;
    for (auto* ptr : fPosHashes)
      delete[] ptr;
    fPosHashes.clear();
    fMM->release();
  }

  void dumpAll()
  {
    for (uint64_t gid = 0; gid < fPosHashes.size(); ++gid)
      save(gid);
  }

private:
  explicit RowPosHashStorage(const std::string& tmpDir, size_t maxRows)
      : fMaxRows(maxRows)
      , fTmpDir(tmpDir)
  {}

  void init(size_t size)
  {
    size_t groups = (size + (fMaxRows - 1)) / fMaxRows;
    fPosHashes.reserve(groups);
    for (size_t i = 0; i < groups; ++i)
    {
      fMM->acquire(fMaxRows * sizeof(RowPosHash));
      auto *ptr = new RowPosHash[fMaxRows];
      memset(ptr, 0, fMaxRows * sizeof(RowPosHash));
      fPosHashes.push_back(ptr);
      fLRU->add(i);
    }
  }

  std::string makeDumpName() const
  {
    char fname[PATH_MAX];
    snprintf(fname, sizeof(fname), "%s/Agg-PosHash-p%u-t%p-g%u",
             fTmpDir.c_str(), getpid(), fUniqId, fGeneration);
    return fname;
  }

  int getDumpFd()
  {
    if (UNLIKELY(fDumpFd < 0))
    {
      fDumpFd = open(makeDumpName().c_str(), O_RDWR | O_CREAT, 0644);
      if (fDumpFd < 0)
      {
        throw logging::IDBExcept(
            logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR),
            logging::ERR_DISKAGG_FILEIO_ERROR);
      }
    }

    return fDumpFd;
  }

  void save(uint64_t gid)
  {
    if (fPosHashes[gid] == nullptr)
      return;
    int fd = getDumpFd();
    off_t fpos = gid * fMaxRows * sizeof(RowPosHash);
    off_t buf_pos = 0;
    ssize_t to_write = fMaxRows * sizeof(RowPosHash);
    while (to_write > 0)
    {
      auto r = pwrite(fd,
                      reinterpret_cast<const char*>(fPosHashes[gid]) + buf_pos,
                      to_write,
                      fpos);
      if (UNLIKELY(r < 0))
      {
        if (errno == EAGAIN)
          continue;

        throw logging::IDBExcept(
            logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR),
            logging::ERR_DISKAGG_FILEIO_ERROR);
      }

      assert(r <= to_write);
      to_write -= r;
      fpos += r;
      buf_pos += r;
    }

    delete[] fPosHashes[gid];
    fPosHashes[gid] = nullptr;
    fMM->release(fMaxRows * sizeof(RowPosHash));
    fLRU->remove(gid);
  }

  void load(uint64_t gid)
  {
    if (fPosHashes[gid])
      return;
    int fd = getDumpFd();
    if (!fMM->acquire(fMaxRows * sizeof(RowPosHash)))
    {
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
          logging::ERR_AGGREGATION_TOO_BIG),
                               logging::ERR_AGGREGATION_TOO_BIG);
    }
    fPosHashes[gid] = new RowPosHash[fMaxRows];
    memset(fPosHashes[gid], 0, fMaxRows * sizeof(RowPosHash));

    off_t fpos = gid * fMaxRows * sizeof(RowPosHash);
    off_t buf_pos = 0;
    ssize_t to_read = fMaxRows * sizeof(RowPosHash);
    while (to_read > 0)
    {
      errno = 0;
      auto r = pread(fd,
                     reinterpret_cast<char*>(fPosHashes[gid]) + buf_pos,
                     to_read,
                     fpos);
      if (UNLIKELY(r <= 0))
      {
        if (errno == EAGAIN)
          continue;

        throw logging::IDBExcept(
            logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR),
            logging::ERR_DISKAGG_FILEIO_ERROR);
      }

      assert(r <= to_read);
      to_read -= r;
      fpos += r;
      buf_pos += r;
    }
    fLRU->add(gid);
  }

private:
  friend class RowAggStorage;
  const size_t fMaxRows;
  MemManager* fMM{nullptr};
  LRUIface* fLRU{nullptr};
  std::vector<RowPosHash*> fPosHashes;

  uint16_t fGeneration{0};

  int fDumpFd = -1;
  void* fUniqId;
  std::string fTmpDir;
};

/*---------------------------------------------------------------------------
 * Based on the Robin Hood hashmap implementation by Martin Ankerl:
 * https://github.com/martinus/robin-hood-hashing
 *
 * But store actual row data within RowGroupStorage, that stores a vector
 * of RGData with possibility to dump unused to disk. Also store some service
 * information (index and hash) in the vector of portions of the same size in
 * elements, that can be dumped on disk either.
 ----------------------------------------------------------------------------*/
RowAggStorage::RowAggStorage(const std::string& tmpDir,
                             RowGroup *rowGroupOut, RowGroup *keysRowGroup,
                             uint32_t keyCount, size_t maxRows,
                             joblist::ResourceManager *rm,
                             boost::shared_ptr<int64_t> sessLimit,
                             bool enabledDiskAgg,
                             bool allowGenerations)
    : fMaxRows(maxRows)
    , fExtKeys(rowGroupOut != keysRowGroup)
    , fStorage(new RowGroupStorage(tmpDir, rowGroupOut, maxRows, rm, sessLimit, !enabledDiskAgg, !enabledDiskAgg))
    , fKeysStorage(fStorage.get())
    , fLastKeyCol(keyCount - 1)
    , fUniqId(this)
    , fAllowGenerations(allowGenerations)
    , fTmpDir(tmpDir)
{
  if (rm)
  {
    fMM = std::unique_ptr<MemManager>(new RMMemManager(rm, sessLimit, !enabledDiskAgg, !enabledDiskAgg));
    fInitialMemLimit = std::min<int64_t>(rm->getConfiguredUMMemLimit(), *sessLimit);
    fNumOfOuterBuckets = std::max<uint32_t>(1, rm->aggNumBuckets());
  }
  else
  {
    fMM = std::unique_ptr<MemManager>(new MemManager());
    fInitialMemLimit = fMM->getFree();
    fNumOfOuterBuckets = 1;
  }
  if (fExtKeys)
  {
    fKeysStorage = new RowGroupStorage(tmpDir, keysRowGroup, maxRows, rm, sessLimit, !enabledDiskAgg, !enabledDiskAgg);
  }
  fKeysStorage->initRow(fKeyRow);
  fHashes = new RowPosHashStorage(tmpDir, maxRows, 0, rm, sessLimit, enabledDiskAgg);
  reserve(maxRows);
}

RowAggStorage::~RowAggStorage()
{
  if (fExtKeys)
    delete fKeysStorage;
  if (fInfo != nullptr)
    free(fInfo);
  delete fHashes;
}

bool RowAggStorage::getTargetRow(const Row &row, Row &rowOut)
{
  if (UNLIKELY(fSize >= fMaxSize))
  {
    increaseSize();
  }
  size_t idx{};
  uint32_t info{};
  uint64_t hash{};

  rowToIdx(row, info, idx, hash);
  nextWhileLess(info, idx);

  while (info == fInfo[idx])
  {
    auto& pos = fHashes->get(idx);
    if (pos.hash == hash)
    {
      auto& keyRow = fExtKeys ? fKeyRow : rowOut;
      fKeysStorage->getRow(pos.idx, keyRow);
      if (row.equals(keyRow, fLastKeyCol))
      {
        if (!fExtKeys)
          return false;

        fStorage->getRow(pos.idx, rowOut);
        return false;
      }
    }

    next(info, idx);
  }

  const auto ins_idx = idx;
  const auto ins_info = info;
  if (UNLIKELY(ins_info + fInfoInc > 0xFF))
  {
    fMaxSize = 0;
  }

  while (fInfo[idx] != 0)
  {
    next(info, idx);
  }

  if (idx != ins_idx)
  {
    shiftUp(idx, ins_idx);
  }
  RowPosHash pos;
  pos.hash = hash;
  fStorage->putRow(pos.idx, rowOut);
  if (fExtKeys)
  {
    fKeysStorage->putKeyRow(pos.idx, fKeyRow);
    copyRow(row, &fKeyRow);
  }
  fHashes->set(ins_idx, pos);
  fInfo[ins_idx] = static_cast<uint8_t>(ins_info);
  ++fSize;
  return true;
}

void RowAggStorage::dump()
{
  uint64_t i{0};
  constexpr uint64_t ROUNDS = 20;
  constexpr uint8_t POSHASH_DUMP_FREQ = 5;
  while (!fAllowGenerations || ++i < ROUNDS)
  {
    if (fMM->getFree() > 1 * 1024 * 1024) // TODO: make it configurable
      break;

    bool success = fStorage->dump();
    if (fExtKeys)
      success |= fKeysStorage->dump();

    if (success && i % POSHASH_DUMP_FREQ != 0)
    {
      // Try to unload only RGDatas first 'cause unloading RowHashPositions
      // causes a worse performance impact
      continue;
    }

    if (!fHashes->dump())
      break;
  }

  auto rhUsedMem = fMM->getUsed();
  if (rhUsedMem >= size_t(fInitialMemLimit / fNumOfOuterBuckets / 2)
      || (fAllowGenerations && i >= ROUNDS))
  {
    // safety guard so aggregation couldn't eat all available memory
    if (fMM->isStrict() || !fAllowGenerations)
    {
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                   logging::ERR_DISKAGG_TOO_BIG),
                               logging::ERR_DISKAGG_TOO_BIG);
    }
    startNewGeneration();
  }
}

void RowAggStorage::append(RowAggStorage& other)
{
  // we don't need neither key rows storage nor any internal data anymore
  // neither in this RowAggStorage nor in the other
  freeData();
  cleanup();
  if (other.fGeneration == 0)
  {
    other.freeData();
    fStorage->append(std::move(other.fStorage));
    other.cleanup();
    return;
  }

  // iff other RowAggStorage has several generations, sequential load and append
  // them all
  auto gen = other.fGeneration;
  while (true)
  {
    fStorage->append(other.fStorage.get());
    other.cleanup();
    if (gen == 0)
      break;
    //other.loadGeneration(--gen);
    --gen;
    other.fGeneration = gen;
    auto* oh = other.fHashes;
    other.fHashes = other.fHashes->clone(other.fMask + 1, gen);
    delete oh;
    other.fStorage.reset(other.fStorage->clone(gen));
  }
}

std::unique_ptr<RGData> RowAggStorage::getNextRGData()
{
  freeData();
  cleanup();
  return fStorage->getNextRGData();
}

void RowAggStorage::freeData()
{
  if (fExtKeys)
  {
    delete fKeysStorage;
    fKeysStorage = nullptr;
  }
  delete fHashes;
  fHashes = nullptr;
  if (fInfo)
  {
    const size_t memSz = calcSizeWithBuffer(fMask + 1);
    fMM->release(memSz);
    free(fInfo);
    fInfo = nullptr;
  }
}

void RowAggStorage::shiftUp(size_t startIdx, size_t insIdx)
{
  auto idx = startIdx;
  while (idx != insIdx) {
    fInfo[idx] = static_cast<uint8_t>(fInfo[idx - 1] + fInfoInc);
    if (UNLIKELY(fInfo[idx] + fInfoInc > 0xFF)) {
      fMaxSize = 0;
    }
    --idx;
  }
  fHashes->shiftUp(startIdx, insIdx);
}

void RowAggStorage::rowToIdx(const Row &row,
                             uint32_t& info,
                             size_t& idx,
                             uint64_t& hash) const
{
  size_t h = mixHash(hashRow(row, fLastKeyCol));
  hash = h;
  info = fInfoInc + static_cast<uint32_t>((h & INFO_MASK) >> fInfoHashShift);
  idx = (h >> INIT_INFO_BITS) & fMask;
}

void RowAggStorage::increaseSize()
{
  if (fMask == 0)
  {
    initData(INIT_SIZE);
    return;
  }

  const auto maxSize = calcMaxSize(fMask + 1);
  if (fSize < maxSize && tryIncreaseInfo())
    return;

  if (fSize * 2 < calcMaxSize(fMask + 1))
  {
    // something strange happens...
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                 logging::ERR_DISKAGG_ERROR),
                             logging::ERR_DISKAGG_ERROR);
  }

  rehashPowerOfTwo((fMask + 1) * 2);
}

bool RowAggStorage::tryIncreaseInfo()
{
  if (fInfoInc <= 2)
    return false;

  fInfoInc = static_cast<uint8_t>(fInfoInc >> 1U);
  ++fInfoHashShift;
  const auto elems = calcSizeWithBuffer(fMask + 1);
  for (size_t i = 0; i < elems; i += 8)
  {
    uint64_t val;
    memcpy(&val, fInfo + i, sizeof(val));
    val = (val >> 1U) & 0x7f7f7f7f7f7f7f7fULL;
    memcpy(fInfo + i, &val, sizeof(val));
  }

  fInfo[elems] = 1;
  fMaxSize = calcMaxSize(fMask + 1);
  return true;
}

void RowAggStorage::rehashPowerOfTwo(size_t elems)
{
  const size_t oldSz = calcSizeWithBuffer(fMask + 1);
  const uint8_t* const oldInfo = fInfo;
  RowPosHashStorage* oldHashes = fHashes;
  oldHashes->resetLRU();

  initData(elems);

  if (oldSz > 1)
  {
    for (size_t i = 0; i < oldSz; ++i)
    {
      if (UNLIKELY(oldInfo[i] != 0))
      {
        insertSwap(i, oldHashes);
      }
      if (UNLIKELY(i != 0 && i % fMaxRows == 0))
      {
        // we don't need this portion of data anymore so free it ASAP
        oldHashes->dropOld(i / fMaxRows - 1);
      }
    }

    delete oldHashes;
    if (oldInfo != nullptr)
    {
      fMM->release(calcBytes(oldSz));
      free((void*)oldInfo);
    }
  }
}

void RowAggStorage::insertSwap(size_t oldIdx, RowPosHashStorage * oldHashes)
{
  if (fMaxSize == 0 && !tryIncreaseInfo())
  {
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                 logging::ERR_DISKAGG_ERROR),
                             logging::ERR_DISKAGG_ERROR);
  }

  size_t idx{};
  uint32_t info{};
  auto pos = oldHashes->get(oldIdx);
  rowHashToIdx(pos.hash, info, idx);

  while (info <= fInfo[idx])
  {
    ++idx;
    info += fInfoInc;
  }

  // don't need to compare rows here - they are differs by definition
  const auto ins_idx = idx;
  const auto ins_info = static_cast<uint8_t>(info);
  if (UNLIKELY(ins_info + fInfoInc > 0xFF))
    fMaxSize = 0;

  while (fInfo[idx] != 0)
  {
    next(info, idx);
  }

  if (idx != ins_idx)
    shiftUp(idx, ins_idx);

  fHashes->set(ins_idx, pos);
  fInfo[ins_idx] = ins_info;
  ++fSize;
}

void RowAggStorage::initData(size_t elems)
{
  fSize = 0;
  fMask = elems - 1;
  fMaxSize = calcMaxSize(elems);

  const auto sizeWithBuffer = calcSizeWithBuffer(elems, fMaxSize);
  const auto bytes = calcBytes(sizeWithBuffer);

  fHashes = fHashes->clone(elems, fGeneration);
  fMM->acquire(bytes);
  fInfo = reinterpret_cast<uint8_t*>(calloc(1, bytes));
  fInfo[sizeWithBuffer] = 1;
  fInfoInc = INIT_INFO_INC;
  fInfoHashShift = INIT_INFO_HASH_SHIFT;
}

void RowAggStorage::reserve(size_t c)
{
  auto const minElementsAllowed = (std::max)(c, fSize);
  auto newSize = INIT_SIZE;
  while (calcMaxSize(newSize) < minElementsAllowed && newSize != 0) {
    newSize *= 2;
  }
  if (UNLIKELY(newSize == 0)) {
    throw logging::IDBExcept(
        logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_ERROR),
        logging::ERR_DISKAGG_ERROR);
  }

  // only actually do anything when the new size is bigger than the old one. This prevents to
  // continuously allocate for each reserve() call.
  if (newSize > fMask + 1) {
    rehashPowerOfTwo(newSize);
  }
}

void RowAggStorage::startNewGeneration()
{
  if (fSize == 0)
    return;
  // save all data and free storages' memory
  dumpInternalData();
  fHashes->startNewGeneration();
  fStorage->startNewGeneration();
  if (fExtKeys)
    fKeysStorage->startNewGeneration();

  ++fGeneration;
  fMM->release();
  // reinitialize internal structures
  if (fInfo)
  {
    free(fInfo);
    fInfo = nullptr;
  }
  fSize = 0;
  fMask = 0;
  fMaxSize = 0;
  fInfoInc = INIT_INFO_INC;
  fInfoHashShift = INIT_INFO_HASH_SHIFT;
  reserve(fMaxRows);
  fAggregated = false;
}

std::string RowAggStorage::makeDumpFilename(int32_t gen) const
{
  char fname[1024];
  uint16_t rgen = gen < 0 ? fGeneration : gen;
  snprintf(fname, sizeof(fname), "%s/AggMap-p%u-t%p-g%u",
           fTmpDir.c_str(), getpid(), fUniqId, rgen);
  return fname;
}

void RowAggStorage::dumpInternalData() const
{
  if (!fInfo)
    return;

  messageqcpp::ByteStream bs;
  bs << fSize;
  bs << fMask;
  bs << fMaxSize;
  bs << fInfoInc;
  bs << fInfoHashShift;
  bs.append(fInfo, calcBytes(calcSizeWithBuffer(fMask + 1, fMaxSize)));
  int fd = open(makeDumpFilename().c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0)
  {
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
        logging::ERR_AGGREGATION_TOO_BIG),
                             logging::ERR_AGGREGATION_TOO_BIG);
  }
  if (!writeData(fd, (const char*)bs.buf(), bs.length()))
  {
    close(fd);
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
        logging::ERR_AGGREGATION_TOO_BIG),
                             logging::ERR_AGGREGATION_TOO_BIG);
  }
  close(fd);
}

void RowAggStorage::finalize(std::function<void(Row&)> mergeFunc, Row& rowOut)
{
  if (fAggregated || fGeneration == 0)
  {
    cleanup();
    return;
  }

  Row tmpKeyRow;
  fKeysStorage->initRow(tmpKeyRow);
  Row tmpRow;
  fStorage->initRow(tmpRow);
  dumpInternalData();
  fHashes->dumpAll();
  fStorage->dumpAll();
  if (fExtKeys)
    fKeysStorage->dumpAll();

  uint16_t curGen = fGeneration + 1;
  while (--curGen > 0)
  {
    bool genUpdated = false;
    if (fSize == 0)
    {
      fStorage->dumpFinalizedInfo();
      if (fExtKeys)
        fKeysStorage->dumpFinalizedInfo();
      cleanup(curGen);
      loadGeneration(curGen - 1);
      auto* oh = fHashes;
      fHashes = fHashes->clone(fMask + 1, curGen - 1);
      delete oh;
      fHashes->clearData();
      fStorage.reset(fStorage->clone(curGen - 1));
      if (fExtKeys)
      {
        auto* oks = fKeysStorage;
        fKeysStorage = oks->clone(curGen - 1);
        delete oks;
      }
      else
        fKeysStorage = fStorage.get();
      continue;
    }
    std::unique_ptr<RowPosHashStorage> prevHashes;
    size_t prevSize;
    size_t prevMask;
    size_t prevMaxSize;
    uint32_t prevInfoInc;
    uint32_t prevInfoHashShift;
    uint8_t *prevInfo{nullptr};

    std::unique_ptr<RowGroupStorage> prevRowStorage;
    RowGroupStorage *prevKeyRowStorage{nullptr};

    auto elems = calcSizeWithBuffer(fMask + 1);

    for (uint16_t prevGen = 0; prevGen < curGen; ++prevGen)
    {
      prevRowStorage.reset(fStorage->clone(prevGen));
      if (fExtKeys)
      {
        delete prevKeyRowStorage;
        prevKeyRowStorage = fKeysStorage->clone(prevGen);
      }
      else
        prevKeyRowStorage = prevRowStorage.get();

      loadGeneration(prevGen, prevSize, prevMask, prevMaxSize, prevInfoInc,
                     prevInfoHashShift, prevInfo);
      prevHashes = std::unique_ptr<RowPosHashStorage>(fHashes->clone(prevMask + 1, prevGen));
      prevHashes->clearData();

      // iterate over current generation rows
      uint64_t idx{};
      uint32_t info{};
      for (;; next(info, idx))
      {
        nextExisting(info, idx);

        if (idx >= elems)
        {
          // done finalizing generation
          break;
        }

        const auto& pos = fHashes->get(idx);

        if (fKeysStorage->isFinalized(pos.idx))
        {
          // this row was already merged into newer generation, skip it
          continue;
        }

        // now try to find row in the previous generation
        uint32_t pinfo =
            prevInfoInc + static_cast<uint32_t>((pos.hash & INFO_MASK) >> prevInfoHashShift);
        uint64_t pidx = (pos.hash >> INIT_INFO_BITS) & prevMask;
        while (pinfo < prevInfo[pidx])
        {
          ++pidx;
          pinfo += prevInfoInc;
        }
        if (prevInfo[pidx] != pinfo)
        {
          // there is no such row
          continue;
        }

        auto ppos = prevHashes->get(pidx);
        while (prevInfo[pidx] == pinfo && pos.hash != ppos.hash)
        {
          // hashes are not equal => collision, try all matched rows
          ++pidx;
          pinfo += prevInfoInc;
          ppos = prevHashes->get(pidx);
        }
        if (prevInfo[pidx] != pinfo || pos.hash != ppos.hash)
        {
          // no matches
          continue;
        }

        bool found = false;
        auto &keyRow = fExtKeys ? fKeyRow : rowOut;
        fKeysStorage->getRow(pos.idx, keyRow);
        while (!found && prevInfo[pidx] == pinfo)
        {
          // try to find exactly match in case of hash collision
          if (UNLIKELY(pos.hash != ppos.hash))
          {
            // hashes are not equal => no such row
            ++pidx;
            pinfo += prevInfoInc;
            ppos = prevHashes->get(pidx);
            continue;
          }

          prevKeyRowStorage->getRow(ppos.idx, fExtKeys ? tmpKeyRow : tmpRow);
          if (!keyRow.equals(fExtKeys ? tmpKeyRow : tmpRow, fLastKeyCol))
          {
            ++pidx;
            pinfo += prevInfoInc;
            ppos = prevHashes->get(pidx);
            continue;
          }
          found = true;
        }

        if (!found)
        {
          // nothing was found, go to the next row
          continue;
        }

        if (UNLIKELY(prevKeyRowStorage->isFinalized(ppos.idx)))
        {
          // just to be sure, it can NEVER happen
          continue;
        }

        // here it is! So:
        // 1 Get actual row datas
        // 2 Merge it into the current generation
        // 3 Mark generations as updated
        // 4 Mark the prev generation row as finalized
        if (fExtKeys)
        {
          prevRowStorage->getRow(ppos.idx, tmpRow);
          fStorage->getRow(pos.idx, rowOut);
        }
        mergeFunc(tmpRow);
        genUpdated = true;
        prevKeyRowStorage->markFinalized(ppos.idx);
        if (fExtKeys)
        {
          prevRowStorage->markFinalized(ppos.idx);
        }
      }

      prevRowStorage->dumpFinalizedInfo();
      if (fExtKeys)
      {
        prevKeyRowStorage->dumpFinalizedInfo();
      }
    }

    fStorage->dumpFinalizedInfo();
    if (fExtKeys)
      fKeysStorage->dumpFinalizedInfo();
    if (genUpdated)
    {
      // we have to save current generation data to disk 'cause we update some rows
      // TODO: to reduce disk IO we can dump only affected rowgroups
      fStorage->dumpAll(false);
      if (fExtKeys)
        fKeysStorage->dumpAll(false);
    }

    // swap current generation N with the prev generation N-1
    cleanup(curGen);
    fSize = prevSize;
    fMask = prevMask;
    fMaxSize = prevMaxSize;
    fInfoInc = prevInfoInc;
    fInfoHashShift = prevInfoHashShift;
    if (fInfo)
      free(fInfo);
    fInfo = prevInfo;
    if (fHashes)
      delete fHashes;
    fHashes = prevHashes.release();
    fStorage = std::move(prevRowStorage);
    if (fExtKeys)
      delete fKeysStorage;
    fKeysStorage = prevKeyRowStorage;
  }

  fStorage->dumpFinalizedInfo();
  if (fExtKeys)
    fKeysStorage->dumpFinalizedInfo();
  auto* oh = fHashes;
  fHashes = fHashes->clone(fMask + 1, fGeneration);
  delete oh;
  fHashes->clearData();
  fStorage.reset(fStorage->clone(fGeneration));
  if (fExtKeys)
  {
    auto* oks = fKeysStorage;
    fKeysStorage = oks->clone(fGeneration);
    delete oks;
  }
  else
    fKeysStorage = fStorage.get();

  fAggregated = true;
}

void RowAggStorage::loadGeneration(uint16_t gen)
{
  loadGeneration(gen, fSize, fMask, fMaxSize, fInfoInc, fInfoHashShift, fInfo);
}

void RowAggStorage::loadGeneration(uint16_t gen, size_t &size, size_t &mask, size_t &maxSize, uint32_t &infoInc, uint32_t &infoHashShift, uint8_t *&info)
{
  messageqcpp::ByteStream bs;
  int fd = open(makeDumpFilename(gen).c_str(), O_RDONLY);
  if (fd < 0)
  {
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
        logging::ERR_AGGREGATION_TOO_BIG),
                             logging::ERR_AGGREGATION_TOO_BIG);
  }
  struct stat st{};
  fstat(fd, &st);
  bs.needAtLeast(st.st_size);
  bs.restart();
  if (!readData(fd, (char*)bs.getInputPtr(), st.st_size))
  {
    close(fd);
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
        logging::ERR_AGGREGATION_TOO_BIG),
                             logging::ERR_AGGREGATION_TOO_BIG);
  }
  close(fd);
  bs.advanceInputPtr(st.st_size);

  bs >> size;
  bs >> mask;
  bs >> maxSize;
  bs >> infoInc;
  bs >> infoHashShift;
  size_t infoSz = calcBytes(calcSizeWithBuffer(mask + 1, maxSize));
  if (info)
    free(info);
  info = (uint8_t*)calloc(1, infoSz);
  bs >> info;
}

void RowAggStorage::cleanup()
{
  cleanup(fGeneration);
}

void RowAggStorage::cleanup(uint16_t gen)
{
  unlink(makeDumpFilename(gen).c_str());
  if (fHashes)
    fHashes->cleanup();
}

} // namespace rowgroup

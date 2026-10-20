/* Copyright (C) 2021-2022 MariaDB Corporation

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
#include <boost/filesystem.hpp>
#include <cstdint>
#include "branchpred.h"
#include "rowgroup.h"
#include <resourcemanager.h>
#include <fcntl.h>
#include "rowstorage.h"
#include "robin_hood.h"
#include "messagelog.h"
#include <sstream>

// #define DISK_AGG_DEBUG

namespace
{
int writeData(int fd, const char* buf, size_t sz)
{
  if (sz == 0)
    return 0;

  auto to_write = sz;
  while (to_write > 0)
  {
    auto r = write(fd, buf + sz - to_write, to_write);
    if (UNLIKELY(r < 0))
    {
      if (errno == EAGAIN)
        continue;

      return errno;
    }
    assert(size_t(r) <= to_write);
    to_write -= r;
  }

  return 0;
}

int readData(int fd, char* buf, size_t sz)
{
  if (sz == 0)
    return 0;

  auto to_read = sz;
  while (to_read > 0)
  {
    auto r = read(fd, buf + sz - to_read, to_read);
    if (UNLIKELY(r < 0))
    {
      if (errno == EAGAIN)
        continue;

      return errno;
    }

    assert(size_t(r) <= to_read);
    to_read -= r;
  }

  return 0;
}

std::string errorString(int errNo)
{
  char tmp[1024];
  auto* buf = strerror_r(errNo, tmp, sizeof(tmp));
  return {buf};
}
}  // anonymous namespace

namespace rowgroup
{
// Upstream has these in rowgroup.h (from MCOL-5852, not in 23.02).
inline uint64_t rowGidRidToIdx(uint64_t gid, uint32_t rid, uint32_t maxRows)
{
  return gid * maxRows + rid;
}

inline std::pair<uint64_t, uint64_t> rowIdxToGidRid(uint64_t idx, uint32_t maxRows)
{
  return {idx / maxRows, idx % maxRows};
}

uint64_t hashRow(const rowgroup::Row& r, std::size_t lastCol)
{
  uint64_t ret = 0;
  if (lastCol >= r.getColumnCount())
    return 0;

  datatypes::MariaDBHasher h;
  utils::Hasher64_r columnHasher;

  bool strHashUsed = false;

  for (uint32_t i = 0; i <= lastCol; ++i)
  {
    switch (r.getColType(i))
    {
      case execplan::CalpontSystemCatalog::CHAR:
      case execplan::CalpontSystemCatalog::VARCHAR:
      case execplan::CalpontSystemCatalog::BLOB:
      case execplan::CalpontSystemCatalog::TEXT:
      {
        auto cs = r.getCharset(i);
        auto strColValue = r.getConstString(i);
        auto strColValueLen = strColValue.length();
        if (strColValueLen > MaxConstStrSize)
        {
          h.add(cs, strColValue);
          strHashUsed = true;
        }
        else
        {
          // This is relatively big stack allocation.
          // It is aligned for future vectorization of hash calculation.
          uchar buf[MaxConstStrBufSize] __attribute__((aligned(64)));
          // Pay attention to the last strxfrm argument value.
          // It is called flags and in many cases it has padding
          // enabled(MY_STRXFRM_PAD_WITH_SPACE bit). With padding enabled
          // strxfrm returns MaxConstStrBufSize bytes and not the actual
          // weights array length. Here I disable padding.
          auto charset = datatypes::Charset(cs);
          auto trimStrColValue = strColValue.rtrimSpaces();
          // The padding is disabled b/c we previously use rtrimSpaces().
          // strColValueLen is used here.
          size_t nActualWeights = charset.strnxfrm(buf, MaxConstStrBufSize, strColValueLen,
                                                   reinterpret_cast<const uchar*>(trimStrColValue.str()),
                                                   trimStrColValue.length(), 0);
          ret = columnHasher(reinterpret_cast<const void*>(buf), nActualWeights, ret);
        }
        break;
      }
      default: ret = columnHasher(r.getData() + r.getOffset(i), r.getColumnWidth(i), ret); break;
    }
  }

  // The properties of the hash produced are worse if MDB hasher results are incorporated
  // so late but these results must be used very infrequently.
  if (strHashUsed)
  {
    uint64_t strhash = h.finalize();
    ret = columnHasher(&strhash, sizeof(strhash), ret);
  }
  return columnHasher.finalize(ret, lastCol << 2);
}

/** @brief NoOP interface to LRU-cache used by RowGroupStorage & HashStorage
 */
struct LRUIface
{
  using List = std::list<uint64_t>;

  virtual ~LRUIface() = default;
  /** @brief Put an ID to cache or set it as last used */
  virtual void add(uint64_t)
  {
  }
  /** @brief Remove an ID from cache */
  virtual void remove(uint64_t)
  {
  }
  /** @brief Get iterator of the most recently used ID */
  virtual List::const_reverse_iterator begin() const
  {
    return List::const_reverse_iterator();
  }
  /** @brief Get iterator after the latest ID */
  virtual List::const_reverse_iterator end() const
  {
    return List::const_reverse_iterator();
  }
  /** @brief Get iterator of the latest ID */
  virtual List::const_iterator rbegin() const
  {
    return {};
  }
  /** @brief Get iterator after the most recently used ID */
  virtual List::const_iterator rend() const
  {
    return {};
  }

  virtual void clear()
  {
  }
  virtual std::size_t size() const
  {
    return 0;
  }
  virtual bool empty() const
  {
    return true;
  }
  virtual LRUIface* clone() const
  {
    return new LRUIface();
  }
};

struct LRU : public LRUIface
{
  ~LRU() override
  {
    fMap.clear();
    fList.clear();
  }
  inline void add(uint64_t rgid) final
  {
    auto it = fMap.find(rgid);
    if (it != fMap.end())
    {
      fList.erase(it->second);
    }
    fMap[rgid] = fList.insert(fList.end(), rgid);
  }

  inline void remove(uint64_t rgid) final
  {
    auto it = fMap.find(rgid);
    if (UNLIKELY(it != fMap.end()))
    {
      fList.erase(it->second);
      fMap.erase(it);
    }
  }

  inline List::const_reverse_iterator begin() const final
  {
    return fList.crbegin();
  }
  inline List::const_reverse_iterator end() const final
  {
    return fList.crend();
  }
  inline List::const_iterator rbegin() const final
  {
    return fList.cbegin();
  }
  inline List::const_iterator rend() const final
  {
    return fList.cend();
  }
  inline void clear() final
  {
    fMap.clear();
    fList.clear();
  }

  size_t size() const final
  {
    return fMap.size();
  }
  bool empty() const final
  {
    return fList.empty();
  }

  LRUIface* clone() const final
  {
    return new LRU();
  }

  robin_hood::unordered_flat_map<uint64_t, List::iterator> fMap;
  List fList;
};

/** @brief Some service wrapping around ResourceManager (or NoOP) */
class MemManager
{
 public:
  MemManager()
  {
  }
  virtual ~MemManager()
  {
    release(fMemUsed);
  }

  bool acquire(ssize_t amount)
  {
    if (UNLIKELY(-amount > fMemUsed))
      amount = -fMemUsed;
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

  ssize_t getUsed() const
  {
    return fMemUsed;
  }
  virtual int64_t getFree() const
  {
    return std::numeric_limits<int64_t>::max();
  }

  virtual int64_t getConfigured() const
  {
    return std::numeric_limits<int64_t>::max();
  }

  virtual bool isStrict() const
  {
    return false;
  }

  virtual MemManager* clone() const
  {
    return new MemManager();
  }

  virtual joblist::ResourceManager* getResourceManaged()
  {
    return nullptr;
  }
  virtual boost::shared_ptr<int64_t> getSessionLimit()
  {
    return {};
  }

 protected:
  virtual bool acquireImpl(ssize_t amount)
  {
    fMemUsed += amount;
    return true;
  }
  virtual void releaseImpl(ssize_t amount)
  {
    fMemUsed -= amount;
  }
  ssize_t fMemUsed{0};
};

class RMMemManager : public MemManager
{
 public:
  RMMemManager(joblist::ResourceManager* rm, boost::shared_ptr<int64_t> sl, bool wait = true,
               bool strict = true)
   : fRm(rm), fSessLimit(std::move(sl)), fWait(wait), fStrict(strict)
  {
  }

  ~RMMemManager() override
  {
    release(fMemUsed);
    fMemUsed = 0;
  }

  int64_t getConfigured() const final
  {
    return fRm->getConfiguredUMMemLimit();
  }

  int64_t getFree() const final
  {
    return std::min(fRm->availableMemory(), *fSessLimit);
  }

  bool isStrict() const final
  {
    return fStrict;
  }

  MemManager* clone() const final
  {
    return new RMMemManager(fRm, fSessLimit, fWait, fStrict);
  }

  joblist::ResourceManager* getResourceManaged() override
  {
    return fRm;
  }
  boost::shared_ptr<int64_t> getSessionLimit() override
  {
    return fSessLimit;
  }

 protected:
  bool acquireImpl(ssize_t amount) final
  {
    if (amount)
    {
      if (fStrict)
      {
        if (!fRm->getMemory(amount, fSessLimit, fWait))
        {
          return false;
        }
      }
      else
      {
        // ignore the return value as the memory limit will be checked later
        fRm->getMemoryForce(amount, fSessLimit);
      }
      MemManager::acquireImpl(amount);
    }
    return true;
  }

  void releaseImpl(ssize_t amount) override
  {
    if (amount)
    {
      MemManager::releaseImpl(amount);
      fRm->returnMemory(amount, fSessLimit);
    }
  }

 private:
  joblist::ResourceManager* fRm = nullptr;
  boost::shared_ptr<int64_t> fSessLimit;
  const bool fWait;
  const bool fStrict;
};

class Dumper
{
 public:
  Dumper(const compress::CompressInterface* comp, MemManager* mm) : fCompressor(comp), fMM(mm->clone())
  {
  }

  int write(const std::string& fname, const char* buf, size_t sz)
  {
    if (sz == 0)
      return 0;

    int fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (UNLIKELY(fd < 0))
      return errno;

    const char* tmpbuf;
    if (fCompressor)
    {
      auto len = fCompressor->maxCompressedSize(sz);
      checkBuffer(len);
      fCompressor->compress(buf, sz, fTmpBuf.data(), &len);
      tmpbuf = fTmpBuf.data();
      sz = len;
    }
    else
    {
      tmpbuf = buf;
    }

    auto to_write = sz;
    int ret = 0;
    while (to_write > 0)
    {
      auto r = ::write(fd, tmpbuf + sz - to_write, to_write);
      if (UNLIKELY(r < 0))
      {
        if (errno == EAGAIN)
          continue;

        ret = errno;
        close(fd);
        return ret;
      }
      assert(size_t(r) <= to_write);
      to_write -= r;
    }

    close(fd);
    return ret;
  }

  int read(const std::string& fname, std::vector<char>& buf)
  {
    int fd = open(fname.c_str(), O_RDONLY);
    if (UNLIKELY(fd < 0))
      return errno;

    struct stat st
    {
    };
    fstat(fd, &st);
    size_t sz = st.st_size;
    std::vector<char>* tmpbuf;
    if (fCompressor)
    {
      tmpbuf = &fTmpBuf;
      checkBuffer(sz);
    }
    else
    {
      tmpbuf = &buf;
      buf.resize(sz);
    }

    auto to_read = sz;
    int ret = 0;
    while (to_read > 0)
    {
      auto r = ::read(fd, tmpbuf->data() + sz - to_read, to_read);
      if (UNLIKELY(r < 0))
      {
        if (errno == EAGAIN)
          continue;

        ret = errno;
        close(fd);
        return ret;
      }

      assert(size_t(r) <= to_read);
      to_read -= r;
    }

    if (fCompressor)
    {
      size_t len;
      if (!fCompressor->getUncompressedSize(tmpbuf->data(), sz, &len))
      {
        ret = EPROTO;
        close(fd);
        return ret;
      }

      buf.resize(len);
      fCompressor->uncompress(tmpbuf->data(), sz, buf.data(), &len);
    }

    close(fd);
    return ret;
  }

  size_t size() const
  {
    return fTmpBuf.size();
  }

 private:
  void checkBuffer(size_t len)
  {
    if (fTmpBuf.size() < len)
    {
      size_t newtmpsz = (len + 8191) / 8192 * 8192;
      std::vector<char> tmpvec(newtmpsz);
      fMM->acquire(newtmpsz - fTmpBuf.size());
      fTmpBuf.swap(tmpvec);
    }
  }

 private:
  const compress::CompressInterface* fCompressor;
  std::unique_ptr<MemManager> fMM;
  std::vector<char> fTmpBuf;
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
   * @param compressor          pointer to CompressInterface impl or nullptr
   */
  RowGroupStorage(const std::string& tmpDir, RowGroup* rowGroupOut, size_t maxRows,
                  joblist::ResourceManager* rm = nullptr, boost::shared_ptr<int64_t> sessLimit = {},
                  bool wait = false, bool strict = false, compress::CompressInterface* compressor = nullptr)
   : fRowGroupOut(rowGroupOut)
   , fMaxRows(maxRows)
   , fRGDatas()
   , fUniqId(this)
   , fTmpDir(tmpDir)
   , fCompressor(compressor)
  {
    if (rm)
    {
      fMM.reset(new RMMemManager(rm, sessLimit, wait, strict));
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
      fMM.reset(new MemManager());
      fLRU = std::unique_ptr<LRUIface>(new LRUIface());
    }

    fDumper.reset(new Dumper(fCompressor, fMM.get()));

    auto* curRG = new RGData(*fRowGroupOut, fMaxRows);
    fRowGroupOut->setData(curRG);
    fRowGroupOut->resetRowGroup(0);
    fRGDatas.emplace_back(curRG);
    fMM->acquire(rgDataSize(*curRG));
  }

  // Doesn't detach fRowGroupOut: the RowGroup may already be gone when a storage is destroyed
  // (the step owning it can destroy it before the aggregation). Call detachRowGroup() first
  // when the RowGroup outlives this storage.
  ~RowGroupStorage() = default;

  /** @brief Detach fRowGroupOut if it points at one of this storage's RGDatas.
   *
   *    fRowGroupOut is a cursor shared by every storage of the aggregation (generations,
   *    clones, the aggregation itself), so it can be left on any of their RGDatas. It must
   *    never be left on a freed one: a later getRowCount()/setDBRoot()/getSizeWithStrings()
   *    on it would read or write freed memory.
   */
  void detachRowGroup() const
  {
    for (const auto& rgd : fRGDatas)
      forgetRG(rgd.get());
  }

  ssize_t getAproxRGSize() const
  {
    // fRowGroupOut points at a live RGData or at none (then this is the size without strings)
    return fRowGroupOut->getSizeWithStrings(fMaxRows);
  }

  /** @brief Take away RGDatas from another RowGroupStorage
   *
   *    If some of the RGDatas is not in the memory do not load them,
   *    just rename dump file to match new RowGroupStorage pattern
   *
   * @param o RowGroupStorage to take from
   */
  void append(std::unique_ptr<RowGroupStorage> o)
  {
    return append(o.get());
  }
  void append(RowGroupStorage* o)
  {
    std::unique_ptr<RGData> rgd;
    std::string ofname;
    while (o->takeNextRGData(rgd, ofname))
    {
      fRGDatas.push_back(std::move(rgd));
      uint64_t rgid = fRGDatas.size() - 1;
      if (fRGDatas[rgid])
      {
        int64_t memSz = rgDataSize(*fRGDatas[rgid]);
        if (!fMM->acquire(memSz))
        {
          throw logging::IDBExcept(
              logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_AGGREGATION_TOO_BIG),
              logging::ERR_AGGREGATION_TOO_BIG);
        }

        if (fMM->getFree() < memSz * 2)
        {
          saveRG(rgid);
          fRGDatas[rgid].reset();
        }
        else
          fLRU->add(rgid);
      }
      else
      {
        auto r = rename(ofname.c_str(), makeRGFilename(rgid).c_str());
        if (UNLIKELY(r < 0))
        {
          throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                       logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errno)),
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

      fMM->release(rgDataSize(*rgdata));
      fLRU->remove(rgid);
      fRowGroupOut->setData(rgdata.get());
      if (fRowGroupOut->getRowCount() == 0)
      {
        forgetRG(rgdata.get());
        continue;
      }
      // the caller owns it now, and detaches fRowGroupOut before freeing it
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

  /** @brief Get the row at the specified position, skipping nonexistent rows
   *
   * @param idx(in, out) index (from 0) of the row, set to the actual index
   * @param row(out)     resulting row
   */
  bool getRowForFinalization(uint64_t& idx, Row& row)
  {
    auto [rgid, rid] = rowIdxToGidRid(idx, fMaxRows);
    while (rgid < fRGDatas.size())
    {
      if (UNLIKELY(!fRGDatas[rgid]))
      {
        loadRG(rgid);
      }
      fRowGroupOut->setData(fRGDatas[rgid].get());
      if (UNLIKELY(rid >= fRowGroupOut->getRowCount()))
      {
        ++rgid;
        rid = 0;
        continue;
      }
      fRGDatas[rgid]->getRow(rid, &row);
      idx = rowGidRidToIdx(rgid, rid, fMaxRows);
      return true;
    }
    return false;
  }

  size_t getRowsPerRG() const
  {
    return fMaxRows;
  }

  void dropRGData(uint64_t rgid)
  {
    if (UNLIKELY(!fRGDatas[rgid]))
    {
      return;
    }
    fMM->release(rgDataSize(*fRGDatas[rgid]));
    forgetRG(fRGDatas[rgid].get());
    fRGDatas[rgid].reset();
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
      // estimate from the RGData fRowGroupOut points at, live or none (then without strings)
      auto memSz = fRowGroupOut->getSizeWithStrings(fMaxRows);
      if (!fMM->acquire(memSz))
      {
        throw logging::IDBExcept(
            logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_AGGREGATION_TOO_BIG),
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

    while (rgid >= fRGDatas.size())
    {
      // estimate from the RGData fRowGroupOut points at, live or none (then without strings)
      int64_t memSz = fRowGroupOut->getSizeWithStrings(fMaxRows);
      if (!fMM->acquire(memSz))
      {
        throw logging::IDBExcept(
            logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_AGGREGATION_TOO_BIG),
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

    idbassert(idx % fMaxRows == fRowGroupOut->getRowCount());
    fRowGroupOut->getRow(fRowGroupOut->getRowCount(), &row);
    fRowGroupOut->incRowCount();
  }

  /** @brief Dump the oldest RGData to disk, freeing memory
   *
   * @returns true if any RGData was dumped
   */
  bool dump()
  {
    // Always leave at least 2 RG as this is the minimum size of the hashmap
    constexpr size_t MIN_INMEMORY = 2;
    if (fLRU->size() <= MIN_INMEMORY)
    {
      return false;
    }
    size_t moved = 0;
    auto it = fLRU->rbegin();
    while (LIKELY(it != fLRU->rend()))
    {
      if (fLRU->size() <= MIN_INMEMORY)
        return false;
      uint64_t rgid = *it;
      if (UNLIKELY(!fRGDatas[rgid]))
      {
        ++it;
        fLRU->remove(rgid);
        continue;
      }
      fRowGroupOut->setData(fRGDatas[rgid].get());
      if (moved <= MIN_INMEMORY && fRowGroupOut->getRowCount() < fMaxRows)
      {
        ++it;
        ++moved;
        fLRU->add(rgid);
        continue;
      }
      saveRG(rgid);
      fLRU->remove(rgid);
      fRGDatas[rgid].reset();
      return true;
    }
    return false;
  }

  /** @brief Dump all data, clear state and start over */
  void startNewGeneration()
  {
    dumpAll();
    fLRU->clear();
    fMM->release();
    clearRGDatas();

    // we need at least one RGData so create it right now
    auto* curRG = new RGData(*fRowGroupOut, fMaxRows);
    fRowGroupOut->setData(curRG);
    fRowGroupOut->resetRowGroup(0);
    fRGDatas.emplace_back(curRG);
    auto memSz = rgDataSize(*curRG);
    if (!fMM->acquire(memSz))
    {
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_AGGREGATION_TOO_BIG),
                               logging::ERR_AGGREGATION_TOO_BIG);
    }
    fCurRgid = 0;
    ++fGeneration;
  }

  /** @brief Save "finalized" bitmap to disk for future use */
  void dumpFinalizedInfo() const
  {
    auto fname = makeFinalizedFilename();
    int fd = open(fname.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (UNLIKELY(fd < 0))
    {
      throw logging::IDBExcept(
          logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errno)),
          logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    uint64_t sz = fRGDatas.size();
    uint64_t finsz = fFinalizedRows.size();

    int errNo;
    if ((errNo = writeData(fd, (const char*)&sz, sizeof(sz))) != 0 ||
        (errNo = writeData(fd, (const char*)&finsz, sizeof(finsz)) != 0) ||
        (errNo = writeData(fd, (const char*)fFinalizedRows.data(), finsz * sizeof(uint64_t)) != 0))
    {
      close(fd);
      unlink(fname.c_str());
      throw logging::IDBExcept(
          logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errNo)),
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
      throw logging::IDBExcept(
          logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errno)),
          logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    uint64_t sz;
    uint64_t finsz;
    int errNo;
    if ((errNo = readData(fd, (char*)&sz, sizeof(sz)) != 0) ||
        (errNo = readData(fd, (char*)&finsz, sizeof(finsz)) != 0))
    {
      close(fd);
      unlink(fname.c_str());
      throw logging::IDBExcept(
          logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errNo)),
          logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    fRGDatas.resize(sz);
    fFinalizedRows.resize(finsz);
    if ((errNo = readData(fd, (char*)fFinalizedRows.data(), finsz * sizeof(uint64_t))) != 0)
    {
      close(fd);
      unlink(fname.c_str());
      throw logging::IDBExcept(
          logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errNo)),
          logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    close(fd);
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
    ret->clearRGDatas();
    ret->fLRU.reset(fLRU->clone());
    ret->fMM.reset(fMM->clone());
    ret->fUniqId = fUniqId;
    ret->fGeneration = gen;
    ret->fCompressor = fCompressor;
    ret->fDumper.reset(new Dumper(fCompressor, fMM.get()));
    ret->fLog = fLog;
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

  void getTmpFilePrefixes(std::vector<std::string>& prefixes) const
  {
    char buf[PATH_MAX];
    snprintf(buf, sizeof(buf), "Agg-p%u-t%p-rg", getpid(), fUniqId);
    prefixes.emplace_back(buf);

    snprintf(buf, sizeof(buf), "AggFin-p%u-t%p-g", getpid(), fUniqId);
    prefixes.emplace_back(buf);
  }

 private:
  /** @brief getNextRGData() for a storage that hands its RGDatas over to another one.
   *
   *    Detaches fRowGroupOut from the returned RGData: the receiving storage detaches only its
   *    own RowGroup when it frees it. Also detaches if loading it fails, as unwinding frees it.
   */
  bool takeNextRGData(std::unique_ptr<RGData>& rgdata, std::string& fname)
  {
    try
    {
      bool more = getNextRGData(rgdata, fname);
      forgetRG(rgdata.get());
      return more;
    }
    catch (...)
    {
      forgetRG(rgdata.get());
      throw;
    }
  }

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
          // every row is finalized: drop it. rgdata is freed by the next iteration or by the caller
          fMM->release(rgDataSize(*rgdata));
          forgetRG(rgdata.get());
          fLRU->remove(rgid);
          unlink(makeRGFilename(rgid).c_str());
          continue;
        }

        fRowGroupOut->setData(rgdata.get());
        fRowGroupOut->setRowCount(pos);
      }

      if (rgdata)
      {
        fMM->release(rgDataSize(*rgdata));
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
    memmove(data + to * rowsz, data + from * rowsz, numRows * rowsz);
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

    std::vector<char> data;
    int errNo;
    if ((errNo = fDumper->read(fname, data)) != 0)
    {
      unlink(fname.c_str());
      throw logging::IDBExcept(
          logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errNo)),
          logging::ERR_DISKAGG_FILEIO_ERROR);
    }

    if (fLog)
    {
      ++fLog->fRGsRead;
      fLog->fBytesRead += data.size();
    }
    messageqcpp::ByteStream bs(reinterpret_cast<uint8_t*>(data.data()), data.size());

    if (unlinkDump)
      unlink(fname.c_str());
    rgdata.reset(new RGData());
    rgdata->deserialize(bs, fRowGroupOut->getDataSize(fMaxRows));

    fRowGroupOut->setData(rgdata.get());
    auto memSz = rgDataSize(*rgdata);

    if (!fMM->acquire(memSz))
    {
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_AGGREGATION_TOO_BIG),
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
    fMM->release(rgDataSize(*rgdata));

    try
    {
      saveRG(rgid, rgdata.get());
    }
    catch (...)
    {
      forgetRG(rgdata.get());  // rgdata is freed by unwinding
      throw;
    }
    forgetRG(rgdata.get());  // rgdata is freed on return
  }

  /** @brief Dump RGData to disk.
   *
   * @param rgid(in)   RGData ID
   * @param rgdata(in) pointer to RGData itself
   */
  void saveRG(uint64_t rgid, RGData* rgdata) const
  {
    messageqcpp::ByteStream bs;
    fRowGroupOut->setData(rgdata);
    rgdata->serialize(bs, fRowGroupOut->getDataSize());

    int errNo;
    if ((errNo = fDumper->write(makeRGFilename(rgid), (char*)bs.buf(), bs.length())) != 0)
    {
      throw logging::IDBExcept(
          logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errNo)),
          logging::ERR_DISKAGG_FILEIO_ERROR);
    }
    if (fLog)
    {
      ++fLog->fRGsWritten;
      fLog->fBytesWritten += bs.length();
    }
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

  /** @brief Memory an RGData of this storage holds: the fixed-size rows plus its strings.
   *
   *    Reads rgd itself rather than fRowGroupOut, which may point at another RGData.
   */
  int64_t rgDataSize(const RGData& rgd) const
  {
    return fRowGroupOut->getDataSize(fMaxRows) + (rgd.strings ? rgd.strings->getSize() : 0);
  }

  /** @brief Detach fRowGroupOut if it points at rgd, which is about to be freed. */
  void forgetRG(const RGData* rgd) const
  {
    if (rgd && fRowGroupOut->getRGData() == rgd)
      fRowGroupOut->detachData();
  }

  /** @brief Free every RGData in memory, detaching fRowGroupOut from them first. */
  void clearRGDatas()
  {
    detachRowGroup();
    fRGDatas.clear();
  }

  /** @brief Create dump filename.
   */
  std::string makeRGFilename(uint64_t rgid) const
  {
    char buf[PATH_MAX];
    snprintf(buf, sizeof(buf), "%s/Agg-p%u-t%p-rg%lu-g%u", fTmpDir.c_str(), getpid(), fUniqId, rgid,
             fGeneration);
    return buf;
  }

  std::string makeFinalizedFilename() const
  {
    char fname[PATH_MAX];
    snprintf(fname, sizeof(fname), "%s/AggFin-p%u-t%p-g%u", fTmpDir.c_str(), getpid(), fUniqId, fGeneration);
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
  compress::CompressInterface* fCompressor;
  std::unique_ptr<Dumper> fDumper;
  DiskAggLog* fLog{nullptr};  // owned by RowAggStorage
};

/** @brief Internal data for the hashmap */
struct RowPosHash
{
  uint64_t hash;  ///< Row hash
  uint64_t idx;   ///< index in the RowGroupStorage
};

/***
 * @ brief Storage for row positions and hashes memory management
 *         and the ability to save on disk
 */
class RowPosHashStorage
{
 public:
  /***
   * @brief Default constructor
   *
   * The internal data is stored as a plain vector of row pos & hash
   *
   * @param tmpDir(in)      directory for tmp data
   * @param size(in)        maximum size of storage (NB: without the padding)
   * @param rm              ResourceManager to use
   * @param sessLimit       session memory limit
   * @param enableDiskAgg   is disk aggregation enabled?
   */
  RowPosHashStorage(const std::string& tmpDir, size_t size, joblist::ResourceManager* rm,
                    boost::shared_ptr<int64_t> sessLimit, bool enableDiskAgg,
                    compress::CompressInterface* compressor)
   : fUniqId(this), fTmpDir(tmpDir), fCompressor(compressor)
  {
    if (rm)
      fMM.reset(new RMMemManager(rm, sessLimit, !enableDiskAgg, !enableDiskAgg));
    else
      fMM.reset(new MemManager());

    fDumper.reset(new Dumper(fCompressor, fMM.get()));

    if (size != 0)
      init(size);
  }

  /***
   * @brief Get the row position and hash at the idx
   *
   * @param idx(in) index (from 0) of the row
   */
  RowPosHash& get(uint64_t idx)
  {
    return fPosHashes[idx];
  }

  /***
   * @brief Store the row position and hash at the idx
   *
   * @param idx(in)     index of the row
   * @param rowData(in) position and hash of the row
   */
  void set(uint64_t idx, const RowPosHash& pos)
  {
    memcpy(&fPosHashes[idx], &pos, sizeof(pos));
  }

  /*** @return Size of data */
  ssize_t memUsage() const
  {
    return fMM->getUsed();
  }

  /*** @brief Unregister used memory */
  void releaseMemory()
  {
    fMM->release();
  }

  /***
   * @brief Move row positions & hashes inside [insIdx, startIdx] up by 1
   *
   * @param startIdx(in) last index to move
   * @param insIdx(in)   first index to move
   */
  void shiftUp(uint64_t startIdx, uint64_t insIdx)
  {
    memmove(&fPosHashes[insIdx + 1], &fPosHashes[insIdx],
            (startIdx - insIdx) * sizeof(decltype(fPosHashes)::value_type));
  }

  /***
   * @brief Create new storage with the same MemManager & uniq ID
   * @param size(in) maximum size of storage without padding
   * @param gen(in)  new generation
   */
  RowPosHashStoragePtr clone(size_t size, uint16_t gen, bool loadDump = false) const
  {
    RowPosHashStoragePtr cloned;

    cloned.reset(new RowPosHashStorage());
    cloned->fMM.reset(fMM->clone());
    cloned->fTmpDir = fTmpDir;
    cloned->init(size);
    cloned->fUniqId = fUniqId;
    cloned->fGeneration = gen;
    cloned->fCompressor = fCompressor;
    cloned->fDumper.reset(new Dumper(fCompressor, cloned->fMM.get()));
    if (loadDump)
      cloned->load();
    return cloned;
  }

  /*** @brief Remove dump file */
  void cleanup() const
  {
    unlink(makeDumpName().c_str());
  }

  /***
   * @brief Dump all data, clear state, and start over with the new generation
   */
  void startNewGeneration()
  {
    dump();
    ++fGeneration;
    fPosHashes.clear();
    fMM->release();
  }

  void dump()
  {
    int errNo;
    size_t sz = fPosHashes.size() * sizeof(decltype(fPosHashes)::value_type);
    if ((errNo = fDumper->write(makeDumpName(), (char*)fPosHashes.data(), sz)) != 0)
    {
      throw logging::IDBExcept(
          logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errNo)),
          logging::ERR_DISKAGG_FILEIO_ERROR);
    }
  }

 private:
  RowPosHashStorage() = default;

  void init(size_t size)
  {
    auto bkts = size + 0xFFUL;
    if (!fMM->acquire(bkts * sizeof(decltype(fPosHashes)::value_type)))
    {
      throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_AGGREGATION_TOO_BIG),
                               logging::ERR_AGGREGATION_TOO_BIG);
    }
    fPosHashes.resize(bkts);
  }

  std::string makeDumpName() const
  {
    char fname[PATH_MAX];
    snprintf(fname, sizeof(fname), "%s/Agg-PosHash-p%u-t%p-g%u", fTmpDir.c_str(), getpid(), fUniqId,
             fGeneration);
    return fname;
  }

  void load()
  {
    int errNo;
    std::vector<char> data;
    if ((errNo = fDumper->read(makeDumpName(), data)) != 0)
    {
      throw logging::IDBExcept(
          logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errNo)),
          logging::ERR_DISKAGG_FILEIO_ERROR);
    }

    fPosHashes.resize(data.size() / sizeof(decltype(fPosHashes)::value_type));
    memcpy(fPosHashes.data(), data.data(), data.size());
  }

 private:
  std::unique_ptr<MemManager> fMM;
  std::vector<RowPosHash> fPosHashes;
  uint16_t fGeneration{0};  ///< current aggregation generation
  void* fUniqId;            ///< uniq ID to make an uniq dump filename
  std::string fTmpDir;
  compress::CompressInterface* fCompressor;
  std::unique_ptr<Dumper> fDumper;
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
RowAggStorage::RowAggStorage(const std::string& tmpDir, RowGroup* rowGroupOut, RowGroup* keysRowGroup,
                             uint32_t keyCount, joblist::ResourceManager* rm,
                             boost::shared_ptr<int64_t> sessLimit, bool enabledDiskAgg, bool allowGenerations,
                             compress::CompressInterface* compressor)
 : fMaxRows(getMaxRows(enabledDiskAgg))
 , fExtKeys(rowGroupOut != keysRowGroup)
 , fLastKeyCol(keyCount - 1)
 , fUniqId(this)
 , fAllowGenerations(allowGenerations)
 , fEnabledDiskAggregation(enabledDiskAgg)
 , fCompressor(compressor)
 , fTmpDir(tmpDir)
 , fRowGroupOut(rowGroupOut)
 , fKeysRowGroup(keysRowGroup)
 , fRD()
 , fRandGen(fRD())
 , fRandDistr(0, 100)
{
  char suffix[PATH_MAX];
  snprintf(suffix, sizeof(suffix), "/p%u-t%p/", getpid(), this);
  fTmpDir.append(suffix);
  if (enabledDiskAgg)
    boost::filesystem::create_directories(fTmpDir);

  if (rm)
  {
    fMM.reset(new RMMemManager(rm, sessLimit, !enabledDiskAgg, !enabledDiskAgg));
    fNumOfInputRGPerThread = std::max<uint32_t>(1, rm->aggNumRowGroups());
  }
  else
  {
    fMM.reset(new MemManager());
    fNumOfInputRGPerThread = 1;
  }
  fStorage.reset(new RowGroupStorage(fTmpDir, rowGroupOut, 1, rm, sessLimit, !enabledDiskAgg, !enabledDiskAgg,
                                     fCompressor.get()));
  if (fExtKeys)
  {
    fRealKeysStorage.reset(new RowGroupStorage(fTmpDir, keysRowGroup, 1, rm, sessLimit, !enabledDiskAgg,
                                               !enabledDiskAgg, fCompressor.get()));
    fKeysStorage = fRealKeysStorage.get();
  }
  else
  {
    fKeysStorage = fStorage.get();
  }
  fKeysStorage->initRow(fKeyRow);
  fGens.emplace_back(new Data);
  fCurData = fGens.back().get();
  fCurData->fHashes.reset(
      new RowPosHashStorage(fTmpDir, 0, rm, sessLimit, fEnabledDiskAggregation, fCompressor.get()));
}

RowAggStorage::~RowAggStorage()
{
  cleanupAll();
}

void DiskAggLog::log(const std::string& msg) const
{
  logging::Message::Args args;
  args.add("Disk-based aggregation: " + msg);
  logging::Message m(6);  // "%1%"
  m.format(args);
  logging::MessageLog ml(logging::LoggingID(16, fSessionId));
  ml.logInfoMessage(m);
}

void DiskAggLog::progress(const char* phase)
{
  using namespace std::chrono;
  const int64_t now = steady_clock::now().time_since_epoch().count();
  int64_t last = fLastProgress.load(std::memory_order_relaxed);
  if (now - last < duration_cast<steady_clock::duration>(PROGRESS_INTERVAL).count() ||
      !fLastProgress.compare_exchange_strong(last, now))
    return;  // logged recently, or another bucket is logging right now

  std::ostringstream oss;
  oss << phase << ", " << duration_cast<seconds>(steady_clock::now() - fStart).count() << " s: " << fSpilling
      << " buckets spilled, " << fGenerations << " new generations, written " << fRGsWritten
      << " rowgroups / " << (fBytesWritten >> 20) << " MiB, read back " << fRGsRead << " / "
      << (fBytesRead >> 20) << " MiB";
  if (fFinalizing)
    oss << ", final merge " << fFinalized << " of " << fFinalizing << " buckets done, " << fRowsMerged
        << " rows merged";
  log(oss.str());
}

void RowAggStorage::setDiskAggLog(DiskAggLogPtr log)
{
  fLog = std::move(log);
  if (fStorage)
    fStorage->fLog = fLog.get();
  if (fRealKeysStorage)
    fRealKeysStorage->fLog = fLog.get();
}

void RowAggStorage::noteSpill()
{
  if (!fLog)
    return;
  if (!fSpilled)
  {
    fSpilled = true;
    if (fLog->fSpilling++ == 0)
    {
      std::ostringstream oss;
      oss << "started spilling to " << fTmpDir << ", free memory " << (fMM->getFree() >> 20) << " MiB of "
          << (fMM->getConfigured() >> 20) << " MiB";
      fLog->log(oss.str());
    }
  }
  fLog->progress("aggregating");
}

bool RowAggStorage::getTargetRow(const Row& row, Row& rowOut)
{
  uint64_t hash = hashRow(row, fLastKeyCol);
  return getTargetRow(row, hash, rowOut);
}

bool RowAggStorage::getTargetRow(const Row& row, uint64_t hash, Row& rowOut)
{
  if (UNLIKELY(!fInitialized))
  {
    fInitialized = true;
    fStorage.reset(new RowGroupStorage(fTmpDir, fRowGroupOut, fMaxRows, fMM->getResourceManaged(),
                                       fMM->getSessionLimit(), !fEnabledDiskAggregation,
                                       !fEnabledDiskAggregation, fCompressor.get()));
    if (fExtKeys)
    {
      fRealKeysStorage.reset(new RowGroupStorage(fTmpDir, fKeysRowGroup, fMaxRows, fMM->getResourceManaged(),
                                                 fMM->getSessionLimit(), !fEnabledDiskAggregation,
                                                 !fEnabledDiskAggregation, fCompressor.get()));
      fKeysStorage = fRealKeysStorage.get();
    }
    else
    {
      fKeysStorage = fStorage.get();
    }
    setDiskAggLog(fLog);
    fKeysStorage->initRow(fKeyRow);
    reserve(fMaxRows);
  }
  else if (UNLIKELY(fCurData->fSize >= fCurData->fMaxSize))
  {
    increaseSize();
  }
  auto [info, idx] = rowHashToIdx(hash);

  nextWhileLess(info, idx);

  while (info == fCurData->fInfo[idx])
  {
    auto& pos = fCurData->fHashes->get(idx);
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

  if (!fEnabledDiskAggregation && fGeneration != 0)
  {
    // there are several generations here, so let's try to find suitable row in them
    uint16_t gen = fGeneration - 1;
    do
    {
      auto* genData = fGens[gen].get();
      auto [ginfo, gidx] = rowHashToIdx(hash, genData->fMask, genData->hashMultiplier_, genData->fInfoInc,
                                        genData->fInfoHashShift);
      nextWhileLess(ginfo, gidx, genData);

      while (ginfo == genData->fInfo[gidx])
      {
        auto& pos = genData->fHashes->get(idx);
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
        next(ginfo, gidx, genData);
      }
    } while (gen-- != 0);
  }

  const auto ins_idx = idx;
  const auto ins_info = info;
  if (UNLIKELY(ins_info + fCurData->fInfoInc > 0xFF))
  {
    fCurData->fMaxSize = 0;
  }

  while (fCurData->fInfo[idx] != 0)
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
  fCurData->fHashes->set(ins_idx, pos);
  fCurData->fInfo[ins_idx] = static_cast<uint8_t>(ins_info);
  ++fCurData->fSize;
  return true;
}

void RowAggStorage::dump()
{
  if (!fEnabledDiskAggregation)
    return;

  constexpr int64_t freeMemLimit = 50LL * 1024LL * 1024LL;

  // Keep room for one more input batch: fNumOfInputRGPerThread rowgroups whose rows may all be new
  // groups, each needing an output row and a hash bucket. Acquires are not strict with disk
  // aggregation on, so this loop is what keeps the aggregation within its memory limit.
  const int64_t leaveFree =
      std::max<int64_t>(freeMemLimit, fNumOfInputRGPerThread *
                                          (fRowGroupOut->getDataSize(fMaxRows) + fMaxRows * getBucketSize()));
  uint64_t freeAttempts{0};
  int64_t freeMem = 0;
  bool spilled = false;
  while (true)
  {
    ++freeAttempts;
    freeMem = fMM->getFree();
    if (freeMem > leaveFree)
      break;

    bool success = fStorage->dump();
    if (fExtKeys)
      success |= fKeysStorage->dump();

    if (!success)
      break;
    spilled = true;
  }
  if (spilled)
    noteSpill();

  int64_t totalMem = fMM->getConfigured();
  // If the generations are allowed and there are less than half of
  // rowgroups in memory, then we start a new generation
  if (fAllowGenerations && fStorage->fLRU->size() < fStorage->fRGDatas.size() / 2 &&
      fStorage->fRGDatas.size() > 10)
  {
    startNewGeneration();
  }
  else if (fAllowGenerations && freeMem < totalMem / 10 * 3 && fRandDistr(fRandGen) < 30)
  {
    startNewGeneration();
  }
  else if (fAllowGenerations && fMM->getFree() < freeMemLimit)
  {
    startNewGeneration();
  }
  else if (!fAllowGenerations && freeMem < 0 && freeAttempts == 1)
  {
    // safety guard so aggregation couldn't eat all available memory
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_TOO_BIG),
                             logging::ERR_DISKAGG_TOO_BIG);
  }
}

void RowAggStorage::append(RowAggStorage& other)
{
  // we don't need neither key rows storage nor any internal data anymore
  // neither in this RowAggStorage nor in the other
  cleanup();
  freeData();
  if (other.fGeneration == 0 || !fEnabledDiskAggregation)
  {
    // even if there is several generations in the other RowAggStorage
    // in case of in-memory-only aggregation they all share the same RowStorage
    other.cleanup();
    other.freeData();
    fStorage->append(std::move(other.fStorage));
    return;
  }

  // iff other RowAggStorage has several generations, sequential load and append
  // them all
  // the only needed data is the aggregated RowStorage itself
  auto gen = other.fGeneration;
  while (true)
  {
    fStorage->append(other.fStorage.get());
    other.cleanup();
    if (gen == 0)
      break;
    --gen;
    other.fGeneration = gen;
    other.fStorage.reset(other.fStorage->clone(gen));
  }
}

std::unique_ptr<RGData> RowAggStorage::getNextRGData()
{
  if (!fStorage)
  {
    return {};
  }
  cleanup();
  freeData();
  return fStorage->getNextRGData();
}

void RowAggStorage::freeData()
{
  for (auto& data : fGens)
  {
    data->fHashes.reset();
    if (data->fInfo)
    {
      const size_t memSz = calcSizeWithBuffer(data->fMask + 1);
      fMM->release(memSz);
      data->fInfo.reset();
    }
  }
  fGens.clear();
  fCurData = nullptr;
}

void RowAggStorage::shiftUp(size_t startIdx, size_t insIdx)
{
  auto idx = startIdx;
  while (idx != insIdx)
  {
    fCurData->fInfo[idx] = static_cast<uint8_t>(fCurData->fInfo[idx - 1] + fCurData->fInfoInc);
    if (UNLIKELY(fCurData->fInfo[idx] + fCurData->fInfoInc > 0xFF))
    {
      fCurData->fMaxSize = 0;
    }
    --idx;
  }
  fCurData->fHashes->shiftUp(startIdx, insIdx);
}

void RowAggStorage::increaseSize()
{
  if (fCurData->fMask == 0)
  {
    initData(INIT_SIZE, fCurData->fHashes.get());
  }

  const auto maxSize = calcMaxSize(fCurData->fMask + 1);
  if (fCurData->fSize < maxSize && tryIncreaseInfo())
    return;

  constexpr size_t maxMaskMultiplierWoRehashing = 1U << (INIT_INFO_INC - 1);
  // We don't check for the overflow here b/c it is impractical to has fSize so that multiplication
  // overflows.
  if (fCurData->fSize * maxMaskMultiplierWoRehashing < calcMaxSize(fCurData->fMask + 1))
  {
    // something strange happens...
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_OVERFLOW2),
                             logging::ERR_DISKAGG_OVERFLOW2);
  }

  auto freeMem = fMM->getFree();
  if (fEnabledDiskAggregation ||
      freeMem > (fMM->getUsed() + fCurData->fHashes->memUsage() + fStorage->getAproxRGSize()) * 2)
  {
    if (fCurData->fSize * 2 < maxSize)
    {
      // we have to resize, even though there would still be plenty of space left!
      // Try to rehash instead. Delete freed memory so we don't steadyily increase mem in case
      // we have to rehash a few times
      // adding an *even* number, so that the multiplier will always stay odd. This is necessary
      // so that the hash stays a mixing function (and thus doesn't have any information loss).
      fCurData->hashMultiplier_ += 0xc4ceb9fe1a85ec54;
      rehashPowerOfTwo(fCurData->fMask + 1);
    }
    else
    {
      rehashPowerOfTwo((fCurData->fMask + 1) * 2);
    }
  }
  else if (fGeneration < MAX_INMEMORY_GENS - 1)
  {
    startNewGeneration();
  }
  else
  {
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_AGGREGATION_TOO_BIG),
                             logging::ERR_AGGREGATION_TOO_BIG);
  }
}

bool RowAggStorage::tryIncreaseInfo()
{
  if (fCurData->fInfoInc <= 2)
    return false;

  fCurData->fInfoInc = static_cast<uint8_t>(fCurData->fInfoInc >> 1U);
  ++fCurData->fInfoHashShift;
  const auto elems = calcSizeWithBuffer(fCurData->fMask + 1);
  for (size_t i = 0; i < elems; i += 8)
  {
    uint64_t val;
    memcpy(&val, fCurData->fInfo.get() + i, sizeof(val));
    val = (val >> 1U) & 0x7f7f7f7f7f7f7f7fULL;
    memcpy(fCurData->fInfo.get() + i, &val, sizeof(val));
  }

  fCurData->fInfo[elems] = 1;
  fCurData->fMaxSize = calcMaxSize(fCurData->fMask + 1);
  return true;
}

void RowAggStorage::rehashPowerOfTwo(size_t elems)
{
  const size_t oldSz = calcSizeWithBuffer(fCurData->fMask + 1);
  auto oldInfo = std::move(fCurData->fInfo);
  auto oldHashes = std::move(fCurData->fHashes);
  fMM->release(calcBytes(oldSz));

  initData(elems, oldHashes.get());
  oldHashes->releaseMemory();

  if (oldSz > 1)
  {
    for (size_t i = 0; i < oldSz; ++i)
    {
      if (UNLIKELY(oldInfo[i] != 0))
      {
        insertSwap(i, oldHashes.get());
      }
    }
  }
}

void RowAggStorage::insertSwap(size_t oldIdx, RowPosHashStorage* oldHashes)
{
  if (fCurData->fMaxSize == 0 && !tryIncreaseInfo())
  {
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_OVERFLOW1),
                             logging::ERR_DISKAGG_OVERFLOW1);
  }

  auto pos = oldHashes->get(oldIdx);
  auto [info, idx] = rowHashToIdx(pos.hash);

  while (info <= fCurData->fInfo[idx])
  {
    ++idx;
    info += fCurData->fInfoInc;
  }

  // don't need to compare rows here - they differ by definition
  const auto ins_idx = idx;
  const auto ins_info = static_cast<uint8_t>(info);
  if (UNLIKELY(ins_info + fCurData->fInfoInc > 0xFF))
    fCurData->fMaxSize = 0;

  while (fCurData->fInfo[idx] != 0)
  {
    next(info, idx);
  }

  if (idx != ins_idx)
    shiftUp(idx, ins_idx);

  fCurData->fHashes->set(ins_idx, pos);
  fCurData->fInfo[ins_idx] = ins_info;
  ++fCurData->fSize;
}

void RowAggStorage::initData(size_t elems, const RowPosHashStorage* oldHashes)
{
  fCurData->fSize = 0;
  fCurData->fMask = elems - 1;
  fCurData->fMaxSize = calcMaxSize(elems);

  const auto sizeWithBuffer = calcSizeWithBuffer(elems, fCurData->fMaxSize);
  const auto bytes = calcBytes(sizeWithBuffer);

  if (!fMM->acquire(bytes))
  {
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_AGGREGATION_TOO_BIG),
                             logging::ERR_AGGREGATION_TOO_BIG);
  }
  fCurData->fHashes = oldHashes->clone(elems, fGeneration);
  fCurData->fInfo.reset(new uint8_t[bytes]());
  fCurData->fInfo[sizeWithBuffer] = 1;
  fCurData->fInfoInc = INIT_INFO_INC;
  fCurData->fInfoHashShift = INIT_INFO_HASH_SHIFT;
}

void RowAggStorage::reserve(size_t c)
{
  auto const minElementsAllowed = (std::max)(c, fCurData->fSize);
  auto newSize = INIT_SIZE;
  while (calcMaxSize(newSize) < minElementsAllowed && newSize != 0)
  {
    newSize *= 2;
  }
  if (UNLIKELY(newSize == 0))
  {
    throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_ERROR),
                             logging::ERR_DISKAGG_ERROR);
  }

  // only actually do anything when the new size is bigger than the old one. This prevents to
  // continuously allocate for each reserve() call.
  if (newSize > fCurData->fMask + 1)
  {
    rehashPowerOfTwo(newSize);
  }
}

void RowAggStorage::startNewGeneration()
{
  if (!fEnabledDiskAggregation)
  {
    ++fGeneration;
    fGens.emplace_back(new Data);
    auto* newData = fGens.back().get();
    newData->fHashes = fCurData->fHashes->clone(0, fGeneration);
    fCurData = newData;
    reserve(fMaxRows);
    return;
  }

  if (fCurData->fSize == 0)
    return;
  // save all data and free storages' memory
  dumpInternalData();
  fCurData->fHashes->startNewGeneration();
  fStorage->startNewGeneration();
  if (fExtKeys)
    fKeysStorage->startNewGeneration();

  ++fGeneration;
  if (fLog)
  {
    ++fLog->fGenerations;
    noteSpill();
  }
  fMM->release();
  // reinitialize internal structures
  fCurData->fInfo.reset();
  fCurData->fSize = 0;
  fCurData->fMask = 0;
  fCurData->fMaxSize = 0;
  fCurData->fInfoInc = INIT_INFO_INC;
  fCurData->fInfoHashShift = INIT_INFO_HASH_SHIFT;
  reserve(fMaxRows);
  fAggregated = false;
}

std::string RowAggStorage::makeDumpFilename(int32_t gen) const
{
  char fname[PATH_MAX];
  uint16_t rgen = gen < 0 ? fGeneration : gen;
  snprintf(fname, sizeof(fname), "%s/AggMap-p%u-t%p-g%u", fTmpDir.c_str(), getpid(), fUniqId, rgen);
  return fname;
}

void RowAggStorage::dumpInternalData() const
{
  if (!fCurData->fInfo)
    return;

  messageqcpp::ByteStream bs;
  bs << fCurData->fSize;
  bs << fCurData->fMask;
  bs << fCurData->fMaxSize;
  bs << fCurData->hashMultiplier_;
  bs << fCurData->fInfoInc;
  bs << fCurData->fInfoHashShift;
  bs.append(fCurData->fInfo.get(), calcBytes(calcSizeWithBuffer(fCurData->fMask + 1, fCurData->fMaxSize)));
  int fd = open(makeDumpFilename().c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0)
  {
    throw logging::IDBExcept(
        logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errno)),
        logging::ERR_DISKAGG_FILEIO_ERROR);
  }

  int errNo;
  if ((errNo = writeData(fd, (const char*)bs.buf(), bs.length())) != 0)
  {
    close(fd);
    throw logging::IDBExcept(
        logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errNo)),
        logging::ERR_DISKAGG_FILEIO_ERROR);
  }
  close(fd);
}

void RowAggStorage::finalize(std::function<void(Row&)> mergeFunc, Row& rowOut)
{
  if (fAggregated || fGeneration == 0 || !fEnabledDiskAggregation)
  {
    cleanup();
    return;
  }

  uint64_t merged = 0;  // added to fLog->fRowsMerged in batches
  if (fLog && fLog->fFinalizing++ == 0)
  {
    std::ostringstream oss;
    oss << "final merge starting: " << fGeneration + 1 << " generations in the first bucket, "
        << fLog->fSpilling << " buckets spilled, written " << fLog->fRGsWritten << " rowgroups / "
        << (fLog->fBytesWritten >> 20) << " MiB";
    fLog->log(oss.str());
  }

  Row tmpKeyRow;
  fKeysStorage->initRow(tmpKeyRow);
  Row tmpRow;
  fStorage->initRow(tmpRow);
  dumpInternalData();
  fCurData->fHashes->dump();
  fStorage->dumpAll();
  if (fExtKeys)
    fKeysStorage->dumpAll();

  uint16_t curGen = fGeneration + 1;
  while (--curGen > 0)
  {
    bool genUpdated = false;
    if (fCurData->fSize == 0)
    {
      fStorage->dumpFinalizedInfo();
      if (fExtKeys)
        fKeysStorage->dumpFinalizedInfo();
      cleanup(curGen);
      loadGeneration(curGen - 1);
      auto oh = std::move(fCurData->fHashes);
      fCurData->fHashes = oh->clone(0, curGen - 1, true);
      fStorage.reset(fStorage->clone(curGen - 1));
      if (fExtKeys)
      {
        fRealKeysStorage.reset(fRealKeysStorage->clone(curGen - 1));
        fKeysStorage = fRealKeysStorage.get();
      }
      else
        fKeysStorage = fStorage.get();
      continue;
    }

    std::unique_ptr<RowGroupStorage> prevRowStorage;
    std::unique_ptr<RowGroupStorage> prevRealKeyRowStorage;
    RowGroupStorage* prevKeyRowStorage{nullptr};

    for (uint16_t prevGen = 0; prevGen < curGen; ++prevGen)
    {
      prevRowStorage.reset(fStorage->clone(prevGen));
      if (fExtKeys)
      {
        prevRealKeyRowStorage.reset(fKeysStorage->clone(prevGen));
        prevKeyRowStorage = prevRealKeyRowStorage.get();
      }
      else
        prevKeyRowStorage = prevRowStorage.get();

      if (fExtKeys)
      {
        fKeysRowGroup->initRow(&tmpKeyRow);
      }
      auto& prevRow = fExtKeys ? tmpKeyRow : tmpRow;

      // iterate over rows in prev generation
      for (uint64_t rowidx = 0; prevKeyRowStorage->getRowForFinalization(rowidx, prevRow); ++rowidx)
      {
        auto [rgid, rid] = rowIdxToGidRid(rowidx, prevKeyRowStorage->getRowsPerRG());
        if (rgid != 0 && rid == 0)
        {
          // start next RGData. At this point we don't need the previous one and can free the data
          // as it has no changes to dump
          prevKeyRowStorage->dropRGData(rgid - 1);
          if (fExtKeys)
          {
            // with external keys the values live in their own storage, filled in step with the keys
            prevRowStorage->dropRGData(rgid - 1);
          }
        }

        if (UNLIKELY(fLog && (rowidx & 0xFFFF) == 0))
        {
          fLog->fRowsMerged += merged;
          merged = 0;
          fLog->progress("final merge");
        }

        if (prevKeyRowStorage->isFinalized(rowidx))
        {
          continue;
        }

        // TODO: store hashes in the RowGroupStorage?
        uint64_t hash = hashRow(prevRow, fLastKeyCol);
        auto [info, idx] = rowHashToIdx(hash);
        nextWhileLess(info, idx);
        constexpr uint64_t NOTFOUND_IDX = -1;
        uint64_t curidx = NOTFOUND_IDX;
        while (info == fCurData->fInfo[idx])
        {
          auto& pos = fCurData->fHashes->get(idx);
          if (pos.hash == hash)
          {
            auto& keyRow = fExtKeys ? fKeyRow : rowOut;
            fKeysStorage->getRow(pos.idx, keyRow);
            if (prevRow.equals(keyRow, fLastKeyCol))
            {
              // Hallelujah!
              curidx = pos.idx;
              break;
            }
          }

          next(info, idx);
        }

        if (curidx == NOTFOUND_IDX)
        {
          // nothing was found, go to the next row
          continue;
        }

        if (UNLIKELY(fKeysStorage->isFinalized(curidx)))
        {
          continue;
        }

        // here it is! So:
        // 1 Get actual row datas
        // 2 Merge it into the current generation
        // 3 Mark generations as updated
        // 4 Mark the prev generation row as finalized
        if (fExtKeys)
        {
          prevRowStorage->getRow(rowidx, tmpRow);
          fStorage->getRow(curidx, rowOut);
        }
        mergeFunc(tmpRow);
        ++merged;
        genUpdated = true;
        prevKeyRowStorage->markFinalized(rowidx);
        if (fExtKeys)
        {
          prevRowStorage->markFinalized(rowidx);
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

    // load previous generation (reusing RowGroupStorages)
    loadGeneration(curGen - 1);
    auto oh = std::move(fCurData->fHashes);
    fCurData->fHashes = oh->clone(0, curGen - 1, true);

    // dumpAll() above left the rowgroups on this generation's RGDatas, which are freed here
    fStorage->detachRowGroup();
    if (fExtKeys)
      fRealKeysStorage->detachRowGroup();
    fStorage = std::move(prevRowStorage);
    if (fExtKeys)
    {
      fRealKeysStorage = std::move(prevRealKeyRowStorage);
      fKeysStorage = fRealKeysStorage.get();
    }
    else
      fKeysStorage = prevKeyRowStorage;
  }

  fStorage->dumpFinalizedInfo();
  if (fExtKeys)
    fKeysStorage->dumpFinalizedInfo();
  auto oh = std::move(fCurData->fHashes);
  fCurData->fHashes = oh->clone(fCurData->fMask + 1, fGeneration, true);
  fStorage.reset(fStorage->clone(fGeneration));
  if (fExtKeys)
  {
    fRealKeysStorage.reset(fRealKeysStorage->clone(fGeneration));
    fKeysStorage = fRealKeysStorage.get();
  }
  else
    fKeysStorage = fStorage.get();

  fAggregated = true;
  if (fLog)
  {
    fLog->fRowsMerged += merged;
    if (++fLog->fFinalized == fLog->fFinalizing)
    {
      std::ostringstream oss;
      oss << "final merge done in all " << fLog->fFinalized << " buckets that spilled, "
          << std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - fLog->fStart)
                 .count()
          << " s after the step started: " << fLog->fRowsMerged << " rows merged, read back "
          << fLog->fRGsRead << " rowgroups / " << (fLog->fBytesRead >> 20) << " MiB";
      fLog->log(oss.str());
    }
  }
}

void RowAggStorage::loadGeneration(uint16_t gen)
{
  loadGeneration(gen, fCurData->fSize, fCurData->fMask, fCurData->fMaxSize, fCurData->hashMultiplier_,
                 fCurData->fInfoInc, fCurData->fInfoHashShift, fCurData->fInfo);
}

void RowAggStorage::loadGeneration(uint16_t gen, size_t& size, size_t& mask, size_t& maxSize,
                                   size_t& hashMultiplier, uint32_t& infoInc, uint32_t& infoHashShift,
                                   std::unique_ptr<uint8_t[]>& info)
{
  messageqcpp::ByteStream bs;
  int fd = open(makeDumpFilename(gen).c_str(), O_RDONLY);
  if (fd < 0)
  {
    throw logging::IDBExcept(
        logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errno)),
        logging::ERR_DISKAGG_FILEIO_ERROR);
  }
  struct stat st
  {
  };
  fstat(fd, &st);
  bs.needAtLeast(st.st_size);
  bs.restart();
  int errNo;
  if ((errNo = readData(fd, (char*)bs.getInputPtr(), st.st_size)) != 0)
  {
    close(fd);
    throw logging::IDBExcept(
        logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR, errorString(errNo)),
        logging::ERR_DISKAGG_FILEIO_ERROR);
  }
  close(fd);
  bs.advanceInputPtr(st.st_size);

  bs >> size;
  bs >> mask;
  bs >> maxSize;
  bs >> hashMultiplier;
  bs >> infoInc;
  bs >> infoHashShift;
  size_t infoSz = calcBytes(calcSizeWithBuffer(mask + 1, maxSize));
  info.reset(new uint8_t[infoSz]());
  uint8_t* tmp = info.get();
  bs >> tmp;
}

void RowAggStorage::cleanupAll() noexcept
{
  try
  {
    boost::filesystem::remove_all(fTmpDir);
  }
  catch (...)
  {
  }
}

void RowAggStorage::cleanup()
{
  cleanup(fGeneration);
}

void RowAggStorage::cleanup(uint16_t gen)
{
  if (!fInitialized)
    return;
  unlink(makeDumpFilename(gen).c_str());
}

size_t RowAggStorage::getBucketSize()
{
  return 1 /* info byte */ + sizeof(RowPosHash);
}

uint32_t calcNumberOfBuckets(ssize_t availMem, uint32_t numOfThreads, uint32_t numOfBuckets,
                             uint32_t groupsPerThread, uint32_t inRowSize, uint32_t outRowSize,
                             bool enabledDiskAggr)
{
  if (availMem < 0)
  {
    // Most likely, nothing can be processed, but we will still try
    return 1;
  }
  uint32_t ret = numOfBuckets;

  ssize_t minNeededMemPerThread = groupsPerThread * inRowSize * RowAggStorage::getMaxRows(false);
  auto rowGroupSize = RowAggStorage::getMaxRows(enabledDiskAggr);
  ssize_t minNeededMemPerBucket =
      outRowSize * rowGroupSize * 2 +
      RowAggStorage::calcSizeWithBuffer(rowGroupSize, rowGroupSize) * RowAggStorage::getBucketSize();
  if ((availMem - minNeededMemPerThread * numOfThreads) / numOfBuckets < minNeededMemPerBucket)
  {
    ret = 0;
    if (availMem > minNeededMemPerThread * numOfThreads)
    {
      ret = (availMem - minNeededMemPerThread * numOfThreads) / minNeededMemPerBucket;
    }

    if (ret < numOfThreads)
    {
      ret = availMem / (minNeededMemPerBucket + minNeededMemPerThread);
    }
  }

  return ret == 0 ? 1 : ret;
}

}  // namespace rowgroup

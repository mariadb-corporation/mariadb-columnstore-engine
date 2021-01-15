//
// Created by kemm on 05.03.2021.
//

#include <unistd.h>
#include <sys/stat.h>
#include "rowgroup.h"
#include <resourcemanager.h>
#include <fcntl.h>
#include "rowstorage.h"
#include "robin_hood.h"

namespace
{

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
  RowGroupStorage(RowGroup* rowGroupOut,
                  size_t maxRows,
                  joblist::ResourceManager* rm = nullptr,
                  boost::shared_ptr<int64_t> sessLimit = {},
                  bool wait = false,
                  bool strict = false)
      : fRowGroupOut(rowGroupOut)
      , fMaxRows(maxRows)
      , fRGDatas()
  {
    auto* curRG = new RGData(*fRowGroupOut, fMaxRows);
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
  void append(std::unique_ptr<RowGroupStorage> o)
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
    }
  }

  /** @brief Returns next RGData, load it from disk if necessary.
   *
   * @returns pointer to the next RGData or empty pointer if there is nothing
   */
  std::unique_ptr<RGData> getNextRGData()
  {
    if (fRGDatas.empty())
      return {};

    uint64_t rgid = fRGDatas.size() - 1;
    if (!fRGDatas[rgid])
      loadRG(rgid);

    auto rgdata = std::move(fRGDatas[rgid]);
    fRGDatas.pop_back();

    fRowGroupOut->setData(rgdata.get());
    int64_t memSz = fRowGroupOut->getSizeWithStrings(fMaxRows);
    fMM->release(memSz);
    fLRU->remove(rgid);
    return rgdata;
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
    if (UNLIKELY(!fRGDatas[fCurRgid]))
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

private:
  /** @brief Get next available RGData and fill filename of the dump if it's
   *         not in the memory.
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
    uint64_t rgid = fRGDatas.size() - 1;
    rgdata = std::move(fRGDatas[rgid]);
    fRGDatas.pop_back();
    if (rgdata)
    {
      fRowGroupOut->setData(rgdata.get());
      int64_t memSz = fRowGroupOut->getSizeWithStrings(fMaxRows);
      fMM->release(memSz);
    }
    else
    {
      fname = makeRGFilename(rgid);
    }
    fLRU->remove(rgid);
    return true;
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

      auto to_read = st.st_size;
      bs.needAtLeast(to_read);
      bs.restart();
      while (to_read > 0)
      {
        errno = 0;
        auto r = read(fd, bs.getInputPtr(), to_read);
        if (UNLIKELY(r <= 0))
        {
          if (errno == EAGAIN)
            continue;

          throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                       logging::ERR_DISKAGG_FILEIO_ERROR),
                                   logging::ERR_DISKAGG_FILEIO_ERROR);
        }
        bs.advanceInputPtr(r);
        to_read -= r;
        assert(to_read >= 0);
      }
      close(fd);
    }
    catch (...)
    {
      close(fd);
      throw;
    }

    unlink(fname.c_str());
    fRGDatas[rgid] = std::unique_ptr<RGData>(new RGData());
    fRGDatas[rgid]->deserialize(bs);

    fRowGroupOut->setData(fRGDatas[rgid].get());
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
    auto to_write = bs.length();
    while (to_write > 0)
    {
      auto r = write(fd, bs.buf() + bs.length() - to_write, to_write);
      if (UNLIKELY(r < 0))
      {
        if (errno == EAGAIN)
          continue;

        close(fd);
        throw logging::IDBExcept(logging::IDBErrorInfo::instance()->errorMsg(
                                     logging::ERR_DISKAGG_FILEIO_ERROR),
                                 logging::ERR_DISKAGG_FILEIO_ERROR);
      }
      assert(r <= to_write);
      to_write -= r;
    }
    close(fd);
  }

#ifdef DISK_AGG_DEBUG
  void dumpAll() const
  {
    dumpMeta();
    for (uint64_t i = 0; i < fRGDatas.size(); ++i)
    {
      if (fRGDatas[i])
        saveRG(i, fRGDatas[i].get());
    }
  }

  void dumpMeta() const
  {
    messageqcpp::ByteStream bs;
    fRowGroupOut->serialize(bs);

    char buf[1024];
    snprintf(buf, sizeof(buf), "/tmp/kemm/META-p%u-t%p", getpid(), this);
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
    snprintf(buf, sizeof(buf), "/tmp/kemm/Agg-p%u-t%p-rg%lu", getpid(), this, rgid);
    return buf;
  }

private:
  RowGroup* fRowGroupOut = nullptr;
  const size_t fMaxRows;
  std::unique_ptr<MemManager> fMM;
  std::unique_ptr<LRUIface> fLRU;
  RGDataStorage fRGDatas;
  std::string fNamePrefix;

  uint64_t fCurRgid = 0;
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
  RowPosHashStorage(size_t maxRows,
                    size_t size,
                    joblist::ResourceManager* rm,
                    boost::shared_ptr<int64_t> sessLimit,
                    bool enableDiskAgg)
      : fMaxRows(maxRows)
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

  RowPosHashStorage* clone(size_t size)
  {
    if (UNLIKELY(fPosHashes.empty()))
    {
      init(size + 1);
      return this;
    }
    auto* cloned = new RowPosHashStorage(fMaxRows);
    cloned->fLRU = fLRU->clone();
    cloned->fMM = fMM->clone();
    cloned->init(size + 1);
    return cloned;
  }

private:
  explicit RowPosHashStorage(size_t maxRows) : fMaxRows(maxRows) {}

  void init(size_t size)
  {
    size_t groups = (size + (fMaxRows - 1)) / fMaxRows;
    fPosHashes.reserve(groups);
    for (size_t i = 0; i < groups; ++i)
    {
      fMM->acquire(fMaxRows * sizeof(RowPosHash));
      auto *ptr = new RowPosHash[fMaxRows];
      fPosHashes.push_back(ptr);
      fLRU->add(i);
    }
  }

  int getDumpFd()
  {
    if (UNLIKELY(fDumpFd < 0))
    {
      char fname[PATH_MAX];
      snprintf(fname, sizeof(fname), "/tmp/kemm/Agg-PosHash-p%u-t%p", getpid(), this);
      fDumpFd = open(fname, O_RDWR | O_CREAT | O_TRUNC, 0644);
      if (fDumpFd < 0)
      {
        throw logging::IDBExcept(
            logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_FILEIO_ERROR),
            logging::ERR_DISKAGG_FILEIO_ERROR);
      }
      unlink(fname);
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
  const size_t fMaxRows;
  MemManager* fMM{nullptr};
  LRUIface* fLRU{nullptr};
  std::vector<RowPosHash*> fPosHashes;

  int fDumpFd = -1;
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
RowAggStorage::RowAggStorage(RowGroup *rowGroupOut, RowGroup *keysRowGroup,
                             uint32_t keyCount, size_t maxRows,
                             joblist::ResourceManager *rm,
                             boost::shared_ptr<int64_t> sessLimit,
                             bool enabledDiskAgg)
    : fMaxRows(maxRows)
    , fExtKeys(rowGroupOut != keysRowGroup)
    , fStorage(new RowGroupStorage(rowGroupOut, maxRows, rm, sessLimit, !enabledDiskAgg, !enabledDiskAgg))
    , fKeysStorage(fStorage.get())
    , fLastKeyCol(keyCount - 1)
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
    fKeysStorage = new RowGroupStorage(keysRowGroup, maxRows, rm, sessLimit, !enabledDiskAgg, !enabledDiskAgg);
  }
  fKeysStorage->initRow(fKeyRow);
  fHashes = new RowPosHashStorage(maxRows, 0, rm, sessLimit, enabledDiskAgg);
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
  while (true)
  {
    if (fMM->getFree() > 1 * 1024 * 1024) // TODO: make it configurable
      break;

    bool success = fStorage->dump();
    if (fExtKeys)
      success |= fKeysStorage->dump();

    if (success)
    {
      // Try to unload only RGDatas first 'cause unloading RowHashPositions
      // causes a worse performance impact
      continue;
    }

    if (!fHashes->dump())
      break;
  }

  auto rhUsedMem = fMM->getUsed();
  if (rhUsedMem >= size_t(fInitialMemLimit / fNumOfOuterBuckets))
  {
    // safety guard so aggregation couldn't eat all available memory
    // TODO: shall we make several rounds of aggregation?
    throw logging::IDBExcept(
        logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_DISKAGG_TOO_BIG),
        logging::ERR_DISKAGG_TOO_BIG);
  }
}

void RowAggStorage::append(RowAggStorage& other)
{
  // we don't need neither key rows storage nor any internal data anymore
  // neither in this RowAggStorage nor in the other
  freeData();
  other.freeData();
  fStorage->append(std::move(other.fStorage));
}

std::unique_ptr<RGData> RowAggStorage::getNextRGData()
{
  freeData();
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
  RowPosHashStorage * oldHashes = fHashes;

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

  fHashes = fHashes->clone(elems);
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

} // namespace rowgroup

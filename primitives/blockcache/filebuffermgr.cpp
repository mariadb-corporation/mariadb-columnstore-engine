/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016-2023 MariaDB Corporation

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

/***************************************************************************
 *
 *   $Id: filebuffermgr.cpp 2045 2013-01-30 20:26:59Z pleblanc $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/

// #define NDEBUG
#include <boost/unordered/unordered_flat_set.hpp>
#include <cassert>
#include <limits>
#include <mutex>
#include <optional>

#include <pthread.h>

#include "brmtypes.h"
#include "hasher.h"
#include "stats.h"
#include "configcpp.h"
#include "filebuffermgr.h"
#include "mcsconfig.h"

using namespace config;
using namespace boost;
using namespace std;
using namespace BRM;

namespace dbbc
{
const uint32_t gReportingFrequencyMin(32768);

FileBufferMgr::FileBufferMgr(const uint32_t numBlcks, const uint32_t blkSz, const uint32_t deleteBlocks)
 : fMaxNumBlocks(numBlcks / PartitionsNumber)
 , fBlockSz(blkSz)
 , fCacheSize(0)
 , fFBPool()
 , fDeleteBlocks(deleteBlocks)
 , fReportFrequency(0)
{
  // fCacheSizes = std::vector<size_t>(PartitionsNumber, 0);

  fFBPool.reserve(numBlcks);
  fConfig = Config::makeConfig();
  setReportingFrequency(1);
  fLog.open(string(MCSLOGDIR) + "/trace/bc", ios_base::app | ios_base::ate);
}

FileBufferMgr::~FileBufferMgr()
{
  takeLocksAndflushCache();
}

// param d is used as a togle only
void FileBufferMgr::setReportingFrequency(const uint32_t d)
{
  if (d == 0)
  {
    fReportFrequency = 0;
    return;
  }

  // ////std::cout << "setReportingFrequency" << std::endl;
  const string val = fConfig->getConfig("DBBC", "ReportFrequency");
  uint32_t temp = 0;

  if (val.length() > 0)
    temp = static_cast<int>(Config::fromText(val));

  if (!temp)
  {
    fReportFrequency = 0;
  }
  else if (temp > 0 && temp <= gReportingFrequencyMin)
  {
    fReportFrequency = gReportingFrequencyMin;
  }
  else
  {
    fReportFrequency = temp;
  }
}

void FileBufferMgr::takeLocksAndflushCache()
{
  // std::cout << "takeLocksAndflushCache" << std::endl;

  std::scoped_lock overalLock(fWLocks[0], fWLocks[1], fWLocks[2], fWLocks[3], fWLocks[4], fWLocks[5],
                              fWLocks[6], fWLocks[7]);
  return flushCache();
}

// Mutex locks must be taken before calling flushCache
void FileBufferMgr::flushCache()
{
  std::cout << "flushCache " << std::endl;

  assert(fbLists.size() == fbSets.size() && fbSets.size() == fEmptyPoolsSlots.size());
  for (size_t i = 0; i < fbLists.size(); ++i)
  {
    filebuffer_uset_t sEmpty;
    filebuffer_list_t lEmpty;
    emptylist_t vEmpty;

    fbLists[i].swap({lEmpty});
    fbSets[i].swap(sEmpty);
    fEmptyPoolsSlots[i].swap(vEmpty);
  }
  // fCacheSize = 0;
  fCacheSizes = std::vector<size_t>(PartitionsNumber, 0);

  // the block pool should not be freed in the above block to allow us
  // to continue doing concurrent unprotected-but-"safe" memcpys
  // from that memory
  if (fReportFrequency)
  {
    fLog << "Clearing entire cache" << endl;
  }
  fFBPool.clear();
}

void FileBufferMgr::flushOne(const BRM::LBID_t lbid, const BRM::VER_t ver)
{
  // std::cout << "flushOne" << std::endl;

  auto part = partition(lbid);

  HashObject_t fbIndex(lbid, ver, 0);
  std::scoped_lock lk(fWLocks[part]);

  auto iter = fbSets[part].find(fbIndex);

  if (iter != fbSets[part].end())
  {
    // remove it from fbList
    uint32_t idx = iter->poolIdx;
    fbLists[part].erase(fFBPool[idx].listLoc());
    // add to fEmptyPoolSlots
    fEmptyPoolsSlots[part].push_back(idx);
    // remove it from fbSet
    fbSets[part].erase(iter);
    // adjust fCacheSize
    --fCacheSizes[part];
  }
}

// change the array with a vector to enable range iteration
void FileBufferMgr::flushMany(const LbidAtVer* laVptr, uint32_t cnt)
{
  // std::cout << "flushMany" << std::endl;

  if (fReportFrequency)
  {
    fLog << "flushMany " << cnt << " items: ";
    for (uint32_t j = 0; j < cnt; j++)
    {
      fLog << "lbid: " << laVptr[j].LBID << " ver: " << laVptr[j].Ver << ", ";
    }
    fLog << endl;
  }

  for (uint32_t j = 0; j < cnt; j++)
  {
    auto [lbid, ver] = *laVptr;
    HashObject_t fbIndex(lbid, ver, 0);
    auto part = partition(lbid);

    std::scoped_lock lk(fWLocks[part]);

    auto iter = fbSets[part].find(fbIndex);

    if (iter != fbSets[part].end())
    {
      // remove it from fbList
      uint32_t idx = iter->poolIdx;
      fbLists[part].erase(fFBPool[idx].listLoc());
      // add to fEmptyPoolSlots
      fEmptyPoolsSlots[part].push_back(idx);
      // remove it from fbSet
      fbSets[part].erase(iter);
      // adjust fCacheSize
      --fCacheSizes[part];
    }

    ++laVptr;
  }
}

// change the array with a vector to enable range iteration
void FileBufferMgr::flushManyAllversion(const LBID_t* laVptr, uint32_t cnt)
{
  // std::cout << " flushManyAllversion" << std::endl;

  // put lbids into separate buckets and loop over the buckets separately

  if (fReportFrequency)
  {
    fLog << "flushManyAllversion " << cnt << " items: ";
    for (uint32_t i = 0; i < cnt; i++)
    {
      fLog << laVptr[i] << ", ";
    }
    fLog << endl;
  }
  if (cnt == 0)
    return;

  boost::unordered_flat_set<LBID_t> uniquer;
  for (uint32_t i = 0; i < cnt; i++)
    uniquer.insert(laVptr[i]);

  for (uint32_t i = 0; i < cnt; i++)
  {
    auto lbid = *laVptr;
    auto part = partition(lbid);
    std::scoped_lock lk(fWLocks[part]);
    if (fCacheSizes[part] == 0)
      return;
    for (auto it = fbSets[part].begin(); it != fbSets[part].end();)
    {
      if (uniquer.find(it->lbid) != uniquer.end())
      {
        const uint32_t idx = it->poolIdx;
        fbLists[part].erase(fFBPool[idx].listLoc());
        fEmptyPoolsSlots[part].push_back(idx);
        auto tmpIt = it;
        ++it;
        fbSets[part].erase(tmpIt);
        --fCacheSizes[part];
      }
      else
        ++it;
    }
  }
}

template <typename OIDsContainer>
EMEntriesVec FileBufferMgr::getExtentsByOIDs(OIDsContainer oids, const uint32_t count) const
{
  vector<EMEntry> extents;
  {
    const uint32_t clearThreshold = 50000;
    DBRM dbrm;
    // This is moved outside the loop to allocate space only once.
    vector<EMEntry> extentsLocal;

    for (size_t i = 0; i < count; i++)
    {
      extentsLocal.clear();

      // change sorted to false b/c there is no need to sort the extents.
      auto err = dbrm.getExtents(oids[i], extentsLocal, false, true,
                                 true);  // @Bug 3838 Include outofservice extents
      // std::cout << "flushOIDs id " << oids[i] << " " << extents.size() << std::endl;

      // WIP make clearThreshold dynamic and based on the cache size.
      if (err < 0 || (i == 0 && (extents.size() * count) > clearThreshold))
      {
        return std::nullopt;
      }
      extents.insert(std::begin(extents), std::begin(extentsLocal), std::end(extentsLocal));
    }
  }
  return std::move(extents);
}

void FileBufferMgr::flushOIDs(const uint32_t* oids, uint32_t count)
{
  // std::cout << " flushOIDs" << std::endl;
  if (count == 0)
    return;

  if (fReportFrequency)
  {
    std::cout << "flushOIDs " << count << " items: ";
    for (uint32_t i = 0; i < count; i++)
    {
      std::cout << oids[i] << ", ";
    }
    std::cout << endl;
  }

  // If there are more than this # of extents to drop, the whole cache will be cleared

  auto extents = getExtentsByOIDs(oids, count);
  if (!extents)
    return takeLocksAndflushCache();

  auto notInPartitions = [](const EMEntry& range) { return false; };
  return flushExtents(extents.value(), notInPartitions);
}

void FileBufferMgr::flushPartition(const vector<OID_t>& oids, const set<BRM::LogicalPartition>& partitions)
{
  // //std::cout << " flushPartition" << std::endl;
  const uint32_t count = oids.size();
  if (fReportFrequency)
  {
    std::set<BRM::LogicalPartition>::iterator sit;
    fLog << "flushPartition oids: ";
    for (uint32_t i = 0; i < count; i++)
    {
      fLog << oids[i] << ", ";
    }
    fLog << "flushPartition partitions: ";
    for (sit = partitions.begin(); sit != partitions.end(); ++sit)
    {
      fLog << (*sit).toString() << ", ";
    }
    fLog << endl;
  }

  if (count == 0 || partitions.size() == 0)
    return;

  auto extents = getExtentsByOIDs(oids, count);
  if (!extents)
    return takeLocksAndflushCache();

  auto notInPartitions = [&partitions](const EMEntry& range)
  {
    LogicalPartition logicalPartNum(range.dbRoot, range.partitionNum, range.segmentNum);
    return partitions.find(logicalPartNum) == partitions.end();
  };

  return flushExtents(extents.value(), notInPartitions);
}

bool FileBufferMgr::exists(const BRM::LBID_t& lbid, const BRM::VER_t ver)
{
  std::cout << " exists1 " << lbid << std::endl;

  const HashObject_t fb(lbid, ver, 0);

  const bool b = exists(fb);
  return b;
}

FileBuffer* FileBufferMgr::findPtr(const HashObject_t& keyFb)
{
  auto part = partition(keyFb.lbid);
  std::scoped_lock lk(fWLocks[part]);

  auto it = fbSets[part].find(keyFb);

  if (fbSets[part].end() != it)
  {
    FileBuffer* fb = &(fFBPool[it->poolIdx]);
    fFBPool[it->poolIdx].listLoc()->hits++;
    fbLists[part].splice(fbLists[part].begin(), fbLists[part], (fFBPool[it->poolIdx]).listLoc());
    return fb;
  }

  return nullptr;
}

bool FileBufferMgr::find(const HashObject_t& keyFb, FileBuffer& fb)
{
  std::cout << " find1 " << keyFb.lbid << std::endl;

  auto part = partition(keyFb.lbid);

  std::scoped_lock lk(fWLocks[part]);

  auto it = fbSets[part].find(keyFb);

  if (fbSets[part].end() != it)
  {
    fFBPool[it->poolIdx].listLoc()->hits++;
    fbLists[part].splice(fbLists[part].begin(), fbLists[part], (fFBPool[it->poolIdx]).listLoc());
    fb = fFBPool[it->poolIdx];
    return true;
  }

  return false;
}

bool FileBufferMgr::find(const HashObject_t& keyFb, void* bufferPtr)
{
  std::cout << " find2 " << keyFb.lbid << std::endl;

  auto part = partition(keyFb.lbid);

  bool found = false;
  uint32_t idx = 0;
  {
    std::scoped_lock lk(fWLocks[part]);

    auto it = fbSets[part].find(keyFb);

    if (fbSets[part].end() != it)
    {
      found = true;
      idx = it->poolIdx;
      std::cout << " find2 " << keyFb.lbid << " idx " << idx << std::endl;

      //@bug 669 LRU cache, move block to front of list as last recently used.
      fFBPool[idx].listLoc()->hits++;
      fbLists[part].splice(fbLists[part].begin(), fbLists[part], (fFBPool[idx]).listLoc());
    }
  }
  // There is a potential race b/w the moment the block is found and the copy below.
  if (found)
  {
    memcpy(bufferPtr, (fFBPool[idx]).getData(), 8192);
    return true;
  }

  return false;
}

uint32_t FileBufferMgr::bulkFind(const BRM::LBID_t* lbids, const BRM::VER_t* vers, uint8_t** buffers,
                                 bool* wasCached, uint32_t count)
{
  std::cout << " bulkFind " << std::endl;

  uint32_t ret = 0;
  // filebuffer_uset_iter_t* it = (filebuffer_uset_iter_t*)alloca(count * sizeof(filebuffer_uset_iter_t));
  std::vector<uint32_t> indexes(count, 0);
  // utils::Hasher64_r hasher;

  // It is worth to distribute lbids into buckets before going over them
  for (uint32_t i = 0; i < count; i++)
  {
    // std::cout << " bulkFind l " << std::dec << lbids[i] << std::endl;
    // new ((void*)&it[i]) filebuffer_uset_iter_t();
    auto part = partition(lbids[i]);  // hasher(lbids + i, sizeof(lbids[0])) % PartitionsNumber;
    std::scoped_lock lk(fWLocks[part]);

    auto it = fbSets[part].find(HashObject_t(lbids[i], vers[i], 0));
    wasCached[i] = it != fbSets[part].end();

    if (wasCached[i])
    {
      const auto poolIdx = it->poolIdx;
      indexes[i] = poolIdx;
      // std::cout << " bulkFind l " << lbids[i] << " i " << indexes[i] << std::endl;

      ++fFBPool[poolIdx].listLoc()->hits;
      // There is a potential race b/w the next line and memcpy below.
      // Less realistic to happen b/c this algo puts the used list entries at the front of the list.
      // WIP can be future improved the memcpy block in this loop but outside the crit section.
      fbLists[part].splice(fbLists[part].begin(), fbLists[part], (fFBPool[poolIdx]).listLoc());
    }
  }

  // lk.unlock();
  for (uint32_t i = 0; i < count; i++)
  {
    const uint32_t idx = indexes[i];
    if (idx)
    {
      memcpy(buffers[i], fFBPool[idx].getData(), 8192);
      // int64_t* v = (int64_t*)(buffers[i]);
      // std::cout << " bulkFind l " << lbids[i] << " v " << std::hex << *v << std::dec << std::endl;

      ++ret;
    }
    // it[i].filebuffer_uset_iter_t::~filebuffer_uset_iter_t();
  }

  return ret;
}

bool FileBufferMgr::exists(const HashObject_t& fb)
{
  std::cout << " exists2 " << fb.lbid << std::endl;

  auto part = partition(fb.lbid);
  std::scoped_lock lk(fWLocks[part]);

  auto it = fbSets[part].find(fb);

  if (it != fbSets[part].end())
  {
    // std::cout << " exists2 true " << fb.lbid << std::endl;
    fFBPool[it->poolIdx].listLoc()->hits++;
    fbLists[part].splice(fbLists[part].begin(), fbLists[part], (fFBPool[it->poolIdx]).listLoc());
    return true;
  }

  return false;
}

// The bucket lock must be taken calling this method
void FileBufferMgr::depleteCache(const size_t part)
{
  // std::cout << " depleteCache" << std::endl;

  for (uint32_t i = 0; i < fDeleteBlocks && !fbLists[part].empty(); ++i)
  {
    FBData_t fbdata(fbLists[part].back());  // the lru block
    HashObject_t lastFB(fbdata.lbid, fbdata.ver, 0);
    auto iter = fbSets[part].find(lastFB);

    idbassert(iter != fbSets[part].end());
    uint32_t idx = iter->poolIdx;
    idbassert(idx < fFBPool.size());
    // Save position in FileBuffer pool for reuse.
    fEmptyPoolsSlots[part].push_back(idx);
    fbSets[part].erase(iter);

    if (fbLists[part].back().hits == 0)
      fBlksNotUsed++;

    fbLists[part].pop_back();
    --fCacheSizes[part];
  }
}

ostream& FileBufferMgr::formatLRUList(ostream& os) const
{
  size_t i = 0;
  for (auto l : fbLists)
  {
    for (auto [lbid, ver, hits] : l)
    {
      cout << i << to_string(lbid) << "\t" << to_string(ver) << std::endl;
    }
    ++i;
  }

  for (size_t i = 0; auto& buf : fFBPool)
  {
    cout << i << " " << buf.Lbid() << " " << buf.Verid() << " list it (" << buf.listLoc()->lbid << " "
         << buf.listLoc()->ver << ")" << std::endl;
    ++i;
  }

  return os;
}

// WIP this will break the LRU list.!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// puts the new entry at the front of the list
void FileBufferMgr::updateLRU(const FBData_t& f, const size_t part)
{
  // std::cout << " updateLRU" << std::endl;

  if (fCacheSizes[part] > maxCacheSize())
  {
    auto last = fbLists[part].end();
    --last;
    FBData_t& fbdata = *last;
    HashObject_t lastFB(fbdata.lbid, fbdata.ver, 0);
    auto iter = fbSets[part].find(lastFB);
    fEmptyPoolsSlots[part].push_back(iter->poolIdx);

    if (!fbdata.hits)
      fBlksNotUsed++;

    fbSets[part].erase(iter);
    fbLists[part].splice(fbLists[part].begin(), fbLists[part], last);
    fbdata = f;
    --fCacheSizes[part];
  }
  else
  {
    fbLists[part].push_front(f);
  }
}

uint32_t FileBufferMgr::doBlockCopy(const BRM::LBID_t& lbid, const BRM::VER_t& ver, const uint8_t* data,
                                    const size_t bucket)
{
  // std::cout << " doBlockCopy" << std::endl;

  uint32_t poolIdx;

  // WIP !!!!!!!!!!!!!!!!!!! very inefficient if fEmptyPoolsSlots is not common
  if (!fEmptyPoolsSlots[bucket].empty())
  {
    poolIdx = fEmptyPoolsSlots[bucket].front();
    fEmptyPoolsSlots[bucket].pop_front();
  }
  else
  {
    poolIdx = fFBPool.size();
    fFBPool.resize(poolIdx + 1);  // shouldn't trigger a 'real' resize b/c of the reserve call
  }

  fFBPool[poolIdx].Lbid(lbid);
  fFBPool[poolIdx].Verid(ver);
  fFBPool[poolIdx].setData(data);
  return poolIdx;
}

int FileBufferMgr::bulkInsert(const CacheInsertVec& ops)
{
  // std::cout << " bulkInsert" << std::endl;
  int ret = 0;

  if (fReportFrequency)
  {
    fLog << "bulkInsert: ";
  }
  for (size_t i = 0; i < ops.size(); i++)
  {
    const CacheInsert_t& op = ops[i];

    {
      HashObject_t fbIndex(op.lbid, op.ver, 0);
      size_t part = partition(op.Lbid());
      std::scoped_lock lk(fWLocks[part]);
      auto pr = fbSets[part].insert(fbIndex);

      if (!pr.second)
      {
        continue;
      }

      if (fReportFrequency)
      {
        fLog << op.lbid << " " << op.ver << ", ";
      }

      ++fCacheSizes[part];
      ++fBlksLoaded;
      FBData_t fbdata = {op.lbid, op.ver, 0};
      updateLRU(fbdata, part);
      HashObject_t& ref = const_cast<HashObject_t&>(*pr.first);

      // crit section to put the block into the buffer pool
      // WIP what if shared blocks are better
      {
        // TODO This might worth to reduce the scope moving fBufferWLock lock into else in the doBlockCopy
        std::scoped_lock lk(fBufferWLock);
        int32_t pi = doBlockCopy(op.lbid, op.ver, op.data, part);
        // int64_t* v = (int64_t*)(op.data);
        // std::cout << "bulkInsert lbid " << op.lbid << " ver " << op.ver << " pi " << pi << " v " <<
        // std::hex
        //           << *v << std::dec << std::endl;
        ref.poolIdx = pi;
        fFBPool[pi].listLoc(fbLists[part].begin());  // updateLRU sets the front of the list
      }

      ++ret;
      idbassert(fCacheSizes[part] <= maxCacheSize());  // WIP!!!!!
    }
  }
  if (fReportFrequency)
  {
    fLog << endl;
  }

  return ret;
}

}  // namespace dbbc
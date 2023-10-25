/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation

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
#include <cassert>
#include <limits>
#include <mutex>

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

  utils::Hasher64_r hasher;
  HashObject_t fbIndex(lbid, ver, 0);
  size_t bucket = hasher(&lbid, sizeof(lbid)) % PartitionsNumber;
  std::scoped_lock lk(fWLocks[bucket]);

  filebuffer_uset_iter_t iter = fbSets[bucket].find(fbIndex);

  if (iter != fbSets[bucket].end())
  {
    // remove it from fbList
    uint32_t idx = iter->poolIdx;
    fbLists[bucket].erase(fFBPool[idx].listLoc());
    // add to fEmptyPoolSlots
    fEmptyPoolsSlots[bucket].push_back(idx);
    // remove it from fbSet
    fbSets[bucket].erase(iter);
    // adjust fCacheSize
    --fCacheSizes[bucket];
  }
}

// change the array with a vector to enable range iteration
void FileBufferMgr::flushMany(const LbidAtVer* laVptr, uint32_t cnt)
{
  // std::cout << "flushMany" << std::endl;

  utils::Hasher64_r hasher;

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
    size_t bucket = hasher(&lbid, sizeof(lbid)) % PartitionsNumber;
    std::scoped_lock lk(fWLocks[bucket]);

    auto iter = fbSets[bucket].find(fbIndex);

    if (iter != fbSets[bucket].end())
    {
      if (fReportFrequency)
      {
        fLog << "flushMany hit, lbid: " << lbid << " index: " << iter->poolIdx << endl;
      }
      // remove it from fbList
      uint32_t idx = iter->poolIdx;
      fbLists[bucket].erase(fFBPool[idx].listLoc());
      // add to fEmptyPoolSlots
      fEmptyPoolsSlots[bucket].push_back(idx);
      // remove it from fbSet
      fbSets[bucket].erase(iter);
      // adjust fCacheSize
      --fCacheSizes[bucket];
    }

    ++laVptr;
  }
}

// change the array with a vector to enable range iteration
void FileBufferMgr::flushManyAllversion(const LBID_t* laVptr, uint32_t cnt)
{
  // std::cout << " flushManyAllversion" << std::endl;

  utils::Hasher64_r hasher;
  tr1::unordered_set<LBID_t> uniquer;
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

  for (uint32_t i = 0; i < cnt; i++)
    uniquer.insert(laVptr[i]);

  for (uint32_t i = 0; i < cnt; i++)
  {
    auto lbid = *laVptr;
    // HashObject_t fbIndex(lbid, ver, 0);
    size_t bucket = hasher(&lbid, sizeof(lbid)) % PartitionsNumber;
    std::scoped_lock lk(fWLocks[bucket]);
    if (fCacheSizes[bucket] == 0)
      return;
    for (auto it = fbSets[bucket].begin(); it != fbSets[bucket].end();)
    {
      if (uniquer.find(it->lbid) != uniquer.end())
      {
        if (fReportFrequency)
        {
          fLog << "flushManyAllversion hit: " << it->lbid << " index: " << it->poolIdx << endl;
        }
        const uint32_t idx = it->poolIdx;
        fbLists[bucket].erase(fFBPool[idx].listLoc());
        fEmptyPoolsSlots[bucket].push_back(idx);
        auto tmpIt = it;
        ++it;
        fbSets[bucket].erase(tmpIt);
        --fCacheSizes[bucket];
      }
      else
        ++it;
    }
  }
}

// void FileBufferMgr::flushOIDs(const uint32_t* oids, uint32_t count)
// {
//   DBRM dbrm;
//   uint32_t i;
//   vector<EMEntry> extents;
//   int err;
//   uint32_t currentExtent;
//   LBID_t currentLBID;
//   typedef tr1::unordered_multimap<LBID_t, filebuffer_uset_t::iterator> byLBID_t;
//   byLBID_t byLBID;
//   pair<byLBID_t::iterator, byLBID_t::iterator> itList;
//   filebuffer_uset_t::iterator it;

//   if (fReportFrequency)
//   {
//     fLog << "flushOIDs " << count << " items: ";
//     for (uint32_t i = 0; i < count; i++)
//     {
//       fLog << oids[i] << ", ";
//     }
//     fLog << endl;
//   }

//   // If there are more than this # of extents to drop, the whole cache will be cleared
//   const uint32_t clearThreshold = 50000;

//   boost::mutex::scoped_lock lk(fWLock);

//   if (fCacheSize == 0 || count == 0)
//     return;

//   /* Index the cache by LBID */
//   for (it = fbSet.begin(); it != fbSet.end(); it++)
//     byLBID.insert(pair<LBID_t, filebuffer_uset_t::iterator>(it->lbid, it));

//   for (i = 0; i < count; i++)
//   {
//     extents.clear();
//     err = dbrm.getExtents(oids[i], extents, true, true, true);  // @Bug 3838 Include outofservice
//     extents

//     if (err < 0 || (i == 0 && (extents.size() * count) > clearThreshold))
//     {
//       // (The i == 0 should ensure it's not a dictionary column)
//       lk.unlock();
//       flushCache();
//       return;
//     }

//     for (currentExtent = 0; currentExtent < extents.size(); currentExtent++)
//     {
//       EMEntry& range = extents[currentExtent];
//       LBID_t lastLBID = range.range.start + (range.range.size * 1024);

//       for (currentLBID = range.range.start; currentLBID < lastLBID; currentLBID++)
//       {
//         itList = byLBID.equal_range(currentLBID);

//         for (byLBID_t::iterator tmpIt = itList.first; tmpIt != itList.second; tmpIt++)
//         {
//           fbList.erase(fFBPool[tmpIt->second->poolIdx].listLoc());
//           fEmptyPoolSlots.push_back(tmpIt->second->poolIdx);
//           fbSet.erase(tmpIt->second);
//           fCacheSize--;
//         }
//       }
//     }
//   }
// }

void FileBufferMgr::flushExtents(const vector<BRM::EMEntry>& extents)
{
  using byLBID_t = tr1::unordered_multimap<LBID_t, filebuffer_uset_t::iterator>;
  byLBID_t byLBID;
  // WIP This expression should be refactored if possible.
  // Take multiple mutexes in a safe way w/o a loop.
  std::scoped_lock overalLock(fWLocks[0], fWLocks[1], fWLocks[2], fWLocks[3], fWLocks[4], fWLocks[5],
                              fWLocks[6], fWLocks[7]);

  /* Index the cache by LBID */
  // multiple threads can sync here so here should be a random start
  // TODO take only those sets that are affected by this flush.
  for (auto& fbSet : fbSets)
  {
    // for (filebuffer_uset_t::iterator it = fbSet.begin(); it != fbSet.end(); ++it)
    for (auto it = fbSet.begin(); it != fbSet.end(); ++it)
    {
      byLBID.insert({it->lbid, it});
    }
  }

  for (auto& extent : extents)
  {
    const LBID_t lastLBID = extent.range.start + (extent.range.size * 1024);
    for (auto currentLBID = extent.range.start; currentLBID < lastLBID; ++currentLBID)
    {
      auto fbSetRange4LBID = byLBID.equal_range(currentLBID);

      for (auto lbidAndfbSetPair = fbSetRange4LBID.first; lbidAndfbSetPair != fbSetRange4LBID.second;
           ++lbidAndfbSetPair)
      {
        const auto lbid = lbidAndfbSetPair->first;
        auto fbSetItToErase = lbidAndfbSetPair->second;
        const auto poolIdx = fbSetItToErase->poolIdx;

        // WIP make a simplier hashe>>r to avoid needless complex math every loop iter.
        // don't need to calculate bucket but get it from byLBID
        const size_t part = partition(lbid);
        if (!fCacheSizes[part])
          return;

        fbLists[part].erase(fFBPool[poolIdx].listLoc());
        fEmptyPoolsSlots[part].push_back(poolIdx);
        fbSets[part].erase(fbSetItToErase);
        --fCacheSizes[part];
      }
    }
  }
}

void FileBufferMgr::flushOIDs(const uint32_t* oids, uint32_t count)
{
  // std::cout << " flushOIDs" << std::endl;
  if (count == 0)
    return;

  uint32_t i;

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
  const uint32_t clearThreshold = 50000;

  vector<EMEntry> extents;
  {
    DBRM dbrm;
    vector<EMEntry> extentsLocal;

    for (i = 0; i < count; i++)
    {
      extentsLocal.clear();
      // WIP need to check if I can move these BRM traversal oustside crit section.
      // extents.clear();

      // change sorted to false b/c there is no need to sort the extents.
      auto err = dbrm.getExtents(oids[i], extentsLocal, false, true,
                                 true);  // @Bug 3838 Include outofservice extents
      // std::cout << "flushOIDs id " << oids[i] << " " << extents.size() << std::endl;

      // WIP make clearThreshold dynamic and based on the cache size.
      if (err < 0 || (i == 0 && (extents.size() * count) > clearThreshold))
      {
        // TODO add a printout here.
        return takeLocksAndflushCache();
      }
      extents.insert(std::begin(extents), std::begin(extentsLocal), std::end(extentsLocal));
    }
  }
  return flushExtents(extents);
}

// void FileBufferMgr::flushPartition(const vector<OID_t>& oids, const set<BRM::LogicalPartition>&
// partitions)
// {
//   // //std::cout << " flushPartition" << std::endl;

//   DBRM dbrm;
//   uint32_t i;
//   vector<EMEntry> extents;
//   int err;
//   uint32_t currentExtent;
//   LBID_t currentLBID;
//   typedef tr1::unordered_multimap<LBID_t, filebuffer_uset_t::iterator> byLBID_t;
//   byLBID_t byLBID;
//   pair<byLBID_t::iterator, byLBID_t::iterator> itList;
//   filebuffer_uset_t::iterator it;
//   uint32_t count = oids.size();

//   boost::mutex::scoped_lock lk(fWLock);

//   if (fReportFrequency)
//   {
//     std::set<BRM::LogicalPartition>::iterator sit;
//     fLog << "flushPartition oids: ";
//     for (uint32_t i = 0; i < count; i++)
//     {
//       fLog << oids[i] << ", ";
//     }
//     fLog << "flushPartition partitions: ";
//     for (sit = partitions.begin(); sit != partitions.end(); ++sit)
//     {
//       fLog << (*sit).toString() << ", ";
//     }
//     fLog << endl;
//   }

//   if (fCacheSize == 0 || oids.size() == 0 || partitions.size() == 0)
//     return;

//   /* Index the cache by LBID */
//   for (it = fbSet.begin(); it != fbSet.end(); it++)
//     byLBID.insert(pair<LBID_t, filebuffer_uset_t::iterator>(it->lbid, it));

//   for (i = 0; i < count; i++)
//   {
//     extents.clear();
//     err = dbrm.getExtents(oids[i], extents, true, true, true);  // @Bug 3838 Include outofservice
//     extents

//     if (err < 0)
//     {
//       lk.unlock();
//       flushCache();  // better than returning an error code to the user
//       return;
//     }

//     for (currentExtent = 0; currentExtent < extents.size(); currentExtent++)
//     {
//       EMEntry& range = extents[currentExtent];

//       LogicalPartition logicalPartNum(range.dbRoot, range.partitionNum, range.segmentNum);

//       if (partitions.find(logicalPartNum) == partitions.end())
//         continue;

//       LBID_t lastLBID = range.range.start + (range.range.size * 1024);

//       for (currentLBID = range.range.start; currentLBID < lastLBID; currentLBID++)
//       {
//         itList = byLBID.equal_range(currentLBID);

//         for (byLBID_t::iterator tmpIt = itList.first; tmpIt != itList.second; tmpIt++)
//         {
//           fbList.erase(fFBPool[tmpIt->second->poolIdx].listLoc());
//           fEmptyPoolSlots.push_back(tmpIt->second->poolIdx);
//           fbSet.erase(tmpIt->second);
//           fCacheSize--;
//         }
//       }
//     }
//   }
// }

void FileBufferMgr::flushPartition(const vector<OID_t>& oids, const set<BRM::LogicalPartition>& partitions)
{
  // //std::cout << " flushPartition" << std::endl;

  DBRM dbrm;
  uint32_t i;
  vector<EMEntry> extents;
  int err;
  uint32_t currentExtent;
  LBID_t currentLBID;
  typedef tr1::unordered_multimap<LBID_t, filebuffer_uset_t::iterator> byLBID_t;
  byLBID_t byLBID;
  pair<byLBID_t::iterator, byLBID_t::iterator> itList;
  filebuffer_uset_t::iterator it;
  uint32_t count = oids.size();

  // WIP This expression should be refactored if possible.
  std::scoped_lock overalLock(fWLocks[0], fWLocks[1], fWLocks[2], fWLocks[3], fWLocks[4], fWLocks[5],
                              fWLocks[6], fWLocks[7]);

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

  if (oids.size() == 0 || partitions.size() == 0)
    return;

  /* Index the cache by LBID */
  for (auto fbSet : fbSets)
  {
    for (auto it = fbSet.begin(); it != fbSet.end(); it++)
      byLBID.insert({it->lbid, it});
  }

  for (i = 0; i < count; i++)
  {
    extents.clear();
    err = dbrm.getExtents(oids[i], extents, true, true, true);  // @Bug 3838 Include outofservice extents

    if (err < 0)
    {
      return flushCache();
    }

    utils::Hasher64_r hasher;

    for (currentExtent = 0; currentExtent < extents.size(); currentExtent++)
    {
      EMEntry& range = extents[currentExtent];

      LogicalPartition logicalPartNum(range.dbRoot, range.partitionNum, range.segmentNum);

      if (partitions.find(logicalPartNum) == partitions.end())
        continue;

      LBID_t lastLBID = range.range.start + (range.range.size * 1024);

      for (currentLBID = range.range.start; currentLBID < lastLBID; currentLBID++)
      {
        auto fbSetRange4LBID = byLBID.equal_range(currentLBID);

        for (auto lbidAndfbSetPair = fbSetRange4LBID.first; lbidAndfbSetPair != fbSetRange4LBID.second;
             ++lbidAndfbSetPair)
        {
          auto [lbid, fbSetIter] = lbidAndfbSetPair;
          // WIP make a simplier hasher to avoid needless complex math every loop iter.
          size_t bucket = hasher(&lbid, sizeof(lbid)) % PartitionsNumber;
          if (!fCacheSizes[bucket])
            return;
          fbLists[bucket].erase(fFBPool[lbidAndfbSetPair->second->poolIdx].listLoc());
          fEmptyPoolsSlots[bucket].push_back(lbidAndfbSetPair->second->poolIdx);
          fbSets[bucket].erase(lbidAndfbSetPair->second);
          --fCacheSizes[bucket];
        }
      }
    }
  }
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
  utils::Hasher64_r hasher;
  size_t bucket = hasher(&keyFb.lbid, sizeof(keyFb.lbid)) % PartitionsNumber;
  std::scoped_lock lk(fWLocks[bucket]);

  auto it = fbSets[bucket].find(keyFb);

  if (fbSets[bucket].end() != it)
  {
    FileBuffer* fb = &(fFBPool[it->poolIdx]);
    fFBPool[it->poolIdx].listLoc()->hits++;
    fbLists[bucket].splice(fbLists[bucket].begin(), fbLists[bucket], (fFBPool[it->poolIdx]).listLoc());
    return fb;
  }

  return nullptr;
}

bool FileBufferMgr::find(const HashObject_t& keyFb, FileBuffer& fb)
{
  std::cout << " find1 " << keyFb.lbid << std::endl;

  utils::Hasher64_r hasher;
  size_t bucket = hasher(&keyFb.lbid, sizeof(keyFb.lbid)) % PartitionsNumber;
  std::scoped_lock lk(fWLocks[bucket]);

  auto it = fbSets[bucket].find(keyFb);

  if (fbSets[bucket].end() != it)
  {
    fFBPool[it->poolIdx].listLoc()->hits++;
    fbLists[bucket].splice(fbLists[bucket].begin(), fbLists[bucket], (fFBPool[it->poolIdx]).listLoc());
    fb = fFBPool[it->poolIdx];
    return true;
  }

  return false;
}

bool FileBufferMgr::find(const HashObject_t& keyFb, void* bufferPtr)
{
  std::cout << " find2 " << keyFb.lbid << std::endl;

  utils::Hasher64_r hasher;
  size_t bucket = hasher(&keyFb.lbid, sizeof(keyFb.lbid)) % PartitionsNumber;

  bool found = false;
  uint32_t idx = 0;
  {
    std::scoped_lock lk(fWLocks[bucket]);

    auto it = fbSets[bucket].find(keyFb);

    if (fbSets[bucket].end() != it)
    {
      found = true;
      idx = it->poolIdx;
      std::cout << " find2 " << keyFb.lbid << " idx " << idx << std::endl;

      //@bug 669 LRU cache, move block to front of list as last recently used.
      fFBPool[idx].listLoc()->hits++;
      fbLists[bucket].splice(fbLists[bucket].begin(), fbLists[bucket], (fFBPool[idx]).listLoc());
      // lk.unlock();
      // memcpy(bufferPtr, (fFBPool[idx]).getData(), 8192);
      // return true;
    }
  }
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
  filebuffer_uset_iter_t* it = (filebuffer_uset_iter_t*)alloca(count * sizeof(filebuffer_uset_iter_t));
  uint32_t* indexes = (uint32_t*)alloca(count * 4);
  utils::Hasher64_r hasher;

  // It is worth to distribute lbids into buckets before going over them
  for (uint32_t i = 0; i < count; i++)
  {
    // std::cout << " bulkFind l " << std::dec << lbids[i] << std::endl;
    new ((void*)&it[i]) filebuffer_uset_iter_t();
    size_t bucket = hasher(lbids + i, sizeof(lbids[0])) % PartitionsNumber;
    std::scoped_lock lk(fWLocks[bucket]);

    it[i] = fbSets[bucket].find(HashObject_t(lbids[i], vers[i], 0));

    if (it[i] != fbSets[bucket].end())
    {
      const auto poolIdx = it[i]->poolIdx;
      indexes[i] = poolIdx;
      // std::cout << " bulkFind l " << lbids[i] << " i " << indexes[i] << std::endl;

      wasCached[i] = true;
      fFBPool[poolIdx].listLoc()->hits++;
      // There is a potential race b/w the next line and memcpy below.
      // Less realistic to happen b/c this algo puts the used list entries at the front of the list.
      // WIP can be future improved the memcpy block in this loop but outside the crit section.
      fbLists[bucket].splice(fbLists[bucket].begin(), fbLists[bucket], (fFBPool[poolIdx]).listLoc());
    }
    else
    {
      wasCached[i] = false;
      indexes[i] = 0;
    }
  }

  // lk.unlock();
  for (uint32_t i = 0; i < count; i++)
  {
    if (wasCached[i])
    {
      memcpy(buffers[i], fFBPool[indexes[i]].getData(), 8192);
      // int64_t* v = (int64_t*)(buffers[i]);
      // std::cout << " bulkFind l " << lbids[i] << " v " << std::hex << *v << std::dec << std::endl;

      ret++;
    }
    it[i].filebuffer_uset_iter_t::~filebuffer_uset_iter_t();
  }

  return ret;
}

bool FileBufferMgr::exists(const HashObject_t& fb)
{
  std::cout << " exists2 " << fb.lbid << std::endl;

  utils::Hasher64_r hasher;
  size_t bucket = hasher(&fb.lbid, sizeof(fb.lbid)) % PartitionsNumber;
  std::scoped_lock lk(fWLocks[bucket]);

  filebuffer_uset_iter_t it = fbSets[bucket].find(fb);

  if (it != fbSets[bucket].end())
  {
    // std::cout << " exists2 true " << fb.lbid << std::endl;
    fFBPool[it->poolIdx].listLoc()->hits++;
    fbLists[bucket].splice(fbLists[bucket].begin(), fbLists[bucket], (fFBPool[it->poolIdx]).listLoc());
    return true;
  }

  return false;
}

// default insert operation.
// add a new fb into fbMgr and to fbList
// add to the front and age out from the back
// so add new fbs to the front of the list
//@bug 665: keep filebuffer in a vector. HashObject keeps the index of the filebuffer

// int FileBufferMgr::insert(const BRM::LBID_t lbid, const BRM::VER_t ver, const uint8_t* data)
// {
//   int ret = 0;
//   //std::cout << " insert lbid " << lbid << " ver " << ver << std::endl;

//   if (gPMProfOn && gPMStatsPtr)
//     gPMStatsPtr->markEvent(lbid, pthread_self(), gSession, 'I');
//   utils::Hasher64_r hasher;
//   size_t bucket = hasher(&lbid, sizeof(lbid)) % PartitionsNumber;
//   HashObject_t fbIndex(lbid, ver, 0);
//   std::scoped_lock<std::mutex> lk(fWLocks[bucket]);

//   filebuffer_pair_t pr = fbSets[bucket].insert(fbIndex);

//   if (pr.second)
//   {
//     // It was inserted (it wasn't there before)
//     // Right now we have an invalid cache: we have inserted an entry with a -1 index.
//     // We need to fix this quickly...
//     fCacheSizes[bucket]++;
//     FBData_t fbdata = {lbid, ver, 0};
//     fbLists[bucket].push_front(fbdata);
//     fBlksLoaded++;

//     if (fReportFrequency && (fBlksLoaded % fReportFrequency) == 0)
//     {
//       struct timespec tm;
//       clock_gettime(CLOCK_MONOTONIC, &tm);
//       fLog << "insert: " << left << fixed << ((double)(tm.tv_sec + (1.e-9 * tm.tv_nsec))) << " " <<
//       right
//            << setw(12) << fBlksLoaded << " " << right << setw(12) << fBlksNotUsed << endl;
//     }
//   }
//   else
//   {
//     // if it's a duplicate there's nothing to do
//     if (gPMProfOn && gPMStatsPtr)
//       gPMStatsPtr->markEvent(lbid, pthread_self(), gSession, 'D');
//     return ret;
//   }

//   uint32_t pi = numeric_limits<int>::max();

//   if (fCacheSizes[bucket] > maxCacheSize())
//   {
//     // If the insert above caused the cache to exceed its max size, find the lru block in
//     // the cache and use its pool index to store the block data.
//     FBData_t& fbdata = fbLists[bucket].back();  // the lru block
//     HashObject_t lastFB(fbdata.lbid, fbdata.ver, 0);
//     filebuffer_uset_iter_t iter = fbSets[bucket].find(lastFB);  // should be there

//     idbassert(iter != fbSets[bucket].end());
//     pi = iter->poolIdx;
//     idbassert(pi < maxCacheSize());
//     idbassert(pi < fFBPool.size());

//     // set iters are always const. We are not changing the hash here, and this gets us
//     // the pointer we need cheaply...
//     HashObject_t& ref = const_cast<HashObject_t&>(*pr.first);
//     ref.poolIdx = pi;

//     // replace the lru block with this block
//     FileBuffer fb(lbid, ver, NULL, 0);
//     fFBPool[pi] = fb;
//     fFBPool[pi].setData(data, 8192);
//     fbSets[bucket].erase(iter);

//     if (fbLists[bucket].back().hits == 0)
//       fBlksNotUsed++;

//     fbLists[bucket].pop_back();
//     --fCacheSizes[bucket];
//     depleteCache(bucket);
//     ret = 1;
//   }
//   else
//   {
//     if (!fEmptyPoolsSlots[bucket].empty())
//     {
//       pi = fEmptyPoolsSlots[bucket].front();
//       //std::cout << " insert lbid " << lbid << " ver " << ver << " pi " << pi << std::endl;

//       fEmptyPoolsSlots[bucket].pop_front();
//       FileBuffer fb(lbid, ver, NULL, 0);
//       fFBPool[pi] = fb;
//       fFBPool[pi].setData(data, 8192);
//     }
//     else
//     {
//       pi = fFBPool.size();
//       //std::cout << " insert lbid " << lbid << " ver " << ver << " pi " << pi << std::endl;

//       FileBuffer fb(lbid, ver, NULL, 0);
//       fFBPool.push_back(fb);
//       fFBPool[pi].setData(data, 8192);
//     }

//     // See comment above
//     HashObject_t& ref = const_cast<HashObject_t&>(*pr.first);
//     ref.poolIdx = pi;
//     ret = 1;
//   }

//   idbassert(pi < fFBPool.size());
//   fFBPool[pi].listLoc(fbLists[bucket].begin());

//   if (gPMProfOn && gPMStatsPtr)
//     gPMStatsPtr->markEvent(lbid, pthread_self(), gSession, 'J');

//   idbassert(fCacheSizes[bucket] <= maxCacheSize());
//   return ret;
// }

// The bucket lock must be taken calling this method
void FileBufferMgr::depleteCache(const size_t bucket)
{
  // std::cout << " depleteCache" << std::endl;

  for (uint32_t i = 0; i < fDeleteBlocks && !fbLists[bucket].empty(); ++i)
  {
    FBData_t fbdata(fbLists[bucket].back());  // the lru block
    HashObject_t lastFB(fbdata.lbid, fbdata.ver, 0);
    auto iter = fbSets[bucket].find(lastFB);

    idbassert(iter != fbSets[bucket].end());
    uint32_t idx = iter->poolIdx;
    idbassert(idx < fFBPool.size());
    // Save position in FileBuffer pool for reuse.
    fEmptyPoolsSlots[bucket].push_back(idx);
    fbSets[bucket].erase(iter);

    if (fbLists[bucket].back().hits == 0)
      fBlksNotUsed++;

    fbLists[bucket].pop_back();
    fCacheSizes[bucket]--;
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
void FileBufferMgr::updateLRU(const FBData_t& f, const size_t bucket)
{
  // std::cout << " updateLRU" << std::endl;

  if (fCacheSizes[bucket] > maxCacheSize())
  {
    auto last = fbLists[bucket].end();
    --last;
    FBData_t& fbdata = *last;
    HashObject_t lastFB(fbdata.lbid, fbdata.ver, 0);
    auto iter = fbSets[bucket].find(lastFB);
    fEmptyPoolsSlots[bucket].push_back(iter->poolIdx);

    if (!fbdata.hits)
      fBlksNotUsed++;

    fbSets[bucket].erase(iter);
    fbLists[bucket].splice(fbLists[bucket].begin(), fbLists[bucket], last);
    fbdata = f;
    --fCacheSizes[bucket];
  }
  else
  {
    fbLists[bucket].push_front(f);
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
      size_t bucket = partition(op.Lbid());
      std::scoped_lock lk(fWLocks[bucket]);
      filebuffer_pair_t pr = fbSets[bucket].insert(fbIndex);

      if (!pr.second)
      {
        continue;
      }

      if (fReportFrequency)
      {
        fLog << op.lbid << " " << op.ver << ", ";
      }

      ++fCacheSizes[bucket];
      ++fBlksLoaded;
      FBData_t fbdata = {op.lbid, op.ver, 0};
      updateLRU(fbdata, bucket);
      HashObject_t& ref = const_cast<HashObject_t&>(*pr.first);

      // crit section to put the block into the buffer pool
      // WIP what if shared blocks are better
      {
        std::scoped_lock lk(fBufferWLock);
        int32_t pi = doBlockCopy(op.lbid, op.ver, op.data, bucket);
        // int64_t* v = (int64_t*)(op.data);
        // std::cout << "bulkInsert lbid " << op.lbid << " ver " << op.ver << " pi " << pi << " v " <<
        // std::hex
        //           << *v << std::dec << std::endl;
        ref.poolIdx = pi;
        fFBPool[pi].listLoc(fbLists[bucket].begin());  // updateLRU sets the front of the list
      }

      ++ret;
      idbassert(fCacheSizes[bucket] <= maxCacheSize());  // WIP!!!!!
    }
  }
  if (fReportFrequency)
  {
    fLog << endl;
  }

  return ret;
}

}  // namespace dbbc
/* Copyright (C) 2014 InfiniDB, Inc.

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
 *   $Id: filebuffermgr.h 2042 2013-01-30 16:12:54Z pleblanc $
 *
 *                                                                         *
 ***************************************************************************/

#pragma once

#include <boost/unordered/unordered_flat_set.hpp>
#include <concepts>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <vector>

#include "brmtypes.h"
#include "extentmap.h"
#include "hasher.h"
#include "primitivemsg.h"
#include "blocksize.h"
#include "filebuffer.h"
#include "rwlock_local.h"

/**
        @author Jason Rodriguez <jrodriguez@calpont.com>
*/

/**
 * @brief manages storage of Disk Block Buffers via and LRU cache using the stl classes unordered_set and
 *list.
 *
 **/

namespace dbbc
{

class FileBufferMgrTest;  // WIP remove
class FileBufferMgrTest_bulkInsert_Test;
class FileBufferMgrTest_flushOIDs_Test;
/**
 * @brief used as the hasher algorithm for the unordered_set used to store the disk blocks
 **/

struct FileBufferIndex
{
  FileBufferIndex(BRM::LBID_t l, BRM::VER_t v, uint32_t p) : lbid(l), ver(v), poolIdx(p)
  {
  }
  BRM::LBID_t lbid;
  BRM::VER_t ver;
  uint32_t poolIdx;
};

struct CacheInsert_t
{
  CacheInsert_t(const BRM::LBID_t& l, const BRM::VER_t v, const uint8_t* d) : lbid(l), ver(v), data(d)
  {
  }
  BRM::LBID_t lbid;
  BRM::VER_t ver;
  const uint8_t* data;

  BRM::LBID_t Lbid() const
  {
    return lbid;
  }

  BRM::VER_t Verid() const
  {
    return ver;
  }
};

using CacheInsertVec = std::vector<CacheInsert_t>;
using EMEntriesVec = std::optional<vector<BRM::EMEntry>>;

typedef FileBufferIndex HashObject_t;

class bcHasher
{
 public:
  inline size_t operator()(const HashObject_t& rhs) const
  {
    return (((rhs.ver & 0xffffULL) << 48) | (rhs.lbid & 0xffffffffffffULL));
  }
};

class bcEqual
{
 public:
  inline bool operator()(const HashObject_t& f1, const HashObject_t& f2) const
  {
    return ((f1.lbid == f2.lbid) && (f1.ver == f2.ver));
  }
};

inline bool operator<(const HashObject_t& f1, const HashObject_t& f2)
{
  return ((f1.lbid < f2.lbid) || (f1.lbid == f2.lbid && f1.ver < f2.ver));
}

class FileBufferMgr
{
 public:
  static constexpr const size_t PartitionsNumber = 8;

  using filebuffer_uset_t = boost::unordered_flat_set<HashObject_t, bcHasher, bcEqual>;
  typedef std::deque<uint32_t> emptylist_t;

  /**
   * @brief ctor. Set max buffer size to numBlcks and block buffer size to blckSz
   **/

  FileBufferMgr(uint32_t numBlcks, uint32_t blckSz = BLOCK_SIZE, uint32_t deleteBlocks = 0);

  /**
   * @brief default dtor
   **/
  virtual ~FileBufferMgr();

  /**
   * @brief return TRUE if the Disk block lbid@ver is loaded into the Disk Block Buffer cache otherwise return
   *FALSE.
   **/
  bool exists(const BRM::LBID_t& lbid, const BRM::VER_t ver);

  /**
   * @brief return TRUE if the Disk block referenced by fb is loaded into the Disk Block Buffer cache
   *otherwise return FALSE.
   **/
  bool exists(const HashObject_t& fb);

  /**
   * @brief add the Disk Block reference by fb into the Disk Block Buffer Cache
   **/
  // int insert(const BRM::LBID_t lbid, const BRM::VER_t ver, const uint8_t* data);

  int bulkInsert(const CacheInsertVec&);

  /**
   * @brief returns the total number of Disk Blocks in the Cache
   **/
  // uint32_t size() const
  // {
  //   return fbSet.size();
  // }

  void takeLocksAndflushCache();

  void flushCache();

  /**
   * @brief
   **/
  void flushOne(const BRM::LBID_t lbid, const BRM::VER_t ver);

  /**
   * @brief
   **/
  void flushMany(const LbidAtVer* laVptr, uint32_t cnt);

  /**
   * @brief  flush all versions
   **/
  void flushManyAllversion(const BRM::LBID_t* laVptr, uint32_t cnt);

  void flushOIDs(const uint32_t* oids, uint32_t count);
  void flushPartition(const std::vector<BRM::OID_t>& oids, const std::set<BRM::LogicalPartition>& partitions);
  template <typename OIDsContainer>
  EMEntriesVec getExtentsByOIDs(OIDsContainer oids, const uint32_t count) const;
  template <typename Invokable>
    requires std::invocable<Invokable, BRM::EMEntry&>
  void flushExtents(const vector<BRM::EMEntry>& extents, Invokable notInPartitions);
  // void flushExtents(const vector<BRM::EMEntry>& extents);

  /**
   * @brief return the disk Block referenced by fb
   **/

  FileBuffer* findPtr(const HashObject_t& keyFb);

  bool find(const HashObject_t& keyFb, FileBuffer& fb);

  /**
   * @brief return the disk Block referenced by bufferPtr
   **/

  bool find(const HashObject_t& keyFb, void* bufferPtr);
  uint32_t bulkFind(const BRM::LBID_t* lbids, const BRM::VER_t* vers, uint8_t** buffers, bool* wasCached,
                    uint32_t blockCount);

  uint32_t maxCacheSize() const
  {
    return fMaxNumBlocks;
  }

  void setReportingFrequency(const uint32_t d);
  uint32_t ReportingFrequency() const
  {
    return fReportFrequency;
  }

  std::ostream& formatLRUList(std::ostream& os) const;

 private:
  uint32_t fMaxNumBlocks;  // the max number of blockSz blocks to keep in the Cache list
  uint32_t fBlockSz;       // size in bytes size of a data block - probably 8

  std::mutex fBufferWLock;
  std::array<std::mutex, PartitionsNumber> fWLocks;

  // mutable filebuffer_uset_t fbSet;
  std::array<filebuffer_uset_t, PartitionsNumber> fbSets;

  // mutable filebuffer_list_t fbList;        // rename this
  std::array<filebuffer_list_t, PartitionsNumber> fbLists;  // rename this

  uint32_t fCacheSize;
  std::vector<size_t> fCacheSizes = std::vector<size_t>(PartitionsNumber, 0);

  FileBufferPool_t fFBPool;  // ve)ctor<FileBuffer>
  uint32_t fDeleteBlocks;
  // emptylist_t fEmptyPoolSlots;                // keep track of FBPool slots that can be reused
  std::array<emptylist_t, PartitionsNumber>
      fEmptyPoolsSlots;  // keep track of FBPool slots that can be reused

  // void depleteCache();
  void depleteCache(const size_t bucket);

  uint64_t fBlksLoaded;       // number of blocks inserted into cache
  uint64_t fBlksNotUsed;      // number of blocks inserted and not used
  uint64_t fReportFrequency;  // how many blocks are read between reports
  std::ofstream fLog;
  config::Config* fConfig;

  // do not implement
  FileBufferMgr(const FileBufferMgr& fbm);
  const FileBufferMgr& operator=(const FileBufferMgr& fbm);

  // used by bulkInsert
  void updateLRU(const FBData_t& f, const size_t bucket);
  uint32_t doBlockCopy(const BRM::LBID_t& lbid, const BRM::VER_t& ver, const uint8_t* data,
                       const size_t bucket);

  size_t partition(const BRM::LBID_t lbid) const
  {
    utils::Hasher64_r hasher;
    return hasher(&lbid, sizeof(BRM::LBID_t)) % PartitionsNumber;
  }

  friend FileBufferMgrTest;
  // friend FileBufferMgrTest_bulkInsert_Test;
  // friend FileBufferMgrTest_flushOIDs_Test;
};

template <typename Invokable>
  requires std::invocable<Invokable, BRM::EMEntry&>
void FileBufferMgr::flushExtents(const vector<BRM::EMEntry>& extents, Invokable notInPartitions)
{
  using byLBID_t = std::unordered_multimap<BRM::LBID_t, filebuffer_uset_t::iterator>;
  // Take multiple mutexes in a safe way w/o a loop.
  std::scoped_lock overalLock(fWLocks[0], fWLocks[1], fWLocks[2], fWLocks[3], fWLocks[4], fWLocks[5],
                              fWLocks[6], fWLocks[7]);

  /* Index the cache by LBID */
  // TODO take only those sets that are affected by this flush.
  byLBID_t byLBID;
  for (auto& fbSet : fbSets)
  {
    for (auto it = fbSet.begin(); it != fbSet.end(); ++it)
    {
      byLBID.insert({it->lbid, it});
    }
  }

  for (auto& extent : extents)
  {
    if (notInPartitions(extent))
      continue;

    const BRM::LBID_t lastLBID = extent.range.start + (extent.range.size * 1024);
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

}  // namespace dbbc

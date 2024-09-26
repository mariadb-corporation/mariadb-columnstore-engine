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

/*****************************************************************************
 * $Id: vss.cpp 1926 2013-06-30 21:18:14Z wweeks $
 *
 ****************************************************************************/

#include <iostream>
#include <sstream>
// #define NDEBUG
#include <cassert>

#include <sys/types.h>
// #include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>

#include "rwlock.h"
#include "brmtypes.h"
#include "mastersegmenttable.h"
#include "vbbm.h"
#include "extentmap.h"
#include "cacheutils.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"

#include "vss.h"

#define VSS_MAGIC_V1 0x7218db12

using namespace std;
using namespace boost;
using namespace idbdatafile;

namespace BRM
{
VSSEntry::VSSEntry() : lbid(-1), verID(0), vbFlag(false), locked(false), next(-1)
{
}

VSSEntry::VSSEntry(const LBID_t lbid, const VER_t verID, const bool vbFlag, const bool locked)
 : lbid(lbid), verID(verID), vbFlag(vbFlag), locked(locked), next(-1)
{
}

/*static*/
std::vector<std::mutex> VSSImplScaled::vssImplMutexes_(VssFactor);
std::vector<VSSImplScaled*> VSSImplScaled::vssImplInstances_(VssFactor, nullptr);
// std::vector<std::mutex> VSS::vssMutexes_(VssFactor);  // TBD
std::vector<std::mutex> VSSCluster::vssMutexes_(VssFactor);

VSSImplScaled::VSSImplScaled(unsigned key, off_t size, bool readOnly) : fVSS(key, size, readOnly)
{
}

VSSImplScaled* VSSImplScaled::makeVSSImpl(unsigned key, off_t size,
                                          const MasterSegmentTable::ShmemType shmType, bool readOnly)
{
  auto partition = shmemType2ContinuesIdx(shmType);
  assert(partition < vssImplMutexes_.size() && partition < VSSImplScaled::vssImplInstances_.size());
  std::lock_guard lk(vssImplMutexes_[partition]);

  if (vssImplInstances_[partition])
  {
    if (key != vssImplInstances_[partition]->fVSS.key())
    {
      BRMShmImpl newShm(key, size);
      vssImplInstances_[partition]->swapout(newShm);
    }

    idbassert(key == vssImplInstances_[partition]->fVSS.key());
    return vssImplInstances_[partition];
  }

  vssImplInstances_[partition] = new VSSImplScaled(key, size, readOnly);

  return vssImplInstances_[partition];
}

size_t VSSImplScaled::shmemType2ContinuesIdx(const MasterSegmentTable::ShmemType shmemType)
{
  assert(MasterSegmentTable::VSSSegment_ == 3 && MasterSegmentTable::extVSS1 == 6 &&
         shmemType >= MasterSegmentTable::extVSS1 && shmemType <= MasterSegmentTable::extVSS8 &&
         shmemType < MasterSegmentTable::end);

  return shmemType - MasterSegmentTable::extVSS1;
}

// VSS::VSS(const MasterSegmentTable::ShmemType vssShmType)
//  : vss(nullptr)
//  , hashBuckets(nullptr)
//  , storage(nullptr)
//  , r_only(false)
//  , currentVSSShmkey(-1)
//  , vssShmid(0)
//  , vssShminfo(nullptr)
//  , vssShmType_(vssShmType)
//  , vssImpl_(nullptr)
// {
// }

// VSSCluster methods
VSSCluster::VSSCluster() : vssImpl_(nullptr)
{
  for (auto shmemType : MasterSegmentTable::VssShmemTypes)
  {
    vssShards_.emplace_back(new VSSShard(shmemType));
    // vssShards_.back()->setReadOnly();
  }
}

// WIP
// The state machine here assumes that two lock-related operations comes one after another
// first comes lock_ and then release.
void VSSCluster::lock_(const LBID_t lbid, const OPS op)
{
  auto p = partition(lbid);
  vssShards_[p]->lock_(op, vssMutexes_[p]);
  vssShards_[p]->lockShard(op);
}

void VSSCluster::lock_(OPS op)
{
  for (size_t p = 0; auto& vssShard : vssShards_)
  {
    vssShard->lock_(op, vssMutexes_[p]);
    vssShard->lockShard(op);
    ++p;
  }
}

void VSSCluster::release(const LBID_t lbid, const OPS op)
{
  auto p = partition(lbid);
  vssShards_[p]->release(op);
  vssShards_[p]->releaseShard(op);
}

void VSSCluster::releaseIfNeeded(const LBID_t lbid, const OPS op)
{
  auto p = partition(lbid);
  if (vssShards_[p]->shardIsLocked())
  {
    vssShards_[p]->release(op);
    vssShards_[p]->releaseShard(op);
  }
}

void VSSCluster::release(const OPS op)
{
  for (auto& vssShard : vssShards_)
  {
    vssShard->release(op);
    vssShard->releaseShard(op);
  }
}

void VSSCluster::releaseIfNeeded(const OPS op)
{
  for (auto& vssShard : vssShards_)
  {
    if (vssShard->shardIsLocked())
    {
      vssShard->release(op);
      vssShard->releaseShard(op);
    }
  }
}

// void VSSCluster::setReadOnly()
// {
//   for (auto& vssShard : vssShards_)
//   {
//     vssShard->setReadOnly();
//   }
// }

bool VSSCluster::isTooOld(const LBID_t lbid, const VER_t verID)
{
  auto p = partition(lbid);
  return vssShards_[p]->isTooOld(lbid, verID);
}

void VSSCluster::save(const string& filenamePrefix)
{
  const string vssFilenamePrefix = filenamePrefix + "_vss";

  // lock_(VSSCluster::READ);

  std::unique_ptr<IDBDataFile> file(
      IDBDataFile::open(IDBPolicy::getType(vssFilenamePrefix.c_str(), IDBPolicy::WRITEENG),
                        vssFilenamePrefix.c_str(), "wb", IDBDataFile::USE_VBUF));

  // WIP

  if (file->seek(sizeof(VSSCluster::Header), SEEK_SET))
  {
    log_errno("VSSCluster::save()");
    throw runtime_error("VSSCluster::save(): Failed to skip header in the file");
  }

  size_t totalEntriesNum = 0;

  for (auto& vssShard : vssShards_)
  {
    totalEntriesNum += vssShard->save(*file);
  }

  if (file->seek(0, SEEK_SET))
  {
    log_errno("VSSCluster::save()");
    throw runtime_error("VSSCluster::save(): Failed to seek to write header in the file");
  }

  // WIP make this max a const
  if (totalEntriesNum > std::numeric_limits<int32_t>::max())
  {
    log_errno("VSSCluster::save()");
    std::ostringstream oss;
    oss << "VSSCluster::save(): Too many entries to save: " << totalEntriesNum;
    throw runtime_error(oss.str());
  }

  struct Header header
  {
    .magic = VSS_MAGIC_V1, .entries = static_cast<uint32_t>(totalEntriesNum)
  };

  if (file->write((char*)&header, sizeof(header)) != sizeof(header))
  {
    log_errno("VSSCluster::save()");
    throw runtime_error("VSSCluster::save(): Failed to write header to the file");
  }

  // release(VSSCluster::READ);
}

void VSSCluster::load(const string& filenamePrefix)
{
  string vssFilenamePrefix = filenamePrefix + "_vss";

  // lock_(VSSCluster::WRITE);

  unique_ptr<IDBDataFile> in(
      IDBDataFile::open(IDBPolicy::getType(vssFilenamePrefix.c_str(), IDBPolicy::WRITEENG),
                        vssFilenamePrefix.c_str(), "rb", 0));

  if (!in)
  {
    log_errno("VSSCluster::load()");
    throw runtime_error("VSSCluster::load(): Failed to open the file");
  }

  struct Header header;
  if (in->read((char*)&header, sizeof(header)) != sizeof(header))
  {
    log_errno("VSSCluster::load()");
    throw runtime_error("VSSCluster::load(): Failed to read header");
  }

  if (header.magic != VSS_MAGIC_V1)
  {
    log("VSSCluster::load(): Bad magic.  Not a VSS file?");
    throw runtime_error("VSSCluster::load(): Bad magic.  Not a VSS file?");
  }

  // WIP make this max a const
  if (header.entries >= std::numeric_limits<int32_t>::max())
  {
    log("VSSCluster::load(): Bad size.  Not a VSS file?");
    throw runtime_error("VSSCluster::load(): Bad size.  Not a VSS file?");
  }

  size_t readSize = header.entries * sizeof(VSSEntry);
  auto readBuf = std::make_unique<char[]>(readSize);
  size_t progress = 0;
  int err;
  while (progress < readSize)
  {
    err = in->read(readBuf.get() + progress, readSize - progress);
    if (err < 0)
    {
      log_errno("VSS::load()");
      throw runtime_error("VSS::load(): Failed to load, check the critical log file");
    }
    else if (err == 0)
    {
      log("VSS::load(): Got early EOF");
      throw runtime_error("VSS::load(): Got early EOF");
    }
    progress += err;
  }

  const char* readBufPtr = readBuf.get();
  for (auto& vssShard : vssShards_)
  {
    vssShard->growForLoad_(header.entries);
  }

  const VSSEntry* loadedEntries = reinterpret_cast<const VSSEntry*>(readBufPtr);
  for (uint32_t i = 0; i < header.entries; ++i)
  {
    // NB can be future optimized b/c all entries belong to the same partition
    // locate sequentially in the
    auto p = VSSCluster::partition(loadedEntries[i].lbid);
    vssShards_[p]->insert_(loadedEntries[i].lbid, loadedEntries[i].verID, loadedEntries[i].vbFlag,
                           loadedEntries[i].locked, true);
  }

  // for (auto& vssShard : vssShards_)
  // {
  //   vssShard->load(readBufPtr, header.entries);
  // }

  // release(VSSCluster::WRITE);
}

// WIP
bool VSSCluster::isEmpty(const LBID_t lbid, const bool lockShard)
{
  auto p = partition(lbid);

  if (!lockShard)
  {
    return vssShards_[p]->isEmpty(lockShard);
  }

  vssShards_[p]->lock_(VSSCluster::READ, vssMutexes_[p]);

  auto ret = vssShards_[p]->isEmpty(lockShard);

  release(VSSCluster::READ);

  return ret;
}

int VSSCluster::lookup(LBID_t lbid, const QueryContext_vss& verInfo, VER_t txnID, VER_t* outVer, bool* vbFlag,
                       bool vbOnly) const
{
  auto p = partition(lbid);

  // vssShards_[p]->lock_(VSSCluster::READ, vssMutexes_[p]);
  // vssShards_[p]->lockShard(VSSCluster::READ);

  return vssShards_[p]->lookup(lbid, verInfo, txnID, outVer, vbFlag, vbOnly);

  // vssShards_[p]->release(VSSCluster::READ);
  // vssShards_[p]->releaseShard(VSSCluster::READ);

  // return rc;
}

VER_t VSSCluster::getCurrentVersion(LBID_t lbid, bool* isLocked) const
{
  auto p = partition(lbid);

  vssShards_[p]->lock_(VSSCluster::READ, vssMutexes_[p]);
  vssShards_[p]->lockShard(VSSCluster::READ);

  auto res = vssShards_[p]->getCurrentVersion(lbid, isLocked);

  vssShards_[p]->release(VSSCluster::READ);
  vssShards_[p]->releaseShard(VSSCluster::READ);

  return res;
}

VER_t VSSCluster::getCurrentVersionWExtLock(LBID_t lbid, bool* isLocked) const
{
  auto p = partition(lbid);

  return vssShards_[p]->getCurrentVersion(lbid, isLocked);
}

// lockShard is used by SlaveDBRMNode::vbRollback
VER_t VSSCluster::getHighestVerInVB(LBID_t lbid, VER_t max, const bool lockShard) const
{
  auto p = partition(lbid);
  if (!lockShard)
  {
    return vssShards_[p]->getHighestVerInVB(lbid, max);
  }

  vssShards_[p]->lock_(VSSCluster::READ, vssMutexes_[p]);
  vssShards_[p]->lockShard(VSSCluster::READ);

  auto res = vssShards_[p]->getHighestVerInVB(lbid, max);

  vssShards_[p]->release(VSSCluster::READ);
  vssShards_[p]->releaseShard(VSSCluster::READ);

  return res;
}

bool VSSCluster::isVersioned(LBID_t lbid, VER_t version) const
{
  auto p = partition(lbid);

  vssShards_[p]->lock_(VSSCluster::READ, vssMutexes_[p]);
  vssShards_[p]->lockShard(VSSCluster::READ);

  auto res = vssShards_[p]->isVersioned(lbid, version);

  vssShards_[p]->release(VSSCluster::READ);
  vssShards_[p]->releaseShard(VSSCluster::READ);

  return res;
}

// WIP TBD There can be a race in the original algo when CF moves locking only one partition at a time.
// Whilst the same txn can insert into the head.
void VSSCluster::getUncommittedLBIDs(VER_t txnID, vector<LBID_t>& lbids)
{
  for (size_t p = 0; auto& vssShard : vssShards_)
  {
    vssShard->lock_(VSSCluster::READ, vssMutexes_[p]);
    vssShard->lockShard(VSSCluster::READ);

    vssShard->getUncommittedLBIDs(txnID, lbids);

    vssShard->release(VSSCluster::READ);
    vssShard->releaseShard(VSSCluster::READ);
    ++p;
  }
}

void VSSCluster::getCurrentTxnIDs(set<VER_t>& txnList)
{
  for (auto& vssShard : vssShards_)
  {
    vssShard->getCurrentTxnIDs(txnList);
  }
}

void VSSCluster::getUnlockedLBIDs(BlockList_t& lbids)
{
  for (auto& vssShard : vssShards_)
  {
    vssShard->getUnlockedLBIDs(lbids);
  }
}

// This method needs an explicit external lock on a VSS cluster
LbidVersionVec VSSCluster::removeEntryFromDB(const LBID_t& lbid)
{
  auto p = partition(lbid);
  return vssShards_[p]->removeEntryFromDB(lbid);
}

// To be used in SlaveDBRMNode::deleteOID
// This method needs an explicit external lock on a VSS cluster
// LbidVersionVec VSSCluster::removeEntriesFromDB(const LBIDRange_v& lbidRanges)
// {
//   LbidVersionVec res;

//   // WIP resize res in front
//   for (auto lbidRange : lbidRanges)
//   {
//     for (auto lbid = lbidRange.start; lbid < lbidRange.start + lbidRange.size; ++lbid)
//     {
//       auto p = partition(lbid);
//       auto lbidVersions = vssShards_[p]->removeEntryFromDB(lbid);
//       res.insert(res.end(), lbidVersions.begin(), lbidVersions.end());
//     }
//   }

//   return res;
// }

// This method needs an explicit external lock on a VSS shard
void VSSCluster::setVBFlag(LBID_t lbid, VER_t verID, bool vbFlag)
{
  auto p = partition(lbid);

  return vssShards_[p]->setVBFlag(lbid, verID, vbFlag);
}

// This method needs an explicit external lock on a VSS shard
void VSSCluster::insert_(LBID_t lbid, VER_t verID, bool vbFlag, bool locked, bool loading)
{
  auto p = partition(lbid);

  return vssShards_[p]->insert_(lbid, verID, vbFlag, locked, loading);
}

// This method needs an explicit external lock on a VSS shard
bool VSSCluster::isLocked(const LBID_t lbid, VER_t transID) const
{
  auto p = partition(lbid);

  return vssShards_[p]->isLocked(lbid, transID);
}

// This method needs an explicit external lock on a VSS cluster
void VSSCluster::removeEntry(LBID_t lbid, VER_t verID, vector<LBID_t>* flushList)
{
  auto p = partition(lbid);

  return vssShards_[p]->removeEntry(lbid, verID, flushList);
}

// This method needs an explicit external lock on a VSS cluster
void VSSCluster::commit(VER_t txnID)
{
  for (auto& vssShard : vssShards_)
  {
    vssShard->commit(txnID);
  }
}

void VSSCluster::confirmChangesIfNeededAndRelease(const VSSCluster::OPS op)
{
  for (auto& vssShard : vssShards_)
  {
    if (vssShard->shardIsLocked())
    {
      vssShard->confirmChanges();
      vssShard->release(op);
      vssShard->releaseShard(op);
    }
  }
}

void VSSCluster::undoChangesIfNeededAndRelease(const VSSCluster::OPS op)
{
  for (auto& vssShard : vssShards_)
  {
    if (vssShard->shardIsLocked())
    {
      vssShard->undoChanges();
      vssShard->release(op);
      vssShard->releaseShard(op);
    }
  }
}

// This method needs an explicit external lock on a VSS cluster
void VSSCluster::undoChanges()
{
  for (auto& vssShard : vssShards_)
  {
    vssShard->undoChanges();
  }
}

// This method needs an explicit external lock on a VSS cluster
void VSSCluster::clear_()
{
  for (auto& vssShard : vssShards_)
  {
    vssShard->undoChanges();
  }
}

// This method needs an explicit external lock on a VSS shard
bool VSSCluster::isEntryLocked(LBID_t lbid, VER_t verID) const
{
  auto p = partition(lbid);

  return vssShards_[p]->isEntryLocked(lbid, verID);
}

// void VSS::lock_(OPS op)
// {
//   char* shmseg;
//   auto partition = VSSImplScaled::shmemType2ContinuesIdx(vssShmType_);
//   assert(partition < vssMutexes_.size());

//   if (op == READ)
//   {
//     vssShminfo = mst.getTable_read(vssShmType_);
//     vssMutexes_[partition].lock();
//   }
//   else
//     vssShminfo = mst.getTable_write(vssShmType_);

//   // this means that either the VSS isn't attached or that it was resized
//   if (!vssImpl_ || vssImpl_->key() != (unsigned)vssShminfo->tableShmkey)
//   {
//     if (vssShminfo->allocdSize == 0)
//     {
//       if (op == READ)
//       {
//         vssMutexes_[partition].unlock();
//         mst.getTable_upgrade(vssShmType_);

//         try
//         {
//           growVSS_();
//         }
//         catch (...)
//         {
//           release(WRITE);
//           throw;
//         }

//         mst.getTable_downgrade(vssShmType_);
//       }
//       else
//       {
//         try
//         {
//           growVSS_();
//         }
//         catch (...)
//         {
//           release(WRITE);
//           throw;
//         }
//       }
//     }
//     else
//     {
//       vssImpl_ = VSSImplScaled::makeVSSImpl(vssShminfo->tableShmkey, 0, vssShmType_);
//       idbassert(vssImpl_);

//       if (r_only)
//         vssImpl_->makeReadOnly();

//       vss = vssImpl_->get();
//       shmseg = reinterpret_cast<char*>(vss);
//       hashBuckets = reinterpret_cast<int*>(&shmseg[sizeof(VSSShmsegHeader)]);
//       storage =
//           reinterpret_cast<VSSEntry*>(&shmseg[sizeof(VSSShmsegHeader) + vss->numHashBuckets *
//           sizeof(int)]);

//       if (op == READ)
//         vssMutexes_[partition].unlock();
//     }
//   }
//   else
//   {
//     vss = vssImpl_->get();
//     shmseg = reinterpret_cast<char*>(vss);
//     hashBuckets = reinterpret_cast<int*>(&shmseg[sizeof(VSSShmsegHeader)]);
//     storage =
//         reinterpret_cast<VSSEntry*>(&shmseg[sizeof(VSSShmsegHeader) + vss->numHashBuckets * sizeof(int)]);

//     if (op == READ)
//       vssMutexes_[partition].unlock();
//   }
// }

// VSSShard methods

VSSShard::VSSShard(const MasterSegmentTable::ShmemType vssShmType) : vssShmType_(vssShmType)
{
}

void VSSShard::lock_(const VSSCluster::OPS op, std::mutex& vssMutex)
{
  char* shmseg;

  if (op == VSSCluster::READ)
  {
    vssShminfo = mst.getTable_read(vssShmType_);
    vssMutex.lock();
  }
  else
    vssShminfo = mst.getTable_write(vssShmType_);

  // this means that either the VSS isn't attached or that it was resized
  if (!vssImpl_ || vssImpl_->key() != (unsigned)vssShminfo->tableShmkey)
  {
    if (vssShminfo->allocdSize == 0)
    {
      if (op == VSSCluster::READ)
      {
        vssMutex.unlock();
        mst.getTable_upgrade(vssShmType_);

        try
        {
          growVSS_();
        }
        catch (...)
        {
          release(VSSCluster::WRITE);
          throw;
        }

        mst.getTable_downgrade(vssShmType_);
      }
      else
      {
        try
        {
          growVSS_();
        }
        catch (...)
        {
          release(VSSCluster::WRITE);
          throw;
        }
      }
    }
    else
    {
      vssImpl_ = VSSImplScaled::makeVSSImpl(vssShminfo->tableShmkey, 0, vssShmType_);
      idbassert(vssImpl_);

      if (r_only)
        vssImpl_->makeReadOnly();

      vss = vssImpl_->get();
      shmseg = reinterpret_cast<char*>(vss);
      hashBuckets = reinterpret_cast<int*>(&shmseg[sizeof(VSSShmsegHeader)]);
      storage =
          reinterpret_cast<VSSEntry*>(&shmseg[sizeof(VSSShmsegHeader) + vss->numHashBuckets * sizeof(int)]);

      if (op == VSSCluster::READ)
        vssMutex.unlock();
    }
  }
  else
  {
    vss = vssImpl_->get();
    shmseg = reinterpret_cast<char*>(vss);
    hashBuckets = reinterpret_cast<int*>(&shmseg[sizeof(VSSShmsegHeader)]);
    storage =
        reinterpret_cast<VSSEntry*>(&shmseg[sizeof(VSSShmsegHeader) + vss->numHashBuckets * sizeof(int)]);

    if (op == VSSCluster::READ)
      vssMutex.unlock();
  }
}

// ported from ExtentMap
void VSSShard::release(const VSSCluster::OPS op)
{
  if (op == VSSCluster::READ)
    mst.releaseTable_read(vssShmType_);
  else
    mst.releaseTable_write(vssShmType_);
}

// void VSSShard::save(string filename)
// {
//   const char* filename_p = filename.c_str();
//   scoped_ptr<IDBDataFile> out(IDBDataFile::open(IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
//                                                 filename_p, "wb", IDBDataFile::USE_VBUF));

//   if (!out)
//   {
//     log_errno("VSSShard::save()");
//     throw runtime_error("VSSShard::save(): Failed to open the file");
//   }

//   if (vss->currentSize < 0)
//   {
//     log_errno("VSSShard::save()");
//     throw runtime_error("VSSShard::save(): VSS::currentSize is negative");
//   }

//   struct VSSShard::Header header
//   {
//     .magic = VSS_MAGIC_V1, .entries = static_cast<uint32_t>(vss->currentSize)
//   };

//   if (out->write((char*)&header, sizeof(header)) != sizeof(header))
//   {
//     log_errno("VSSShard::save()");
//     throw runtime_error("VSSShard::save(): Failed to write header to the file");
//   }

//   int first = -1, last = -1, err;
//   size_t progress, writeSize;
//   for (int i = 0; i < vss->capacity; i++)
//   {
//     if (storage[i].lbid != -1 && first == -1)
//       first = i;
//     else if (storage[i].lbid == -1 && first != -1)
//     {
//       last = i;
//       writeSize = (last - first) * sizeof(VSSEntry);
//       progress = 0;
//       char* writePos = (char*)&storage[first];
//       while (progress < writeSize)
//       {
//         err = out->write(writePos + progress, writeSize - progress);
//         if (err < 0)
//         {
//           log_errno("VSSShard::save()");
//           throw runtime_error("VSSShard::save(): Failed to write the file");
//         }
//         progress += err;
//       }
//       first = -1;
//     }
//   }
//   if (first != -1)
//   {
//     writeSize = (vss->capacity - first) * sizeof(VSSEntry);
//     progress = 0;
//     char* writePos = (char*)&storage[first];
//     while (progress < writeSize)
//     {
//       err = out->write(writePos + progress, writeSize - progress);
//       if (err < 0)
//       {
//         log_errno("VSSShard::save()");
//         throw runtime_error("VSSShard::save(): Failed to write the file");
//       }
//       progress += err;
//     }
//   }
// }

uint32_t VSSShard::save(idbdatafile::IDBDataFile& file)
{
  // const char* filename_p = filename.c_str();
  // scoped_ptr<IDBDataFile> out(IDBDataFile::open(IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
  //                                               filename_p, "wb", IDBDataFile::USE_VBUF));

  // if (!out)
  // {
  //   log_errno("VSSShard::save()");
  //   throw runtime_error("VSSShard::save(): Failed to open the file");
  // }

  if (vss->currentSize < 0)
  {
    log_errno("VSSShard::save()");
    throw runtime_error("VSSShard::save(): VSS::currentSize is negative");
  }

  // struct VSSShard::Header header
  // {
  //   .magic = VSS_MAGIC_V1, .entries = static_cast<uint32_t>(vss->currentSize)
  // };

  // if (out->write((char*)&header, sizeof(header)) != sizeof(header))
  // {
  //   log_errno("VSSShard::save()");
  //   throw runtime_error("VSSShard::save(): Failed to write header to the file");
  // }

  int first = -1, last = -1, err;
  size_t progress, writeSize;
  for (int i = 0; i < vss->capacity; i++)
  {
    if (storage[i].lbid != -1 && first == -1)
      first = i;
    else if (storage[i].lbid == -1 && first != -1)
    {
      last = i;
      writeSize = (last - first) * sizeof(VSSEntry);
      progress = 0;
      char* writePos = (char*)&storage[first];
      while (progress < writeSize)
      {
        err = file.write(writePos + progress, writeSize - progress);
        if (err < 0)
        {
          log_errno("VSSShard::save()");
          throw runtime_error("VSSShard::save(): Failed to write the file");
        }
        progress += err;
      }
      first = -1;
    }
  }
  if (first != -1)
  {
    writeSize = (vss->capacity - first) * sizeof(VSSEntry);
    progress = 0;
    char* writePos = (char*)&storage[first];
    while (progress < writeSize)
    {
      err = file.write(writePos + progress, writeSize - progress);
      if (err < 0)
      {
        log_errno("VSSShard::save()");
        throw runtime_error("VSSShard::save(): Failed to write the file");
      }
      progress += err;
    }
  }

  // This invariant is checked in the begining.
  assert(vss->currentSize >= 0);
  return static_cast<uint32_t>(vss->currentSize);
}

bool VSSShard::isTooOld(const LBID_t lbid, const VER_t verID) const
{
  VER_t minVer = 0;

  const int bucket = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;

  int index = hashBuckets[bucket];

  while (index != -1)
  {
    VSSEntry* listEntry = &storage[index];

    if (listEntry->lbid == lbid && minVer > listEntry->verID)
      minVer = listEntry->verID;

    index = listEntry->next;
  }

  return (minVer > verID);
}

// Ideally, we;d like to get in and out of this fcn as quickly as possible.
// assumes read lock is held
bool VSSShard::isEmpty(const bool lockShard)
{
  return (vss->currentSize == 0);
}

// assumes read lock is held
int VSSShard::lookup(LBID_t lbid, const QueryContext_vss& verInfo, VER_t txnID, VER_t* outVer, bool* vbFlag,
                     bool vbOnly) const
{
  int hashIndex, maxVersion = -1, minVersion = -1, currentIndex;
  VSSEntry *listEntry, *maxEntry = nullptr;

  hashIndex = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;

  currentIndex = hashBuckets[hashIndex];

  while (currentIndex != -1)
  {
    listEntry = &storage[currentIndex];

    if (listEntry->lbid == lbid)
    {
      if (vbOnly && !listEntry->vbFlag)
        goto next;

      /* "if it is/was part of a different transaction, ignore it"
       */
      if (txnID != listEntry->verID && (verInfo.txns->find(listEntry->verID) != verInfo.txns->end()))
        goto next;

      /* fast exit if the exact version is found */
      if (verInfo.currentScn == listEntry->verID)
      {
        *outVer = listEntry->verID;
        *vbFlag = listEntry->vbFlag;
        return 0;
      }

      /* Track the min version of this LBID */
      if (minVersion > listEntry->verID || minVersion == -1)
        minVersion = listEntry->verID;

      /* Pick the highest version <= the SCN of the query */
      if (verInfo.currentScn > listEntry->verID && maxVersion < listEntry->verID)
      {
        maxVersion = listEntry->verID;
        maxEntry = listEntry;
      }
    }

  next:
    currentIndex = listEntry->next;
  }

  if (maxEntry != nullptr)
  {
    *outVer = maxVersion;
    *vbFlag = maxEntry->vbFlag;
    return 0;
  }
  else if (minVersion > verInfo.currentScn)
  {
    *outVer = 0;
    *vbFlag = false;
    return ERR_SNAPSHOT_TOO_OLD;
  }

  *outVer = 0;
  *vbFlag = false;
  return -1;
}

// WIP to unconditionaly return (listEntry->locked, listEntry->verID) pair
VER_t VSSShard::getCurrentVersion(LBID_t lbid, bool* isLocked) const
{
  int hashIndex, currentIndex;
  VSSEntry* listEntry;

  hashIndex = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;
  currentIndex = hashBuckets[hashIndex];

  while (currentIndex != -1)
  {
    listEntry = &storage[currentIndex];

    if (listEntry->lbid == lbid && !listEntry->vbFlag)
    {
      if (isLocked != nullptr)
        *isLocked = listEntry->locked;

      return listEntry->verID;
    }

    currentIndex = listEntry->next;
  }

  if (isLocked != nullptr)
    *isLocked = false;

  return 0;
}

VER_t VSSShard::getHighestVerInVB(LBID_t lbid, VER_t max) const
{
  int hashIndex, currentIndex;
  VER_t ret = -1;
  VSSEntry* listEntry;

  hashIndex = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;
  currentIndex = hashBuckets[hashIndex];

  while (currentIndex != -1)
  {
    listEntry = &storage[currentIndex];

    if ((listEntry->lbid == lbid && listEntry->vbFlag) && (listEntry->verID <= max && listEntry->verID > ret))
      ret = listEntry->verID;

    currentIndex = listEntry->next;
  }

  return ret;
}

bool VSSShard::isVersioned(LBID_t lbid, VER_t version) const
{
  int hashIndex, currentIndex;
  VSSEntry* listEntry;

  hashIndex = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;
  currentIndex = hashBuckets[hashIndex];

  while (currentIndex != -1)
  {
    listEntry = &storage[currentIndex];

    if (listEntry->lbid == lbid && listEntry->verID == version)
      return listEntry->vbFlag;

    currentIndex = listEntry->next;
  }

  return false;
}

// WIP replace vbbm with lambda
// Very counterintuitive to take READ and conduct changes in some edge cases.
// void VSSShard::removeEntriesFromDB(const LBIDRange& range, VBBM& vbbm, bool use_vbbm)
// {
// #ifdef BRM_DEBUG

//   if (range.start < 0)
//   {
//     log("VSS::removeEntriesFromDB(): lbids must be positive.", logging::LOG_TYPE_DEBUG);
//     throw invalid_argument("VSS::removeEntriesFromDB(): lbids must be positive.");
//   }

//   if (range.size < 1)
//   {
//     log("VSS::removeEntriesFromDB(): size must be > 0", logging::LOG_TYPE_DEBUG);
//     throw invalid_argument("VSS::removeEntriesFromDB(): size must be > 0");
//   }

// #endif

//   makeUndoRecord(vss, sizeof(VSSShmsegHeader));

//   LBID_t lastLBID = range.start + range.size - 1;

//   for (LBID_t lbid = range.start; lbid <= lastLBID; lbid++)
//   {
//     int bucket = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;
//     int index = hashBuckets[bucket];
//     int prev = -1;

//     for (; index != -1; index = storage[index].next)
//     {
//       if (storage[index].lbid == lbid)
//       {
//         if (storage[index].vbFlag && use_vbbm)
//           vbbm.removeEntry(storage[index].lbid, storage[index].verID);

//         makeUndoRecord(&storage[index], sizeof(VSSEntry));
//         storage[index].lbid = -1;

//         if (prev == -1)
//         {
//           makeUndoRecord(&hashBuckets[bucket], sizeof(int));
//           hashBuckets[bucket] = storage[index].next;
//         }
//         else
//         {
//           makeUndoRecord(&storage[prev], sizeof(VSSEntry));
//           storage[prev].next = storage[index].next;
//         }

//         vss->currentSize--;

//         if (storage[index].locked && (vss->lockedEntryCount > 0))
//           vss->lockedEntryCount--;

//         if (index < vss->LWM)
//           vss->LWM = index;
//       }
//       else
//         prev = index;
//     }
//   }
// }

// read lock
void VSSShard::getUncommittedLBIDs(VER_t txnID, vector<LBID_t>& lbids)
{
  int i;

#ifdef BRM_DEBUG

  if (txnID < 1)
  {
    log("VSSShard::getUncommittedLBIDs(): txnID must be > 0", logging::LOG_TYPE_DEBUG);
    throw invalid_argument("VSS::getUncommittedLBIDs(): txnID must be > 0");
  }

#endif

  for (i = 0; i < vss->capacity; i++)
    if (storage[i].lbid != -1 && storage[i].verID == txnID)
    {
      if (storage[i].locked == false)
      {
        log("VSSShard::getUncommittedLBIDs(): found an unlocked block with that TxnID",
            logging::LOG_TYPE_DEBUG);
        throw logic_error("VSSShard::getUncommittedLBIDs(): found an unlocked block with that TxnID");
      }

      if (storage[i].vbFlag == true)
      {
        log("VSSShard::getUncommittedLBIDs(): found a block with that TxnID in the VB",
            logging::LOG_TYPE_DEBUG);
        throw logic_error("VSSShard::getUncommittedLBIDs(): found a block with that TxnID in the VB");
      }

      lbids.push_back(storage[i].lbid);
    }
}

void VSSShard::getCurrentTxnIDs(set<VER_t>& list)
{
  int i;

  for (i = 0; i < vss->capacity; i++)
    if (storage[i].lbid != -1 && storage[i].locked)
      list.insert(storage[i].verID);
}

void VSSShard::getUnlockedLBIDs(BlockList_t& lbids)
{
  for (int i = 0; i < vss->capacity; i++)
    if (storage[i].lbid != -1 && !storage[i].locked)
      lbids.push_back(LVP_t(storage[i].lbid, storage[i].verID));
}

LbidVersionVec VSSShard::removeEntryFromDB(const LBID_t& lbid)
{
  makeUndoRecord(vss, sizeof(VSSShmsegHeader));
  auto bucket = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;
  LbidVersionVec lbidsVersions;

  int prev = -1;
  auto index = hashBuckets[bucket];
  for (; index != -1; index = storage[index].next, prev = index)
  {
    if (storage[index].lbid == lbid)
    {
      if (storage[index].vbFlag)
      {
        lbidsVersions.emplace_back(storage[index].lbid, storage[index].verID);
      }

      makeUndoRecord(&storage[index], sizeof(VSSEntry));
      storage[index].lbid = -1;

      if (prev == -1)
      {
        makeUndoRecord(&hashBuckets[bucket], sizeof(int));
        hashBuckets[bucket] = storage[index].next;
      }
      else
      {
        makeUndoRecord(&storage[prev], sizeof(VSSEntry));
        storage[prev].next = storage[index].next;
        // If the index to has next == -1, than set hash[bucket] to -1 as well.
        // hash[bucket] == -1 has the last entry in the blocks chain.
        if (storage[index].next == -1)
        {
          hashBuckets[bucket] = storage[index].next;
        }
      }

      --vss->currentSize;

      if (storage[index].locked && (vss->lockedEntryCount > 0))
        --vss->lockedEntryCount;

      if (index < vss->LWM)
        vss->LWM = index;
    }
    else
    {
      prev = index;
    }
  }

  return lbidsVersions;
}

void VSSShard::setVBFlag(LBID_t lbid, VER_t verID, bool vbFlag)
{
  int index, prev, bucket;

  index = getIndex(lbid, verID, prev, bucket);

  if (index == -1)
  {
    ostringstream ostr;

    ostr << "VSSShard::setVBFlag(): that entry doesn't exist lbid=" << lbid << " ver=" << verID;
    log(ostr.str(), logging::LOG_TYPE_DEBUG);
    throw logic_error(ostr.str());
  }

  makeUndoRecord(&storage[index], sizeof(VSSEntry));
  storage[index].vbFlag = vbFlag;
}

void VSSShard::insert_(LBID_t lbid, VER_t verID, bool vbFlag, bool locked, bool loading)
{
  VSSEntry entry(lbid, verID, vbFlag, locked);
  cerr << "Insert to vss lbid:verID:locked:vbFlag = " << entry.lbid << ":" << entry.verID << ":"
       << entry.locked << ":" << entry.vbFlag << endl;

  // check for resize
  if (vss->currentSize == vss->capacity)
    growVSS_();

  _insert(entry, vss, hashBuckets, storage, loading);

  if (!loading)
    makeUndoRecord(&vss->currentSize, sizeof(vss->currentSize));

  ++vss->currentSize;

  if (locked)
    ++vss->lockedEntryCount;
}

bool VSSShard::isLocked(const LBID_t lbid, VER_t transID) const
{
  int hashIndex, currentIndex;
  VSSEntry* listEntry;

  hashIndex = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;

  currentIndex = hashBuckets[hashIndex];
  while (currentIndex != -1)
  {
    listEntry = &storage[currentIndex];

    if (listEntry->lbid == lbid && listEntry->locked)
    {
      if (listEntry->verID == transID)
        return false;
      else
        return true;
    }

    currentIndex = listEntry->next;
  }

  return false;
}

// requires write lock
void VSSShard::removeEntry(LBID_t lbid, VER_t verID, vector<LBID_t>* flushList)
{
  int index, prev, bucket;

#ifdef BRM_DEBUG

  if (lbid < 0)
  {
    log("VSS::removeEntry(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
    throw invalid_argument("VSS::removeEntry(): lbid must be >= 0");
  }

  if (verID < 0)
  {
    log("VSS::removeEntry(): verID must be >= 0", logging::LOG_TYPE_DEBUG);
    throw invalid_argument("VSS::removeEntry(): verID must be >= 0");
  }

#endif

  index = getIndex(lbid, verID, prev, bucket);

  if (index == -1)
  {
#ifdef BRM_DEBUG
    ostringstream ostr;

    ostr << "VSS::removeEntry(): that entry doesn't exist lbid = " << lbid << " verID = " << verID;
    log(ostr.str(), logging::LOG_TYPE_DEBUG);
    throw logic_error(ostr.str());
#else
    return;
#endif
  }

  makeUndoRecord(&storage[index], sizeof(VSSEntry));
  storage[index].lbid = -1;

  if (prev != -1)
  {
    makeUndoRecord(&storage[prev], sizeof(VSSEntry));
    storage[prev].next = storage[index].next;
  }
  else
  {
    makeUndoRecord(&hashBuckets[bucket], sizeof(int));
    hashBuckets[bucket] = storage[index].next;
  }

  makeUndoRecord(vss, sizeof(VSSShmsegHeader));
  --vss->currentSize;

  if (storage[index].locked && (vss->lockedEntryCount > 0))
    --vss->lockedEntryCount;

  if (index < vss->LWM)
    vss->LWM = index;

  // scan the list of entries with that lbid to see if there are others
  // to remove.
  for (index = hashBuckets[bucket]; index != -1; index = storage[index].next)
    if (storage[index].lbid == lbid && (storage[index].vbFlag || storage[index].locked))
      return;

  // if execution gets here, we should be able to remove all entries
  // with the given lbid because none point to the VB.
  for (prev = -1, index = hashBuckets[bucket]; index != -1; index = storage[index].next)
  {
    if (storage[index].lbid == lbid)
    {
      makeUndoRecord(&storage[index], sizeof(VSSEntry));
      storage[index].lbid = -1;

      if (prev == -1)
      {
        makeUndoRecord(&hashBuckets[bucket], sizeof(int));
        hashBuckets[bucket] = storage[index].next;
      }
      else
      {
        makeUndoRecord(&storage[prev], sizeof(VSSEntry));
        storage[prev].next = storage[index].next;
      }

      --vss->currentSize;

      if (storage[index].locked && (vss->lockedEntryCount > 0))
        vss->lockedEntryCount--;

      if (index < vss->LWM)
        vss->LWM = index;
    }
    else
      prev = index;
  }

  flushList->push_back(lbid);
}

void VSSShard::commit(VER_t txnID)
{
  int i;
#ifdef BRM_DEBUG

  if (txnID < 1)
  {
    log("VSS::commit(): txnID must be > 0", logging::LOG_TYPE_DEBUG);
    throw invalid_argument("VSS::commit(): txnID must be > 0");
  }

#endif

  for (i = 0; i < vss->capacity; i++)
    if (storage[i].lbid != -1 && storage[i].verID == txnID)
    {
#ifdef BRM_DEBUG

      if (storage[i].locked != true)
      {
        ostringstream ostr;
        ostr << "VSS::commit(): An entry has already been unlocked..? txnId = " << txnID;
        log(ostr.str(), logging::LOG_TYPE_DEBUG);
        throw logic_error(ostr.str());
      }

#endif
      makeUndoRecord(&storage[i], sizeof(VSSEntry));
      storage[i].locked = false;

      // @ bug 1426 fix. Decrease the counter when an entry releases its lock.
      if (vss->lockedEntryCount > 0)
        --vss->lockedEntryCount;
    }
}

void VSSShard::clear_()
{
  auto allocSize = VssInitialSize;
  auto newshmkey = chooseShmkey_(vssShmType_);

  idbassert(vssImpl_);
  idbassert(vssImpl_->key() != newshmkey);
  vssImpl_->clear(newshmkey, allocSize);
  vssShminfo->tableShmkey = newshmkey;
  vssShminfo->allocdSize = allocSize;
  vss = vssImpl_->get();
  initShmseg();

  if (r_only)
  {
    vssImpl_->makeReadOnly();
    vss = vssImpl_->get();
  }

  auto newshmseg = reinterpret_cast<char*>(vss);
  hashBuckets = reinterpret_cast<int*>(&newshmseg[sizeof(VSSShmsegHeader)]);
  storage =
      reinterpret_cast<VSSEntry*>(&newshmseg[sizeof(VSSShmsegHeader) + vss->numHashBuckets * sizeof(int)]);
}

// void VSSShard::load(const char* readBufPtr, const uint32_t entries)
// {
//   // struct Header header;
//   struct VSSEntry entry;

//   // const char* filename_p = filename.c_str();
//   // scoped_ptr<IDBDataFile> in(
//   //     IDBDataFile::open(IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG), filename_p, "rb", 0));

//   // if (!in)
//   // {
//   //   log_errno("VSS::load()");
//   //   throw runtime_error("VSS::load(): Failed to open the file");
//   // }

//   // if (in->read((char*)&header, sizeof(header)) != sizeof(header))
//   // {
//   //   log_errno("VSS::load()");
//   //   throw runtime_error("VSS::load(): Failed to read header");
//   // }

//   // if (header.magic != VSS_MAGIC_V1)
//   // {
//   //   log("VSS::load(): Bad magic.  Not a VSS file?");
//   //   throw runtime_error("VSS::load(): Bad magic.  Not a VSS file?");
//   // }

//   // if (header.entries >= std::numeric_limits<int>::max())
//   // {
//   //   log("VSS::load(): Bad size.  Not a VSS file?");
//   //   throw runtime_error("VSS::load(): Bad size.  Not a VSS file?");
//   // }

//   growForLoad_(entries);

//   // size_t readSize = header.entries * sizeof(entry);
//   // char* readBuf = new char[readSize];
//   // size_t progress = 0;
//   // int err;
//   // while (progress < readSize)
//   // {
//   //   err = in->read(readBuf + progress, readSize - progress);
//   //   if (err < 0)
//   //   {
//   //     log_errno("VSS::load()");
//   //     throw runtime_error("VSS::load(): Failed to load, check the critical log file");
//   //   }
//   //   else if (err == 0)
//   //   {
//   //     log("VSS::load(): Got early EOF");
//   //     throw runtime_error("VSS::load(): Got early EOF");
//   //   }
//   //   progress += err;
//   // }

//   const VSSEntry* loadedEntries = reinterpret_cast<const VSSEntry*>(readBufPtr);
//   for (uint32_t i = 0; i < entries; i++)
//   {
//     auto p = VSSCluster::partition(loadedEntries[i].lbid);
//     if (p != vssShmType_)
//       continue;
//     insert_(loadedEntries[i].lbid, loadedEntries[i].verID, loadedEntries[i].vbFlag,
//     loadedEntries[i].locked,
//             true);
//   }
// }

// void VSSShard::load(string filename)
// {
//   struct Header header;
//   struct VSSEntry entry;

//   const char* filename_p = filename.c_str();
//   scoped_ptr<IDBDataFile> in(
//       IDBDataFile::open(IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG), filename_p, "rb", 0));

//   if (!in)
//   {
//     log_errno("VSS::load()");
//     throw runtime_error("VSS::load(): Failed to open the file");
//   }

//   if (in->read((char*)&header, sizeof(header)) != sizeof(header))
//   {
//     log_errno("VSS::load()");
//     throw runtime_error("VSS::load(): Failed to read header");
//   }

//   if (header.magic != VSS_MAGIC_V1)
//   {
//     log("VSS::load(): Bad magic.  Not a VSS file?");
//     throw runtime_error("VSS::load(): Bad magic.  Not a VSS file?");
//   }

//   if (header.entries >= std::numeric_limits<int>::max())
//   {
//     log("VSS::load(): Bad size.  Not a VSS file?");
//     throw runtime_error("VSS::load(): Bad size.  Not a VSS file?");
//   }

//   growForLoad_(header.entries);

//   size_t readSize = header.entries * sizeof(entry);
//   char* readBuf = new char[readSize];
//   size_t progress = 0;
//   int err;
//   while (progress < readSize)
//   {
//     err = in->read(readBuf + progress, readSize - progress);
//     if (err < 0)
//     {
//       log_errno("VSS::load()");
//       throw runtime_error("VSS::load(): Failed to load, check the critical log file");
//     }
//     else if (err == 0)
//     {
//       log("VSS::load(): Got early EOF");
//       throw runtime_error("VSS::load(): Got early EOF");
//     }
//     progress += err;
//   }

//   VSSEntry* loadedEntries = (VSSEntry*)readBuf;
//   for (uint32_t i = 0; i < header.entries; i++)
//     insert_(loadedEntries[i].lbid, loadedEntries[i].verID, loadedEntries[i].vbFlag,
//     loadedEntries[i].locked,
//             true);
// }

// read lock
int VSSShard::getIndex(LBID_t lbid, VER_t verID, int& prev, int& bucket) const
{
  prev = -1;
  bucket = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;

  int currentIndex = hashBuckets[bucket];

  while (currentIndex != -1)
  {
    VSSEntry* listEntry = &storage[currentIndex];

    if (listEntry->lbid == lbid && listEntry->verID == verID)
      return currentIndex;

    prev = currentIndex;
    currentIndex = listEntry->next;
  }

  return -1;
}

void VSSShard::growVSS_()
{
  int allocSize;
  key_t newshmkey;
  char* newshmseg;

  if (vssShminfo->allocdSize == 0)
    allocSize = VssInitialSize;
  else
    allocSize = vssShminfo->allocdSize + VssIncrement;

  // auto partition = VSSImplScaled::shmemType2ContinuesIdx(vssShmType_);
  // assert(partition < vssMutexes_.size());

  newshmkey = chooseShmkey_(vssShmType_);
  idbassert((allocSize == VssInitialSize && !vssImpl_) || vssImpl_);

  if (vssImpl_)
  {
    BRMShmImpl newShm(newshmkey, allocSize);
    newshmseg = static_cast<char*>(newShm.fMapreg.get_address());
    memset(newshmseg, 0, allocSize);
    idbassert(vss);
    VSSShmsegHeader* tmp = reinterpret_cast<VSSShmsegHeader*>(newshmseg);
    tmp->capacity = vss->capacity + VssStorageIncrement / sizeof(VSSEntry);
    tmp->numHashBuckets = vss->numHashBuckets + VssStorageIncrement / sizeof(int);
    tmp->LWM = 0;
    copyVSS(tmp);
    vssImpl_->swapout(newShm);
  }
  else
  {
    vssImpl_ = VSSImplScaled::makeVSSImpl(newshmkey, allocSize, vssShmType_);
    newshmseg = reinterpret_cast<char*>(vssImpl_->get());
    memset(newshmseg, 0, allocSize);
  }

  vss = vssImpl_->get();

  if (allocSize == VssInitialSize)
    initShmseg();

  vssShminfo->tableShmkey = newshmkey;
  vssShminfo->allocdSize = allocSize;

  if (r_only)
  {
    vssImpl_->makeReadOnly();
    vss = vssImpl_->get();
  }

  newshmseg = reinterpret_cast<char*>(vss);
  hashBuckets = reinterpret_cast<int*>(&newshmseg[sizeof(VSSShmsegHeader)]);
  storage =
      reinterpret_cast<VSSEntry*>(&newshmseg[sizeof(VSSShmsegHeader) + vss->numHashBuckets * sizeof(int)]);
}

void VSSShard::growForLoad_(uint32_t elementCount)
{
  int allocSize;
  key_t newshmkey;
  char* newshmseg;
  int i;

  if (elementCount < VssStorageInitialCount)
    elementCount = VssStorageInitialCount;

  /* round up to the next normal increment out of paranoia */
  if (elementCount % VssStorageIncrementCount)
    elementCount = ((elementCount / VssStorageIncrementCount) + 1) * VssStorageIncrementCount;

  allocSize = VssSize(elementCount);

  newshmkey = chooseShmkey_(vssShmType_);

  if (vssImpl_)
  {
    // isn't this the same as makeVSSImpl()?
    BRMShmImpl newShm(newshmkey, allocSize);
    vssImpl_->swapout(newShm);
  }
  else
  {
    vssImpl_ = VSSImplScaled::makeVSSImpl(newshmkey, allocSize, vssShmType_);
  }

  vss = vssImpl_->get();
  vss->capacity = elementCount;
  vss->currentSize = 0;
  vss->LWM = 0;
  vss->numHashBuckets = elementCount / 4;
  vss->lockedEntryCount = 0;
  undoRecords.clear();
  newshmseg = reinterpret_cast<char*>(vss);
  hashBuckets = reinterpret_cast<int*>(&newshmseg[sizeof(VSSShmsegHeader)]);
  storage =
      reinterpret_cast<VSSEntry*>(&newshmseg[sizeof(VSSShmsegHeader) + vss->numHashBuckets * sizeof(int)]);

  for (i = 0; i < vss->capacity; i++)
    storage[i].lbid = -1;

  for (i = 0; i < vss->numHashBuckets; i++)
    hashBuckets[i] = -1;

  vssShminfo->tableShmkey = newshmkey;
  vssShminfo->allocdSize = allocSize;
}

// Needs an explicit external lock on a shard
bool VSSShard::isEntryLocked(LBID_t lbid, VER_t verID) const
{
  if (lbid == -1)
    return false;

  // This version considers the blocks needed for rollback to be locked,
  // otherwise they're unlocked.  Note, the version with the 'locked' flag set
  // will never be a candidate for aging out b/c it's not in the version buffer.
  // TODO:  Update this when we support multiple transactions at once.  Need to
  // identify ALL versions needed for rollback.
  bool hasALockedEntry = false;
  VER_t rollbackVersion = 0;

  const int bucket = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;

  int index = hashBuckets[bucket];

  while (index != -1)
  {
    VSSEntry* listEntry = &storage[index];

    if (listEntry->lbid == lbid)
    {
      if (listEntry->locked)
        hasALockedEntry = true;
      else if (rollbackVersion < listEntry->verID)
        rollbackVersion = listEntry->verID;
    }

    index = listEntry->next;
  }

  return (hasALockedEntry && verID == rollbackVersion);
}

// assumes write lock is held and that it is properly sized already
// metadata is modified by the caller
void VSSShard::_insert(VSSEntry& e, VSSShmsegHeader* dest, int* destHash, VSSEntry* destStorage, bool loading)
{
  const int hashIndex = hasher((char*)&e.lbid, sizeof(e.lbid)) % dest->numHashBuckets;
  int insertIndex = dest->LWM;
  // std::cout << "_insert " << vssShmType_ << " insertIndex init " << insertIndex << endl;

  while (destStorage[insertIndex].lbid != -1)
  {
    ++insertIndex;
#ifdef BRM_DEBUG

    if (insertIndex == dest->capacity)
    {
      log("VSS:_insert(): There are no empty entries. Check resize condition.", logging::LOG_TYPE_DEBUG);
      throw logic_error("VSS:_insert(): There are no empty entries. Check resize condition.");
    }

#endif
  }

  if (!loading)
    makeUndoRecord(dest, sizeof(VSSShmsegHeader));

  dest->LWM = insertIndex + 1;

  if (!loading)
  {
    makeUndoRecord(&destStorage[insertIndex], sizeof(VSSEntry));
    makeUndoRecord(&destHash[hashIndex], sizeof(int));
  }

  e.next = destHash[hashIndex];
  destStorage[insertIndex] = e;
  destHash[hashIndex] = insertIndex;
}

// assumes write lock is held and the src is vbbm
// and that dest->{numHashBuckets, capacity, LWM} have been set.
void VSSShard::copyVSS(VSSShmsegHeader* dest)
{
  int i;
  int* newHashtable;
  VSSEntry* newStorage;
  char* cDest = reinterpret_cast<char*>(dest);

  // copy metadata
  dest->currentSize = vss->currentSize;
  dest->lockedEntryCount = vss->lockedEntryCount;

  newHashtable = reinterpret_cast<int*>(&cDest[sizeof(VSSShmsegHeader)]);
  newStorage =
      reinterpret_cast<VSSEntry*>(&cDest[sizeof(VSSShmsegHeader) + dest->numHashBuckets * sizeof(int)]);

  // initialize new storage & hash
  for (i = 0; i < dest->numHashBuckets; i++)
    newHashtable[i] = -1;

  for (i = 0; i < dest->capacity; i++)
    newStorage[i].lbid = -1;

  // walk the storage & re-hash all entries;
  for (i = 0; i < vss->currentSize; i++)
    if (storage[i].lbid != -1)
    {
      _insert(storage[i], dest, newHashtable, newStorage, true);
    }
}

uint32_t VSSShard::chooseShmkey_(const MasterSegmentTable::ShmemType shmemType) const
{
  int fixedKeys = 1;
  auto rangeBaseNumber = vssIdx2ShmkeyBase_(vssShmType_);
  if (vssShminfo->tableShmkey + 1 == (key_t)(rangeBaseNumber + fShmKeys.KEYRANGE_SIZE - 1) ||
      (unsigned)vssShminfo->tableShmkey < rangeBaseNumber)
    return rangeBaseNumber + fixedKeys;
  return vssShminfo->tableShmkey + 1;
}

void VSSShard::initShmseg()
{
  int i;
  char* newshmseg;
  int* buckets;
  VSSEntry* stor;

  vss->capacity = VssStorageInitialSize / sizeof(VSSEntry);
  vss->currentSize = 0;
  vss->lockedEntryCount = 0;
  vss->LWM = 0;
  vss->numHashBuckets = VssTableInitialSize / sizeof(int);
  newshmseg = reinterpret_cast<char*>(vss);

  buckets = reinterpret_cast<int*>(&newshmseg[sizeof(VSSShmsegHeader)]);
  stor = reinterpret_cast<VSSEntry*>(&newshmseg[sizeof(VSSShmsegHeader) + vss->numHashBuckets * sizeof(int)]);

  for (i = 0; i < vss->numHashBuckets; i++)
    buckets[i] = -1;

  for (i = 0; i < vss->capacity; i++)
    stor[i].lbid = -1;
}

// // ported from ExtentMap
// void VSS::release(OPS op)
// {
//   if (op == READ)
//     mst.releaseTable_read(vssShmType_);
//   else
//     mst.releaseTable_write(vssShmType_);
// }

// void VSS::initShmseg()
// {
//   int i;
//   char* newshmseg;
//   int* buckets;
//   VSSEntry* stor;

//   vss->capacity = VssStorageInitialSize / sizeof(VSSEntry);
//   vss->currentSize = 0;
//   vss->lockedEntryCount = 0;
//   vss->LWM = 0;
//   vss->numHashBuckets = VssTableInitialSize / sizeof(int);
//   newshmseg = reinterpret_cast<char*>(vss);

//   buckets = reinterpret_cast<int*>(&newshmseg[sizeof(VSSShmsegHeader)]);
//   stor = reinterpret_cast<VSSEntry*>(&newshmseg[sizeof(VSSShmsegHeader) + vss->numHashBuckets *
//   sizeof(int)]);

//   for (i = 0; i < vss->numHashBuckets; i++)
//     buckets[i] = -1;

//   for (i = 0; i < vss->capacity; i++)
//     stor[i].lbid = -1;
// }

// void VSS::growVSS_()
// {
//   int allocSize;
//   key_t newshmkey;
//   char* newshmseg;

//   if (vssShminfo->allocdSize == 0)
//     allocSize = VssInitialSize;
//   else
//     allocSize = vssShminfo->allocdSize + VssIncrement;

//   auto partition = VSSImplScaled::shmemType2ContinuesIdx(vssShmType_);
//   assert(partition < vssMutexes_.size());

//   newshmkey = chooseShmkey_(vssShmType_);
//   idbassert((allocSize == VssInitialSize && !vssImpl_) || vssImpl_);

//   if (vssImpl_)
//   {
//     BRMShmImpl newShm(newshmkey, allocSize);
//     newshmseg = static_cast<char*>(newShm.fMapreg.get_address());
//     memset(newshmseg, 0, allocSize);
//     idbassert(vss);
//     VSSShmsegHeader* tmp = reinterpret_cast<VSSShmsegHeader*>(newshmseg);
//     tmp->capacity = vss->capacity + VssStorageIncrement / sizeof(VSSEntry);
//     tmp->numHashBuckets = vss->numHashBuckets + VssStorageIncrement / sizeof(int);
//     tmp->LWM = 0;
//     copyVSS(tmp);
//     vssImpl_->swapout(newShm);
//   }
//   else
//   {
//     vssImpl_ = VSSImplScaled::makeVSSImpl(newshmkey, allocSize, vssShmType_);
//     newshmseg = reinterpret_cast<char*>(vssImpl_->get());
//     memset(newshmseg, 0, allocSize);
//   }

//   vss = vssImpl_->get();

//   if (allocSize == VssInitialSize)
//     initShmseg();

//   vssShminfo->tableShmkey = newshmkey;
//   vssShminfo->allocdSize = allocSize;

//   if (r_only)
//   {
//     vssImpl_->makeReadOnly();
//     vss = vssImpl_->get();
//   }

//   newshmseg = reinterpret_cast<char*>(vss);
//   hashBuckets = reinterpret_cast<int*>(&newshmseg[sizeof(VSSShmsegHeader)]);
//   storage =
//       reinterpret_cast<VSSEntry*>(&newshmseg[sizeof(VSSShmsegHeader) + vss->numHashBuckets * sizeof(int)]);
// }

// void VSS::growForLoad_(uint32_t elementCount)
// {
//   int allocSize;
//   key_t newshmkey;
//   char* newshmseg;
//   int i;

//   if (elementCount < VssStorageInitialCount)
//     elementCount = VssStorageInitialCount;

//   /* round up to the next normal increment out of paranoia */
//   if (elementCount % VssStorageIncrementCount)
//     elementCount = ((elementCount / VssStorageIncrementCount) + 1) * VssStorageIncrementCount;

//   allocSize = VssSize(elementCount);

//   newshmkey = chooseShmkey_(vssShmType_);

//   if (vssImpl_)
//   {
//     // isn't this the same as makeVSSImpl()?
//     BRMShmImpl newShm(newshmkey, allocSize);
//     vssImpl_->swapout(newShm);
//   }
//   else
//   {
//     vssImpl_ = VSSImplScaled::makeVSSImpl(newshmkey, allocSize, vssShmType_);
//   }

//   vss = vssImpl_->get();
//   vss->capacity = elementCount;
//   vss->currentSize = 0;
//   vss->LWM = 0;
//   vss->numHashBuckets = elementCount / 4;
//   vss->lockedEntryCount = 0;
//   undoRecords.clear();
//   newshmseg = reinterpret_cast<char*>(vss);
//   hashBuckets = reinterpret_cast<int*>(&newshmseg[sizeof(VSSShmsegHeader)]);
//   storage =
//       reinterpret_cast<VSSEntry*>(&newshmseg[sizeof(VSSShmsegHeader) + vss->numHashBuckets * sizeof(int)]);

//   for (i = 0; i < vss->capacity; i++)
//     storage[i].lbid = -1;

//   for (i = 0; i < vss->numHashBuckets; i++)
//     hashBuckets[i] = -1;

//   vssShminfo->tableShmkey = newshmkey;
//   vssShminfo->allocdSize = allocSize;
// }

// assumes write lock is held and the src is vbbm
// and that dest->{numHashBuckets, capacity, LWM} have been set.
// void VSS::copyVSS(VSSShmsegHeader* dest)
// {
//   int i;
//   int* newHashtable;
//   VSSEntry* newStorage;
//   char* cDest = reinterpret_cast<char*>(dest);

//   // copy metadata
//   dest->currentSize = vss->currentSize;
//   dest->lockedEntryCount = vss->lockedEntryCount;

//   newHashtable = reinterpret_cast<int*>(&cDest[sizeof(VSSShmsegHeader)]);
//   newStorage =
//       reinterpret_cast<VSSEntry*>(&cDest[sizeof(VSSShmsegHeader) + dest->numHashBuckets * sizeof(int)]);

//   // initialize new storage & hash
//   for (i = 0; i < dest->numHashBuckets; i++)
//     newHashtable[i] = -1;

//   for (i = 0; i < dest->capacity; i++)
//     newStorage[i].lbid = -1;

//   // walk the storage & re-hash all entries;
//   for (i = 0; i < vss->currentSize; i++)
//     if (storage[i].lbid != -1)
//     {
//       _insert(storage[i], dest, newHashtable, newStorage, true);
//     }
// }

// uint32_t VSS::chooseShmkey_(const MasterSegmentTable::ShmemType shmemType) const
// {
//   int fixedKeys = 1;
//   auto rangeBaseNumber = vssIdx2ShmkeyBase(vssShmType_);
//   if (vssShminfo->tableShmkey + 1 == (key_t)(rangeBaseNumber + fShmKeys.KEYRANGE_SIZE - 1) ||
//       (unsigned)vssShminfo->tableShmkey < rangeBaseNumber)
//     return rangeBaseNumber + fixedKeys;
//   return vssShminfo->tableShmkey + 1;
// }

// void VSS::insert_(LBID_t lbid, VER_t verID, bool vbFlag, bool locked, bool loading)
// {
//   VSSEntry entry(lbid, verID, vbFlag, locked);
//   cerr << "Insert to vss lbid:verID:locked:vbFlag = " << entry.lbid << ":" << entry.verID << ":"
//        << entry.locked << ":" << entry.vbFlag << endl;

//   // check for resize
//   if (vss->currentSize == vss->capacity)
//     growVSS_();

//   _insert(entry, vss, hashBuckets, storage, loading);

//   if (!loading)
//     makeUndoRecord(&vss->currentSize, sizeof(vss->currentSize));

//   ++vss->currentSize;

//   if (locked)
//     ++vss->lockedEntryCount;
// }

// // assumes write lock is held and that it is properly sized already
// // metadata is modified by the caller
// void VSS::_insert(VSSEntry& e, VSSShmsegHeader* dest, int* destHash, VSSEntry* destStorage, bool loading)
// {
//   const int hashIndex = hasher((char*)&e.lbid, sizeof(e.lbid)) % dest->numHashBuckets;
//   int insertIndex = dest->LWM;
//   // std::cout << "_insert " << vssShmType_ << " insertIndex init " << insertIndex << endl;

//   while (destStorage[insertIndex].lbid != -1)
//   {
//     ++insertIndex;
// #ifdef BRM_DEBUG

//     if (insertIndex == dest->capacity)
//     {
//       log("VSS:_insert(): There are no empty entries. Check resize condition.", logging::LOG_TYPE_DEBUG);
//       throw logic_error("VSS:_insert(): There are no empty entries. Check resize condition.");
//     }

// #endif
//   }

//   if (!loading)
//     makeUndoRecord(dest, sizeof(VSSShmsegHeader));

//   dest->LWM = insertIndex + 1;

//   if (!loading)
//   {
//     makeUndoRecord(&destStorage[insertIndex], sizeof(VSSEntry));
//     makeUndoRecord(&destHash[hashIndex], sizeof(int));
//   }

//   e.next = destHash[hashIndex];
//   destStorage[insertIndex] = e;
//   destHash[hashIndex] = insertIndex;
// }

// // assumes read lock is held
// int VSS::lookup(LBID_t lbid, const QueryContext_vss& verInfo, VER_t txnID, VER_t* outVer, bool* vbFlag,
//                 bool vbOnly) const
// {
//   int hashIndex, maxVersion = -1, minVersion = -1, currentIndex;
//   VSSEntry *listEntry, *maxEntry = nullptr;

//   hashIndex = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;

//   currentIndex = hashBuckets[hashIndex];

//   while (currentIndex != -1)
//   {
//     listEntry = &storage[currentIndex];

//     if (listEntry->lbid == lbid)
//     {
//       if (vbOnly && !listEntry->vbFlag)
//         goto next;

//       /* "if it is/was part of a different transaction, ignore it"
//        */
//       if (txnID != listEntry->verID && (verInfo.txns->find(listEntry->verID) != verInfo.txns->end()))
//         goto next;

//       /* fast exit if the exact version is found */
//       if (verInfo.currentScn == listEntry->verID)
//       {
//         *outVer = listEntry->verID;
//         *vbFlag = listEntry->vbFlag;
//         return 0;
//       }

//       /* Track the min version of this LBID */
//       if (minVersion > listEntry->verID || minVersion == -1)
//         minVersion = listEntry->verID;

//       /* Pick the highest version <= the SCN of the query */
//       if (verInfo.currentScn > listEntry->verID && maxVersion < listEntry->verID)
//       {
//         maxVersion = listEntry->verID;
//         maxEntry = listEntry;
//       }
//     }

//   next:
//     currentIndex = listEntry->next;
//   }

//   if (maxEntry != nullptr)
//   {
//     *outVer = maxVersion;
//     *vbFlag = maxEntry->vbFlag;
//     return 0;
//   }
//   else if (minVersion > verInfo.currentScn)
//   {
//     *outVer = 0;
//     *vbFlag = false;
//     return ERR_SNAPSHOT_TOO_OLD;
//   }

//   *outVer = 0;
//   *vbFlag = false;
//   return -1;
// }

// VER_t VSS::getCurrentVersion(LBID_t lbid, bool* isLocked) const
// {
//   int hashIndex, currentIndex;
//   VSSEntry* listEntry;

//   hashIndex = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;
//   currentIndex = hashBuckets[hashIndex];

//   while (currentIndex != -1)
//   {
//     listEntry = &storage[currentIndex];

//     if (listEntry->lbid == lbid && !listEntry->vbFlag)
//     {
//       if (isLocked != nullptr)
//         *isLocked = listEntry->locked;

//       return listEntry->verID;
//     }

//     currentIndex = listEntry->next;
//   }

//   if (isLocked != nullptr)
//     *isLocked = false;

//   return 0;
// }

// VER_t VSS::getHighestVerInVB(LBID_t lbid, VER_t max) const
// {
//   int hashIndex, currentIndex;
//   VER_t ret = -1;
//   VSSEntry* listEntry;

//   hashIndex = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;
//   currentIndex = hashBuckets[hashIndex];

//   while (currentIndex != -1)
//   {
//     listEntry = &storage[currentIndex];

//     if ((listEntry->lbid == lbid && listEntry->vbFlag) && (listEntry->verID <= max && listEntry->verID >
//     ret))
//       ret = listEntry->verID;

//     currentIndex = listEntry->next;
//   }

//   return ret;
// }

// bool VSS::isVersioned(LBID_t lbid, VER_t version) const
// {
//   int hashIndex, currentIndex;
//   VSSEntry* listEntry;

//   hashIndex = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;
//   currentIndex = hashBuckets[hashIndex];

//   while (currentIndex != -1)
//   {
//     listEntry = &storage[currentIndex];

//     if (listEntry->lbid == lbid && listEntry->verID == version)
//       return listEntry->vbFlag;

//     currentIndex = listEntry->next;
//   }

//   return false;
// }

// bool VSS::isLocked(const LBID_t lbid, VER_t transID) const
// {
//   int hashIndex, currentIndex;
//   VSSEntry* listEntry;

//   hashIndex = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;

//   currentIndex = hashBuckets[hashIndex];
//   while (currentIndex != -1)
//   {
//     listEntry = &storage[currentIndex];

//     if (listEntry->lbid == lbid && listEntry->locked)
//     {
//       if (listEntry->verID == transID)
//         return false;
//       else
//         return true;
//     }

//     currentIndex = listEntry->next;
//   }

//   return false;
// }

// // requires write lock
// void VSS::removeEntry(LBID_t lbid, VER_t verID, vector<LBID_t>* flushList)
// {
//   int index, prev, bucket;

// #ifdef BRM_DEBUG

//   if (lbid < 0)
//   {
//     log("VSS::removeEntry(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
//     throw invalid_argument("VSS::removeEntry(): lbid must be >= 0");
//   }

//   if (verID < 0)
//   {
//     log("VSS::removeEntry(): verID must be >= 0", logging::LOG_TYPE_DEBUG);
//     throw invalid_argument("VSS::removeEntry(): verID must be >= 0");
//   }

// #endif

//   index = getIndex(lbid, verID, prev, bucket);

//   if (index == -1)
//   {
// #ifdef BRM_DEBUG
//     ostringstream ostr;

//     ostr << "VSS::removeEntry(): that entry doesn't exist lbid = " << lbid << " verID = " << verID;
//     log(ostr.str(), logging::LOG_TYPE_DEBUG);
//     throw logic_error(ostr.str());
// #else
//     return;
// #endif
//   }

//   makeUndoRecord(&storage[index], sizeof(VSSEntry));
//   storage[index].lbid = -1;

//   if (prev != -1)
//   {
//     makeUndoRecord(&storage[prev], sizeof(VSSEntry));
//     storage[prev].next = storage[index].next;
//   }
//   else
//   {
//     makeUndoRecord(&hashBuckets[bucket], sizeof(int));
//     hashBuckets[bucket] = storage[index].next;
//   }

//   makeUndoRecord(vss, sizeof(VSSShmsegHeader));
//   --vss->currentSize;

//   if (storage[index].locked && (vss->lockedEntryCount > 0))
//     --vss->lockedEntryCount;

//   if (index < vss->LWM)
//     vss->LWM = index;

//   // scan the list of entries with that lbid to see if there are others
//   // to remove.
//   for (index = hashBuckets[bucket]; index != -1; index = storage[index].next)
//     if (storage[index].lbid == lbid && (storage[index].vbFlag || storage[index].locked))
//       return;

//   // if execution gets here, we should be able to remove all entries
//   // with the given lbid because none point to the VB.
//   for (prev = -1, index = hashBuckets[bucket]; index != -1; index = storage[index].next)
//   {
//     if (storage[index].lbid == lbid)
//     {
//       makeUndoRecord(&storage[index], sizeof(VSSEntry));
//       storage[index].lbid = -1;

//       if (prev == -1)
//       {
//         makeUndoRecord(&hashBuckets[bucket], sizeof(int));
//         hashBuckets[bucket] = storage[index].next;
//       }
//       else
//       {
//         makeUndoRecord(&storage[prev], sizeof(VSSEntry));
//         storage[prev].next = storage[index].next;
//       }

//       --vss->currentSize;

//       if (storage[index].locked && (vss->lockedEntryCount > 0))
//         vss->lockedEntryCount--;

//       if (index < vss->LWM)
//         vss->LWM = index;
//     }
//     else
//       prev = index;
//   }

//   flushList->push_back(lbid);
// }

// bool VSS::isTooOld(LBID_t lbid, VER_t verID) const
// {
//   VER_t minVer = 0;

//   const int bucket = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;

//   int index = hashBuckets[bucket];

//   while (index != -1)
//   {
//     VSSEntry* listEntry = &storage[index];

//     if (listEntry->lbid == lbid && minVer > listEntry->verID)
//       minVer = listEntry->verID;

//     index = listEntry->next;
//   }

//   return (minVer > verID);
// }

// bool VSS::isEntryLocked(LBID_t lbid, VER_t verID) const
// {
//   if (lbid == -1)
//     return false;

//   // This version considers the blocks needed for rollback to be locked,
//   // otherwise they're unlocked.  Note, the version with the 'locked' flag set
//   // will never be a candidate for aging out b/c it's not in the version buffer.
//   // TODO:  Update this when we support multiple transactions at once.  Need to
//   // identify ALL versions needed for rollback.
//   bool hasALockedEntry = false;
//   VER_t rollbackVersion = 0;

//   const int bucket = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;

//   int index = hashBuckets[bucket];

//   while (index != -1)
//   {
//     VSSEntry* listEntry = &storage[index];

//     if (listEntry->lbid == lbid)
//     {
//       if (listEntry->locked)
//         hasALockedEntry = true;
//       else if (rollbackVersion < listEntry->verID)
//         rollbackVersion = listEntry->verID;
//     }

//     index = listEntry->next;
//   }

//   return (hasALockedEntry && verID == rollbackVersion);
// }

// // read lock
// int VSS::getIndex(LBID_t lbid, VER_t verID, int& prev, int& bucket) const
// {
//   prev = -1;
//   bucket = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;

//   int currentIndex = hashBuckets[bucket];

//   while (currentIndex != -1)
//   {
//     VSSEntry* listEntry = &storage[currentIndex];

//     if (listEntry->lbid == lbid && listEntry->verID == verID)
//       return currentIndex;

//     prev = currentIndex;
//     currentIndex = listEntry->next;
//   }

//   return -1;
// }

// // write lock
// void VSS::setVBFlag(LBID_t lbid, VER_t verID, bool vbFlag)
// {
//   int index, prev, bucket;

//   index = getIndex(lbid, verID, prev, bucket);

//   if (index == -1)
//   {
//     ostringstream ostr;

//     ostr << "VSS::setVBFlag(): that entry doesn't exist lbid=" << lbid << " ver=" << verID;
//     log(ostr.str(), logging::LOG_TYPE_DEBUG);
//     throw logic_error(ostr.str());
//   }

//   makeUndoRecord(&storage[index], sizeof(VSSEntry));
//   storage[index].vbFlag = vbFlag;
// }

// // write lock
// void VSS::commit(VER_t txnID)
// {
//   int i;
// #ifdef BRM_DEBUG

//   if (txnID < 1)
//   {
//     log("VSS::commit(): txnID must be > 0", logging::LOG_TYPE_DEBUG);
//     throw invalid_argument("VSS::commit(): txnID must be > 0");
//   }

// #endif

//   for (i = 0; i < vss->capacity; i++)
//     if (storage[i].lbid != -1 && storage[i].verID == txnID)
//     {
// #ifdef BRM_DEBUG

//       if (storage[i].locked != true)
//       {
//         ostringstream ostr;
//         ostr << "VSS::commit(): An entry has already been unlocked..? txnId = " << txnID;
//         log(ostr.str(), logging::LOG_TYPE_DEBUG);
//         throw logic_error(ostr.str());
//       }

// #endif
//       makeUndoRecord(&storage[i], sizeof(VSSEntry));
//       storage[i].locked = false;

//       // @ bug 1426 fix. Decrease the counter when an entry releases its lock.
//       if (vss->lockedEntryCount > 0)
//         --vss->lockedEntryCount;
//     }
// }

// // read lock
// void VSS::getUncommittedLBIDs(VER_t txnID, vector<LBID_t>& lbids)
// {
//   int i;

// #ifdef BRM_DEBUG

//   if (txnID < 1)
//   {
//     log("VSS::getUncommittedLBIDs(): txnID must be > 0", logging::LOG_TYPE_DEBUG);
//     throw invalid_argument("VSS::getUncommittedLBIDs(): txnID must be > 0");
//   }

// #endif

//   for (i = 0; i < vss->capacity; i++)
//     if (storage[i].lbid != -1 && storage[i].verID == txnID)
//     {
//       // #ifdef BRM_DEBUG

//       if (storage[i].locked == false)
//       {
//         log("VSS::getUncommittedLBIDs(): found an unlocked block with that TxnID",
//         logging::LOG_TYPE_DEBUG); throw logic_error("VSS::getUncommittedLBIDs(): found an unlocked block
//         with that TxnID");
//       }

//       if (storage[i].vbFlag == true)
//       {
//         log("VSS::getUncommittedLBIDs(): found a block with that TxnID in the VB",
//         logging::LOG_TYPE_DEBUG); throw logic_error("VSS::getUncommittedLBIDs(): found a block with that
//         TxnID in the VB");
//       }

//       // #endif
//       lbids.push_back(storage[i].lbid);
//     }
// }

// void VSS::getUnlockedLBIDs(BlockList_t& lbids)
// {
//   for (int i = 0; i < vss->capacity; i++)
//     if (storage[i].lbid != -1 && !storage[i].locked)
//       lbids.push_back(LVP_t(storage[i].lbid, storage[i].verID));
// }

// WIP replace vbbm with lambda
// void VSS::removeEntriesFromDB(const LBIDRange& range, VBBM& vbbm, bool use_vbbm)
// {
// #ifdef BRM_DEBUG

//   if (range.start < 0)
//   {
//     log("VSS::removeEntriesFromDB(): lbids must be positive.", logging::LOG_TYPE_DEBUG);
//     throw invalid_argument("VSS::removeEntriesFromDB(): lbids must be positive.");
//   }

//   if (range.size < 1)
//   {
//     log("VSS::removeEntriesFromDB(): size must be > 0", logging::LOG_TYPE_DEBUG);
//     throw invalid_argument("VSS::removeEntriesFromDB(): size must be > 0");
//   }

// #endif

//   makeUndoRecord(vss, sizeof(VSSShmsegHeader));

//   LBID_t lastLBID = range.start + range.size - 1;

//   for (LBID_t lbid = range.start; lbid <= lastLBID; lbid++)
//   {
//     int bucket = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;
//     int index = hashBuckets[bucket];
//     int prev = -1;

//     for (; index != -1; index = storage[index].next)
//     {
//       if (storage[index].lbid == lbid)
//       {
//         if (storage[index].vbFlag && use_vbbm)
//           vbbm.removeEntry(storage[index].lbid, storage[index].verID);

//         makeUndoRecord(&storage[index], sizeof(VSSEntry));
//         storage[index].lbid = -1;

//         if (prev == -1)
//         {
//           makeUndoRecord(&hashBuckets[bucket], sizeof(int));
//           hashBuckets[bucket] = storage[index].next;
//         }
//         else
//         {
//           makeUndoRecord(&storage[prev], sizeof(VSSEntry));
//           storage[prev].next = storage[index].next;
//         }

//         vss->currentSize--;

//         if (storage[index].locked && (vss->lockedEntryCount > 0))
//           vss->lockedEntryCount--;

//         if (index < vss->LWM)
//           vss->LWM = index;
//       }
//       else
//         prev = index;
//     }
//   }
// }

// This traverses the vss hash map searching for the first lbid match.
// LbidVersionVec VSS::removeEntryFromDB(const LBID_t& lbid)
// {
//   makeUndoRecord(vss, sizeof(VSSShmsegHeader));
//   auto bucket = hasher((char*)&lbid, sizeof(lbid)) % vss->numHashBuckets;
//   LbidVersionVec lbidsVersions;

//   int prev = -1;
//   auto index = hashBuckets[bucket];
//   for (; index != -1; index = storage[index].next, prev = index)
//   {
//     if (storage[index].lbid == lbid)
//     {
//       if (storage[index].vbFlag)
//       {
//         lbidsVersions.emplace_back(storage[index].lbid, storage[index].verID);
//       }

//       makeUndoRecord(&storage[index], sizeof(VSSEntry));
//       storage[index].lbid = -1;

//       if (prev == -1)
//       {
//         makeUndoRecord(&hashBuckets[bucket], sizeof(int));
//         hashBuckets[bucket] = storage[index].next;
//       }
//       else
//       {
//         makeUndoRecord(&storage[prev], sizeof(VSSEntry));
//         storage[prev].next = storage[index].next;
//         // If the index to has next == -1, than set hash[bucket] to -1 as well.
//         // hash[bucket] == -1 has the last entry in the blocks chain.
//         if (storage[index].next == -1)
//         {
//           hashBuckets[bucket] = storage[index].next;
//         }
//       }

//       --vss->currentSize;

//       if (storage[index].locked && (vss->lockedEntryCount > 0))
//         --vss->lockedEntryCount;

//       if (index < vss->LWM)
//         vss->LWM = index;
//     }
//     else
//     {
//       prev = index;
//     }
//   }

//   return lbidsVersions;
// }

// int VSS::size() const
// {
//   int i, ret = 0;

//   for (i = 0; i < vss->capacity; i++)
//     if (storage[i].lbid != -1)
//       ret++;

//   if (ret != vss->currentSize)
//   {
//     ostringstream ostr;

//     ostr << "VSS: actual size & recorded size disagree.  actual size = " << ret
//          << " recorded size = " << vss->currentSize;
//     log(ostr.str(), logging::LOG_TYPE_DEBUG);
//     throw logic_error(ostr.str());
//   }

//   return ret;
// }

// bool VSS::hashEmpty() const
// {
//   int i;

//   for (i = 0; i < vss->numHashBuckets; i++)
//     if (hashBuckets[i] != -1)
//       return false;

//   return true;
// }

// void VSS::clear_()
// {
//   auto allocSize = VssInitialSize;
//   auto newshmkey = chooseShmkey_(vssShmType_);

//   idbassert(vssImpl_);
//   idbassert(vssImpl_->key() != newshmkey);
//   vssImpl_->clear(newshmkey, allocSize);
//   vssShminfo->tableShmkey = newshmkey;
//   vssShminfo->allocdSize = allocSize;
//   vss = vssImpl_->get();
//   initShmseg();

//   if (r_only)
//   {
//     vssImpl_->makeReadOnly();
//     vss = vssImpl_->get();
//   }

//   auto newshmseg = reinterpret_cast<char*>(vss);
//   hashBuckets = reinterpret_cast<int*>(&newshmseg[sizeof(VSSShmsegHeader)]);
//   storage =
//       reinterpret_cast<VSSEntry*>(&newshmseg[sizeof(VSSShmsegHeader) + vss->numHashBuckets * sizeof(int)]);
// }

// // read lock
// int VSS::checkConsistency(const VBBM& vbbm, ExtentMap& em) const
// {
//   /*
//   1. Every valid entry in the VSS has an entry either in the VBBM or in the
//   EM.  Verify that.
//   2. Struct consistency checks
//       a. current size agrees with actual # of used entries
//       b. there are no empty elements in the hashed lists
//       c. each hash table entry points to a non-empty element or -1
//       d. verify that there are no empty entries below the LWM
//       e. verify uniqueness of the entries

//   */

//   int i, j, k, err;

//   /* Test 1 */

//   OID_t oid;
//   uint32_t fbo;

//   for (i = 0; i < vss->capacity; i++)
//   {
//     if (storage[i].lbid != -1)
//     {
//       if (storage[i].vbFlag)
//       {
//         err = vbbm.lookup(storage[i].lbid, storage[i].verID, oid, fbo);

//         if (err != 0)
//         {
//           cerr << "VSS: lbid=" << storage[i].lbid << " verID=" << storage[i].verID
//                << " vbFlag=true isn't in the VBBM" << endl;
//           throw logic_error("VSS::checkConsistency(): a VSS entry with vbflag set is not in the VBBM");
//         }
//       }
//       else
//       {
// // This version of em.lookup was made obsolete with multiple files per OID.
// // If ever want to really use this checkConsistency() function, this section
// // of code needs to be updated to use the new lookup() API.
// #if 0
//                 err = em.lookup(storage[i].lbid, (int&)oid, fbo);

//                 if (err != 0)
//                 {
//                     cerr << "VSS: lbid=" << storage[i].lbid << " verID=" <<
//                          storage[i].verID << " vbFlag=false has no extent" <<
//                          endl;
//                     throw logic_error("VSS::checkConsistency(): a VSS entry with vbflag unset is not in the
//                     EM");
//                 }

// #endif
//       }
//     }
//   }

//   /* Test 2a is already implemented */
//   size();

//   /* Tests 2b & 2c - no empty elements reachable from the hash table */

//   int nextElement;

//   for (i = 0; i < vss->numHashBuckets; i++)
//   {
//     if (hashBuckets[i] != -1)
//       for (nextElement = hashBuckets[i]; nextElement != -1; nextElement = storage[nextElement].next)
//         if (storage[nextElement].lbid == -1)
//           throw logic_error(
//               "VSS::checkConsistency(): an empty storage entry is reachable from the hash table");
//   }

//   /* Test 2d - verify that there are no empty entries below the LWM */

//   for (i = 0; i < vss->LWM; i++)
//     if (storage[i].lbid == -1)
//     {
//       cerr << "VSS: LWM=" << vss->LWM << " first empty entry=" << i << endl;
//       throw logic_error("VSS::checkConsistency(): LWM accounting error");
//     }

//   /* Test 2e - verify uniqueness of each entry */

//   for (i = 0; i < vss->numHashBuckets; i++)
//     if (hashBuckets[i] != -1)
//       for (j = hashBuckets[i]; j != -1; j = storage[j].next)
//         for (k = storage[j].next; k != -1; k = storage[k].next)
//           if (storage[j].lbid == storage[k].lbid && storage[j].verID == storage[k].verID)
//           {
//             cerr << "VSS: lbid=" << storage[j].lbid << " verID=" << storage[j].verID << endl;
//             throw logic_error("VSS::checkConsistency(): Duplicate entry found");
//           }

//   return 0;
// }

// void VSS::setReadOnly()
// {
//   r_only = true;
// }

// void VSS::getCurrentTxnIDs(set<VER_t>& list) const
// {
//   int i;

//   for (i = 0; i < vss->capacity; i++)
//     if (storage[i].lbid != -1 && storage[i].locked)
//       list.insert(storage[i].verID);
// }

// /* File Format:

//                 VSS V1 magic (32-bits)
//                 # of VSS entries in capacity (32-bits)
//                 struct VSSEntry * #
// */
// // WIP TBR
// struct Header
// {
//   int magic;
//   uint32_t entries;
// };

// // read lock
// void VSS::save(string filename)
// {
//   const char* filename_p = filename.c_str();
//   scoped_ptr<IDBDataFile> out(IDBDataFile::open(IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
//                                                 filename_p, "wb", IDBDataFile::USE_VBUF));

//   if (!out)
//   {
//     log_errno("VSS::save()");
//     throw runtime_error("VSS::save(): Failed to open the file");
//   }

//   if (vss->currentSize < 0)
//   {
//     log_errno("VSS::save()");
//     throw runtime_error("VSS::save(): VSS::currentSize is negative");
//   }

//   struct Header header
//   {
//     .magic = VSS_MAGIC_V1, .entries = static_cast<uint32_t>(vss->currentSize)
//   };

//   // header.magic = VSS_MAGIC_V1;
//   // header.entries = vss->currentSize;

//   if (out->write((char*)&header, sizeof(header)) != sizeof(header))
//   {
//     log_errno("VSS::save()");
//     throw runtime_error("VSS::save(): Failed to write header to the file");
//   }

//   int first = -1, last = -1, err;
//   size_t progress, writeSize;
//   for (int i = 0; i < vss->capacity; i++)
//   {
//     if (storage[i].lbid != -1 && first == -1)
//       first = i;
//     else if (storage[i].lbid == -1 && first != -1)
//     {
//       last = i;
//       writeSize = (last - first) * sizeof(VSSEntry);
//       progress = 0;
//       char* writePos = (char*)&storage[first];
//       while (progress < writeSize)
//       {
//         err = out->write(writePos + progress, writeSize - progress);
//         if (err < 0)
//         {
//           log_errno("VSS::save()");
//           throw runtime_error("VSS::save(): Failed to write the file");
//         }
//         progress += err;
//       }
//       first = -1;
//     }
//   }
//   if (first != -1)
//   {
//     writeSize = (vss->capacity - first) * sizeof(VSSEntry);
//     progress = 0;
//     char* writePos = (char*)&storage[first];
//     while (progress < writeSize)
//     {
//       err = out->write(writePos + progress, writeSize - progress);
//       if (err < 0)
//       {
//         log_errno("VSS::save()");
//         throw runtime_error("VSS::save(): Failed to write the file");
//       }
//       progress += err;
//     }
//   }
// }

// // Ideally, we;d like to get in and out of this fcn as quickly as possible.
// bool VSS::isEmpty(bool useLock)
// {
//   // Should be race-free, but takes along time...
//   bool rc;

//   if (useLock)
//     lock_(READ);

//   rc = (vssImpl_->get()->currentSize == 0);

//   if (useLock)
//     release(READ);

//   return rc;
// }

// void VSS::load(string filename)
// {
//   struct Header header;
//   struct VSSEntry entry;

//   const char* filename_p = filename.c_str();
//   scoped_ptr<IDBDataFile> in(
//       IDBDataFile::open(IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG), filename_p, "rb", 0));

//   if (!in)
//   {
//     log_errno("VSS::load()");
//     throw runtime_error("VSS::load(): Failed to open the file");
//   }

//   if (in->read((char*)&header, sizeof(header)) != sizeof(header))
//   {
//     log_errno("VSS::load()");
//     throw runtime_error("VSS::load(): Failed to read header");
//   }

//   if (header.magic != VSS_MAGIC_V1)
//   {
//     log("VSS::load(): Bad magic.  Not a VSS file?");
//     throw runtime_error("VSS::load(): Bad magic.  Not a VSS file?");
//   }

//   if (header.entries >= std::numeric_limits<int>::max())
//   {
//     log("VSS::load(): Bad size.  Not a VSS file?");
//     throw runtime_error("VSS::load(): Bad size.  Not a VSS file?");
//   }

//   growForLoad_(header.entries);

//   size_t readSize = header.entries * sizeof(entry);
//   char* readBuf = new char[readSize];
//   size_t progress = 0;
//   int err;
//   while (progress < readSize)
//   {
//     err = in->read(readBuf + progress, readSize - progress);
//     if (err < 0)
//     {
//       log_errno("VSS::load()");
//       throw runtime_error("VSS::load(): Failed to load, check the critical log file");
//     }
//     else if (err == 0)
//     {
//       log("VSS::load(): Got early EOF");
//       throw runtime_error("VSS::load(): Got early EOF");
//     }
//     progress += err;
//   }

//   VSSEntry* loadedEntries = (VSSEntry*)readBuf;
//   for (uint32_t i = 0; i < header.entries; i++)
//     insert_(loadedEntries[i].lbid, loadedEntries[i].verID, loadedEntries[i].vbFlag,
//     loadedEntries[i].locked,
//             true);
// }

QueryContext_vss::QueryContext_vss(const QueryContext& qc) : currentScn(qc.currentScn)
{
  txns.reset(new set<VER_t>());

  for (uint32_t i = 0; i < qc.currentTxns->size(); i++)
    txns->insert((*qc.currentTxns)[i]);
}

}  // namespace BRM

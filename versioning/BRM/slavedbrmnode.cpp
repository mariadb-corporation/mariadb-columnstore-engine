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
 * $Id: slavedbrmnode.cpp 1890 2013-05-30 18:59:24Z dhall $
 *
 ****************************************************************************/

#include <iostream>
#include <sys/types.h>
#include <numeric>
#include <vector>
// #include "hasher.h"
#if __linux__
#include <values.h>
#endif
#include <sstream>
// #include <limits>

#include "brmtypes.h"
// #include "rwlock.h"
// #include "mastersegmenttable.h"
#include "extentmap.h"
#include "copylocks.h"
#include "vss.h"
#include "vbbm.h"
#include "exceptclasses.h"
#define SLAVEDBRMNODE_DLLEXPORT
#include "slavedbrmnode.h"
#undef SLAVEDBRMNODE_DLLEXPORT
// #include "messagelog.h"
// #include "loggingid.h"
// #include "errorcodes.h"
// #include "idberrorinfo.h"
#include "cacheutils.h"
using namespace std;
using namespace logging;

namespace BRM
{
SlaveDBRMNode::SlaveDBRMNode()
{
  locked[0] = false;
  locked[1] = false;
  locked[2] = false;
  vss_ = make_unique<VSSCluster>();

  // for (auto s : MasterSegmentTable::VssShmemTypes)
  // {
  //   vss_.emplace_back(std::unique_ptr<VSS>(new VSS(s)));
  //   vssIsLocked_.push_back(false);
  // }
}

SlaveDBRMNode::~SlaveDBRMNode() throw()
{
}

SlaveDBRMNode& SlaveDBRMNode::operator=(const SlaveDBRMNode& brm)
{
  throw logic_error("WorkerDBRMNode: Don't use the = operator.");
}

void SlaveDBRMNode::lockVSS(const VSSCluster::OPS op)
{
  vss_->lock_(op);
  locked[1] = true;
}

void SlaveDBRMNode::releaseVSS(const VSSCluster::OPS op)
{
  vss_->release(op);
  locked[1] = false;
}

void SlaveDBRMNode::releaseVSSifNeeded(const VSSCluster::OPS op)
{
  vss_->releaseIfNeeded(op);
  locked[1] = false;
}

void SlaveDBRMNode::confirmChangesIfNeededAndReleaseVSS(const VSSCluster::OPS op)
{
  vss_->confirmChangesIfNeededAndRelease(op);
  locked[1] = false;
}

void SlaveDBRMNode::undoChangesIfNeededAndReleaseVSS(const VSSCluster::OPS op)
{
  vss_->undoChangesIfNeededAndRelease(op);
  locked[1] = false;
}

int SlaveDBRMNode::lookup(OID_t oid, LBIDRange_v& lbidList) throw()
{
  try
  {
    em.lookup(oid, lbidList);
    return 0;
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }
}

//------------------------------------------------------------------------------
// Create a "stripe" of column extents for the specified column OIDs and DBRoot.
//------------------------------------------------------------------------------
int SlaveDBRMNode::createStripeColumnExtents(const std::vector<CreateStripeColumnExtentsArgIn>& cols,
                                             uint16_t dbRoot, uint32_t& partitionNum, uint16_t& segmentNum,
                                             std::vector<CreateStripeColumnExtentsArgOut>& extents) throw()
{
  try
  {
    em.createStripeColumnExtents(cols, dbRoot, partitionNum, segmentNum, extents);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Create an extent for the specified OID and DBRoot.
//------------------------------------------------------------------------------
int SlaveDBRMNode::createColumnExtent_DBroot(OID_t oid, uint32_t colWidth, uint16_t dbRoot,
                                             execplan::CalpontSystemCatalog::ColDataType colDataType,
                                             uint32_t& partitionNum, uint16_t& segmentNum, LBID_t& lbid,
                                             int& allocdSize, uint32_t& startBlockOffset) throw()
{
  try
  {
    em.createColumnExtent_DBroot(oid, colWidth, dbRoot, colDataType, partitionNum, segmentNum, lbid,
                                 allocdSize, startBlockOffset);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Create extent for the exact segment file specified by the requested
// OID, DBRoot, partition, and segment.
//------------------------------------------------------------------------------
int SlaveDBRMNode::createColumnExtentExactFile(OID_t oid, uint32_t colWidth, uint16_t dbRoot,
                                               uint32_t partitionNum, uint16_t segmentNum,
                                               execplan::CalpontSystemCatalog::ColDataType colDataType,
                                               LBID_t& lbid, int& allocdSize,
                                               uint32_t& startBlockOffset) throw()
{
  try
  {
    em.createColumnExtentExactFile(oid, colWidth, dbRoot, partitionNum, segmentNum, colDataType, lbid,
                                   allocdSize, startBlockOffset);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Create a dictionary store extent for the specified OID, dbRoot, partition
// number and segment number.
//------------------------------------------------------------------------------
int SlaveDBRMNode::createDictStoreExtent(OID_t oid, uint16_t dbRoot, uint32_t partitionNum,
                                         uint16_t segmentNum, LBID_t& lbid, int& allocdSize) throw()
{
  try
  {
    em.createDictStoreExtent(oid, dbRoot, partitionNum, segmentNum, lbid, allocdSize);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Rollback (delete) the extents that logically trail the specified extent for
// the given OID and DBRoot.  Also sets the HWM for the specified extent.
//------------------------------------------------------------------------------
int SlaveDBRMNode::rollbackColumnExtents_DBroot(OID_t oid, bool bDeleteAll, uint16_t dbRoot,
                                                uint32_t partitionNum, uint16_t segmentNum, HWM_t hwm) throw()
{
  try
  {
    em.rollbackColumnExtents_DBroot(oid, bDeleteAll, dbRoot, partitionNum, segmentNum, hwm);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Rollback (delete) the extents that follow the specified extents for the
// given OID and DBRoot.
// Also sets the HWMs for the last extents to be kept in each segment file in
// the specified partition.
//------------------------------------------------------------------------------
int SlaveDBRMNode::rollbackDictStoreExtents_DBroot(OID_t oid, uint16_t dbRoot, uint32_t partitionNum,
                                                   const vector<uint16_t>& segNums,
                                                   const vector<HWM_t>& hwms) throw()
{
  try
  {
    em.rollbackDictStoreExtents_DBroot(oid, dbRoot, partitionNum, segNums, hwms);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

int SlaveDBRMNode::deleteEmptyColExtents(const ExtentsInfoMap_t& extentsInfo) throw()
{
  try
  {
    em.deleteEmptyColExtents(extentsInfo);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

int SlaveDBRMNode::deleteEmptyDictStoreExtents(const ExtentsInfoMap_t& extentsInfo) throw()
{
  try
  {
    em.deleteEmptyDictStoreExtents(extentsInfo);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

int SlaveDBRMNode::deleteOID(OID_t oid, const bool vbbmIsLocked, const bool vssIsLocked, const bool cleanUpEM,
                             const bool failOnNoLBID) throw()
{
  try
  {
    if (!vbbmIsLocked)
    {
      vbbm.lock(VBBM::WRITE);
      locked[0] = true;
    }

    if (!vssIsLocked)
    {
      lockVSS(VSSCluster::WRITE);

      // for (size_t i = 0; auto& v : vss_)
      // {
      //   v->lock_(VSS::WRITE);
      //   vssIsLocked_[i++] = true;
      // }
    }

    LBIDRange_v lbids;
    auto err = lookup(oid, lbids);

    if (err == -1 || (failOnNoLBID && lbids.empty()))
      return -1;
    // TODO lbid ranges are painful to process. I suggest a bitmap for a range to filter out
    // lbids that are not in vss bucket.
    for (auto lbidRange : lbids)
    {
      for (auto lbid = lbidRange.start; lbid < lbidRange.start + lbidRange.size; ++lbid)
      {
        // auto partition = VSS::partition(lbid);
        // assert(partition < vss_.size() && partition < vssIsLocked_.size() && vss_[partition]);
        // removeEntryFromDB returns a vector of lbid/version pairs to be removed from vbbm.
        // for (auto [lbid, version] : vss_[partition]->removeEntryFromDB(lbid))
        for (auto [lbid, version] : vss_->removeEntryFromDB(lbid))
        {
          vbbm.removeEntry(lbid, version);
        }
      }
    }

    if (cleanUpEM)
    {
      em.deleteOID(oid);
    }
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

int SlaveDBRMNode::deleteOIDs(const OidsMap_t& oids) throw()
{
  try
  {
    vbbm.lock(VBBM::WRITE);
    const bool vbbmIsLocked = true;
    locked[0] = vbbmIsLocked;

    lockVSS(VSSCluster::WRITE);

    // for (size_t i = 0; auto& v : vss_)
    // {
    //   v->lock_(VSS::WRITE);
    //   vssIsLocked_[i++] = true;
    // }

    const bool cleanUpEM = false;
    const bool vssIsLocked = true;
    const bool failOnNoLBID = false;
    int err;
    for (auto [u, oid] : oids)
    {
      if ((err = deleteOID(oid, vbbmIsLocked, vssIsLocked, cleanUpEM, failOnNoLBID)) != 0)
      {
        return err;
      }
    }
    em.deleteOIDs(oids);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}
//------------------------------------------------------------------------------
// Set the HWM for the specified OID, partition, and segment.  Used to set the
// HWM for a specific dictionary or column segment file.
//------------------------------------------------------------------------------
int SlaveDBRMNode::setLocalHWM(OID_t oid, uint32_t partitionNum, uint16_t segmentNum, HWM_t hwm,
                               bool firstNode) throw()
{
  try
  {
    em.setLocalHWM(oid, partitionNum, segmentNum, hwm, firstNode);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

int SlaveDBRMNode::bulkSetHWM(const vector<BulkSetHWMArg>& args, VER_t transID, bool firstNode) throw()
{
  try
  {
    if (transID)
      vbCommit(transID);

    em.bulkSetHWM(args, firstNode);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

int SlaveDBRMNode::bulkSetHWMAndCP(const vector<BulkSetHWMArg>& hwmArgs,
                                   const std::vector<CPInfo>& setCPDataArgs,
                                   const std::vector<CPInfoMerge>& mergeCPDataArgs, VER_t transID,
                                   bool firstNode) throw()
{
  uint32_t i;
  bool firstCall = true;
  CPMaxMin setCPEntry;
  CPMaxMinMap_t bulkSetCPMap;
  CPMaxMinMerge mergeCPEntry;
  CPMaxMinMergeMap_t bulkMergeCPMap;

  try
  {
    if (transID)
      vbCommit(transID);

    for (i = 0; i < hwmArgs.size(); i++)
    {
      em.setLocalHWM(hwmArgs[i].oid, hwmArgs[i].partNum, hwmArgs[i].segNum, hwmArgs[i].hwm, firstNode,
                     firstCall);
      firstCall = false;
    }

    if (setCPDataArgs.size() > 0)
    {
      for (i = 0; i < setCPDataArgs.size(); i++)
      {
        setCPEntry.isBinaryColumn = setCPDataArgs[i].isBinaryColumn;
        if (!setCPEntry.isBinaryColumn)
        {
          setCPEntry.max = setCPDataArgs[i].max;
          setCPEntry.min = setCPDataArgs[i].min;
        }
        else
        {
          setCPEntry.bigMax = setCPDataArgs[i].bigMax;
          setCPEntry.bigMin = setCPDataArgs[i].bigMin;
        }
        setCPEntry.seqNum = setCPDataArgs[i].seqNum;
        bulkSetCPMap[setCPDataArgs[i].firstLbid] = setCPEntry;
      }

      em.setExtentsMaxMin(bulkSetCPMap, firstNode, firstCall);
      firstCall = false;
    }

    if (mergeCPDataArgs.size() > 0)
    {
      for (i = 0; i < mergeCPDataArgs.size(); i++)
      {
        mergeCPEntry.type = mergeCPDataArgs[i].type;
        mergeCPEntry.colWidth = mergeCPDataArgs[i].colWidth;
        if (mergeCPDataArgs[i].colWidth <= 8)
        {
          mergeCPEntry.max = mergeCPDataArgs[i].max;
          mergeCPEntry.min = mergeCPDataArgs[i].min;
        }
        else
        {
          mergeCPEntry.bigMax = mergeCPDataArgs[i].bigMax;
          mergeCPEntry.bigMin = mergeCPDataArgs[i].bigMin;
        }
        mergeCPEntry.newExtent = mergeCPDataArgs[i].newExtent;
        mergeCPEntry.seqNum = mergeCPDataArgs[i].seqNum;
        bulkMergeCPMap[mergeCPDataArgs[i].startLbid] = mergeCPEntry;
      }

      em.mergeExtentsMaxMin(bulkMergeCPMap, firstCall);
      firstCall = false;
    }
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

int SlaveDBRMNode::bulkUpdateDBRoot(const vector<BulkUpdateDBRootArg>& args) throw()
{
  try
  {
    em.bulkUpdateDBRoot(args);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

int SlaveDBRMNode::writeVBEntry(VER_t transID, LBID_t lbid, OID_t vbOID, uint32_t vbFBO,
                                const bool vbbmIsLocked, const bool vssIsLocked) throw()
{
  try
  {
    if (!vbbmIsLocked)
    {
      vbbm.lock(VBBM::WRITE);
      locked[0] = true;
    }

    // figure out the current version of the block
    // NOTE!  This will currently error out to preserve the assumption that
    // larger version numbers imply more recent changes.  If we ever change that
    // assumption, we'll need to revise the vbRollback() fcns as well.
    // auto partition = VSS::partition(lbid);
    // assert(partition < vss_.size() && partition < vssIsLocked_.size() && vss_[partition]);

    bool* isLocked = nullptr;

    if (!vssIsLocked)
    {
      vss_->lock_(lbid, VSSCluster::WRITE);
      // lockVSS(VSSCluster::WRITE);
      // vss_[partition]->lock_(VSS::WRITE);
      // vssIsLocked_[partition] = true;
    }

    VER_t oldVerID = vss_->getCurrentVersionWExtLock(lbid, isLocked);

    if (oldVerID == transID)
      return 0;
    else if (oldVerID > transID)
    {
      ostringstream str;

      str << "WorkerDBRMNode::writeVBEntry(): Overlapping transactions detected.  "
             "Transaction "
          << transID
          << " cannot overwrite blocks written by "
             "transaction "
          << oldVerID;
      log(str.str());
      return ERR_OLDTXN_OVERWRITING_NEWTXN;
    }

    vbbm.insert(lbid, oldVerID, vbOID, vbFBO);

    if (oldVerID > 0)
    {
      vss_->setVBFlag(lbid, oldVerID, true);
    }
    else
    {
      vss_->insert_(lbid, oldVerID, true, false);
    }

    vss_->insert_(lbid, transID, false, true);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

int SlaveDBRMNode::bulkWriteVBEntry(VER_t transID, const std::vector<BRM::LBID_t>& lbids, OID_t vbOID,
                                    const std::vector<uint32_t>& vbFBOs) throw()
{
  try
  {
    vbbm.lock(VBBM::WRITE);
    locked[0] = true;
    // TODO take only specific locks preprocessing lbids first.
    lockVSS(VSSCluster::WRITE);

    // for (size_t i = 0; auto& v : vss_)
    // {
    //   v->lock_(VSS::WRITE);
    //   vssIsLocked_[i++] = true;
    // }

    assert(lbids.size() == vbFBOs.size());
    bool vbbmIsLocked = true;
    bool vssIsLocked = true;
    for (size_t i = 0; i < lbids.size(); ++i)
    {
      size_t rc = writeVBEntry(transID, lbids[i], vbOID, vbFBOs[i], vbbmIsLocked, vssIsLocked);
      if (rc)
        return rc;
    }
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

int SlaveDBRMNode::beginVBCopy(VER_t transID, uint16_t vbOID, const LBIDRange_v& ranges, VBRange_v& freeList,
                               bool flushPMCache) throw()
{
  uint64_t maxRetries;
  uint64_t waitInterval = 50000;  // usecs to sleep between retries
  uint64_t retries;
  std::vector<bool> lockedRanges(ranges.size(), false);
  uint32_t i;

  /* XXXPAT: The controller node will wait up to 5 mins for the response.
   * For now, this alg will try for 1 min to grab all of the locks.
   * After that, it will release them all then grab them.  Releasing
   * them by force opens the slight possibility of a bad result, but more
   * likely something crashed, and there has to be some kind of recovery.  The worst
   * case is better than stalling the system or causing the BRM to go read-only.
   * It should be extremely rare that it has to be done.
   */
  maxRetries = (60 * 1000000) / waitInterval;

  int64_t sum =
      std::accumulate(begin(ranges), end(ranges), 0, [](int sum, const LBIDRange r) { return sum + r.size; });

  try
  {
    vbbm.lock(VBBM::WRITE);
    locked[0] = true;

    lockVSS(VSSCluster::WRITE);
    // for (size_t i = 0; auto& v : vss_)
    // {
    //   v->lock_(VSS::WRITE);
    //   vssIsLocked_[i++] = true;
    // }

    /* This check doesn't need to be repeated after the retry loop below.
     * For now, there is no other transaction that could lock these
     * ranges.  When we support multiple transactions at once, the resource
     * graph in the controller node should make this redundant anyway.
     */
    for (auto range : ranges)
    {
      LBID_t rangeEnd = range.start + range.size;
      assert(range.size);
      for (LBID_t lbid = range.start; lbid < rangeEnd; ++lbid)
      {
        // auto partition = VSS::partition(lbid);
        if (vss_->isLocked(lbid, transID))
        {
          return -1;
        }
      }
    }

    copylocks.lock(CopyLocks::WRITE);
    locked[2] = true;
    bool allLocked = false;
    /* This version grabs all unlocked ranges in each pass.
     * If there are locked ranges it waits and tries again.
     */
    retries = 0;

    while (!allLocked && retries < maxRetries)
    {
      allLocked = true;

      for (i = 0; i < ranges.size(); i++)
      {
        if (!lockedRanges[i])
        {
          if (copylocks.isLocked(ranges[i]))
            allLocked = false;
          else
          {
            copylocks.lockRange(ranges[i], transID);
            lockedRanges[i] = true;
          }
        }
      }

      /* PrimProc is reading at least 1 range and it could need the locks.
       */
      if (!allLocked)
      {
        copylocks.release(CopyLocks::WRITE);
        locked[2] = false;

        releaseVSS(VSSCluster::WRITE);
        // for (size_t i = 0; auto& v : vss_)
        // {
        //   v->release(VSS::WRITE);
        //   vssIsLocked_[i++] = false;
        // }

        vbbm.release(VBBM::WRITE);
        locked[0] = false;

        usleep(waitInterval);
        retries++;

        vbbm.lock(VBBM::WRITE);
        locked[0] = true;

        lockVSS(VSSCluster::WRITE);
        // for (size_t i = 0; auto& v : vss_)
        // {
        //   v->lock_(VSS::WRITE);
        //   vssIsLocked_[i++] = true;
        // }

        copylocks.lock(CopyLocks::WRITE);
        locked[2] = true;
      }
    }

    if (retries >= maxRetries)
    {
      for (i = 0; i < ranges.size(); i++)
      {
        if (!lockedRanges[i])
        {
          copylocks.forceRelease(ranges[i]);
          copylocks.lockRange(ranges[i], transID);
          lockedRanges[i] = true;
        }
      }
    }

    auto cleanVSS = [](BRM::VSSCluster& vss, const VBBMEntry& storageEntry, vector<VBRange>& freeRanges,
                       const int num, const uint32_t firstFBO, const uint32_t lastFBO,
                       vector<LBID_t>& flushList)
    {
      if (vss.isEntryLocked(storageEntry.lbid, storageEntry.verID))
      {
        ostringstream msg;
        msg << "VBBM::getBlocks(): version buffer overflow. Increase VersionBufferFileSize. Overflow "
               "occured in aged blocks. Requested NumBlocks:VbOid:vbFBO:lastFBO = "
            << num << ":" << storageEntry.vbOID << ":" << firstFBO << ":" << lastFBO << " lbid locked is "
            << storageEntry.lbid << endl;
        log(msg.str(), logging::LOG_TYPE_CRITICAL);
        freeRanges.clear();
        throw logging::VBBMBufferOverFlowExcept(msg.str());
      }

      vss.removeEntry(storageEntry.lbid, storageEntry.verID, &flushList);
    };

    // This function changes vss state using the above lambda.
    vbbm.getBlocks(sum, vbOID, freeList, cleanVSS, *vss_, flushPMCache);
    return 0;
  }
  catch (const logging::VBBMBufferOverFlowExcept& e)
  {
    cerr << e.what() << endl;

    for (i = 0; i < ranges.size(); i++)
      if (lockedRanges[i])
        copylocks.releaseRange(ranges[i]);

    return e.errorCode();
  }
  catch (exception& e)
  {
    for (i = 0; i < ranges.size(); i++)
      if (lockedRanges[i])
        copylocks.releaseRange(ranges[i]);

    cerr << e.what() << endl;
    return -1;
  }
}

int SlaveDBRMNode::endVBCopy(VER_t transID, const LBIDRange_v& ranges) throw()
{
  LBIDRange_v::const_iterator it;

  try
  {
    copylocks.lock(CopyLocks::WRITE);
    locked[2] = true;

    for (it = ranges.begin(); it != ranges.end(); ++it)
      copylocks.releaseRange(*it);

    return 0;
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }
}

// WIP where is the release counterpart
int SlaveDBRMNode::vbCommit(VER_t transID) throw()
{
  try
  {
    // WIP should lock/commit locked-only VSS instances
    lockVSS(VSSCluster::WRITE);
    vss_->commit(transID);
    // for (size_t i = 0; auto& v : vss_)
    // {
    //   v->lock_(VSS::WRITE);
    //   vssIsLocked_[i++] = true;
    //   v->commit(transID);
    // }

    return 0;
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }
}

int SlaveDBRMNode::vbRollback(VER_t transID, const LBIDRange_v& lbidList, bool flushPMCache) throw()
{
  LBIDRange_v::const_iterator it;
  LBID_t lbid;
  VER_t oldVerID;
  vector<LBID_t> flushList;

#ifdef BRM_DEBUG

  if (transID < 1)
  {
    cerr << "SlaveDBRMNode::vbRollback(): transID must be > 0" << endl;
    return -1;
  }

#endif

  try
  {
    vbbm.lock(VBBM::WRITE);
    locked[0] = true;
    lockVSS(VSSCluster::WRITE);
    // for (size_t i = 0; auto& v : vss_)
    // {
    //   v->lock_(VSS::WRITE);
    //   vssIsLocked_[i++] = true;
    // }
    copylocks.lock(CopyLocks::WRITE);
    locked[2] = true;

    copylocks.rollback(transID);

    for (it = lbidList.begin(); it != lbidList.end(); it++)
    {
      for (lbid = (*it).start; lbid < (*it).start + (*it).size; lbid++)
      {
        // auto partition = VSS::partition(lbid);
        oldVerID = vss_->getHighestVerInVB(lbid, transID, false);  /// !!!

        if (oldVerID != -1)
        {
          vbbm.removeEntry(lbid, oldVerID);
          vss_->setVBFlag(lbid, oldVerID, false);  // !!!
        }

        vss_->removeEntry(lbid, transID, &flushList);  // !!!
      }
    }

    if (flushPMCache && !flushList.empty())
      cacheutils::flushPrimProcAllverBlocks(flushList);

    return 0;
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    ostringstream ostr;
    ostr << "SlaveDBRMNode::vbRollback error. " << e.what();
    log(ostr.str());
    return -1;
  }
}

int SlaveDBRMNode::vbRollback(VER_t transID, const vector<LBID_t>& lbidList, bool flushPMCache) throw()
{
  VER_t oldVerID;
  vector<LBID_t> flushList;

#ifdef BRM_DEBUG

  if (transID < 1)
  {
    cerr << "SlaveDBRMNode::vbRollback(): transID must be > 0" << endl;
    return -1;
  }

#endif

  try
  {
    vbbm.lock(VBBM::WRITE);
    locked[0] = true;
    lockVSS(VSSCluster::WRITE);
    // for (size_t i = 0; auto& v : vss_)
    // {
    //   v->lock_(VSS::WRITE);
    //   vssIsLocked_[i++] = true;
    // }
    copylocks.lock(CopyLocks::WRITE);
    locked[2] = true;

    copylocks.rollback(transID);

    for (auto lbid : lbidList)
    {
      // auto partition = VSS::partition(lbid);
      oldVerID = vss_->getHighestVerInVB(lbid, transID, false);

      if (oldVerID != -1)
      {
        vbbm.removeEntry(lbid, oldVerID);
        vss_->setVBFlag(lbid, oldVerID, false);
      }

      vss_->removeEntry(lbid, transID, &flushList);
    }

    if (flushPMCache && !flushList.empty())
      cacheutils::flushPrimProcAllverBlocks(flushList);

    return 0;
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }
}

void SlaveDBRMNode::confirmChanges() throw()
{
  try
  {
    em.confirmChanges();

    if (locked[0])
    {
      vbbm.confirmChanges();
      vbbm.release(VBBM::WRITE);
      locked[0] = false;
    }

    // {
    if (locked[1])
    {
      confirmChangesIfNeededAndReleaseVSS(VSSCluster::WRITE);
      // releaseVSS(VSSCluster::WRITE);
      // locked[1] = false;
    }

    // for (size_t i = 0; auto& v : vss_)
    // {
    //   if (vssIsLocked_[i])
    //   {
    //     assert(v);
    //     v->confirmChanges();
    //     v->release(VSS::WRITE);
    //   }
    //   vssIsLocked_[i++] = false;
    // }
    // }

    if (locked[2])
    {
      copylocks.confirmChanges();
      copylocks.release(CopyLocks::WRITE);
      locked[2] = false;
    }
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
  }
}

void SlaveDBRMNode::undoChanges() throw()
{
  try
  {
    em.undoChanges();

    if (locked[0])
    {
      vbbm.undoChanges();
      vbbm.release(VBBM::WRITE);
      locked[0] = false;
    }

    undoChangesIfNeededAndReleaseVSS(VSSCluster::WRITE);
    // {
    //   for (size_t i = 0; auto& v : vss_)
    //   {
    //     if (vssIsLocked_[i])
    //     {
    //       v->undoChanges();
    //       v->release(VSS::WRITE);
    //     }
    //     vssIsLocked_[i++] = false;
    //   }
    // }

    if (locked[2])
    {
      copylocks.undoChanges();
      copylocks.release(CopyLocks::WRITE);
      locked[2] = false;
    }
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
  }
}

int SlaveDBRMNode::clear() throw()
{
  bool llocked[2] = {false, false};
  std::vector<bool> vssIsLocked(VssFactor, false);
  // assert(vssIsLocked.size() == vss_.size());

  try
  {
    vbbm.lock(VBBM::WRITE);
    llocked[0] = true;
    lockVSS(VSSCluster::WRITE);
    // for (size_t i = 0; auto& v : vss_)
    // {
    //   v->lock_(VSS::WRITE);
    //   vssIsLocked[i++] = true;
    // }

    vbbm.clear();

    vss_->clear_();
    releaseVSS(VSSCluster::WRITE);
    // for (size_t i = 0; auto& v : vss_)
    // {
    //   v->clear_();
    //   v->release(VSS::WRITE);
    //   vssIsLocked[i++] = false;
    // }

    vbbm.release(VBBM::WRITE);
    llocked[0] = false;
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;

    releaseVSSifNeeded(VSSCluster::WRITE);
    // for (size_t i = 0; auto& v : vss_)
    // {
    //   if (vssIsLocked[i++])
    //     v->release(VSS::WRITE);
    // }

    if (llocked[0])
      vbbm.release(VBBM::WRITE);

    return -1;
  }

  return 0;
}

// int SlaveDBRMNode::checkConsistency() throw()
// {
//   bool llocked[2] = {false, false};
//   std::vector<bool> vssIsLocked(VssFactor, false);
//   assert(vssIsLocked.size() == vss_.size());

//   try
//   {
//     em.checkConsistency();
//   }
//   catch (exception& e)
//   {
//     cerr << e.what() << endl;
//     return -1;
//   }

//   try
//   {
//     vbbm.lock(VBBM::READ);
//     llocked[0] = true;
//     for (size_t i = 0; auto& v : vss_)
//     {
//       v->lock_(VSS::WRITE);
//       vssIsLocked[i++] = true;
//       v->checkConsistency(vbbm, em);
//       v->release(VSS::READ);
//     }

//     // vss.checkConsistency(vbbm, em);
//     // vss.release(VSS::READ);
//     // llocked[1] = false;
//     vbbm.release(VBBM::READ);
//     llocked[0] = false;
//   }
//   catch (exception& e)
//   {
//     cerr << e.what() << endl;

//     for (size_t i = 0; auto& v : vss_)
//     {
//       if (vssIsLocked[i++])
//       {
//         v->release(VSS::READ);
//       }
//     }
//     if (llocked[0])
//       vbbm.release(VBBM::READ);

//     return -1;
//   }

//   try
//   {
//     vbbm.lock(VBBM::READ);
//     vbbm.checkConsistency();
//     vbbm.release(VBBM::READ);
//   }
//   catch (exception& e)
//   {
//     cerr << e.what() << endl;
//     vbbm.release(VBBM::READ);
//     return -1;
//   }

//   return 0;
// }

int SlaveDBRMNode::saveState(string filenamePrefix) throw()
{
  string emFilename = filenamePrefix + "_em";
  // string vssFilename = filename + "_vss";
  string vbbmFilename = filenamePrefix + "_vbbm";
  bool locked[2] = {false, false};
  std::vector<bool> vssIsLocked(VssFactor, false);
  // assert(vssIsLocked.size() == vss_.size());

  try
  {
    vbbm.lock(VBBM::READ);
    locked[0] = true;

    saveExtentMap(emFilename);
    vbbm.save(vbbmFilename);

    vss_->save(filenamePrefix);

    // for (size_t i = 0; auto& v : vss_)
    // {
    //   assert(i < MasterSegmentTable::VssShmemTypes.size());
    //   v->lock_(VSS::READ);
    //   vssIsLocked[i] = true;
    //   v->save(vssFilename + std::to_string(i + 1));
    //   v->release(VSS::READ);
    //   vssIsLocked[i] = false;
    //   ++i;
    // }

    vbbm.release(VBBM::READ);
    locked[0] = false;
  }
  catch (exception& e)
  {
    // assert(vssIsLocked.size() == vss_.size());
    releaseVSSifNeeded(VSSCluster::READ);
    // for (size_t i = 0; auto& v : vss_)
    // {
    //   if (vssIsLocked[i++])
    //     v->release(VSS::READ);
    // }

    if (locked[0])
      vbbm.release(VBBM::READ);

    return -1;
  }

  return 0;
}

int SlaveDBRMNode::loadState(string filenamePrefix) throw()
{
  string emFilename = filenamePrefix + "_em";
  // string vssFilename = filenamePrefix + "_vss";
  string vbbmFilename = filenamePrefix + "_vbbm";
  bool locked[2] = {false, false};
  std::vector<bool> vssIsLocked(VssFactor, false);
  // assert(vssIsLocked.size() == vss_.size());

  try
  {
    vbbm.lock(VBBM::WRITE);
    locked[0] = true;

    loadExtentMap(emFilename);
    vbbm.load(vbbmFilename);

    vss_->load(filenamePrefix);
    // for (size_t i = 0; auto& v : vss_)
    // {
    //   assert(i < MasterSegmentTable::VssShmemTypes.size());
    //   v->lock_(VSS::WRITE);
    //   vssIsLocked[i] = true;
    //   // The vss image filename numeric suffix begins with 1.
    //   v->load(vssFilename + std::to_string(i + 1));
    //   v->release(VSS::WRITE);
    //   vssIsLocked[i] = false;
    //   ++i;
    // }

    vbbm.release(VBBM::WRITE);
    locked[0] = false;
  }
  catch (exception& e)
  {
    releaseVSSifNeeded(VSSCluster::WRITE);
    // assert(vssIsLocked.size() == vss_.size());
    // for (size_t i = 0; auto& v : vss_)
    // {
    //   if (vssIsLocked[i++])
    //   {
    //     v->release(VSS::WRITE);
    //   }
    // }

    if (locked[0])
      vbbm.release(VBBM::WRITE);

    return -1;
  }

  return 0;
}

int SlaveDBRMNode::loadExtentMap(const string& filename)
{
  em.load(filename);
  return 0;
}

int SlaveDBRMNode::saveExtentMap(const string& filename)
{
  em.save(filename);
  return 0;
}

// Casual partitioning support
//
int SlaveDBRMNode::markExtentInvalid(const LBID_t lbid,
                                     execplan::CalpontSystemCatalog::ColDataType colDataType)
{
  int err = 0;

  try
  {
    err = em.markInvalid(lbid, colDataType);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return err;
}

int SlaveDBRMNode::markExtentsInvalid(
    const vector<LBID_t>& lbids, const std::vector<execplan::CalpontSystemCatalog::ColDataType>& colDataTypes)
{
  int err = 0;

  try
  {
    err = em.markInvalid(lbids, colDataTypes);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return err;
}

int SlaveDBRMNode::setExtentMaxMin(const LBID_t lbid, const int64_t max, const int64_t min,
                                   const int32_t seqNum, bool firstNode)
{
  int err = 0;

  try
  {
    err = em.setMaxMin(lbid, max, min, seqNum, firstNode);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return err;
}

// @bug 1970 - Added setExtentsMaxMin below.
int SlaveDBRMNode::setExtentsMaxMin(const CPMaxMinMap_t& cpMap, bool firstNode)
{
  try
  {
    em.setExtentsMaxMin(cpMap, firstNode);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// @bug 2117 - Added mergeExtentsMaxMin() to merge CP min/max information.
//------------------------------------------------------------------------------
int SlaveDBRMNode::mergeExtentsMaxMin(CPMaxMinMergeMap_t& cpMap)
{
  try
  {
    em.mergeExtentsMaxMin(cpMap);
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Delete all extents for the specified OID(s) and partition number.
//------------------------------------------------------------------------------
int SlaveDBRMNode::deletePartition(const std::set<OID_t>& oids, set<LogicalPartition>& partitionNums,
                                   string& emsg) throw()
{
  try
  {
    em.deletePartition(oids, partitionNums, emsg);
  }
  catch (IDBExcept& iex)
  {
    cerr << iex.what() << endl;

    if (iex.errorCode() == ERR_PARTITION_NOT_EXIST)
      return ERR_NOT_EXIST_PARTITION;
    else if (iex.errorCode() == ERR_INVALID_LAST_PARTITION)
      return ERR_INVALID_OP_LAST_PARTITION;
    else if (iex.errorCode() == WARN_NO_PARTITION_PERFORMED)
      return ERR_NO_PARTITION_PERFORMED;
    else
      return -1;
  }
  catch (DBRMException& e)
  {
    // exceptions that can be ignored
    return 0;
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s) and partition
// number.
//------------------------------------------------------------------------------
int SlaveDBRMNode::markPartitionForDeletion(const std::set<OID_t>& oids, set<LogicalPartition>& partitionNums,
                                            string& emsg) throw()
{
  try
  {
    em.markPartitionForDeletion(oids, partitionNums, emsg);
  }
  catch (IDBExcept& iex)
  {
    cerr << iex.what() << endl;

    if (iex.errorCode() == ERR_PARTITION_ALREADY_DISABLED)
      return ERR_PARTITION_DISABLED;
    else if (iex.errorCode() == ERR_PARTITION_NOT_EXIST)
      return ERR_NOT_EXIST_PARTITION;
    else if (iex.errorCode() == ERR_INVALID_LAST_PARTITION)
      return ERR_INVALID_OP_LAST_PARTITION;
    else if (iex.errorCode() == WARN_NO_PARTITION_PERFORMED)
      return ERR_NO_PARTITION_PERFORMED;
    else
      return -1;
  }
  catch (DBRMException& e)
  {
    // exceptions that can be ignored
    return 0;
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s)
//------------------------------------------------------------------------------
int SlaveDBRMNode::markAllPartitionForDeletion(const std::set<OID_t>& oids) throw()
{
  try
  {
    em.markAllPartitionForDeletion(oids);
  }
  catch (IDBExcept& iex)
  {
    cerr << iex.what() << endl;

    if (iex.errorCode() == ERR_PARTITION_ALREADY_DISABLED)
      return ERR_PARTITION_DISABLED;
    else if (iex.errorCode() == ERR_PARTITION_NOT_EXIST)
      return ERR_NOT_EXIST_PARTITION;
    else if (iex.errorCode() == ERR_INVALID_LAST_PARTITION)
      return ERR_INVALID_OP_LAST_PARTITION;
    else
      return -1;
  }
  catch (DBRMException& e)
  {
    // exceptions that can be ignored
    return 0;
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Restore all extents for the specified OID(s) and partition number.
//------------------------------------------------------------------------------
int SlaveDBRMNode::restorePartition(const std::set<OID_t>& oids, set<LogicalPartition>& partitionNums,
                                    string& emsg) throw()
{
  try
  {
    em.restorePartition(oids, partitionNums, emsg);
  }
  catch (IDBExcept& iex)
  {
    cerr << iex.what() << endl;

    if (iex.errorCode() == ERR_PARTITION_NOT_EXIST)
      return ERR_NOT_EXIST_PARTITION;
    else if (iex.errorCode() == ERR_PARTITION_ALREADY_ENABLED)
      return ERR_PARTITION_ENABLED;
    else if (iex.errorCode() == ERR_INVALID_LAST_PARTITION)
      return ERR_INVALID_OP_LAST_PARTITION;
    else
      return -1;
  }
  catch (DBRMException& e)
  {
    // exceptions that can be ignored
    return 0;
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Delete all extents for the dbroot
//------------------------------------------------------------------------------
int SlaveDBRMNode::deleteDBRoot(uint16_t dbroot) throw()
{
  try
  {
    em.deleteDBRoot(dbroot);
  }
  catch (IDBExcept& iex)
  {
    cerr << iex.what() << endl;
    return -1;
  }
  catch (DBRMException& e)
  {
    // exceptions that can be ignored
    return 0;
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }

  return 0;
}

int SlaveDBRMNode::dmlLockLBIDRanges(const vector<LBIDRange>& ranges, int txnID)
{
  uint64_t maxRetries;
  uint64_t waitInterval = 50000;  // usecs to sleep between retries
  uint64_t retries;
  bool* lockedRanges = (bool*)alloca(ranges.size() * sizeof(bool));
  bool allLocked;
  uint32_t i;

  /* XXXPAT: The controller node will wait up to 5 mins for the response.
   * For now, this alg will try for 1 min to grab all of the locks.
   * After that, it will release them all then grab them.  Releasing
   * them by force opens the slight possibility of a bad result, but more
   * likely something crashed, and there has to be some kind of recovery.  The worst
   * case is better than stalling the system or causing the BRM to go read-only.
   * It should be extremely rare that it has to be done.
   */
  maxRetries = (60 * 1000000) / waitInterval;

  for (i = 0; i < ranges.size(); i++)
    lockedRanges[i] = false;

  try
  {
    copylocks.lock(CopyLocks::WRITE);
    locked[2] = true;
    allLocked = false;
    /* This version grabs all unlocked ranges in each pass.
     * If there are locked ranges it waits and tries again.
     */
    retries = 0;

    while (!allLocked && retries < maxRetries)
    {
      allLocked = true;

      for (i = 0; i < ranges.size(); i++)
      {
        if (!lockedRanges[i])
        {
          if (copylocks.isLocked(ranges[i]))
            allLocked = false;
          else
          {
            copylocks.lockRange(ranges[i], txnID);
            lockedRanges[i] = true;
          }
        }
      }

      /* PrimProc is reading at least 1 range and it could need the locks.
       */
      if (!allLocked)
      {
        copylocks.release(CopyLocks::WRITE);
        locked[2] = false;
        usleep(waitInterval);
        retries++;
        copylocks.lock(CopyLocks::WRITE);
        locked[2] = true;
      }
    }

    if (retries >= maxRetries)
    {
      for (i = 0; i < ranges.size(); i++)
      {
        if (!lockedRanges[i])
        {
          copylocks.forceRelease(ranges[i]);
          copylocks.lockRange(ranges[i], txnID);
          lockedRanges[i] = true;
        }
      }
    }

    return 0;
  }
  catch (exception& e)
  {
    for (i = 0; i < ranges.size(); i++)
      if (lockedRanges[i])
        copylocks.releaseRange(ranges[i]);

    cerr << e.what() << endl;
    return -1;
  }
}

int SlaveDBRMNode::dmlReleaseLBIDRanges(const vector<LBIDRange>& ranges)
{
  try
  {
    copylocks.lock(CopyLocks::WRITE);
    locked[2] = true;

    for (uint32_t i = 0; i < ranges.size(); ++i)
      copylocks.releaseRange(ranges[i]);

    return 0;
  }
  catch (exception& e)
  {
    cerr << e.what() << endl;
    return -1;
  }
}

const std::atomic<bool>* SlaveDBRMNode::getEMFLLockStatus()
{
  return em.getEMFLLockStatus();
}

const std::atomic<bool>* SlaveDBRMNode::getEMLockStatus()
{
  return em.getEMLockStatus();
}

const std::atomic<bool>* SlaveDBRMNode::getEMIndexLockStatus()
{
  return em.getEMIndexLockStatus();
}

const std::atomic<bool>* SlaveDBRMNode::getVBBMLockStatus()
{
  return &locked[0];
}

const std::atomic<bool>* SlaveDBRMNode::getVSSLockStatus()
{
  return &locked[1];
}

}  // namespace BRM

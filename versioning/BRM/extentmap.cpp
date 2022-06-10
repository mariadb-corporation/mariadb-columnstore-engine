/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016-2022 MariaDB Corporation

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
 * $Id: extentmap.cpp 1936 2013-07-09 22:10:29Z dhall $
 *
 ****************************************************************************/

#include <iostream>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
#include <algorithm>
#include <ios>
#include <cerrno>
#include <sstream>
#include <vector>
#include <limits>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#ifndef _MSC_VER
#include <tr1/unordered_set>
#else
#include <unordered_set>
#endif

#if BOOST_VERSION >= 106501
#include <boost/unordered/detail/implementation.hpp>
#else
#include <boost/unordered/detail/buckets.hpp>
#endif
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
namespace bi = boost::interprocess;

#include "liboamcpp.h"
#include "brmtypes.h"
#include "configcpp.h"
#include "rwlock.h"
#include "calpontsystemcatalog.h"
#include "mastersegmenttable.h"
#include "blocksize.h"
#include "dataconvert.h"
#include "oamcache.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"
#ifdef BRM_INFO
#include "tracer.h"
#include "configcpp.h"
#endif

#define EXTENTMAP_DLLEXPORT
#include "extentmap.h"
#undef EXTENTMAP_DLLEXPORT

#define EM_MAX_SEQNUM               2000000000
#define MAX_IO_RETRIES 10
#define EM_MAGIC_V1 0x76f78b1c
#define EM_MAGIC_V2 0x76f78b1d
#define EM_MAGIC_V3 0x76f78b1e
#define EM_MAGIC_V4 0x76f78b1f

#ifndef NDEBUG
#define ASSERT(x) \
	if (!(x)) { \
		cerr << "assertion at file " << __FILE__ << " line " << __LINE__ << " failed" << endl; \
		throw logic_error("assertion failed"); \
	}
#else
#define ASSERT(x)
#endif

using namespace std;
using namespace boost;
using namespace logging;
using namespace idbdatafile;

namespace
{
unsigned ExtentSize = 0; // dmc-need to deprecate
unsigned ExtentRows              = 0;
unsigned filesPerColumnPartition = 0;
unsigned extentsPerSegmentFile   = 0;

// Increment CP sequence (version) number, and wrap-around when applicable
inline void incSeqNum(int32_t& seqNum)
{
    seqNum++;

    if (seqNum > EM_MAX_SEQNUM)
        seqNum = 0;
}

}

namespace BRM
{
static const char* EmIndexObjectName = "i";
//------------------------------------------------------------------------------
// EMCasualPartition_struct methods
//------------------------------------------------------------------------------

EMCasualPartition_struct::EMCasualPartition_struct()
{
    lo_val = numeric_limits<int64_t>::min();
    hi_val = numeric_limits<int64_t>::max();
    sequenceNum = 0;
    isValid = CP_INVALID;
}

EMCasualPartition_struct::EMCasualPartition_struct(const int64_t lo, const int64_t hi, const int32_t seqNum)
{
    lo_val = lo;
    hi_val = hi;
    sequenceNum = seqNum;
    isValid = CP_INVALID;
}

EMCasualPartition_struct::EMCasualPartition_struct(const EMCasualPartition_struct& em)
{
    lo_val = em.lo_val;
    hi_val = em.hi_val;
    sequenceNum = em.sequenceNum;
    isValid = em.isValid;
}

EMCasualPartition_struct& EMCasualPartition_struct::operator= (const EMCasualPartition_struct& em)
{
    lo_val = em.lo_val;
    hi_val = em.hi_val;
    sequenceNum = em.sequenceNum;
    isValid = em.isValid;
    return *this;
}

//------------------------------------------------------------------------------
// Version 4 EmEntry methods
//------------------------------------------------------------------------------

EMEntry::EMEntry()
{
    fileID = 0;
    blockOffset = 0;
    HWM = 0;
    partitionNum = 0;
    segmentNum   = 0;
    dbRoot       = 0;
    colWid       = 0;
    status		= 0;
}

EMEntry::EMEntry(const EMEntry& e)
{
    range.start = e.range.start;
    range.size = e.range.size;
    fileID = e.fileID;
    blockOffset = e.blockOffset;
    HWM = e.HWM;
    partition = e.partition;
    partitionNum = e.partitionNum;
    segmentNum   = e.segmentNum;
    dbRoot       = e.dbRoot;
    colWid       = e.colWid;
    status		= e.status;
}

EMEntry& EMEntry::operator= (const EMEntry& e)
{
    range.start = e.range.start;
    range.size = e.range.size;
    fileID = e.fileID;
    blockOffset = e.blockOffset;
    HWM = e.HWM;
    partition = e.partition;
    partitionNum = e.partitionNum;
    segmentNum   = e.segmentNum;
    colWid       = e.colWid;
    dbRoot       = e.dbRoot;
    status		= e.status;
    return *this;
}

bool EMEntry::operator< (const EMEntry& e) const
{
    if (range.start < e.range.start)
        return true;

    return false;
}

/*static*/
boost::mutex ExtentMap::mutex;
boost::mutex ExtentMap::emIndexMutex;

boost::mutex ExtentMapRBTreeImpl::fInstanceMutex;
ExtentMapRBTreeImpl* ExtentMapRBTreeImpl::fInstance = nullptr;

ExtentMapRBTreeImpl* ExtentMapRBTreeImpl::makeExtentMapRBTreeImpl(unsigned key, off_t size, bool readOnly)
{
    boost::mutex::scoped_lock lk(fInstanceMutex);

    if (fInstance)
    {
        if (key != fInstance->fManagedShm.key())
        {
            fInstance->fManagedShm.reMapSegment();
        }
        return fInstance;
    }

    fInstance = new ExtentMapRBTreeImpl(key, size, readOnly);
    return fInstance;
}

ExtentMapRBTreeImpl::ExtentMapRBTreeImpl(unsigned key, off_t size, bool readOnly)
    : fManagedShm(key, size, readOnly)
{
}

/*static*/
boost::mutex FreeListImpl::fInstanceMutex;

/*static*/
FreeListImpl* FreeListImpl::fInstance = 0;

/*static*/
FreeListImpl* FreeListImpl::makeFreeListImpl(unsigned key, off_t size, bool readOnly)
{
    boost::mutex::scoped_lock lk(fInstanceMutex);

    if (fInstance)
    {
        if (key != fInstance->fFreeList.key())
        {
            BRMShmImpl newShm(key, 0);
            fInstance->swapout(newShm);
        }

        ASSERT(key == fInstance->fFreeList.key());
        return fInstance;
    }

    fInstance = new FreeListImpl(key, size, readOnly);

    return fInstance;
}

FreeListImpl::FreeListImpl(unsigned key, off_t size, bool readOnly) :
    fFreeList(key, size, readOnly)
{
}

/*static*/
std::mutex ExtentMapIndexImpl::fInstanceMutex_;

/*static*/
ExtentMapIndexImpl* ExtentMapIndexImpl::fInstance_ = nullptr;

/*static*/
ExtentMapIndexImpl* ExtentMapIndexImpl::makeExtentMapIndexImpl(unsigned key, off_t size, bool readOnly)
{
    std::lock_guard<std::mutex> lock(fInstanceMutex_);

    if (fInstance_)
    {
        if (size != fInstance_->getShmemSize())
        {
            fInstance_->fBRMManagedShmMemImpl_.remap();
        }

        return fInstance_;
    }

    fInstance_ = new ExtentMapIndexImpl(key, size, readOnly);
    fInstance_->createExtentMapIndexIfNeeded();

    return fInstance_;
}

ExtentMapIndexImpl::ExtentMapIndexImpl(unsigned key, off_t size, bool readOnly) :
    fBRMManagedShmMemImpl_(key, size, readOnly)
{
}

void ExtentMapIndexImpl::createExtentMapIndexIfNeeded()
{
    // pair<T*, size>
    auto managedShmemSearchPair = fBRMManagedShmMemImpl_.getManagedSegment()->find<ExtentMapIndex>(EmIndexObjectName);
    if (!managedShmemSearchPair.first || managedShmemSearchPair.second == 0)
    {
        ShmVoidAllocator alloc(fBRMManagedShmMemImpl_.getManagedSegment()->get_segment_manager());
        fBRMManagedShmMemImpl_.getManagedSegment()->construct<ExtentMapIndex>(EmIndexObjectName)(alloc);
    }
}

ExtentMapIndex* ExtentMapIndexImpl::get()
{
    // pair<T*, size>
    auto managedShmemSearchPair = fBRMManagedShmMemImpl_.getManagedSegment()->find<ExtentMapIndex>(EmIndexObjectName);
    assert(managedShmemSearchPair.first && managedShmemSearchPair.second > 0);
    return managedShmemSearchPair.first;
}

bool ExtentMapIndexImpl::growIfNeeded(const size_t memoryNeeded)
{
    auto freeShmem = getShmemFree();
    // Worst case managed segment can't get continues buffer with len = memoryNeeded
    if (freeShmem < memoryNeeded)
    {
        const size_t currentShmemSize = getShmemSize();
        constexpr static const size_t minAllowance = 16 * 1024 * 1024;
        const size_t newShmemSize = std::max(minAllowance, memoryNeeded) + currentShmemSize;
        grow(newShmemSize);
        return true;
    }
    return false;
}

InsertUpdateShmemKeyPair ExtentMapIndexImpl::insert(const EMEntry& emEntry, const LBID_t lbid)
{
    auto dbRoot = emEntry.dbRoot;
    assert(dbRoot > 0);
    auto* extentMapIndexPtr = get();
    bool shmemHasGrown = false;

    while (dbRoot >= extentMapIndexPtr->size())
    {
        const size_t memNeeded = (extentMapIndexPtr->capacity() + extraUnits_) * dbRootContainerUnitSize_;
        shmemHasGrown = growIfNeeded(memNeeded);
        // Need to refresh all refs and iterators b/c the local address range changed.
        extentMapIndexPtr = get();
        assert(extentMapIndexPtr);
        ShmVoidAllocator alloc(fBRMManagedShmMemImpl_.getManagedSegment()->get_segment_manager());
        OIDIndexContainerT oidIndices(alloc);
        extentMapIndexPtr->push_back(std::move(oidIndices));
    }
    auto& extentMapIndex = *extentMapIndexPtr;
    return insert2ndLayerWrapper(extentMapIndex[dbRoot], emEntry, lbid, shmemHasGrown);
}

InsertUpdateShmemKeyPair ExtentMapIndexImpl::insert2ndLayer(OIDIndexContainerT& oids,
                                                            const EMEntry& emEntry,
                                                            const LBID_t lbid,
                                                            const bool aShmemHasGrown)
{
    OID_t oid = emEntry.fileID;
    ShmVoidAllocator alloc(fBRMManagedShmMemImpl_.getManagedSegment()->get_segment_manager());

    PartitionIndexContainerT partitionIndex(alloc);
    auto iterAndResult = oids.insert({oid, std::move(partitionIndex)});

    if (iterAndResult.second)
    {
        PartitionIndexContainerT& partitionsContainer = (*iterAndResult.first).second;
        return insert3dLayerWrapper(partitionsContainer, emEntry, lbid, aShmemHasGrown);
    }
    else
        return {false, aShmemHasGrown};
}

InsertUpdateShmemKeyPair ExtentMapIndexImpl::insert2ndLayerWrapper(OIDIndexContainerT& oids,
                                                                   const EMEntry& emEntry,
                                                                   const LBID_t lbid,
                                                                   const bool aShmemHasGrown)
{
    OID_t oid = emEntry.fileID;
    auto oidsIter = oids.find(oid);
    bool shmemHasGrown = aShmemHasGrown;
    if (oidsIter == oids.end())
    {
        const size_t freeShmem = fBRMManagedShmMemImpl_.getManagedSegment()->get_free_memory();
        const size_t memNeeded = (oids.size() + extraUnits_) * oidContainerUnitSize_;
        if (oids.load_factor() >= oids.max_load_factor() ||
            freeShmem <= freeSpaceThreshold_)
        {
            // Need to refresh all refs and iterators b/c the local address range changed.
            shmemHasGrown = growIfNeeded(memNeeded);
            auto* extMapIndexPtr = get();
            assert(extMapIndexPtr);
            auto& extMapIndex = *extMapIndexPtr;
            shmemHasGrown = shmemHasGrown || aShmemHasGrown;
            // The dbroot must be here b/c it was already found once in the upper insert().
            OIDIndexContainerT& refreshedOidsRef = extMapIndex[emEntry.dbRoot];
            return insert2ndLayer(refreshedOidsRef, emEntry, lbid, shmemHasGrown);
        }
        return insert2ndLayer(oids, emEntry, lbid, shmemHasGrown);
    }
    PartitionIndexContainerT& partitions = (*oidsIter).second;
    return insert3dLayerWrapper(partitions, emEntry, lbid, shmemHasGrown);
}

// This method presumes and doesn't check that emEntry.dbRoot exists at the 1st layer
PartitionIndexContainerT& ExtentMapIndexImpl::refreshPartitionRef(const EMEntry& emEntry)
{
    auto* extMapIndexPtr = get();
    assert(extMapIndexPtr);
    auto& extMapIndex = *extMapIndexPtr;
    // The dbroot must be here b/c we found it once in insert().
    OIDIndexContainerT& refreshedOidsRef = extMapIndex[emEntry.dbRoot];
    auto oidsIter = refreshedOidsRef.find(emEntry.fileID);
    PartitionIndexContainerT& refreshedPartitionsRef = (*oidsIter).second;
    return refreshedPartitionsRef;
}

InsertUpdateShmemKeyPair ExtentMapIndexImpl::insert3dLayer(PartitionIndexContainerT& partitions,
                                                           const EMEntry& emEntry,
                                                           const LBID_t lbid,
                                                           const bool aShmemHasGrown)
{
    auto partitionNumber = emEntry.partitionNum;
    ShmVoidAllocator alloc(fBRMManagedShmMemImpl_.getManagedSegment()->get_segment_manager());
    LBID_tVectorT lbids(alloc);
    lbids.push_back(lbid);
    // map node is 56 bytes but rehashing allocates a dynamic number of bytes so
    // the rehashing might throw
    auto iterAndResult = partitions.insert({partitionNumber, std::move(lbids)});
    return {iterAndResult.second, aShmemHasGrown};
}

bool ExtentMapIndexImpl::fragmentedMemExceptionHandler(const std::string& funcName,
    bi::bad_alloc& ex, const size_t partitionsSize)
{
    std::ostringstream os;
    os << funcName << ": " << ex.what() << ". Extending managed shared memory and continue." << std::endl;
    log(os.str(), logging::LOG_TYPE_INFO);
    // Extend managed shmem
    static const size_t minGrowSize = 16 * 1024 * 1024;
    // This number is taken from boost unordered impl and might change in future with
    // a small probability
    const auto bucketCapacity = boost::unordered::detail::next_prime(partitionsSize + 1);
    const size_t currentShmemSize = getShmemSize();
    const size_t newSize = std::max(bucketCapacity * partitionContainerUnitSize_, minGrowSize) + currentShmemSize;
    // If the grow() throws then workernode runtime throws and a cluster becomes read-only that is expected
    grow(newSize);
    return true;
}

InsertUpdateShmemKeyPair
ExtentMapIndexImpl::insert3dLayerWrapper(PartitionIndexContainerT& partitions,
                                         const EMEntry& emEntry, const LBID_t lbid,
                                         const bool aShmemHasGrown)
{
    auto partitionNumber = emEntry.partitionNum;
    auto partitionsIter = partitions.find(partitionNumber);
    bool shmemHasGrown = aShmemHasGrown;
    if (partitionsIter == partitions.end())
    {
        const size_t freeShmem = fBRMManagedShmMemImpl_.getManagedSegment()->get_free_memory();
        const size_t memNeeded = (partitions.size() + extraUnits_) * partitionContainerUnitSize_ + emIdentUnitSize_;
        if (partitions.load_factor() >= partitions.max_load_factor() ||
            freeShmem <= freeSpaceThreshold_)
        {
            // Need to refresh all refs and iterators b/c the local address range changed.
            shmemHasGrown = growIfNeeded(memNeeded);
            shmemHasGrown = shmemHasGrown || aShmemHasGrown;
            PartitionIndexContainerT& refreshedPartitionsRef = refreshPartitionRef(emEntry);
            try
            {
                return insert3dLayer(refreshedPartitionsRef, emEntry, lbid, shmemHasGrown);
            }
            catch (bi::bad_alloc& ex)
            {
                static const std::string funcName{"insert3dLayerWrapper()"};
                fragmentedMemExceptionHandler(funcName, ex, partitions.size());
                PartitionIndexContainerT& refreshedPartitionsRef = refreshPartitionRef(emEntry);
                ShmVoidAllocator alloc(fBRMManagedShmMemImpl_.getManagedSegment()->get_segment_manager());
                LBID_tVectorT emIndices(alloc);
                emIndices.push_back(lbid);
                shmemHasGrown = true;
                return insert3dLayer(refreshedPartitionsRef, emEntry, lbid, shmemHasGrown);
            }
        }
        return insert3dLayer(partitions, emEntry, lbid, shmemHasGrown);
    }

    LBID_tVectorT& lbids = (*partitionsIter).second;
    lbids.push_back(lbid);
    return {true, shmemHasGrown};
}

LBID_tFindResult ExtentMapIndexImpl::find(const DBRootT dbroot, const OID_t oid,
                                          const PartitionNumberT partitionNumber)
{
    ExtentMapIndex& emIndex = *get();
    if (dbroot >= emIndex.size())
        return {};
    return search2ndLayer(emIndex[dbroot], oid, partitionNumber);
}

LBID_tFindResult ExtentMapIndexImpl::find(const DBRootT dbroot, const OID_t oid)
{
    ExtentMapIndex& emIndex = *get();
    if (dbroot >= emIndex.size())
        return {};
    return search2ndLayer(emIndex[dbroot], oid);
}

LBID_tFindResult ExtentMapIndexImpl::search2ndLayer(OIDIndexContainerT& oids, const OID_t oid,
                                                    const PartitionNumberT partitionNumber)
{
    auto oidsIter = oids.find(oid);
    if (oidsIter == oids.end())
        return {};

    PartitionIndexContainerT& partitions = (*oidsIter).second;
    return search3dLayer(partitions, partitionNumber);
}

LBID_tFindResult ExtentMapIndexImpl::search2ndLayer(OIDIndexContainerT& oids, const OID_t oid)
{
    auto oidsIter = oids.find(oid);
    if (oidsIter == oids.end())
        return {};

    LBID_tFindResult result;
    PartitionIndexContainerT& partitions = (*oidsIter).second;
    for (auto& partKeyValue: partitions)
    {
        LBID_tVectorT& emIdentifiers = partKeyValue.second;
        for (auto& emIdent: emIdentifiers)
            result.push_back(emIdent);
    }

    return result;
}

LBID_tFindResult ExtentMapIndexImpl::search3dLayer(PartitionIndexContainerT& partitions,
                                                   const PartitionNumberT partitionNumber)
{
    auto partitionsIter = partitions.find(partitionNumber);
    if (partitionsIter == partitions.end())
        return {};

    LBID_tFindResult result;
    LBID_tVectorT& emIndicesVec = (*partitionsIter).second;
    for (auto& emIndex: emIndicesVec)
        result.push_back(emIndex);
    return result;
}

bool ExtentMapIndexImpl::isDBRootEmpty(const DBRootT dbroot)
{
    ExtentMapIndex& extMapIndex = *get();
    if (dbroot >= extMapIndex.size())
        return true;
    return extMapIndex[dbroot].empty();
}

void ExtentMapIndexImpl::deleteDbRoot(const DBRootT dbroot)
{
    auto& extMapIndex = *get();
    if (dbroot >= extMapIndex.size())
      return;  // nothing to delete
    extMapIndex[dbroot].clear();
}

void ExtentMapIndexImpl::deleteOID(const DBRootT dbroot, const OID_t oid)
{
    auto& extMapIndex = *get();
     // nothing to delete
    // In some cases containers demonstrate UB that might allow to access
    // dbroot that doesn't exist. In some cases this ends up with workernode
    // recieves FPE signal.
    if (dbroot >= extMapIndex.size())
        return;
    if (extMapIndex[dbroot].empty())
        return;
    auto oidsIter = extMapIndex[dbroot].find(oid);
    // Nothing to delete. Might be a sign of a problem.
    if (oidsIter == extMapIndex[dbroot].end())
        return;
    extMapIndex[dbroot].erase(oidsIter);
}

void ExtentMapIndexImpl::deleteEMEntry(const EMEntry& emEntry, const ExtentMapIdxT emIdent)
{
    // find partition
    auto& extMapIndex = *get();
    if (emEntry.dbRoot >= extMapIndex.size())
        return;
    if (extMapIndex[emEntry.dbRoot].empty())
        return;
    auto oidsIter = extMapIndex[emEntry.dbRoot].find(emEntry.fileID);
    if (oidsIter == extMapIndex[emEntry.dbRoot].end())
        return;
    PartitionIndexContainerT& partitions = (*oidsIter).second;
    auto partitionsIter = partitions.find(emEntry.partitionNum);
    if (partitionsIter == partitions.end())
        return;
    LBID_tVectorT& emIdentifiers = (*partitionsIter).second;
    // pop the identifier
    if (emIdentifiers.size() > 1)
    {
        auto emIdentifiersTargetIter = std::find(emIdentifiers.begin(), emIdentifiers.end(), emIdent);
        std::swap(*emIdentifiersTargetIter, emIdentifiers.back());
        emIdentifiers.pop_back();
    }
    else // only 1 ident in this partition
    {
        partitions.erase(partitionsIter);
    }
}

ExtentMap::ExtentMap()
{
    fCurrentEMShmkey = -1;
    fCurrentFLShmkey = -1;

    fEMRBTreeShminfo = nullptr;
    fEMIndexShminfo = nullptr;
    fFLShminfo = nullptr;

    r_only = false;
    flLocked = false;
    emLocked = false;
    emIndexLocked = false;

    fPFreeListImpl = nullptr;
    fPExtMapIndexImpl_ = nullptr;
    fPExtMapRBTreeImpl = nullptr;

    fFreeList = nullptr;
    fExtentMapRBTree = nullptr;

#ifdef BRM_INFO
    fDebug = ("Y" == config::Config::makeConfig()->getConfig("DBRM", "Debug"));
#endif
}

ExtentMap::~ExtentMap()
{
    PmDbRootMap_t::iterator iter = fPmDbRootMap.begin();
    PmDbRootMap_t::iterator end = fPmDbRootMap.end();

    while (iter != end)
    {
        delete iter->second;
        iter->second = 0;
        ++iter;
    }

    fPmDbRootMap.clear();
}

ExtentMapRBTree::iterator ExtentMap::findByLBID(const LBID_t lbid)
{
    auto emIt = fExtentMapRBTree->lower_bound(lbid);
    auto end = fExtentMapRBTree->end();
    if (emIt == end)
    {
        if (fExtentMapRBTree->size() == 0)
            return end;

        // Check the last one.
        auto last = std::prev(end);
        const auto lastBlock = (last->second.range.size * 1024);
        if ((last->first <= lbid) && (lbid < (last->first + lastBlock)))
        {
            return last;
        }
        return end;
    }

    // Lower bound returns the first element not less than the given key.
    if (emIt->first != lbid)
    {
        if (emIt == fExtentMapRBTree->begin())
        {
            return end;
        }
        emIt = std::prev(emIt);
    }

    return emIt;
}

// Casual Partioning support
//

/**
* @brief mark the max/min values of an extent as invalid
*
* mark the extent containing the lbid as invalid and
* increment the sequenceNum value. If the lbid is found
* in the extent map a 0 is returned otherwise a 1.
*
**/

int ExtentMap::_markInvalid(const LBID_t lbid, const execplan::CalpontSystemCatalog::ColDataType colDataType)
{
  auto emIt = findByLBID(lbid);
  if (emIt == fExtentMapRBTree->end())
    throw logic_error("ExtentMap::markInvalid(): lbid isn't allocated");

  auto& emEntry = emIt->second;
  makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
  emEntry.partition.cprange.isValid = CP_UPDATING;

  if (isUnsigned(colDataType))
  {
    emEntry.partition.cprange.lo_val = numeric_limits<uint64_t>::max();
    emEntry.partition.cprange.hi_val = 0;
  }
  else
  {
    emEntry.partition.cprange.lo_val = numeric_limits<int64_t>::max();
    emEntry.partition.cprange.hi_val = numeric_limits<int64_t>::min();
  }

  incSeqNum(emEntry.partition.cprange.sequenceNum);
  return 0;
}

int ExtentMap::markInvalid(const LBID_t lbid,
                           const execplan::CalpontSystemCatalog::ColDataType colDataType)
{
#ifdef BRM_DEBUG

    if (lbid < 0)
        throw invalid_argument("ExtentMap::markInvalid(): lbid must be >= 0");

#endif
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("_markInvalid");
        TRACER_ADDINPUT(lbid);
        TRACER_WRITE;
    }

#endif

#ifdef BRM_DEBUG
    ostringstream os;
    os << "ExtentMap::markInvalid(" << lbid << "," << colDataType << ")";
    log(os.str(), logging::LOG_TYPE_DEBUG);
#endif

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
    return _markInvalid(lbid, colDataType);
}

/**
* @brief calls markInvalid(LBID_t lbid) for each extent containing any lbid in vector<LBID_t>& lbids
*
**/

int ExtentMap::markInvalid(const vector<LBID_t>& lbids,
                           const vector<execplan::CalpontSystemCatalog::ColDataType>& colDataTypes)
{
    uint32_t i, size = lbids.size();

#ifdef BRM_DEBUG

    for (i = 0; i < size; ++i)
        if (lbids[i] < 0)
            throw invalid_argument("ExtentMap::markInvalid(vector): all lbids must be >= 0");

#endif
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("_markInvalid");
        TRACER_ADDINPUT(size);
        TRACER_WRITE;
    }

#endif

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);

    // XXXPAT: what's the proper return code when one and only one fails?
    for (i = 0; i < size; ++i)
    {
#ifdef BRM_DEBUG
        ostringstream os;
        os << "ExtentMap::markInvalid() lbids[" << i << "]=" << lbids[i] <<
           " colDataTypes[" << i << "]=" << colDataTypes[i];
        log(os.str(), logging::LOG_TYPE_DEBUG);
#endif

        try
        {
            _markInvalid(lbids[i], colDataTypes[i]);
        }
        catch (std::exception& e)
        {
            cerr << "ExtentMap::markInvalid(vector): warning!  lbid " << lbids[i] <<
                 " caused " << e.what() << endl;
        }
    }

    return 0;
}

/**
* @brief set the max/min values for the extent if the seqNum matches the extents sequenceNum
*
* reset the lbid's hi_val to max and lo_val to min
* the seqNum matches the ExtentMap.sequenceNum. Then increments
* the current sequenceNum value by 1. If the sequenceNum does not
* match the seqNum value do not update the lbid's max/min values
* or increment the sequenceNum value and return a -1.

**/

int ExtentMap::setMaxMin(const LBID_t lbid, const int64_t max, const int64_t min, const int32_t seqNum,
                         bool firstNode)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("updateMaxMin");
        TRACER_ADDINPUT(lbid);
        TRACER_ADDINPUT(max);
        TRACER_ADDINPUT(min);
        TRACER_ADDINPUT(seqNum);
        TRACER_WRITE;
    }

#endif
    int32_t curSequence;

#ifdef BRM_DEBUG

    if (lbid < 0)
        throw invalid_argument("ExtentMap::setMaxMin(): lbid must be >= 0");

#endif

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);

    auto emIt = findByLBID(lbid);
    if (emIt == fExtentMapRBTree->end())
        throw logic_error("ExtentMap::getMaxMin(): that lbid isn't allocated");

    auto& emEntry = emIt->second;
    curSequence = emEntry.partition.cprange.sequenceNum;

#ifdef BRM_DEBUG
    if (firstNode)
    {
      ostringstream os;
      os << "ExtentMap::setMaxMin(): casual partitioning update: firstLBID=" << emEntry.range.start
         << " lastLBID=" << emEntry.range.start + emEntry.range.size * 1024 - 1 << " OID=" << emEntry.fileID
         << " min=" << min << " max=" << max << "seq=" << seqNum;
      log(os.str(), logging::LOG_TYPE_DEBUG);
    }

#endif

    if (curSequence == seqNum)
    {
      makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
      emEntry.partition.cprange.hi_val = max;
      emEntry.partition.cprange.lo_val = min;
      emEntry.partition.cprange.isValid = CP_VALID;
      incSeqNum(emEntry.partition.cprange.sequenceNum);
      return 0;
    }
    // special val to indicate a reset--used by editem -c.
    // Also used by COMMIT and ROLLBACK to invalidate CP.
    else if (seqNum == -1)
    {
      makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
      // We set hi_val and lo_val to correct values for signed or unsigned
      // during the markinvalid step, which sets the invalid variable to CP_UPDATING.
      // During this step (seqNum == -1), the min and max passed in are not reliable
      // and should not be used.
      emEntry.partition.cprange.isValid = CP_INVALID;
      incSeqNum(emEntry.partition.cprange.sequenceNum);
      return 0;
    }
    else
    {
      return 0;
    }

    if (emIndexLocked)
        releaseEMIndex(WRITE);

    if (emLocked)
        releaseEMEntryTable(WRITE);

    throw logic_error("ExtentMap::setMaxMin(): lbid isn't allocated");
}

// @bug 1970.  Added updateExtentsMaxMin function.
// @note - The key passed in the map must the the first LBID in the extent.
void ExtentMap::setExtentsMaxMin(const CPMaxMinMap_t& cpMap, bool firstNode, bool useLock)
{
    CPMaxMinMap_t::const_iterator it;

#ifdef BRM_DEBUG
    log("ExtentMap::setExtentsMaxMin()", logging::LOG_TYPE_DEBUG);

    for (it = cpMap.begin(); it != cpMap.end(); ++it)
    {
        ostringstream os;
        os << "FirstLBID=" << it->first <<
           " min=" << it->second.min <<
           " max=" << it->second.max <<
           " seq=" << it->second.seqNum;
        log(os.str(), logging::LOG_TYPE_DEBUG);
    }

#endif


#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("setExtentsMaxMin");

        for (it = cpMap.begin(); it != cpMap.end(); ++it)
        {
            TRACER_ADDINPUT((*it).first);
            TRACER_ADDINPUT((*it).second.max);
            TRACER_ADDINPUT((*it).second.min);
            TRACER_ADDINPUT((*it).second.seqNum);
            TRACER_WRITE;
        }
    }

#endif
    int32_t curSequence;
    const int32_t extentsToUpdate = cpMap.size();
    int32_t extentsUpdated = 0;

#ifdef BRM_DEBUG

    if (extentsToUpdate <= 0)
      throw invalid_argument("ExtentMap::setExtentsMaxMin(): cpMap must be populated");

#endif

    if (useLock)
    {
      grabEMEntryTable(WRITE);
      grabEMIndex(WRITE);
    }

    for (it = cpMap.begin(); it != cpMap.end(); ++it)
    {
      auto emIt = findByLBID(it->first);
      if (emIt == fExtentMapRBTree->end())
        throw logic_error("ExtentMap::setExtentsMaxMin(): lbid not found");

      auto& emEntry = emIt->second;
      curSequence = emEntry.partition.cprange.sequenceNum;

      if (curSequence == it->second.seqNum && emEntry.partition.cprange.isValid == CP_INVALID)
      {
        makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
        emEntry.partition.cprange.hi_val = it->second.max;
        emEntry.partition.cprange.lo_val = it->second.min;
        emEntry.partition.cprange.isValid = CP_VALID;
        incSeqNum(emEntry.partition.cprange.sequenceNum);
        extentsUpdated++;
      }
      // special val to indicate a reset -- ignore the min/max
      else if (it->second.seqNum == -1)
      {
        makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
        // We set hiVal and loVal to correct values for signed or unsigned
        // during the markinvalid step, which sets the invalid variable to CP_UPDATING.
        // During this step (seqNum == -1), the min and max passed in are not reliable
        // and should not be used.
        emEntry.partition.cprange.isValid = CP_INVALID;
        incSeqNum(emEntry.partition.cprange.sequenceNum);
        extentsUpdated++;
      }
      // special val to indicate a reset -- assign the min/max
      else if (it->second.seqNum == -2)
      {
        makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
        emEntry.partition.cprange.hi_val = it->second.max;
        emEntry.partition.cprange.lo_val = it->second.min;
        emEntry.partition.cprange.isValid = CP_INVALID;
        incSeqNum(emEntry.partition.cprange.sequenceNum);
        extentsUpdated++;
      }
      // else sequence has changed since start of the query.  Don't update the EM entry.
      else
      {
        extentsUpdated++;
      }

      if (extentsUpdated == extentsToUpdate)
      {
        return;
      }
    }

    throw logic_error("ExtentMap::setExtentsMaxMin(): lbid isn't allocated");
}

//------------------------------------------------------------------------------
// @bug 1970.  Added mergeExtentsMaxMin to merge CP info for list of extents.
// @note - The key passed in the map must the starting LBID in the extent.
// Used by cpimport to update extentmap casual partition min/max.
// nullptr or empty values should not be passed in as min/max values.
// seqNum in the input struct is not currently used.
//
// Note that DML calls markInvalid() to flag an extent as CP_UPDATING and incre-
// ments the sequence number prior to any change, and then marks the extent as
// CP_INVALID at transaction's end.
// Since cpimport locks the entire table prior to making any changes, it is
// assumed that the state of an extent will not be changed (by anyone else)
// during an import; so cpimport does not employ the intermediate CP_UPDATING
// state that DML uses.  cpimport just waits till the end of the job and incre-
// ments the sequence number and changes the state to CP_INVALID at that time.
// We may want/need to reconsider this at some point.
//------------------------------------------------------------------------------
void ExtentMap::mergeExtentsMaxMin(CPMaxMinMergeMap_t& cpMap, bool useLock)
{
  CPMaxMinMergeMap_t::const_iterator it;

#ifdef BRM_DEBUG
  log("ExtentMap::mergeExtentsMaxMin()", logging::LOG_TYPE_DEBUG);

  for (it = cpMap.begin(); it != cpMap.end(); ++it)
  {
    ostringstream os;
    os << "FirstLBID=" << it->first << " min=" << it->second.min << " max=" << it->second.max
       << " seq=" << it->second.seqNum << " typ: " << (*it).second.type << " new: " << (*it).second.newExtent;
    log(os.str(), logging::LOG_TYPE_DEBUG);
  }

#endif

#ifdef BRM_INFO

  if (fDebug)
  {
    TRACER_WRITENOW("mergeExtentsMaxMin");
    unsigned int count = 1;

    for (it = cpMap.begin(); it != cpMap.end(); ++it)
    {
      ostringstream oss;
      oss << "  " << count << ". LBID: " << (*it).first << "; max: " << (*it).second.max
          << "; min: " << (*it).second.min << "; seq: " << (*it).second.seqNum
          << "; typ: " << (*it).second.type << "; new: " << (*it).second.newExtent;
      TRACER_WRITEDIRECT(oss.str());
      count++;
    }
  }

#endif

  const int32_t extentsToMerge = cpMap.size();
  int32_t extentsMerged = 0;

#ifdef BRM_DEBUG

  if (extentsToMerge <= 0)
    throw invalid_argument(
        "ExtentMap::mergeExtentsMaxMin(): "
        "cpMap must be populated");

#endif

  if (useLock)
  {
    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
  }

  for (it = cpMap.begin(); it != cpMap.end(); ++it)
  {
    auto emIt = findByLBID(it->first);
    if (emIt == fExtentMapRBTree->end())
      throw logic_error("ExtentMap::mergeExtentsMaxMin(): lbid not found");

    auto& emEntry = emIt->second;

#ifdef BRM_DEBUG
    ostringstream os;
    os << "ExtentMap::mergeExtentsMaxMin(): casual partitioning update: firstLBID=" << emEntry.range.start
       << " lastLBID=" << emEntry.range.start + emEntry.range.size * 1024 - 1 << " OID=" << emEntry.fileID
       << " hi_val=" << emEntry.partition.cprange.hi_val << " lo_val=" << emEntry.partition.cprange.lo_val
       << " min=" << it->second.min << " max=" << it->second.max << " seq=" << it->second.seqNum;
    log(os.str(), logging::LOG_TYPE_DEBUG);
#endif

    switch (emEntry.partition.cprange.isValid)
    {
      // Merge input min/max with current min/max
      case CP_VALID:
      {
        if (!isValidCPRange(it->second.max, it->second.min, it->second.type))
        {
          break;
        }

        makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);

        // We check the validity of the current min/max,
        // because isValid could be CP_VALID for an extent
        // having all NULL values, in which case the current
        // min/max needs to be set instead of merged.

        if (isValidCPRange(emEntry.partition.cprange.hi_val, emEntry.partition.cprange.lo_val,
                           it->second.type))
        {
          // Swap byte order to do binary string comparison
          if (isCharType(it->second.type))
          {
            int64_t newMinVal = static_cast<int64_t>(uint64ToStr(static_cast<uint64_t>(it->second.min)));
            int64_t newMaxVal = static_cast<int64_t>(uint64ToStr(static_cast<uint64_t>(it->second.max)));
            int64_t oldMinVal =
                static_cast<int64_t>(uint64ToStr(static_cast<uint64_t>(emEntry.partition.cprange.lo_val)));
            int64_t oldMaxVal =
                static_cast<int64_t>(uint64ToStr(static_cast<uint64_t>(emEntry.partition.cprange.hi_val)));

            if (newMinVal < oldMinVal)
              emEntry.partition.cprange.lo_val = it->second.min;

            if (newMaxVal > oldMaxVal)
              emEntry.partition.cprange.hi_val = it->second.max;
          }
          else if (isUnsigned(it->second.type))
          {
            if (static_cast<uint64_t>(it->second.min) <
                static_cast<uint64_t>(emEntry.partition.cprange.lo_val))
            {
              emEntry.partition.cprange.lo_val = it->second.min;
            }

            if (static_cast<uint64_t>(it->second.max) >
                static_cast<uint64_t>(emEntry.partition.cprange.hi_val))
            {
              emEntry.partition.cprange.hi_val = it->second.max;
            }
          }
          else
          {
            if (it->second.min < emEntry.partition.cprange.lo_val)
              emEntry.partition.cprange.lo_val = it->second.min;

            if (it->second.max > emEntry.partition.cprange.hi_val)
              emEntry.partition.cprange.hi_val = it->second.max;
          }
        }
        else
        {
          emEntry.partition.cprange.lo_val = it->second.min;
          emEntry.partition.cprange.hi_val = it->second.max;
        }

        incSeqNum(emEntry.partition.cprange.sequenceNum);
        break;
      }

      // DML is updating; just increment seqnum.
      // This case is here for completeness.  Table lock should
      // prevent this state from occurring (see notes at top of
      // this function)
      case CP_UPDATING:
      {
        makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
        incSeqNum(emEntry.partition.cprange.sequenceNum);

        break;
      }

      // Reset min/max to new min/max only "if" we can treat this
      // as a new extent, else leave the extent marked as INVALID
      case CP_INVALID:
      default:
      {
        makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
        if (it->second.newExtent)
        {
          if (isValidCPRange(it->second.max, it->second.min, it->second.type))
          {
            emEntry.partition.cprange.lo_val = it->second.min;
            emEntry.partition.cprange.hi_val = it->second.max;
          }

          // Even if invalid range; we set state to CP_VALID,
          // because the extent is valid, it is just empty.
          emEntry.partition.cprange.isValid = CP_VALID;
        }

        incSeqNum(emEntry.partition.cprange.sequenceNum);
        break;
      }
    }  // switch on isValid state

    extentsMerged++;

    if (extentsMerged == extentsToMerge)
    {
      return;  // Leave when all extents in map are matched
    }
  }

  throw logic_error("ExtentMap::mergeExtentsMaxMin(): lbid not found");
}

//------------------------------------------------------------------------------
// Use this function to see if the range is a valid min/max range or not.
// Range is considered invalid if min or max, are nullptr (min()), or EMPTY
// (min()+1). For unsigned types nullptr is max() and EMPTY is max()-1.
//------------------------------------------------------------------------------
bool ExtentMap::isValidCPRange(int64_t max, int64_t min,
                               execplan::CalpontSystemCatalog::ColDataType type) const
{
    if (isUnsigned(type))
    {
        if ((static_cast<uint64_t>(min) >= (numeric_limits<uint64_t>::max() - 1)) ||
            (static_cast<uint64_t>(max) >= (numeric_limits<uint64_t>::max() - 1)))
        {
            return false;
        }
    }
    else
    {
        if ((min <= (numeric_limits<int64_t>::min() + 1)) || (max <= (numeric_limits<int64_t>::min() + 1)))
        {
            return false;
        }
    }

    return true;
}

/**
 * @brief retrieve the hi_val and lo_val or sequenceNum of the extent containing the LBID lbid.
 *
 * For the extent containing the LBID lbid, return the max/min values if the extent range values
 * are valid and a -1 in the seqNum parameter. If the range values are flaged as invalid
 * return the sequenceNum of the extent and the max/min values as -1.
 **/

int ExtentMap::getMaxMin(const LBID_t lbid, int64_t& max, int64_t& min, int32_t& seqNum)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("getMaxMin");
        TRACER_ADDINPUT(lbid);
        TRACER_ADDOUTPUT(max);
        TRACER_ADDOUTPUT(min);
        TRACER_ADDOUTPUT(seqNum);
        TRACER_WRITE;
    }

#endif
    max = numeric_limits<uint64_t>::max();
    min = 0;
    seqNum *= (-1);
    int isValid = CP_INVALID;

#ifdef BRM_DEBUG

    if (lbid < 0)
        throw invalid_argument("ExtentMap::getMaxMin(): lbid must be >= 0");

#endif

    grabEMEntryTable(READ);
    grabEMIndex(READ);
    auto emIt = findByLBID(lbid);

    if (emIt == fExtentMapRBTree->end())
        throw logic_error("ExtentMap::getMaxMin(): that lbid isn't allocated");

    auto& emEntry = emIt->second;
    max = emEntry.partition.cprange.hi_val;
    min = emEntry.partition.cprange.lo_val;
    seqNum = emEntry.partition.cprange.sequenceNum;
    isValid = emEntry.partition.cprange.isValid;

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
    return isValid;
}

/* Removes a range from the freelist.  Used by load() */
void ExtentMap::reserveLBIDRange(LBID_t start, uint8_t size)
{
    int i;
    int flEntries = fFLShminfo->allocdSize / sizeof(InlineLBIDRange);
    LBID_t lastLBID = start + (size * 1024) - 1;
    int32_t freeIndex = -1;

    /* Find a range the request intersects.  There should be one and only one. */
    for (i = 0; i < flEntries; i++)
    {
        LBID_t eLastLBID;

        // while scanning, grab the first free slot
        if (fFreeList[i].size == 0)
        {
            if (freeIndex == -1)
                freeIndex = i;

            continue;
        }

        eLastLBID = fFreeList[i].start + (((int64_t) fFreeList[i].size) * 1024) - 1;

        /* if it's at the front... */
        if (start == fFreeList[i].start)
        {
            /* if the request is larger than the freelist entry -> implies an extent
             * overlap.  This is debugging code. */
            //idbassert(size > fFreeList[i].size);
            makeUndoRecord(&fFreeList[i], sizeof(InlineLBIDRange));
            fFreeList[i].start += size * 1024;
            fFreeList[i].size -= size;

            if (fFreeList[i].size == 0)
            {
                makeUndoRecord(fFLShminfo, sizeof(MSTEntry));
                fFLShminfo->currentSize -= sizeof(InlineLBIDRange);
            }

            break;
        }
        /* if it's at the back... */
        else if (eLastLBID == lastLBID)
        {
            makeUndoRecord(&fFreeList[i], sizeof(InlineLBIDRange));
            fFreeList[i].size -= size;

            if (fFreeList[i].size == 0)
            {
                makeUndoRecord(fFLShminfo, sizeof(MSTEntry));
                fFLShminfo->currentSize -= sizeof(InlineLBIDRange);
            }

            break;
            /* This entry won't be the same size as the request or the first
             * clause would have run instead.
             */
        }
        /* if it's in the middle... */
        /* break it into two elements */
        else if (fFreeList[i].start < start && eLastLBID > lastLBID)
        {
            if (freeIndex == -1)
            {
                if (fFLShminfo->currentSize == fFLShminfo->allocdSize)
                {
                    growFLShmseg();
                    freeIndex = flEntries;
                }
                else
                    for (freeIndex = i + 1; freeIndex < flEntries; freeIndex++)
                        if (fFreeList[freeIndex].size == 0)
                            break;

#ifdef BRM_DEBUG
                idbassert(nextIndex < flEntries);
#endif
            }

            makeUndoRecord(&fFreeList[i], sizeof(InlineLBIDRange));
            makeUndoRecord(&fFreeList[freeIndex], sizeof(InlineLBIDRange));
            makeUndoRecord(fFLShminfo, sizeof(MSTEntry));
            fFreeList[i].size = (start - fFreeList[i].start) / 1024;
            fFreeList[freeIndex].start = start + (size * 1024);
            fFreeList[freeIndex].size = (eLastLBID - lastLBID) / 1024;
            fFLShminfo->currentSize += sizeof(InlineLBIDRange);
            break;
        }
    }
}

std::vector<EMEntry> ExtentMap::getEmIdentsByLbids(const std::vector<LBID_t>& lbids)
{
    std::vector<EMEntry> emEntries;
    for (auto lbid : lbids)
    {
        auto emIt = findByLBID(lbid);
        if (emIt == fExtentMapRBTree->end())
            throw logic_error("ExtentMap::markInvalid(): lbid isn't allocated");
        // Materialize.
        emEntries.push_back(emIt->second);
    }
    return emEntries;
}

std::vector<ExtentMapRBTree::iterator> ExtentMap::getEmIteratorsByLbids(const std::vector<LBID_t>& lbids)
{
    // ExtentMapRBTree::iterator
    std::vector<ExtentMapRBTree::iterator> emEntries;
    for (auto lbid : lbids)
    {
        auto emIt = findByLBID(lbid);
        if (emIt == fExtentMapRBTree->end())
            throw logic_error("ExtentMap::markInvalid(): lbid isn't allocated");
        // Materialize.
        emEntries.push_back(emIt);
    }
    return emEntries;
}

/*
	The file layout looks like this:

	  EM Magic (32-bits)
	  number of EM entries  (32-bits)
	  number of FL entries  (32-bits)
	  EMEntry
	    ...   (* numEM)
	  struct InlineLBIDRange
	    ...   (* numFL)
*/

void ExtentMap::loadVersion4(IDBDataFile* in)
{
    uint32_t emNumElements = 0;
    uint32_t flNumElements = 0;

    uint32_t nbytes = 0;
    nbytes += in->read((char*) &emNumElements, sizeof(uint32_t));
    nbytes += in->read((char*) &flNumElements, sizeof(uint32_t));
    idbassert(emNumElements > 0);

    if (nbytes != (2 * sizeof(uint32_t)))
    {
        log_errno("ExtentMap::loadVersion4(): read ");
        throw runtime_error("ExtentMap::loadVersion4(): read failed. Check the error log.");
    }

    // Clear the extent map.
    fExtentMapRBTree->clear();
    fEMRBTreeShminfo->currentSize = 0;

    // Init the free list.
    memset(fFreeList, 0, fFLShminfo->allocdSize);
    fFreeList[0].size = (1 << 26);   // 2^36 LBIDs
    fFLShminfo->currentSize = sizeof(InlineLBIDRange);

    // Calculate how much memory we need.
    const uint32_t memorySizeNeeded = (emNumElements * EM_RB_TREE_NODE_SIZE) + EM_RB_TREE_EMPTY_SIZE;
    constexpr const uint32_t freeShmemThreshold = EM_RB_TREE_INITIAL_SIZE >> 4;
    growEMShmseg(memorySizeNeeded);

    size_t progress = 0, writeSize = emNumElements * sizeof(EMEntry);
    int err;
    char *writePos;

    for (uint32_t i = 0; i < emNumElements; ++i)
    {
        progress = 0;
        EMEntry emEntry;
        writeSize = sizeof(EMEntry);
        writePos = reinterpret_cast<char*>(&emEntry);

        while (progress < writeSize)
        {
            err = in->read(writePos + progress, writeSize - progress);
            if (err <= 0)
            {
                log_errno("ExtentMap::loadVersion4(): read ");
                throw runtime_error(
                    "ExtentMap::loadVersion4(): read failed. Check the error log.");
            }
            progress += (uint) err;
        }
        if (fPExtMapRBTreeImpl->getFreeMemory() < freeShmemThreshold)
            growEMShmseg(EM_RB_TREE_INCREMENT);

        std::pair<int64_t, EMEntry> lbidEMEntryPair = make_pair(emEntry.range.start, emEntry);
        fExtentMapRBTree->insert(lbidEMEntryPair);
    }

    for (auto& lbidEMEntryPair : *fExtentMapRBTree)
    {
        EMEntry& emEntry = lbidEMEntryPair.second;
        reserveLBIDRange(emEntry.range.start, emEntry.range.size);

        //@bug 1911 - verify status value is valid
        if (emEntry.status < EXTENTSTATUSMIN || emEntry.status > EXTENTSTATUSMAX)
            emEntry.status = EXTENTAVAILABLE;

        auto resShmemHasGrownPair = fPExtMapIndexImpl_->insert(emEntry, emEntry.range.start);

        if (resShmemHasGrownPair.second)
            fEMIndexShminfo->allocdSize = fPExtMapIndexImpl_->getShmemSize();

        if (!resShmemHasGrownPair.first)
            logAndSetEMIndexReadOnly("loadVersion4");
    }

    fEMRBTreeShminfo->currentSize = (emNumElements * EM_RB_TREE_NODE_SIZE) + EM_RB_TREE_EMPTY_SIZE;

#ifdef DUMP_EXTENT_MAP
    cout << "lbid\tsz\toid\tfbo\thwm\tpart#\tseg#\tDBRoot\twid\tst\thi\tlo\tsq\tv" << endl;

    for (const auto& lbidEMEntryPair : *fExtentMapRBTRee)
    {
        const EMEntry& emEntry = lbidEMEntryPair.second;
        cout <<
             emEntry.start
             << '\t' << emEntry.size
             << '\t' << emEntry.fileID
             << '\t' << emEntry.blockOffset
             << '\t' << emEntry.HWM
             << '\t' << emEntry.partitionNum
             << '\t' << emEntry.segmentNum
             << '\t' << emEntry.dbRoot
             << '\t' << emEntry.status
             << '\t' << emEntry.partition.cprange.hiVal
             << '\t' << emEntry.partition.cprange.loVal
             << '\t' << emEntry.partition.cprange.sequenceNum
             << '\t' << (int)(emEntry.partition.cprange.isValid)
             << endl;
    }

    cout << "Free list entries:" << endl;
    cout << "start\tsize" << endl;

    for (uint32_t i = 0; i < flNumElements; i++)
        cout << fFreeList[i].start << '\t' << fFreeList[i].size << endl;

#endif
}

void ExtentMap::load(const string& filename, bool fixFL)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("load");
        TRACER_ADDSTRINPUT(filename);
        TRACER_WRITE;
    }

#endif

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);

    try
    {
        grabFreeList(WRITE);
    }
    catch (...)
    {
        releaseEMIndex(WRITE);
        releaseEMEntryTable(WRITE);
        throw;
    }

    const char* filename_p = filename.c_str();
    scoped_ptr<IDBDataFile>  in(IDBDataFile::open(
                                    IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
                                    filename_p, "r", 0));

    if (!in)
    {
        log_errno("ExtentMap::load(): open");

        releaseFreeList(WRITE);
        releaseEMIndex(WRITE);
        releaseEMEntryTable(WRITE);

        throw ios_base::failure("ExtentMap::load(): open failed. Check the error log.");
    }

    try
    {
        int emVersion = 0;
        int bytes = in->read((char*) &emVersion, sizeof(int));

        if (bytes == (int) sizeof(int) && emVersion == EM_MAGIC_V4)
            loadVersion4(in.get());
        else
        {
            log("ExtentMap::load(): That file is not a valid ExtentMap image");
            throw runtime_error("ExtentMap::load(): That file is not a valid ExtentMap image");
        }
    }
    catch (...)
    {
        releaseFreeList(WRITE);
        releaseEMIndex(WRITE);
        releaseEMEntryTable(WRITE);
        throw;
    }

    releaseFreeList(WRITE);
    releaseEMIndex(WRITE);
    releaseEMEntryTable(WRITE);
//	checkConsistency();
}

void ExtentMap::save(const string& filename)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("save");
        TRACER_ADDSTRINPUT(filename);
        TRACER_WRITE;
    }

#endif

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    try
    {
        grabFreeList(READ);
    }
    catch (...)
    {
        releaseEMIndex(READ);
        releaseEMEntryTable(READ);
        throw;
    }

    if (fEMRBTreeShminfo->currentSize == 0)
    {
        log("ExtentMap::save(): got request to save an empty BRM");
        releaseFreeList(READ);
        releaseEMIndex(READ);
        releaseEMEntryTable(READ);
        throw runtime_error("ExtentMap::save(): got request to save an empty BRM");
    }

    const char* filename_p = filename.c_str();
    scoped_ptr<IDBDataFile> out(IDBDataFile::open(
                                    IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
                                    filename_p, "wb", IDBDataFile::USE_VBUF));

    if (!out)
    {
        log_errno("ExtentMap::save(): open");
        releaseFreeList(READ);
        releaseEMIndex(READ);
        releaseEMEntryTable(READ);
        throw ios_base::failure("ExtentMap::save(): open failed. Check the error log.");
    }

    int loadSize[3];
    loadSize[0] = EM_MAGIC_V4;
    loadSize[1] = fExtentMapRBTree->size();
    loadSize[2] = fFLShminfo->allocdSize / sizeof(InlineLBIDRange); // needs to send all entries

    try
    {
        const int32_t wsize = 3 * sizeof(uint32_t);
        const int32_t bytes = out->write((char*) loadSize, wsize);

        if (bytes != wsize)
            throw ios_base::failure("ExtentMap::save(): write failed. Check the error log.");
    }
    catch (...)
    {
        releaseFreeList(READ);
        releaseEMIndex(READ);
        releaseEMEntryTable(READ);
        throw;
    }

    for (auto& lbidEMEntryPair : *fExtentMapRBTree)
    {
        EMEntry& emEntry = lbidEMEntryPair.second;
        const uint32_t writeSize = sizeof(EMEntry);
        char* writePos = reinterpret_cast<char*>(&emEntry);
        uint32_t progress = 0;

        while (progress < writeSize)
        {
            auto err = out->write(writePos + progress, writeSize - progress);
            if (err < 0)
            {
                releaseFreeList(READ);
                releaseEMIndex(READ);
                releaseEMEntryTable(READ);
                throw ios_base::failure("ExtentMap::save(): write failed. Check the error log.");
            }
            progress += err;
        }
    }

    uint32_t progress = 0;
    const uint32_t writeSize = fFLShminfo->allocdSize;
    char* writePos = reinterpret_cast<char*>(fFreeList);
    while (progress < writeSize)
    {
        auto err = out->write(writePos + progress, writeSize - progress);
        if (err < 0)
        {
            releaseFreeList(READ);
            releaseEMIndex(READ);
            releaseEMEntryTable(READ);
            throw ios_base::failure("ExtentMap::save(): write failed. Check the error log.");
        }
        progress += err;
    }

    releaseFreeList(READ);
    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
}

void ExtentMap::grabEMEntryTable(OPS op)
{
    boost::mutex::scoped_lock lk(mutex);

    if (op == READ)
        fEMRBTreeShminfo = fMST.getTable_read(MasterSegmentTable::EMTable);
    else
    {
        fEMRBTreeShminfo = fMST.getTable_write(MasterSegmentTable::EMTable);
        emLocked = true;
    }

    if (!fPExtMapRBTreeImpl || fPExtMapRBTreeImpl->key() != (uint32_t) fEMRBTreeShminfo->tableShmkey)
    {
        if (fEMRBTreeShminfo->allocdSize == 0)
        {
            if (op == READ)
            {
                fMST.getTable_upgrade(MasterSegmentTable::EMTable);
                emLocked = true;

                if (fEMRBTreeShminfo->allocdSize == 0)
                    growEMShmseg();

                // Has to be done holding the write lock.
                emLocked = false;
                fMST.getTable_downgrade(MasterSegmentTable::EMTable);
            }
            else
            {
                growEMShmseg();
            }
        }
        else
        {
            fPExtMapRBTreeImpl =
                ExtentMapRBTreeImpl::makeExtentMapRBTreeImpl(fEMRBTreeShminfo->tableShmkey, 0);

            ASSERT(fPExtMapRBTreeImpl);

            fExtentMapRBTree = fPExtMapRBTreeImpl->get();
            if (fExtentMapRBTree == nullptr)
            {
                log_errno("ExtentMap cannot create RBTree in shared memory segment");
                throw runtime_error("ExtentMap cannot create RBTree in shared memory segment");
            }
        }
    }
    else
    {
        fExtentMapRBTree = fPExtMapRBTreeImpl->get();
    }
}

/* always returns holding the FL lock */
void ExtentMap::grabFreeList(OPS op)
{
    boost::mutex::scoped_lock lk(mutex, boost::defer_lock);

    if (op == READ)
    {
        fFLShminfo = fMST.getTable_read(MasterSegmentTable::EMFreeList);
        lk.lock();
    }
    else
    {
        fFLShminfo = fMST.getTable_write(MasterSegmentTable::EMFreeList);
        flLocked = true;
    }

    if (!fPFreeListImpl || fPFreeListImpl->key() != (unsigned)fFLShminfo->tableShmkey)
    {
        if (fFreeList != nullptr)
        {
            fFreeList = nullptr;
        }

        if (fFLShminfo->allocdSize == 0)
        {
            if (op == READ)
            {
                lk.unlock();
                fMST.getTable_upgrade(MasterSegmentTable::EMFreeList);
                flLocked = true;

                if (fFLShminfo->allocdSize == 0)
                    growFLShmseg();

                flLocked = false;		// has to be done holding the write lock
                fMST.getTable_downgrade(MasterSegmentTable::EMFreeList);
            }
            else
                growFLShmseg();
        }
        else
        {
            fPFreeListImpl = FreeListImpl::makeFreeListImpl(fFLShminfo->tableShmkey, 0);
            ASSERT(fPFreeListImpl);

            if (r_only)
                fPFreeListImpl->makeReadOnly();

            fFreeList = fPFreeListImpl->get();

            if (fFreeList == nullptr)
            {
                log_errno("ExtentMap::grabFreeList(): shmat");
                throw runtime_error("ExtentMap::grabFreeList(): shmat failed.  Check the error log.");
            }

            if (op == READ)
                lk.unlock();
        }
    }
    else
    {
        fFreeList = fPFreeListImpl->get();

        if (op == READ)
            lk.unlock();
    }
}

void ExtentMap::grabEMIndex(OPS op)
{
    boost::mutex::scoped_lock lk(emIndexMutex);

    if (op == READ)
    {
        fEMIndexShminfo = fMST.getTable_read(MasterSegmentTable::EMIndex);
    }
    else
    {
        fEMIndexShminfo = fMST.getTable_write(MasterSegmentTable::EMIndex);
        emIndexLocked = true;
    }

    if (!fPExtMapIndexImpl_)
    {
        if (fEMIndexShminfo->allocdSize == 0)
        {
            if (op == READ)
            {
                fMST.getTable_upgrade(MasterSegmentTable::EMIndex);
                emIndexLocked = true;

                // Checking race conditions
                if (fEMIndexShminfo->allocdSize == 0)
                    growEMIndexShmseg();

                emIndexLocked = false;
                fMST.getTable_downgrade(MasterSegmentTable::EMIndex);
            }
            else
            {
                growEMIndexShmseg();
            }
        }
        else
        {
            // Sending down current Managed Shmem size. If EMIndexImpl instance size doesn't match
            // fEMIndexShminfo->allocdSize makeExtentMapIndexImpl will remap managed shmem segment.
            fPExtMapIndexImpl_ =
                ExtentMapIndexImpl::makeExtentMapIndexImpl(getInitialEMIndexShmkey(), fEMIndexShminfo->allocdSize);

            if (r_only)
                fPExtMapIndexImpl_->makeReadOnly();
        }
    }
    else if (fPExtMapIndexImpl_->getShmemImplSize() != (unsigned)fEMIndexShminfo->allocdSize)
    {
        fPExtMapIndexImpl_->refreshShm();
        fPExtMapIndexImpl_ =
            ExtentMapIndexImpl::makeExtentMapIndexImpl(getInitialEMIndexShmkey(), fEMIndexShminfo->allocdSize);
    }
}

void ExtentMap::releaseEMEntryTable(OPS op)
{
    if (op == READ)
        fMST.releaseTable_read(MasterSegmentTable::EMTable);
    else
    {
      // Note: Technically we should mark it unlocked after it's unlocked,
      // however, that's a race condition. The only reason the up operation
      // here will fail is if the underlying semaphore doesn't exist anymore
      // or there is a locking logic error somewhere else.  Either way,
      // declaring the EM unlocked here is OK. Same with all similar assignments.
      emLocked = false;
      fMST.releaseTable_write(MasterSegmentTable::EMTable);
    }
}

void ExtentMap::releaseFreeList(OPS op)
{
    if (op == READ)
        fMST.releaseTable_read(MasterSegmentTable::EMFreeList);
    else
    {
        flLocked = false;
        fMST.releaseTable_write(MasterSegmentTable::EMFreeList);
    }
}

void ExtentMap::releaseEMIndex(OPS op)
{
    if (op == READ)
    {
        fMST.releaseTable_read(MasterSegmentTable::EMIndex);
    }
    else
    {
        emIndexLocked = false;
        fMST.releaseTable_write(MasterSegmentTable::EMIndex);
    }
}

key_t ExtentMap::chooseEMShmkey()
{
  return chooseShmkey(fEMRBTreeShminfo, fShmKeys.KEYRANGE_EXTENTMAP_BASE);
}

key_t ExtentMap::chooseFLShmkey()
{
    return chooseShmkey(fFLShminfo, fShmKeys.KEYRANGE_EMFREELIST_BASE);
}

// The key values is fixed b/c MCS doesn't need to increase a segment id number
key_t ExtentMap::chooseEMIndexShmkey()
{
    return chooseShmkey(fEMIndexShminfo, fShmKeys.KEYRANGE_EXTENTMAP_INDEX_BASE);
}

key_t ExtentMap::getInitialEMIndexShmkey() const
{
    return fShmKeys.KEYRANGE_EXTENTMAP_INDEX_BASE + 1;
}

key_t ExtentMap::chooseShmkey(const MSTEntry* masterTableEntry, const uint32_t keyRangeBase) const
{
    int fixedKeys = 1;

    if (masterTableEntry->tableShmkey + 1 == (key_t) (keyRangeBase + fShmKeys.KEYRANGE_SIZE - 1) ||
        (unsigned)masterTableEntry->tableShmkey < keyRangeBase)
    {
        return keyRangeBase + fixedKeys;
    }
    return masterTableEntry->tableShmkey + 1;
}


/* Must be called holding the EM write lock
   Returns with the new shmseg mapped */
void ExtentMap::growEMShmseg(size_t size)
{
    size_t allocSize;
    auto newShmKey = chooseEMShmkey();

    if (fEMRBTreeShminfo->allocdSize == 0)
        allocSize = EM_RB_TREE_INITIAL_SIZE;
    else
        allocSize = EM_RB_TREE_INCREMENT;

    allocSize = std::max(size, allocSize);

    ASSERT((allocSize == EM_RB_TREE_INITIAL_SIZE && !fPExtMapRBTreeImpl) || fPExtMapRBTreeImpl);

    if (!fPExtMapRBTreeImpl)
    {
        if (fEMRBTreeShminfo->tableShmkey == 0)
            fEMRBTreeShminfo->tableShmkey = newShmKey;

        fPExtMapRBTreeImpl = ExtentMapRBTreeImpl::makeExtentMapRBTreeImpl(
            fEMRBTreeShminfo->tableShmkey, allocSize, r_only);
    }
    else
    {
        fEMRBTreeShminfo->tableShmkey = newShmKey;
        fPExtMapRBTreeImpl->grow(fEMRBTreeShminfo->tableShmkey, allocSize);
    }

    fEMRBTreeShminfo->allocdSize += allocSize;

    fExtentMapRBTree = fPExtMapRBTreeImpl->get();

    // That's mean we have a initial size.
    if (fEMRBTreeShminfo->currentSize == 0)
        fEMRBTreeShminfo->currentSize = EM_RB_TREE_EMPTY_SIZE;
}

void ExtentMap::growIfNeededOnExtentCreate()
{
    const auto freeMem = fPExtMapRBTreeImpl->getFreeMemory();
    const auto memNeeded = EM_RB_TREE_NODE_SIZE * 10;
    if (freeMem <= memNeeded)
    {
        // The accurate size is memNeeded - freeMem.
        growEMShmseg(memNeeded);
    }
}

void ExtentMap::growEMIndexShmseg(const size_t suggestedSize)
{
    static const constexpr int InitEMIndexSize_ = 16 * 1024 * 1024;
    size_t allocSize = std::max(InitEMIndexSize_, fEMIndexShminfo->allocdSize);
    key_t newshmkey = chooseEMIndexShmkey();
    key_t fixedManagedSegmentKey = getInitialEMIndexShmkey();

    allocSize = std::max(allocSize, suggestedSize);
    if (!fPExtMapIndexImpl_)
    {
        fPExtMapIndexImpl_ = ExtentMapIndexImpl::makeExtentMapIndexImpl(fixedManagedSegmentKey, allocSize, r_only);
    }
    else
    {
        fPExtMapIndexImpl_->growIfNeeded(allocSize);
    }

   if (r_only)
        fPExtMapIndexImpl_->makeReadOnly();

    fEMIndexShminfo->tableShmkey = newshmkey;
    fEMIndexShminfo->allocdSize = allocSize;
}

/* Must be called holding the FL lock
   Returns with the new shmseg mapped */
void ExtentMap::growFLShmseg()
{
    size_t allocSize;
    key_t newshmkey;

    if (fFLShminfo->allocdSize == 0)
        allocSize = EM_FREELIST_INITIAL_SIZE;
    else
        allocSize = fFLShminfo->allocdSize + EM_FREELIST_INCREMENT;

    newshmkey = chooseFLShmkey();
    ASSERT((allocSize == EM_FREELIST_INITIAL_SIZE && !fPFreeListImpl) || fPFreeListImpl);

    if (!fPFreeListImpl)
        fPFreeListImpl = FreeListImpl::makeFreeListImpl(newshmkey, allocSize, false);
    else
        fPFreeListImpl->grow(newshmkey, allocSize);

    fFLShminfo->tableShmkey = newshmkey;
    fFreeList = fPFreeListImpl->get();

    // init freelist entry
    if (fFLShminfo->allocdSize == 0)
    {
        fFreeList->size = (1ULL << 36) / 1024;
        fFLShminfo->currentSize = sizeof(InlineLBIDRange);
    }

    fFLShminfo->allocdSize = allocSize;

    if (r_only)
        fPFreeListImpl->makeReadOnly();

    fFreeList = fPFreeListImpl->get();
}

// @bug 1509.  Added new version of lookup that returns the first and last lbid for the extent that contains the
// given lbid.
int ExtentMap::lookup(LBID_t lbid, LBID_t& firstLbid, LBID_t& lastLbid)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("lookup");
        TRACER_ADDINPUT(lbid);
        TRACER_ADDOUTPUT(firstLbid);
        TRACER_ADDOUTPUT(lastLbid);
        TRACER_WRITE;
    }

#endif

#ifdef BRM_DEBUG

    if (lbid < 0)
    {
        log("ExtentMap::lookup(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
        cout << "ExtentMap::lookup(): lbid must be >= 0.  Lbid passed was " << lbid << endl;
        throw invalid_argument("ExtentMap::lookup(): lbid must be >= 0");
    }

#endif

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    auto emIt = findByLBID(lbid);
    if (emIt == fExtentMapRBTree->end())
        return -1;

    auto& emEntry = emIt->second;
    LBID_t lastBlock = emEntry.range.start + (static_cast<LBID_t>(emEntry.range.size) * 1024) - 1;
    firstLbid = emEntry.range.start;
    lastLbid = lastBlock;

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
    return 0;
}

// @bug 1055+.  New functions added for multiple files per OID enhancement.
int ExtentMap::lookupLocal(LBID_t lbid, int& OID, uint16_t& dbRoot, uint32_t& partitionNum,
                           uint16_t& segmentNum, uint32_t& fileBlockOffset)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("lookupLocal");
        TRACER_ADDINPUT(lbid);
        TRACER_ADDOUTPUT(OID);
        TRACER_ADDSHORTOUTPUT(dbRoot);
        TRACER_ADDOUTPUT(partitionNum);
        TRACER_ADDSHORTOUTPUT(segmentNum);
        TRACER_ADDOUTPUT(fileBlockOffset);
        TRACER_WRITE;
    }

#endif
#ifdef EM_AS_A_TABLE_POC__

    if (lbid >= (1LL << 54))
    {
        OID = 1084;
        dbRoot = 1;
        partitionNum = 0;
        segmentNum = 0;
        fileBlockOffset = 0;
        return 0;
    }

#endif
    if (lbid < 0)
    {
        ostringstream oss;
        oss << "ExtentMap::lookupLocal(): invalid lbid requested: " << lbid;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    auto emIt = findByLBID(lbid);
    if (emIt == fExtentMapRBTree->end())
        return -1;

    auto& emEntry = emIt->second;
    OID = emEntry.fileID;
    dbRoot = emEntry.dbRoot;
    segmentNum = emEntry.segmentNum;
    partitionNum = emEntry.partitionNum;

    // TODO:  Offset logic.
    auto offset = lbid - emEntry.range.start;
    fileBlockOffset = emEntry.blockOffset + offset;

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
    return 0;
}

int ExtentMap::lookupLocal(int OID, uint32_t partitionNum, uint16_t segmentNum, uint32_t fileBlockOffset, LBID_t& LBID)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("lookupLocal");
        TRACER_ADDINPUT(OID);
        TRACER_ADDINPUT(partitionNum);
        TRACER_ADDSHORTINPUT(segmentNum);
        TRACER_ADDINPUT(fileBlockOffset);
        TRACER_ADDOUTPUT(LBID);
        TRACER_WRITE;
    }

#endif
    int offset;

    if (OID < 0)
    {
        log("ExtentMap::lookup(): OID and FBO must be >= 0", logging::LOG_TYPE_DEBUG);
        throw invalid_argument("ExtentMap::lookup(): OID and FBO must be >= 0");
    }

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    DBRootVec dbRootVec(getAllDbRoots());

    for (auto dbRoot: dbRootVec)
    {
        const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID, partitionNum);
        const auto emIdents = getEmIdentsByLbids(lbids);
        for (auto& emEntry : emIdents)
        {
            // TODO:  Blockoffset logic.
            if (emEntry.range.size != 0 && emEntry.segmentNum == segmentNum &&
                emEntry.blockOffset <= fileBlockOffset &&
                fileBlockOffset <=
                    (emEntry.blockOffset + (static_cast<LBID_t>(emEntry.range.size) * 1024) - 1))
            {

                offset = fileBlockOffset - emEntry.blockOffset;
                LBID = emEntry.range.start + offset;
                releaseEMIndex(READ);
                releaseEMEntryTable(READ);
                return 0;
            }
        }
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
    return -1;
}

int ExtentMap::lookupLocal_DBroot(int OID, uint16_t dbroot, uint32_t partitionNum, uint16_t segmentNum,
                                  uint32_t fileBlockOffset, LBID_t& LBID)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("lookupLocal");
        TRACER_ADDINPUT(OID);
        TRACER_ADDINPUT(partitionNum);
        TRACER_ADDSHORTINPUT(segmentNum);
        TRACER_ADDINPUT(fileBlockOffset);
        TRACER_ADDOUTPUT(LBID);
        TRACER_WRITE;
    }

#endif

    if (OID < 0)
    {
        log("ExtentMap::lookup(): OID and FBO must be >= 0", logging::LOG_TYPE_DEBUG);
        throw invalid_argument("ExtentMap::lookup(): OID and FBO must be >= 0");
    }

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    const auto lbids = fPExtMapIndexImpl_->find(dbroot, OID, partitionNum);
    const auto emIdents = getEmIdentsByLbids(lbids);
    for (auto& emEntry : emIdents)
    {
        if (emEntry.segmentNum == segmentNum &&
            emEntry.blockOffset <= fileBlockOffset &&
            fileBlockOffset <= (emEntry.blockOffset + (static_cast<LBID_t>(emEntry.range.size) * 1024) - 1))
        {
            auto offset = fileBlockOffset - emEntry.blockOffset;
            LBID = emEntry.range.start + offset;
            releaseEMIndex(READ);
            releaseEMEntryTable(READ);
            return 0;
        }
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
    return -1;
}

// @bug 1055-.

//------------------------------------------------------------------------------
// Lookup/return starting LBID for the specified OID, partition, segment, and
// file block offset.
//------------------------------------------------------------------------------
int ExtentMap::lookupLocalStartLbid(int OID, uint32_t partitionNum, uint16_t segmentNum,
                                    uint32_t fileBlockOffset, LBID_t& LBID)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("lookupLocalStartLbid");
        TRACER_ADDINPUT(OID);
        TRACER_ADDINPUT(partitionNum);
        TRACER_ADDSHORTINPUT(segmentNum);
        TRACER_ADDINPUT(fileBlockOffset);
        TRACER_ADDOUTPUT(LBID);
        TRACER_WRITE;
    }

#endif

    if (OID < 0)
    {
        log("ExtentMap::lookupLocalStartLbid(): OID and FBO must be >= 0",
            logging::LOG_TYPE_DEBUG);
        throw invalid_argument("ExtentMap::lookupLocalStartLbid(): "
                               "OID and FBO must be >= 0");
    }

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    DBRootVec dbRootVec(getAllDbRoots());

    for (auto dbRoot: dbRootVec)
    {
        const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID, partitionNum);
        const auto emIdents = getEmIdentsByLbids(lbids);
        for (auto& emEntry : emIdents)
        {
            if (emEntry.range.size != 0 && emEntry.segmentNum == segmentNum &&
                emEntry.blockOffset <= fileBlockOffset &&
                fileBlockOffset <=
                    (emEntry.blockOffset + (static_cast<LBID_t>(emEntry.range.size) * 1024) - 1))
            {
                LBID = emEntry.range.start;
                releaseEMIndex(READ);
                releaseEMEntryTable(READ);
                return 0;
            }
        }
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);

    return -1;
}

//------------------------------------------------------------------------------
// Creates a "stripe" of column extents across a table, for the specified
// columns and DBRoot.
//   cols         - Vector of columns OIDs and widths to be allocated
//   dbRoot       - DBRoot to be used for new extents
//   partitionNum - when creating the first extent for a column (on dbRoot),
//                  partitionNum must be specified as an input argument.
//                  If not the first extent on dbRoot, then partitionNum
//                  for the new extents will be assigned and returned, based
//                  on the current last extent for dbRoot.
// output:
//   partitionNum - Partition number for new extents
//   segmentNum   - Segment number for new exents
//   extents      - starting Lbid, numBlocks, and FBO for new extents
//------------------------------------------------------------------------------
void ExtentMap::createStripeColumnExtents(const vector<CreateStripeColumnExtentsArgIn>& cols, uint16_t dbRoot,
                                          uint32_t& partitionNum, uint16_t& segmentNum,
                                          vector<CreateStripeColumnExtentsArgOut>& extents)
{
    LBID_t    startLbid;
    int       allocSize;
    uint32_t startBlkOffset;

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
    grabFreeList(WRITE);

    OID_t     baselineOID = -1;
    uint16_t baselineSegmentNum = -1;
    uint32_t baselinePartNum = -1;

    for (uint32_t i = 0; i < cols.size(); i++)
    {
        createColumnExtent_DBroot(cols[i].oid, cols[i].width, dbRoot, cols[i].colDataType, partitionNum,
                                  segmentNum, startLbid, allocSize, startBlkOffset, false);

        if (i == 0)
        {
            baselineOID = cols[i].oid;
            baselineSegmentNum = segmentNum;
            baselinePartNum = partitionNum;
        }
        else
        {
            if ((segmentNum != baselineSegmentNum) || (partitionNum != baselinePartNum))
            {
                ostringstream oss;
                oss << "ExtentMap::createStripeColumnExtents(): "
                       "Inconsistent segment extent creation: "
                    << "DBRoot: " << dbRoot << "OID1: " << baselineOID << "; Part#: " << baselinePartNum
                    << "; Seg#: " << baselineSegmentNum << " <versus> OID2: " << cols[i].oid
                    << "; Part#: " << partitionNum << "; Seg#: " << segmentNum;
                log(oss.str(), logging::LOG_TYPE_CRITICAL);
                throw invalid_argument(oss.str());
            }
        }

        CreateStripeColumnExtentsArgOut extentInfo;
        extentInfo.startLbid = startLbid;
        extentInfo.allocSize      = allocSize;
        extentInfo.startBlkOffset = startBlkOffset;
        extents.push_back( extentInfo );
    }
}

//------------------------------------------------------------------------------
// Creates an extent for a column file on the specified DBRoot.  This is the
// external API function referenced by the dbrm wrapper class.
// required input:
//   OID          - column OID for which the extent is to be created
//   colWidth     - width of column in bytes
//   dbRoot       - DBRoot where extent is to be added
//   partitionNum - when creating the first extent for a column (on dbRoot),
//                  partitionNum must be specified as an input argument.
//                  If not the first extent on dbRoot, then partitionNum
//                  for the new extent will be assigned and returned, based
//                  on the current last extent for dbRoot.
//   useLock      - Grab ExtentMap and FreeList WRITE lock to perform work
// output:
//   partitionNum - partition number for the new extent
//   segmentNum   - segment number for the new extent
//   lbid         - starting LBID of the created extent
//   allocdsize   - number of LBIDs allocated
//   startBlockOffset-starting block of the created extent
//------------------------------------------------------------------------------
void ExtentMap::createColumnExtent_DBroot(int OID, uint32_t colWidth, uint16_t dbRoot,
                                          execplan::CalpontSystemCatalog::ColDataType colDataType,
                                          uint32_t& partitionNum, uint16_t& segmentNum, LBID_t& lbid,
                                          int& allocdsize, uint32_t& startBlockOffset,
                                          bool useLock) // defaults to true
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("createColumnExtent_DBroot");
        TRACER_ADDINPUT(OID);
        TRACER_ADDINPUT(colWidth);
        TRACER_ADDSHORTINPUT(dbRoot);
        TRACER_ADDOUTPUT(partitionNum);
        TRACER_ADDSHORTOUTPUT(segmentNum);
        TRACER_ADDINT64OUTPUT(lbid);
        TRACER_ADDOUTPUT(allocdsize);
        TRACER_ADDOUTPUT(startBlockOffset);
        TRACER_WRITE;
    }

#endif

#ifdef BRM_DEBUG

    if (OID <= 0)
    {
        log("ExtentMap::createColumnExtent_DBroot(): OID must be > 0",
            logging::LOG_TYPE_DEBUG);
        throw invalid_argument(
            "ExtentMap::createColumnExtent_DBroot(): OID must be > 0");
    }

#endif

    // Convert extent size in rows to extent size in 8192-byte blocks.
    // extentRows should be multiple of blocksize (8192).
    const unsigned EXTENT_SIZE = (getExtentRows() * colWidth) / BLOCK_SIZE;

    if (useLock)
    {
        grabEMEntryTable(WRITE);
        grabEMIndex(WRITE);
        grabFreeList(WRITE);
    }

    growIfNeededOnExtentCreate();

    //  size is the number of multiples of 1024 blocks.
    //  ex: size=1 --> 1024 blocks
    //      size=2 --> 2048 blocks
    //      size=3 --> 3072 blocks, etc.
    uint32_t size = EXTENT_SIZE / 1024;

    lbid = _createColumnExtent_DBroot(size, OID, colWidth, dbRoot, colDataType, partitionNum, segmentNum,
                                      startBlockOffset);

    allocdsize = EXTENT_SIZE;
}

//------------------------------------------------------------------------------
// Creates an extent for a column file for the specified DBRoot.  This is the
// internal implementation function.
// input:
//   size         - number of multiples of 1024 blocks allocated to the extent
//                  ex: size=1 --> 1024 blocks
//                      size=2 --> 2048 blocks
//                      size=3 --> 3072 blocks, etc.
//   OID          - column OID for which the extent is to be created
//   colWidth     - width of column in bytes
//   dbRoot       - dbRoot where extent is to be added
//   partitionNum - when creating the first extent for an empty dbRoot,
//                  partitionNum must be specified as an input argument.
// output:
//   partitionNum - when adding an extent to a dbRoot,
//                  partitionNum will be the assigned partition number
//   segmentNum   - segment number for the new extent
//   startBlockOffset-starting block of the created extent
// returns starting LBID of the created extent.
//------------------------------------------------------------------------------
LBID_t ExtentMap::_createColumnExtent_DBroot(uint32_t size, int OID, uint32_t colWidth, uint16_t dbRoot,
                                             execplan::CalpontSystemCatalog::ColDataType colDataType,
                                             uint32_t& partitionNum, uint16_t& segmentNum,
                                             uint32_t& startBlockOffset)
{
    uint32_t highestOffset = 0;
    uint32_t highestPartNum = 0;
    uint16_t highestSegNum = 0;
    const unsigned FILES_PER_COL_PART = getFilesPerColumnPartition();
    const unsigned EXTENT_ROWS = getExtentRows();
    const unsigned EXTENTS_PER_SEGFILE = getExtentsPerSegmentFile();
    const unsigned DBROOT_COUNT = getDbRootCount();

    // Variables that track list of segfiles in target (HWM) DBRoot & partition.
    // Map segment number to the highest fbo extent in each file
    using TargetDbRootSegsMap = tr1::unordered_map<uint16_t, uint32_t>;
    using TargetDbRootSegsMapIter = TargetDbRootSegsMap::iterator;
    using TargetDbRootSegsMapConstIter = TargetDbRootSegsMap::const_iterator;
    TargetDbRootSegsMap targetDbRootSegs;

    uint32_t highEmptySegNum = 0; // high seg num for user specified partition;
    // only comes into play for empty DBRoot.
    bool bHighEmptySegNumSet = false;

    //--------------------------------------------------------------------------
    // First Step: Scan ExtentMap
    // 1. find HWM extent in relevant DBRoot
    // 2. if DBRoot is empty, track highest seg num in user specified partition
    // 3. Find first unused extent map entry
    //--------------------------------------------------------------------------

    LBID_t startLBID = getLBIDsFromFreeList(size);
    EMEntry* lastExtent = nullptr;

    const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID);
    auto emIdents = getEmIdentsByLbids(lbids);
    for (auto& emEntry : emIdents)
    {
      // 1. Find HWM extent in relevant DBRoot
      if ((emEntry.partitionNum > highestPartNum) ||
          ((emEntry.partitionNum == highestPartNum) && (emEntry.blockOffset > highestOffset)) ||
          ((emEntry.partitionNum == highestPartNum) && (emEntry.blockOffset == highestOffset) &&
           (emEntry.segmentNum >= highestSegNum)))
      {
        lastExtent = &emEntry;
        highestPartNum = emEntry.partitionNum;
        highestSegNum = emEntry.segmentNum;
        highestOffset = emEntry.blockOffset;
      }  // found extentmap entry for specified OID
    } // Loop through extent map entries

    DBRootVec dbRootVec(getAllDbRoots());

    // 2. for empty DBRoot track hi seg# in user specified part#
    if (lastExtent == nullptr)
    {
      // loop over all extents that doesn't belong to the target dbroot
      for (auto dbRootFromList : dbRootVec)
      {
        if (dbRootFromList == dbRoot)
          continue;

        const auto lbidsLocal = fPExtMapIndexImpl_->find(dbRootFromList, OID, partitionNum);
        auto emIdentsLocal = getEmIdentsByLbids(lbidsLocal);
        for (auto& emEntry : emIdentsLocal)
        {
          if ((emEntry.segmentNum > highEmptySegNum) || (!bHighEmptySegNumSet))
          {
            highEmptySegNum = emEntry.segmentNum;
            bHighEmptySegNumSet = true;
          }
        }
      }
    }

    //--------------------------------------------------------------------------
    // If DBRoot is not empty, then...
    // Second Step: Scan ExtentMap again after I know the last partition
    // 4. track highest seg num for HWM+1 partition
    // 5. track highest seg num for HWM    partition
    // 6. save list of segment numbers and fbos in target DBRoot and partition
    //
    // Scanning the extentmap a second time is not a good thing to be doing.
    // But the alternative isn't good either.  There is certain information
    // I need to capture about the last partition and DBRoot, and for the next
    // partition as well (which may contain segment files on other DBRoots),
    // but until I scan the extentmap, I don't know what my last partition is.
    // If I try to do this in a single scan, then I am forced to spend time
    // capturing information about partitions that turn out to be inconse-
    // quential because the "known" last partition will keep changing as I
    // scan the extentmap.
    //--------------------------------------------------------------------------
    bool bSegsOutOfService = false;
    int partHighSeg = -1;     // hi seg num for last partition
    int partHighSegNext = -1; // hi seg num for next partition

    if (lastExtent)
    {
      uint32_t targetDbRootPart = lastExtent->partitionNum;
      uint32_t targetDbRootPartNext = targetDbRootPart + 1;
      partHighSeg = lastExtent->segmentNum;
      targetDbRootSegs.insert(
          TargetDbRootSegsMap::value_type(lastExtent->segmentNum, lastExtent->blockOffset));

      for (auto dbRootFromList : dbRootVec)
      {
        if (dbRootFromList == dbRoot)
        {
          const auto lbids = fPExtMapIndexImpl_->find(dbRootFromList, OID, targetDbRootPart);
          const auto emIdents = getEmIdentsByLbids(lbids);
          for (const auto& emEntry : emIdents)
          {
            // 5. Track hi seg for hwm partition
            if (emEntry.segmentNum > partHighSeg)
            {
              partHighSeg = emEntry.segmentNum;
            }

            // 6. Save list of seg files in target DBRoot/Partition,
            //    along with the highest fbo for each seg file
            if (emEntry.status == EXTENTOUTOFSERVICE)
              bSegsOutOfService = true;

            TargetDbRootSegsMapIter iter = targetDbRootSegs.find(emEntry.segmentNum);

            if (iter == targetDbRootSegs.end())
            {
              targetDbRootSegs.insert(
                  TargetDbRootSegsMap::value_type(emEntry.segmentNum, emEntry.blockOffset));
            }
            else
            {
              if (emEntry.blockOffset > iter->second)
              {
                iter->second = emEntry.blockOffset;
              }
            }
          }  // loop over em idents
        }    // current dbroot == target dbroot
        else
        {
          // 4. Track hi seg for hwm+1 partition
          const auto lbidsNext = fPExtMapIndexImpl_->find(dbRootFromList, OID, targetDbRootPartNext);
          const auto emIdentsNext = getEmIdentsByLbids(lbidsNext);
          for (const auto& emEntry : emIdentsNext)
          {
            if (emEntry.segmentNum > partHighSegNext)
              partHighSegNext = emEntry.segmentNum;
          }

          // 5. Track hi seg for hwm partition
          const auto lbids = fPExtMapIndexImpl_->find(dbRootFromList, OID, targetDbRootPart);
          const auto emIdents = getEmIdentsByLbids(lbids);
          for (const auto& emEntry : emIdents)
          {
            if (emEntry.segmentNum > partHighSeg)
              partHighSeg = emEntry.segmentNum;
          }
        }  // current dbroot != target dbroot
      }    // loop over dbroots
    }  // (lastExtent != nullptr)

    //--------------------------------------------------------------------------
    // Third Step: Select partition and segment number for new extent
    // 1. Loop through targetDbRootSegs to find segment file for next extent
    // 2. Check for exceptions that warrant going to next physical partition
    //    a. See if any extents are marked outOfService
    //    b. See if extents are not evenly layered as expected
    // 3. Perform additional new partition/segment logic as applicable
    //    a. No action taken if 2a or 2b already detected need for new partition
    //    b. If HWM extent is in last file of DBRoot/Partition, see if next
    //       extent goes in new partition, or if wrap-around within current
    //       partition.
    //    c. If extent needs to go in next partition, figure out the next
    //       partition and the next available segment in that partition.
    // 4. Set blockOffset of new extent based on where extent is being added
    //--------------------------------------------------------------------------
    uint16_t newDbRoot = dbRoot;
    uint32_t newPartitionNum = partitionNum;
    uint16_t newSegmentNum = 0;
    uint32_t newBlockOffset = 0;

    // If this is not the first extent for this OID and DBRoot then
    //   extrapolate part# and seg# from last extent; wrap around segment and
    //   partition number as needed.
    // else
    //   use part# that the user specifies
    if (lastExtent)
    {
        bool startNewPartition = false;
        bool startNewStripeInSegFile = false;
        const unsigned int filesPerDBRootPerPartition = FILES_PER_COL_PART / DBROOT_COUNT;

        // Find first, last, next seg files in target partition and DBRoot
        uint16_t firstTargetSeg = lastExtent->segmentNum;
        uint16_t lastTargetSeg = lastExtent->segmentNum;
        uint16_t nextTargetSeg = lastExtent->segmentNum;

        // 1. Loop thru targetDbRootSegs[] to find next segment after
        //    lastExtIdx in target list.
        //    We save low and high segment to use in wrap-around case.
        if (targetDbRootSegs.size() > 1)
        {
            bool bNextSegSet = false;

            for (TargetDbRootSegsMapConstIter iter = targetDbRootSegs.begin();
                 iter != targetDbRootSegs.end(); ++iter)
            {
                uint16_t targetSeg = iter->first;

                if (targetSeg < firstTargetSeg)
                    firstTargetSeg = targetSeg;
                else if (targetSeg > lastTargetSeg)
                    lastTargetSeg = targetSeg;

                if (targetSeg > lastExtent->segmentNum)
                {
                    if ((targetSeg < nextTargetSeg) || (!bNextSegSet))
                    {
                        nextTargetSeg = targetSeg;
                        bNextSegSet = true;
                    }
                }
            }
        }

        newPartitionNum = lastExtent->partitionNum;

        // 2a. Skip to next physical partition if any extents in HWM partition/
        //     DBRoot are marked as outOfService
        if (bSegsOutOfService)
        {

            //			cout << "Skipping to next partition (outOfService segs)" <<
            //				": oid-"  << fExtentMap[lastExtentIndex].fileID <<
            //				"; root-" << fExtentMap[lastExtentIndex].dbRoot <<
            //				"; part-" << fExtentMap[lastExtentIndex].partitionNum <<
            //endl;

            startNewPartition = true;
        }

        // @bug 4765
        // 2b. Skip to next physical partition if we have a set of
        // segment files that are not "layered" as expected, meaning we
        // have > 1 layer of extents with an incomplete lower layer (could
        // be caused by the dropping of logical partitions).
        else if (targetDbRootSegs.size() < filesPerDBRootPerPartition)
        {
            for (TargetDbRootSegsMapConstIter iter = targetDbRootSegs.begin();
                 iter != targetDbRootSegs.end(); ++iter)
            {
                if (iter->second > 0)
                {

                    //					cout << "Skipping to next partition (unbalanced)"
                    //<<
                    //						": oid-"  << fExtentMap[lastExtentIndex].fileID
                    //<<
                    //						"; root-" << fExtentMap[lastExtentIndex].dbRoot
                    //<<
                    //						"; part-" << fExtentMap[lastExtentIndex].partitionNum
                    //<<
                    //						"; seg-"  << iter->first  <<
                    //						"; hifbo-"<< iter->second << endl;

                    startNewPartition = true;
                    break;
                }
            }
        }

        // 3a.If we already detected need for new partition, then take no action
        if (startNewPartition)
        {
            // no action taken here; we take additional action later.
        }

        // 3b.If HWM extent is in last seg file for this partition and DBRoot,
        //    find out if we need to add a new partition for next extent.
        else if (targetDbRootSegs.size() >= filesPerDBRootPerPartition)
        {
            if (lastExtent->segmentNum == lastTargetSeg)
            {
                // Use blockOffset of lastExtIdx to see if we need to add
                // the next extent to a new partition.
                if (lastExtent->blockOffset ==
                    ((EXTENTS_PER_SEGFILE - 1) * (EXTENT_ROWS * colWidth / BLOCK_SIZE)))
                {
                    startNewPartition = true;
                }
                else // Wrap-around; add extent to low seg in this partition
                {
                    startNewStripeInSegFile = true;
                    newSegmentNum = firstTargetSeg;
                }
            }
            else
            {
                newSegmentNum = nextTargetSeg;
            }
        }
        else // Select next segment file in current HWM partition
        {
            newSegmentNum = partHighSeg + 1;
        }

        // 3c. Find new partition and segment if we can't create
        //     an extent for this DBRoot in the current HWM partition.
        if (startNewPartition)
        {
            newPartitionNum++;

            if (partHighSegNext == -1)
                newSegmentNum = 0;
            else
                newSegmentNum = partHighSegNext + 1;
        }

        // 4. Set blockOffset (fbo) for new extent relative to it's seg file
        // case1: Init fbo to 0 if first extent in partition/DbRoot
        // case2: Init fbo to 0 if first extent in segment file (other than
        //        first segment in this partition/DbRoot, which case1 handled)
        // case3: Init fbo based on previous extent

        // case1: leave newBlockOffset set to 0
        if (startNewPartition)
        {
            //...no action necessary
        }

        // case2: leave newBlockOffset set to 0
        else if ((lastExtent->blockOffset == 0) && (newSegmentNum > firstTargetSeg))
        {
            //...no action necessary
        }

        // case3: Init blockOffset based on previous extent.  If we are adding
        //        extent to 1st seg file, then need to bump up the offset; else
        //        adding extent to same stripe and can repeat the same offset.
        else
        {
            if (startNewStripeInSegFile) // start next stripe
            {
                newBlockOffset =
                    static_cast<uint64_t>(lastExtent->range.size) * 1024 + lastExtent->blockOffset;
            }
            else // next extent, same stripe
            {
                newBlockOffset = lastExtent->blockOffset;
            }
        }
    }    // lastExtentIndex >= 0
    else // Empty DBRoot; use part# that the user specifies
    {
        if (bHighEmptySegNumSet)
            newSegmentNum = highEmptySegNum + 1;
        else
            newSegmentNum = 0;
    }

    //--------------------------------------------------------------------------
    // Fourth Step: Construct the new extentmap entry
    //--------------------------------------------------------------------------

    EMEntry e;
    e.range.start = startLBID;
    e.range.size = size;
    e.fileID = OID;

    if (isUnsigned(colDataType))
    {
        e.partition.cprange.lo_val = numeric_limits<uint64_t>::max();
        e.partition.cprange.hi_val = 0;
    }
    else
    {
        e.partition.cprange.lo_val = numeric_limits<int64_t>::max();
        e.partition.cprange.hi_val = numeric_limits<int64_t>::min();
    }

    e.partition.cprange.sequenceNum = 0;
    e.colWid = colWidth;
    e.dbRoot = newDbRoot;
    e.partitionNum = newPartitionNum;
    e.segmentNum = newSegmentNum;
    e.blockOffset = newBlockOffset;
    e.HWM = 0;
    e.status = EXTENTUNAVAILABLE; // mark extent as in process

    // Partition, segment, and blockOffset 0 represents new table or column.
    // When DDL creates a table, we can mark the first extent as VALID, since
    // the table has no data.  Marking as VALID enables cpimport to update
    // the CP min/max for the first import.
    // If DDL is adding a column to an existing table, setting to VALID won't
    // hurt, because DDL resets to INVALID after the extent is created.
    if ((e.partitionNum == 0) && (e.segmentNum == 0) && (e.blockOffset == 0))
        e.partition.cprange.isValid = CP_VALID;
    else
        e.partition.cprange.isValid = CP_INVALID;

    partitionNum = e.partitionNum;
    segmentNum = e.segmentNum;
    startBlockOffset = e.blockOffset;

    makeUndoRecordRBTree(UndoRecordType::INSERT, e);
    makeUndoRecord(fEMRBTreeShminfo, sizeof(MSTEntry));
    // Insert into RBTree.
    std::pair<int64_t, EMEntry> lbidEmEntryPair = make_pair(startLBID, e);
    fExtentMapRBTree->insert(lbidEmEntryPair);
    fEMRBTreeShminfo->currentSize += EM_RB_TREE_NODE_SIZE;

    // Insert into Index.
    auto resShmemHasGrownPair = fPExtMapIndexImpl_->insert(e, startLBID);
    if (resShmemHasGrownPair.second)
        fEMIndexShminfo->allocdSize = fPExtMapIndexImpl_->getShmemSize();
    if (!resShmemHasGrownPair.first)
        logAndSetEMIndexReadOnly("_createColumnExtent_DBroot");

    return startLBID;
}

//------------------------------------------------------------------------------
// Creates an extent for the exact segment column file specified by the
// requested OID, DBRoot, partition number, and segment number.  This is
// the external API function referenced by the dbrm wrapper class.
// required input:
//   OID          - column OID for which the extent is to be created
//   colWidth     - width of column in bytes
//   dbRoot       - DBRoot where extent is to be added
//   partitionNum - partitionNum
//   segmentNum   - segmentNum
// output:
//   lbid         - starting LBID of the created extent
//   allocdsize   - number of LBIDs allocated
//   startBlockOffset-starting block of the created extent
//------------------------------------------------------------------------------
void ExtentMap::createColumnExtentExactFile(int OID, uint32_t colWidth, uint16_t dbRoot,
                                            uint32_t partitionNum, uint16_t segmentNum,
                                            execplan::CalpontSystemCatalog::ColDataType colDataType,
                                            LBID_t& lbid, int& allocdsize, uint32_t& startBlockOffset)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("createColumnExtentExactFile");
        TRACER_ADDINPUT(OID);
        TRACER_ADDINPUT(colWidth);
        TRACER_ADDSHORTINPUT(dbRoot);
        TRACER_ADDOUTPUT(partitionNum);
        TRACER_ADDSHORTOUTPUT(segmentNum);
        TRACER_ADDINT64OUTPUT(lbid);
        TRACER_ADDOUTPUT(allocdsize);
        TRACER_ADDOUTPUT(startBlockOffset);
        TRACER_WRITE;
    }

#endif

#ifdef BRM_DEBUG

    if (OID <= 0)
    {
        log("ExtentMap::createColumnExtentExactFile(): OID must be > 0",
            logging::LOG_TYPE_DEBUG);
        throw invalid_argument(
            "ExtentMap::createColumnExtentExactFile(): OID must be > 0");
    }

#endif

    // Convert extent size in rows to extent size in 8192-byte blocks.
    // extentRows should be multiple of blocksize (8192).
    const unsigned EXTENT_SIZE = (getExtentRows() * colWidth) / BLOCK_SIZE;
    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
    grabFreeList(WRITE);

    growIfNeededOnExtentCreate();

    //  size is the number of multiples of 1024 blocks.
    //  ex: size=1 --> 1024 blocks
    //      size=2 --> 2048 blocks
    //      size=3 --> 3072 blocks, etc.
    uint32_t size = EXTENT_SIZE / 1024;

    lbid = _createColumnExtentExactFile(size, OID, colWidth,
                                        dbRoot, partitionNum, segmentNum, colDataType, startBlockOffset);

    allocdsize = EXTENT_SIZE;
}

void ExtentMap::logAndSetEMIndexReadOnly(const std::string& funcName)
{
    fPExtMapIndexImpl_->makeReadOnly();
    ostringstream os;
    os << "ExtentMap::" << funcName << ": "
        << "Can not update EM Index. EM Index shmem segment is set to"
        << " readonly. Please restart Columnstore.";
    log(os.str(), logging::LOG_TYPE_CRITICAL);

    throw logic_error(os.str());
}

//------------------------------------------------------------------------------
// Creates an extent for the exact segment file specified by the requested
// OID, DBRoot, partition, and segment.  This is the internal implementation
// function.
// input:
//   size         - number of multiples of 1024 blocks allocated to the extent
//                  ex: size=1 --> 1024 blocks
//                      size=2 --> 2048 blocks
//                      size=3 --> 3072 blocks, etc.
//   OID          - column OID for which the extent is to be created
//   colWidth     - width of column in bytes
//   dbRoot       - dbRoot where extent is to be added
//   partitionNum - partitionNum
//   segmentNum   - segmentNum
// output:
//   startBlockOffset-starting block of the created extent
// returns starting LBID of the created extent.
//------------------------------------------------------------------------------
LBID_t ExtentMap::_createColumnExtentExactFile(uint32_t size, int OID, uint32_t colWidth, uint16_t dbRoot,
                                               uint32_t partitionNum, uint16_t segmentNum,
                                               execplan::CalpontSystemCatalog::ColDataType colDataType,
                                               uint32_t& startBlockOffset)
{
  uint32_t highestOffset = 0;
  LBID_t startLBID = getLBIDsFromFreeList(size);
  EMEntry* lastEmEntry = nullptr;

  const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID, partitionNum);
  auto emIdents = getEmIdentsByLbids(lbids);
  for (auto& emEntry : emIdents)
  {
    if ((emEntry.segmentNum) == segmentNum && (emEntry.blockOffset >= highestOffset))
    {
      lastEmEntry = &emEntry;
      highestOffset = emEntry.blockOffset;
    }
  }

  EMEntry newEmEntry;
  newEmEntry.range.start = startLBID;
  newEmEntry.range.size = size;
  newEmEntry.fileID = OID;

  if (isUnsigned(colDataType))
  {
    newEmEntry.partition.cprange.lo_val = numeric_limits<uint64_t>::max();
    newEmEntry.partition.cprange.hi_val = 0;
  }
  else
  {
    newEmEntry.partition.cprange.lo_val = numeric_limits<int64_t>::max();
    newEmEntry.partition.cprange.hi_val = numeric_limits<int64_t>::min();
  }

  newEmEntry.partition.cprange.sequenceNum = 0;
  newEmEntry.colWid = colWidth;
  newEmEntry.dbRoot = dbRoot;
  newEmEntry.partitionNum = partitionNum;
  newEmEntry.segmentNum = segmentNum;
  newEmEntry.status = EXTENTUNAVAILABLE;  // mark extent as in process

  // If first extent for this OID, partition, dbroot, and segment then
  //   blockOffset is set to 0
  // else
  //   blockOffset is extrapolated from the last extent
  newEmEntry.HWM = 0;
  if (!lastEmEntry)
  {
    newEmEntry.blockOffset = 0;
  }
  else
  {
    newEmEntry.blockOffset = static_cast<uint64_t>(lastEmEntry->range.size) * 1024 + lastEmEntry->blockOffset;
  }

  if ((newEmEntry.partitionNum == 0) && (newEmEntry.segmentNum == 0) && (newEmEntry.blockOffset == 0))
    newEmEntry.partition.cprange.isValid = CP_VALID;
  else
    newEmEntry.partition.cprange.isValid = CP_INVALID;

  // Insert into RBTree.
  makeUndoRecordRBTree(UndoRecordType::INSERT, newEmEntry);
  std::pair<LBID_t, EMEntry> lbidEmEntryPair = make_pair(startLBID, newEmEntry);
  fExtentMapRBTree->insert(lbidEmEntryPair);
  startBlockOffset = newEmEntry.blockOffset;
  makeUndoRecord(fEMRBTreeShminfo, sizeof(MSTEntry));
  fEMRBTreeShminfo->currentSize += EM_RB_TREE_NODE_SIZE;

  // Insert into Index.
  auto resShmemHasGrownPair = fPExtMapIndexImpl_->insert(newEmEntry, startLBID);
  if (resShmemHasGrownPair.second)
    fEMIndexShminfo->allocdSize = fPExtMapIndexImpl_->getShmemSize();
  if (!resShmemHasGrownPair.first)
    logAndSetEMIndexReadOnly("_createColumnExtentExactFile");

  return startLBID;
}

//------------------------------------------------------------------------------
// Creates an extent for a dictionary store file.  This is the external API
// function.
// input:
//   OID          - column OID for which the extent is to be created
//   dbRoot       - DBRoot to be assigned to the new extent
//   partitionNum - partition number to be assigned to the new extent
//   segmentNum   - segment number to be assigned to the new extent
// output:
//   lbid         - starting LBID of the created extent
//   allocdsize   - number LBIDs of allocated
//------------------------------------------------------------------------------
void ExtentMap::createDictStoreExtent(int OID, uint16_t dbRoot, uint32_t partitionNum, uint16_t segmentNum,
                                      LBID_t& lbid, int& allocdsize)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("createDictStoreExtent");
        TRACER_ADDINPUT(OID);
        TRACER_ADDSHORTINPUT(dbRoot);
        TRACER_ADDINPUT(partitionNum);
        TRACER_ADDSHORTINPUT(segmentNum);
        TRACER_ADDINT64OUTPUT(lbid);
        TRACER_ADDOUTPUT(allocdsize);
        TRACER_WRITE;
    }

#endif

#ifdef BRM_DEBUG

    if (OID <= 0)
    {
        log("ExtentMap::createDictStoreExtent(): OID must be > 0", logging::LOG_TYPE_DEBUG);
        throw invalid_argument("ExtentMap::createDictStoreExtent(): OID must be > 0");
    }

#endif

    // Convert extent size in rows to extent size in 8192-byte blocks.
    // extentRows should be multiple of blocksize (8192).
    const unsigned EXTENT_SIZE = (getExtentRows() * DICT_COL_WIDTH) / BLOCK_SIZE;

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
    grabFreeList(WRITE);

    growIfNeededOnExtentCreate();

    //  size is the number of multiples of 1024 blocks.
    //  ex: size=1 --> 1024 blocks
    //      size=2 --> 2048 blocks
    //      size=3 --> 3072 blocks, etc.
    uint32_t size = EXTENT_SIZE / 1024;

    lbid = _createDictStoreExtent(size, OID, dbRoot, partitionNum, segmentNum);

    allocdsize = EXTENT_SIZE;
}

//------------------------------------------------------------------------------
// Creates an extent for a dictionary store file.  This is the internal
// implementation function.
// input:
//   size         - number of multiples of 1024 blocks allocated to the extent
//                  ex: size=1 --> 1024 blocks
//                      size=2 --> 2048 blocks
//                      size=3 --> 3072 blocks, etc.
//   OID          - column OID for which the extent is to be created
//   dbRoot       - DBRoot to be assigned to the new extent
//   partitionNum - partition number to be assigned to the new extent
//   segmentNum   - segment number to be assigned to the new extent
// returns starting LBID of the created extent.
//------------------------------------------------------------------------------
LBID_t ExtentMap::_createDictStoreExtent(uint32_t size, int OID, uint16_t dbRoot, uint32_t partitionNum,
                                         uint16_t segmentNum)
{
  uint32_t highestOffset = 0;
  LBID_t startLBID = getLBIDsFromFreeList(size);
  EMEntry* lastEmEntry = nullptr;

  const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID, partitionNum);
  auto emIdents = getEmIdentsByLbids(lbids);
  for (auto& emEntry : emIdents)
  {
    if ((emEntry.segmentNum == segmentNum) && (emEntry.blockOffset >= highestOffset))
    {
      lastEmEntry = &emEntry;
      highestOffset = emEntry.blockOffset;
    }
  }

  EMEntry newEmEntry;
  newEmEntry.range.start = startLBID;
  newEmEntry.range.size = size;
  newEmEntry.fileID = OID;
  newEmEntry.status = EXTENTUNAVAILABLE;  // @bug 1911 mark extent as in process
  newEmEntry.partition.cprange.lo_val = numeric_limits<int64_t>::max();
  newEmEntry.partition.cprange.hi_val = numeric_limits<int64_t>::min();
  newEmEntry.partition.cprange.sequenceNum = 0;
  newEmEntry.partition.cprange.isValid = CP_INVALID;
  newEmEntry.colWid = 0;  // we don't store col width for dictionaries;
  newEmEntry.HWM = 0;

  if (!lastEmEntry)
  {
    newEmEntry.blockOffset = 0;
    newEmEntry.segmentNum = segmentNum;
    newEmEntry.partitionNum = partitionNum;
    newEmEntry.dbRoot = dbRoot;
  }
  else
  // TODO: Why is this different comparing to `column extent creation`.
  {
    newEmEntry.blockOffset = static_cast<uint64_t>(lastEmEntry->range.size) * 1024 + lastEmEntry->blockOffset;
    newEmEntry.segmentNum = lastEmEntry->segmentNum;
    newEmEntry.partitionNum = lastEmEntry->partitionNum;
    newEmEntry.dbRoot = lastEmEntry->dbRoot;
    newEmEntry.colWid = lastEmEntry->colWid;
  }

  // Insert into RBTree.
  makeUndoRecordRBTree(UndoRecordType::INSERT, newEmEntry);
  std::pair<LBID_t, EMEntry> lbidEmEntryPair = make_pair(startLBID, newEmEntry);
  fExtentMapRBTree->insert(lbidEmEntryPair);
  makeUndoRecord(fEMRBTreeShminfo, sizeof(MSTEntry));
  fEMRBTreeShminfo->currentSize += EM_RB_TREE_NODE_SIZE;

  // Insert into Index.
  auto resShmemHasGrownPair = fPExtMapIndexImpl_->insert(newEmEntry, startLBID);
  if (resShmemHasGrownPair.second)
    fEMIndexShminfo->allocdSize = fPExtMapIndexImpl_->getShmemSize();
  if (!resShmemHasGrownPair.first)
    logAndSetEMIndexReadOnly("_createDictStoreExtent");

  return startLBID;
}

//------------------------------------------------------------------------------
// Finds and returns the starting LBID for an LBID range taken from the
// free list.
// input:
//   size - number of multiples of 1024 blocks needed from the free list
//          ex: size=1 --> 1024 blocks
//              size=2 --> 2048 blocks
//              size=3 --> 3072 blocks, etc.
// returns selected starting LBID.
//------------------------------------------------------------------------------
LBID_t ExtentMap::getLBIDsFromFreeList(uint32_t size)
{
    LBID_t ret = -1;
    int i;
    int flEntries = fFLShminfo->allocdSize / sizeof(InlineLBIDRange);

    for (i = 0; i < flEntries; i++)
    {
        if (size <= fFreeList[i].size)
        {
            makeUndoRecord(&fFreeList[i], sizeof(InlineLBIDRange));
            ret = fFreeList[i].start;
            fFreeList[i].start += size * 1024;
            fFreeList[i].size -= size;

            if (fFreeList[i].size == 0)
            {
                makeUndoRecord(fFLShminfo, sizeof(MSTEntry));
                fFLShminfo->currentSize -= sizeof(InlineLBIDRange);
            }

            break;
        }
    }

    if (i == flEntries)
    {
        log("ExtentMap::getLBIDsFromFreeList(): out of LBID space");
        throw runtime_error("ExtentMap::getLBIDsFromFreeList(): out of LBID space");
    }

    return ret;
}

#ifdef BRM_DEBUG
void ExtentMap::printEM(const EMEntry& em) const
{
    cout << " Start "
         << em.range.start << " Size "
         << (long) em.range.size << " OID "
         << (long) em.fileID << " offset "
         << (long) em.blockOffset
         << " LV " << em.partition.cprange.lo_val
         << " HV " << em.partition.cprange.hi_val;
    cout << endl;
}


void ExtentMap::printEM(const OID_t& oid) const
{
    int emEntries = 0;

    if (fEMShminfo)
        emEntries = fEMShminfo->allocdSize / sizeof(struct EMEntry);

    cout << "Extent Map (OID=" << oid << ")" << endl;

    for (int idx = 0; idx < emEntries ; idx++)
    {
        struct EMEntry& em = fExtentMap[idx];

        if (em.fileID == oid && em.range.size != 0)
            printEM(em);
    }

    cout << endl;
}

void ExtentMap::printEM() const
{

    int emEntries = 0;

    if (fEMShminfo)
        emEntries = fEMShminfo->allocdSize / sizeof(struct EMEntry);

    cout << "Extent Map (" << emEntries << ")" << endl;

    for (int idx = 0; idx < emEntries ; idx++)
    {
        struct EMEntry& em = fExtentMap[idx];

        if (em.range.size != 0)
            printEM(em);
    }

    cout << endl;
}

void ExtentMap::printFL() const
{

    int flEntries = 0;

    if (fFLShminfo)
        flEntries = fFLShminfo->allocdSize / sizeof(InlineLBIDRange);

    cout << "Free List" << endl;

    for (int idx = 0; idx < flEntries; idx++)
    {

        cout << idx << " "
             << fFreeList[idx].start << " "
             << fFreeList[idx].size
             << endl;
    }

    cout << endl;
}
#endif

//------------------------------------------------------------------------------
// Rollback (delete) the extents that logically follow the specified extent for
// the given OID and DBRoot.  HWM for the last extent is reset to the specified
// value.
// input:
//   oid          - OID of the last logical extent to be retained
//   bDeleteAll   - Flag indicates whether all extents for oid and dbroot are
//                  to be deleted; else part#, seg#, and hwm are used.
//   dbRoot       - DBRoot of the extents to be considered.
//   partitionNum - partition number of the last logical extent to be retained
//   segmentNum   - segment number of the last logical extent to be retained
//   hwm          - HWM to be assigned to the last logical extent retained
//------------------------------------------------------------------------------
void ExtentMap::rollbackColumnExtents_DBroot(int oid, bool bDeleteAll, uint16_t dbRoot, uint32_t partitionNum,
                                             uint16_t segmentNum, HWM_t hwm)
{
#ifdef BRM_INFO
    if (fDebug)
    {
        TRACER_WRITELATER("rollbackColumnExtents");
        TRACER_ADDINPUT(oid);
        TRACER_ADDBOOLINPUT(bDeleteAll);
        TRACER_ADDSHORTINPUT(dbRoot);
        TRACER_ADDINPUT(partitionNum);
        TRACER_ADDSHORTINPUT(segmentNum);
        TRACER_ADDINPUT(hwm);
        TRACER_WRITE;
    }

#endif

#ifdef BRM_DEBUG

    if (oid < 0)
    {
        log("ExtentMap::rollbackColumnExtents_DBroot(): OID must be >= 0",
            logging::LOG_TYPE_DEBUG);
        throw invalid_argument(
            "ExtentMap::rollbackColumnExtents_DBroot(): OID must be >= 0");
    }

#endif

    uint32_t fboLo = 0;
    uint32_t fboHi = 0;
    uint32_t fboLoPreviousStripe = 0;

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
    grabFreeList(WRITE);

    const auto lbids = fPExtMapIndexImpl_->find(dbRoot, oid);
    auto emIdents = getEmIteratorsByLbids(lbids);
    for (auto& emIt : emIdents)
    {
        auto& emEntry = emIt->second;
        // Don't rollback extents that are out of service
        if (emEntry.status == EXTENTOUTOFSERVICE)
        {
            continue;
        }

        // If bDeleteAll is true, then we delete extent w/o regards to
        // partition number, segment number, or HWM
        if (bDeleteAll)
        {
            emIt = deleteExtent(emIt); // case 0
            continue;
        }

        // Calculate fbo range for the stripe containing the given hwm
        if (fboHi == 0)
        {
            uint32_t range = emEntry.range.size * 1024;
            fboLo = hwm - (hwm % range);
            fboHi = fboLo + range - 1;

            if (fboLo > 0)
                fboLoPreviousStripe = fboLo - range;
        }

        // Delete, update, or ignore this extent:
        // Later partition:
        //   case 1: extent in later partition than last extent, so delete
        // Same partition:
        //   case 2: extent is in later stripe than last extent, so delete
        //   case 3: extent is in earlier stripe in the same partition.
        //           No action necessary for case3B and case3C.
        //     case 3A: extent is in trailing segment in previous stripe.
        //              This extent is now the last extent in that segment
        //              file, so reset the local HWM if it was altered.
        //     case 3B: extent in previous stripe but not a trailing segment
        //     case 3C: extent is in stripe that precedes previous stripe
        //   case 4: extent is in the same partition and stripe as the
        //           last logical extent we are to keep.
        //     case 4A: extent is in later segment so can be deleted
        //     case 4B: extent is in earlier segment, reset HWM if changed
        //     case 4C: this is last logical extent, reset HWM if changed
        // Earlier partition:
        //   case 5: extent is in earlier parition, no action necessary

        if (emEntry.partitionNum > partitionNum)
        {
            emIt = deleteExtent(emIt); // case 1
            continue;
        }
        else if (emEntry.partitionNum == partitionNum)
        {
            if (emEntry.blockOffset > fboHi)
            {
                emIt = deleteExtent(emIt); // case 2
                continue;
            }
            else if (emEntry.blockOffset < fboLo)
            {
                if (emEntry.blockOffset >= fboLoPreviousStripe)
                {
                    if (emEntry.segmentNum > segmentNum)
                    {
                        if (emEntry.HWM != (fboLo - 1))
                        {
                            makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
                            emEntry.HWM = fboLo - 1; // case 3A
                            emEntry.status = EXTENTAVAILABLE;
                        }
                    }
                }
            }
            else // extent is in same stripe
            {
                if (emEntry.segmentNum > segmentNum)
                {
                    emIt = deleteExtent(emIt); // case 4A
                    continue;
                }
                else if (emEntry.segmentNum < segmentNum)
                {
                    if (emEntry.HWM != fboHi)
                    {
                        makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
                        emEntry.HWM = fboHi; // case 4B
                        emEntry.status = EXTENTAVAILABLE;
                    }
                }
                else // fExtentMap[i].segmentNum == segmentNum
                {
                    if (emEntry.HWM != hwm)
                    {
                        makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
                        emEntry.HWM = hwm; // case 4C
                        emEntry.status = EXTENTAVAILABLE;
                    }
                }
            }
        }
    }

    // If this function is called, we are already in error recovery mode; so
    // don't worry about reporting an error if the OID is not found, because
    // we don't want/need the extents for that OID anyway.
    // if (!oidExists)
    //{
    //	ostringstream oss;
    //	oss << "ExtentMap::rollbackColumnExtents_DBroot(): "
    //		"Rollback failed: no extents exist for: OID-" << oid <<
    //		"; dbRoot-"    << dbRoot       <<
    //		"; partition-" << partitionNum <<
    //		"; segment-"   << segmentNum   <<
    //		"; hwm-"       << hwm;
    //	log(oss.str(), logging::LOG_TYPE_CRITICAL);
    //	throw invalid_argument(oss.str());
    //}
}

//------------------------------------------------------------------------------
// Rollback (delete) the extents that follow the extents in partitionNum,
// for the given dictionary OID & DBRoot.  The specified hwms represent the HWMs
// to be reset for each of segment store file in this partition.  An HWM will
// not be given for "every" segment file if we are rolling back to a point where
// we had not yet created all the segment files in the partition.  In any case,
// any extents for the "oid" that follow partitionNum, should be deleted.
// Likewise, any extents in the same partition, whose segment file is not in
// segNums[], should be deleted as well.  If hwms is empty, then this DBRoot
// must have been empty at the start of the job, so all the extents for the
// specified oid and dbRoot can be deleted.
// input:
//   oid          - OID of the "last" extents to be retained
//   dbRoot       - DBRoot of the extents to be considered.
//   partitionNum - partition number of the last extents to be retained
//   segNums      - list of segment files with extents to be restored
//   hwms         - HWMs to be assigned to the last retained extent in each of
//                      the corresponding segment store files in segNums.
//                  hwms[0] applies to segment store file segNums[0];
//                  hwms[1] applies to segment store file segNums[1]; etc.
//------------------------------------------------------------------------------
void ExtentMap::rollbackDictStoreExtents_DBroot(int oid, uint16_t dbRoot, uint32_t partitionNum,
                                                const vector<uint16_t>& segNums, const vector<HWM_t>& hwms)
{
#ifdef BRM_INFO
    if (fDebug)
    {
        ostringstream oss;

        for (unsigned int k = 0; k < hwms.size(); k++)
            oss << "; hwms[" << k << "]-"  << hwms[k];

        const string& hwmString(oss.str());

        // put TRACE inside separate scope {} to insure that temporary
        // hwmString still exists when tracer destructor tries to print it.
        {
            TRACER_WRITELATER("rollbackDictStoreExtents_DBroot");
            TRACER_ADDINPUT(oid);
            TRACER_ADDSHORTINPUT(dbRoot);
            TRACER_ADDINPUT(partitionNum);
            TRACER_ADDSTRINPUT(hwmString);
            TRACER_WRITE;
        }
    }

#endif

    // Delete all extents for the specified OID and DBRoot,
    // if we are not given any hwms and segment files.
    bool bDeleteAll = false;

    if (hwms.size() == 0)
        bDeleteAll = true;

    // segToHwmMap maps segment file number to corresponding pair<hwm,fboLo>
    tr1::unordered_map<uint16_t, pair<uint32_t, uint32_t> > segToHwmMap;
    tr1::unordered_map<uint16_t, pair<uint32_t, uint32_t> >::const_iterator
    segToHwmMapIter;

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
    grabFreeList(WRITE);

    const auto lbids = fPExtMapIndexImpl_->find(dbRoot, oid);
    auto emIdents = getEmIteratorsByLbids(lbids);
    for (auto& emIt : emIdents)
    {
        auto& emEntry = emIt->second;
        // Don't rollback extents that are out of service
        if (emEntry.status == EXTENTOUTOFSERVICE)
        {
            continue;
        }

        // If bDeleteAll is true, then we delete extent w/o regards to
        // partition number, segment number, or HWM
        if (bDeleteAll)
        {
            emIt = deleteExtent(emIt); // case 0
            continue;
        }

        // Calculate fbo's for the list of hwms we are given; and store
        // the fbo and hwm in a map, using the segment file number as a key.
        if (segToHwmMap.size() == 0)
        {
            uint32_t range = emEntry.range.size * 1024;
            pair<uint32_t, uint32_t> segToHwmMapEntry;

            for (unsigned int k = 0; k < hwms.size(); k++)
            {
                uint32_t fboLo = hwms[k] - (hwms[k] % range);
                segToHwmMapEntry.first = hwms[k];
                segToHwmMapEntry.second = fboLo;
                segToHwmMap[segNums[k]] = segToHwmMapEntry;
            }
        }

        // Delete, update, or ignore this extent:
        // Later partition:
        //   case 1: extent is in later partition, so delete the extent
        // Same partition:
        //   case 2: extent is in trailing seg file we don't need; so delete
        //   case 3: extent is in partition and segment file of interest
        //     case 3A: earlier extent in segment file; no action necessary
        //     case 3B: specified HWM falls in this extent, so reset HWM
        //     case 3C: later extent in segment file; so delete the extent
        // Earlier partition:
        //   case 4: extent is in earlier parition, no action necessary

        if (emEntry.partitionNum > partitionNum)
        {
            emIt = deleteExtent(emIt); // case 1
            continue;
        }
        else if (emEntry.partitionNum == partitionNum)
        {
            unsigned segNum = emEntry.segmentNum;
            segToHwmMapIter = segToHwmMap.find(segNum);

            if (segToHwmMapIter == segToHwmMap.end())
            {
                emIt = deleteExtent(emIt); // case 2
                continue;
            }
            else // segment number in the map of files to keep
            {
                uint32_t fboLo = segToHwmMapIter->second.second;

                if (emEntry.blockOffset < fboLo)
                {
                    // no action necessary                           case 3A
                }
                else if (emEntry.blockOffset == fboLo)
                {
                    uint32_t hwm = segToHwmMapIter->second.first;

                    if (emEntry.HWM != hwm)
                    {
                        makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
                        emEntry.HWM = hwm;
                        emEntry.status = EXTENTAVAILABLE; // case 3B
                    }
                }
                else
                {
                    emIt = deleteExtent(emIt); // case 3C
                    continue;
                }
            }
        }
    }
}

//------------------------------------------------------------------------------
// Delete the extents specified and reset hwm
//------------------------------------------------------------------------------
void ExtentMap::deleteEmptyColExtents(const ExtentsInfoMap_t& extentsInfo)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("deleteEmptyColExtents");
        TRACER_WRITE;
    }

#endif

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
    grabFreeList(WRITE);

    uint32_t fboLo = 0;
    uint32_t fboHi = 0;
    uint32_t fboLoPreviousStripe = 0;

    ExtentsInfoMap_t::const_iterator emInfoIter = extentsInfo.begin();
    for (; emInfoIter != extentsInfo.end(); ++emInfoIter)
    {
        const OID_t oid = emInfoIter->first;
        const DBRootT dbroot = emInfoIter->second.dbRoot;
        const PartitionNumberT partNum = emInfoIter->second.partitionNum;
        const auto lbids = fPExtMapIndexImpl_->find(dbroot, oid, partNum);
        auto emIters = getEmIteratorsByLbids(lbids);
        for (auto& emIt: emIters)
        {
            auto emEntry = emIt->second;
            // Don't rollback extents that are out of service.
            if (emEntry.status == EXTENTOUTOFSERVICE)
            {
                continue;
            }

            // Calculate fbo range for the stripe containing the given hwm.
            if (fboHi == 0)
            {
                uint32_t range = emEntry.range.size * 1024;
                fboLo = emInfoIter->second.hwm - (emInfoIter->second.hwm % range);
                fboHi = fboLo + range - 1;

                if (fboLo > 0)
                    fboLoPreviousStripe = fboLo - range;
            }

            // Delete, update, or ignore this extent:
            // Later partition:
            //   case 1: extent in later partition than last extent, so delete
            // Same partition:
            //   case 2: extent is in later stripe than last extent, so delete
            //   case 3: extent is in earlier stripe in the same partition.
            //           No action necessary for case3B and case3C.
            //     case 3A: extent is in trailing segment in previous stripe.
            //              This extent is now the last extent in that segment
            //              file, so reset the local HWM if it was altered.
            //     case 3B: extent in previous stripe but not a trailing segment
            //     case 3C: extent is in stripe that precedes previous stripe
            //   case 4: extent is in the same partition and stripe as the
            //           last logical extent we are to keep.
            //     case 4A: extent is in later segment so can be deleted
            //     case 4B: extent is in earlier segment, reset HWM if changed
            //     case 4C: this is last logical extent, reset HWM if changed
            // Earlier partition:
            //   case 5: extent is in earlier parition, no action necessary

            if (emEntry.partitionNum > emInfoIter->second.partitionNum)
            {
                emIt = deleteExtent(emIt); // case 1
                continue;
            }
            else if (emEntry.partitionNum == emInfoIter->second.partitionNum)
            {
                if (emEntry.blockOffset > fboHi)
                {
                    emIt = deleteExtent(emIt); // case 2
                    continue;
                }
                else if (emEntry.blockOffset < fboLo)
                {
                    if (emEntry.blockOffset >= fboLoPreviousStripe)
                    {
                        if (emEntry.segmentNum > emInfoIter->second.segmentNum)
                        {
                            if (emEntry.HWM != (fboLo - 1))
                            {
                                makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
                                emEntry.HWM = fboLo - 1; // case 3A
                                emEntry.status = EXTENTAVAILABLE;
                            }
                        }
                    }
                }
                else
                {
                    // extent is in same stripe
                    if (emEntry.segmentNum > emInfoIter->second.segmentNum)
                    {
                        emIt = deleteExtent(emIt); // case 4A
                        continue;
                    }
                    else if (emEntry.segmentNum < emInfoIter->second.segmentNum)
                    {
                        if (emEntry.HWM != fboHi)
                        {
                            makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
                            emEntry.HWM = fboHi; // case 4B
                            emEntry.status = EXTENTAVAILABLE;
                        }
                    }
                    else
                    {
                        if (emEntry.HWM != emInfoIter->second.hwm)
                        {
                            makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
                            emEntry.HWM = emInfoIter->second.hwm; // case 4C
                            emEntry.status = EXTENTAVAILABLE;
                        }
                    }
                }
            }
        }
    }
}

void ExtentMap::deleteEmptyDictStoreExtents(const ExtentsInfoMap_t& extentsInfo)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("deleteEmptyDictStoreExtents");
        TRACER_WRITE;
    }

#endif

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
    grabFreeList(WRITE);

    uint32_t fboLo = 0;
    uint32_t fboHi = 0;

    ExtentsInfoMap_t::const_iterator emInfoIter = extentsInfo.begin();
    if (emInfoIter->second.newFile) // The extent is the new extent
    {
        for (; emInfoIter != extentsInfo.end(); ++emInfoIter)
        {
            const OID_t oid = emInfoIter->first;
            const DBRootT dbroot = emInfoIter->second.dbRoot;
            const PartitionNumberT partNum = emInfoIter->second.partitionNum;
            const auto lbids = fPExtMapIndexImpl_->find(dbroot, oid, partNum);
            auto emIters = getEmIteratorsByLbids(lbids);
            for (auto& emIt: emIters)
            {
                auto emEntry = emIt->second;
                if (emEntry.segmentNum == emInfoIter->second.segmentNum)
                {
                    emIt = deleteExtent(emIt);
                }
            } // em iterarors loop
        } // em info map loop
    }
    else
    {
        for (; emInfoIter != extentsInfo.end(); ++emInfoIter)
        {
            const OID_t oid = emInfoIter->first;
            const DBRootT dbroot = emInfoIter->second.dbRoot;
            const PartitionNumberT partNum = emInfoIter->second.partitionNum;
            const auto lbids = fPExtMapIndexImpl_->find(dbroot, oid, partNum);
            auto emIters = getEmIteratorsByLbids(lbids);
            for (auto& emIt: emIters)
            {
                auto emEntry = emIt->second;

                // Don't rollback extents that are out of service
                if (emEntry.status == EXTENTOUTOFSERVICE)
                {
                    continue;
                }

                // Calculate fbo
                if (fboHi == 0)
                {
                    uint32_t range = emEntry.range.size * 1024;
                    fboLo = emInfoIter->second.hwm - (emInfoIter->second.hwm % range);
                    fboHi = fboLo + range - 1;
                }

                // Delete, update, or ignore this extent:
                // Later partition:
                //   case 1: extent is in later partition, so delete the extent
                // Same partition:
                //   case 2: extent is in partition and segment file of interest
                //     case 2A: earlier extent in segment file; no action necessary
                //     case 2B: specified HWM falls in this extent, so reset HWM
                //     case 2C: later extent in segment file; so delete the extent
                // Earlier partition:
                //   case 3: extent is in earlier parition, no action necessary

                if (emEntry.partitionNum > emInfoIter->second.partitionNum)
                {
                    emIt = deleteExtent(emIt); // case 1
                    continue;
                }
                else if (emEntry.partitionNum == emInfoIter->second.partitionNum)
                {
                    if (emEntry.segmentNum == emInfoIter->second.segmentNum)
                    {
                        if (emEntry.blockOffset < fboLo)
                        {
                            // no action necessary                           case 2A
                        }
                        else if (emEntry.blockOffset == fboLo)
                        {
                            if (emEntry.HWM != emInfoIter->second.hwm)
                            {
                                makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
                                emEntry.HWM = emInfoIter->second.hwm;
                                emEntry.status = EXTENTAVAILABLE; // case 2B
                            }
                        }
                        else
                        {
                            emIt = deleteExtent(emIt); // case 3C
                            continue;
                        }
                    }
                }
            } // em iterarors loop
        } // em info map loop
    }
}

//------------------------------------------------------------------------------
// Delete all the extents for the specified OID
//------------------------------------------------------------------------------
void ExtentMap::deleteOID(int OID)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("deleteOID");
        TRACER_ADDINPUT(OID);
        TRACER_WRITE;
    }

#endif

    bool OIDExists = false;

#ifdef BRM_DEBUG

    if (OID < 0)
    {
        log("ExtentMap::deleteOID(): OID must be >= 0", logging::LOG_TYPE_DEBUG);
        throw invalid_argument("ExtentMap::deleteOID(): OID must be >= 0");
    }

#endif

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
    grabFreeList(WRITE);

    // Clean up the index and tell deleteExtent to skip the clean-up.
    DBRootVec dbRootVec(getAllDbRoots());

    for (auto dbRoot: dbRootVec)
    {
        const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID);
        auto emIdents = getEmIteratorsByLbids(lbids);
        OIDExists = (!emIdents.empty());
        for (auto& emIt : emIdents)
        {
            emIt = deleteExtent(emIt, false);
        }
        fPExtMapIndexImpl_->deleteOID(dbRoot, OID);
    }

    if (!OIDExists)
    {
        ostringstream oss;
        oss << "ExtentMap::deleteOID(): There are no extent entries for OID " << OID << endl;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }
}

//------------------------------------------------------------------------------
// Delete all the extents for the specified OIDs
//------------------------------------------------------------------------------
void ExtentMap::deleteOIDs(const OidsMap_t& OIDs)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("deleteOIDs");
        TRACER_WRITE;
    }

#endif
    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
    grabFreeList(WRITE);

    DBRootVec dbRootVec(getAllDbRoots());
    for (auto dbRoot: dbRootVec)
    {
        for (auto& oidOidPair : OIDs)
        {
            const auto lbids = fPExtMapIndexImpl_->find(dbRoot, oidOidPair.first);
            auto emIdents = getEmIteratorsByLbids(lbids);
            for (auto& emIt : emIdents)
            {
                emIt = deleteExtent(emIt, false); // Don't clean up the index
            }
            // Clean-up the index at the dbroot/OID at once
            fPExtMapIndexImpl_->deleteOID(dbRoot, oidOidPair.first);
        }
    }

}

//------------------------------------------------------------------------------
// Delete the specified extent from the extentmap and return to the free list.
// emIndex - the index (from the extent map) of the extent to be deleted
//------------------------------------------------------------------------------
ExtentMapRBTree::iterator ExtentMap::deleteExtent(ExtentMapRBTree::iterator it, const bool clearEMIndex)
{
    int flIndex, freeFLIndex, flEntries, preceedingExtent, succeedingExtent;
    LBID_t flBlockEnd, emBlockEnd;

    flEntries = fFLShminfo->allocdSize / sizeof(InlineLBIDRange);
    auto& emEntry = it->second;

    emBlockEnd = emEntry.range.start + (static_cast<LBID_t>(emEntry.range.size) * 1024);

    // Scan the freelist to see where this entry fits in.
    for (flIndex = 0, preceedingExtent = -1, succeedingExtent = -1, freeFLIndex = -1; flIndex < flEntries;
         flIndex++)
    {
        if (fFreeList[flIndex].size == 0)
            freeFLIndex = flIndex;
        else
        {
            flBlockEnd = fFreeList[flIndex].start + (static_cast<LBID_t>(fFreeList[flIndex].size) * 1024);

            if (emBlockEnd == fFreeList[flIndex].start)
                succeedingExtent = flIndex;
            else if (flBlockEnd == emEntry.range.start)
                preceedingExtent = flIndex;
        }
    }

    // Update the freelist.
    // This space is in between 2 blocks in the FL.
    if (preceedingExtent != -1 && succeedingExtent != -1)
    {
        makeUndoRecord(&fFreeList[preceedingExtent], sizeof(InlineLBIDRange));

        // Migrate the entry upward if there's a space.
        if (freeFLIndex < preceedingExtent && freeFLIndex != -1)
        {
            makeUndoRecord(&fFreeList[freeFLIndex], sizeof(InlineLBIDRange));
            memcpy(&fFreeList[freeFLIndex], &fFreeList[preceedingExtent], sizeof(InlineLBIDRange));
            fFreeList[preceedingExtent].size = 0;
            preceedingExtent = freeFLIndex;
        }

        fFreeList[preceedingExtent].size += fFreeList[succeedingExtent].size + emEntry.range.size;
        makeUndoRecord(&fFreeList[succeedingExtent], sizeof(InlineLBIDRange));
        fFreeList[succeedingExtent].size = 0;
        makeUndoRecord(fFLShminfo, sizeof(MSTEntry));
        fFLShminfo->currentSize -= sizeof(InlineLBIDRange);
    }

    // This space has a free block at the end.
    else if (succeedingExtent != -1)
    {
        makeUndoRecord(&fFreeList[succeedingExtent], sizeof(InlineLBIDRange));

        // Migrate the entry upward if there's a space.
        if (freeFLIndex < succeedingExtent && freeFLIndex != -1)
        {
            makeUndoRecord(&fFreeList[freeFLIndex], sizeof(InlineLBIDRange));
            memcpy(&fFreeList[freeFLIndex], &fFreeList[succeedingExtent], sizeof(InlineLBIDRange));
            fFreeList[succeedingExtent].size = 0;
            succeedingExtent = freeFLIndex;
        }

        fFreeList[succeedingExtent].start = emEntry.range.start;
        fFreeList[succeedingExtent].size += emEntry.range.size;
    }

    // This space has a free block at the beginning.
    else if (preceedingExtent != -1)
    {
        makeUndoRecord(&fFreeList[preceedingExtent], sizeof(InlineLBIDRange));

        // Migrate the entry upward if there's a space.
        if (freeFLIndex < preceedingExtent && freeFLIndex != -1)
        {
            makeUndoRecord(&fFreeList[freeFLIndex], sizeof(InlineLBIDRange));
            memcpy(&fFreeList[freeFLIndex], &fFreeList[preceedingExtent], sizeof(InlineLBIDRange));
            fFreeList[preceedingExtent].size = 0;
            preceedingExtent = freeFLIndex;
        }

        fFreeList[preceedingExtent].size += emEntry.range.size;
    }

    // The freelist has no adjacent blocks, so make a new entry.
    else
    {
        if (fFLShminfo->currentSize == fFLShminfo->allocdSize)
        {
            growFLShmseg();
#ifdef BRM_DEBUG

            if (freeFLIndex != -1)
            {
                log("ExtentMap::deleteOID(): found a free FL entry in a supposedly full shmseg",
                    logging::LOG_TYPE_DEBUG);
                throw logic_error(
                    "ExtentMap::deleteOID(): found a free FL entry in a supposedly full shmseg");
            }

#endif
            freeFLIndex = flEntries; // happens to be the right index
            flEntries = fFLShminfo->allocdSize / sizeof(InlineLBIDRange);
        }

#ifdef BRM_DEBUG

        if (freeFLIndex == -1)
        {
            log("ExtentMap::deleteOID(): no available free list entries?", logging::LOG_TYPE_DEBUG);
            throw logic_error("ExtentMap::deleteOID(): no available free list entries?");
        }

#endif
        makeUndoRecord(&fFreeList[freeFLIndex], sizeof(InlineLBIDRange));
        fFreeList[freeFLIndex].start = emEntry.range.start;
        fFreeList[freeFLIndex].size = emEntry.range.size;
        makeUndoRecord(&fFLShminfo, sizeof(MSTEntry));
        fFLShminfo->currentSize += sizeof(InlineLBIDRange);
    }

    // Clear index if needed.
    if (clearEMIndex)
        fPExtMapIndexImpl_->deleteEMEntry(it->second, it->first);

    makeUndoRecordRBTree(UndoRecordType::DELETE, it->second);
    // Erase a node for the given iterator.
    makeUndoRecord(&fEMRBTreeShminfo, sizeof(MSTEntry));
    fEMRBTreeShminfo->currentSize -= EM_RB_TREE_NODE_SIZE;
    return fExtentMapRBTree->erase(it);
}

//------------------------------------------------------------------------------
// Returns the last local HWM for the specified OID for the given DBroot.
// Also returns the DBRoot, and partition, and segment numbers for the relevant
// segment file. Technically, this function finds the "last" extent for the
// specified OID, and returns the HWM for that extent.  It is assumed that the
// HWM for the segment file containing this "last" extent, has been stored in
// that extent's hwm; and that the hwm is not still hanging around in a previous
// extent for the same segment file.
// If no available or outOfService extent is found, then bFound is returned
// as false.
//------------------------------------------------------------------------------
HWM_t ExtentMap::getLastHWM_DBroot(int OID, uint16_t dbRoot, uint32_t& partitionNum, uint16_t& segmentNum,
                                   int& status, bool& bFound)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("getLastHWM_DBroot");
        TRACER_ADDINPUT(OID);
        TRACER_ADDSHORTINPUT(dbRoot);
        TRACER_ADDOUTPUT(partitionNum);
        TRACER_ADDSHORTOUTPUT(segmentNum);
        TRACER_ADDOUTPUT(status);
        TRACER_WRITE;
    }

#endif

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    uint32_t lastExtent = 0;
    partitionNum = 0;
    segmentNum = 0;
    HWM_t hwm = 0;
    bFound = false;

    if (OID < 0)
    {
        ostringstream oss;
        oss << "ExtentMap::getLastHWM_DBroot(): invalid OID requested: " << OID;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }

    const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID);
    const auto emIdents = getEmIdentsByLbids(lbids);
    auto lastEmEntry = emIdents.begin();
    for (auto emEntry = emIdents.begin(); emEntry < emIdents.end(); ++emEntry)
    {
        if (((emEntry->status == EXTENTAVAILABLE) || (emEntry->status == EXTENTOUTOFSERVICE)) &&
            ((emEntry->partitionNum > partitionNum) ||
            ((emEntry->partitionNum == partitionNum) && (emEntry->blockOffset > lastExtent)) ||
            ((emEntry->partitionNum == partitionNum) && (emEntry->blockOffset == lastExtent) && (emEntry->segmentNum >= segmentNum))))
        {
            bFound = true;
            lastExtent = emEntry->blockOffset;
            partitionNum = emEntry->partitionNum;
            segmentNum = emEntry->segmentNum;
            lastEmEntry = emEntry;
        }
    }

    if (bFound)
    {
        hwm = lastEmEntry->HWM;
        status = lastEmEntry->status;
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);

    return hwm;
}

//------------------------------------------------------------------------------
// For the specified OID and PM number, this function will return a vector
// of objects carrying HWM info (for the last segment file) and block count
// information about each DBRoot assigned to the specified PM.
//------------------------------------------------------------------------------
void ExtentMap::getDbRootHWMInfo(int OID, uint16_t pmNumber, EmDbRootHWMInfo_v& emDbRootHwmInfos)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("getDbRootHWMInfo");
        TRACER_ADDINPUT(OID);
        TRACER_ADDSHORTINPUT(pmNumber);
        TRACER_WRITE;
    }

#endif

    if (OID < 0)
    {
        ostringstream oss;
        oss << "ExtentMap::getDbRootHWMInfo(): invalid OID requested: " << OID;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }

    // Determine List of DBRoots for specified PM, and construct map of
    // EmDbRootHWMInfo objects.
    tr1::unordered_map<uint16_t, EmDbRootHWMInfo> emDbRootMap;
    vector<int> dbRootVec;
    getPmDbRoots(pmNumber, dbRootVec);

    if (dbRootVec.size() > 0)
    {
        for (unsigned int iroot = 0; iroot < dbRootVec.size(); iroot++)
        {
            uint16_t rootID = dbRootVec[iroot];
            EmDbRootHWMInfo emDbRootInfo(rootID);
            emDbRootMap[rootID] = emDbRootInfo;
        }
    }
    else
    {
        ostringstream oss;
        oss << "ExtentMap::getDbRootHWMInfo(): "
               "There are no DBRoots for OID "
            << OID << " and PM " << pmNumber << endl;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }

    grabEMEntryTable(READ);
    grabEMIndex(READ);
    tr1::unordered_map<uint16_t, EmDbRootHWMInfo>::iterator emIter;
    // Indicates that we found an extent, the index itself means nothing.
    // TODO: Update to flag.
    uint32_t foundIndex = 0;

    // Searching the array in reverse order should be faster since the last
    // extent is usually at the bottom.  We still have to search the entire
    // array (just in case), but the number of operations per loop iteration
    // will be less.
    for (auto dbRoot : dbRootVec)
    {
        const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID);
        const auto emIdents = getEmIdentsByLbids(lbids);
        for (auto& emEntry : emIdents)
        {
            if (emEntry.fileID == OID)
            {
                // Include this extent in the search, only if the extent's
                // DBRoot falls in the list of DBRoots for this PM.
                emIter = emDbRootMap.find(emEntry.dbRoot);

                if (emIter == emDbRootMap.end())
                    continue;

                EmDbRootHWMInfo& emDbRoot = emIter->second;

                if ((emEntry.status != EXTENTOUTOFSERVICE) && (emEntry.HWM != 0))
                    emDbRoot.totalBlocks += (emEntry.HWM + 1);

                if ((emEntry.partitionNum > emDbRoot.partitionNum) ||
                    ((emEntry.partitionNum == emDbRoot.partitionNum) &&
                     (emEntry.blockOffset > emDbRoot.fbo)) ||
                    ((emEntry.partitionNum == emDbRoot.partitionNum) &&
                     (emEntry.blockOffset == emDbRoot.fbo) && (emEntry.segmentNum >= emDbRoot.segmentNum)))
                {
                    emDbRoot.fbo = emEntry.blockOffset;
                    emDbRoot.partitionNum = emEntry.partitionNum;
                    emDbRoot.segmentNum = emEntry.segmentNum;
                    emDbRoot.localHWM = emEntry.HWM;
                    emDbRoot.startLbid = emEntry.range.start;
                    emDbRoot.status = emEntry.status;
                    emDbRoot.hwmExtentIndex = foundIndex;
                }
            }
            ++foundIndex;
        }
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);

    for (tr1::unordered_map<uint16_t, EmDbRootHWMInfo>::iterator iter = emDbRootMap.begin();
         iter != emDbRootMap.end(); ++iter)
    {
        EmDbRootHWMInfo& emDbRoot = iter->second;

        if (emDbRoot.hwmExtentIndex != -1)
        {
            // @bug 5349: make sure HWM extent for each DBRoot is AVAILABLE
            if (emDbRoot.status == EXTENTUNAVAILABLE)
            {
                ostringstream oss;
                oss << "ExtentMap::getDbRootHWMInfo(): "
                    << "OID " << OID << " has HWM extent that is UNAVAILABLE for "
                    << "DBRoot" << emDbRoot.dbRoot << "; part#: " << emDbRoot.partitionNum
                    << ", seg#: " << emDbRoot.segmentNum << ", fbo: " << emDbRoot.fbo
                    << ", localHWM: " << emDbRoot.localHWM << ", lbid: " << emDbRoot.startLbid << endl;
                log(oss.str(), logging::LOG_TYPE_CRITICAL);
                throw runtime_error(oss.str());
            }

            // In the loop above we ignored "all" the extents with HWM of 0,
            // which is okay most of the time, because each segment file's HWM
            // is carried in the last extent only.  BUT if we have a segment
            // file with HWM=0, having a single extent and a single block at
            // the "end" of the data, we still need to account for this last
            // block.  So we increment the block count for this isolated case.
            if ((emDbRoot.localHWM == 0) && (emDbRoot.status == EXTENTAVAILABLE))
            {
                emDbRoot.totalBlocks++;
            }
        }
    }

    // Copy internal map to the output vector argument
    for (tr1::unordered_map<uint16_t, EmDbRootHWMInfo>::iterator iter = emDbRootMap.begin();
         iter != emDbRootMap.end(); ++iter)
    {
        emDbRootHwmInfos.push_back(iter->second);
    }
}

//------------------------------------------------------------------------------
// Return the existence (bFound) and state (status) for the segment file
// containing the extents for the specified OID, partition, and segment.
// If no extents are found, no exception is thrown.  We instead just return
// bFound=false, so that the application can take the necessary action.
// The value returned in the "status" variable is based on the first extent
// found, since all the extents in a segment file should have the same state.
//------------------------------------------------------------------------------
void ExtentMap::getExtentState(int OID, uint32_t partitionNum,
                               uint16_t segmentNum, bool& bFound, int& status)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("getExtentState");
        TRACER_ADDINPUT(OID);
        TRACER_ADDINPUT(partitionNum);
        TRACER_ADDSHORTINPUT(segmentNum);
        TRACER_ADDOUTPUT(status);
        TRACER_WRITE;
    }

#endif
    bFound = false;
    status = EXTENTAVAILABLE;

    if (OID < 0)
    {
        ostringstream oss;
        oss << "ExtentMap::getExtentState(): invalid OID requested: " << OID;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    DBRootVec dbRootVec(getAllDbRoots());
    for (auto dbRoot : dbRootVec)
    {
        const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID, partitionNum);
        const auto emIdents = getEmIdentsByLbids(lbids);
        for (const auto& emEntry : emIdents)
        {
            if (emEntry.segmentNum == segmentNum)
            {
                bFound = true;
                status = emEntry.status;
                break;
            }
        }
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
}

//------------------------------------------------------------------------------
// Returns the HWM for the specified OID, partition, and segment numbers.
// Used to get the HWM for a specific column or dictionary store segment file.
//------------------------------------------------------------------------------
HWM_t ExtentMap::getLocalHWM(int OID, uint32_t partitionNum, uint16_t segmentNum, int& status)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("getLocalHWM");
        TRACER_ADDINPUT(OID);
        TRACER_ADDINPUT(partitionNum);
        TRACER_ADDSHORTINPUT(segmentNum);
        TRACER_ADDOUTPUT(status);
        TRACER_WRITE;
    }

#endif

#ifdef EM_AS_A_TABLE_POC__

    if (OID == 1084)
    {
        return 0;
    }

#endif

    HWM_t ret = 0;
    bool OIDPartSegExists = false;

    if (OID < 0)
    {
        ostringstream oss;
        oss << "ExtentMap::getLocalHWM(): invalid OID requested: " << OID;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    DBRootVec dbRootVec(getAllDbRoots());

    for (auto dbRoot : dbRootVec)
    {
        const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID, partitionNum);
        const auto emIdents = getEmIdentsByLbids(lbids);
        for (auto& emEntry : emIdents)
        {
            if ((emEntry.segmentNum == segmentNum))
            {
                OIDPartSegExists = true;
                status = emEntry.status;
                if (emEntry.HWM != 0)
                {
                    ret = emEntry.HWM;
                    releaseEMIndex(READ);
                    releaseEMEntryTable(READ);
                    return ret;
                }
            }
        }
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);

    if (OIDPartSegExists)
        return 0;
    else
    {
        ostringstream oss;
        oss << "ExtentMap::getLocalHWM(): There are no extent entries for OID " <<
            OID << "; partition " << partitionNum << "; segment " <<
            segmentNum << endl;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }
}

//------------------------------------------------------------------------------
// Sets the HWM for the specified OID, partition, and segment number.
// In addition, the HWM for the old HWM extent (for this segment file),
// is set to 0, so that the latest HWM is only carried in the last extent
// (per segment file).
// Used for dictionary or column OIDs to set the HWM for specific segment file.
//------------------------------------------------------------------------------
void ExtentMap::setLocalHWM(int OID, uint32_t partitionNum,
                            uint16_t segmentNum, HWM_t newHWM, bool firstNode, bool uselock)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("setLocalHWM");
        TRACER_ADDINPUT(OID);
        TRACER_ADDINPUT(partitionNum);
        TRACER_ADDSHORTINPUT(segmentNum);
        TRACER_ADDINPUT(newHWM);
        TRACER_WRITE;
    }

    bool addedAnExtent = false;

    if (OID < 0)
    {
        log("ExtentMap::setLocalHWM(): OID must be >= 0",
            logging::LOG_TYPE_DEBUG);
        throw invalid_argument(
            "ExtentMap::setLocalHWM(): OID must be >= 0");
    }

#endif

    EMEntry* lastEm = nullptr;
    EMEntry* prevEm = nullptr;
    uint32_t highestOffset = 0;

    if (uselock)
    {
        grabEMEntryTable(WRITE);
        grabEMIndex(WRITE);
    }

    DBRootVec dbRootVec(getAllDbRoots());

    for (auto dbRoot: dbRootVec)
    {
        const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID, partitionNum);
        auto emIdents = getEmIteratorsByLbids(lbids);
        for (auto emIt : emIdents)
        {
            auto& emEntry = emIt->second;
            if (emEntry.segmentNum == segmentNum)
            {
                // Find current HWM extent in case of multiple extents per segment file.
                if (emEntry.blockOffset >= highestOffset)
                {
                    highestOffset = emEntry.blockOffset;
                    lastEm = &emEntry;
                }

                // Find previous HWM extent.
                if (emEntry.HWM != 0)
                    prevEm = &emEntry;
            }
        }
    }

    if (lastEm == nullptr)
    {
        ostringstream oss;
        oss << "ExtentMap::setLocalHWM(): Bad OID/partition/segment argument; "
               "no extent entries for OID "
            << OID << "; partition " << partitionNum << "; segment " << segmentNum << endl;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }

    if (newHWM >= (lastEm->blockOffset + lastEm->range.size * 1024))
    {
        ostringstream oss;
        oss << "ExtentMap::setLocalHWM(): "
            "new HWM is past the end of the file for OID " << OID << "; partition " <<
            partitionNum << "; segment " << segmentNum << "; HWM " << newHWM;
        log(oss.str(), logging::LOG_TYPE_DEBUG);
        throw invalid_argument(oss.str());
    }

    // Save HWM in last extent for this segment file; and mark as AVAILABLE
    makeUndoRecordRBTree(UndoRecordType::DEFAULT, *lastEm);
    lastEm->HWM = newHWM;
    lastEm->status = EXTENTAVAILABLE;

    // Reset HWM in old HWM extent to 0
    if ((prevEm != nullptr) && (prevEm != lastEm))
    {
        makeUndoRecordRBTree(UndoRecordType::DEFAULT, *prevEm);
        prevEm->HWM = 0;
    }

#ifdef BRM_INFO

    if (firstNode)
    {
        ostringstream os;
        os << "ExtentMap::setLocalHWM(): firstLBID=" << fExtentMap[lastExtentIndex].range.start <<
           " lastLBID=" << fExtentMap[lastExtentIndex].range.start +
           fExtentMap[lastExtentIndex].range.size * 1024 - 1 << " newHWM=" << fExtentMap[lastExtentIndex].HWM
           << " min=" << fExtentMap[lastExtentIndex].partition.cprange.lo_val << " max=" <<
           fExtentMap[lastExtentIndex].partition.cprange.hi_val << " seq=" <<
           fExtentMap[lastExtentIndex].partition.cprange.sequenceNum << " status=";

        switch (fExtentMap[lastExtentIndex].partition.cprange.isValid)
        {
            case CP_INVALID:
                os << "invalid.";
                break;

            case CP_UPDATING:
                os << "updating.";
                break;

            case CP_VALID:
                os << "valid.";
                break;

            default:
                os << "unknown(!!).";
                break;
        }

        if (addedAnExtent)
            os << "  Data extended into a new extent.";

        log(os.str(), logging::LOG_TYPE_DEBUG);
    }
#endif
}

void ExtentMap::bulkSetHWM(const vector<BulkSetHWMArg>& v, bool firstNode)
{
    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);

    for (uint32_t i = 0; i < v.size(); i++)
        setLocalHWM(v[i].oid, v[i].partNum, v[i].segNum, v[i].hwm, firstNode, false);
}

class BUHasher
{
public:
    inline uint64_t operator()(const BulkUpdateDBRootArg& b) const
    {
        return b.startLBID;
    }
};

class BUEqual
{
public:
    inline bool operator()(const BulkUpdateDBRootArg& b1, const BulkUpdateDBRootArg& b2) const
    {
        return b1.startLBID == b2.startLBID;
    }
};

void ExtentMap::bulkUpdateDBRoot(const vector<BulkUpdateDBRootArg>& args)
{
    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);

    for (auto& lbidDbroot: args)
    {
        auto emIter = findByLBID(lbidDbroot.startLBID);
        emIter->second.dbRoot = lbidDbroot.dbRoot;
    }
}

void ExtentMap::getExtents(int OID, vector<struct EMEntry>& entries,
                           bool sorted, bool notFoundErr, bool incOutOfService)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("getExtents");
        TRACER_ADDINPUT(OID);
        TRACER_WRITE;
    }

#endif

    entries.clear();

    if (OID < 0)
    {
        ostringstream oss;
        oss << "ExtentMap::getExtents(): invalid OID requested: " << OID;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }

    grabEMEntryTable(READ);
    grabEMIndex(READ);
    // Artibtrary sized constant
    entries.reserve(100);

    DBRootVec dbRootVec(getAllDbRoots());

    for (auto dbRoot: dbRootVec)
    {
        const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID);
        entries.reserve(entries.size() + lbids.size());
        const auto emIdents = getEmIdentsByLbids(lbids);
        for (auto& emEntry : emIdents)
        {
            if (incOutOfService)
            {
                entries.push_back(emEntry);
            }
            else
            {
                if (emEntry.status != EXTENTOUTOFSERVICE)
                    entries.push_back(emEntry);
            }
        }
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);

    if (sorted)
        sort<vector<struct EMEntry>::iterator>(entries.begin(), entries.end());
}

void ExtentMap::getExtents_dbroot(int OID, vector<struct EMEntry>& entries, const uint16_t dbroot)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("getExtents");
        TRACER_ADDINPUT(OID);
        TRACER_WRITE;
    }

#endif

#ifdef EM_AS_A_TABLE_POC__

    if (OID == 1084)
    {
        EMEntry fakeEntry;
        fakeEntry.range.start = (1LL << 54);
        fakeEntry.range.size = 4;
        fakeEntry.fileID = 1084;
        fakeEntry.blockOffset = 0;
        fakeEntry.HWM = 1;
        fakeEntry.partitionNum = 0;
        fakeEntry.segmentNum = 0;
        fakeEntry.dbRoot = 1;
        fakeEntry.colWid = 4;
        fakeEntry.status = EXTENTAVAILABLE;
        fakeEntry.partition.cprange.hi_val = numeric_limits<int64_t>::min() + 2;
        fakeEntry.partition.cprange.lo_val = numeric_limits<int64_t>::max();
        fakeEntry.partition.cprange.sequenceNum = 0;
        fakeEntry.partition.cprange.isValid = CP_INVALID;
        entries.push_back(fakeEntry);
        return;
    }

#endif

    entries.clear();
    if (OID < 0)
    {
        ostringstream oss;
        oss << "ExtentMap::getExtents(): invalid OID requested: " << OID;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    const auto lbids = fPExtMapIndexImpl_->find(dbroot, OID);
    auto emIdents = getEmIdentsByLbids(lbids);
    entries.swap(emIdents);

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
}

//------------------------------------------------------------------------------
// Get the number of extents for the specified OID and DBRoot.
// OutOfService extents are included/excluded depending on the
// value of the incOutOfService flag.
//------------------------------------------------------------------------------
void ExtentMap::getExtentCount_dbroot(int OID, uint16_t dbroot, bool incOutOfService, uint64_t& numExtents)
{
    if (OID < 0)
    {
        ostringstream oss;
        oss << "ExtentMap::getExtentsCount_dbroot(): invalid OID requested: " <<
            OID;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    const auto lbids = fPExtMapIndexImpl_->find(dbroot, OID);
    if (!incOutOfService)
    {
        const auto emIdents = getEmIdentsByLbids(lbids);
        for (auto& emEntry : emIdents)
        {
            if (emEntry.status != EXTENTOUTOFSERVICE)
            {
                ++numExtents;
            }
        }
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
    numExtents = (incOutOfService) ? lbids.size() : numExtents;
}

//------------------------------------------------------------------------------
// Gets the DBRoot for the specified system catalog OID.
// Function assumes the specified System Catalog OID is fully contained on
// a single DBRoot, as the function only searches for and returns the first
// DBRoot entry that is found in the extent map.
//------------------------------------------------------------------------------
void ExtentMap::getSysCatDBRoot(OID_t oid, uint16_t& dbRoot)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("getSysCatDBRoot");
        TRACER_ADDINPUT(oid);
        TRACER_ADDSHORTOUTPUT(dbRoot);
        TRACER_WRITE;
    }

#endif

    bool bFound = false;
    grabEMEntryTable(READ);
    grabEMIndex(READ);

    DBRootVec dbRootVec(getAllDbRoots());
    for (auto localDbRoot: dbRootVec)
    {
        const auto lbids = fPExtMapIndexImpl_->find(localDbRoot, oid);
        if (!lbids.empty())
        {
            auto emIt = findByLBID(lbids[0]);
            dbRoot = emIt->second.dbRoot;
            bFound = true;
            break;
        }
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);

    if (!bFound)
    {
        ostringstream oss;
        oss << "ExtentMap::getSysCatDBRoot(): OID not found: " << oid;
        log(oss.str(), logging::LOG_TYPE_WARNING);
        throw logic_error(oss.str());
    }
}

//------------------------------------------------------------------------------
// Delete all extents for the specified OID(s) and partition number.
// @bug 5237 - Removed restriction that prevented deletion of segment files in
//             the last partition (for a DBRoot).
//------------------------------------------------------------------------------
void ExtentMap::deletePartition(const set<OID_t>& oids,
                                const set<LogicalPartition>& partitions, string& emsg)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITENOW("deletePartition");
        ostringstream oss;
        set<LogicalPartition>::const_iterator partIt;
        oss << "partitions: ";
        for (partIt = partitions.begin(); partIt != partitions.end(); ++partIt)
            oss << (*partIt) << " ";

        oss << endl;
        oss << "OIDS: ";
        set<OID_t>::const_iterator it;

        for (it = oids.begin(); it != oids.end(); ++it)
        {
            oss << (*it) << ", ";
        }

        TRACER_WRITEDIRECT(oss.str());
    }

#endif

    if (oids.size() == 0)
        return;

    int rc = 0;

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
    grabFreeList(WRITE);

    std::set<LogicalPartition> foundPartitions;
    std::vector<ExtentMapRBTree::iterator> extents;
    for (auto oid: oids)
    {
        for (auto& partition: partitions)
        {
            const auto lbids = fPExtMapIndexImpl_->find(partition.dbroot, oid, partition.pp);
            auto emIters = getEmIteratorsByLbids(lbids);
            for (auto& emIter : emIters)
            {
                const auto& emEntry = emIter->second;
                if (emEntry.segmentNum != partition.seg)
                {
                    continue;
                }
                LogicalPartition lp(emEntry.dbRoot, emEntry.partitionNum, emEntry.segmentNum);
                foundPartitions.insert(lp);
                emIter = deleteExtent(emIter);
            }
        }
    }

    if (foundPartitions.size() != partitions.size())
    {
        Message::Args args;
        ostringstream oss;

        for (auto partIt = partitions.begin(), end = partitions.end(); partIt != end;
             ++partIt)
        {
            if (foundPartitions.find((*partIt)) == foundPartitions.end())
            {
                if (!oss.str().empty())
                    oss << ", ";
                oss << (*partIt).toString();
            }
        }

        args.add(oss.str());
        emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NOT_EXIST, args);
        rc = ERR_PARTITION_NOT_EXIST;
    }

    // This has to be the last error code to set and can not be over-written.
    if (foundPartitions.empty())
        rc = WARN_NO_PARTITION_PERFORMED;

    // @bug 4772 throw exception on any error because they are all warnings.
    if (rc)
        throw IDBExcept(emsg, rc);
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s) and partition
// number.
// @bug 5237 - Removed restriction that prevented deletion of segment files in
//             the last partition (for a DBRoot).
//------------------------------------------------------------------------------
void ExtentMap::markPartitionForDeletion(const set<OID_t>& oids, const set<LogicalPartition>& partitions,
                                         string& emsg)
{
#ifdef BRM_INFO
    if (fDebug)
    {
        TRACER_WRITENOW("markPartitionForDeletion");
        ostringstream oss;
        set<LogicalPartition>::const_iterator partIt;
        oss << "partitions: ";
        for (partIt = partitions.begin(); partIt != partitions.end(); ++partIt)
            oss << (*partIt) << " ";

        oss << endl;
        oss << "OIDS: ";
        set<OID_t>::const_iterator it;

        for (it = oids.begin(); it != oids.end(); ++it)
        {
            oss << (*it) << ", ";
        }

        TRACER_WRITEDIRECT(oss.str());
    }

#endif

    if (oids.size() == 0)
        return;

    int rc = 0;

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);

    set<LogicalPartition> foundPartitions;
    vector<ExtentMapRBTree::iterator> extents;
    bool partitionAlreadyDisabled = false;

    for (auto oid: oids)
    {
        for (auto& partition: partitions)
        {
            const auto lbids = fPExtMapIndexImpl_->find(partition.dbroot, oid, partition.pp);
            auto emIters = getEmIteratorsByLbids(lbids);
            for (auto& emIter : emIters)
            {
                const auto& emEntry = emIter->second;
                if ((emEntry.segmentNum != partition.seg) || emEntry.range.size == 0)
                {
                    continue;
                }
                if (emEntry.status == EXTENTOUTOFSERVICE)
                {
                    partitionAlreadyDisabled = true;
                }
                LogicalPartition lp(emEntry.dbRoot, emEntry.partitionNum, emEntry.segmentNum);
                foundPartitions.insert(lp);
                extents.push_back(emIter);
            }
        }
    }

    // really disable partitions
    for (uint32_t i = 0; i < extents.size(); i++)
    {
        makeUndoRecordRBTree(UndoRecordType::DEFAULT, extents[i]->second);
        extents[i]->second.status = EXTENTOUTOFSERVICE;
    }

    // validate against referencing non-existent logical partitions
    if (foundPartitions.size() != partitions.size())
    {
        Message::Args args;
        ostringstream oss;

        for (auto partIt = partitions.begin(); partIt != partitions.end(); ++partIt)
        {
            if (foundPartitions.find((*partIt)) == foundPartitions.end())
            {
                if (!oss.str().empty())
                    oss << ", ";

                oss << (*partIt).toString();
            }
        }

        args.add(oss.str());
        emsg =
            emsg + string("\n") + IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NOT_EXIST, args);
        rc = ERR_PARTITION_NOT_EXIST;
    }

    // check already disabled error now, which could be a non-error
    if (partitionAlreadyDisabled)
    {
        emsg = emsg + string("\n") +
               IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_ALREADY_DISABLED);
        rc = ERR_PARTITION_ALREADY_DISABLED;
    }

    // this rc has to be the last one set and can not be over-written by others.
    if (foundPartitions.empty())
    {
        rc = WARN_NO_PARTITION_PERFORMED;
    }

    // @bug 4772 throw exception on any error because they are all warnings.
    if (rc)
        throw IDBExcept(emsg, rc);
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s)
//------------------------------------------------------------------------------
void ExtentMap::markAllPartitionForDeletion(const set<OID_t>& oids)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITENOW("markPartitionForDeletion");
        ostringstream oss;
        oss << "OIDS: ";
        set<OID_t>::const_iterator it;

        for (it = oids.begin(); it != oids.end(); ++it)
        {
            oss << (*it) << ", ";
        }

        TRACER_WRITEDIRECT(oss.str());
    }

#endif

    if (oids.size() == 0)
        return;

    set<OID_t>::const_iterator it;

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);

    DBRootVec dbRootVec(getAllDbRoots());
    // It would be interesting to pick the smallest set(either oids or partitions) for the second loop.
    for (auto dbRoot: dbRootVec)
    {
        for (auto oid: oids)
        {
            const auto lbids = fPExtMapIndexImpl_->find(dbRoot, oid);
            auto emIters = getEmIteratorsByLbids(lbids);
            for (auto& emIter : emIters)
            {
                auto& emEntry = emIter->second;
                makeUndoRecordRBTree(UndoRecordType::DEFAULT, emEntry);
                emEntry.status = EXTENTOUTOFSERVICE;
            }
        }
    }
}

//------------------------------------------------------------------------------
// Restore all extents for the specified OID(s) and partition number.
//------------------------------------------------------------------------------
void ExtentMap::restorePartition(const set<OID_t>& oids,
                                 const set<LogicalPartition>& partitions, string& emsg)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITENOW("restorePartition");
        ostringstream oss;
        set<LogicalPartition>::const_iterator partIt;
		oss << "partitions: ";
		for (partIt=partitions.begin(); partIt!=partitions.end(); ++partIt)
			oss << (*partIt) << " ";

        oss << endl;
        oss << "OIDS: ";
        set<OID_t>::const_iterator it;

        for (it = oids.begin(); it != oids.end(); ++it)
        {
            oss << (*it) << ", ";
        }

        TRACER_WRITEDIRECT(oss.str());
    }

#endif

    if (oids.size() == 0)
        return;

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);

    vector<ExtentMapRBTree::iterator> extents;
    set<LogicalPartition> foundPartitions;
    bool partitionAlreadyEnabled = false;

    for (auto oid: oids)
    {
        for (auto& partition: partitions)
        {
            const auto lbids = fPExtMapIndexImpl_->find(partition.dbroot, oid, partition.pp);
            auto emIters = getEmIteratorsByLbids(lbids);
            for (auto& emIter : emIters)
            {
                const auto& emEntry = emIter->second;
                if (emEntry.segmentNum != partition.seg)
                {
                    continue;
                }
                if (emEntry.status == EXTENTAVAILABLE)
                {
                    partitionAlreadyEnabled = true;
                }
                LogicalPartition lp(emEntry.dbRoot, emEntry.partitionNum, emEntry.segmentNum);
                extents.push_back(emIter);
                foundPartitions.insert(lp);
            }
        }
    }

    if (foundPartitions.size() != partitions.size())
    {
        Message::Args args;
        ostringstream oss;

        for (auto partIt = partitions.begin(); partIt != partitions.end(); ++partIt)
        {
            if (foundPartitions.empty() || foundPartitions.find((*partIt)) == foundPartitions.end())
            {
                if (!oss.str().empty())
                    oss << ", ";

                oss << (*partIt).toString();
            }
        }

        args.add(oss.str());
        emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NOT_EXIST, args);
        throw IDBExcept(emsg, ERR_PARTITION_NOT_EXIST);
    }

    // really enable partitions
    for (uint32_t i = 0; i < extents.size(); i++)
    {
        makeUndoRecordRBTree(UndoRecordType::DEFAULT, extents[i]->second);
        extents[i]->second.status = EXTENTAVAILABLE;
    }

    if (partitionAlreadyEnabled)
    {
        emsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_ALREADY_ENABLED);
        throw IDBExcept(emsg, ERR_PARTITION_ALREADY_ENABLED);
    }
}

//------------------------------------------------------------------------------
// Return all the out-of-service partitions for the specified OID.
//------------------------------------------------------------------------------
void ExtentMap::getOutOfServicePartitions(OID_t oid, set<LogicalPartition>& partitions)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("getExtents");
        TRACER_ADDINPUT(oid);
        TRACER_WRITE;
    }

#endif

    partitions.clear();

    if (oid < 0)
    {
        ostringstream oss;
        oss << "ExtentMap::getOutOfServicePartitions(): "
            "invalid OID requested: " << oid;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    DBRootVec dbRootVec(getAllDbRoots());
    // It would be interesting to pick the smallest set(either oids or partitions) for the second loop.
    for (auto dbRoot: dbRootVec)
    {
        const auto lbids = fPExtMapIndexImpl_->find(dbRoot, oid);
        const auto emEntries = getEmIdentsByLbids(lbids);
        for (const auto& emEntry : emEntries)
        {
            if (emEntry.status == EXTENTOUTOFSERVICE)
            {
                LogicalPartition lp(emEntry.dbRoot, emEntry.partitionNum, emEntry.segmentNum);
                partitions.insert(lp);
            }
        }
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
}

//------------------------------------------------------------------------------
// Delete all extents for the specified dbroot
//------------------------------------------------------------------------------
void ExtentMap::deleteDBRoot(uint16_t dbroot)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITENOW("deleteDBRoot");
        ostringstream oss;
        oss << "dbroot: " << dbroot;
        TRACER_WRITEDIRECT(oss.str());
    }

#endif

    grabEMEntryTable(WRITE);
    grabEMIndex(WRITE);
    grabFreeList(WRITE);

    auto it = fExtentMapRBTree->begin();
    auto end = fExtentMapRBTree->end();

    while (it != end)
    {
        if (it->second.dbRoot == dbroot)
            it = deleteExtent(it);
        else
            ++it;
    }

    fPExtMapIndexImpl_->deleteDbRoot(dbroot);
}

//------------------------------------------------------------------------------
// Does the specified DBRoot have any extents.
// Throws exception if extentmap shared memory is not loaded.
//------------------------------------------------------------------------------
bool ExtentMap::isDBRootEmpty(uint16_t dbroot)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("isDBRootEmpty");
		TRACER_ADDINPUT(dbroot);
        TRACER_WRITE;
    }

#endif

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    if (fEMRBTreeShminfo->currentSize == 0)
    {
        throw runtime_error(
            "ExtentMap::isDBRootEmpty() shared memory not loaded");
    }

    bool bEmpty = fPExtMapIndexImpl_->isDBRootEmpty(dbroot);

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);

    return bEmpty;
}

void ExtentMap::lookup(OID_t OID, LBIDRange_v& ranges)
{
#ifdef BRM_INFO

    if (fDebug)
    {
        TRACER_WRITELATER("lookup");
        TRACER_ADDINPUT(OID);
        TRACER_WRITE;
    }

#endif

#ifdef EM_AS_A_TABLE_POC__

    if (OID == 1084)
    {
        EMEntry fakeEntry;
        fakeEntry.range.start = (1LL << 54);
        fakeEntry.range.size = 4;
#if 0
        fakeEntry.fileID = 1084;
        fakeEntry.blockOffset = 0;
        fakeEntry.HWM = 1;
        fakeEntry.partitionNum = 0;
        fakeEntry.segmentNum = 0;
        fakeEntry.dbRoot = 1;
        fakeEntry.colWid = 4;
        fakeEntry.status = EXTENTAVAILABLE;
        fakeEntry.partition.cprange.hi_val = numeric_limits<int64_t>::min() + 2;
        fakeEntry.partition.cprange.lo_val = numeric_limits<int64_t>::max();
        fakeEntry.partition.cprange.sequenceNum = 0;
        fakeEntry.partition.cprange.isValid = CP_INVALID;
#endif
        ranges.push_back(fakeEntry.range);
        return;
    }

#endif

    // LBIDRange tmp;
    ranges.clear();

    if (OID < 0)
    {
        ostringstream oss;
        oss << "ExtentMap::lookup(): invalid OID requested: " << OID;
        log(oss.str(), logging::LOG_TYPE_CRITICAL);
        throw invalid_argument(oss.str());
    }

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    DBRootVec dbRootVec(getAllDbRoots());

    for (auto dbRoot: dbRootVec)
    {
        const auto lbids = fPExtMapIndexImpl_->find(dbRoot, OID);
        const auto emIdents = getEmIdentsByLbids(lbids);
        for (auto& emEntry : emIdents)
        {
            if (emEntry.status != EXTENTOUTOFSERVICE)
                ranges.emplace_back(emEntry.range.start, emEntry.range.size * 1024);
        }
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
}

int ExtentMap::checkConsistency()
{
#ifdef BRM_INFO

    if (fDebug) TRACER_WRITENOW("checkConsistency");

#endif

    /*
     LBID space consistency checks
        1. verify that every LBID is either in the EM xor the freelist
                a. for every segment in the EM, make sure there is no overlapping entry in the FL
                b. scan both lists to verify that the entire space is represented
        2. verify that there are no adjacent entries in the freelist
     OID consistency
        3. make sure there are no gaps in the file offsets
        4. make sure that only the last extent has a non-zero HWM
     Struct integrity
        5. verify that the number of entries in each table is consistent with
                the recorded current size
        */

    LBID_t emBegin, emEnd, flBegin, flEnd;
    int i, j, flEntries;
    uint32_t usedEntries;

    grabEMEntryTable(READ);
    grabEMIndex(READ);

    try
    {
        grabFreeList(READ);
    }
    catch (...)
    {
        releaseEMIndex(READ);
        releaseEMEntryTable(READ);
        throw;
    }

    flEntries = fFLShminfo->allocdSize / sizeof(InlineLBIDRange);

    // test 1a - make sure every entry in the EM is not overlapped by an entry in the FL
    for (auto emIt = fExtentMapRBTree->begin(), end = fExtentMapRBTree->end(); emIt != end; ++emIt)
    {
        auto& emEntry = emIt->second;
        {
            emBegin = emEntry.range.start;
            emEnd = emBegin + (emEntry.range.size * 1024) - 1;

            for (j = 0; j < flEntries; j++)
            {
                if (fFreeList[j].size != 0)
                {
                    flBegin = fFreeList[j].start;
                    flEnd = flBegin + (fFreeList[j].size * 1024) - 1;

                    //em entry overlaps the beginning
                    //em entry is contained within
                    //em entry overlaps the end
                    if ((emBegin <= flBegin && emEnd >= flBegin) ||
                            (emBegin >= flBegin && emEnd <= flEnd) ||
                            (emBegin <= flEnd && emEnd >= flEnd))
                    {
                        cerr << "EM::checkConsistency(): Improper LBID allocation detected" << endl;
                        throw logic_error("EM checkConsistency test 1a (data structures are read-locked)");
                    }
                }
            }
        }
    }

    cout << "test 1a passed\n";

    //test 1b - verify that the entire LBID space is accounted for

    int lbid, oldlbid;

    lbid = 0;

    while (lbid < 67108864)      // 2^26  (2^36/1024)
    {
        oldlbid = lbid;

        for (i = 0; i < flEntries; i++)
        {
            if (fFreeList[i].start % 1024 != 0)
            {
                cerr << "EM::checkConsistency(): A freelist entry is not 1024-block aligned" << endl;
                throw logic_error("EM checkConsistency test 1b (data structures are read-locked)");
            }

            if (fFreeList[i].start / 1024 == lbid)
                lbid += fFreeList[i].size;
        }

        for (auto emIt = fExtentMapRBTree->begin(), end = fExtentMapRBTree->end(); emIt != end;
             ++emIt)
        {
            auto& emEntry = emIt->second;
            if (emEntry.range.start % 1024 != 0)
            {
                cerr << "EM::checkConsistency(): An extent map entry is not 1024-block aligned "
                     << i << " " << emEntry.range.start << endl;
                throw logic_error("EM checkConsistency test 1b (data structures are read-locked)");
            }

            if (emEntry.range.start / 1024 == lbid)
                lbid += emEntry.range.size;
        }

        if (oldlbid == lbid)
        {
            cerr << "EM::checkConsistency(): There is a gap in the LBID space at block #" <<
                 static_cast<uint64_t>(lbid * 1024) << endl;
            throw logic_error("EM checkConsistency test 1b (data structures are read-locked)");
        }
    }

    cout << "test 1b passed\n";

    // test 1c - verify that no dbroot is < 1
    bool errorOut = false;

    for (auto emIt = fExtentMapRBTree->begin(), end = fExtentMapRBTree->end(); emIt != end; ++emIt)
    {
        auto& emEntry = emIt->second;
        if (emEntry.range.size != 0)
        {
            //cout << "EM[" << i << "]: dbRoot=" << fExtentMap[i].dbRoot(listMan) << endl;
            if (emEntry.dbRoot == 0)
            {
                errorOut = true;
                cerr << "EM::checkConsistency(): index " << i << " has a 0 dbroot\n";
            }
        }
    }

    if (errorOut)
        throw logic_error("EM checkConsistency test 1c (data structures are read-locked)");

    cout << "test 1c passed\n";

#if 0  // a test ported from the tek2 branch, which requires a RID field to be stored; not relevant here
    // test 1d - verify that each <OID, RID> pair is unique
    cout << "Running test 1d\n";

    set<OIDRID> uniquer;

    for (i = 0; i < emEntries; i++)
    {
        if (fExtentMap[i].size != 0 && !fExtentMap[i].isDict())
        {
            OIDRID element(fExtentMap[i].fileID, fExtentMap[i].rid);

            if (uniquer.insert(element).second == false)
                throw logic_error("EM consistency test 1d failed (data structures are read-locked)");
        }
    }

    uniquer.clear();
    cout << "Test 1d passed\n";
#endif

    // test 2 - verify that the freelist is consolidated
    for (i = 0; i < flEntries; i++)
    {
        if (fFreeList[i].size != 0)
        {
            flEnd = fFreeList[i].start + (fFreeList[i].size * 1024);

            for (j = i + 1; j < flEntries; j++)
                if (fFreeList[j].size != 0 && fFreeList[j].start == flEnd)
                    throw logic_error("EM checkConsistency test 2 (data structures are read-locked)");
        }
    }

    cout << "test 2 passed\n";

// needs to be updated
#if 0
    // test 3 - scan the extent map to make sure files have no LBID gaps
    vector<OID_t> oids;
    vector< vector<uint32_t> > fbos;

    for (i = 0; i < emEntries; i++)
    {
        if (fExtentMap[i].size != 0)
        {
            for (j = 0; j < (int)oids.size(); j++)
                if (oids[j] == fExtentMap[i].fileID)
                    break;

            if (j == (int)oids.size())
            {
                oids.push_back(fExtentMap[i].fileID);
                fbos.push_back(vector<uint32_t>());
            }

            fbos[j].push_back(fExtentMap[i].blockOffset);
        }
    }

    for (i = 0; i < (int)fbos.size(); i++)
        sort<vector<uint32_t>::iterator>(fbos[i].begin(), fbos[i].end());

    const unsigned EXTENT_SIZE = getExtentSize();

    for (i = 0; i < (int)fbos.size(); i++)
    {
        for (j = 0; j < (int)fbos[i].size(); j++)
        {
            if (fbos[i][j] != static_cast<uint32_t>(j * EXTENT_SIZE))
            {
                cerr << "EM: OID " << oids[i] << " has no extent at FBO " <<
                     j* EXTENT_SIZE << endl;
                throw logic_error("EM checkConsistency test 3 (data structures are read-locked)");
            }
        }
    }

    fbos.clear();
    oids.clear();
#endif


    // test 5a - scan freelist to make sure the current size is accurate

    /*
    for (i = 0, usedEntries = 0; i < emEntries; i++)
        if (fExtentMap[i].range.size != 0)
            usedEntries++;

    if (usedEntries != fEMShminfo->currentSize / sizeof(EMEntry))
    {
        cerr << "checkConsistency: used extent map entries = " << usedEntries
             << " metadata says " << fEMShminfo->currentSize / sizeof(EMEntry)
             << endl;
        throw logic_error("EM checkConsistency test 5a (data structures are read-locked)");
    }
    */

    for (i = 0, usedEntries = 0; i < flEntries; i++)
        if (fFreeList[i].size != 0)
            usedEntries++;

    if (usedEntries != fFLShminfo->currentSize / sizeof(InlineLBIDRange))
    {
        cerr << "checkConsistency: used freelist entries = " << usedEntries
             << " metadata says " << fFLShminfo->currentSize / sizeof(InlineLBIDRange)
             << endl;
        throw logic_error("EM checkConsistency test 5a (data structures are read-locked)");
    }

    cout << "test 5a passed\n";

    releaseFreeList(READ);
    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
    return 0;
}

void ExtentMap::setReadOnly()
{
    r_only = true;
}

void ExtentMap::undoChanges()
{
#ifdef BRM_INFO

    if (fDebug) TRACER_WRITENOW("undoChanges");

#endif
    Undoable::undoChanges();
    undoChangesRBTree();
    finishChanges();
}

void ExtentMap::confirmChanges()
{
#ifdef BRM_INFO

    if (fDebug) TRACER_WRITENOW("confirmChanges");

#endif
    Undoable::confirmChanges();
    confirmChangesRBTree();
    finishChanges();
}

void ExtentMap::finishChanges()
{
    if (flLocked)
        releaseFreeList(WRITE);

    if (emIndexLocked)
        releaseEMIndex(WRITE);

    if (emLocked)
        releaseEMEntryTable(WRITE);
}

void ExtentMap::makeUndoRecordRBTree(UndoRecordType type, const EMEntry& emEntry)
{
    undoRecordsRBTree.push_back(make_pair(type, emEntry));
}

void ExtentMap::undoChangesRBTree()
{
    for (const auto& undoPair : undoRecordsRBTree)
    {
        if (undoPair.first == UndoRecordType::INSERT)
        {
            const auto key = undoPair.second.range.start;
            auto emIt = findByLBID(key);
            if (emIt != fExtentMapRBTree->end())
            {
                fExtentMapRBTree->erase(emIt);
            }
        }
        else if (undoPair.first == UndoRecordType::DELETE)
        {
            const auto& emEntry = undoPair.second;
            fExtentMapRBTree->insert(make_pair(emEntry.range.start, emEntry));
        }
        else
        {
            const auto key = undoPair.second.range.start;
            auto emIt = findByLBID(key);
            if (emIt != fExtentMapRBTree->end())
            {
                emIt->second = undoPair.second;
            }
        }
    }
}

void ExtentMap::confirmChangesRBTree() { undoRecordsRBTree.clear(); }

const bool* ExtentMap::getEMFLLockStatus()
{
    return &flLocked;
}

const bool* ExtentMap::getEMLockStatus()
{
    return &emLocked;
}

const bool* ExtentMap::getEMIndexLockStatus()
{
    return &emIndexLocked;
}

//------------------------------------------------------------------------------
// Reload Config cache if config file time stamp has changed
//------------------------------------------------------------------------------
void ExtentMap::checkReloadConfig()
{
    config::Config* cf = config::Config::makeConfig();

    // Immediately return if Columnstore.xml timestamp has not changed
    if (cf->getCurrentMTime() == fCacheTime)
        return;

    //--------------------------------------------------------------------------
    // Initialize outdated attribute still used by primitiveserver.
    // Hardcode to 8K for now, since that's all we support.
    //--------------------------------------------------------------------------
    ExtentSize = 0x2000;

//	string es = cf->getConfig("ExtentMap", "ExtentSize");
//	if (es.length() == 0) es = "8K";
//	if (es == "8K" || es == "8k")
//	{
//		ExtentSize = 0x2000;
//	}
//	else if (es == "1K" || es == "1k")
//	{
//		ExtentSize = 0x400;
//	}
//	else if (es == "64K" || es == "64k")
//	{
//		ExtentSize = 0x10000;
//	}
//	else
//	{
//		throw logic_error("Invalid ExtentSize found in config file!");
//	}

    //--------------------------------------------------------------------------
    // Initialize number of rows per extent
    // Hardcode to 8M for now, since that's all we support.
    //--------------------------------------------------------------------------
    ExtentRows = 0x800000;

//	string er = cf->getConfig("ExtentMap", "ExtentRows");
//	if (er.length() == 0) er = "8M";
//	if (er == "8M" || er == "8m")
//	{
//		ExtentRows = 0x800000;
//	}
//	else if (er == "1M" || er == "1m")
//	{
//		ExtentRows = 0x100000;
//	}
//	else if (er == "64M" || er == "64m")
//	{
//		ExtentRows = 0x4000000;
//	}
//	else
//	{
//		throw logic_error("Invalid ExtentRows found in config file!");
//	}

    //--------------------------------------------------------------------------
    // Initialize segment files per physical partition
    //--------------------------------------------------------------------------
    string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
    filesPerColumnPartition = cf->uFromText(fpc);

    if (filesPerColumnPartition == 0)
        filesPerColumnPartition = 4;

    // Get latest Columnstore.xml timestamp after first access forced a reload
    fCacheTime = cf ->getLastMTime();

    //--------------------------------------------------------------------------
    // Initialize extents per segment file
    //--------------------------------------------------------------------------
    string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
    extentsPerSegmentFile = cf->uFromText(epsf);

    if (extentsPerSegmentFile == 0)
        extentsPerSegmentFile = 2;
}

//------------------------------------------------------------------------------
// Returns the number of extents in a segment file.
// Mutex lock and call to checkReloadConfig() not currently necessary since,
// going with hardcoded value.  See checkReloadConfig().
//------------------------------------------------------------------------------
unsigned ExtentMap::getExtentSize()       // dmc-should deprecate
{
//	boost::mutex::scoped_lock lk(fConfigCacheMutex);
//	checkReloadConfig( );

    ExtentSize = 0x2000;
    return ExtentSize;
}

//------------------------------------------------------------------------------
// Returns the number or rows per extent.  Only supported values are 1m, 8m,
// and 64m.
// Mutex lock and call to checkReloadConfig() not currently necessary since,
// going with hardcoded value.  See checkReloadConfig().
//------------------------------------------------------------------------------
unsigned ExtentMap::getExtentRows()
{
//	boost::mutex::scoped_lock lk(fConfigCacheMutex);
//	checkReloadConfig( );

    ExtentRows = 0x800000;
    return ExtentRows;
}

//------------------------------------------------------------------------------
// Returns the number of column segment files for an OID, that make up a
// partition.
//------------------------------------------------------------------------------
unsigned ExtentMap::getFilesPerColumnPartition()
{
    boost::mutex::scoped_lock lk(fConfigCacheMutex);
    checkReloadConfig( );

    return filesPerColumnPartition;
}

//------------------------------------------------------------------------------
// Returns the number of extents in a segment file.
//------------------------------------------------------------------------------
unsigned ExtentMap::getExtentsPerSegmentFile()
{
    boost::mutex::scoped_lock lk(fConfigCacheMutex);
    checkReloadConfig( );

    return extentsPerSegmentFile;
}

//------------------------------------------------------------------------------
// Returns the number of DBRoots to be used in storing db column files.
//------------------------------------------------------------------------------
unsigned ExtentMap::getDbRootCount()
{
    oam::OamCache* oamcache = oam::OamCache::makeOamCache();
    unsigned int rootCnt = oamcache->getDBRootCount();

    return rootCnt;
}

//------------------------------------------------------------------------------
// Get list of DBRoots that map to the specified PM.  DBRoot list is cached
// internally in fPmDbRootMap after getting from Columnstore.xml via OAM.
//------------------------------------------------------------------------------
void ExtentMap::getPmDbRoots( int pm, vector<int>& dbRootVec )
{
    oam::OamCache* oamcache = oam::OamCache::makeOamCache();
    oam::OamCache::PMDbrootsMap_t pmDbroots = oamcache->getPMToDbrootsMap();

    dbRootVec.clear();
    dbRootVec = (*pmDbroots)[pm];
}

DBRootVec ExtentMap::getAllDbRoots()
{
    DBRootVec dbRootResultVec;
    oam::OamCache* oamcache = oam::OamCache::makeOamCache();
    // NB The routine uses int for dbroot id that contradicts with the type used here, namely uint16_t
    oam::OamCache::PMDbrootsMap_t pmDbroots = oamcache->getPMToDbrootsMap();
    auto& pmDbrootsRef = *pmDbroots;

    for (auto& pmDBRootPair: pmDbrootsRef)
    {
        for(auto dbRootId: pmDBRootPair.second)
            dbRootResultVec.push_back(dbRootId);
    }
    return dbRootResultVec;
}

vector<InlineLBIDRange> ExtentMap::getFreeListEntries()
{
    vector<InlineLBIDRange> v;
    grabEMEntryTable(READ);
    grabEMIndex(READ);
    grabFreeList(READ);

    int allocdSize = fFLShminfo->allocdSize / sizeof(InlineLBIDRange);

    for (int i = 0; i < allocdSize; i++)
        v.push_back(fFreeList[i]);

    releaseFreeList(READ);
    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
    return v;
}

void ExtentMap::dumpTo(ostream& os)
{
    grabEMEntryTable(READ);
    grabEMIndex(READ);

    for (auto emIt = fExtentMapRBTree->begin(), end = fExtentMapRBTree->end(); emIt != end; ++emIt)
    {
        const auto& emEntry = emIt->second;
        {
            os << emEntry.range.start << '|'
               << emEntry.range.size << '|'
               << emEntry.fileID << '|'
               << emEntry.blockOffset << '|'
               << emEntry.HWM << '|'
               << emEntry.partitionNum << '|'
               << emEntry.segmentNum << '|'
               << emEntry.dbRoot << '|'
               << emEntry.colWid << '|'
               << emEntry.status << '|'
               << emEntry.partition.cprange.hi_val << '|'
               << emEntry.partition.cprange.lo_val << '|'
               << emEntry.partition.cprange.sequenceNum << '|'
               << (int)emEntry.partition.cprange.isValid << '|'
               << endl;
        }
    }

    releaseEMIndex(READ);
    releaseEMEntryTable(READ);
}

size_t ExtentMap::EMIndexShmemSize()
{
    grabEMIndex(READ);
    size_t EMIndexShmemSize = fPExtMapIndexImpl_->getShmemSize();
    releaseEMIndex(READ);
    return EMIndexShmemSize;
}

size_t ExtentMap::EMIndexShmemFree()
{
    grabEMIndex(READ);
    size_t EMIndexShmemFree = fPExtMapIndexImpl_->getShmemFree();
    releaseEMIndex(READ);
    return EMIndexShmemFree;
}

}	//namespace
// vim:ts=4 sw=4:

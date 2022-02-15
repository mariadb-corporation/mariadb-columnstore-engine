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

/******************************************************************************
 * $Id: extentmap.h 1936 2013-07-09 22:10:29Z dhall $
 *
 *****************************************************************************/

/** @file
 * class ExtentMap
 */

#ifndef _EXTENTMAP_H_
#define _EXTENTMAP_H_

#include <sys/types.h>
#include <vector>
#include <set>
#include <unordered_map>
#include <tr1/unordered_map>
#include <mutex>

//#define NDEBUG
#include <cassert>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/unordered_map.hpp>
#include <boost/functional/hash.hpp> //boost::hash

#include "shmkeys.h"
#include "brmtypes.h"
#include "mastersegmenttable.h"
#include "undoable.h"

#include "brmshmimpl.h"
#include "exceptclasses.h"

#ifdef NONE
#undef NONE
#endif
#ifdef READ
#undef READ
#endif
#ifdef WRITE
#undef WRITE
#endif

#if defined(_MSC_VER) && defined(xxxEXTENTMAP_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace bi = boost::interprocess;

namespace oam
{
typedef std::vector<uint16_t> DBRootConfigList;
}

namespace idbdatafile
{
class IDBDataFile;
}

namespace BRM
{

using PartitionNumberT = uint32_t;
using DBRootT = uint16_t;
using SegmentT = uint16_t;
using LastExtentIndexT = int;
using EmptyEMEntry = int;
using HighestOffset = uint32_t;
using LastIndEmptyIndEmptyInd = std::pair<LastExtentIndexT, EmptyEMEntry>;
using DBRootVec = std::vector<DBRootT>;

// assumed column width when calculating dictionary store extent size
#define DICT_COL_WIDTH 8

// valid values for EMEntry.status
const int16_t EXTENTSTATUSMIN(0); // equal to minimum valid status value
const int16_t EXTENTAVAILABLE(0);
const int16_t EXTENTUNAVAILABLE(1);
const int16_t EXTENTOUTOFSERVICE(2);
const int16_t EXTENTSTATUSMAX(2); // equal to maximum valid status value

enum partition_type_enum
{
    PART_DEFAULT = 0,
    PART_CASUAL,
    PART_RANGE
};
typedef partition_type_enum EMPartitionType_t;
typedef int64_t RangePartitionData_t;

const char CP_INVALID = 0;
const char CP_UPDATING = 1;
const char CP_VALID = 2;

struct EMCasualPartition_struct
{
    RangePartitionData_t hi_val;	// This needs to be reinterpreted as unsigned for uint64_t column types.
    RangePartitionData_t lo_val;
    int32_t sequenceNum;
    char isValid; //CP_INVALID - No min/max and no DML in progress. CP_UPDATING - Update in progress. CP_VALID- min/max is valid
    EXPORT EMCasualPartition_struct();
    EXPORT EMCasualPartition_struct(const int64_t lo, const int64_t hi, const int32_t seqNum);
    EXPORT EMCasualPartition_struct(const EMCasualPartition_struct& em);
    EXPORT EMCasualPartition_struct& operator= (const EMCasualPartition_struct& em);
};
typedef EMCasualPartition_struct EMCasualPartition_t;

struct EMPartition_struct
{
    EMCasualPartition_t		cprange;
};
typedef EMPartition_struct EMPartition_t;
struct EMEntry
{
    InlineLBIDRange range;
    int         fileID;
    uint32_t    blockOffset;
    HWM_t       HWM;
    PartitionNumberT	partitionNum; // starts at 0
    uint16_t	segmentNum;   // starts at 0
    DBRootT     dbRoot;       // starts at 1 to match Columnstore.xml
    uint16_t	colWid;
    int16_t 	status;       //extent avail for query or not, or out of service
    EMPartition_t partition;
    EXPORT EMEntry();
    EXPORT EMEntry(const EMEntry&);
    EXPORT EMEntry& operator= (const EMEntry&);
    EXPORT bool operator< (const EMEntry&) const;
};

// Bug 2989, moved from joblist
struct ExtentSorter
{
    bool operator()(const EMEntry& e1, const EMEntry& e2)
    {
        if (e1.dbRoot < e2.dbRoot)
            return true;

        if (e1.dbRoot == e2.dbRoot && e1.partitionNum < e2.partitionNum)
            return true;

        if (e1.dbRoot == e2.dbRoot && e1.partitionNum == e2.partitionNum && e1.blockOffset < e2.blockOffset)
            return true;

        if (e1.dbRoot == e2.dbRoot && e1.partitionNum == e2.partitionNum && e1.blockOffset == e2.blockOffset && e1.segmentNum < e2.segmentNum)
            return true;

        return false;
    }
};

using ShmSegmentManagerT = bi::managed_shared_memory::segment_manager;
using ShmVoidAllocator = bi::allocator<void, ShmSegmentManagerT>;

using ExtentMapIdxT = size_t;
using LBID_tAlloc = bi::allocator<LBID_t, ShmSegmentManagerT>;
using PartitionNumberTAlloc = bi::allocator<PartitionNumberT, ShmSegmentManagerT>;
using LBID_tVectorT = std::vector<LBID_t, LBID_tAlloc>;

using PartitionIndexContainerKeyT = PartitionNumberT;
using PartitionIndexContainerValT = std::pair<const PartitionIndexContainerKeyT, LBID_tVectorT>;
using PartitionIndexContainerValTAlloc =
    bi::allocator<PartitionIndexContainerValT, ShmSegmentManagerT>;
// Can't use std::unordered_map presumably b/c the map's pointer type doesn't use offset_type as boost::u_map does
using PartitionIndexContainerT = boost::unordered_map<
    PartitionIndexContainerKeyT, LBID_tVectorT, boost::hash<PartitionIndexContainerKeyT>,
    std::equal_to<PartitionIndexContainerKeyT>, PartitionIndexContainerValTAlloc>;

using OIDIndexContainerKeyT = OID_t;
using OIDIndexContainerValT = std::pair<const OIDIndexContainerKeyT, PartitionIndexContainerT>;
using OIDIndexContainerValTAlloc = bi::allocator<OIDIndexContainerValT, ShmSegmentManagerT>;
using OIDIndexContainerT =
    boost::unordered_map<OIDIndexContainerKeyT, PartitionIndexContainerT,
                         boost::hash<OIDIndexContainerKeyT>, std::equal_to<OIDIndexContainerKeyT>,
                         OIDIndexContainerValTAlloc>;

using DBRootIndexTAlloc = bi::allocator<OIDIndexContainerT, ShmSegmentManagerT>;
using DBRootIndexContainerT = std::vector<OIDIndexContainerT, DBRootIndexTAlloc>;
using ExtentMapIndex = DBRootIndexContainerT;
using LBID_tFindResult = std::vector<LBID_t>;
using InsertUpdateShmemKeyPair = std::pair<bool, bool>;


// RBTREE.
using EMEntryKeyValueType = std::pair<const int64_t, EMEntry>;
using VoidAllocator =
    boost::interprocess::allocator<void, boost::interprocess::managed_shared_memory::segment_manager>;
using EMEntryKeyValueTypeAllocator =
    boost::interprocess::allocator<EMEntryKeyValueType,
                                   boost::interprocess::managed_shared_memory::segment_manager>;
using ExtentMapRBTree =
    boost::interprocess::map<int64_t, EMEntry, std::less<int64_t>, EMEntryKeyValueTypeAllocator>;


class ExtentMapRBTreeImpl
{
  public:
    ~ExtentMapRBTreeImpl() = default;

    static ExtentMapRBTreeImpl* makeExtentMapRBTreeImpl(unsigned key, off_t size, bool readOnly = false);

    static void refreshShm()
    {
        if (fInstance)
        {
            delete fInstance;
            fInstance = NULL;
        }
    }

    inline void grow(unsigned key, off_t size) { fManagedShm.grow(key, size); }

    inline unsigned key() const { return fManagedShm.key(); }

    inline ExtentMapRBTree* get() const
    {
        VoidAllocator allocator(fManagedShm.fShmSegment->get_segment_manager());
        return fManagedShm.fShmSegment->find_or_construct<ExtentMapRBTree>("EmMapRBTree")(
            std::less<LBID_t>(), allocator);
    }

    inline uint64_t getFreeMemory() const { return fManagedShm.fShmSegment->get_free_memory(); }
    inline uint64_t getSize() const { return fManagedShm.fShmSegment->get_size(); }

  private:
    ExtentMapRBTreeImpl(unsigned key, off_t size, bool readOnly = false);
    ExtentMapRBTreeImpl(const ExtentMapRBTreeImpl& rhs);
    ExtentMapRBTreeImpl& operator=(const ExtentMapRBTreeImpl& rhs);

    BRMManagedShmImplRBTree fManagedShm;

    static boost::mutex fInstanceMutex;
    static ExtentMapRBTreeImpl* fInstance;
};

class FreeListImpl
{
public:
    ~FreeListImpl(){};

    static FreeListImpl* makeFreeListImpl(unsigned key, off_t size, bool readOnly = false);
    static void refreshShm()
    {
        if (fInstance)
        {
            delete fInstance;
            fInstance = NULL;
        }
    }

    inline void grow(unsigned key, off_t size)
#ifdef NDEBUG
    {
        fFreeList.grow(key, size);
    }
#else
    {
        int rc = fFreeList.grow(key, size);
        idbassert(rc == 0);
    }
#endif
    inline void makeReadOnly()
    {
        fFreeList.setReadOnly();
    }
    inline void clear(unsigned key, off_t size)
    {
        fFreeList.clear(key, size);
    }
    inline void swapout(BRMShmImpl& rhs)
    {
        fFreeList.swap(rhs);
        rhs.destroy();
    }
    inline unsigned key() const
    {
        return fFreeList.key();
    }

    inline InlineLBIDRange* get() const
    {
        return reinterpret_cast<InlineLBIDRange*>(fFreeList.fMapreg.get_address());
    }

private:
    FreeListImpl(unsigned key, off_t size, bool readOnly = false);
    FreeListImpl(const FreeListImpl& rhs);
    FreeListImpl& operator=(const FreeListImpl& rhs);

    BRMShmImpl fFreeList;

    static boost::mutex fInstanceMutex;
    static FreeListImpl* fInstance;
};

class ExtentMapIndexImpl
{
public:
    ~ExtentMapIndexImpl(){};

    static ExtentMapIndexImpl* makeExtentMapIndexImpl(unsigned key, off_t size, bool readOnly = false);
    static void refreshShm()
    {
        if (fInstance_)
        {
            delete fInstance_;
            fInstance_ = nullptr;
        }
    }

    // The multipliers and constants here are pure theoretical
    // tested using customer's data.
    static size_t estimateEMIndexSize(uint32_t numberOfExtents)
    {
        // These are just educated guess values to calculate initial
        // managed shmem size.
        constexpr const size_t tablesNumber_ = 100ULL;
        constexpr const size_t columnsNumber_ = 200ULL;
        constexpr const size_t dbRootsNumber_ = 3ULL;
        constexpr const size_t filesInPartition_ = 4ULL;
        constexpr const size_t extentsInPartition_ = filesInPartition_ * 2;
        return numberOfExtents * emIdentUnitSize_ +
            numberOfExtents / extentsInPartition_ * partitionContainerUnitSize_ +
            dbRootsNumber_ * tablesNumber_ * columnsNumber_;
    }

    bool growIfNeeded(const size_t memoryNeeded);

    inline void grow(off_t size)
    {
        int rc = fBRMManagedShmMemImpl_.grow(size);
        idbassert(rc == 0);
    }
    // After this call one needs to refresh any refs or ptrs sourced
    // from this shmem.
    inline void makeReadOnly()
    {
        fBRMManagedShmMemImpl_.setReadOnly();
    }

    inline void swapout(BRMManagedShmImpl& rhs)
    {
        fBRMManagedShmMemImpl_.swap(rhs);
    }

    inline unsigned key() const
    {
        return fBRMManagedShmMemImpl_.key();
    }

    unsigned getShmemSize()
    {
        return fBRMManagedShmMemImpl_.getManagedSegment()->get_size();
    }

    size_t getShmemFree()
    {
        return fBRMManagedShmMemImpl_.getManagedSegment()->get_free_memory();
    }

    unsigned getShmemImplSize()
    {
        return fBRMManagedShmMemImpl_.size();
    }

    void createExtentMapIndexIfNeeded();
    ExtentMapIndex* get();
    // Insert functions.
    InsertUpdateShmemKeyPair insert(const EMEntry& emEntry, const LBID_t lbid);
    InsertUpdateShmemKeyPair insert2ndLayerWrapper(OIDIndexContainerT& oids, const EMEntry& emEntry,
                                                   const LBID_t lbid, const bool aShmemHasGrown);
    InsertUpdateShmemKeyPair insert2ndLayer(OIDIndexContainerT& oids, const EMEntry& emEntry,
                                            const LBID_t lbid, const bool aShmemHasGrown);
    InsertUpdateShmemKeyPair insert3dLayerWrapper(PartitionIndexContainerT& partitions,
                                                  const EMEntry& emEntry, const LBID_t lbid,
                                                  const bool aShmemHasGrown);
    InsertUpdateShmemKeyPair insert3dLayer(PartitionIndexContainerT& partitions,
                                           const EMEntry& emEntry, const LBID_t lbid,
                                           const bool aShmemHasGrown);
    // Search functions.
    LBID_tFindResult find(const DBRootT dbroot, const OID_t oid,
        const PartitionNumberT partitionNumber);
    LBID_tFindResult find(const DBRootT dbroot, const OID_t oid);
    LBID_tFindResult search2ndLayer(OIDIndexContainerT& oids, const OID_t oid,
        const PartitionNumberT partitionNumber);
    LBID_tFindResult search2ndLayer(OIDIndexContainerT& oids, const OID_t oid);
    LBID_tFindResult search3dLayer(PartitionIndexContainerT& partitions,
        const PartitionNumberT partitionNumber);
    // Delete functions.
    void deleteDbRoot(const DBRootT dbroot);
    void deleteOID(const DBRootT dbroot, const OID_t oid);
    void deleteEMEntry(const EMEntry& emEntry, const ExtentMapIdxT emIdent);
private:
    BRMManagedShmImpl fBRMManagedShmMemImpl_;
    ExtentMapIndexImpl(unsigned key, off_t size, bool readOnly = false);
    ExtentMapIndexImpl(const ExtentMapIndexImpl& rhs);
    ExtentMapIndexImpl& operator=(const ExtentMapIndexImpl& rhs);

    static std::mutex fInstanceMutex_;
    static ExtentMapIndexImpl* fInstance_;
    static const constexpr uint32_t dbRootContainerUnitSize_ = 64ULL;
    static const constexpr uint32_t oidContainerUnitSize_ = 352ULL; // 2 * map overhead
    static const constexpr uint32_t partitionContainerUnitSize_ = 368ULL; // single map overhead
    static const constexpr uint32_t emIdentUnitSize_ = sizeof(uint64_t);
    static const constexpr uint32_t extraUnits_ = 2;
    static const constexpr size_t freeSpaceThreshold_ = 256 * 1024;
};

/** @brief This class encapsulates the extent map functionality of the system
 *
 * This class encapsulates the extent map functionality of the system.  It
 * is currently implemented in the quickest-to-write (aka dumb) way to
 * get something working into the hands of the other developers ASAP.
 * The Extent Map shared data should be implemented in a more scalable
 * structure such as a tree or hash table.
 */
class ExtentMap : public Undoable
{
public:
    EXPORT ExtentMap();
    EXPORT ~ExtentMap();

    /** @brief Loads the ExtentMap entries from a file
     *
     * Loads the ExtentMap entries from a file.  This will
     * clear out any existing entries.  The intention is that before
     * the system starts, an external tool instantiates a single Extent
     * Map and loads the stored entries.
     * @param filename The file to load from.
     * @note Throws an ios_base::failure exception on an IO error, runtime_error
     * if the file "looks" bad.
     */
    EXPORT void load(const std::string& filename, bool fixFL = false);

    /** @brief Saves the ExtentMap entries to a file
     *
     * Saves the ExtentMap entries to a file.
     * @param filename The file to save to.
     */
    EXPORT void save(const std::string& filename);

    // @bug 1509.  Added new version of lookup below.
    /** @brief Returns the first and last LBID in the range for a given LBID
     *
     * Get the first and last LBID for the extent that contains the given LBID.
     * @param LBID       (in) The lbid to search for
     * @param firstLBID (out) The first lbid for the extent
     * @param lastLBID  (out) the last lbid for the extent
     * @return 0 on success, -1 on error
     */
    EXPORT int lookup(LBID_t LBID, LBID_t& firstLBID, LBID_t& lastLBID);

    // @bug 1055+.  New functions added for multiple files per OID enhancement.

    /** @brief Look up the OID and file block offset assiciated with an LBID
     *
     * Look up the OID and file block offset assiciated with an LBID
     * @param LBID (in) The lbid to search for
     * @param OID (out) The OID associated with lbid
     * @param dbRoot (out) The db root containing the LBID
     * @param partitionNum (out) The partition containing the LBID
     * @param segmentNum (out) The segment containing the LBID
     * @param fileBlockOffset (out) The file block offset associated
     * with LBID
     * @return 0 on success, -1 on error
     */
    EXPORT int lookupLocal(LBID_t LBID, int& OID, uint16_t& dbRoot, uint32_t& partitionNum, uint16_t& segmentNum, uint32_t& fileBlockOffset);

    /** @brief Look up the LBID associated with a given OID, offset, partition, and segment.
     *
     * Look up the LBID associated with a given OID, offset, partition, and segment.
     * @param OID (in) The OID to look up
     * @param fileBlockOffset (in) The file block offset
     * @param partitionNum (in) The partition containing the lbid
     * @param segmentNum (in) The segement containing the lbid
     * @param LBID (out) The LBID associated with the given offset of the OID.
     * @return 0 on success, -1 on error
     */
    EXPORT int lookupLocal(int OID, uint32_t partitionNum, uint16_t segmentNum, uint32_t fileBlockOffset, LBID_t& LBID);

    /** @brief Look up the LBID associated with a given dbroot, OID, offset,
     * partition, and segment.
     *
     * Look up LBID associated with a given OID, offset, partition, and segment.
     * @param OID (in) The OID to look up
     * @param fileBlockOffset (in) The file block offset
     * @param partitionNum (in) The partition containing the lbid
     * @param segmentNum (in) The segement containing the lbid
     * @param LBID (out) The LBID associated with the given offset of the OID.
     * @return 0 on success, -1 on error
     */
    EXPORT int lookupLocal_DBroot(int OID, uint16_t dbroot,
                                  uint32_t partitionNum, uint16_t segmentNum, uint32_t fileBlockOffset,
                                  LBID_t& LBID);

    // @bug 1055-.

    /** @brief Look up the starting LBID associated with a given OID,
     *  partition, segment, and offset.
     *
     * @param OID (in) The OID to look up
     * @param partitionNum (in) The partition containing the lbid
     * @param segmentNum (in) The segement containing the lbid
     * @param fileBlockOffset (in) The file block offset
     * @param LBID (out) The starting LBID associated with the extent
     *        containing the given offset
     * @return 0 on success, -1 on error
     */
    int lookupLocalStartLbid(int OID,
                             uint32_t partitionNum,
                             uint16_t segmentNum,
                             uint32_t fileBlockOffset,
                             LBID_t& LBID);

    /** @brief Get a complete list of LBID ranges assigned to an OID
     *
     * Get a complete list of LBID ranges assigned to an OID.
     */
    EXPORT void lookup(OID_t oid, LBIDRange_v& ranges);

    /** @brief Allocate a "stripe" of extents for columns in a table (in DBRoot)
     *
     * If this is the first extent for the OID/DBRoot, it will start at
     * file offset 0.  If space for the OID already exists, the new
     * extent will "logically" be appended to the end of the already-
     * allocated space, although the extent may reside in a different
     * physical file as indicated by dbRoot, partition, and segment.
     * Partition and segment numbers are 0 based, dbRoot is 1 based.
     *
     * Allocate a "stripe" of extents for the specified columns and DBRoot
     * @param cols (in) List of column OIDs and column widths
     * @param dbRoot (in) DBRoot for requested extents.
     * @param partitionNum (in/out) Partition number in file path.
     *        If allocating OID's first extent for this DBRoot, then
     *        partitionNum is input, else it is an output arg.
     * @param segmentNum (out) Segment number selected for new extents.
     * @param extents (out) list of lbids, numBlks, and fbo for new extents
     * @return 0 on success, -1 on error
     */
    EXPORT void createStripeColumnExtents(
        const std::vector<CreateStripeColumnExtentsArgIn>& cols,
        uint16_t  dbRoot,
        uint32_t& partitionNum,
        uint16_t& segmentNum,
        std::vector<CreateStripeColumnExtentsArgOut>& extents);

    /** @brief Allocates an extent for a column file
     *
     * Allocates an extent for the specified OID and DBroot.
     * If this is the first extent for the OID/DBRoot, it will start at
     * file offset 0.  If space for the OID already exists, the new
     * extent will "logically" be appended to the end of the already-
     * allocated space, although the extent may reside in a different
     * physical file as indicated by dbRoot, partition, and segment.
     * Partition and segment numbers are 0 based, dbRoot is 1 based.
     *
     * @param OID (in) The OID requesting the extent.
     * @param colWidth (in) Column width of the OID.
     * @param dbRoot (in) DBRoot where extent is to be added.
     * @param colDataType (in) the column type
     * @param partitionNum (in/out) Partition number in file path.
     *        If allocating OID's first extent for this DBRoot, then
     *        partitionNum is input, else it is an output arg.
     * @param segmentNum (out) Segment number assigned to the extent.
     * @param lbid (out) The first LBID of the extent created.
     * @param allocdsize (out) The total number of LBIDs allocated.
     * @param startBlockOffset (out) The first block of the extent created.
     * @param useLock Grab ExtentMap and FreeList WRITE lock to perform work
     */
    // @bug 4091: To be deprecated as public function.  Should just be a
    // private function used by createStripeColumnExtents().
    EXPORT void createColumnExtent_DBroot(int OID,
                                          uint32_t  colWidth,
                                          uint16_t  dbRoot,
                                          execplan::CalpontSystemCatalog::ColDataType colDataType,
                                          uint32_t& partitionNum,
                                          uint16_t& segmentNum,
                                          LBID_t&    lbid,
                                          int&       allocdsize,
                                          uint32_t& startBlockOffset,
                                          bool       useLock = true);

    /** @brief Allocates extent for exact file that is specified
     *
     * Allocates an extent for the exact file specified by OID, DBRoot,
     * partition, and segment.
     * If this is the first extent for the OID/DBRoot, it will start at
     * file offset 0.  If space for the OID already exists, the new
     * extent will "logically" be appended to the end of the already-
     * allocated space.
     * Partition and segment numbers are 0 based, dbRoot is 1 based.
     *
     * @param OID (in) The OID requesting the extent.
     * @param colWidth (in) Column width of the OID.
     * @param dbRoot (in) DBRoot where extent is to be added.
     * @param partitionNum (in) Partition number in file path.
     *        If allocating OID's first extent for this DBRoot, then
     *        partitionNum is input, else it is an output arg.
     * @param segmentNum (in) Segment number in file path.
     *        If allocating OID's first extent for this DBRoot, then
     *        segmentNum is input, else it is an output arg.
     * @param colDataType (in) the column type
     * @param lbid (out) The first LBID of the extent created.
     * @param allocdSize (out) The total number of LBIDs allocated.
     * @param startBlockOffset (out) The first block of the extent created.
     */
    EXPORT void createColumnExtentExactFile(int OID,
                                            uint32_t  colWidth,
                                            uint16_t  dbRoot,
                                            uint32_t  partitionNum,
                                            uint16_t  segmentNum,
                                            execplan::CalpontSystemCatalog::ColDataType colDataType,
                                            LBID_t&    lbid,
                                            int&       allocdsize,
                                            uint32_t& startBlockOffset);

    /** @brief Allocates an extent for a dictionary store file
     *
     * Allocates an extent for the specified dictionary store OID,
     * dbRoot, partition number, and segment number.   These should
     * correlate with those belonging to the corresponding token file.
     * The first extent for each store file will start at file offset 0.
     * Other extents will be appended to the end of the already-
     * allocated space for the same store file.
     * Partition and segment numbers are 0 based, dbRoot is 1 based.
     *
     * @param OID (in) The OID requesting the extent.
     * @param dbRoot (in) DBRoot to assign to the extent.
     * @param partitionNum (in) Partition number to assign to the extent.
     * @param segmentNum (in) Segment number to assign to the extent.
     * @param lbid (out) The first LBID of the extent created.
     * @param allocdsize (out) The total number of LBIDs allocated.
     */
    EXPORT void createDictStoreExtent(int OID,
                                      uint16_t   dbRoot,
                                      uint32_t   partitionNum,
                                      uint16_t   segmentNum,
                                      LBID_t&    lbid,
                                      int&       allocdsize);

    /** @brief Rollback (delete) a set of extents for the specified OID.
     *
     * Deletes all the extents that logically follow the specified
     * column extent; and sets the HWM for the specified extent.
     * @param oid OID of the extents to be deleted.
     * @param partitionNum Last partition to be kept.
     * @param segmentNum Last segment in partitionNum to be kept.
     * @param hwm HWM to be assigned to the last extent that is kept.
     */
    EXPORT void rollbackColumnExtents(int oid,
                                      uint32_t partitionNum,
                                      uint16_t segmentNum,
                                      HWM_t     hwm);

    /** @brief Rollback (delete) set of extents for specified OID & DBRoot.
     *
     * Deletes all the extents that logically follow the specified
     * column extent; and sets the HWM for the specified extent.
     * @param oid OID of the extents to be deleted.
     * @param bDeleteAll Flag indicates if all extents for oid and dbroot are
     *        to be deleted, else part#, seg#, and HWM are used.
     * @param dbRoot DBRoot of the extents to be deleted.
     * @param partitionNum Last partition to be kept.
     * @param segmentNum Last segment in partitionNum to be kept.
     * @param hwm HWM to be assigned to the last extent that is kept.
     */
    EXPORT void rollbackColumnExtents_DBroot(int oid, bool bDeleteAll, uint16_t dbRoot, uint32_t partitionNum,
                                             uint16_t segmentNum, HWM_t hwm);

    /** @brief delete of column extents for the specified extents.
     *
     * Deletes the extents that logically follow the specified
     * column extent in  extentsInfo. It use the same algorithm as in
     * rollbackColumnExtents.
     * @param extentInfo the information for extents
     */
    EXPORT void deleteEmptyColExtents(const ExtentsInfoMap_t& extentsInfo);

    /** @brief delete of dictionary extents for the specified extents.
     *
     * Arguments specify the last stripe for all the oids.  Any extents after this are
     * deleted.  The hwm's of the extents in the last stripe are updated
     * based on the hwm in extentsInfo.  It use the same algorithm as in
     * rollbackDictStoreExtents.
     * @param extentInfo the information for extents to be resetted
     */
    EXPORT void deleteEmptyDictStoreExtents(const ExtentsInfoMap_t& extentsInfo);

    /** @brief Rollback (delete) a set of dict store extents for an OID.
     *
     * Arguments specify the last stripe.  Any extents after this are
     * deleted.  The hwm's of the extents in the last stripe are updated
     * based on the contents of the hwm vector.  If hwms is a partial list,
     * (as in the first stripe of a partition), then any extents in sub-
     * sequent segment files for that partition are deleted.
     * @param oid OID of the extents to be deleted or updated.
     * @param partitionNum Last partition to be kept.
     * @param hwms Vector of hwms for the last partition to be kept.
     */
    EXPORT void rollbackDictStoreExtents(int oid, uint32_t partitionNum, const std::vector<HWM_t>& hwms);

    /** @brief Rollback (delete) a set of dict store extents for an OID & DBRoot
     *
     * Arguments specify the last stripe.  Any extents after this are
     * deleted.  The hwm's of the extents in the last stripe are updated
     * based on the contents of the hwm vector.  If hwms is a partial list,
     * (as in the first stripe of a partition), then any extents in sub-
     * sequent segment files for that partition are deleted.  If hwms is empty
     * then all the extents in dbRoot are deleted.
     * @param oid OID of the extents to be deleted or updated.
     * @param dbRoot DBRoot of the extents to be deleted.
     * @param partitionNum Last partition to be kept.
     * @param segNums Vector of segment files in last partition to be kept.
     * @param hwms Vector of hwms for the last partition to be kept.
     */
    EXPORT void rollbackDictStoreExtents_DBroot(int oid, uint16_t dbRoot, uint32_t partitionNum,
                                                const std::vector<uint16_t>& segNums,
                                                const std::vector<HWM_t>& hwms);

    /** @brief Deallocates all extents associated with OID
     *
     * Deallocates all extents associated with OID
     * @param OID The OID to delete
     */
    EXPORT void deleteOID(int OID);

    /** @brief Deallocates all extents associated with each OID
     *
     * Deallocates all extents associated with each OID
     * @param OIDs The OIDs to delete
     */
    EXPORT void deleteOIDs(const OidsMap_t& OIDs);

    /** @brief Check if any of the given partitions is the last one of a DBroot
     *
     * This is for partitioning operations to use. The last partition of a DBroot
     * can not be dropped or disabled.
     *
     * @param OID (in) The OID
     * @param partitionNums (in) The logical partition numbers to check.
     * @return true if any of the partitions in the set is the last partition of
     * a DBroot.
     */

    /** @brief Gets the last local high water mark of an OID for a given dbRoot
     *
     * Get last local high water mark of an OID for a given dbRoot, relative to
     * a segment file. The partition and segment numbers for the pertinent
     * segment are also returned.

     * @param OID (in) The OID
     * @param dbRoot (in) The relevant DBRoot
     * @param partitionNum (out) The relevant partition number
     * @param segmentNum (out) The relevant segment number
     * @param status (out) State of the extent (Available, OutOfService, etc)
     * @param bFound (out) Indicates whether an extent was found for dbRoot
     * @return The last file block number written to in the last
     * partition/segment file for the given OID.
     */
    EXPORT HWM_t getLastHWM_DBroot(int OID, uint16_t dbRoot,
                                   uint32_t& partitionNum, uint16_t& segmentNum,
                                   int& status, bool& bFound);

    /** @brief Gets the current high water mark of an OID,partition,segment
     *
     * Get current local high water mark of an OID, partition, segment;
     * where HWM is relative to the specific segment file.
     * @param OID (in) The OID
     * @param partitionNum (in) The relevant partition number
     * @param segmentNum (in) The relevant segment number
     * @param status (out) State of the extent (Available, OutOfService, etc)
     * @return The last file block number written to in the specified
     * partition/segment file for the given OID.
     */
    EXPORT HWM_t getLocalHWM(int OID, uint32_t partitionNum,
                             uint16_t segmentNum, int& status);

    /** @brief Sets the current high water mark of an OID,partition,segment
     *
     * Sets the current local high water mark of an OID, partition, segment;
     * where HWM is relative to the specific segment file.
     * @param OID The OID
     * @param partitionNum (in) The relevant partition number
     * @param segmentNum (in) The relevant segment number
     * @param HWM The high water mark to record
     */
    EXPORT void setLocalHWM(int OID, uint32_t partitionNum,
                            uint16_t segmentNum, HWM_t HWM, bool firstNode,
                            bool uselock = true);

    EXPORT void bulkSetHWM(const std::vector<BulkSetHWMArg>&, bool firstNode);

    EXPORT void bulkUpdateDBRoot(const std::vector<BulkUpdateDBRootArg>&);

    /** @brief Get HWM information about last segment file for each DBRoot
     *  assigned to a specific PM.
     *
     * Vector will contain an entry for each DBRoot.  If no "available" extents
     * are found for a DBRoot, then totalBlocks will be 0 (and hwmExtentIndex
     * will be -1) for that DBRoot.
     * @param OID The oid of interest.
     * @param pmNumber The PM number of interest.
     * @param emDbRootHwmInfos The vector of DbRoot/HWM related objects.
     */
    EXPORT void getDbRootHWMInfo(int OID, uint16_t pmNumber,
                                 EmDbRootHWMInfo_v& emDbRootHwmInfos);

    /** @brief Get the status (AVAILABLE, OUTOFSERVICE, etc) for the
     * segment file represented by the specified OID, part# and seg#.
     *
     * Unlike many of the other DBRM functions, this function does
     * not throw an exception if no extent is found; the "found"
     * flag indicates whether an extent was found or not.
     *
     * @param oid (in) The OID of interest
     * @param partitionNum (in) The partition number of interest
     * @param segmentNum (in) The segment number of interest
     * @param bFound (out) Indicates if extent was found or not
     * @param status (out) The state of the extents in the specified
     *        segment file.
    */
    EXPORT void getExtentState(int OID, uint32_t partitionNum,
                               uint16_t segmentNum, bool& bFound, int& status);

    /** @brief Gets the extents of a given OID
     *
     * Gets the extents of a given OID.  The returned entries will
     * be NULL-terminated and will have to be destroyed individually
     * using delete.
     * @note Untested
     * @param OID (in) The OID to get the extents for.
     * @param entries (out) A snapshot of the OID's Extent Map entries
     * sorted by starting LBID; note that The Real Entries can change at
     * any time.
     * @param sorted (in) indicates if output is to be sorted
     * @param notFoundErr (in) indicates if no extents is considered an err
     * @param incOutOfService (in) include/exclude out of service extents
     */
    EXPORT void getExtents(int OID, std::vector<struct EMEntry>& entries,
                           bool sorted = true, bool notFoundErr = true,
                           bool incOutOfService = false);

    /** @brief Gets the extents of a given OID under specified dbroot
     *
     * Gets the extents of a given OID under specified dbroot.  The returned entries will
     * be NULL-terminated and will have to be destroyed individually
     * using delete.
     * @param OID (in) The OID to get the extents for.
     * @param entries (out) A snapshot of the OID's Extent Map entries for the dbroot
     * @param dbroot (in) the specified dbroot
     */
    EXPORT void getExtents_dbroot(int OID, std::vector<struct EMEntry>& entries,
                                  const uint16_t dbroot);

    /** @brief Gets the number of extents for the specified OID and DBRoot
     *
     * @param OID (in) The OID of interest
     * @param dbroot (in) The DBRoot of interest
     * @param incOutOfService (in) include/exclude out of service extents
     * @param numExtents (out) number of extents found for OID and dbroot
     * @return 0 on success, non-0 on error (see brmtypes.h)
     */
    EXPORT void getExtentCount_dbroot(int OID, uint16_t dbroot,
                                      bool incOutOfService, uint64_t& numExtents);

    /** @brief Gets the size of an extent in rows
     *
     * Gets the size of an extent in rows.
     * @return The number of rows in an extent.
     */
    EXPORT unsigned getExtentSize(); //dmc-consider deprecating
    EXPORT unsigned getExtentRows();

    /** @brief Gets the DBRoot for the specified system catalog OID
     *
     * Function should only be called for System Catalog OIDs, as it assumes
     * the OID is fully contained on a single DBRoot, returning the first
     * DBRoot found.  This only makes sence for a System Catalog
     * OID, because all other column OIDs can span multiple DBRoots.
     *
     * @param oid The system catalog OID
     * @param dbRoot (out) the DBRoot holding the system catalog OID
     */
    EXPORT void getSysCatDBRoot(OID_t oid, uint16_t& dbRoot);

    /** @brief Delete a Partition for the specified OID(s).
     *
     * @param oids (in) the OIDs of interest.
     * @param partitionNums (in) the set of partitions to be deleted.
     */
    EXPORT void deletePartition(const std::set<OID_t>& oids,
                                const std::set<LogicalPartition>& partitionNums, std::string& emsg);

    /** @brief Mark a Partition for the specified OID(s) as out of service.
     *
     * @param oids (in) the OIDs of interest.
     * @param partitionNums (in) the set of partitions to be marked out of service.
     */
    EXPORT void markPartitionForDeletion(const std::set<OID_t>& oids,
                                         const std::set<LogicalPartition>& partitionNums, std::string& emsg);

    /** @brief Mark all Partition for the specified OID(s) as out of service.
     *
     * @param oids (in) the OIDs of interest.
     */
    EXPORT void markAllPartitionForDeletion(const std::set<OID_t>& oids);

    /** @brief Restore a Partition for the specified OID(s).
     *
     * @param oids (in) the OIDs of interest.
     * @param partitionNums (in) the set of partitions to be restored.
     */
    EXPORT void restorePartition(const std::set<OID_t>& oids,
                                 const std::set<LogicalPartition>& partitionNums, std::string& emsg);

    /** @brief Get the list of out-of-service partitions for a given OID
     *
     * @param OID (in) the OID of interest.
     * @param partitionNums (out) the out-of-service partitions for the oid.
     * partitionNums will be in sorted order.
     */
    EXPORT void getOutOfServicePartitions(OID_t oid,
                                          std::set<LogicalPartition>& partitionNums);

    /** @brief Delete all extent map rows for the specified dbroot
     *
     * @param dbroot (in) the dbroot
     */
    EXPORT void deleteDBRoot(uint16_t dbroot);

    /** @brief Is the specified DBRoot empty with no extents.
     *  Throws exception if extentmap shared memory is not loaded.
     *
     * @param dbroot DBRoot of interest
     */
    EXPORT bool isDBRootEmpty(uint16_t dbroot);

    /** @brief Performs internal consistency checks (for testing only)
     *
     * Performs internal consistency checks (for testing only).
     * @note It's incomplete
     * @return 0 if all tests pass, -1 (or throws logic_error) if not.
     */
    EXPORT int checkConsistency();

    EXPORT void setReadOnly();

    EXPORT virtual void undoChanges();

    EXPORT virtual void confirmChanges();

    EXPORT int markInvalid(const LBID_t lbid,
                           const execplan::CalpontSystemCatalog::ColDataType colDataType);
    EXPORT int markInvalid(const std::vector<LBID_t>& lbids,
                           const std::vector<execplan::CalpontSystemCatalog::ColDataType>& colDataTypes);

    EXPORT int setMaxMin(const LBID_t lbidRange, const int64_t max, const int64_t min, const int32_t seqNum,
                         bool firstNode);

    // @bug 1970.  Added setExtentsMaxMin function below.

    /** @brief Updates the extents in the passed map of CPMaxMin objects.
     * @param cpMap - The key must be the first LBID in the range.
     *                The values are a CPMaxMin struct with the
     *                min, max, and sequence.
     * @param firstNode - if true, logs a debugging msg when CP data is updated
     * @return 0 if all tests pass, -1 (or throws logic_error) if not.
    */
    EXPORT void setExtentsMaxMin(const CPMaxMinMap_t& cpMap, bool firstNode, bool useLock = true);

    /** @brief Merges the CP info for the extents contained in cpMap.
     * @param cpMap - The key must be the starting LBID in the range.
     * @return 0 if all tests pass, -1 (or throws logic_error) if not.
    */
    void mergeExtentsMaxMin(CPMaxMinMergeMap_t& cpMap, bool useLock = true);

    EXPORT int getMaxMin(const LBID_t lbidRange, int64_t& max, int64_t& min, int32_t& seqNum);

    inline bool empty()
    {
        if (fEMRBTreeShminfo == nullptr)
        {
            grabEMEntryTable(READ);
            releaseEMEntryTable(READ);
        }
        // Initial size.
        return (fEMRBTreeShminfo->currentSize == EM_RB_TREE_EMPTY_SIZE);
    }

    EXPORT std::vector<InlineLBIDRange> getFreeListEntries();

    EXPORT void dumpTo(std::ostream& os);
    EXPORT const bool* getEMLockStatus();
    EXPORT const bool* getEMFLLockStatus();
    EXPORT const bool* getEMIndexLockStatus();
    size_t EMIndexShmemSize();
    size_t EMIndexShmemFree();

#ifdef BRM_DEBUG
    EXPORT void printEM() const;
    EXPORT void printEM(const OID_t& oid) const;
    EXPORT void printEM(const EMEntry& em) const;
    EXPORT void printFL() const;
#endif

private:

  enum class UndoRecordType
  {
      DEFAULT,
      INSERT,
      DELETE
  };


  static const constexpr size_t EM_INCREMENT_ROWS = 1000;
  static const constexpr size_t EM_INITIAL_SIZE = EM_INCREMENT_ROWS * 10 * sizeof(EMEntry);
  static const constexpr size_t EM_INCREMENT = EM_INCREMENT_ROWS * sizeof(EMEntry);
  static const constexpr size_t EM_FREELIST_INITIAL_SIZE = 50 * sizeof(InlineLBIDRange);
  static const constexpr size_t EM_FREELIST_INCREMENT = 50 * sizeof(InlineLBIDRange);
  // RBTree constants.
  static const size_t EM_RB_TREE_NODE_SIZE = 10 * (sizeof(EMEntry) + 8 * sizeof(uint64_t));
  static const size_t EM_RB_TREE_EMPTY_SIZE = 1024;
  static const size_t EM_RB_TREE_INITIAL_SIZE =
      EM_INCREMENT_ROWS * 10 * EM_RB_TREE_NODE_SIZE + EM_RB_TREE_EMPTY_SIZE;
  static const size_t EM_RB_TREE_INCREMENT = EM_INCREMENT_ROWS * EM_RB_TREE_NODE_SIZE;

  ExtentMap(const ExtentMap& em);
  ExtentMap& operator=(const ExtentMap& em);

  ExtentMapRBTree* fExtentMapRBTree;
  InlineLBIDRange* fFreeList;

  key_t fCurrentEMShmkey;
  key_t fCurrentFLShmkey;

  MSTEntry* fEMRBTreeShminfo;
  MSTEntry* fFLShminfo;
  MSTEntry* fEMIndexShminfo;

  const MasterSegmentTable fMST;
  bool r_only;
  typedef std::tr1::unordered_map<int, oam::DBRootConfigList*> PmDbRootMap_t;
  PmDbRootMap_t fPmDbRootMap;
  time_t fCacheTime; // timestamp associated with config cache

  int numUndoRecords;
  bool flLocked, emLocked, emIndexLocked;

  static boost::mutex mutex; // @bug5355 - made mutex static
  static boost::mutex emIndexMutex;
  boost::mutex fConfigCacheMutex; // protect access to Config Cache

  enum OPS
  {
      NONE,
      READ,
      WRITE
  };

  OPS EMLock, FLLock;

  LastIndEmptyIndEmptyInd _createExtentCommonSearch(const OID_t OID, const DBRootT dbRoot,
                                                    const PartitionNumberT partitionNum,
                                                    const SegmentT segmentNum);

  void logAndSetEMIndexReadOnly(const std::string& funcName);

  // Create extents.
  LBID_t _createColumnExtent_DBroot(uint32_t size, int OID, uint32_t colWidth, uint16_t dbRoot,
                                    execplan::CalpontSystemCatalog::ColDataType colDataType,
                                    uint32_t& partitionNum, uint16_t& segmentNum, uint32_t& startBlockOffset);
  LBID_t _createColumnExtentExactFile(uint32_t size, int OID, uint32_t colWidth, uint16_t dbRoot,
                                      uint32_t partitionNum, uint16_t segmentNum,
                                      execplan::CalpontSystemCatalog::ColDataType colDataType,
                                      uint32_t& startBlockOffset);
  LBID_t _createDictStoreExtent(uint32_t size, int OID, uint16_t dbRoot, uint32_t partitionNum,
                                uint16_t segmentNum);

  // Delete extent.
  ExtentMapRBTree::iterator deleteExtent(ExtentMapRBTree::iterator it, const bool clearEMIndex = true);

  bool isValidCPRange(int64_t max, int64_t min, execplan::CalpontSystemCatalog::ColDataType type) const;
  LBID_t getLBIDsFromFreeList(uint32_t size);
  void reserveLBIDRange(LBID_t start, uint8_t size); // used by load() to allocate pre-existing LBIDs
  std::vector<EMEntry> getEmIdentsByLbids(const std::vector<LBID_t>& lbids);
  std::vector<ExtentMapRBTree::iterator> getEmIteratorsByLbids(const std::vector<LBID_t>& lbids);

  // Choose keys.
  key_t chooseEMShmkey();
  key_t chooseFLShmkey();
  key_t chooseEMIndexShmkey();

  key_t getInitialEMIndexShmkey() const;
  // see the code for how keys are segmented
  key_t chooseShmkey(const MSTEntry* masterTableEntry, const uint32_t keyRangeBase) const;

  // Grab table.
  void grabEMEntryTable(OPS op);
  void grabFreeList(OPS op);
  void grabEMIndex(OPS op);

  // Release table.
  void releaseEMEntryTable(OPS op);
  void releaseFreeList(OPS op);
  void releaseEMIndex(OPS op);

  // Grow memory.
  void growEMShmseg(size_t nrows = 0);
  void growFLShmseg();
  void growEMIndexShmseg(const size_t suggestedSize = 0);
  void growIfNeededOnExtentCreate();

  // Finish.
  void finishChanges();

  EXPORT unsigned getFilesPerColumnPartition();
  unsigned getExtentsPerSegmentFile();
  unsigned getDbRootCount();
  void getPmDbRoots(int pm, std::vector<int>& dbRootList);
  DBRootVec getAllDbRoots();
  void checkReloadConfig();
  ShmKeys fShmKeys;

  void makeUndoRecordRBTree(UndoRecordType type, const EMEntry& emEntry);
  void undoChangesRBTree();
  void confirmChangesRBTree();

  bool fDebug;

  int _markInvalid(const LBID_t lbid, const execplan::CalpontSystemCatalog::ColDataType colDataType);

  void loadVersion4(std::ifstream& in);
  void loadVersion4(idbdatafile::IDBDataFile* in);

  ExtentMapRBTree::iterator findByLBID(const LBID_t lbid);

  using UndoRecordPair = std::pair<UndoRecordType, EMEntry>;
  std::vector<UndoRecordPair> undoRecordsRBTree;

  ExtentMapRBTreeImpl* fPExtMapRBTreeImpl;
  FreeListImpl* fPFreeListImpl;
  ExtentMapIndexImpl* fPExtMapIndexImpl_;
};

inline std::ostream& operator<<(std::ostream& os, ExtentMap& rhs)
{
    rhs.dumpTo(os);
    return os;
}

} //namespace

#undef EXPORT

#endif

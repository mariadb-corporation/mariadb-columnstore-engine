/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016-2026 MariaDB Corporation

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

#pragma once

#include <sys/types.h>
#include <vector>
#include <set>
#include <unordered_map>
#include <tr1/unordered_map>
#include <mutex>

// #define NDEBUG
#include <cassert>
#include <boost/functional/hash.hpp>  //boost::hash
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/unordered_map.hpp>

#include "shmkeys.h"
#include "brmtypes.h"

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

#define EXPORT

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
#define EM_MAGIC_V1 0x76f78b1c
#define EM_MAGIC_V2 0x76f78b1d
#define EM_MAGIC_V3 0x76f78b1e
#define EM_MAGIC_V4 0x76f78b1f
#define EM_MAGIC_V5 0x76f78b20

using PartitionNumberT = uint32_t;
using DBRootT = uint16_t;
using SegmentT = uint16_t;
using HighestOffset = uint32_t;
using DBRootVec = std::vector<DBRootT>;

// assumed column width when calculating dictionary store extent size
#define DICT_COL_WIDTH 8

// valid values for EMEntry.status
const int16_t EXTENTSTATUSMIN(0);  // equal to minimum valid status value
const int16_t EXTENTAVAILABLE(0);
const int16_t EXTENTUNAVAILABLE(1);
const int16_t EXTENTOUTOFSERVICE(2);
const int16_t EXTENTSTATUSMAX(2);  // equal to maximum valid status value

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

struct EMCasualPartition_struct_v4
{
  RangePartitionData_t hi_val;  // This needs to be reinterpreted as unsigned for uint64_t column types.
  RangePartitionData_t lo_val;
  int32_t sequenceNum;
  char isValid;  // CP_INVALID - No min/max and no DML in progress. CP_UPDATING - Update in progress.
                 // CP_VALID- min/max is valid
};

struct EMPartition_struct_v4
{
  EMCasualPartition_struct_v4 cprange;
};
struct EMEntry_v4
{
  InlineLBIDRange range;
  int fileID;
  uint32_t blockOffset;
  HWM_t HWM;
  PartitionNumberT partitionNum;  // starts at 0
  uint16_t segmentNum;            // starts at 0
  DBRootT dbRoot;                 // starts at 1 to match Columnstore.xml
  uint16_t colWid;
  int16_t status;  // extent avail for query or not, or out of service
  EMPartition_struct_v4 partition;
};

// MCOL-641: v5 structs of the extent map. This version supports int128_t min
// and max values for casual partitioning.
struct EMCasualPartition_struct
{
  int32_t sequenceNum;
  char isValid;  // CP_INVALID - No min/max and no DML in progress. CP_UPDATING - Update in progress.
                 // CP_VALID- min/max is valid
  union
  {
    int128_t bigLoVal;  // These need to be reinterpreted as unsigned for uint64_t/uint128_t column types.
    int64_t loVal;
  };
  union
  {
    int128_t bigHiVal;
    int64_t hiVal;
  };
  EXPORT EMCasualPartition_struct();
  EXPORT EMCasualPartition_struct(const int64_t lo, const int64_t hi, const int32_t seqNum);
  EXPORT EMCasualPartition_struct(const int128_t bigLo, const int128_t bigHi, const int32_t seqNum);
  EXPORT EMCasualPartition_struct(const int64_t lo, const int64_t hi, const int32_t seqNum,
                                  const char status);
  EXPORT EMCasualPartition_struct(const EMCasualPartition_struct& em);
  EXPORT EMCasualPartition_struct& operator=(const EMCasualPartition_struct& em);
};
using EMCasualPartition_t = EMCasualPartition_struct;

struct EMPartition_struct
{
  EMCasualPartition_t cprange;
};
typedef EMPartition_struct EMPartition_t;

struct EMEntry
{
  InlineLBIDRange range;
  int fileID;
  uint32_t blockOffset;
  HWM_t HWM;
  PartitionNumberT partitionNum;  // starts at 0
  uint16_t segmentNum;            // starts at 0
  DBRootT dbRoot;                 // starts at 1 to match Columnstore.xml
  uint16_t colWid;
  int16_t status;  // extent avail for query or not, or out of service
  EMPartition_t partition;
  EXPORT EMEntry();
  EMEntry(const InlineLBIDRange& range, int fileID, uint32_t bOffset, HWM_t hwm, PartitionNumberT pNum,
          uint16_t sNum, DBRootT dRoot, uint16_t cWid, int16_t s, EMPartition_t partition);
  EXPORT EMEntry(const EMEntry&);
  EXPORT EMEntry& operator=(const EMEntry&);
  EXPORT bool operator<(const EMEntry&) const;
  EXPORT void setHWMAndInvalidate(HWM_t newHWM); // used in cpimport failure rollback
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

    if (e1.dbRoot == e2.dbRoot && e1.partitionNum == e2.partitionNum && e1.blockOffset == e2.blockOffset &&
        e1.segmentNum < e2.segmentNum)
      return true;

    return false;
  }
};

using ShmSegmentManagerT = bi::managed_shared_memory::segment_manager;
using ShmVoidAllocator = bi::allocator<void, ShmSegmentManagerT>;

using LBID_tAlloc = bi::allocator<LBID_t, ShmSegmentManagerT>;
using PartitionNumberTAlloc = bi::allocator<PartitionNumberT, ShmSegmentManagerT>;
using LBID_tVectorT = bi::vector<LBID_t, LBID_tAlloc>;

using PartitionIndexContainerKeyT = PartitionNumberT;
using PartitionIndexContainerValT = std::pair<const PartitionIndexContainerKeyT, LBID_tVectorT>;
using PartitionIndexContainerValTAlloc = bi::allocator<PartitionIndexContainerValT, ShmSegmentManagerT>;
// Can't use std::unordered_map presumably b/c the map's pointer type doesn't use offset_type as boost::u_map
// does
using PartitionIndexContainerT =
    boost::unordered_map<PartitionIndexContainerKeyT, LBID_tVectorT, boost::hash<PartitionIndexContainerKeyT>,
                         std::equal_to<PartitionIndexContainerKeyT>, PartitionIndexContainerValTAlloc>;

using OIDIndexContainerKeyT = OID_t;
using OIDIndexContainerValT = std::pair<const OIDIndexContainerKeyT, PartitionIndexContainerT>;
using OIDIndexContainerValTAlloc = bi::allocator<OIDIndexContainerValT, ShmSegmentManagerT>;
using OIDIndexContainerT =
    boost::unordered_map<OIDIndexContainerKeyT, PartitionIndexContainerT, boost::hash<OIDIndexContainerKeyT>,
                         std::equal_to<OIDIndexContainerKeyT>, OIDIndexContainerValTAlloc>;

using DBRootIndexTAlloc = bi::allocator<OIDIndexContainerT, ShmSegmentManagerT>;
using DBRootIndexContainerT = bi::vector<OIDIndexContainerT, DBRootIndexTAlloc>;
using ExtentMapIndex = DBRootIndexContainerT;
using LBID_tFindResult = bi::vector<LBID_t>;
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

// The name of the RBTree object inside an extent map data area.
static const constexpr char* EmMapRBTreeObjectName = "EmMapRBTree";

// The name of the index object inside an extent map index data area.
static const constexpr char* EmIndexObjectName = "i";

/** @brief The extent map RBTree, held in a versioned shared memory segment.
 *
 * Readers work off the data area the metadata area points at. Writers never
 * touch it: beginUpdate() gives them a private copy to change and
 * publishUpdate() makes that copy the current one with a single atomic store.
 * See BRMVersionedShmImpl for the layout.
 */
class ExtentMapRBTreeImpl
{
 public:
  /// A data area that stays mapped for as long as the handle is held.
  using DataAreaPin = BRMVersionedShmImpl::DataAreaPin;

  ~ExtentMapRBTreeImpl() = default;

  static ExtentMapRBTreeImpl* makeExtentMapRBTreeImpl(unsigned keyBase, off_t size, bool readOnly = false);

  // Reader side.
  inline uint64_t currentId() const
  {
    return fManagedShm.currentId();
  }
  inline uint64_t mappedId() const
  {
    return fManagedShm.mappedId();
  }
  inline bool refresh()
  {
    return fManagedShm.refresh();
  }
  /* Pins the published data area and returns it. This, rather than a lock on the
     EM table, is what a reader relies on: the area stays mapped for as long as
     the pin is held, so pointers and iterators into it stay good even after a
     writer has published a replacement. */
  inline DataAreaPin pin()
  {
    return fManagedShm.pin();
  }

  /* The tree inside a pinned data area. Unlike get(), this never constructs
     anything: an area is only ever published with the tree already in it, so a
     null return means the area was not properly published. Static because it
     depends on nothing but the pin - in particular not on what this process has
     mapped right now, which may already be a newer area.
   *
   * find_no_lock() rather than find(), and this matters: find() takes a mutex
   * that lives inside the image, and a writer copies a published area with a
   * memcpy of the whole image. A copy taken while a reader held that mutex
   * would carry it locked, and since the copy is what gets published next,
   * every later lookup in it would wait for an owner that never existed. It is
   * also what makes a read of this class need no lock at all, which is the
   * point of the whole arrangement. Safe because a published area is never
   * written: named objects are only ever constructed in a copy nobody else can
   * reach, so the index this walks cannot change under it. */
  static inline ExtentMapRBTree* treeIn(const DataAreaPin& area)
  {
    return area ? area->find_no_lock<ExtentMapRBTree>(EmMapRBTreeObjectName).first : nullptr;
  }

  // Writer side; all of these need the EM write lock.
  inline bool inUpdate() const
  {
    return fManagedShm.inUpdate();
  }
  /* Marks the window in which this group of data areas - the extent map, its
     index and the free list - is part published and part not. The extent
     map's segment is where the whole group's sequence lives; see
     ExtentMap::finishChanges() and ExtentMap::grabEMAndIndexForRead(). */
  inline void beginGroupPublish()
  {
    fManagedShm.beginGroupPublish();
  }
  inline void endGroupPublish()
  {
    fManagedShm.endGroupPublish();
  }
  inline uint64_t groupPublishSequence() const
  {
    return fManagedShm.groupPublishSequence();
  }
  inline void beginUpdate(off_t minSize)
  {
    fManagedShm.beginUpdate(minSize);
  }
  inline void growUpdate(off_t incSize)
  {
    fManagedShm.growUpdate(incSize);
  }
  inline uint64_t publishUpdate()
  {
    return fManagedShm.publishUpdate();
  }
  inline void discardUpdate()
  {
    fManagedShm.discardUpdate();
  }

  inline void makeReadOnly()
  {
    fManagedShm.setReadOnly();
  }

  inline unsigned key() const
  {
    return fManagedShm.key();
  }

  /* Returns the tree of the area currently being worked on: the copy while an
     update is open, the published area otherwise. Writer side - it needs the EM
     write lock, which is what stops the mapping it reads from being replaced
     under it; a reader goes through pin() and treeIn(). The tree is only ever
     constructed in a copy, so finding nothing means looking at a data area that
     was never properly published, which this says rather than allocating in a
     segment other processes are reading. */
  inline ExtentMapRBTree* get() const
  {
    auto* segment = fManagedShm.segment();

    if (!segment)
      return nullptr;

    // No lock on a published area, for the reason treeIn() gives
    if (!fManagedShm.inUpdate())
      return segment->find_no_lock<ExtentMapRBTree>(EmMapRBTreeObjectName).first;

    VoidAllocator allocator(segment->get_segment_manager());
    return segment->find_or_construct<ExtentMapRBTree>(EmMapRBTreeObjectName)(std::less<LBID_t>(),
                                                                              allocator);
  }

  inline uint64_t getFreeMemory() const
  {
    auto* segment = fManagedShm.segment();
    return segment ? segment->get_free_memory() : 0;
  }
  inline uint64_t getSize() const
  {
    auto* segment = fManagedShm.segment();
    return segment ? segment->get_size() : 0;
  }

 private:
  ExtentMapRBTreeImpl(unsigned keyBase, off_t size, bool readOnly = false);
  ExtentMapRBTreeImpl(const ExtentMapRBTreeImpl& rhs);
  ExtentMapRBTreeImpl& operator=(const ExtentMapRBTreeImpl& rhs);

  BRMVersionedShmImpl fManagedShm;

  static boost::mutex fInstanceMutex;
  static ExtentMapRBTreeImpl* fInstance;
};

/** @brief The header at the front of the free list's shared memory image.
 *
 * The entries follow it. Having it inside the image is what makes the image
 * self-describing: the counts used to live in the master segment table, next
 * to the free list's lock, where a versioned data area has no business keeping
 * them - the published area and the copy an update is open on have different
 * capacities for as long as the update lasts, and there was only one entry to
 * describe both. The table has since gone entirely.
 *
 * Both counts are in entries, not bytes.
 */
struct FreeListHeader
{
  /// Entries the image has room for.
  uint32_t capacity;
  /** Entries in use. The used entries are not a prefix of the array: releasing
      a range zeroes its entry in place and leaves the hole for the next
      allocation to find, so this is a population count and not an extent. */
  uint32_t currentSize;
};

static_assert(sizeof(FreeListHeader) % alignof(InlineLBIDRange) == 0,
              "the free list entries must land aligned immediately after the header");

/// The entries of a free list image, which follow its header.
inline InlineLBIDRange* freeListEntriesOf(FreeListHeader* header)
{
  return reinterpret_cast<InlineLBIDRange*>(header + 1);
}

/// How large an image holding capacity entries has to be.
inline off_t freeListImageSize(uint32_t capacity)
{
  return static_cast<off_t>(sizeof(FreeListHeader)) +
         static_cast<off_t>(capacity) * static_cast<off_t>(sizeof(InlineLBIDRange));
}

/// How many entries an image of imageSize bytes has room for.
inline uint32_t freeListCapacityFor(off_t imageSize)
{
  if (imageSize <= static_cast<off_t>(sizeof(FreeListHeader)))
    return 0;

  return static_cast<uint32_t>((imageSize - sizeof(FreeListHeader)) / sizeof(InlineLBIDRange));
}

/** @brief The extent map free list, held in a versioned shared memory segment.
 *
 * Same scheme as ExtentMapRBTreeImpl, over a raw image rather than a boost
 * managed segment: a reader works off the data area the metadata area points at
 * and holds it mapped with a pin, a writer copies that area, changes the copy
 * and publishes it. See BRMVersionedShmImplT.
 */
class FreeListImpl
{
 public:
  using DataAreaPin = BRMVersionedRawShmImpl::DataAreaPin;

  ~FreeListImpl() = default;

  static FreeListImpl* makeFreeListImpl(unsigned keyBase, off_t size, bool readOnly = false);

  /// The id of the published data area, 0 if nothing has been published yet.
  inline uint64_t currentId() const
  {
    return fFreeList.currentId();
  }
  inline bool refresh()
  {
    return fFreeList.refresh();
  }
  /// Keeps the published data area mapped for as long as the pin is held.
  inline DataAreaPin pin()
  {
    return fFreeList.pin();
  }
  /// The header inside a pinned area. Null for a null pin.
  static inline FreeListHeader* headerIn(const DataAreaPin& area)
  {
    return static_cast<FreeListHeader*>(BRMVersionedRawShmImpl::imageIn(area));
  }

  // The write side. All of these need the free list write lock.
  inline bool inUpdate() const
  {
    return fFreeList.inUpdate();
  }
  inline void beginUpdate(off_t minSize)
  {
    fFreeList.beginUpdate(minSize);
  }
  inline uint64_t publishUpdate()
  {
    return fFreeList.publishUpdate();
  }
  inline void discardUpdate()
  {
    fFreeList.discardUpdate();
  }
  /// Grows the copy an update is open on so it holds at least capacity entries.
  void growUpdateTo(uint32_t capacity);

  inline void makeReadOnly()
  {
    fFreeList.setReadOnly();
  }
  inline unsigned key() const
  {
    return fFreeList.key();
  }

  /** @brief The header of the image being worked on.
   *
   * That is the copy while an update is open and the published area otherwise,
   * which makes this the writer's accessor. A reader has to go through pin() and
   * headerIn(): this one can move under it at any time.
   */
  inline FreeListHeader* get() const
  {
    return static_cast<FreeListHeader*>(fFreeList.image());
  }
  /// The size of get()'s image in bytes, with the same caveats.
  inline off_t imageSize() const
  {
    return fFreeList.imageSize();
  }

 private:
  FreeListImpl(unsigned keyBase, off_t size, bool readOnly = false);
  FreeListImpl(const FreeListImpl& rhs);
  FreeListImpl& operator=(const FreeListImpl& rhs);

  BRMVersionedRawShmImpl fFreeList;

  static boost::mutex fInstanceMutex;
  static FreeListImpl* fInstance;
};

/** @brief The extent map index, held in a versioned shared memory segment.
 *
 * Same scheme as ExtentMapRBTreeImpl: a reader works off the data area the
 * metadata area points at and takes no lock, a writer changes a private copy
 * and makes it current with a single atomic store. See BRMVersionedShmImpl.
 */
class ExtentMapIndexImpl
{
 public:
  /// A data area that stays mapped for as long as the handle is held.
  using DataAreaPin = BRMVersionedShmImpl::DataAreaPin;

  ~ExtentMapIndexImpl() = default;

  static ExtentMapIndexImpl* makeExtentMapIndexImpl(unsigned keyBase, off_t size, bool readOnly = false);

  // Reader side.
  inline uint64_t currentId() const
  {
    return fManagedShm.currentId();
  }
  inline uint64_t mappedId() const
  {
    return fManagedShm.mappedId();
  }
  inline bool refresh()
  {
    return fManagedShm.refresh();
  }
  /* Pins the published data area and returns it. This, rather than a lock on
     the EM index table, is what a reader relies on: the area stays mapped for
     as long as the pin is held, so references and iterators into it stay good
     even after a writer has published a replacement. */
  inline DataAreaPin pin()
  {
    return fManagedShm.pin();
  }

  /** @brief Freezes updates to this segment without making one.
   *
   * What save() holds while it pins a snapshot, in place of a read lock on a
   * table nobody writes any more. Held for the length of a pin, not the length
   * of a file write.
   */
  inline void lockUpdates()
  {
    fManagedShm.lockUpdates();
  }
  inline void unlockUpdates()
  {
    fManagedShm.unlockUpdates();
  }

  /* The index inside a pinned data area. Unlike get(), this never constructs
     anything: an area is only ever published with the index already in it, so
     a null return means the area was not properly published. Static because it
     depends on nothing but the pin - in particular not on what this process has
     mapped right now, which may already be a newer area. find_no_lock() for
     the reason ExtentMapRBTreeImpl::treeIn() gives. */
  static inline ExtentMapIndex* indexIn(const DataAreaPin& area)
  {
    return area ? area->find_no_lock<ExtentMapIndex>(EmIndexObjectName).first : nullptr;
  }

  // Writer side; all of these need the EM index write lock.
  inline bool inUpdate() const
  {
    return fManagedShm.inUpdate();
  }
  inline void beginUpdate(off_t minSize)
  {
    fManagedShm.beginUpdate(minSize);
  }
  inline uint64_t publishUpdate()
  {
    return fManagedShm.publishUpdate();
  }
  inline void discardUpdate()
  {
    fManagedShm.discardUpdate();
  }

  /* Enlarges the unpublished copy if it cannot satisfy memoryNeeded. Returns
     whether it did, because growing relocates the mapping and every reference
     into it has to be taken again. */
  bool growIfNeeded(const size_t memoryNeeded);

  inline void makeReadOnly()
  {
    fManagedShm.setReadOnly();
  }

  inline unsigned key() const
  {
    return fManagedShm.key();
  }

  inline unsigned getShmemSize() const
  {
    auto* segment = fManagedShm.segment();
    return segment ? segment->get_size() : 0;
  }

  inline size_t getShmemFree() const
  {
    auto* segment = fManagedShm.segment();
    return segment ? segment->get_free_memory() : 0;
  }

  /* The index of the area currently being worked on: the copy while an update
     is open, the published one otherwise. Writer side - it needs the EM index
     write lock; a reader goes through pin() and indexIn(). The index is only
     ever constructed in a copy, so finding nothing outside an update means
     looking at a data area that was never properly published, which this says
     rather than allocating in a segment other processes are reading. */
  ExtentMapIndex* get() const;

  InsertUpdateShmemKeyPair insert(const EMEntry& emEntry, const LBID_t lbid);
  InsertUpdateShmemKeyPair insert2ndLayerWrapper(OIDIndexContainerT& oids, const EMEntry& emEntry,
                                                 const LBID_t lbid, const bool aShmemHasGrown);
  InsertUpdateShmemKeyPair insert2ndLayer(OIDIndexContainerT& oids, const EMEntry& emEntry, const LBID_t lbid,
                                          const bool aShmemHasGrown);
  InsertUpdateShmemKeyPair insert3dLayerWrapper(PartitionIndexContainerT& partitions, const EMEntry& emEntry,
                                                const LBID_t lbid, const bool aShmemHasGrown);
  InsertUpdateShmemKeyPair insert3dLayer(PartitionIndexContainerT& partitions, const EMEntry& emEntry,
                                         const LBID_t lbid, const bool aShmemHasGrown);
  InsertUpdateShmemKeyPair insert4thLayerWrapper(LBID_tVectorT& lbids, const EMEntry& emEntry,
                                                 const LBID_t lbid, const bool aShmemHasGrown);
  InsertUpdateShmemKeyPair insert4thLayer(LBID_tVectorT& lbids, const LBID_t lbid,
                                          const bool aShmemHasGrown);

  /* The lookups and the deletions take the index to work on rather than
     finding it themselves, because which one is right depends on the caller:
     a reader's is the one it pinned, a writer's is the copy its update is
     open on. Both are held by ExtentMap for the span of a grab. */
  static LBID_tFindResult find(ExtentMapIndex& emIndex, const DBRootT dbroot, const OID_t oid,
                               const PartitionNumberT partitionNumber);
  static LBID_tFindResult find(ExtentMapIndex& emIndex, const DBRootT dbroot, const OID_t oid);
  static LBID_tFindResult search2ndLayer(OIDIndexContainerT& oids, const OID_t oid,
                                         const PartitionNumberT partitionNumber);
  static LBID_tFindResult search2ndLayer(OIDIndexContainerT& oids, const OID_t oid);
  static LBID_tFindResult search3dLayer(PartitionIndexContainerT& partitions,
                                        const PartitionNumberT partitionNumber);
  static bool isDBRootEmpty(ExtentMapIndex& emIndex, const DBRootT dbroot);
  static void deleteDbRoot(ExtentMapIndex& emIndex, const DBRootT dbroot);
  static void deleteOID(ExtentMapIndex& emIndex, const DBRootT dbroot, const OID_t oid);
  static void deleteEMEntry(ExtentMapIndex& emIndex, const EMEntry& emEntry, const LBID_t lbid);

 private:
  BRMVersionedShmImpl fManagedShm;
  ExtentMapIndexImpl(unsigned keyBase, off_t size, bool readOnly = false);
  ExtentMapIndexImpl(const ExtentMapIndexImpl& rhs);
  ExtentMapIndexImpl& operator=(const ExtentMapIndexImpl& rhs);

  static std::mutex fInstanceMutex_;
  static ExtentMapIndexImpl* fInstance_;
  static constexpr uint32_t dbRootContainerUnitSize_ = 64ULL;
  static constexpr uint32_t oidContainerUnitSize_ = 352ULL;        // 2 * map overhead
  static constexpr uint32_t partitionContainerUnitSize_ = 368ULL;  // single map overhead
  static constexpr uint32_t emIdentUnitSize_ = sizeof(uint64_t);
  static constexpr uint32_t extraUnits_ = 2;
  static constexpr size_t freeSpaceThreshold_ = 256 * 1024;
};

/** @brief This class encapsulates the extent map functionality of the system
 *
 * This class encapsulates the extent map functionality of the system.  It
 * is currently implemented in the quickest-to-write (aka dumb) way to
 * get something working into the hands of the other developers ASAP.
 * The Extent Map shared data should be implemented in a more scalable
 * structure such as a tree or hash table.
 */
class ExtentMap
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

  /** @brief Loads the ExtentMap entries from a binary blob.
   *
   * Loads the ExtentMap entries from a file.  This will
   * clear out any existing entries.  The intention is that before
   * the system starts, an external tool instantiates a single Extent
   * Map and loads the stored entries.
   * @param pointer to a binary blob.
   */
  EXPORT void loadFromBinaryBlob(const char* blob);

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
  EXPORT int lookupLocal(LBID_t LBID, int& OID, uint16_t& dbRoot, uint32_t& partitionNum,
                         uint16_t& segmentNum, uint32_t& fileBlockOffset);

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
  EXPORT int lookupLocal(int OID, uint32_t partitionNum, uint16_t segmentNum, uint32_t fileBlockOffset,
                         LBID_t& LBID);

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
  EXPORT int lookupLocal_DBroot(int OID, uint16_t dbroot, uint32_t partitionNum, uint16_t segmentNum,
                                uint32_t fileBlockOffset, LBID_t& LBID);

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
  int lookupLocalStartLbid(int OID, uint32_t partitionNum, uint16_t segmentNum, uint32_t fileBlockOffset,
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
  EXPORT void createStripeColumnExtents(const std::vector<CreateStripeColumnExtentsArgIn>& cols,
                                        uint16_t dbRoot, uint32_t& partitionNum, uint16_t& segmentNum,
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
  EXPORT void createColumnExtent_DBroot(int OID, uint32_t colWidth, uint16_t dbRoot,
                                        execplan::CalpontSystemCatalog::ColDataType colDataType,
                                        uint32_t& partitionNum, uint16_t& segmentNum, LBID_t& lbid,
                                        int& allocdsize, uint32_t& startBlockOffset, bool useLock = true);

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
  EXPORT void createColumnExtentExactFile(int OID, uint32_t colWidth, uint16_t dbRoot, uint32_t partitionNum,
                                          uint16_t segmentNum,
                                          execplan::CalpontSystemCatalog::ColDataType colDataType,
                                          LBID_t& lbid, int& allocdsize, uint32_t& startBlockOffset);

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
  EXPORT void createDictStoreExtent(int OID, uint16_t dbRoot, uint32_t partitionNum, uint16_t segmentNum,
                                    LBID_t& lbid, int& allocdsize);

  /** @brief Rollback (delete) a set of extents for the specified OID.
   *
   * Deletes all the extents that logically follow the specified
   * column extent; and sets the HWM for the specified extent.
   * @param oid OID of the extents to be deleted.
   * @param partitionNum Last partition to be kept.
   * @param segmentNum Last segment in partitionNum to be kept.
   * @param hwm HWM to be assigned to the last extent that is kept.
   */
  EXPORT void rollbackColumnExtents(int oid, uint32_t partitionNum, uint16_t segmentNum, HWM_t hwm);

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
  EXPORT HWM_t getLastHWM_DBroot(int OID, uint16_t dbRoot, uint32_t& partitionNum, uint16_t& segmentNum,
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
  EXPORT HWM_t getLocalHWM(int OID, uint32_t partitionNum, uint16_t segmentNum, int& status);

  /** @brief Sets the current high water mark of an OID,partition,segment
   *
   * Sets the current local high water mark of an OID, partition, segment;
   * where HWM is relative to the specific segment file.
   * @param OID The OID
   * @param partitionNum (in) The relevant partition number
   * @param segmentNum (in) The relevant segment number
   * @param HWM The high water mark to record
   */
  EXPORT void setLocalHWM(int OID, uint32_t partitionNum, uint16_t segmentNum, HWM_t HWM, bool firstNode,
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
  EXPORT void getDbRootHWMInfo(int OID, uint16_t pmNumber, EmDbRootHWMInfo_v& emDbRootHwmInfos);

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
  EXPORT void getExtentState(int OID, uint32_t partitionNum, uint16_t segmentNum, bool& bFound, int& status);

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
  EXPORT void getExtents(int OID, std::vector<struct EMEntry>& entries, bool sorted = true,
                         bool notFoundErr = true, bool incOutOfService = false);

  /** @brief Gets the extents of a given OID under specified dbroot
   *
   * Gets the extents of a given OID under specified dbroot.  The returned entries will
   * be NULL-terminated and will have to be destroyed individually
   * using delete.
   * @param OID (in) The OID to get the extents for.
   * @param entries (out) A snapshot of the OID's Extent Map entries for the dbroot
   * @param dbroot (in) the specified dbroot
   */
  EXPORT void getExtents_dbroot(int OID, std::vector<struct EMEntry>& entries, const uint16_t dbroot);

  /** @brief Gets the number of extents for the specified OID and DBRoot
   *
   * @param OID (in) The OID of interest
   * @param dbroot (in) The DBRoot of interest
   * @param incOutOfService (in) include/exclude out of service extents
   * @param numExtents (out) number of extents found for OID and dbroot
   * @return 0 on success, non-0 on error (see brmtypes.h)
   */
  EXPORT void getExtentCount_dbroot(int OID, uint16_t dbroot, bool incOutOfService, uint64_t& numExtents);

  /** @brief Gets the size of an extent in rows
   *
   * Gets the size of an extent in rows.
   * @return The number of rows in an extent.
   */
  EXPORT unsigned getExtentSize();  // dmc-consider deprecating
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
  EXPORT void deletePartition(const std::set<OID_t>& oids, const std::set<LogicalPartition>& partitionNums,
                              std::string& emsg);

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
  EXPORT void restorePartition(const std::set<OID_t>& oids, const std::set<LogicalPartition>& partitionNums,
                               std::string& emsg);

  /** @brief Get the list of out-of-service partitions for a given OID
   *
   * @param OID (in) the OID of interest.
   * @param partitionNums (out) the out-of-service partitions for the oid.
   * partitionNums will be in sorted order.
   */
  EXPORT void getOutOfServicePartitions(OID_t oid, std::set<LogicalPartition>& partitionNums);

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

  /** @brief Ends the write transaction across all three tables.
   *
   * undoChanges() throws away the copies the transaction was changing, so
   * nothing it did becomes visible; confirmChanges() lets releaseEMEntryTable()
   * and friends publish them. Either way finishChanges() gives the locks back.
   */
  EXPORT void undoChanges();

  EXPORT void confirmChanges();

  /** @brief Whether a write transaction is still open on this structure.
   *
   * An open update holds the segment's update mutex, so one left behind by a
   * transaction that was never confirmed or rolled back wedges every writer
   * in the cluster. This is how the worker notices it has one.
   */
  EXPORT bool hasOpenUpdate() const;

  /** @brief Pins what save() writes, holding nothing against writers.
   *
   * A pinned data area cannot change, so the pins are the snapshot: a lock is
   * only needed to make the extent map, its index and its free list agree
   * with each other while the pins are taken, which is microseconds rather
   * than the length of a file write. save() calls this itself. A caller that
   * has to snapshot the extent map together with the VBBM and the VSS calls
   * it while it still holds those against writers, and can then let go of
   * everything before writing anything.
   *
   * Nests: called inside a snapshot that is already held it counts onto those
   * pins and takes no lock at all.
   */
  EXPORT void pinForSave();
  EXPORT void unpinForSave();

  EXPORT int markInvalid(const LBID_t lbid, const execplan::CalpontSystemCatalog::ColDataType colDataType);
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

  template <typename T>
  EXPORT int getMaxMin(const LBID_t lbidRange, T& max, T& min, int32_t& seqNum);

  EXPORT void getCPMaxMin(const LBID_t lbidRange,
                          CPMaxMin& cpMaxMin); /** @brief Get whole record for untyped use. */

  /** @brief Whether the extent map holds no extents at all.
   *
   * Asks the tree rather than the MST entry's currentSize counter. The counter
   * says the same thing, but it lives outside the data area and so does not
   * come back with it when a write transaction is rolled back, whereas the tree
   * a pin hands out is always one some transaction published.
   */
  inline bool empty()
  {
    grabEMEntryTable(BRM::ExtentMap::READ);

    try
    {
      bool res = fExtentMapRBTree->empty();
      releaseEMEntryTable(BRM::ExtentMap::READ);
      return res;
    }
    catch (...)
    {
      releaseEMEntryTable(BRM::ExtentMap::READ);
      throw;
    }
  }

  EXPORT std::vector<InlineLBIDRange> getFreeListEntries();

  EXPORT void dumpTo(std::ostream& os);
  size_t EMIndexShmemSize();
  size_t EMIndexShmemFree();

 private:
  static constexpr size_t EM_INCREMENT_ROWS = 1000;
  static constexpr size_t EM_INITIAL_SIZE = EM_INCREMENT_ROWS * 10 * sizeof(EMEntry);
  static constexpr size_t EM_INCREMENT = EM_INCREMENT_ROWS * sizeof(EMEntry);
  static constexpr uint32_t EM_FREELIST_INITIAL_ENTRIES = 50;
  static constexpr uint32_t EM_FREELIST_ENTRY_INCREMENT = 50;
  static constexpr size_t EM_SAVE_NUM_PER_BATCH = 1000000;
  // RBTree constants.
  static constexpr size_t EM_RB_TREE_NODE_SIZE = sizeof(EMEntry) + 8 * sizeof(uint64_t);
  static constexpr size_t EM_RB_TREE_EMPTY_SIZE = 1024;
  /* The floor a data area is created at, not a cap: beginEMUpdate() asks for
     max(what the map needs, this), and growEMShmseg() takes it further when
     the map outgrows it. */
  static constexpr size_t EM_RB_TREE_INITIAL_SIZE = 1024 * 1024;
  static constexpr size_t EM_RB_TREE_INCREMENT = 16 * 1024 * 1024;
  // EM index constants.
  static constexpr size_t EM_INDEX_INITIAL_SIZE = 1024 * 1024;

  ExtentMap(const ExtentMap& em);
  ExtentMap& operator=(const ExtentMap& em);

  // This view of the three data areas is per-thread, not per-object.
  // A "grab" consists of a pin and a nesting depth, which must be strictly
  // balanced by the caller. The pointers below are valid only while the pin is held.
  // This state cannot be shared between threads. If two threads shared an ExtentMap,
  // they would race on the nesting depth counters:
  // A lost decrement keeps depth > 0 forever. The outermost grab never fires again,
  // freezing the thread on a single stale data area until process exit.
  // A lost increment drops the pin prematurely while another thread is still reading
  // the data area.
  static thread_local ExtentMapRBTree* fExtentMapRBTree;
  static thread_local ExtentMapIndex* fEMIndex;
  static thread_local FreeListHeader* fFLHeader;
  static thread_local InlineLBIDRange* fFreeList;

  /* That keeps fExtentMapRBTree pointing at mapped memory during a read, now
     that a read takes no lock on the EM table. Held from this thread's
     outermost grabEMEntryTable(READ) to the matching releaseEMEntryTable(READ);
     a nested read keeps the outer one's pin, so however deeply a read nests it
     sees one snapshot of the extent map throughout. */
  static thread_local ExtentMapRBTreeImpl::DataAreaPin fEMDataAreaPin;
  static thread_local uint32_t fEMReadDepth;

  /* The same for fEMIndex, held from the outermost grabEMIndex(READ) to the
     matching releaseEMIndex(READ). The index and the extent map are two
     independent data areas with independent publish points, so pinning them
     separately is all a reader gets: one grab of each does not see them as of
     a single instant. */
  static thread_local ExtentMapIndexImpl::DataAreaPin fEMIndexDataAreaPin;
  static thread_local uint32_t fEMIndexReadDepth;

  /* The same again for fFLHeader and fFreeList, held from the outermost
     grabFreeList(READ) to the matching releaseFreeList(READ). */
  static thread_local FreeListImpl::DataAreaPin fFLDataAreaPin;
  static thread_local uint32_t fFLReadDepth;

  bool r_only;
  using PmDbRootMap_t = std::tr1::unordered_map<int, oam::DBRootConfigList*>;
  PmDbRootMap_t fPmDbRootMap;
  time_t fCacheTime;  // timestamp associated with config cache

  int numUndoRecords;

  static boost::mutex mutex;  // @bug5355 - made mutex static
  static boost::mutex emIndexMutex;
  boost::mutex fConfigCacheMutex;  // protect access to Config Cache

  enum OPS
  {
    NONE,
    READ,
    WRITE
  };

  OPS EMLock, FLLock;

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

  template <typename T>
  bool isValidCPRange(const T& max, const T& min, execplan::CalpontSystemCatalog::ColDataType type) const;
  LBID_t getLBIDsFromFreeList(uint32_t size);
  void reserveLBIDRange(LBID_t start, uint8_t size);  // used by load() to allocate pre-existing LBIDs
  std::vector<EMEntry> getEmIdentsByLbids(const bi::vector<LBID_t>& lbids);
  std::vector<ExtentMapRBTree::iterator> getEmIteratorsByLbids(const bi::vector<LBID_t>& lbids);

  /* The EM index table's read lock, on its own. save() holds it to order the
     extent map pin it takes against writers, which hold the index write lock
     across a change to the extent map; it does not read the index itself, and
     grabEMIndex(READ) no longer takes a lock to give it. */
  void lockEMIndexForSave();
  void unlockEMIndexForSave();

  // Grab table.
  void grabEMEntryTable(OPS op);
  void grabEMAndIndexForRead();
  void grabFreeList(OPS op);
  void grabEMIndex(OPS op);

  // Release table.
  void releaseEMEntryTable(OPS op);
  void releaseFreeList(OPS op);
  void releaseEMIndex(OPS op);

  // Extent map data area versioning. All three need the EM write lock.
  // Opens a write transaction: takes a private copy of the published data area.
  void beginEMUpdate(size_t sizeNeeded = 0);
  // Makes the copy the published data area. Called when the write lock is dropped.
  void publishEMUpdate();
  // Throws the copy away, which is how a write is rolled back.
  void discardEMUpdate();
  // Creates and publishes an empty data area if nothing has been published yet.
  void createEMImplIfNeeded();
  void ensureEMDataArea();
  void ensureEMIndexDataArea();
  void initEMDataAreaIfNeeded();

  // The same for the EM index data area; all need the EM index write lock.
  // There is no ensureEMDataArea() counterpart. That one exists for save(),
  // which cannot bootstrap the extent map from inside its grab without asking
  // for the EM write lock while already holding locks writers take after it;
  // save() takes the index lock directly and never grabs the index, so the
  // bootstrap inside grabEMIndex() is the only one needed. It asks for the EM
  // index write lock, which is the order writers take it in anyway - after the
  // extent map's, before the free list's - so no grab inverts on it.
  void beginEMIndexUpdate();
  void publishEMIndexUpdate();
  void discardEMIndexUpdate();
  void createEMIndexImplIfNeeded();
  void initEMIndexDataAreaIfNeeded();

  // And for the free list's, all needing the free list write lock. Its
  // bootstrap is the one grabFreeList() does, for the same reason as the
  // index's: the free list lock is the last of the three a writer takes, so
  // asking for it from inside a grab cannot invert on anything.
  void beginFLUpdate();
  void publishFLUpdate();
  void discardFLUpdate();
  void createFreeListImplIfNeeded();
  void initFLDataAreaIfNeeded();

  // Grow memory.
  void growEMShmseg(size_t nrows = 0);
  void growFLShmseg();
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

  bool fDebug;

  int _markInvalid(const LBID_t lbid, const execplan::CalpontSystemCatalog::ColDataType colDataType);

  template <typename T>
  void load(T* in);

  template <typename T>
  void loadVersion4or5(T* in, bool upgradeV4ToV5);

  ExtentMapRBTree::iterator findByLBID(const LBID_t lbid);

  ExtentMapRBTreeImpl* fPExtMapRBTreeImpl;
  FreeListImpl* fPFreeListImpl;
  ExtentMapIndexImpl* fPExtMapIndexImpl_;
};

inline std::ostream& operator<<(std::ostream& os, ExtentMap& rhs)
{
  rhs.dumpTo(os);
  return os;
}

}  // namespace BRM

#undef EXPORT

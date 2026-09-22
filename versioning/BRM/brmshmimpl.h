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

/******************************************************************************
 * $Id$
 *
 *****************************************************************************/

/** @file
 * class BRMShmImpl
 */

#pragma once

#include <pthread.h>
// #define NDEBUG
#include <cassert>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <string>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/mapped_region.hpp>

namespace bi = boost::interprocess;

namespace BRM
{
class BRMShmImplParent
{
 public:
  BRMShmImplParent(unsigned key, off_t size, bool readOnly = false);
  virtual ~BRMShmImplParent();

  inline unsigned key() const
  {
    return fKey;
  }
  inline off_t size() const
  {
    return fSize;
  }
  inline bool isReadOnly() const
  {
    return fReadOnly;
  }

  virtual void setReadOnly() = 0;
  virtual int clear(unsigned newKey, off_t newSize) = 0;
  virtual void destroy() = 0;

 protected:
  unsigned fKey;
  off_t fSize;
  bool fReadOnly;
};

class BRMShmImpl : public BRMShmImplParent
{
 public:
  BRMShmImpl(unsigned key, off_t size, bool readOnly = false);
  BRMShmImpl(const BRMShmImpl& rhs) = delete;
  BRMShmImpl& operator=(const BRMShmImpl& rhs) = delete;
  ~BRMShmImpl()
  {
  }

  int clear(unsigned newKey, off_t newSize) override;
  void destroy() override;
  void setReadOnly() override;

  int grow(unsigned newKey, off_t newSize);
  void swap(BRMShmImpl& rhs);

  bi::shared_memory_object fShmobj;
  bi::mapped_region fMapreg;
};

class BRMManagedShmImpl : public BRMShmImplParent
{
 public:
  BRMManagedShmImpl(unsigned key, off_t size, bool readOnly = false);
  BRMManagedShmImpl(const BRMManagedShmImpl& rhs) = delete;
  BRMManagedShmImpl& operator=(const BRMManagedShmImpl& rhs) = delete;
  ~BRMManagedShmImpl()
  {
    delete fShmSegment;
  }

  int clear(unsigned newKey, off_t newSize) override;
  void destroy() override;
  void setReadOnly() override;

  int grow(off_t newSize);
  void remap(bool readOnly = false);
  void swap(BRMManagedShmImpl& rhs);
  bi::managed_shared_memory* getManagedSegment()
  {
    assert(fShmSegment);
    return fShmSegment;
  }

 private:
  bi::managed_shared_memory* fShmSegment;
};

// The data areas cycle through this count of shm objects. A publication goes into
// the one the publication DataAreaSlots ago left, so this is how many versions
// can be alive at once and how long a retired area is left alone before a
// writer wants its object back
static constexpr uint64_t DataAreaSlots = 3;

// One per thread that reads, claimed on its first pin.
// Unlike DataAreaSlots these are 64 bytes each, not a copy of the area, so
// the whole table costs 32KiB and there is no footprint reason to trim it
static constexpr uint32_t ReaderSlots = 512;
static constexpr size_t ReaderSlotStride = 64;

/** @brief The fixed-size metadata area of a versioned shared memory segment.
 *
 * The area is created once, is never resized and is never relocated, so every
 * process maps it once and keeps that mapping for its whole life. All it carries
 * is the id of the data area that is currently published and the mutex that
 * lets exactly one writer at a time produce the next area. Writers publish a
 * new data area by storing its id here, which
 * is a single atomic store, so a reader sees either the whole set of changes
 * that went into the new area or none of them.
 */
struct ShmMetadata
{
  static constexpr uint32_t MetadataMagic = 0x4b454d4d;  // 'KEMM'
  static constexpr uint32_t MetadataVersion = 1;

  // Written last when the area is created, so that a process that opens the
  // area concurrently can tell an initialized area from a zeroed one.
  std::atomic<uint32_t> magic;
  std::atomic<uint32_t> version;

  // The id of the data area readers must use. 0 means nothing has been
  // published yet. The next data area to be published is always this + 1, so
  // the id a writer is about to use is decided by the mutex below, not stored.
  std::atomic<uint64_t> currentDataAreaId;

  // Held from beginUpdate() until publishUpdate() or discardUpdate(), so at
  // most one update is open segment-wide. Initialized by whichever process
  // creates the area, before the magic is published
  //
  // It's a pthread_mutex_t instead of boost::interprocess::mutex because
  // boost's robust mutexes unlock and throws not_recoverable exception
  // in both cases, EOWNERDEAD and ENOTRECOVERED, making it impossible
  // to correctly revive the mutex
  pthread_mutex_t updateMutex;

  // Which version lives in each data area slot and how many times the object
  // behind that slot has been thrown away and made again. Both are written
  // only by a writer holding updateMutex, and always before the id that sends
  // readers to them is published, so an acquire load of currentDataAreaId is
  // enough to see them
  //
  // The incarnation is what tells a process that the mapping it kept of a slot
  // is of an object that no longer exists
  std::atomic<uint64_t> slotId[DataAreaSlots];
  std::atomic<uint64_t> slotIncarnation[DataAreaSlots];

  // What a reader is reading, so a writer can tell whether the object it wants
  // to write into again is in use. Cheaper than counting readers into the slot
  // they are in: a reader writes its own line and nobody else's, and a writer
  // reads the lot once per publication
  //
  // owner is what the writer uses to tell an announcement that will never be
  // withdrawn - the thread holding it is died - from one it has to respect
  struct ReaderSlot
  {
    std::atomic<uint64_t> readingId;  // 0 when this thread is not in an area
    // The thread holding the slot: its pid in the high 32 bits and its
    // tid in the low half, 0 when the slot is unclaimed. A single variable
    // is used so atomic CAS can be used and locks can be avoided.
    std::atomic<uint64_t> owner;

    char pad[ReaderSlotStride - 2 * sizeof(std::atomic<uint64_t>)];
  };

  ReaderSlot readers[ReaderSlots];

  // Threads that wanted a reader slot and found none. Reuse is off while this
  // is non-zero, because a reader that cannot announce cannot be seen. It
  // takes more than ReaderSlots reading threads on one machine to get here
  std::atomic<uint32_t> unannouncedReaders;

  // Odd while a writer is publishing a group of data areas that have to be
  // read together, even when none is. The extent map, index and the free
  // list are three areas with publish points of their own, and between the
  // first and the last a reader sees a pair that never existed together.
  //
  // Lives in the extent map's metadata area and covers its group; the other
  // structures do not use it
  std::atomic<uint64_t> groupPublishSeq;
};

// Room for the reader slots and then some reserve, so that adding a field to
// ShmMetadata later on does not change the layout of anything else
static constexpr size_t MetadataAreaSize = 64 * 1024;
static_assert(sizeof(ShmMetadata) <= MetadataAreaSize, "ShmMetadata does not fit the metadata area");

// How long a writer waits for the update mutex before giving up. A writer that
// dies holding it is handled by the mutex being robust, so what is left for
// this to catch is a writer that is alive and stuck - rarer, and not something
// waiting longer would fix
static constexpr uint32_t UpdateMutexTimeoutSeconds = 240;

/** @brief The part of a versioned segment that does not depend on the area type.
 *
 * Everything to do with the metadata area: finding it, the published id, and the
 * mutex that serializes writers. A data area is either a boost managed segment
 * or a raw image, which is what the derived class knows about; none of that
 * makes any difference here.
 */
class BRMVersionedShmBase : public BRMShmImplParent
{
 public:
  BRMVersionedShmBase(unsigned keyBase, bool readOnly);
  ~BRMVersionedShmBase() override;
  BRMVersionedShmBase(const BRMVersionedShmBase& rhs) = delete;
  BRMVersionedShmBase& operator=(const BRMVersionedShmBase& rhs) = delete;

  /// The id of the published data area, 0 if there is none yet.
  uint64_t currentId() const;

  /** @brief Marks the window in which this segment's group is inconsistent.
   *
   * A reader takes the sequence, pins everything in the group, and takes it
   * again; unchanged and even means no group publication overlapped, so what
   * it pinned belongs together. See ExtentMap::grabEMAndIndexForRead().
   */
  void beginGroupPublish();
  void endGroupPublish();
  /// Odd while a group publication is in flight.
  uint64_t groupPublishSequence() const;

  /** @brief Hold updates to this segment without making one.
   *
   * Used in dumping shm on disk.
   */
  void lockUpdates()
  {
    lockUpdateMutex();
  }
  void unlockUpdates()
  {
    unlockUpdateMutex();
  }

  /// @brief Does this thread is the one with the segment's update open?
  bool ownsUpdate() const
  {
    return fUpdateOwner.load(std::memory_order_acquire) == std::this_thread::get_id();
  }

  // The announcement. A reader says which version it wants to read before
  // it reads it, and stops saying so when it is done; a writer will not write
  // into an object anybody is announcing
  //
  // Per thread, and the depth counter is what lets a nested read keep the
  // announcement its outermost one made. The thread that announced is the
  // thread that must withdraw it - which is why these are reachable from the
  // handle pin() hands out, rather than tucked away with the writer's half
  void announceRead(uint64_t id);
  void withdrawRead();

  int clear(unsigned newKey, off_t newSize) override;

 protected:
  /// Whether the object in that slot can be written into again: nobody is
  /// announcing the version it holds, and nobody is reading unannounced.
  bool slotIsQuiet(uint64_t slot) const;

  /// pid of reader in the high 32 bits, tid in the low 32 bits
  static uint64_t readerOwnerToken();
  static bool readerOwnerIsGone(uint64_t token);

  void openOrCreateMetadataArea();
  ShmMetadata* metadata() const;
  unsigned metadataKey() const;
  std::string metadataName() const;
  static uint64_t slotOf(uint64_t id)
  {
    return id % DataAreaSlots;
  }
  unsigned dataAreaKey(uint64_t id) const;
  std::string dataAreaName(uint64_t id) const;

  void lockUpdateMutex();
  void unlockUpdateMutex();
  /// The metadata area's name, and the data areas still named, for destroy().
  void removeAllNames();
  static bi::permissions unrestrictedPerms();

  const unsigned fKeyBase;

  bi::shared_memory_object fMetadataShmObj;
  std::shared_ptr<bi::mapped_region> fMetadataRegion;

  struct ReadState
  {
    /// Which segment this state is for; see fReadStates.
    unsigned keyBase = 0;
    /// The reader slot this thread claimed, -1 before it has claimed one.
    int32_t slot = -1;
    /// How deep this thread's reads are nested. The outermost one announces.
    uint32_t depth = 0;
    /// Set when the thread counted itself into unannouncedReaders instead.
    bool unannounced = false;
  };

  // Indexed by fReadStateIndex
  static constexpr uint32_t MaxReadStates = 16;
  static thread_local ReadState fReadStates[MaxReadStates];

  /** @brief A dense index into fReadStates for this segment.
   *
   * Segments with the same key base share one.
   */
  static uint32_t acquireReadStateIndex(unsigned keyBase);
  static void releaseReadStateIndex(unsigned keyBase);

  const uint32_t fReadStateIndex;

  // Serializes the writer threads of this process before they reach the mutex
  // in shared memory, which is what the master segment table's write lock used
  // to do. It has to be a lock of our own rather than that mutex on its own,
  // because an open update is process-wide state
  std::mutex fUpdateGate;

  // Which thread holds the gate, so that a thread which does not can be told
  // it is not in an update rather than shown the one that is. Read without the
  // gate atomic
  std::atomic<std::thread::id> fUpdateOwner{std::thread::id()};

  // Whether this process holds the metadata area's update mutex. Taken by
  // beginUpdate() before anything else, so it is set even while there is no
  // copy yet.
  bool fHoldsUpdateMutex = false;
};

/** @brief A boost managed segment as a data area.
 *
 * Relocatable: every pointer inside the image is an offset from the image
 * itself, so the whole thing can be copied with a memcpy.
 */
struct ManagedAreaPolicy
{
  using Area = bi::managed_shared_memory;

  // A managed segment records its own size inside the image, so a copy cannot
  // be created larger than its source and filled in - it has to be created at
  // exactly the source's size and grown after if needed
  static constexpr bool SizeIsInternal = true;

  static Area* create(const std::string& name, off_t size, const bi::permissions& perms);
  static Area* open(const std::string& name, bool readOnly);
  static off_t size(const Area& area);
  static void* address(const Area& area);

  /// Grows the object named by inc bytes and get back a fresh mapping of it.
  /// The caller must have dropped every mapping it has of the object first: a
  /// managed segment cannot be resized while it is mapped at all
  static Area* grow(const std::string& name, off_t inc);

  // A managed image can be overwritten with another of exactly its size -
  // that is all a copy has ever been here - but not one of a different size,
  // because the segment manager the mapping points at records the size it was
  // built for
  static bool overwrite(Area& area, off_t size, const void* src, off_t srcSize);
};

/** @brief A raw shared memory image as a data area.
 *
 * The object and its mapping travel together, because the mapping is what the
 * caller reads and it is only valid for as long as the object is alive
 */
struct RawAreaPolicy
{
  struct Area
  {
    bi::shared_memory_object obj;
    bi::mapped_region reg;
  };

  // A raw image knows nothing about its own size, so a copy can be created at
  // whatever size is wanted and the shorter source copied into the front of it.
  // The kernel has already zeroed the tail
  static constexpr bool SizeIsInternal = false;

  static Area* create(const std::string& name, off_t size, const bi::permissions& perms);
  static Area* open(const std::string& name, bool readOnly);
  static off_t size(const Area& area);
  static void* address(const Area& area);

  // Grows the object named by inc bytes and hands back a fresh mapping of it.
  // The caller must have dropped every mapping it has of the object first:
  // none of them is valid at the size the object no longer has.
  static Area* grow(const std::string& name, off_t inc);

  // Anything big enough will do: the source goes in the front and the tail is
  // zeroed, which is what a freshly created object would have given
  static bool overwrite(Area& area, off_t size, const void* src, off_t srcSize);
};

/** @brief A shared memory segment that is replaced instead of modified.
 *
 * The segment is split into a fixed-size metadata area and a data area:
 *
 *  - the metadata area lives at `keyBase + 1` and holds the id of the current
 *    data area;
 *  - a data area is named for `keyBase + 2` and its own id, so every version
 *    ever published has a name of its own and a name never comes to mean a
 *    different object.
 *
 * `keyBase + 0` is left unused. The rwlock of the master segment table used to
 * be there, out of the same flat namespace as these (see ShmKeys::keyToName);
 * nothing owns it now, but the layout stays as it is rather than shifting
 * every key down by one.
 *
 * A reader calls pin(), which maps the data area the metadata area points at if
 * the one already mapped is stale, and hands back a handle to it. A writer never
 * modifies that area. Instead it calls beginUpdate(), which copies the current
 * data area into the next slot, changes the copy, and then publishes it with
 * publishUpdate(). Because the copy is not reachable by anybody until it is
 * published, growing it is safe - unlike growing a segment other processes have
 * mapped - and a writer that dies halfway through leaves the published data area
 * untouched.
 *
 * Reclamation is on the kernel. Publishing a new area unlinks the name of an
 * older one, but an shm object outlives its name for as long as anybody has it
 * mapped, so a reader holding the handle keeps reading the version it opened
 * and the pages go back when the last handle in any process is dropped. That is
 * inode counting, done by the kernel, correct across a process being killed
 * - which is why there is no reader bookkeeping here at all, no counts to take,
 * nothing to wait for and nothing a dead reader can hold up.
 *
 * What it costs is that a publication cannot write into an object that already
 * exists: it makes a new one, so the kernel allocates and zeroes its pages.
 * That is the price of not having to know who is reading.
 *
 * Only the two policies below are instantiated , explicitly, in brmshmimpl.cpp
 */
template <typename AreaPolicy>
class BRMVersionedShmImplT : public BRMVersionedShmBase
{
 public:
  using Area = typename AreaPolicy::Area;

  /** @brief A data area kept mapped for as long as the handle is held.
   *
   * Copyable and cheap to copy. Holding one says nothing about what the area
   * contains: a newer one may well have been published since, and a reader that
   * wants to see it simply takes a fresh pin
   */
  using DataAreaPin = std::shared_ptr<Area>;

  BRMVersionedShmImplT(unsigned keyBase, off_t initialSize, bool readOnly = false);
  ~BRMVersionedShmImplT() override;

  // Reader side. Safe to call concurrently with a writer in another thread of
  // this process, and with writers in other processes

  // The id of the data area this process has mapped, 0 if there is none.
  uint64_t mappedId() const;

  // Maps the published data area if the one we have is stale. Returns true if remapped.
  bool refresh();

  // Refreshes and hands back the published data area, which stays mapped until
  // the returned handle - and every copy of it - is gone. Null if nothing has
  // been published yet. This is the only thing a reader needs: the refresh and
  // the handing back happen as one step, so the area pinned is the area that
  // was just checked.
  DataAreaPin pin();

  // The image inside a pinned area. Null for a null pin.
  static void* imageIn(const DataAreaPin& area)
  {
    return area ? AreaPolicy::address(*area) : nullptr;
  }

  // Writer side

  // The area to work with: the copy while an update is open, the published one
  // otherwise. For writers only, and only where nothing else can publish or
  // refresh meanwhile - the caller's own exclusion, the extent map's write lock
  // say - because the pointer is this process's current mapping and says
  // nothing about how long it stays mapped. A reader must go through pin().
  Area* segment() const
  {
    return fUpdateSegment ? fUpdateSegment.get() : fSegment.get();
  }

  // The image of segment(), with the same caveats.
  void* image() const
  {
    auto* area = segment();
    return area ? AreaPolicy::address(*area) : nullptr;
  }

  // The size of segment()'s image in bytes, 0 if there is none.
  off_t imageSize() const
  {
    auto* area = segment();
    return area ? AreaPolicy::size(*area) : 0;
  }

  // This thread's, not the process's. A thread that has not opened an update
  // must not be shown one another thread is in the middle of: it would work on
  // that copy and then publish it, which is two threads publishing the same
  // update. See ownsUpdate()
  bool inUpdate() const
  {
    return fUpdateSegment != nullptr && ownsUpdate();
  }

  // Takes the update mutex, then allocates a new data area of at least minSize
  // bytes and copies the current one into it
  void beginUpdate(off_t minSize);

  // Enlarges the not-yet-published copy by incSize bytes. Invalidates pointers into it
  void growUpdate(off_t incSize);

  // Atomically points the metadata area at the copy and drops the update
  // mutex. Returns the id now published
  uint64_t publishUpdate();

  // Throws the copy away and drops the update mutex, leaving the published
  // data area as it was
  void discardUpdate();

  void setReadOnly() override;
  void destroy() override;

 private:
  // Throws the slot's object away and makes a new one, bumping the slot's
  // incarnation so that everybody else drops the mapping they kept of it
  DataAreaPin createDataArea(uint64_t id, off_t size);

  // Writes the copy straight into the object already in the slot. Null if
  // that cannot be done - somebody is reading it, this process has no current
  // mapping of it, or what is there is the wrong size - and the caller falls
  // back on createDataArea()

  DataAreaPin reuseSlotFor(uint64_t newId, off_t createSize, const void* src, off_t srcSize);

  // This process's mapping of the slot if it is of the object that is there
  // now, null otherwise. fSegmentMutex must be held
  DataAreaPin mappingForSlotLocked(uint64_t slot, uint64_t incarnation) const;
  void cacheSlotLocked(uint64_t slot, uint64_t incarnation, const DataAreaPin& area);
  void forgetSlotLocked(uint64_t slot);

  // refresh(), with fSegmentMutex already held
  bool refreshLocked();

  // Makes area the one this process hands out, retiring whatever was there.
  void adoptLocked(const DataAreaPin& area, uint64_t id);

  // Guards the published mapping below - and fKey/fSize, which describe it -
  // against a thread of this process publishing while another one reads. The
  // update mutex in the metadata area is always taken first where both are
  // needed. A writer's own copy needs no guarding: nobody else can reach it.
  mutable std::mutex fSegmentMutex;

  // The published data area, as mapped by this process. Held by shared_ptr so
  // that publishing can hand out a newer area without pulling this one out from
  // under a reader that is still walking it; see pin().
  DataAreaPin fSegment;
  uint64_t fMappedId = 0;

  // The copy an open update writes into. Nobody else can reach it: its id has
  // not been published. It may be the object already in the slot, written into
  // again, in which case the handle is shared with fSlotMappings.
  DataAreaPin fUpdateSegment;
  uint64_t fUpdateId = 0;

  /** @brief What this process has mapped of one slot.
   *
   * A slot's object is written into again whenever it can be, so a mapping of
   * it stays good across publications and is worth keeping - that is what
   * spares a reader the open and the page faults every time a version is
   * published. The incarnation says which object the mapping is of, so one
   * left over from an object that has since been replaced is recognized and
   * dropped rather than read.
   */
  struct SlotMapping
  {
    uint64_t incarnation = 0;
    DataAreaPin area;
  };

  SlotMapping fSlotMappings[DataAreaSlots];  // guarded by fSegmentMutex
};

extern template class BRMVersionedShmImplT<ManagedAreaPolicy>;
extern template class BRMVersionedShmImplT<RawAreaPolicy>;

// A versioned segment holding a boost managed segment, for the containers.
using BRMVersionedShmImpl = BRMVersionedShmImplT<ManagedAreaPolicy>;
// A versioned segment holding a raw image, for the array-shaped structures.
using BRMVersionedRawShmImpl = BRMVersionedShmImplT<RawAreaPolicy>;

}  // namespace BRM

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
 * $Id: vss.h 1926 2013-06-30 21:18:14Z wweeks $
 *
 *****************************************************************************/

/** @file
 * class XXX interface
 */

#pragma once

#include <set>
//#define NDEBUG
#include <cassert>
#include <boost/thread.hpp>

#include "brmshmimpl.h"

#include "brmtypes.h"
#include "shmkeys.h"
#include "hasher.h"

#ifdef NONE
#undef NONE
#endif
#ifdef READ
#undef READ
#endif
#ifdef WRITE
#undef WRITE
#endif

// These config parameters need to be loaded

// will get a small hash function performance boost by using powers of 2
#define VSSSTORAGE_INITIAL_COUNT 200000
#define VSSSTORAGE_INITIAL_SIZE (VSSSTORAGE_INITIAL_COUNT * sizeof(VSSEntry))
#define VSSSTORAGE_INCREMENT_COUNT 20000
#define VSSSTORAGE_INCREMENT (VSSSTORAGE_INCREMENT_COUNT * sizeof(VSSEntry))

// (average list length = 4)
#define VSSTABLE_INITIAL_SIZE (50000 * sizeof(int))
#define VSSTABLE_INCREMENT (5000 * sizeof(int))

#define EXPORT

namespace BRM
{
struct VSSEntry
{
  LBID_t lbid;
  VER_t verID;
  bool vbFlag;
  bool locked;
  int next;
#ifndef __LP64__
  uint32_t pad1;
#endif
  EXPORT VSSEntry();
};

struct VSSShmsegHeader
{
  int capacity;
  int currentSize;
  int LWM;
  int numHashBuckets;
  int lockedEntryCount;

  //  the rest of the overlay looks like this
  // 	int hashBuckets[numHashBuckets];
  // 	VSSEntry storage[capacity];
};

/** @brief Where the two arrays of a VSS image live, given its header.
 *
 * The storage array sits behind the hash table, which is sized by a header
 * field, so where it starts depends on the bucket count and neither array can
 * be remembered across a change to it. Derived rather than cached anywhere but
 * in VSS's own members, which it refreshes whenever it touches those counts.
 */
struct VSSLayout
{
  int* hashBuckets;
  VSSEntry* storage;
};

inline VSSLayout vssLayoutOf(VSSShmsegHeader* header)
{
  char* base = reinterpret_cast<char*>(header);
  const size_t bucketsEnd = sizeof(VSSShmsegHeader) + header->numHashBuckets * sizeof(int);

  return {reinterpret_cast<int*>(base + sizeof(VSSShmsegHeader)),
          reinterpret_cast<VSSEntry*>(base + bucketsEnd)};
}

/// How large an image holding that many hash buckets and entries has to be.
inline off_t vssImageSize(int numHashBuckets, int capacity)
{
  return static_cast<off_t>(sizeof(VSSShmsegHeader)) + numHashBuckets * static_cast<off_t>(sizeof(int)) +
         capacity * static_cast<off_t>(sizeof(VSSEntry));
}

class QueryContext_vss
{
 public:
  QueryContext_vss() : currentScn(0)
  {
    txns.reset(new std::set<VER_t>());
  }
  QueryContext_vss(const QueryContext& qc);
  VER_t currentScn;
  boost::shared_ptr<std::set<VER_t> > txns;
};

/** @brief The VSS's image, in a versioned shared memory segment.
 *
 * The same scheme the extent map, its free list and the VBBM use: a reader
 * works off the data area the metadata area currently points at and holds it
 * mapped with a pin, while a writer copies that area, changes the copy and
 * publishes it with one atomic store. See BRMVersionedShmImplT.
 */
class VSSImpl
{
 public:
  using DataAreaPin = BRMVersionedRawShmImpl::DataAreaPin;

  static VSSImpl* makeVSSImpl(unsigned keyBase, off_t size, bool readOnly = false);

  /// The id of the published data area; 0 if nothing has been published yet.
  inline uint64_t currentId() const
  {
    return fVSS.currentId();
  }
  inline bool refresh()
  {
    return fVSS.refresh();
  }
  /// Keeps the published data area mapped for as long as the pin is held.
  inline DataAreaPin pin()
  {
    return fVSS.pin();
  }

  /** @brief Freezes updates to this segment without making one.
   *
   * What save() holds while it pins a snapshot, in place of a read lock on a
   * table nobody writes any more. Held for the length of a pin, not the length
   * of a file write.
   */
  inline void lockUpdates()
  {
    fVSS.lockUpdates();
  }
  inline void unlockUpdates()
  {
    fVSS.unlockUpdates();
  }
  /// The header inside a pinned area, null for a null pin.
  static inline VSSShmsegHeader* headerIn(const DataAreaPin& area)
  {
    return static_cast<VSSShmsegHeader*>(BRMVersionedRawShmImpl::imageIn(area));
  }

  // The write side. All of these want the VSS write lock held.
  inline bool inUpdate() const
  {
    return fVSS.inUpdate();
  }
  inline void beginUpdate(off_t minSize)
  {
    fVSS.beginUpdate(minSize);
  }
  inline uint64_t publishUpdate()
  {
    return fVSS.publishUpdate();
  }
  inline void discardUpdate()
  {
    fVSS.discardUpdate();
  }
  /// Grows the copy the open update works on so its image is at least size bytes.
  void growUpdateTo(off_t size);

  inline void makeReadOnly()
  {
    fVSS.setReadOnly();
  }
  inline unsigned key() const
  {
    return fVSS.key();
  }

  /** @brief The header of the image currently being worked on.
   *
   * That is the copy while an update is open and the published area otherwise,
   * which makes this the writer's accessor. A reader has to go through pin()
   * and headerIn(): what this returns can move out from under it at any time.
   */
  inline VSSShmsegHeader* get() const
  {
    return static_cast<VSSShmsegHeader*>(fVSS.image());
  }
  /// The size of get()'s image in bytes, with the same caveat.
  inline off_t imageSize() const
  {
    return fVSS.imageSize();
  }

 private:
  VSSImpl(unsigned keyBase, off_t size, bool readOnly = false);
  ~VSSImpl() = default;
  VSSImpl(const VSSImpl& rhs);
  VSSImpl& operator=(const VSSImpl& rhs);

  BRMVersionedRawShmImpl fVSS;

  static boost::mutex fInstanceMutex;
  static VSSImpl* fInstance;
};

class VBBM;
class ExtentMap;

/** @brief The Version Substitution Structure (VSS)
 *
 * At a high level, the VSS maintains a table that associates an LBID with
 * a version number and 2 flags that indicate whether or not the block
 * identified by the <LBID, VerID> pair exists in the main database files
 * or the Version Buffer.  The VSS's main purpose is to resolve the version of
 * a specified block the caller can safely use given that there may be concurrent
 * writes to that block.
 *
 * As implemented, it is a hash table and a set of lists that exist in
 * shared memory.  The hash table is keyed by LBID, and
 * each valid entry points to the head of a unique list.  Each list element
 * contains the LBID, VerID, & the two flags that encapsulate "an entry in the
 * VSS table".  Every list contains all elements that collide on that hash table
 * entry that points to it, "load factor" has no bearing on performance,
 * and lists can grow arbitrarily large.
 * Technically lookups are O(n), but in normal circumstances it'll
 * be constant time.  As things are right now, we expect there to be about
 * 200k VSS entries.  The hash table is sized such that on average there will be 4
 * entries per list when it's at capacity.
 *
 * The memory management & structure manipulation code is nearly identical
 * to that in the VBBM, so any bugs found here are likely there as well.
 *
 * The shared memory segment is versioned rather than modified in place, as the
 * ExtentMap's and the VBBM's are: a writer copies the published data area,
 * changes and if need be grows the copy, and makes it current with one atomic
 * store.  Readers hold a pin on the area they are walking, so an area a publish
 * has retired is unmapped only once the last reader of it lets go.
 */

class VSS
{
 public:
  enum OPS
  {
    NONE,
    READ,
    WRITE
  };

  EXPORT VSS();
  EXPORT ~VSS();

  EXPORT bool isLocked(const LBIDRange& l, VER_t txnID = -1) const;
  EXPORT void removeEntry(LBID_t lbid, VER_t verID, std::vector<LBID_t>* flushList);

  // Note, the use_vbbm switch should be used for unit testing the VSS only
  EXPORT void removeEntriesFromDB(const LBIDRange& range, VBBM& vbbm, bool use_vbbm = true);
  EXPORT int lookup(LBID_t lbid, const QueryContext_vss&, VER_t txnID, VER_t* outVer, bool* vbFlag,
                    bool vbOnly = false) const;

  /// Returns the version in the main DB files
  EXPORT VER_t getCurrentVersion(LBID_t lbid, bool* isLocked) const;  // returns the ver in the main DB files

  /// Returns the highest version in the version buffer, less than max
  EXPORT VER_t getHighestVerInVB(LBID_t lbid, VER_t max) const;

  /// returns true if that block is in the version buffer, false otherwise
  EXPORT bool isVersioned(LBID_t lbid, VER_t version) const;

  EXPORT void setVBFlag(LBID_t lbid, VER_t verID, bool vbFlag);
  EXPORT void insert(LBID_t, VER_t, bool vbFlag, bool locked);
  EXPORT void commit(VER_t txnID);
  EXPORT void getUncommittedLBIDs(VER_t txnID, std::vector<LBID_t>& lbids);
  EXPORT void getUnlockedLBIDs(BlockList_t& lbids);
  EXPORT void getLockedLBIDs(BlockList_t& lbids);
  EXPORT void lock(OPS op);
  EXPORT void release(OPS op);
  EXPORT void setReadOnly();

  EXPORT int checkConsistency(const VBBM& vbbm, ExtentMap& em) const;
  EXPORT int size() const;
  EXPORT bool hashEmpty() const;
  EXPORT void getCurrentTxnIDs(std::set<VER_t>& txnList) const;

  EXPORT void clear();
  EXPORT void load(std::string filename);
  EXPORT void save(std::string filename);

  EXPORT void confirmChanges();

  /** @brief Whether a write transaction is still open on this structure.
   *
   * An open update holds the segment's update mutex, so one left behind by a
   * transaction that was never confirmed or rolled back wedges every writer
   * in the cluster. This is how the worker notices it has one.
   */
  EXPORT bool hasOpenUpdate() const;
  EXPORT void undoChanges();

  /** @brief Publishes an empty data area if nothing ever has been.
   *
   * lock(READ) does this itself, but it needs the write lock for it, which a
   * caller already holding this table's read lock cannot ask for. Calling this
   * first leaves the bootstrap inside lock(READ) a no-op. See DBRM::saveState().
   */
  EXPORT void ensureDataArea();

  /** @brief Takes this table's read lock, excluding writers across a save.
   *
   * lock(READ) takes no lock - a reader is kept safe by its pin, which says
   * nothing about which published version it caught - so it cannot make two
   * structures read as of one instant. This can, and is what DBRM::saveState()
   * holds so the files it writes agree with each other. Wants ensureDataArea()
   * called beforehand, and to be taken in the order writers take the write
   * locks. See DBRM::saveState().
   */
  EXPORT void lockForSave();
  EXPORT void unlockForSave();

  /// Whether the published VSS holds no entries. Takes no lock.
  EXPORT bool isEmpty();

  /* Bug 2293.  VBBM will use this fcn to determine whether a block is
   * currently in use. */
  EXPORT bool isEntryLocked(LBID_t lbid, VER_t verID) const;
  EXPORT bool isTooOld(LBID_t lbid, VER_t verID) const;

 private:
  VSS(const VSS&);
  VSS& operator=(const VSS&);

  /* ------------------------------------------------------------------------ *
   * This thread's view of the data area.
   *
   * Per thread, not per object: a read grab is a pin plus a nesting depth that
   * whoever took them has to balance, and the pointers are only valid while the
   * pin is held. Sharing that between threads does not work - a lost decrement
   * leaves the depth above zero for good, after which the outermost grab never
   * fires again and the thread reads one data area until the process ends, and
   * a lost increment drops the pin while another thread is still in the area.
   * One DBRM per thread pool is the normal arrangement here, so the objects
   * below really are reached from many threads at once; see ExtentMap's copy of
   * this note for the longer version.
   * ------------------------------------------------------------------------ */

  static thread_local struct VSSShmsegHeader* vss;
  static thread_local int* hashBuckets;
  static thread_local VSSEntry* storage;

  /// Held for as long as a read lock is, keeping the area vss points into mapped.
  static thread_local VSSImpl::DataAreaPin fDataAreaPin;
  /// Nesting depth of the read locks this thread holds; the pin drops at 0.
  static thread_local uint32_t fReadDepth;

  bool r_only;
  static boost::mutex mutex;  // @bug5355 - made mutex static

  static const int MAX_IO_RETRIES = 10;

  void growVSS();
  void growForLoad(int count);
  void initShmseg();

  /// Points hashBuckets/storage into the image header describes.
  void setPointers(VSSShmsegHeader* header);
  int hashIndexOf(LBID_t lbid) const;
  /// Rebuilds the hash table from the entries in storage, keeping their indices.
  void rehash();

  void createImplIfNeeded();
  void initDataAreaIfNeeded();
  void beginUpdate();
  void publishUpdate();
  void discardUpdate();

  int getIndex(LBID_t lbid, VER_t verID, int& prev, int& bucket) const;
  void _insert(VSSEntry& e);
  ShmKeys fShmKeys;

  VSSImpl* fPVSSImpl;
  utils::Hasher hasher;
};

}  // namespace BRM

#undef EXPORT

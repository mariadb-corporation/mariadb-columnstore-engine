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
 * $Id: copylocks.h 1936 2013-07-09 22:10:29Z dhall $
 *
 *****************************************************************************/

/** @file
 * class XXX interface
 */

#pragma once

#include <set>
#include <sys/types.h>
//#define NDEBUG
#include <cassert>

#include <boost/thread.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include "brmtypes.h"

#include "shmkeys.h"

#include "brmshmimpl.h"

/* Should load these from a config file */
#define CL_INITIAL_COUNT 50
#define CL_INCREMENT_COUNT 50

#define EXPORT

namespace idbdatafile
{
class IDBDataFile;
}

namespace BRM
{
struct CopyLockEntry
{
  LBID_t start;
  int size;
  VER_t txnID;
  EXPORT CopyLockEntry();
};

/** @brief The header at the front of a CopyLocks image.
 *
 * There used to be none: the region was a bare CopyLockEntry[] and every one of
 * the seven operations rederived the entry count from a size counter in the MST
 * entry. Two of those operations run without the write lock, which is
 * exactly what the getTable_noLock() contract says a reader must not do, and
 * none of it works at all once the image is a data area that a publish can
 * replace. The image says how big it is and how much of it is in use instead.
 */
struct CLShmsegHeader
{
  int capacity;     // entries the image has room for
  int currentSize;  // how many of them are in use

  // the rest of the overlay looks like this
  // 	CopyLockEntry entries[capacity];
};

inline CopyLockEntry* clEntriesOf(CLShmsegHeader* header)
{
  return reinterpret_cast<CopyLockEntry*>(reinterpret_cast<char*>(header) + sizeof(CLShmsegHeader));
}

/// How large an image holding that many entries has to be.
inline off_t clImageSize(int capacity)
{
  return static_cast<off_t>(sizeof(CLShmsegHeader)) + capacity * static_cast<off_t>(sizeof(CopyLockEntry));
}

/** @brief The copy locks' image, in a versioned shared memory segment.
 *
 * The same scheme the extent map, its free list, the VBBM and the VSS use: a
 * reader works off the data area the metadata area currently points at and
 * holds it mapped with a pin, while a writer copies that area, changes the copy
 * and publishes it with one atomic store. See BRMVersionedShmImplT.
 */
class CopyLocksImpl
{
 public:
  using DataAreaPin = BRMVersionedRawShmImpl::DataAreaPin;

  static CopyLocksImpl* makeCopyLocksImpl(unsigned keyBase, off_t size, bool readOnly = false);

  /// The id of the published data area; 0 if nothing has been published yet.
  inline uint64_t currentId() const
  {
    return fCopyLocks.currentId();
  }
  inline bool refresh()
  {
    return fCopyLocks.refresh();
  }
  /// Keeps the published data area mapped for as long as the pin is held.
  inline DataAreaPin pin()
  {
    return fCopyLocks.pin();
  }

  /** @brief Freezes updates to this segment without making one.
   *
   * What save() holds while it pins a snapshot, in place of a read lock on a
   * table nobody writes any more. Held for the length of a pin, not the length
   * of a file write.
   */
  inline void lockUpdates()
  {
    fCopyLocks.lockUpdates();
  }
  inline void unlockUpdates()
  {
    fCopyLocks.unlockUpdates();
  }
  /// The header inside a pinned area, null for a null pin.
  static inline CLShmsegHeader* headerIn(const DataAreaPin& area)
  {
    return static_cast<CLShmsegHeader*>(BRMVersionedRawShmImpl::imageIn(area));
  }

  // The write side. All of these want the CopyLocks write lock held.
  inline bool inUpdate() const
  {
    return fCopyLocks.inUpdate();
  }
  inline void beginUpdate(off_t minSize)
  {
    fCopyLocks.beginUpdate(minSize);
  }
  inline uint64_t publishUpdate()
  {
    return fCopyLocks.publishUpdate();
  }
  inline void discardUpdate()
  {
    fCopyLocks.discardUpdate();
  }
  /// Grows the copy the open update works on so its image is at least size bytes.
  void growUpdateTo(off_t size);

  inline void makeReadOnly()
  {
    fCopyLocks.setReadOnly();
  }
  inline unsigned key() const
  {
    return fCopyLocks.key();
  }

  /** @brief The header of the image currently being worked on.
   *
   * That is the copy while an update is open and the published area otherwise,
   * which makes this the writer's accessor. A reader has to go through pin()
   * and headerIn(): what this returns can move out from under it at any time.
   */
  inline CLShmsegHeader* get() const
  {
    return static_cast<CLShmsegHeader*>(fCopyLocks.image());
  }
  /// The size of get()'s image in bytes, with the same caveat.
  inline off_t imageSize() const
  {
    return fCopyLocks.imageSize();
  }

 private:
  CopyLocksImpl(unsigned keyBase, off_t size, bool readOnly = false);
  ~CopyLocksImpl() = default;
  CopyLocksImpl(const CopyLocksImpl& rhs);
  CopyLocksImpl& operator=(const CopyLocksImpl& rhs);

  BRMVersionedRawShmImpl fCopyLocks;

  static boost::mutex fInstanceMutex;
  static CopyLocksImpl* fInstance;
};

/** @brief The ranges of the version buffer a transaction is copying into.
 *
 * The shared memory segment is versioned rather than modified in place, as the
 * other five BRM structures' are: a writer copies the published data area,
 * changes and if need be grows the copy, and makes it current with one atomic
 * store.  Readers hold a pin on the area they are walking, so an area a publish
 * has retired is unmapped only once the last reader of it lets go - and take no
 * lock at all.
 */
class CopyLocks
{
 public:
  enum OPS
  {
    NONE,
    READ,
    WRITE
  };

  EXPORT CopyLocks();
  EXPORT ~CopyLocks();

  EXPORT void lockRange(const LBIDRange& range, VER_t txnID);
  EXPORT void releaseRange(const LBIDRange& range);
  EXPORT bool isLocked(const LBIDRange& range) const;
  EXPORT void rollback(VER_t txnID);

  EXPORT void lock(OPS op);
  EXPORT void release(OPS op);
  EXPORT void setReadOnly();
  EXPORT void getCurrentTxnIDs(std::set<VER_t>& txnList) const;

  EXPORT void forceRelease(const LBIDRange& range);

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

 private:
  CopyLocks(const CopyLocks&);
  CopyLocks& operator=(const CopyLocks&);

  /// Makes room in the copy for CL_INCREMENT_COUNT more entries.
  void growCL();

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

  static thread_local CLShmsegHeader* header;
  static thread_local CopyLockEntry* entries;

  /// Held for as long as a read lock is, keeping the area entries points into mapped.
  static thread_local CopyLocksImpl::DataAreaPin fDataAreaPin;
  /// Nesting depth of the read locks this thread holds; the pin drops at 0.
  static thread_local uint32_t fReadDepth;

  bool r_only;
  static boost::mutex mutex;
  static const int MAX_IO_RETRIES = 10;
  ShmKeys fShmKeys;
  CopyLocksImpl* fCopyLocksImpl;

  /// Points header and entries at the image h is the header of.
  void setPointers(CLShmsegHeader* h);
  /// Lays out an empty image at the initial size in the copy.
  void initShmseg();

  void createImplIfNeeded();
  void initDataAreaIfNeeded();
  void beginUpdate();
  void publishUpdate();
  void discardUpdate();
};

}  // namespace BRM

#undef EXPORT

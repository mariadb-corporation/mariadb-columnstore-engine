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
 * $Id: vbbm.h 1926 2013-06-30 21:18:14Z wweeks $
 *
 *****************************************************************************/

/** @file
 * class XXX interface
 */

#pragma once

#include <vector>
// #define NDEBUG
#include <cassert>
#include <boost/thread.hpp>

#include "brmshmimpl.h"

#include "shmkeys.h"
#include "brmtypes.h"

// These config parameters need to be loaded

// will get a small hash function performance boost by using powers of 2
#define VBSTORAGE_INITIAL_COUNT 100000
#define VBSTORAGE_INITIAL_SIZE (VBSTORAGE_INITIAL_COUNT * sizeof(VBBMEntry))
#define VBSTORAGE_INCREMENT_COUNT 10000
#define VBSTORAGE_INCREMENT (VBSTORAGE_INCREMENT_COUNT * sizeof(VBBMEntry))

// (average list length = 4)
#define VBTABLE_INITIAL_SIZE (25000 * sizeof(int))
#define VBTABLE_INCREMENT (2500 * sizeof(int))

#define EXPORT

namespace idbdatafile
{
class IDBDataFile;
}

namespace BRM
{
class VSS;

struct VBFileMetadata
{
  OID_t OID;
  uint64_t fileSize;
  uint64_t nextOffset;
};

struct VBBMEntry
{
  LBID_t lbid;
  VER_t verID;
  OID_t vbOID;
  uint32_t vbFBO;
  int next;
  EXPORT VBBMEntry();
};

struct VBShmsegHeader
{
  int nFiles;
  int vbCapacity;
  int vbCurrentSize;
  int vbLWM;
  int numHashBuckets;

  // the rest of the overlay looks like this
  // 	VBFileMetadata files[nFiles];
  // 	int hashBuckets[numHashBuckets];
  // 	VBBMEntry storage[vbCapacity];
};

/** @brief Where the three arrays of a VBBM image live, given its header.
 *
 * Each of them is sized by a header field, so where the later ones start
 * depends on the earlier ones' counts and none of the three can be remembered
 * across a change to the header. Derived rather than cached anywhere but in
 * VBBM's own members, which it refreshes whenever it touches those counts.
 */
struct VBBMLayout
{
  VBFileMetadata* files;
  int* hashBuckets;
  VBBMEntry* storage;
};

inline VBBMLayout vbbmLayoutOf(VBShmsegHeader* header)
{
  char* base = reinterpret_cast<char*>(header);
  const size_t filesEnd = sizeof(VBShmsegHeader) + header->nFiles * sizeof(VBFileMetadata);
  const size_t bucketsEnd = filesEnd + header->numHashBuckets * sizeof(int);

  return {reinterpret_cast<VBFileMetadata*>(base + sizeof(VBShmsegHeader)),
          reinterpret_cast<int*>(base + filesEnd), reinterpret_cast<VBBMEntry*>(base + bucketsEnd)};
}

/// How large an image holding that many files, hash buckets and entries has to be.
inline off_t vbbmImageSize(int nFiles, int numHashBuckets, int vbCapacity)
{
  return static_cast<off_t>(sizeof(VBShmsegHeader)) + nFiles * static_cast<off_t>(sizeof(VBFileMetadata)) +
         numHashBuckets * static_cast<off_t>(sizeof(int)) +
         vbCapacity * static_cast<off_t>(sizeof(VBBMEntry));
}

/** @brief The VBBM's image, in a versioned shared memory segment.
 *
 * The same scheme the extent map and its free list use: a reader works off the
 * data area the metadata area currently points at and holds it mapped with a
 * pin, while a writer copies that area, changes the copy and publishes it with
 * one atomic store. See BRMVersionedShmImplT.
 */
class VBBMImpl
{
 public:
  using DataAreaPin = BRMVersionedRawShmImpl::DataAreaPin;

  static VBBMImpl* makeVBBMImpl(unsigned keyBase, off_t size, bool readOnly = false);

  /// The id of the published data area; 0 if nothing has been published yet.
  inline uint64_t currentId() const
  {
    return fVBBM.currentId();
  }
  inline bool refresh()
  {
    return fVBBM.refresh();
  }
  /// Keeps the published data area mapped for as long as the pin is held.
  inline DataAreaPin pin()
  {
    return fVBBM.pin();
  }

  /** @brief Freezes updates to this segment without making one.
   *
   * What save() holds while it pins a snapshot, in place of a read lock on a
   * table nobody writes any more. Held for the length of a pin, not the length
   * of a file write.
   */
  inline void lockUpdates()
  {
    fVBBM.lockUpdates();
  }
  inline void unlockUpdates()
  {
    fVBBM.unlockUpdates();
  }
  /// The header inside a pinned area, null for a null pin.
  static inline VBShmsegHeader* headerIn(const DataAreaPin& area)
  {
    return static_cast<VBShmsegHeader*>(BRMVersionedRawShmImpl::imageIn(area));
  }

  // The write side. All of these want the VBBM write lock held.
  inline bool inUpdate() const
  {
    return fVBBM.inUpdate();
  }
  inline void beginUpdate(off_t minSize)
  {
    fVBBM.beginUpdate(minSize);
  }
  inline uint64_t publishUpdate()
  {
    return fVBBM.publishUpdate();
  }
  inline void discardUpdate()
  {
    fVBBM.discardUpdate();
  }
  /// Grows the copy the open update works on so its image is at least size bytes.
  void growUpdateTo(off_t size);

  inline void makeReadOnly()
  {
    fVBBM.setReadOnly();
  }
  inline unsigned key() const
  {
    return fVBBM.key();
  }

  /** @brief The header of the image currently being worked on.
   *
   * That is the copy while an update is open and the published area otherwise,
   * which makes this the writer's accessor. A reader has to go through pin()
   * and headerIn(): what this returns can move out from under it at any time.
   */
  inline VBShmsegHeader* get() const
  {
    return static_cast<VBShmsegHeader*>(fVBBM.image());
  }
  /// The size of get()'s image in bytes, with the same caveat.
  inline off_t imageSize() const
  {
    return fVBBM.imageSize();
  }

 private:
  VBBMImpl(unsigned keyBase, off_t size, bool readOnly = false);
  ~VBBMImpl() = default;
  VBBMImpl(const VBBMImpl& rhs);
  VBBMImpl& operator=(const VBBMImpl& rhs);

  BRMVersionedRawShmImpl fVBBM;

  static boost::mutex fInstanceMutex;
  static VBBMImpl* fInstance;
};

/** @brief The Version Buffer Block Map (VBBM)
 *
 * At a high level, the VBBM maintains a table describing the contents of
 * the Version Buffer.  For every entry in the Version Buffer, it associates its
 * <LBID, VerID> identifier with the OID and offset it is stored at.
 *
 * As implemented, it is a hash table and a set of lists that exist in
 * shared memory.  The hash table is keyed by <LBID, VerID>, and
 * each valid entry points to the head of a unique list.  Each list element
 * contains the LBID, VerID, VB OID, and VB offset that encapsulate "an entry in the
 * VBBM table".  Every list contains all elements that collide on that hash table
 * entry that points to it, "load factor" has no bearing on performance,
 * and lists can grow arbitrarily large.
 * Technically lookups are O(n), but in normal circumstances it'll
 * be constant time.  As things are right now, we expect there to be about
 * 100k VBBM entries.  The hash table is sized such that on average there will be 4
 * entries per list when it's at capacity.
 *
 * The memory management & structure manipulation code is nearly identical
 * to that in the VSS, so any bugs found here are likely there as well.
 *
 * The shared memory segment is versioned rather than modified in place, as the
 * ExtentMap's is: a writer copies the published data area, changes and if need
 * be grows the copy, and makes it current with one atomic store.  Readers hold
 * a pin on the area they are walking, so an area a publish has retired is
 * unmapped only once the last reader of it lets go.
 */

class VBBM
{
 public:
  enum OPS
  {
    NONE,
    READ,
    WRITE
  };

  EXPORT VBBM();
  EXPORT ~VBBM();

  EXPORT void lock(OPS op);
  EXPORT void release(OPS op);
  EXPORT int lookup(LBID_t lbid, VER_t ver, OID_t& oid, uint32_t& fbo) const;
  EXPORT void insert(LBID_t lbid, VER_t ver, OID_t oid, uint32_t fbo);
  EXPORT void getBlocks(int num, OID_t vbOID, std::vector<VBRange>& vbRanges, VSS& vss, bool flushPMCache);
  EXPORT void removeEntry(LBID_t, VER_t ver);

  EXPORT int size() const;
  EXPORT bool hashEmpty() const;
  EXPORT int checkConsistency() const;
  EXPORT void setReadOnly();

  EXPORT void clear();
  EXPORT void load(std::string filename);
  EXPORT void loadVersion2(idbdatafile::IDBDataFile* in);
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

 private:
  VBBM(const VBBM&);
  VBBM& operator=(const VBBM&);

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

  static thread_local VBShmsegHeader* vbbm;
  static thread_local VBFileMetadata* files;
  static thread_local int* hashBuckets;
  static thread_local VBBMEntry* storage;

  /// Held for as long as a read lock is, keeping the area vbbm points into mapped.
  static thread_local VBBMImpl::DataAreaPin fDataAreaPin;
  /// Nesting depth of the read locks this thread holds; the pin drops at 0.
  static thread_local uint32_t fReadDepth;

  bool r_only;
  static boost::mutex mutex;  // @bug5355 - made mutex static
  static const int MAX_IO_RETRIES = 10;

  void growVBBM(bool addAFile = false);
  void growForLoad(int count);
  void initShmseg(int nFiles);

  /// Points files/hashBuckets/storage into the image header describes.
  void setPointers(VBShmsegHeader* header);
  int hashIndexOf(LBID_t lbid, VER_t verID) const;
  /// Rebuilds the hash table from the entries in storage, keeping their indices.
  void rehash();

  void createImplIfNeeded();
  void initDataAreaIfNeeded();
  void beginUpdate();
  void publishUpdate();
  void discardUpdate();

  void _insert(VBBMEntry& e);
  int getIndex(LBID_t lbid, VER_t verID, int& prev, int& bucket) const;
  ShmKeys fShmKeys;
  VBBMImpl* fPVBBMImpl;

  /* Shared nothing mods */
  uint64_t currentFileSize;
  void setCurrentFileSize();
  uint32_t addVBFileIfNotExists(OID_t vbOID);
};

}  // namespace BRM

#undef EXPORT

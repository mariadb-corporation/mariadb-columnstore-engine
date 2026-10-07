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
 * $Id: vbbm.cpp 1926 2013-06-30 21:18:14Z wweeks $
 *
 ****************************************************************************/

#include <iostream>
#include <vector>
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>

#include <values.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>

#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>

#include "blocksize.h"
#include "rwlock.h"
#include "brmtypes.h"
#include "vss.h"
#include "configcpp.h"
#include "exceptclasses.h"
#include "hasher.h"
#include "cacheutils.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"

#define VBBM_DLLEXPORT
#include "vbbm.h"
#undef VBBM_DLLEXPORT

#define VBBM_MAGIC_V1 0x7b27ec13
#define VBBM_MAGIC_V2 0x1fb58c7a
#define VBBM_CHUNK_SIZE 100

using namespace std;
using namespace boost;
using namespace idbdatafile;

namespace BRM
{
VBBMEntry::VBBMEntry()
{
  lbid = -1;
  verID = 0;
  vbOID = 0;
  vbFBO = 0;
  next = -1;
}

/*static*/
boost::mutex VBBMImpl::fInstanceMutex;
/* This thread's view of the data area; see the declarations for why it is per
   thread. Zero-initialized as every thread starts, so a thread that has never
   locked anything looks like one that has released everything. */
thread_local VBShmsegHeader* VBBM::vbbm = nullptr;
thread_local VBFileMetadata* VBBM::files = nullptr;
thread_local int* VBBM::hashBuckets = nullptr;
thread_local VBBMEntry* VBBM::storage = nullptr;
thread_local VBBMImpl::DataAreaPin VBBM::fDataAreaPin;
thread_local uint32_t VBBM::fReadDepth = 0;

boost::mutex VBBM::mutex;

/*static*/
VBBMImpl* VBBMImpl::fInstance = 0;

/*static*/
VBBMImpl* VBBMImpl::makeVBBMImpl(unsigned keyBase, off_t size, bool readOnly)
{
  boost::mutex::scoped_lock lk(fInstanceMutex);

  // The metadata area is at a fixed key and never moves, so the instance is
  // created once. Which data area it looks at is decided by refresh(), not here.
  if (!fInstance)
    fInstance = new VBBMImpl(keyBase, size, readOnly);

  return fInstance;
}

VBBMImpl::VBBMImpl(unsigned keyBase, off_t size, bool readOnly) : fVBBM(keyBase, size, readOnly)
{
}

void VBBMImpl::growUpdateTo(off_t size)
{
  off_t have = fVBBM.imageSize();

  if (size > have)
    fVBBM.growUpdate(size - have);
}

VBBM::VBBM()
{
  /* vbbm, files, hashBuckets, storage, the pin and the depth are deliberately
     not touched here: they belong to the calling thread rather than to this
     object, so clearing them would cut the ground from under a read lock that
     thread already holds on another VBBM. A thread starts with them zeroed. */
  r_only = false;
  fPVBBMImpl = 0;
  currentFileSize = 0;
}

VBBM::~VBBM()
{
}

/* Points the three array members at the arrays of the image header describes.
   Every change to nFiles or numHashBuckets moves the arrays behind it, so this
   has to run again after any of them. */
void VBBM::setPointers(VBShmsegHeader* header)
{
  const VBBMLayout layout = vbbmLayoutOf(header);

  vbbm = header;
  files = layout.files;
  hashBuckets = layout.hashBuckets;
  storage = layout.storage;
}

/* Lays an empty VBBM out in the image currently being worked on: the given
   number of version buffer files, an empty hash table and an empty entry array,
   both at their initial sizes. The files array itself is left alone - it sits
   ahead of the other two, so it survives this in place.

   The caller is responsible for the image being big enough to hold all three. */
void VBBM::initShmseg(int nFiles)
{
  vbbm->nFiles = nFiles;
  vbbm->vbCapacity = VBSTORAGE_INITIAL_SIZE / sizeof(VBBMEntry);
  vbbm->vbCurrentSize = 0;
  vbbm->vbLWM = 0;
  vbbm->numHashBuckets = VBTABLE_INITIAL_SIZE / sizeof(int);
  setPointers(vbbm);

  for (int i = 0; i < vbbm->numHashBuckets; i++)
    hashBuckets[i] = -1;

  for (int i = 0; i < vbbm->vbCapacity; i++)
    storage[i].lbid = -1;
}

/* Maps the metadata area. The impl is a process-wide singleton, hence the mutex. */
void VBBM::createImplIfNeeded()
{
  boost::mutex::scoped_lock lk(mutex);

  if (fPVBBMImpl)
    return;

  fPVBBMImpl = VBBMImpl::makeVBBMImpl(
      fShmKeys.KEYRANGE_VBBM_BASE,
      vbbmImageSize(0, VBTABLE_INITIAL_SIZE / sizeof(int), VBSTORAGE_INITIAL_SIZE / sizeof(VBBMEntry)),
      r_only);
  idbassert(fPVBBMImpl);

  if (r_only)
    fPVBBMImpl->makeReadOnly();
}

/* The first process to look at the VBBM finds no data area at all, the metadata
   area holding id 0. Publish an initialized, empty one so that a reader has
   something to map. A no-op from the first VBBM access of the cluster's life
   onwards, which is why it is the read path that carries it. */
void VBBM::initDataAreaIfNeeded()
{
  if (fPVBBMImpl->currentId() != 0)
    return;

  /* The caller is already making the first one - nothing to do, and taking
     the exclusion below would be taking it twice. */
  if (fPVBBMImpl->inUpdate())
    return;

  /* beginUpdate() takes the segment's update mutex, and that is the whole of
     the exclusion now. So the re-check goes under it rather than before it:
     another process may have published while we waited, in which case what
     beginUpdate() copied is theirs and there is nothing left to publish. */
  beginUpdate();

  try
  {
    if (fPVBBMImpl->currentId() == 0)
      publishUpdate();
    else
      discardUpdate();
  }
  catch (...)
  {
    discardUpdate();
    throw;
  }
}

/* Takes the copy the write transaction works on. Idempotent: a nested grab of
   the VBBM write lock keeps working on the copy that is already open. */
void VBBM::beginUpdate()
{
  bool alreadyOpen = fPVBBMImpl->inUpdate();

  if (!alreadyOpen)
    fPVBBMImpl->beginUpdate(
        vbbmImageSize(0, VBTABLE_INITIAL_SIZE / sizeof(int), VBSTORAGE_INITIAL_SIZE / sizeof(VBBMEntry)));

  /* Whether the copy came from nothing and so needs laying out. Only the call
     that opened the update can tell: it returns with the update mutex held, so
     the published id it reads back is exactly the one it copied from. */
  bool firstEver = !alreadyOpen && fPVBBMImpl->currentId() == 0;

  setPointers(fPVBBMImpl->get());

  if (firstEver)
    initShmseg(0);
}

/* Makes the copy the current data area. This is the only point at which the
   changes of a write transaction become visible, and it is a single atomic
   store, so a reader sees either all of them or none. */
void VBBM::publishUpdate()
{
  if (!fPVBBMImpl || !fPVBBMImpl->inUpdate())
    return;

  fPVBBMImpl->publishUpdate();
  setPointers(fPVBBMImpl->get());
}

/* Rollback the write transaction by dropping the copy; the published data
   area was never touched. */
void VBBM::discardUpdate()
{
  if (!fPVBBMImpl || !fPVBBMImpl->inUpdate())
    return;

  fPVBBMImpl->discardUpdate();
  // Back to whatever is published.
  fPVBBMImpl->refresh();
  setPointers(fPVBBMImpl->get());
}

/* Nothing to do: the changes are in a copy nobody else can see, and it is
   release(WRITE) that publishes it. Kept because SlaveDBRMNode names the two
   ends of a transaction explicitly. */
bool VBBM::hasOpenUpdate() const
{
  return fPVBBMImpl && fPVBBMImpl->inUpdate();
}

void VBBM::confirmChanges()
{
}

void VBBM::undoChanges()
{
  discardUpdate();
}

/* The bootstrap on its own, for a caller that is about to take this table's
   read lock and so cannot let lock(READ) reach for the write lock. A no-op once
   anything has ever been published, which is from the first access of the
   cluster's life onwards. */
void VBBM::ensureDataArea()
{
  createImplIfNeeded();
  initDataAreaIfNeeded();
}

/* The read lock on its own. A read of this class takes no lock - the pin is
   what keeps it safe - so nothing orders one read against another, let alone a
   read of this class against a read of a different one. This is what does, for
   the one caller that needs it; see DBRM::saveState(). */
/* The update mutex, not a read lock on the master segment table. It is the
   same exclusion a writer takes and it is held only while save() pins its
   snapshot, so the mutex's timeout is nowhere near it. createImplIfNeeded()
   because a caller reaches this before anything else has touched the VBBM. */
void VBBM::lockForSave()
{
  createImplIfNeeded();
  fPVBBMImpl->lockUpdates();
}

void VBBM::unlockForSave()
{
  fPVBBMImpl->unlockUpdates();
}

/* Returns with the VBBM mapped, holding the VBBM write lock for a write op and
   no lock at all for a read: a writer publishes a replacement data area rather
   than changing the one a reader is walking, so a reader needs the area to stay
   mapped, not to be excluded from it. The pin below is what gives it that.

   A read op inside a write op would point the members at the published area
   while the transaction's copy is the one being changed, and nothing stops it
   any more now that the two do not contend for the same lock. No caller does
   it, and the pair was mutually exclusive before this, so none can have. */
void VBBM::lock(OPS op)
{
  createImplIfNeeded();

  if (op == WRITE)
  {
    /* The update mutex beginUpdate() takes is the write lock: it is held for
       the same span the table's used to be, from here to release(WRITE), and
       it is the one a writer in another process contends on. beginUpdate()
       gives it back itself if it throws. */
    beginUpdate();
    return;
  }

  initDataAreaIfNeeded();

  if (fReadDepth == 0)
    fDataAreaPin = fPVBBMImpl->pin();

  VBShmsegHeader* header = VBBMImpl::headerIn(fDataAreaPin);

  if (header == nullptr)
  {
    if (fReadDepth == 0)
      fDataAreaPin.reset();

    throw runtime_error("VBBM::lock(): there is no VBBM data area to read");
  }

  setPointers(header);
  ++fReadDepth;
}

void VBBM::release(OPS op)
{
  if (op == READ)
  {
    /* No lock was taken, so there is none to give back. Dropping the pin is
       what lets a data area that has since been replaced be unmapped, and it is
       also what makes the members stale, so nothing may use them past here. */
    if (fReadDepth > 0 && --fReadDepth == 0)
    {
      fDataAreaPin.reset();
      vbbm = NULL;
      files = NULL;
      hashBuckets = NULL;
      storage = NULL;
    }

    return;
  }

  /* The changes went into a copy nobody else can see yet; publish it here,
     while the write lock is still held. undoChanges() has already dropped the
     copy if the transaction is being rolled back, which leaves this a no-op. */
  publishUpdate();
}

/* Makes room in the copy the write transaction works on, for either one more
   version buffer file or another increment's worth of entries - the two are
   mutually exclusive, as they were before.

   Assumes the write lock is held. */
void VBBM::growVBBM(bool addAFile)
{
  int oldFiles = vbbm->nFiles;
  int oldCapacity = vbbm->vbCapacity;

  int newFiles = oldFiles;
  int newBuckets = vbbm->numHashBuckets;
  int newCapacity = oldCapacity;

  if (addAFile)
    newFiles++;
  else
  {
    newBuckets += VBTABLE_INCREMENT / sizeof(int);
    newCapacity += VBSTORAGE_INCREMENT / sizeof(VBBMEntry);
  }

  fPVBBMImpl->growUpdateTo(vbbmImageSize(newFiles, newBuckets, newCapacity));

  /* Growing can remap the copy, so nothing derived from the old mapping
     survives it. The header still describes the old layout, which is what
     locates the entries that have to be carried over. */
  vbbm = fPVBBMImpl->get();
  VBBMEntry* oldStorage = vbbmLayoutOf(vbbm).storage;

  vbbm->nFiles = newFiles;
  vbbm->numHashBuckets = newBuckets;
  vbbm->vbCapacity = newCapacity;
  setPointers(vbbm);

  /* The entries sit behind both arrays that just grew, so they slide up by
     however much those grew by. The two regions overlap by construction, hence
     memmove. */
  memmove(storage, oldStorage, oldCapacity * sizeof(VBBMEntry));

  for (int i = oldFiles; i < newFiles; i++)
  {
    files[i].OID = 0;
    files[i].fileSize = 0;
    files[i].nextOffset = 0;
  }

  for (int i = oldCapacity; i < newCapacity; i++)
    storage[i].lbid = -1;

  /* The hash table moved too, and a changed bucket count invalidates every
     chain in it anyway, so rebuild it rather than move it. */
  rehash();
}

/* Sizes the copy for a load of count entries and empties it. The caller is
   about to insert the entries it read out of the save file.

   Assumes the write lock is held. */
void VBBM::growForLoad(int count)
{
  int nFiles = vbbm->nFiles;

  if (count < VBSTORAGE_INITIAL_COUNT)
    count = VBSTORAGE_INITIAL_COUNT;

  // round up to next normal increment point out of paranoia.
  if (count % VBSTORAGE_INCREMENT_COUNT)
    count = ((count / VBSTORAGE_INCREMENT_COUNT) + 1) * VBSTORAGE_INCREMENT_COUNT;

  int numHashBuckets = count / 4;

  fPVBBMImpl->growUpdateTo(vbbmImageSize(nFiles, numHashBuckets, count));

  vbbm = fPVBBMImpl->get();
  vbbm->vbCapacity = count;
  vbbm->numHashBuckets = numHashBuckets;
  vbbm->vbLWM = 0;
  /* Explicitly, unlike every other field here: this used to come from a
     freshly created and so zero-filled segment, and a copy carries the count
     the VBBM had before the load over. */
  vbbm->vbCurrentSize = 0;
  setPointers(vbbm);

  for (int i = 0; i < vbbm->numHashBuckets; i++)
    hashBuckets[i] = -1;

  for (int i = 0; i < vbbm->vbCapacity; i++)
    storage[i].lbid = -1;
}

/* Rebuilds the hash table from the entries in storage, leaving each of them
   where it is: only the bucket heads and the chain links change.

   Also recounts the entries. That is how growVBBM() gets away with moving the
   array and throwing the old table away, and it repairs vbCurrentSize if it has
   drifted from the number of occupied slots, which is what it is defined as. */
void VBBM::rehash()
{
  for (int i = 0; i < vbbm->numHashBuckets; i++)
    hashBuckets[i] = -1;

  int used = 0;

  for (int i = 0; i < vbbm->vbCapacity; i++)
  {
    if (storage[i].lbid == -1)
      continue;

    const int bucket = hashIndexOf(storage[i].lbid, storage[i].verID);
    storage[i].next = hashBuckets[bucket];
    hashBuckets[bucket] = i;
    ++used;
  }

  vbbm->vbCurrentSize = used;
  /* _insert() scans up from the low water mark for a free slot, so the mark has
     to sit below the first hole; the bottom is the cheapest correct choice. */
  vbbm->vbLWM = 0;
}

/// Which hash bucket an entry with that key belongs in.
int VBBM::hashIndexOf(LBID_t lbid, VER_t verID) const
{
  constexpr int cHashlen = sizeof(LBID_t) + sizeof(VER_t);
  char cHash[cHashlen];
  utils::Hasher hasher;

  memcpy(cHash, &lbid, sizeof(LBID_t));
  memcpy(&cHash[sizeof(LBID_t)], &verID, sizeof(VER_t));

  return hasher(cHash, cHashlen) % vbbm->numHashBuckets;
}

// write lock
void VBBM::insert(LBID_t lbid, VER_t verID, OID_t vbOID, uint32_t vbFBO)
{
  VBBMEntry entry;

#ifdef BRM_DEBUG
  int i;

  if (lbid < 0)
  {
    log("VBBM::insert(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
    throw invalid_argument("VBBM::insert(): lbid must be >= 0");
  }

  if (verID < 0)
  {
    log("VBBM::insert(): verID must be >= 0", logging::LOG_TYPE_DEBUG);
    throw invalid_argument("VBBM::insert(): verID must be >= 0");
  }

  for (i = 0; i < vbbm->nFiles; i++)
    if (vbOID == files[i].OID)
      break;

  if (i == vbbm->nFiles)
  {
    log("VBBM::insert(): vbOID must be the OID of a Version Buffer file", logging::LOG_TYPE_DEBUG);
    throw invalid_argument("VBBM::insert(): vbOID must be the OID of a Version Buffer file");
  }

  if (vbFBO > (files[i].fileSize / BLOCK_SIZE) || vbFBO < 0)
  {
    log("VBBM::insert(): vbFBO is out of bounds for that vbOID", logging::LOG_TYPE_DEBUG);
    throw invalid_argument("VBBM::insert(): vbFBO is out of bounds for that vbOID");
  }

#endif

  entry.lbid = lbid;
  entry.verID = verID;
  entry.vbOID = vbOID;
  entry.vbFBO = vbFBO;

  // check for resize
  if (vbbm->vbCurrentSize == vbbm->vbCapacity)
    growVBBM();

  _insert(entry);
  vbbm->vbCurrentSize++;
}

// assumes write lock is held and that it is properly sized already
void VBBM::_insert(VBBMEntry& e)
{
  int hashIndex = hashIndexOf(e.lbid, e.verID);
  int insertIndex = vbbm->vbLWM;

  while (storage[insertIndex].lbid != -1)
  {
    insertIndex++;
#ifdef BRM_DEBUG

    if (insertIndex == vbbm->vbCapacity)
    {
      log("VBBM:_insert(): There are no empty entries. Possibly bad resize condition.",
          logging::LOG_TYPE_DEBUG);
      throw logic_error("VBBM:_insert(): There are no empty entries. Possibly bad resize condition.");
    }

#endif
  }

  vbbm->vbLWM = insertIndex;

  e.next = hashBuckets[hashIndex];
  storage[insertIndex] = e;
  hashBuckets[hashIndex] = insertIndex;
}

// assumes read lock is held
int VBBM::lookup(LBID_t lbid, VER_t verID, OID_t& oid, uint32_t& fbo) const
{
  int index, prev, bucket;

  // #ifdef BRM_DEBUG
  if (lbid < 0)
  {
    log("VBBM::lookup(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
    throw invalid_argument("VBBM::lookup(): lbid must be >= 0");
  }

  if (verID < 0)
  {
    log("VBBM::lookup(): verID must be > 1)", logging::LOG_TYPE_DEBUG);
    throw invalid_argument("VBBM::lookup(): verID must be > 1)");
  }

  // #endif

  index = getIndex(lbid, verID, prev, bucket);

  if (index == -1)
    return -1;

  oid = storage[index].vbOID;
  fbo = storage[index].vbFBO;
  return 0;
}

// assumes write lock
void VBBM::getBlocks(int num, OID_t vbOID, vector<VBRange>& freeRanges, VSS& vss, bool flushPMCache)
{
  int blocksLeftInFile, blocksGathered = 0, i;
  uint32_t fileIndex;
  uint32_t firstFBO, lastFBO;
  VBRange range;
  vector<VBRange>::iterator it;
  vector<LBID_t> flushList;

  freeRanges.clear();

  fileIndex = addVBFileIfNotExists(vbOID);

  /*
  for (i = 0; i < vbbm->nFiles; i++) {
      cout << "file " << i << " vbOID=" << files[i].OID << " size=" << files[i].fileSize
                      << endl;
  }
  */

  if ((uint32_t)num > files[fileIndex].fileSize / BLOCK_SIZE)
  {
    cout << "num = " << num << " filesize = " << files[fileIndex].fileSize << endl;
    log("VBBM::getBlocks(): num is larger than the size of the version buffer", logging::LOG_TYPE_DEBUG);
    throw logging::VBBMBufferOverFlowExcept(
        "VBBM::getBlocks(): num is larger than the size of the version buffer");
  }

  while ((vbbm->vbCurrentSize + num) > vbbm->vbCapacity)
  {
    growVBBM();
    // cout << " requested num = " << num << " and Growing vbbm ... " << endl;
  }

  while (blocksGathered < num)
  {
    blocksLeftInFile = (files[fileIndex].fileSize - files[fileIndex].nextOffset) / BLOCK_SIZE;
    int blocksLeft = num - blocksGathered;

    range.vbOID = files[fileIndex].OID;
    range.vbFBO = files[fileIndex].nextOffset / BLOCK_SIZE;
    range.size = (blocksLeftInFile >= blocksLeft ? blocksLeft : blocksLeftInFile);

    if (range.size == (uint32_t)blocksLeftInFile)
      files[fileIndex].nextOffset = 0;
    else
      files[fileIndex].nextOffset += range.size * BLOCK_SIZE;

    blocksGathered += range.size;
    freeRanges.push_back(range);
  }

  // age the returned blocks out of the VB
  for (it = freeRanges.begin(); it != freeRanges.end(); it++)
  {
    uint32_t firstChunk, lastChunk;

    vbOID = it->vbOID;
    firstFBO = it->vbFBO;
    lastFBO = it->vbFBO + it->size - 1;

    /* Age out at least 100 blocks at a time to reduce the # of times we have to do it.
     * How to detect when it needs to be done and when it doesn't?
     *
     * Split VB space into 100-block chunks.  When a chunk boundary is crossed,
     * clear the whole chunk.
     */

    firstChunk = firstFBO / VBBM_CHUNK_SIZE;
    lastChunk = lastFBO / VBBM_CHUNK_SIZE;

    // if the current range falls in the middle of a chunk and doesn't span chunks,
    // there's nothing to do b/c the chunk is assumed to have been cleared already
    if (((firstFBO % VBBM_CHUNK_SIZE) != 0) && (firstChunk == lastChunk))
      continue;

    // round up to the next chunk boundaries
    if ((firstFBO % VBBM_CHUNK_SIZE) != 0)            // this implies the range spans chunks
      firstFBO = (firstChunk + 1) * VBBM_CHUNK_SIZE;  // the first FBO of the next chunk

    lastFBO = ((lastChunk + 1) * VBBM_CHUNK_SIZE - 1);  // the last FBO of the last chunk

    // don't go past the end of the file
    if (lastFBO > files[fileIndex].fileSize / BLOCK_SIZE)
      lastFBO = files[fileIndex].fileSize / BLOCK_SIZE;

    // at this point [firstFBO, lastFBO] is the range to age out.

    // ugh, walk the whole vbbm looking for matches.
    for (i = 0; i < vbbm->vbCapacity; i++)
      if (storage[i].lbid != -1 && storage[i].vbOID == vbOID && storage[i].vbFBO >= firstFBO &&
          storage[i].vbFBO <= lastFBO)
      {
        if (vss.isEntryLocked(storage[i].lbid, storage[i].verID))
        {
          ostringstream msg;
          msg << "VBBM::getBlocks(): version buffer overflow. Increase VersionBufferFileSize. Overflow "
                 "occurred in aged blocks. Requested NumBlocks:VbOid:vbFBO:lastFBO = "
              << num << ":" << vbOID << ":" << firstFBO << ":" << lastFBO << " lbid locked is "
              << storage[i].lbid << endl;
          log(msg.str(), logging::LOG_TYPE_CRITICAL);
          freeRanges.clear();
          throw logging::VBBMBufferOverFlowExcept(msg.str());
        }

        vss.removeEntry(storage[i].lbid, storage[i].verID, &flushList);
        removeEntry(storage[i].lbid, storage[i].verID);
      }
  }

  if (flushPMCache && !flushList.empty())
    cacheutils::flushPrimProcAllverBlocks(flushList);
}

// read lock
int VBBM::getIndex(LBID_t lbid, VER_t verID, int& prev, int& bucket) const
{
  int currentIndex;
  VBBMEntry* listEntry;

  bucket = hashIndexOf(lbid, verID);
  prev = -1;

  if (hashBuckets[bucket] == -1)
    return -1;

  currentIndex = hashBuckets[bucket];

  while (currentIndex != -1)
  {
    listEntry = &storage[currentIndex];

    if (listEntry->lbid == lbid && listEntry->verID == verID)
      return currentIndex;

    prev = currentIndex;
    currentIndex = listEntry->next;
  }

  return -1;
}

void VBBM::removeEntry(LBID_t lbid, VER_t verID)
{
  int index, prev, bucket;

#ifdef BRM_DEBUG

  if (lbid < 0)
  {
    log("VBBM::removeEntry(): lbid must be >= 0", logging::LOG_TYPE_DEBUG);
    throw invalid_argument("VBBM::removeEntry(): lbid must be >= 0");
  }

  if (verID < 0)
  {
    log("VBBM::removeEntry(): verID must be >= 0", logging::LOG_TYPE_DEBUG);
    throw invalid_argument("VBBM::removeEntry(): verID must be >= 0");
  }

#endif

  index = getIndex(lbid, verID, prev, bucket);

  if (index == -1)
  {
#ifdef BRM_DEBUG
    ostringstream ostr;

    ostr << "VBBM::removeEntry(): that entry doesn't exist lbid = " << lbid << " verID = " << verID << endl;
    log(ostr.str(), logging::LOG_TYPE_DEBUG);
    throw logic_error(ostr.str());
#else
    return;
#endif
  }

  storage[index].lbid = -1;

  if (prev != -1)
    storage[prev].next = storage[index].next;
  else
    hashBuckets[bucket] = storage[index].next;

  vbbm->vbCurrentSize--;

  if (vbbm->vbLWM > index)
    vbbm->vbLWM = index;
}

// read lock
int VBBM::size() const
{
#ifdef BRM_DEBUG
  int i, ret = 0;

  for (i = 0; i < vbbm->vbCapacity; i++)
    if (storage[i].lbid != -1)
      ret++;

  if (ret != vbbm->vbCurrentSize)
  {
    ostringstream ostr;

    ostr << "VBBM::checkConsistency(): actual size is " << ret << ", recorded size is "
         << vbbm->vbCurrentSize;
    log(ostr.str(), logging::LOG_TYPE_DEBUG);
    throw logic_error(ostr.str());
  }

  return ret;
#else
  return vbbm->vbCurrentSize;
#endif
}

// read lock
bool VBBM::hashEmpty() const
{
  int i;

  for (i = 0; i < vbbm->numHashBuckets; i++)
    if (hashBuckets[i] != -1)
      return false;

  return true;
}

/* Empties the copy the write transaction works on, keeping the version buffer
   files it knows about and rewinding each of them.

   Assumes the write lock is held. The image is not shrunk back to the initial
   size: it is a copy, so there is nothing to recreate smaller, and whatever the
   VBBM once grew to stays allocated. */
void VBBM::clear()
{
  int nFiles = vbbm->nFiles;

  setCurrentFileSize();

  fPVBBMImpl->growUpdateTo(
      vbbmImageSize(nFiles, VBTABLE_INITIAL_SIZE / sizeof(int), VBSTORAGE_INITIAL_SIZE / sizeof(VBBMEntry)));
  setPointers(fPVBBMImpl->get());
  initShmseg(nFiles);

  // The files array sits ahead of the two initShmseg() lays out, so it came
  // through in place and only needs rewinding.
  for (int i = 0; i < nFiles; i++)
  {
    files[i].fileSize = currentFileSize;
    files[i].nextOffset = 0;
  }
}

// read lock
int VBBM::checkConsistency() const
{
  /*

  Struct integrity tests
  1: Verify that the recorded size matches the actual size
  2: Verify there are no empty entries reachable from the hash table
  3: Verify there are no empty entries below the LWM

  4a: Make sure every VBBM entry points to a unique position in the VB
  4b: Make sure every VBBM entry has unique LBID & VERID.
  */

  int i, j, k;

  /* Test 1 is already implemented */
  size();

  /* Test 2 - no empty elements reachable from the hash table */

  int nextElement;

  for (i = 0; i < vbbm->numHashBuckets; i++)
  {
    if (hashBuckets[i] != -1)
      for (nextElement = hashBuckets[i]; nextElement != -1; nextElement = storage[nextElement].next)
        if (storage[nextElement].lbid == -1)
          throw logic_error(
              "VBBM::checkConsistency(): an empty storage entry is reachable from the hash table");
  }

  /* Test 3 - verify that there are no empty entries below the LWM */

  for (i = 0; i < vbbm->vbLWM; i++)
  {
    if (storage[i].lbid == -1)
    {
      cerr << "VBBM: LWM=" << vbbm->vbLWM << " first empty entry=" << i << endl;
      throw logic_error("VBBM::checkConsistency(): LWM accounting error");
    }
  }

  /* Test 4b - verify the uniqueness of the entries */

  for (i = 0; i < vbbm->numHashBuckets; i++)
    if (hashBuckets[i] != -1)
      for (j = hashBuckets[i]; j != -1; j = storage[j].next)
        for (k = storage[j].next; k != -1; k = storage[k].next)
          if (storage[j].lbid == storage[k].lbid && storage[j].verID == storage[k].verID)
          {
            cerr << "VBBM: lbid=" << storage[j].lbid << " verID=" << storage[j].verID << endl;
            throw logic_error("VBBM::checkConsistency(): Duplicate entry found");
          }

  /* Test 4a - verify the uniqueness of vbOID, vbFBO fields */
  for (i = 0; i < vbbm->vbCapacity; i++)
    if (storage[i].lbid != -1)
      for (j = i + 1; j < vbbm->vbCapacity; j++)
        if (storage[j].lbid != -1)
          if (storage[j].vbOID == storage[i].vbOID && storage[j].vbFBO == storage[i].vbFBO)
          {
            cerr << "VBBM: lbid1=" << storage[i].lbid << " lbid2=" << storage[j].lbid
                 << " verID1=" << storage[i].verID << " verID2=" << storage[j].verID
                 << " share vbOID=" << storage[j].vbOID << " vbFBO=" << storage[j].vbFBO << endl;
            throw logic_error("VBBM::checkConsistency(): 2 VBBM entries share space in the VB");
          }

  return 0;
}

void VBBM::setReadOnly()
{
  r_only = true;

  // Both callers do this straight after construction, before the impl exists;
  // handle the other order anyway rather than silently staying writable.
  if (fPVBBMImpl)
    fPVBBMImpl->makeReadOnly();
}

/* File Format (V1)

                VBBM V1 magic (32-bits)
                # of VBBM entries in capacity (32-bits)
                struct VBBMEntry * #

                These entries are considered optional to support going backward
                and forward between versions.
                nFiles (32-bits)
                currentFileIndex (32-bits)
                VBFileMetadata * nFiles
*/

/* File Format (V2):
 *
 * 		Version 2 magic (int)
 * 		number of used VBBM entries (numEntries) (int)
 * 		current number of VB files (nFiles) (int)
 * 		VBFileMetadata * nFiles
 * 		struct VBBMEntry * numEntries
 */

void VBBM::loadVersion2(IDBDataFile* in)
{
  int vbbmEntries;
  int nFiles;
  int i;
  VBBMEntry entry;

  if (in->read((char*)&vbbmEntries, 4) != 4)
  {
    log_errno("VBBM::load()");
    throw runtime_error("VBBM::load(): Failed to read entry number");
  }

  if (in->read((char*)&nFiles, 4) != 4)
  {
    log_errno("VBBM::load()");
    throw runtime_error("VBBM::load(): Failed to read file number");
  }

  // Need to make clear() truncate the files section
  if (vbbm->nFiles > nFiles)
    vbbm->nFiles = nFiles;

  clear();

  while (vbbm->nFiles < nFiles)
    growVBBM(true);  // this allocates one file, doesn't grow the main storage

  growForLoad(vbbmEntries);

  const int nfileSize = sizeof(VBFileMetadata) * nFiles;

  if (in->read((char*)files, nfileSize) != nfileSize)
  {
    log_errno("VBBM::load()");
    throw runtime_error("VBBM::load(): Failed to load vb file meta data");
  }

  size_t readSize = vbbmEntries * sizeof(entry);
  std::unique_ptr<char[]> readBuf(new char[readSize]);
  size_t progress = 0;
  int err;
  while (progress < readSize)
  {
    err = in->read(readBuf.get() + progress, readSize - progress);
    if (err < 0)
    {
      log_errno("VBBM::load()");
      throw runtime_error("VBBM::load(): Failed to load, check the critical log file");
    }
    else if (err == 0)
    {
      log("VBBM::load(): Got early EOF");
      throw runtime_error("VBBM::load(): Got early EOF");
    }
    progress += err;
  }

  VBBMEntry* loadedEntries = reinterpret_cast<VBBMEntry*>(readBuf.get());
  for (i = 0; i < vbbmEntries; i++)
    insert(loadedEntries[i].lbid, loadedEntries[i].verID, loadedEntries[i].vbOID, loadedEntries[i].vbFBO);
}

// #include "boost/date_time/posix_time/posix_time.hpp"
//  using namespace boost::posix_time;

void VBBM::load(string filename)
{
  int magic;
  const char* filename_p = filename.c_str();
  scoped_ptr<IDBDataFile> in(
      IDBDataFile::open(IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG), filename_p, "rb", 0));
  // ptime time1, time2;

  // time1 = microsec_clock::local_time();
  // cout << "loading the VBBM " << time1 << endl;

  if (!in)
  {
    log_errno("VBBM::load()");
    throw runtime_error("VBBM::load(): Failed to open the file");
  }

  int bytes = in->read((char*)&magic, 4);

  if (bytes != 4)
  {
    log("VBBM::load(): failed to read magic.");
    throw runtime_error("VBBM::load(): failed to read magic.");
  }

  switch (magic)
  {
    case VBBM_MAGIC_V2: loadVersion2(in.get()); break;

    default:
      log("VBBM::load(): Bad magic.  Not a VBBM file?");
      throw runtime_error("VBBM::load(): Bad magic.  Not a VBBM file?");
  }

  // time2 = microsec_clock::local_time();
  // cout << "done loading " << time2 << " duration: " << time2-time1 << endl;
}

// read lock
void VBBM::save(string filename)
{
  int i;
  int var;

  const char* filename_p = filename.c_str();
  scoped_ptr<IDBDataFile> out(IDBDataFile::open(IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
                                                filename_p, "wb", IDBDataFile::USE_VBUF));

  if (!out)
  {
    log_errno("VBBM::save()");
    throw runtime_error("VBBM::save(): Failed to open the file");
  }

  var = VBBM_MAGIC_V2;
  [[maybe_unused]] int bytesWritten = 0;
  [[maybe_unused]] int bytesToWrite = 12;
  bytesWritten += out->write((char*)&var, 4);
  bytesWritten += out->write((char*)&vbbm->vbCurrentSize, 4);
  bytesWritten += out->write((char*)&vbbm->nFiles, 4);

  bytesWritten += out->write((char*)files, sizeof(VBFileMetadata) * vbbm->nFiles);
  bytesToWrite += sizeof(VBFileMetadata) * vbbm->nFiles;

  int first = -1, last = -1, err;
  size_t progress, writeSize;

  for (i = 0; i < vbbm->vbCapacity; i++)
  {
    if (storage[i].lbid != -1 && first == -1)
      first = i;
    else if (storage[i].lbid == -1 && first != -1)
    {
      last = i;
      writeSize = (last - first) * sizeof(VBBMEntry);
      progress = 0;
      char* writePos = (char*)&storage[first];
      while (progress < writeSize)
      {
        err = out->write(writePos + progress, writeSize - progress);
        if (err < 0)
        {
          log_errno("VBBM::save()");
          throw runtime_error("VBBM::save(): Failed to write the file");
        }
        progress += err;
      }
      first = -1;
    }
  }
  if (first != -1)
  {
    writeSize = (vbbm->vbCapacity - first) * sizeof(VBBMEntry);
    progress = 0;
    char* writePos = (char*)&storage[first];
    while (progress < writeSize)
    {
      err = out->write(writePos + progress, writeSize - progress);
      if (err < 0)
      {
        log_errno("VBBM::save()");
        throw runtime_error("VBBM::save(): Failed to write the file");
      }
      progress += err;
    }
  }

#if 0
    cout << "saving... nfiles=" << vbbm->nFiles << "\n";

    for (i = 0; i < vbbm->nFiles; i++)
    {
        cout << "file " << i << " vboid=" << files[i].OID << " size=" << files[i].fileSize << endl;
    }

#endif
}

uint32_t VBBM::addVBFileIfNotExists(OID_t vbOID)
{
  int i;

  /* Check if vbOID exists,
   * 	add it if not, init to pos=0
   */

  for (i = 0; i < vbbm->nFiles; i++)
    if (files[i].OID == vbOID)
      break;

  if (i == vbbm->nFiles)
  {
    setCurrentFileSize();
    growVBBM(true);
    files[i].OID = vbOID;
    files[i].fileSize = currentFileSize;
    files[i].nextOffset = 0;
  }

  return i;
}

void VBBM::setCurrentFileSize()
{
  config::Config* conf = config::Config::makeConfig();
  string stmp;
  int64_t ltmp;

  currentFileSize = 2147483648ULL;  // 2 GB default

  try
  {
    stmp = conf->getConfig("VersionBuffer", "VersionBufferFileSize");
  }
  catch (std::exception& e)
  {
    log("VBBM: Missing a VersionBuffer/VersionBufferFileSize key in the config file");
    throw invalid_argument("VBBM: Missing a VersionBuffer/VersionBufferFileSize key in the config file");
  }

  ltmp = conf->fromText(stmp.c_str());

  if (ltmp < 1)
  {
    log("VBBM: Config error: VersionBuffer/VersionBufferFileSize must be positive");
    throw invalid_argument("VBBM: Config error: VersionBuffer/VersionBufferFileSize must be positive");
  }
  else
  {
    currentFileSize = ltmp;
  }
}

}  // namespace BRM
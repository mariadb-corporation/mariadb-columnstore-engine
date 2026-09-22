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
 * $Id: copylocks.cpp 1936 2013-07-09 22:10:29Z dhall $
 *
 ****************************************************************************/

#include <sys/types.h>
#include <iostream>
#include <stdexcept>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <string>

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
namespace bi = boost::interprocess;

#include "shmkeys.h"
#include "brmtypes.h"
#include "rwlock.h"
#define COPYLOCKS_DLLEXPORT
#include "copylocks.h"
#undef COPYLOCKS_DLLEXPORT

#define CL_MAGIC_V1 0x789ba6c1

#ifndef O_BINARY
#define O_BINARY 0
#endif
#ifndef O_DIRECT
#define O_DIRECT 0
#endif
#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif
#ifndef O_NOATIME
#define O_NOATIME 0
#endif

using namespace std;
using namespace boost;
using namespace logging;

#include "IDBDataFile.h"
#include "IDBPolicy.h"
using namespace idbdatafile;

namespace BRM
{
CopyLockEntry::CopyLockEntry()
{
  start = 0;
  size = 0;
  txnID = 0;
}

/*static*/
boost::mutex CopyLocksImpl::fInstanceMutex;
/* This thread's view of the data area; see the declarations for why it is per
   thread. Zero-initialized as every thread starts, so a thread that has never
   locked anything looks like one that has released everything. */
thread_local CLShmsegHeader* CopyLocks::header = nullptr;
thread_local CopyLockEntry* CopyLocks::entries = nullptr;
thread_local CopyLocksImpl::DataAreaPin CopyLocks::fDataAreaPin;
thread_local uint32_t CopyLocks::fReadDepth = 0;

boost::mutex CopyLocks::mutex;

/*static*/
CopyLocksImpl* CopyLocksImpl::fInstance = 0;

/*static*/
CopyLocksImpl* CopyLocksImpl::makeCopyLocksImpl(unsigned keyBase, off_t size, bool readOnly)
{
  boost::mutex::scoped_lock lk(fInstanceMutex);

  // The metadata area is at a fixed key and never moves, so the instance is
  // created once. Which data area it looks at is decided by refresh(), not here.
  if (!fInstance)
    fInstance = new CopyLocksImpl(keyBase, size, readOnly);

  return fInstance;
}

CopyLocksImpl::CopyLocksImpl(unsigned keyBase, off_t size, bool readOnly)
 : fCopyLocks(keyBase, size, readOnly)
{
}

void CopyLocksImpl::growUpdateTo(off_t size)
{
  const off_t have = fCopyLocks.imageSize();

  if (size > have)
    fCopyLocks.growUpdate(size - have);
}

CopyLocks::CopyLocks()
{
  /* header, entries, the pin and the depth are not touched here:
     they belong to the calling thread rather than to this object, so clearing
     them would cut the ground from under a read lock that thread already holds
     on another CopyLocks. A thread starts with them zeroed anyway. */
  r_only = false;
  fCopyLocksImpl = 0;
}

CopyLocks::~CopyLocks()
{
}

void CopyLocks::setReadOnly()
{
  r_only = true;

  // The one caller does this straight after construction, before the impl
  // exists; handle the other order anyway rather than silently staying writable.
  if (fCopyLocksImpl)
    fCopyLocksImpl->makeReadOnly();
}

/* Points the members at the image header describes. The entry array sits
   immediately behind the header and so never moves relative to it, but the
   image as a whole does - every publish and every growth of the copy relocates
   it - so this has to run again after either. */
void CopyLocks::setPointers(CLShmsegHeader* h)
{
  header = h;
  entries = clEntriesOf(h);
}

/* Maps the metadata area. The impl is a process-wide singleton, hence the mutex. */
void CopyLocks::createImplIfNeeded()
{
  boost::mutex::scoped_lock lk(mutex);

  if (fCopyLocksImpl)
    return;

  fCopyLocksImpl =
      CopyLocksImpl::makeCopyLocksImpl(fShmKeys.KEYRANGE_CL_BASE, clImageSize(CL_INITIAL_COUNT), r_only);
  idbassert(fCopyLocksImpl);

  if (r_only)
    fCopyLocksImpl->makeReadOnly();
}

/* The first process to look at the copy locks finds no data area at all, the
   metadata area holding id 0. Publish an initialized, empty one so that a
   reader has something to map. A no-op from the first access of the cluster's
   life onwards, which is why it is the read path that carries it. */
void CopyLocks::initDataAreaIfNeeded()
{
  if (fCopyLocksImpl->currentId() != 0)
    return;

  /* The caller is already making the first one - nothing to do, and taking
     the exclusion below would be taking it twice. */
  if (fCopyLocksImpl->inUpdate())
    return;

  /* beginUpdate() takes the segment's update mutex, and that is the whole of
     the exclusion now. So the re-check goes under it rather than before it:
     another process may have published while we waited, in which case what
     beginUpdate() copied is theirs and there is nothing left to publish. */
  beginUpdate();

  try
  {
    if (fCopyLocksImpl->currentId() == 0)
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
   the CopyLocks write lock keeps working on the copy that is already open. */
void CopyLocks::beginUpdate()
{
  const bool alreadyOpen = fCopyLocksImpl->inUpdate();

  if (!alreadyOpen)
    fCopyLocksImpl->beginUpdate(clImageSize(CL_INITIAL_COUNT));

  /* Whether the copy came from nothing and so needs laying out. Only the call
     that opened the update can tell: it returns with the update mutex held, so
     the published id it reads back is exactly the one it copied from. */
  const bool firstEver = !alreadyOpen && fCopyLocksImpl->currentId() == 0;

  setPointers(fCopyLocksImpl->get());

  if (firstEver)
    initShmseg();
}

/* Makes the copy the current data area. This is the only point at which the
   changes of a write transaction become visible, and it is a single atomic
   store, so a reader sees either all of them or none. */
void CopyLocks::publishUpdate()
{
  if (!fCopyLocksImpl || !fCopyLocksImpl->inUpdate())
    return;

  fCopyLocksImpl->publishUpdate();
  setPointers(fCopyLocksImpl->get());
}

/* Rolls the write transaction back by dropping the copy; the published data
   area was never touched. */
void CopyLocks::discardUpdate()
{
  if (!fCopyLocksImpl || !fCopyLocksImpl->inUpdate())
    return;

  fCopyLocksImpl->discardUpdate();
  // Back to whatever is published.
  fCopyLocksImpl->refresh();
  setPointers(fCopyLocksImpl->get());
}

/* Nothing to do: the changes are in a copy nobody else can see, and it is
   release(WRITE) that publishes it. Kept because SlaveDBRMNode and DBRM name
   the two ends of a transaction explicitly. */
bool CopyLocks::hasOpenUpdate() const
{
  return fCopyLocksImpl && fCopyLocksImpl->inUpdate();
}

void CopyLocks::confirmChanges()
{
}

void CopyLocks::undoChanges()
{
  discardUpdate();
}

/* The bootstrap on its own, for a caller that is about to take this table's
   read lock and so cannot let lock(READ) reach for the write lock. A no-op once
   anything has ever been published, which is from the first access of the
   cluster's life onwards. */
void CopyLocks::ensureDataArea()
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
   because a caller reaches this before anything else has touched the copy locks. */
void CopyLocks::lockForSave()
{
  createImplIfNeeded();
  fCopyLocksImpl->lockUpdates();
}

void CopyLocks::unlockForSave()
{
  fCopyLocksImpl->unlockUpdates();
}

/* Returns with the copy locks mapped, holding the write lock for a write op and
   no lock at all for a read: a writer publishes a replacement data area rather
   than changing the one a reader is walking, so a reader needs the area to stay
   mapped, not to be excluded from it. The pin below is what gives it that. */
void CopyLocks::lock(OPS op)
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
    fDataAreaPin = fCopyLocksImpl->pin();

  CLShmsegHeader* h = CopyLocksImpl::headerIn(fDataAreaPin);

  if (h == nullptr)
  {
    if (fReadDepth == 0)
      fDataAreaPin.reset();

    throw runtime_error("CopyLocks::lock(): there is no CopyLocks data area to read");
  }

  setPointers(h);
  ++fReadDepth;
}

void CopyLocks::release(OPS op)
{
  if (op == READ)
  {
    /* No lock was taken, so there is none to give back. Dropping the pin is
       what lets a data area that has since been replaced be unmapped, and it is
       also what makes the members stale, so nothing may use them past here.
       Tolerates being called without a matching lock(READ): several error paths
       do that. */
    if (fReadDepth > 0 && --fReadDepth == 0)
    {
      fDataAreaPin.reset();
      header = NULL;
      entries = NULL;
    }

    return;
  }

  /* The changes went into a copy nobody else can see yet; publish it here,
     while the write lock is still held. undoChanges() has already dropped the
     copy if the transaction is being rolled back, which leaves this a no-op. */
  publishUpdate();
}

/* Lays out an empty set of copy locks at the initial size in the copy the write
   transaction works on. */
void CopyLocks::initShmseg()
{
  header->capacity = CL_INITIAL_COUNT;
  header->currentSize = 0;

  for (int i = 0; i < header->capacity; i++)
    entries[i] = CopyLockEntry();
}

/* Only ever called with the write transaction's copy open, so the growth is
   invisible to everybody else until that copy is published. */
void CopyLocks::growCL()
{
  int newCapacity = header->capacity + CL_INCREMENT_COUNT;
  int oldCapacity = header->capacity;

  // Relocates the image, so the members have to be rederived afterwards.
  fCopyLocksImpl->growUpdateTo(clImageSize(newCapacity));
  setPointers(fCopyLocksImpl->get());

  /* What the growth added reads as zero, and a zero-sized entry is exactly what
     an empty one is, but say so rather than lean on that. */
  for (int i = oldCapacity; i < newCapacity; i++)
    entries[i] = CopyLockEntry();

  header->capacity = newCapacity;
}

// this fcn is dumb; relies on external check on whether it's safe or not
// also relies on external write lock grab
void CopyLocks::lockRange(const LBIDRange& l, VER_t txnID)
{
  // grow if necessary
  if (header->currentSize == header->capacity)
    growCL();

  /* debugging code, check for an existing lock */
  // assert(!isLocked(l));

  // ostringstream os;
  // os << "Copylocks locking <" << l.start << ", " << l.size << "> txnID = " << txnID;
  // log(os.str());

  // scan for an empty entry
  for (int i = 0; i < header->capacity; i++)
  {
    if (entries[i].size == 0)
    {
      entries[i].start = l.start;
      entries[i].size = l.size;
      entries[i].txnID = txnID;
      header->currentSize++;

      // make sure isLocked() now sees the lock
      // assert(isLocked(l));
      return;
    }
  }

  log(string("CopyLocks::lockRange(): shm metadata problem: could not find an empty copylock entry"));
  throw std::logic_error(
      "CopyLocks::lockRange(): shm metadata problem: could not find an empty copylock entry");
}

// this fcn is dumb; relies on external check on whether it's safe or not
// also relies on external write lock grab
void CopyLocks::releaseRange(const LBIDRange& l)
{
  LBID_t lastBlock = l.start + l.size - 1;
  LBID_t eLastBlock;

#ifdef BRM_DEBUG
  // debatable whether this should be included or not given the timers
  // that automatically release locks
  idbassert(isLocked(l));
#endif

  for (int i = 0; i < header->capacity; i++)
  {
    CopyLockEntry& e = entries[i];

    if (e.size != 0)
    {
      eLastBlock = e.start + e.size - 1;

      if (l.start <= eLastBlock && lastBlock >= e.start)
      {
        e.size = 0;
        header->currentSize--;
      }
    }
  }

#ifdef BRM_DEBUG
  idbassert(!isLocked(l));
  // log(string("CopyLocks::releaseRange(): that range isn't locked", LOG_TYPE_WARNING));
  // throw std::invalid_argument("CopyLocks::releaseRange(): that range isn't locked");
#endif
}

void CopyLocks::forceRelease(const LBIDRange& l)
{
  LBID_t lastBlock = l.start + l.size - 1;
  LBID_t eLastBlock;

  // ostringstream os;
  // os << "Copylocks force-releasing <" << l.start << ", " << l.size << ">";
  // log(os.str());

  /* If a range intersects l, get rid of it. */
  for (int i = 0; i < header->capacity; i++)
  {
    CopyLockEntry& e = entries[i];

    if (e.size != 0)
    {
      eLastBlock = e.start + e.size - 1;

      if (l.start <= eLastBlock && lastBlock >= e.start)
      {
        e.size = 0;
        header->currentSize--;
      }
    }
  }

  // assert(!isLocked(l));
}

/* Works off whichever image the caller's lock(op) pointed the members at: the
   pinned data area under a read lock, the copy under a write lock. */
bool CopyLocks::isLocked(const LBIDRange& l) const
{
  LBID_t lLastBlock, lastBlock;

  lLastBlock = l.start + l.size - 1;

  for (int i = 0; i < header->capacity; i++)
  {
    if (entries[i].size != 0)
    {
      lastBlock = entries[i].start + entries[i].size - 1;

      if (lLastBlock >= entries[i].start && l.start <= lastBlock)
        return true;
    }
  }

  return false;
}

void CopyLocks::rollback(VER_t txnID)
{
  for (int i = 0; i < header->capacity; i++)
    if (entries[i].size != 0 && entries[i].txnID == txnID)
    {
      entries[i].size = 0;
      header->currentSize--;
    }
}

/* Same as isLocked(): reads whichever image lock(op) selected. */
void CopyLocks::getCurrentTxnIDs(std::set<VER_t>& list) const
{
  for (int i = 0; i < header->capacity; i++)
    if (entries[i].size != 0)
      list.insert(entries[i].txnID);
}

}  // namespace BRM
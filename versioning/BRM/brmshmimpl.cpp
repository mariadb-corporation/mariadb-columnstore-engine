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
 * $Id$
 *
 ****************************************************************************/

#include <iostream>
#include <string>
// #define NDEBUG
#include <algorithm>
#include <cstring>
#include <map>
#include <vector>
#include <mutex>
#include <sstream>
#include <cerrno>
#include <ctime>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
using namespace std;

#include <boost/date_time/posix_time/posix_time_types.hpp>

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
namespace bi = boost::interprocess;

#include "shmkeys.h"
#include "brmshmimpl.h"
#include "brmtypes.h"

#if !defined(HAVE_GETTID)
#if defined(HAVE_GETTID_SYSCALL)
#include <sys/syscall.h>

namespace
{

pid_t gettid()
{
  return syscall(SYS_gettid);
}

}
#else
#error "gettid(2) is required"
#endif // HAVE_GETTID_SYSCALL
#endif // !HAVE_GETTID

#if !defined(HAVE_TGKILL)
#if defined(HAVE_TGKILL_SYSCALL)
#include <sys/syscall.h>

namespace
{

int tgkill(pid_t pid, pid_t tid, int sig)
{
  return syscall(SYS_tgkill, pid, tid, sig);
}

}
#endif // HAVE_TGKILL_SYSCALL
#endif // !HAVE_TGKILL

namespace BRM
{
constexpr uint32_t ShmCreateMaxRetries = 10;
constexpr unsigned int NapTimer = 500000;

BRMShmImplParent::BRMShmImplParent(unsigned key, off_t size, bool readOnly)
 : fKey(key), fSize(size), fReadOnly(readOnly) {};

BRMShmImplParent::~BRMShmImplParent() = default;

BRMShmImpl::BRMShmImpl(unsigned key, off_t size, bool readOnly) : BRMShmImplParent(key, size, readOnly)
{
  string keyName = ShmKeys::keyToName(fKey);

  if (fSize == 0)
  {
    unsigned tries = 0;
  again:

    try
    {
      bi::shared_memory_object shm(bi::open_only, keyName.c_str(), bi::read_write);
      off_t curSize = 0;
      shm.get_size(curSize);

      if (curSize == 0)
        throw bi::interprocess_exception("shm size is zero");
    }
    catch (bi::interprocess_exception&)
    {
      if (++tries > 10)
      {
        log("BRMShmImpl::BRMShmImpl(): failed on size==0");
        throw;
      }

      cerr << "BRMShmImpl::BRMShmImpl(): retrying on size==0" << endl;
      usleep(500 * 1000);
      goto again;
    }
  }

  try
  {
    bi::permissions perms;
    perms.set_unrestricted();
    bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write, perms);
    idbassert(fSize > 0);
    shm.truncate(fSize);
    fShmobj.swap(shm);
  }
  catch (bi::interprocess_exception& b)
  {
    if (b.get_error_code() != bi::already_exists_error)
    {
      ostringstream o;
      o << "BRM caught an exception creating a shared memory segment: " << b.what();
      log(o.str());
      throw;
    }
    bi::shared_memory_object* shm = NULL;
    try
    {
      shm = new bi::shared_memory_object(bi::open_only, keyName.c_str(), bi::read_write);
    }
    catch (exception& e)
    {
      ostringstream o;
      o << "BRM caught an exception attaching to a shared memory segment (" << keyName << "): " << b.what();
      log(o.str());
      throw;
    }
    off_t curSize = 0;
    shm->get_size(curSize);
    idbassert(curSize > 0);
    idbassert(curSize >= fSize);
    fShmobj.swap(*shm);
    delete shm;
    fSize = curSize;
  }

  if (fReadOnly)
  {
    bi::mapped_region ro_region(fShmobj, bi::read_only);
    fMapreg.swap(ro_region);
  }
  else
  {
    bi::mapped_region region(fShmobj, bi::read_write);
    fMapreg.swap(region);
  }
}

int BRMShmImpl::grow(unsigned newKey, off_t newSize)
{
  idbassert(newKey != fKey);
  idbassert(newSize >= fSize);

  string oldName = fShmobj.get_name();

  string keyName = ShmKeys::keyToName(newKey);
  bi::permissions perms;
  perms.set_unrestricted();
  bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write, perms);
  shm.truncate(newSize);

  bi::mapped_region region(shm, bi::read_write);

  // Copy old data into new region
  memcpy(region.get_address(), fMapreg.get_address(), fSize);
  // clear new region
  // make some versions of gcc happier...
  memset(reinterpret_cast<void*>(reinterpret_cast<ptrdiff_t>(region.get_address()) + fSize), 0,
         newSize - fSize);

  fShmobj.swap(shm);
  fMapreg.swap(region);

  if (!oldName.empty())
    bi::shared_memory_object::remove(oldName.c_str());

  fKey = newKey;
  fSize = newSize;

  if (fReadOnly)
  {
    bi::mapped_region ro_region(fShmobj, bi::read_only);
    fMapreg.swap(ro_region);
  }

  return 0;
}

int BRMShmImpl::clear(unsigned newKey, off_t newSize)
{
  idbassert(newKey != fKey);

  string oldName = fShmobj.get_name();

  string keyName = ShmKeys::keyToName(newKey);
  bi::permissions perms;
  perms.set_unrestricted();
  bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write, perms);
  shm.truncate(newSize);

  bi::mapped_region region(shm, bi::read_write);

  // clear new region
  memset(region.get_address(), 0, newSize);

  fShmobj.swap(shm);
  fMapreg.swap(region);

  if (!oldName.empty())
    bi::shared_memory_object::remove(oldName.c_str());

  fKey = newKey;
  fSize = newSize;

  if (fReadOnly)
  {
    bi::mapped_region ro_region(fShmobj, bi::read_only);
    fMapreg.swap(ro_region);
  }

  return 0;
}

void BRMShmImpl::setReadOnly()
{
  if (fReadOnly)
    return;

  bi::mapped_region ro_region(fShmobj, bi::read_only);
  fMapreg.swap(ro_region);

  fReadOnly = true;
}

void BRMShmImpl::swap(BRMShmImpl& rhs)
{
  fShmobj.swap(rhs.fShmobj);
  fMapreg.swap(rhs.fMapreg);
  std::swap(fKey, rhs.fKey);
  std::swap(fSize, rhs.fSize);
  std::swap(fReadOnly, rhs.fReadOnly);
}

void BRMShmImpl::destroy()
{
  string oldName = fShmobj.get_name();

  if (!oldName.empty())
    bi::shared_memory_object::remove(oldName.c_str());
}

BRMManagedShmImpl::BRMManagedShmImpl(unsigned key, off_t size, bool readOnly)
 : BRMShmImplParent(key, size, readOnly)
{
  string keyName = ShmKeys::keyToName(fKey);
  off_t curSize = 0;

  for (uint32_t tries = 0; fSize == 0 && tries <= ShmCreateMaxRetries; ++tries)
  {
    try
    {
      auto* shmSegment = new boost::interprocess::managed_shared_memory(bi::open_only, keyName.c_str());
      curSize = shmSegment->get_size();

      if (curSize == 0)
      {
        delete shmSegment;
        throw bi::interprocess_exception("shared memory segment size is 0.");
      }
      else
      {
        fShmSegment = shmSegment;
        fSize = curSize;
        return;
      }
    }
    catch (bi::interprocess_exception&)
    {
      if (tries == ShmCreateMaxRetries)
      {
        log("BRMManagedShmImpl::BRMManagedShmImpl(): re-creating shared memory segment\
 b/c of its size == 0. Re-throw.");
        throw;
      }

      cerr << "BRMManagedShmImpl::BRMManagedShmImpl(): re-creating shared memory segment\
 b/c of its size == 0"
           << endl;
      usleep(NapTimer);
    }
  }

  try
  {
    bi::permissions perms;
    perms.set_unrestricted();
    fShmSegment = new bi::managed_shared_memory(bi::create_only, keyName.c_str(), fSize,
                                                0,  // use a default address to map the segment
                                                perms);
    // fSize == 0 on any process startup but managed_shared_memory ctor throws
    // so control flow doesn't get here.
    idbassert(fSize > 0);
  }
  catch (bi::interprocess_exception& b)
  {
    if (b.get_error_code() != bi::already_exists_error)
    {
      ostringstream o;
      o << "BRM caught an exception creating a shared memory segment: " << b.what();
      log(o.str());
      throw;
    }
    bi::managed_shared_memory* shmSegment = nullptr;
    try
    {
      if (fReadOnly)
        shmSegment = new bi::managed_shared_memory(bi::open_read_only, keyName.c_str());
      else
        shmSegment = new bi::managed_shared_memory(bi::open_only, keyName.c_str());
    }
    catch (exception& e)
    {
      ostringstream o;
      o << "BRM caught an exception attaching to a shared memory segment (" << keyName << "): " << b.what();
      log(o.str());
      throw;
    }
    off_t curSize = shmSegment->get_size();

    idbassert(curSize > 0);
    idbassert(curSize >= fSize);
    fShmSegment = shmSegment;
    fSize = curSize;
  }
}

int BRMManagedShmImpl::grow(off_t newSize)
{
  auto keyName = ShmKeys::keyToName(fKey);

  if (newSize > fSize)
  {
    auto incSize = newSize - fSize;
    if (fShmSegment)
    {
      // Call destructor to unmap the segment.
      delete fShmSegment;
      // WARNING: boost documentation states that managed_shared_memory::grow() requires
      // that no other process has the segment mapped. Other processes (readers) may still
      // have active mappings at this point. The grow() call modifies segment_manager
      // metadata (size) in shared memory, which becomes visible to those processes via
      // MAP_SHARED, even though their mappings remain at the old (smaller) size.
      // The MST write lock and the remap logic in makeExtentMapIndexImpl/grabEMIndex
      // mitigate this by ensuring readers remap before accessing the grown region.
      bi::managed_shared_memory::grow(keyName.c_str(), incSize);
      // Open only with the assumption ::grow() can be called on read-write shmem.
      fShmSegment = new bi::managed_shared_memory(bi::open_only, keyName.c_str());
      // Update size.
      fSize = newSize;
    }
  }

  return 0;
}

// Dummy method that has no references in the code.
int BRMManagedShmImpl::clear(unsigned newKey, off_t newSize)
{
  return 0;
}

// This method calls for all related shmem pointers to be refreshed.
void BRMManagedShmImpl::setReadOnly()
{
  if (fReadOnly)
    return;
  bool readOnly = true;
  remap(readOnly);
  fReadOnly = true;
}

void BRMManagedShmImpl::swap(BRMManagedShmImpl& rhs)
{
  fShmSegment->swap(*rhs.fShmSegment);
  std::swap(fKey, rhs.fKey);
  std::swap(fSize, rhs.fSize);
  std::swap(fReadOnly, rhs.fReadOnly);
}

// The method was copied from non-managed shmem impl class
// and it has no refences in MCS 6.x code.
void BRMManagedShmImpl::destroy()
{
  string keyName = ShmKeys::keyToName(fKey);
  try
  {
    bi::shared_memory_object::remove(keyName.c_str());
  }
  catch (bi::interprocess_exception& b)
  {
    std::ostringstream o;
    o << "BRMManagedShmImpl::destroy caught an exception removing a managed shared memory segment: "
      << b.what();
    log(o.str());
    throw;
  }
}

void BRMManagedShmImpl::remap(bool readOnly)
{
  delete fShmSegment;
  fShmSegment = nullptr;
  string keyName = ShmKeys::keyToName(fKey);
  if (readOnly)
    fShmSegment = new bi::managed_shared_memory(bi::open_read_only, keyName.c_str());
  else
    fShmSegment = new bi::managed_shared_memory(bi::open_only, keyName.c_str());
  fSize = fShmSegment->get_size();
}

BRMVersionedShmBase::BRMVersionedShmBase(unsigned keyBase, bool readOnly)
 : BRMShmImplParent(keyBase, 0, readOnly), fKeyBase(keyBase), fReadStateIndex(acquireReadStateIndex(keyBase))
{
  openOrCreateMetadataArea();
}

// Nothing to do, and in particular not discardUpdate(): by the time this runs
// the derived class - which is what owns the copy an update is open on - is
// already gone. Each derived destructor discards its own
BRMVersionedShmBase::~BRMVersionedShmBase()
{
  releaseReadStateIndex(fKeyBase);
}

bi::permissions BRMVersionedShmBase::unrestrictedPerms()
{
  bi::permissions perms;
  perms.set_unrestricted();
  return perms;
}

// The metadata area is the one piece of the segment whose location never
// changes, so it is the only thing a process has to find on its own. It is
// always mapped read-write: the only writes to it happen in publishUpdate(),
// which read-only processes never call, and mapping it read-write keeps a
// read-only process able to bootstrap an empty data area (which is what the
// in-place implementation used to do)
void BRMVersionedShmBase::openOrCreateMetadataArea()
{
  string name = metadataName();

  try
  {
    bi::shared_memory_object shm(bi::create_only, name.c_str(), bi::read_write, unrestrictedPerms());
    shm.truncate(MetadataAreaSize);
    auto region = std::make_shared<bi::mapped_region>(shm, bi::read_write);

    auto* meta = static_cast<ShmMetadata*>(region->get_address());
    meta->version.store(ShmMetadata::MetadataVersion, std::memory_order_relaxed);
    meta->currentDataAreaId.store(0, std::memory_order_relaxed);

    for (uint64_t slot = 0; slot < DataAreaSlots; ++slot)
    {
      meta->slotId[slot].store(0, std::memory_order_relaxed);
      meta->slotIncarnation[slot].store(0, std::memory_order_relaxed);
    }

    meta->groupPublishSeq.store(0, std::memory_order_relaxed);

    for (uint32_t i = 0; i < ReaderSlots; ++i)
    {
      meta->readers[i].readingId.store(0, std::memory_order_relaxed);
      meta->readers[i].owner.store(0, std::memory_order_relaxed);
    }

    meta->unannouncedReaders.store(0, std::memory_order_relaxed);

    // The mutex lives in shared memory, so it has to be initialized exactly
    // once, by whoever creates the area.
    // Nobody may touch it before the magic below is published
    pthread_mutexattr_t attr;
    int rc = pthread_mutexattr_init(&attr);

    if (rc == 0)
    {
      rc = pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);

      if (rc == 0)
        rc = pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);

      if (rc == 0)
        rc = pthread_mutex_init(&meta->updateMutex, &attr);

      pthread_mutexattr_destroy(&attr);
    }

    if (rc != 0)
    {
      bi::shared_memory_object::remove(name.c_str());
      ostringstream o;
      o << "BRM could not initialize the update mutex of the metadata area (" << name
        << "): " << strerror(rc);
      log(o.str());
      throw runtime_error(o.str());
    }

    // Published last: until this store lands the area reads as zeroes and
    // anybody else who opens it keeps waiting.
    meta->magic.store(ShmMetadata::MetadataMagic, std::memory_order_release);

    fMetadataShmObj.swap(shm);
    fMetadataRegion = std::move(region);
    return;
  }
  catch (bi::interprocess_exception& b)
  {
    if (b.get_error_code() != bi::already_exists_error)
    {
      ostringstream o;
      o << "BRM caught an exception creating the metadata area (" << name << "): " << b.what();
      log(o.str());
      throw;
    }
  }

  // Somebody else got there first; wait until they are done initializing it.
  for (uint32_t tries = 0;; ++tries)
  {
    try
    {
      bi::shared_memory_object shm(bi::open_only, name.c_str(), bi::read_write);
      off_t curSize = 0;
      shm.get_size(curSize);

      if (curSize >= static_cast<off_t>(MetadataAreaSize))
      {
        auto region = std::make_shared<bi::mapped_region>(shm, bi::read_write);
        auto* meta = static_cast<ShmMetadata*>(region->get_address());

        if (meta->magic.load(std::memory_order_acquire) == ShmMetadata::MetadataMagic)
        {
          uint32_t version = meta->version.load(std::memory_order_relaxed);

          if (version != ShmMetadata::MetadataVersion)
          {
            ostringstream o;
            o << "BRM found metadata area " << name << " with version " << version << ", expected "
              << ShmMetadata::MetadataVersion;
            log(o.str());
            throw runtime_error(o.str());
          }

          fMetadataShmObj.swap(shm);
          fMetadataRegion = std::move(region);
          return;
        }
      }
    }
    catch (bi::interprocess_exception&)
    {
      if (tries >= ShmCreateMaxRetries)
        throw;
    }

    if (tries >= ShmCreateMaxRetries)
    {
      ostringstream o;
      o << "BRM gave up waiting for the metadata area " << name << " to be initialized";
      log(o.str());
      throw runtime_error(o.str());
    }

    usleep(NapTimer);
  }
}

ShmMetadata* BRMVersionedShmBase::metadata() const
{
  idbassert(fMetadataRegion);
  auto* meta = static_cast<ShmMetadata*>(fMetadataRegion->get_address());
  idbassert(meta);
  return meta;
}

// Key base + 0 is left alone. It used to hold the structure's rwlock, put
// there by the master segment table out of the same flat namespace as these
// areas; nothing owns it now that the table is gone, but the layout stays as
// it is
unsigned BRMVersionedShmBase::metadataKey() const
{
  return fKeyBase + 1;
}

string BRMVersionedShmBase::metadataName() const
{
  return ShmKeys::keyToName(metadataKey());
}

// One name per slot, reused as the ids come round. That is what lets an object
// be written into again rather than made afresh, which is the whole point:
// making one costs the kernel a page of allocation and a fault for every page
// of the area, and measured at ten times what writing into a resident one
// does. A name therefore means different versions at different times, and
// slotId says which - see announceRead()
unsigned BRMVersionedShmBase::dataAreaKey(uint64_t id) const
{
  return fKeyBase + 2 + static_cast<unsigned>(slotOf(id));
}

string BRMVersionedShmBase::dataAreaName(uint64_t id) const
{
  return ShmKeys::keyToName(dataAreaKey(id));
}

/* ------------------------------------------------------------------------- *
 * The announcements.
 *
 * A reader says which version it is about to read, and a writer will not write
 * into an object anybody is saying that about. That is the whole protocol; it
 * replaces counting readers into slots, the generation that made those counts
 * meaningful, and the handshake between the two.
 *
 * What is left of the handshake is the loop below: announce, then check that
 * the version announced is still the published one. It has to be there. A
 * writer reads the announcements and then writes; a reader announces and then
 * reads. If both could miss each other the writer would write into an area
 * somebody is in. Announcing before re-reading the published id, with both
 * sequentially consistent, is what stops that - and it means a reader can only
 * ever settle on a version that was current when it settled, so no reader can
 * newly appear in a version the writer has already looked past.
 * ------------------------------------------------------------------------- */

thread_local BRMVersionedShmBase::ReadState
    BRMVersionedShmBase::fReadStates[BRMVersionedShmBase::MaxReadStates];

// The key bases that have an index, and how many objects are on each. Only
// touched when one of these objects is made or destroyed, which a process does
// a handful of times at startup
namespace
{
struct ReadStateIndexEntry
{
  uint32_t index;
  uint32_t refs;
};

std::mutex gReadStateIndexMutex;
std::map<unsigned, ReadStateIndexEntry> gReadStateIndexes;
std::vector<uint32_t> gFreeReadStateIndexes;
uint32_t gNextReadStateIndex = 0;
}  // namespace

uint32_t BRMVersionedShmBase::acquireReadStateIndex(unsigned keyBase)
{
  std::lock_guard<std::mutex> lk(gReadStateIndexMutex);

  auto it = gReadStateIndexes.find(keyBase);

  if (it != gReadStateIndexes.end())
  {
    ++it->second.refs;
    return it->second.index;
  }

  uint32_t index;

  if (!gFreeReadStateIndexes.empty())
  {
    index = gFreeReadStateIndexes.back();
    gFreeReadStateIndexes.pop_back();
  }
  else
  {
    // A process has one segment for each of the extent map, its index, the
    // free list, the VSS, the VBBM and the copy locks. Running out means a
    // seventh kind was added without the table being grown, so say so at
    // startup rather than quietly sharing a slot with another segment
    idbassert(gNextReadStateIndex < MaxReadStates);
    index = gNextReadStateIndex++;
  }

  gReadStateIndexes[keyBase] = ReadStateIndexEntry{index, 1};
  return index;
}

void BRMVersionedShmBase::releaseReadStateIndex(unsigned keyBase)
{
  std::lock_guard<std::mutex> lk(gReadStateIndexMutex);

  auto it = gReadStateIndexes.find(keyBase);

  if (it == gReadStateIndexes.end())
    return;

  if (--it->second.refs > 0)
    return;

  // Handed back for another key base to take. A thread that read this segment
  // still has whatever it left in that entry, which is why announceRead()
  // checks the key base before trusting the slot in it
  gFreeReadStateIndexes.push_back(it->second.index);
  gReadStateIndexes.erase(it);
}

uint64_t BRMVersionedShmBase::readerOwnerToken()
{
  uint64_t pid = static_cast<uint64_t>(getpid());
  uint64_t tid = static_cast<uint64_t>(gettid());
  return (pid << 32) | (tid & 0xffffffffULL);
}

bool BRMVersionedShmBase::readerOwnerIsGone(uint64_t token)
{
  if (token == 0)
    return false;  // unclaimed, not abandoned

  pid_t pid = static_cast<pid_t>(token >> 32);
  pid_t tid = static_cast<pid_t>(token & 0xffffffffULL);

  if (pid <= 0 || tid <= 0)
    return false;

  return tgkill(pid, tid, 0) < 0 && errno == ESRCH;
}

void BRMVersionedShmBase::announceRead(uint64_t id)
{
  auto& me_ = fReadStates[fReadStateIndex];

  // A nested read of this segment keeps the announcement the outermost one
  // made: it is reading the same version, so it is the same pin. A read of
  // another segment nested inside this one is not that, and has a state and a
  // depth of its own. Taken before metadata() so that the nested case, which
  // is the common one, does no work at all beyond this counter
  if (me_.depth++ > 0)
    return;

  auto* meta = metadata();

  // This entry last belonged to a segment that has since gone, so whatever
  // slot is recorded in it was claimed in that segment's table and means
  // nothing here
  if (me_.keyBase != fKeyBase)
  {
    me_.keyBase = fKeyBase;
    me_.slot = -1;
    me_.unannounced = false;
  }

  if (me_.slot < 0)
  {
    // First read this thread has ever done: take a slot of its own
    const uint64_t me = readerOwnerToken();

    for (uint32_t i = 0; i < ReaderSlots; ++i)
    {
      uint64_t unclaimed = 0;

      if (meta->readers[i].owner.compare_exchange_strong(unclaimed, me, std::memory_order_acq_rel,
                                                         std::memory_order_relaxed))
      {
        me_.slot = static_cast<int32_t>(i);
        break;
      }
    }
  }

  if (me_.slot < 0)
  {
    const uint64_t me = readerOwnerToken();

    for (uint32_t i = 0; i < ReaderSlots; ++i)
    {
      const uint64_t held = meta->readers[i].owner.load(std::memory_order_acquire);

      if (!readerOwnerIsGone(held))
      {
        // reader is alive
        continue;
      }

      meta->readers[i].readingId.store(0, std::memory_order_release);

      uint64_t expected = held;
      if (meta->readers[i].owner.compare_exchange_strong(expected, me, std::memory_order_acq_rel,
                                                         std::memory_order_relaxed))
      {
        me_.slot = static_cast<int32_t>(i);
        break;
      }
    }
  }

  if (me_.slot < 0)
  {
    // More reading threads than slots. Rather than read unseen, say so: reuse
    // is off segment-wide while anybody is in here, which costs performance
    // and nothing else
    me_.unannounced = true;
    meta->unannouncedReaders.fetch_add(1, std::memory_order_seq_cst);
    return;
  }

  meta->readers[me_.slot].readingId.store(id, std::memory_order_seq_cst);
}

void BRMVersionedShmBase::withdrawRead()
{
  auto& me_ = fReadStates[fReadStateIndex];

  if (me_.depth == 0 || --me_.depth > 0)
    return;

  auto* meta = metadata();

  if (me_.unannounced)
  {
    me_.unannounced = false;
    meta->unannouncedReaders.fetch_sub(1, std::memory_order_seq_cst);
    return;
  }

  // release, where the announcement needs seq_cst: nothing this thread does
  // afterwards has to be ordered against the withdrawal. A withdrawal the
  // writer has not seen yet only makes it believe the slot is still being
  // read, so it leaves the area alone and takes a fresh one - slower, never
  // wrong
  if (me_.slot >= 0)
    meta->readers[me_.slot].readingId.store(0, std::memory_order_release);
}

// Read after the announcements have had their chance to land, which is what
// the sequential consistency on both sides is for. A reader that settled
// before this ran is seen; one that settles after cannot have settled on the
// version in this slot, because that version is no longer the published one
// and the reader's own re-check would have sent it elsewhere.
bool BRMVersionedShmBase::slotIsQuiet(uint64_t slot) const
{
  auto* meta = metadata();

  if (meta->unannouncedReaders.load(std::memory_order_seq_cst) != 0)
    return false;

  uint64_t occupant = meta->slotId[slot].load(std::memory_order_acquire);

  if (occupant == 0)
    return false;  // nothing has ever lived here

  for (uint32_t i = 0; i < ReaderSlots; ++i)
  {
    if (meta->readers[i].readingId.load(std::memory_order_seq_cst) != occupant)
      continue;

    // Somebody is in it, or was when they died. A thread that is gone will
    // never withdraw its announcement, so take it back rather than let one
    // crashed reader stop this slot ever being written into again
    const uint64_t held = meta->readers[i].owner.load(std::memory_order_acquire);

    if (!readerOwnerIsGone(held))
      return false;

    meta->readers[i].readingId.store(0, std::memory_order_release);
    meta->readers[i].owner.store(0, std::memory_order_release);
  }

  return true;
}

void BRMVersionedShmBase::beginGroupPublish()
{
  metadata()->groupPublishSeq.fetch_add(1, std::memory_order_acq_rel);
}

void BRMVersionedShmBase::endGroupPublish()
{
  metadata()->groupPublishSeq.fetch_add(1, std::memory_order_acq_rel);
}

uint64_t BRMVersionedShmBase::groupPublishSequence() const
{
  return metadata()->groupPublishSeq.load(std::memory_order_acquire);
}

uint64_t BRMVersionedShmBase::currentId() const
{
  return metadata()->currentDataAreaId.load(std::memory_order_acquire);
}

/* Every slot's name plus the metadata area's, for destroy(). */
void BRMVersionedShmBase::removeAllNames()
{
  for (uint64_t slot = 0; slot < DataAreaSlots; ++slot)
    bi::shared_memory_object::remove(ShmKeys::keyToName(fKeyBase + 2 + unsigned(slot)).c_str());

  bi::shared_memory_object::remove(metadataName().c_str());
}

/* Serializes writers segment-wide: only the process holding this may have an
   update open, which is what makes "the next data area is the current one + 1"
   a safe way to pick an id.

   If a writer killed between beginUpdate() and publish/discard. The mutex is robust,
   so the next writer is told about it with EOWNERDEAD rather than waiting on a lock
   nobody will ever release - and there is nothing for it to repair, because of
   what a writer does. It only ever changes a copy: the published data area is
   untouched, and the copy has a name of its own that no published id points at,
   so nobody can reach it.
   Wherever the dead writer got to,

     - before it created the copy, nothing happened at all;
     - after creating it but before the id was published, no reader can name
       it, and the next writer is handed the same id and removes the leftover
       before making its own;
     - after the id was published, the update is complete and visible.

   the shared state is consistent in every one of them. So recovery is saying
   so and carrying on.

   The timeout that is left catches a writer that is alive and stuck, which is
   not something waiting longer would fix. */
void BRMVersionedShmBase::lockUpdateMutex()
{
  std::unique_lock<std::mutex> gate(fUpdateGate);

  idbassert(!fHoldsUpdateMutex);

  struct timespec deadline;
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += UpdateMutexTimeoutSeconds;

  auto* meta = metadata();
  int rc = pthread_mutex_timedlock(&meta->updateMutex, &deadline);

  if (rc == EOWNERDEAD)
  {
    ostringstream o;
    o << "BRM found the update mutex of the metadata area " << metadataName()
      << " held by a process that died, and has taken it over. The data area it was preparing was never"
         " published and has been left for the next update to overwrite.";
    log(o.str());

    /* A writer killed between beginGroupPublish() and endGroupPublish()
       leaves the sequence odd, and a reader waiting for it to go even would
       wait for a process that is gone. Nothing of the group was published -
       the areas it was preparing were never named by anybody - so the last
       complete state is the one still current, and saying so is just making
       the count even again.

       Here rather than anywhere else because this is the moment the segment's
       writer role is taken over, and the extent map's update mutex is the one
       held across its whole group publication. */
    if (meta->groupPublishSeq.load(std::memory_order_acquire) & 1)
      meta->groupPublishSeq.fetch_add(1, std::memory_order_acq_rel);

    int consistentRc = pthread_mutex_consistent(&meta->updateMutex);

    if (consistentRc != 0)
    {
      pthread_mutex_unlock(&meta->updateMutex);
      ostringstream oo;
      oo << "BRM could not make the update mutex of the metadata area " << metadataName()
         << " consistent: " << strerror(consistentRc);
      log(oo.str());
      throw runtime_error(oo.str());
    }

    rc = 0;
  }

  if (rc != 0)
  {
    ostringstream o;

    if (rc == ETIMEDOUT)
      o << "BRM timed out after " << UpdateMutexTimeoutSeconds
        << "s waiting for the update mutex of the metadata area " << metadataName()
        << ". A writer is most likely alive and stuck holding it.";
    else if (rc == ENOTRECOVERABLE)
      o << "BRM found the update mutex of the metadata area " << metadataName()
        << " unrecoverable: a writer died holding it and whoever took it over could not make it"
           " consistent, which retires the mutex for good. The BRM shared memory has to be cleared."
           " (A process that takes it over and then dies itself is not this case - the next writer"
           " simply gets EOWNERDEAD in its turn.)";
    else
      o << "BRM could not take the update mutex of the metadata area " << metadataName() << ": "
        << strerror(rc);

    log(o.str());
    throw runtime_error(o.str());
  }

  // Both are ours now. release() detaches the guard without unlocking, so the
  // gate stays held until unlockUpdateMutex() gives it back - and nothing of
  // the guard outlives this call to be shared with the next thread. Anything
  // that threw above left the guard to unlock it on the way out
  gate.release();
  fUpdateOwner.store(std::this_thread::get_id(), std::memory_order_release);
  fHoldsUpdateMutex = true;
}

void BRMVersionedShmBase::unlockUpdateMutex()
{
  if (!fHoldsUpdateMutex)
    return;

  fHoldsUpdateMutex = false;
  fUpdateOwner.store(std::thread::id(), std::memory_order_release);
  pthread_mutex_unlock(&metadata()->updateMutex);
  // Last, so no thread of this process can be inside an update before the
  // shared mutex has actually been given back.
  fUpdateGate.unlock();
}

// Dummy method that has no references in the code.
int BRMVersionedShmBase::clear(unsigned /*newKey*/, off_t /*newSize*/)
{
  return 0;
}

////////////////////////////////////////////////////////////////////////////////////
// Area policies
////////////////////////////////////////////////////////////////////////////////////

ManagedAreaPolicy::Area* ManagedAreaPolicy::create(const string& name, off_t size,
                                                   const bi::permissions& perms)
{
  return new Area(bi::create_only, name.c_str(), size, 0, perms);
}

ManagedAreaPolicy::Area* ManagedAreaPolicy::open(const string& name, bool readOnly)
{
  return readOnly ? new Area(bi::open_read_only, name.c_str()) : new Area(bi::open_only, name.c_str());
}

off_t ManagedAreaPolicy::size(const Area& area)
{
  return static_cast<off_t>(area.get_size());
}

void* ManagedAreaPolicy::address(const Area& area)
{
  return area.get_address();
}

ManagedAreaPolicy::Area* ManagedAreaPolicy::grow(const string& name, off_t inc)
{
  bi::managed_shared_memory::grow(name.c_str(), inc);
  return new Area(bi::open_only, name.c_str());
}

// The segment manager at the front of the image records the size the image
// was built for, and the mapping is of an object of that size, so an image of
// some other size cannot be dropped on top of this one. At the same size it
// can: that is all a copy has ever been, a memcpy over a segment created at
// the source's size, and none of the offsets inside a managed image depend on
// where it is mapped
bool ManagedAreaPolicy::overwrite(Area& area, off_t size, const void* src, off_t srcSize)
{
  if (size != srcSize || static_cast<off_t>(area.get_size()) != srcSize)
    return false;

  memcpy(area.get_address(), src, srcSize);
  return true;
}

RawAreaPolicy::Area* RawAreaPolicy::create(const string& name, off_t size, const bi::permissions& perms)
{
  std::unique_ptr<Area> area(new Area);
  bi::shared_memory_object obj(bi::create_only, name.c_str(), bi::read_write, perms);
  obj.truncate(size);
  bi::mapped_region region(obj, bi::read_write);
  area->obj.swap(obj);
  area->reg.swap(region);
  return area.release();
}

RawAreaPolicy::Area* RawAreaPolicy::open(const string& name, bool readOnly)
{
  std::unique_ptr<Area> area(new Area);
  auto mode = readOnly ? bi::read_only : bi::read_write;
  bi::shared_memory_object obj(bi::open_only, name.c_str(), mode);
  bi::mapped_region region(obj, mode);
  area->obj.swap(obj);
  area->reg.swap(region);
  return area.release();
}

off_t RawAreaPolicy::size(const Area& area)
{
  return static_cast<off_t>(area.reg.get_size());
}

void* RawAreaPolicy::address(const Area& area)
{
  return area.reg.get_address();
}

// No copy: the object is resized in place and mapped again. The caller has
// dropped the mapping it had, so nothing is left mapped at the size the object
// no longer has, and the tail the resize adds is zero-filled by the kernel
RawAreaPolicy::Area* RawAreaPolicy::grow(const string& name, off_t inc)
{
  std::unique_ptr<Area> area(new Area);
  bi::shared_memory_object obj(bi::open_only, name.c_str(), bi::read_write);
  off_t curSize = 0;
  obj.get_size(curSize);
  obj.truncate(curSize + inc);
  bi::mapped_region region(obj, bi::read_write);
  area->obj.swap(obj);
  area->reg.swap(region);
  return area.release();
}

bool RawAreaPolicy::overwrite(Area& area, off_t size, const void* src, off_t srcSize)
{
  off_t have = static_cast<off_t>(area.reg.get_size());

  if (have < size || have < srcSize)
    return false;

  auto* base = static_cast<char*>(area.reg.get_address());
  memcpy(base, src, srcSize);
  memset(base + srcSize, 0, have - srcSize);
  return true;
}


template <typename AreaPolicy>
BRMVersionedShmImplT<AreaPolicy>::BRMVersionedShmImplT(unsigned keyBase, off_t initialSize, bool readOnly)
 : BRMVersionedShmBase(keyBase, readOnly)
{
  (void)initialSize;  // the size of a data area is decided when it is allocated
  // Picks up the data area that is published right now, if any.
  refresh();
}

template <typename AreaPolicy>
BRMVersionedShmImplT<AreaPolicy>::~BRMVersionedShmImplT()
{
  // An update that was neither published nor discarded belongs to nobody now.
  discardUpdate();
  // The published area goes with fSegment, unless a pin handed out from here is
  // still alive somewhere, in which case it goes with the last of those.
}

template <typename AreaPolicy>
uint64_t BRMVersionedShmImplT<AreaPolicy>::mappedId() const
{
  std::lock_guard<std::mutex> lk(fSegmentMutex);
  return fMappedId;
}

template <typename AreaPolicy>
bool BRMVersionedShmImplT<AreaPolicy>::refresh()
{
  std::lock_guard<std::mutex> lk(fSegmentMutex);
  return refreshLocked();
}

// Refresh and hand-out in one step under the same lock. Doing them separately
// would leave a window in which a writer publishes and this process remaps
// between the two, so the caller would get an area it never checked - and,
// worse for a reader that has already looked at the extent map, a different
// area from the one the previous call in the same read gave it.
//
// The handle keeps the area mapped here. The announcement, which the handle
// also carries, is what stops a writer writing into it: the object is one of
// a few that get used again and again, so unlike a mapping it is not enough
// simply to hold on to it.
//
// The announcement is this thread's, so the handle must be let go of by the
// thread that took it. Every caller does: the pins live in thread_local state
// and are dropped by the grab that took them
template <typename AreaPolicy>
typename BRMVersionedShmImplT<AreaPolicy>::DataAreaPin BRMVersionedShmImplT<AreaPolicy>::pin()
{
  // Withdraws this thread's announcement when the last copy of the handle
  // goes. Holds the metadata mapping too, because a pin can outlive the object
  // that produced it and the announcement still has to be taken back
  struct ReadHold
  {
    DataAreaPin area;
    std::shared_ptr<bi::mapped_region> meta;
    BRMVersionedShmBase* owner;

    ReadHold(DataAreaPin a, std::shared_ptr<bi::mapped_region> m, BRMVersionedShmBase* o)
     : area(std::move(a)), meta(std::move(m)), owner(o)
    {
    }

    ~ReadHold()
    {
      owner->withdrawRead();
    }
  };

  for (;;)
  {
    uint64_t id = currentId();

    if (id == 0)  // nothing has been published yet
      return DataAreaPin();

    announceRead(id);

    /* Announced - now check nothing was published while we were saying so. If
       something was, this announcement is for a version that may already be
       being written into, so take it back and start again with the new one.
       Terminates: each turn reads a later id. */
    if (currentId() != id)
    {
      withdrawRead();
      continue;
    }

    DataAreaPin mapping;

    {
      std::lock_guard<std::mutex> lk(fSegmentMutex);
      refreshLocked();
      mapping = fSegment;
    }

    if (!mapping)
    {
      withdrawRead();
      return DataAreaPin();
    }

    std::shared_ptr<ReadHold> held = std::make_shared<ReadHold>(std::move(mapping), fMetadataRegion, this);

    return DataAreaPin(held, held->area.get());
  }
}

// Handing the new area over rather than unmapping the old one in place is the
// whole point: a pin taken on the old area stays valid, and the area is unmapped
// when the last of those pins is dropped, which may be long after this call
template <typename AreaPolicy>
void BRMVersionedShmImplT<AreaPolicy>::adoptLocked(const DataAreaPin& area, uint64_t id)
{
  fSegment = area;
  fMappedId = id;
  fKey = dataAreaKey(id);
  fSize = AreaPolicy::size(*area);
}

template <typename AreaPolicy>
bool BRMVersionedShmImplT<AreaPolicy>::refreshLocked()
{
  uint64_t id = currentId();

  if (id == 0)  // nothing has been published yet
    return false;

  if (fSegment && id == fMappedId)
    return false;

  uint64_t slot = slotOf(id);

  // Read before the mapping is looked at, never after: an incarnation read
  // afterwards could be one the mapping is not of, and a mapping filed under
  // an incarnation it is not of would be handed out as current later on. Read
  // first, the worst that happens is a mapping filed under an incarnation
  // that has already gone by, which costs one reopen and no correctness
  uint64_t incarnation = metadata()->slotIncarnation[slot].load(std::memory_order_acquire);
  DataAreaPin area = mappingForSlotLocked(slot, incarnation);

  if (!area)
  {
    string name = dataAreaName(id);

    try
    {
      // Mapped before the old one is dropped, so that a failure here leaves
      // this process with the mapping it already had.
      area.reset(AreaPolicy::open(name, fReadOnly));
    }
    catch (bi::interprocess_exception& b)
    {
      ostringstream o;
      o << "BRM: the metadata area " << metadataName() << " points at data area " << id << " (" << name
        << ") which cannot be opened: " << b.what()
        << ". The BRM shared memory is inconsistent and has to be cleared.";
      log(o.str());
      throw runtime_error(o.str());
    }

    cacheSlotLocked(slot, incarnation, area);
  }

  adoptLocked(area, id);

  return true;
}

template <typename AreaPolicy>
typename BRMVersionedShmImplT<AreaPolicy>::DataAreaPin BRMVersionedShmImplT<AreaPolicy>::mappingForSlotLocked(
    uint64_t slot, uint64_t incarnation) const
{
  const SlotMapping& mapped = fSlotMappings[slot];

  return (mapped.area && mapped.incarnation == incarnation) ? mapped.area : DataAreaPin();
}

template <typename AreaPolicy>
void BRMVersionedShmImplT<AreaPolicy>::cacheSlotLocked(uint64_t slot, uint64_t incarnation,
                                                       const DataAreaPin& area)
{
  fSlotMappings[slot].incarnation = incarnation;
  fSlotMappings[slot].area = area;
}

template <typename AreaPolicy>
void BRMVersionedShmImplT<AreaPolicy>::forgetSlotLocked(uint64_t slot)
{
  fSlotMappings[slot].incarnation = 0;
  fSlotMappings[slot].area.reset();
}

template <typename AreaPolicy>
typename BRMVersionedShmImplT<AreaPolicy>::DataAreaPin BRMVersionedShmImplT<AreaPolicy>::createDataArea(
    uint64_t id, off_t size)
{
  idbassert(size > 0);
  uint64_t slot = slotOf(id);
  string name = dataAreaName(id);

  // Did before the object goes, not after: every mapping anybody has of this
  // slot is about to be of something that is not there any more, and the only
  // thing that tells them so is the incarnation. Doing it afterwards would be
  // right only if the creation below could not fail - and it can, a data area
  // is megabytes and /dev/shm is finite
  metadata()->slotIncarnation[slot].fetch_add(1, std::memory_order_acq_rel);

  {
    std::lock_guard<std::mutex> lk(fSegmentMutex);
    forgetSlotLocked(slot);
  }

  bi::shared_memory_object::remove(name.c_str());

  DataAreaPin area(AreaPolicy::create(name, size, unrestrictedPerms()));

  std::lock_guard<std::mutex> lk(fSegmentMutex);
  cacheSlotLocked(slot, metadata()->slotIncarnation[slot].load(std::memory_order_acquire), area);

  return area;
}

// The fast way, and the point of the whole slot arrangement: the object in
// the slot is still there, nobody is announcing the version in it, and this
// process already has it mapped - so the copy is a memcpy into resident pages
// and the publication needs no trip into the kernel at all. Ten times cheaper
// than making a new one, measured. Everything that can make it impossible ends
// in a null return and sends the caller to createDataArea()
template <typename AreaPolicy>
typename BRMVersionedShmImplT<AreaPolicy>::DataAreaPin BRMVersionedShmImplT<AreaPolicy>::reuseSlotFor(
    uint64_t newId, off_t createSize, const void* src, off_t srcSize)
{
  uint64_t slot = slotOf(newId);

  if (fReadOnly)
    return DataAreaPin();

  // Nobody may be inside the version this object still holds.
  if (!slotIsQuiet(slot))
    return DataAreaPin();

  DataAreaPin area;

  {
    std::lock_guard<std::mutex> lk(fSegmentMutex);
    area = mappingForSlotLocked(slot, metadata()->slotIncarnation[slot].load(std::memory_order_acquire));
  }

  // Nothing mapped here, or what is mapped is of an object that has since been
  // replaced, and reopening it would cost the syscalls this path exists to save.
  if (!area)
    return DataAreaPin();

  // The copy itself is made with no lock held. It is of a whole extent map in
  // the worst case, and the only thread that could be told to take an interest
  // in this slot is this one - readers follow the published id, which is not
  // here yet
  if (!AreaPolicy::overwrite(*area, createSize, src, srcSize))
    return DataAreaPin();

  return area;
}

template <typename AreaPolicy>
void BRMVersionedShmImplT<AreaPolicy>::beginUpdate(off_t minSize)
{
  // Held until the update is published or discarded, so no other writer can
  // publish underneath us and the id picked below stays ours - and so that a
  // thread arriving while another is mid-update waits here rather than going
  // on to work on that thread's copy
  lockUpdateMutex();

  try
  {
    idbassert(!fUpdateSegment);

    // Copy what is published right now, not what we happened to map last time.
    // Pinned rather than read off fSegment, so that the source of the memcpy
    // below cannot be swapped out by a reader in another thread refreshing.
    const DataAreaPin src = pin();

    // The published id cannot change while we hold the mutex, so the next one
    // needs no counter of its own. An id a discarded update burned is simply
    // reused; createDataArea() removes whatever that name still refers to.
    uint64_t newId = metadata()->currentDataAreaId.load(std::memory_order_relaxed) + 1;

    // 0 is what currentDataAreaId holds when nothing has ever been published,
    // so publishing it would tell every reader the segment was empty
    if (newId == 0)
      newId = 1;
    off_t srcSize = src ? AreaPolicy::size(*src) : 0;

    if (srcSize == 0)
    {
      // The very first data area: nothing to copy, and nothing to take a size
      // from either, so the caller has to say how big it should be.
      idbassert(minSize > 0);
      fUpdateSegment = createDataArea(newId, minSize);
      fUpdateId = newId;
      return;
    }

    off_t createSize = AreaPolicy::SizeIsInternal ? srcSize : std::max(minSize, srcSize);
    const void* srcAddr = AreaPolicy::address(*src);

    fUpdateSegment = reuseSlotFor(newId, createSize, srcAddr, srcSize);

    if (!fUpdateSegment)
    {
      fUpdateSegment = createDataArea(newId, createSize);
      memcpy(AreaPolicy::address(*fUpdateSegment), srcAddr, srcSize);
    }

    fUpdateId = newId;

    // A reused object may well be larger than the copy needed, so what it
    // actually came out at is what says whether there is growing left to do.
    off_t have = AreaPolicy::size(*fUpdateSegment);

    if (minSize > have)
      growUpdate(minSize - have);
  }
  catch (...)
  {
    // Drops the copy if there is one, and the mutex either way.
    discardUpdate();
    unlockUpdateMutex();
    throw;
  }
}

template <typename AreaPolicy>
void BRMVersionedShmImplT<AreaPolicy>::growUpdate(off_t incSize)
{
  idbassert(fUpdateSegment);

  if (incSize <= 0)
    return;

  const string name = dataAreaName(fUpdateId);

  uint64_t slot = slotOf(fUpdateId);

  try
  {
    // The object size is changing, invalidating all existing mappings (including
    // this process's cached one).
    // To signal to other processes that their cached mappings are now stale,
    // we bump the slot's incarnation number.
    // Resizing is safe here because the object hasn't been published yet, meaning
    // no other process is reading it. However, pointers held by this process will
    // become invalid, so the caller must re-fetch them after the resize.
    // Note: We unmap/drop the cached segment before growing it, because managed
    // segments cannot be resized while any process (including our own local cache)
    // holds an active mapping.
    metadata()->slotIncarnation[slot].fetch_add(1, std::memory_order_acq_rel);

    {
      std::lock_guard<std::mutex> lk(fSegmentMutex);
      forgetSlotLocked(slot);
    }

    fUpdateSegment.reset();
    fUpdateSegment.reset(AreaPolicy::grow(name, incSize));

    std::lock_guard<std::mutex> lk(fSegmentMutex);
    cacheSlotLocked(slot, metadata()->slotIncarnation[slot].load(std::memory_order_acquire), fUpdateSegment);
  }
  catch (exception& e)
  {
    ostringstream o;
    o << "BRMVersionedShmImplT::growUpdate(): " << name << ": " << e.what();
    log(o.str());
    throw;
  }
}

template <typename AreaPolicy>
uint64_t BRMVersionedShmImplT<AreaPolicy>::publishUpdate()
{
  idbassert(fUpdateSegment);

  auto* meta = metadata();
  uint64_t publishedId = fUpdateId;

  {
    std::lock_guard<std::mutex> lk(fSegmentMutex);

    // The slot says which version it holds before the id sends anybody there,
    // so a reader that follows the new id finds the slot already agreeing
    meta->slotId[slotOf(publishedId)].store(publishedId, std::memory_order_release);

    // The switch. Everything reachable through the new id is already written,
    // hence the release: a reader that sees the new id sees a complete data area.
    meta->currentDataAreaId.store(publishedId, std::memory_order_release);

    // The copy stops being a private one and becomes what readers get. Whoever
    // is still reading the area this replaces is unaffected; it is unmapped once
    // they let go of it.
    adoptLocked(fUpdateSegment, publishedId);
  }

  fUpdateSegment.reset();
  fUpdateId = 0;

  unlockUpdateMutex();

  return publishedId;
}

template <typename AreaPolicy>
void BRMVersionedShmImplT<AreaPolicy>::discardUpdate()
{
  if (fUpdateSegment)
  {
    fUpdateSegment.reset();
    fUpdateId = 0;
  }

  // No-op unless this process actually has an update open.
  unlockUpdateMutex();
}

template <typename AreaPolicy>
void BRMVersionedShmImplT<AreaPolicy>::setReadOnly()
{
  if (fReadOnly)
    return;

  fReadOnly = true;

  std::lock_guard<std::mutex> lk(fSegmentMutex);

  // Every mapping kept here is read-write and none may be handed out now. What
  // is dropped is remapped read-only the next time it is wanted.
  for (uint64_t slot = 0; slot < DataAreaSlots; ++slot)
    forgetSlotLocked(slot);

  if (!fSegment)
    return;

  // Remapping the same area read-only, so a reader that pinned it read-write is
  // left with a mapping of its own and is not disturbed.
  DataAreaPin area(AreaPolicy::open(dataAreaName(fMappedId), true));
  adoptLocked(area, fMappedId);
}

template <typename AreaPolicy>
void BRMVersionedShmImplT<AreaPolicy>::destroy()
{
  discardUpdate();

  {
    std::lock_guard<std::mutex> lk(fSegmentMutex);
    fSegment.reset();
    fMappedId = 0;

    for (uint64_t slot = 0; slot < DataAreaSlots; ++slot)
      forgetSlotLocked(slot);
  }

  removeAllNames();
}

/* The only two instantiations. Keeping them here rather than letting every
   includer make its own is what keeps the definitions above out of the header. */
template class BRMVersionedShmImplT<ManagedAreaPolicy>;
template class BRMVersionedShmImplT<RawAreaPolicy>;

}  // namespace BRM

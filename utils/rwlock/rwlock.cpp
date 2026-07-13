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

#include <unistd.h>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sstream>
#include <iomanip>
#include <tr1/unordered_map>

#ifndef NDEBUG
#define NDEBUG
#endif
#include <cassert>
using namespace std;

#include <boost/thread/thread.hpp>

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/sync/named_semaphore.hpp>
#include <boost/version.hpp>
namespace bi = boost::interprocess;

#define RWLOCK_DLLEXPORT
#include "rwlock.h"
#undef RWLOCK_DLLEXPORT

using namespace boost::posix_time;

#include "shmkeys.h"

#include "installdir.h"

namespace
{
using namespace rwlock;

// This mutex needs to be fully instantiated by the runtime static object
// init mechanism or the lock in makeRWLockShmImpl() will fail
boost::mutex instanceMapMutex;
typedef std::tr1::unordered_map<int, RWLockShmImpl*> LockMap_t;
// Windows doesn't init static objects the same as Linux, so make this a ptr
LockMap_t* lockMapPtr = 0;

}  // namespace

namespace rwlock
{
#if defined(DEBUG) && !defined(_MSC_VER)
#define RWLOCK_DEBUG
#endif

#ifdef RWLOCK_DEBUG
#define PRINTSTATE()                                                      \
  cerr << "  reading = " << fPImpl->fState->reading << endl               \
       << "  writing = " << fPImpl->fState->writing << endl               \
       << "  readerswaiting = " << fPImpl->fState->readerswaiting << endl \
       << "  writerswaiting = " << fPImpl->fState->writerswaiting << endl

#define CHECKSAFETY()                                                                           \
  do                                                                                            \
  {                                                                                             \
    if (!((fPImpl->fState->reading == 0 &&                                                      \
           (fPImpl->fState->writing == 0 || fPImpl->fState->writing == 1)) ||                   \
          (fPImpl->fState->reading > 0 && fPImpl->fState->writing == 0)))                       \
    {                                                                                           \
      cerr << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": safety invariant violation" << endl; \
      PRINTSTATE();                                                                             \
      throw std::logic_error("RWLock: safety invariant violation");                             \
    }                                                                                           \
  } while (0)

#define CHECKLIVENESS()                                                                           \
  do                                                                                              \
  {                                                                                               \
    if (!((!(fPImpl->fState->readerswaiting > 0 || fPImpl->fState->writerswaiting > 0) ||         \
           (fPImpl->fState->reading > 0 || fPImpl->fState->writing > 0)) ||                       \
          (!(fPImpl->fState->reading == 0 && fPImpl->fState->writing == 0) ||                     \
           (fPImpl->fState->readerswaiting == 0 && fPImpl->fState->writerswaiting == 0))))        \
    {                                                                                             \
      cerr << __PRETTY_FUNCTION__ << ":" << __LINE__ << ": liveness invariant violation" << endl; \
      PRINTSTATE();                                                                               \
      throw std::logic_error("RWLock: liveness invariant violation");                             \
    }                                                                                             \
  } while (0)
#else
#define PRINTSTATE() (void)0
#define CHECKSAFETY() (void)0
#define CHECKLIVENESS() (void)0
#endif

/*static*/
RWLockShmImpl* RWLockShmImpl::makeRWLockShmImpl(int key, bool* excl)
{
  boost::mutex::scoped_lock lk(instanceMapMutex);
  LockMap_t::iterator iter;

  if (!lockMapPtr)
    lockMapPtr = new LockMap_t();

  iter = lockMapPtr->find(key);

  if (iter == lockMapPtr->end())
  {
    RWLockShmImpl* ptr = 0;
    bool bExcl = excl ? *excl : false;
    ptr = new RWLockShmImpl(key, bExcl);
    lockMapPtr->insert(make_pair(key, ptr));
    return ptr;
  }
  else if (excl)
  {
    *excl = false;  // This isn't the first time for this lock.
  }

  return iter->second;
}

RWLockShmImpl::RWLockShmImpl(int key, bool excl)
{
  string keyName = BRM::ShmKeys::keyToName(key);
  fKeyString = keyName;

  try
  {
    bi::permissions perms;
    perms.set_unrestricted();
    bi::shared_memory_object shm(bi::create_only, keyName.c_str(), bi::read_write, perms);
    shm.truncate(sizeof(struct State));
    fStateShm.swap(shm);
    bi::mapped_region region(fStateShm, bi::read_write);
    fRegion.swap(region);
    fState = static_cast<State*>(fRegion.get_address());
    fState->writerswaiting = 0;
    fState->readerswaiting = 0;
    fState->reading = 0;

    if (excl)
      fState->writing = 1;
    else
      fState->writing = 0;

    new (&fState->rwMutex) bi::interprocess_upgradable_mutex();
  }
  catch (bi::interprocess_exception& e)
  {
    if (e.get_error_code() == bi::security_error)
    {
      cerr << "RWLock:  Failed to create the lock.  Check perms on /dev/shm; should be 1777" << endl;
      throw;
    }
    if (e.get_error_code() == bi::already_exists_error && excl)
      throw not_excl();
    if (e.get_error_code() != bi::already_exists_error)
      throw;

    try
    {
      bi::shared_memory_object shm(bi::open_only, keyName.c_str(), bi::read_write);
      fStateShm.swap(shm);
    }
    catch (exception& e)
    {
      cerr << "RWLock failed to attach to the " << keyName << " shared mem segment, got " << e.what() << endl;
      throw;
    }
    bi::mapped_region region(fStateShm, bi::read_write);
    fRegion.swap(region);
    fState = static_cast<State*>(fRegion.get_address());
  }
  catch (...)
  {
    runtime_error rex("RWLockShmImpl::RWLockShmImpl(): caught unknown exception");
    cerr << rex.what() << endl;
    throw rex;
  }
}

RWLock::RWLock(int key, bool* excl)
{
  fPImpl = RWLockShmImpl::makeRWLockShmImpl(key, excl);
}

RWLock::~RWLock()
{
}

class not_implemented : public std::exception
{
 public:
  const char* what() const noexcept override
  {
    return "not implemented";
  }
};
void RWLock::down(int /*num*/, bool /*block*/)
{
  throw not_implemented();
}

bool RWLock::timed_down(int /*num*/, const ptime& /*delay*/)
{
  throw not_implemented();
  return false;
}

void RWLock::up(int /*num*/)
{
  throw not_implemented();
}

void RWLock::read_lock(bool block)
{
  if (!block)
  {
    if (!fPImpl->fState->rwMutex.try_lock_sharable())
    {
      throw wouldblock();
    }
    fPImpl->fState->reading += 1;
    return ;
  }
  fPImpl->fState->readerswaiting += 1;
  fPImpl->fState->rwMutex.lock_sharable();
  fPImpl->fState->readerswaiting -= 1;
  fPImpl->fState->reading += 1;
  return ;
}

void RWLock::read_lock_priority(bool block)
{
  // will redirect to read_lock, as the functionality is quite close.
  read_lock(block);
}

void RWLock::read_unlock()
{
  fPImpl->fState->rwMutex.unlock_sharable();
  fPImpl->fState->reading -= 1;
}

void RWLock::write_lock(bool block)
{
  if (!block)
  {
    if (!fPImpl->fState->rwMutex.try_lock())
    {
      throw wouldblock();
    }
    fPImpl->fState->writing += 1;
    return ;
  }
  // XXX Time to check, time to use??? (TOCTOU, google it)
  fPImpl->fState->writerswaiting += 1;
  fPImpl->fState->rwMutex.lock();
  fPImpl->fState->writerswaiting -= 1;
  fPImpl->fState->writing += 1;
  return ;
}

// this exists only for the sake of code cleanup
#define RETURN_STATE(mutex_state, state)                           \
  if (state)                                                       \
  {                                                                \
    state->mutexLocked = mutex_state;                              \
    state->readerswaiting = fPImpl->fState->readerswaiting.load(); \
    state->reading = fPImpl->fState->reading.load();               \
    state->writerswaiting = fPImpl->fState->writerswaiting.load(); \
    state->writing = fPImpl->fState->writing.load();               \
  }

bool RWLock::timed_write_lock(const struct timespec& ts, struct LockState* state)
{
  ptime delay;
  fPImpl->fState->writerswaiting += 1;
  delay = microsec_clock::local_time() + seconds(ts.tv_sec) + microsec(ts.tv_nsec / 1000);
  if (fPImpl->fState->rwMutex.timed_lock(delay))
  {
    fPImpl->fState->writerswaiting -= 1;
    fPImpl->fState->writing += 1;
    RETURN_STATE(false, state);
    return true;
  }
  else
  {
    fPImpl->fState->writerswaiting -= 1;
    RETURN_STATE(false, state);
    return false;
  }
}

void RWLock::write_unlock()
{
  fPImpl->fState->rwMutex.unlock();
  fPImpl->fState->writing -= 1;
  return ;
}

void RWLock::upgrade_to_write()
{
  fPImpl->fState->writerswaiting += 1;
  if (fPImpl->fState->rwMutex.try_unlock_sharable_and_lock())
  {
    fPImpl->fState->writerswaiting -= 1;
    fPImpl->fState->writing += 1;
    fPImpl->fState->reading -= 1;
    return ;
  }
  fPImpl->fState->rwMutex.unlock_sharable();
  fPImpl->fState->reading -= 1;
  fPImpl->fState->rwMutex.lock();
  fPImpl->fState->writing += 1;
  fPImpl->fState->writerswaiting -= 1;
}

/* It's safe (and necessary) to simply convert this writer to a reader without
 blocking */
void RWLock::downgrade_to_read()
{
  fPImpl->fState->readerswaiting += 1;
  fPImpl->fState->rwMutex.unlock_and_lock_sharable();
  fPImpl->fState->readerswaiting -= 1;
  fPImpl->fState->writing -= 1;
  fPImpl->fState->reading += 1;
  return ;
}

void RWLock::reset()
{
  fPImpl->fState->writerswaiting = 0;
  fPImpl->fState->readerswaiting = 0;
  fPImpl->fState->reading = 0;
  fPImpl->fState->writing = 0;
  // XXX Reset mutex?
}

LockState RWLock::getLockState()
{
  LockState ret;

  // XXX Possible TOCTOU.
  ret.reading = fPImpl->fState->reading.load();
  ret.writing = fPImpl->fState->writing.load();
  ret.readerswaiting = fPImpl->fState->readerswaiting.load();
  ret.writerswaiting = fPImpl->fState->writerswaiting.load();
  ret.mutexLocked = false;
  return ret;
}

}  // namespace rwlock

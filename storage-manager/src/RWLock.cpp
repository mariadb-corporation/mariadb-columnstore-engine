/* Copyright (C) 2019 MariaDB Corporation

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

#include "RWLock.h"

namespace storagemanager
{
RWLock::RWLock() : readersWaiting(0), readersRunning(0), writersWaiting(0), writersRunning(0)
{
}

RWLock::~RWLock()
{
  assert(!readersWaiting);
  assert(!readersRunning);
  assert(!writersWaiting);
  assert(!writersRunning);
}

bool RWLock::inUse()
{
  boost::unique_lock<boost::mutex> s(m);
  return readersWaiting || readersRunning || writersWaiting || writersRunning;
}

void RWLock::readLock()
{
  boost::unique_lock<boost::mutex> s(m);

  ++readersWaiting;
  while (writersWaiting != 0 || writersRunning != 0)
    okToRead.wait(s);

  ++readersRunning;
  --readersWaiting;
}

void RWLock::readLock(boost::unique_lock<boost::mutex>& l)
{
  boost::unique_lock<boost::mutex> s(m);
  l.unlock();

  ++readersWaiting;
  while (writersWaiting != 0 || writersRunning != 0)
    okToRead.wait(s);

  ++readersRunning;
  --readersWaiting;
}

void RWLock::readUnlock()
{
  boost::unique_lock<boost::mutex> s(m);

  assert(readersRunning > 0);
  --readersRunning;
  if (readersRunning == 0 && writersWaiting != 0)
    okToWrite.notify_one();
}

void RWLock::writeLock()
{
  boost::unique_lock<boost::mutex> s(m);

  ++writersWaiting;
  while (readersRunning != 0 || writersRunning != 0)
    okToWrite.wait(s);

  ++writersRunning;
  --writersWaiting;
}

void RWLock::writeLock(boost::unique_lock<boost::mutex>& l)
{
  boost::unique_lock<boost::mutex> s(m);
  l.unlock();

  ++writersWaiting;
  while (readersRunning != 0 || writersRunning != 0)
    okToWrite.wait(s);

  ++writersRunning;
  --writersWaiting;
}

void RWLock::writeUnlock()
{
  boost::unique_lock<boost::mutex> s(m);

  assert(writersRunning > 0);
  --writersRunning;
  if (writersWaiting != 0)
    okToWrite.notify_one();
  else if (readersWaiting != 0)
    okToRead.notify_all();
}

}  // namespace storagemanager

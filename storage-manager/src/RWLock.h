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

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

/* Quicky impl of a read-write lock that prioritizes writers. */
namespace storagemanager
{
class RWLock
{
 public:
  RWLock();
  ~RWLock();

  void readLock();
  // this version will release the lock in the parameter after locking this instance
  void readLock(boost::unique_lock<boost::mutex>&);
  void readUnlock();
  void writeLock();
  // this version will release the lock in the parameter after locking this instance
  void writeLock(boost::unique_lock<boost::mutex>&);
  void writeUnlock();

  // returns true if anything is blocked on or owns this lock instance.
  bool inUse();

 private:
  uint readersWaiting;
  uint readersRunning;
  uint writersWaiting;
  uint writersRunning;
  boost::mutex m;
  boost::condition okToWrite;
  boost::condition okToRead;
};
}  // namespace storagemanager

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

#ifndef UTILITIES_H_
#define UTILITIES_H_

#include <string>

namespace storagemanager
{
class IOCoordinator;

// a few utility classes we've coded here and there, now de-duped and centralized.
// modify as necessary.

struct ScopedFileLock
{
  ScopedFileLock(IOCoordinator* i, const std::string& k);
  virtual ~ScopedFileLock();

  virtual void lock() = 0;
  virtual void unlock() = 0;

  IOCoordinator* ioc;
  bool locked;
  const std::string key;
};

struct ScopedReadLock : public ScopedFileLock
{
  ScopedReadLock(IOCoordinator* i, const std::string& k);
  virtual ~ScopedReadLock();
  void lock();
  void unlock();
};

struct ScopedWriteLock : public ScopedFileLock
{
  ScopedWriteLock(IOCoordinator* i, const std::string& k);
  virtual ~ScopedWriteLock();
  void lock();
  void unlock();
};

struct ScopedCloser
{
  ScopedCloser();
  ScopedCloser(int f);
  ~ScopedCloser();
  int fd;
};

class SharedCloser
{
 public:
  SharedCloser(int f);
  SharedCloser(const SharedCloser&);
  ~SharedCloser();

 private:
  struct CtrlBlock
  {
    int fd;
    uint refCount;
  };

  CtrlBlock* block;
};

}  // namespace storagemanager

#endif

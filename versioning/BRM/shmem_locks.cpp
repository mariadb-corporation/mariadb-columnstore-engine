/* Copyright (C) 2016-2024 MariaDB Corporation

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

#include <iostream>
#include <sstream>
#include <string>
#include <stdlib.h>
#include <rwlock.h>

#include "CLI11.hpp"

using namespace std;
using namespace rwlock;

static const char* BIN_NAME = "mcs-load-brm-from-file";

std::string getShmemLocksList()
{
  std::ostringstream oss;
  size_t lockId = 0;
  oss << std::endl;
  for (auto& lockName : RWLockNames)
  {
    oss << "         " << lockId++ << "=" << lockName << std::endl;
  }
  return oss.str();
}

int viewLock(uint8_t lockId)
{
  size_t minLockId = (lockId > 0) ? lockId : 1;
  size_t maxLockId = (lockId > 0) ? lockId : RWLockNames.size() - 1;

  for (size_t i = minLockId; i <= maxLockId; ++i)
  {
    auto rwlock = RWLock(0x10000 * i);
    auto state = rwlock.getLockState();

    cout << RWLockNames[i] << " RWLock" << std::endl
         << "   readers = " << state.reading << std::endl
         << "   writers = " << state.writing << std::endl
         << "   readers waiting = " << state.readerswaiting << std::endl
         << "   writers waiting = " << state.writerswaiting << std::endl
         << "   mutex locked = " << (int)state.mutexLocked << std::endl;
  }
  return 0;
}

int lockOp(size_t minLockId, size_t maxLockId, bool lock, bool read)
{
  for (size_t i = minLockId; i <= maxLockId; ++i)
  {
    auto rwlock = RWLock(0x10000 * i);

    if (read)
    {
      if (lock)
        rwlock.read_lock();
      else
        rwlock.read_unlock();
    }
    else if (lock)
    {
      rwlock.write_lock();
    }
    else
    {
      rwlock.write_unlock();
    }
  }
  return 0;
}

int main(int argc, char** argv)
{
  CLI::App app{BIN_NAME};
  app.description(
      "A tool to operate or view shmem locks. If neither read nor write operation is specified, the tool will "
      "display the lock state.");
  uint8_t lockId;
  bool debug = false;
  bool read = false;
  bool write = false;
  bool lock = false;
  bool unlock = false;

  app.add_option<uint8_t>("-i, --lock-id", lockId, "Shmem lock numerical id: " + getShmemLocksList())
      ->expected(0, RWLockNames.size())
      ->required();
  app.add_flag("-r, --read-lock", read, "Use read lock.")->default_val(false);
  app.add_flag("-w, --write-lock", write, "Use write lock..")->default_val(false)->excludes("-r");
  app.add_flag("-l, --lock", lock, "Lock the corresponding shmem lock.")->default_val(false);
  app.add_flag("-u, --unlock", unlock, "Unlock the corresponding shmem write lock.")
      ->default_val(false)
      ->excludes("-l");
  app.add_flag("-d,--debug", debug, "Print extra output.")->default_val(false);

  CLI11_PARSE(app, argc, argv);

  if (!read && !write)
  {
    return viewLock(lockId);
  }

  if (lock || unlock)
  {
    size_t minLockId = (lockId > 0) ? lockId : 1;
    size_t maxLockId = (lockId > 0) ? lockId : RWLockNames.size() - 1;
    return lockOp(minLockId, maxLockId, lock, read);
  }

  return 0;
}


/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016-2022 MariaDB Corporation

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

/* Takes two params,
 *    first, which lock use
 *    second, which side to use (read or write)
 *    third, lock or unlock it
 */

#include <iostream>
#include <string>
#include <stdlib.h>
#include <rwlock.h>

using namespace std;
using namespace rwlock;

char* name;

void usage()
{
  std::cout << "Usage " << name << "   which_lock_to_use:" << std::endl;
  size_t lockId = 0;
  for (auto& lockName : RWLockNames)
  {
    std::cout << "         " << lockId++ << "=" << lockName << std::endl;
  }
  exit(1);
}

int main(int argc, char** argv)
{
  uint32_t which_lock;  // 0-6
  RWLock* rwlock;
  LockState state;

  name = argv[0];

  if (argc != 2)
    usage();

  if (strlen(argv[1]) != 1)
    usage();

  try
  {
    which_lock = std::stoi(argv[1]);
  }
  catch (std::exception const& e)
  {
    std::cerr << "Cannot convert the lock id: " << e.what() << std::endl;
    usage();
  }

  if (which_lock >= RWLockNames.size())
    usage();

  size_t minLockId = (which_lock > 0) ? which_lock : 1;
  size_t maxLockId = (which_lock > 0) ? which_lock : RWLockNames.size() - 1;

  for (size_t i = minLockId; i <= maxLockId; ++i)
  {
    rwlock = new RWLock(0x10000 * i);
    state = rwlock->getLockState();

    cout << RWLockNames[i] << " RWLock" << std::endl
         << "   readers = " << state.reading << std::endl
         << "   writers = " << state.writing << std::endl
         << "   readers waiting = " << state.readerswaiting << std::endl
         << "   writers waiting = " << state.writerswaiting << std::endl
         << "   mutex locked = " << (int)state.mutexLocked << std::endl;
    delete rwlock;
  }

  return 0;
}

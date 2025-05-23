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

using namespace std;
using namespace rwlock;

#include <boost/program_options.hpp>
namespace po = boost::program_options;

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

void conflicting_options(const boost::program_options::variables_map& vm, const std::string& opt1,
                         const std::string& opt2)
{
  if (vm.count(opt1) && !vm[opt1].defaulted() && vm.count(opt2) && !vm[opt2].defaulted())
  {
    throw std::logic_error(std::string("Conflicting options '") + opt1 + "' and '" + opt2 + "'.");
  }
}

template <class T>
void check_value(const boost::program_options::variables_map& vm, const std::string& opt1, T lower_bound,
                 T upper_bound)
{
  auto value = vm[opt1].as<T>();
  if (value < lower_bound || value >= upper_bound)
  {
    throw std::logic_error(std::string("Option '") + opt1 + "' is out of range.:  " + std::to_string(value));
  }
}

int main(int argc, char** argv)
{
  int lockId;
  bool debug = false;
  bool read = false;
  bool write = false;
  bool lock = false;
  bool unlock = false;

  po::options_description desc(
      "A tool to operate or view shmem locks. If neither read nor write operation is specified, the tool "
      "will "
      "display the lock state.");

  std::string lockid_description = std::string("Shmem lock numerical id: ") + getShmemLocksList();

  // clang-format off
  desc.add_options()("help", "produce help message")
      ("lock-id,i", po::value<int>(&lockId)->required(), lockid_description.c_str())
      ("read-lock,r", po::bool_switch(&read)->default_value(false), "Use read lock.")
      ("write-lock,w", po::bool_switch(&write)->default_value(false), "Use write lock.")
      ("lock,l", po::bool_switch(&lock)->default_value(false), "Lock the corresponding shmem lock.")
      ("unlock,u", po::bool_switch(&unlock)->default_value(false), "Unlock the corresponding shmem write lock.")
      ("debug,d", po::bool_switch(&debug)->default_value(false), "Print extra output.");
  // clang-format on

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);

  if (argc == 1 || vm.count("help"))
  {
    cout << desc << "\n";
    return 1;
  }

  conflicting_options(vm, "lock", "unlock");
  conflicting_options(vm, "read-lock", "write-lock");
  check_value<int>(vm, "lock-id", 0, RWLockNames.size());

  po::notify(vm);

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

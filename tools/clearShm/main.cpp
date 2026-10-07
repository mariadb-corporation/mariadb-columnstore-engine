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

// $Id: main.cpp 2101 2013-01-21 14:12:52Z rdempsey $

#include "mcsconfig.h"

#include <iostream>
#include <sys/types.h>
#include <cstring>
#include <cerrno>
#include <unistd.h>
#include <cstdlib>
#include <cstdio>
#include <sstream>
#include <iomanip>
#include <string>
#include <mutex>
#include <dirent.h>
using namespace std;

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/sync/named_semaphore.hpp>
#include <boost/interprocess/detail/shared_dir_helpers.hpp>
namespace bi = boost::interprocess;

#include <boost/thread/thread.hpp>
using namespace boost;

#include "shmkeys.h"
using namespace BRM;

namespace
{
bool vFlg;
bool nFlg;
std::mutex coutMutex;

void shmDoitName(key_t shm_key, const string& key_name, const string& label)
{
  if (vFlg)
  {
    try
    {
      bi::shared_memory_object memObj(bi::open_only, key_name.c_str(), bi::read_only);
      bi::offset_t memSize = 0;
      memObj.get_size(memSize);
      std::lock_guard<std::mutex> lk(coutMutex);
      cout << label << ": shm|sem_key: " << shm_key << "; key_name: " << key_name << "; size: " << memSize
           << endl;
    }
    catch (...)
    {
    }
  }

  if (!nFlg)
  {
    bi::shared_memory_object::remove(key_name.c_str());
  }
}

void shmDoit(key_t shm_key, const string& label)
{
  shmDoitName(shm_key, ShmKeys::keyToName(shm_key), label);
}

/* Where the objects actually are. boost hands a name straight to shm_open()
   when BOOST_INTERPROCESS_POSIX_SHARED_MEMORY_OBJECTS is on, which on Linux
   means /dev/shm. ipcdetail::get_shared_dir() answers a different question -
   it is the directory the filesystem-backed emulation uses - and would send us
   to /tmp/boost_interprocess, where none of this lives. */
string sharedMemoryDir()
{
#if defined(BOOST_INTERPROCESS_POSIX_SHARED_MEMORY_OBJECTS) && \
    !defined(BOOST_INTERPROCESS_FILESYSTEM_BASED_POSIX_SHARED_MEMORY)
  return "/dev/shm";
#else
  string dir;
  bi::ipcdetail::get_shared_dir(dir);
  return dir;
#endif
}

/// What ShmKeys::keyToName() puts in front of the key, and how many hex digits
/// of key it writes after it.
const size_t KeyNameHexDigits = 8;

string keyNamePrefix()
{
  const string zero = ShmKeys::keyToName(0);
  return zero.substr(0, zero.size() - KeyNameHexDigits);
}

/* The data areas of a versioned segment are named for the id of the version in
   them as well as for their key - <prefix><key in hex>-<id> - so the sweep over
   the numeric keys below cannot reach them. There is no bound on the id and no
   way to enumerate the ones a process that died mid-publication left behind, so
   they have to be found by looking at what is actually there.

   Named areas are unlinked a couple of publications after they are retired, so
   in the normal course of things this finds one or two per structure. It is the
   abnormal course it is here for. */
void shmDoitSuffixedRange(key_t base, const string& label)
{
  if (base == 0)
    return;

  DIR* d = opendir(sharedMemoryDir().c_str());

  if (!d)
    return;

  const string prefix = keyNamePrefix();
  const size_t keyPos = prefix.size();
  const size_t dashPos = keyPos + KeyNameHexDigits;

  for (const struct dirent* ent = readdir(d); ent != nullptr; ent = readdir(d))
  {
    const string name(ent->d_name);

    // <prefix><8 hex>-<at least one digit>
    if (name.size() <= dashPos + 1 || name.compare(0, prefix.size(), prefix) != 0 || name[dashPos] != '-')
      continue;

    const string keyText = name.substr(keyPos, KeyNameHexDigits);

    if (keyText.find_first_not_of("0123456789abcdefABCDEF") != string::npos)
      continue;

    if (name.find_first_not_of("0123456789", dashPos + 1) != string::npos)
      continue;

    const unsigned long key = strtoul(keyText.c_str(), nullptr, 16);

    if (key < static_cast<unsigned long>(base) ||
        key >= static_cast<unsigned long>(base) + ShmKeys::KEYRANGE_SIZE)
      continue;

    shmDoitName(static_cast<key_t>(key), name, label);
  }

  closedir(d);
}

void semDoit(key_t sem_key, const string& label)
{
  shmDoit(sem_key, label);
}

void shmDoitRange(key_t shm_key, const string& label)
{
  if (shm_key == 0)
    return;

  unsigned shm_key_cnt;

  const key_t base = shm_key;

  for (shm_key_cnt = 0; shm_key_cnt < ShmKeys::KEYRANGE_SIZE; shm_key_cnt++, shm_key++)
  {
    shmDoit(shm_key, label);
  }

  // The names the key sweep above cannot express.
  shmDoitSuffixedRange(base, label);
}

void usage()
{
  cout << "usage: clearShm [-cvnh]" << endl;
  cout << "   delete all ColumnStore shared memory data" << endl;
  cout << "   -h display this help" << endl;
  cout << "   -c only clear ColumnStore Engine data, leave OAM intact" << endl;
  cout << "   -v verbose output" << endl;
  cout << "   -n don't actually delete anything (implies -v)" << endl;
}

class ThdFunc
{
 public:
  ThdFunc() : fShm_key(0)
  {
  }
  ThdFunc(key_t shm_key, const string& label) : fShm_key(shm_key), fLabel(label)
  {
  }

  ~ThdFunc()
  {
  }

  void operator()() const
  {
    shmDoitRange(fShm_key, fLabel);
  }

 private:
  // ThdFunc(const ThdFunc& rhs);
  // ThdFunc& operator=(const ThdFunc& rhs);

  key_t fShm_key;
  string fLabel;
};

}  // namespace

int main(int argc, char** argv)
{

  int c;
  opterr = 0;
  bool cFlg = false;
  vFlg = false;
  nFlg = false;

  while ((c = getopt(argc, argv, "cvnh")) != EOF)
    switch (c)
    {
      case 'c': cFlg = true; break;

      case 'v': vFlg = true; break;

      case 'n': nFlg = true; break;

      case 'h':
      default:
        usage();
        return (c == 'h' ? 0 : 1);
        break;
    }

  if (nFlg)
    vFlg = true;

  ShmKeys BrmKeys;

  boost::thread_group tg;
  boost::thread* tp = 0;
  tp = new boost::thread(ThdFunc(BrmKeys.KEYRANGE_CL_BASE, "COPYLOCK   "));
  tg.add_thread(tp);
  tp = new boost::thread(ThdFunc(BrmKeys.KEYRANGE_EXTENTMAP_BASE, "EXTMAP     "));
  tg.add_thread(tp);
  tp = new boost::thread(ThdFunc(BrmKeys.KEYRANGE_EMFREELIST_BASE, "EXTMAP_FREE"));
  tg.add_thread(tp);
  tp = new boost::thread(ThdFunc(BrmKeys.KEYRANGE_VBBM_BASE, "VBBM       "));
  tg.add_thread(tp);
  tp = new boost::thread(ThdFunc(BrmKeys.KEYRANGE_VSS_BASE, "VSS        "));
  tg.add_thread(tp);
  tp = new boost::thread(ThdFunc(BrmKeys.KEYRANGE_EXTENTMAP_INDEX_BASE, "EXTMAP_INDX"));
  tg.add_thread(tp);
  tg.join_all();

  shmDoit(BrmKeys.MST_SYSVKEY, "MST        ");

  if (!cFlg)
  {
    shmDoit(BrmKeys.PROCESSSTATUS_SYSVKEY, "PROC_STAT  ");
    shmDoit(BrmKeys.SYSTEMSTATUS_SYSVKEY, "SYS_STAT   ");
    shmDoit(BrmKeys.SWITCHSTATUS_SYSVKEY, "SW_STAT    ");
    shmDoit(BrmKeys.NICSTATUS_SYSVKEY, "NIC_STAT   ");
    shmDoit(BrmKeys.DBROOTSTATUS_SYSVKEY, "DBROOT_STAT");
  }

  shmDoit(BrmKeys.DECOMSVRMUTEX_SYSVKEY, "DCMSVRMUTEX");

  semDoit(BrmKeys.KEYRANGE_CL_BASE, "COPYLOCK   ");
  semDoit(BrmKeys.KEYRANGE_EXTENTMAP_BASE, "EXTMAP     ");
  semDoit(BrmKeys.KEYRANGE_EMFREELIST_BASE, "EXTMAP_FREE");
  semDoit(BrmKeys.KEYRANGE_VBBM_BASE, "VBBM       ");
  semDoit(BrmKeys.KEYRANGE_VSS_BASE, "VSS        ");
  semDoit(BrmKeys.KEYRANGE_EXTENTMAP_INDEX_BASE, "EXTMAP_INDX");
  semDoit(BrmKeys.MST_SYSVKEY, "MST        ");

  if (!cFlg)
  {
    semDoit(BrmKeys.PROCESSSTATUS_SYSVKEY, "PROC_STAT  ");
    semDoit(BrmKeys.SYSTEMSTATUS_SYSVKEY, "SYS_STAT   ");
    semDoit(BrmKeys.SWITCHSTATUS_SYSVKEY, "SW_STAT    ");
    semDoit(BrmKeys.NICSTATUS_SYSVKEY, "NIC_STAT   ");
    shmDoit(BrmKeys.DBROOTSTATUS_SYSVKEY, "DBROOT_STAT");
  }

  return 0;
}

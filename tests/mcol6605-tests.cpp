/* Copyright (C) 2026 MariaDB Corporation

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

/* MCOL-6605: the extent map hands out new extents from the lowest free range
   that fits, whatever slot of the free list that range sits in.

   A live delete puts the freed range in a free list slot after the tail range,
   while a load rebuilds the list in LBID order, hole first. Allocation used to
   take the first slot that fit, so a node that had loaded a snapshot and a
   node that had done the delete live gave out different LBIDs for the same
   request, and the controller set DBRM read-only on the mismatch.

   The extent map lives in shared memory under fixed keys (BRM_UID is not
   compiled in), so these tests use the real segments: they refuse to run next
   to a live ColumnStore, take the leftovers of an earlier run off the names
   before the first extent map is made, and must not run in parallel with
   anything else that uses an extent map (RUN_SERIAL in tests/CMakeLists.txt).
 */

#include <dirent.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <boost/interprocess/shared_memory_object.hpp>

#include "IDBPolicy.h"
#include "extentmap.h"
#include "shmkeys.h"

using namespace BRM;

namespace
{
constexpr uint32_t ColWidth = 8;
constexpr uint16_t DBRoot = 1;
constexpr int OidA = 3001;
constexpr int OidB = 3002;
constexpr int OidC = 3003;
constexpr int OidD = 3004;
const std::vector<int> TestOids{OidA, OidB, OidC, OidD};

// A ColumnStore process on this host maps the same fixed-key segments.
std::string liveColumnStoreProcess()
{
  static const std::vector<std::string> names{"PrimProc", "controllernode",  "workernode", "DMLProc",
                                              "DDLProc",  "WriteEngineServ", "ExeMgr",     "mariadbd"};
  std::unique_ptr<DIR, int (*)(DIR*)> proc(opendir("/proc"), closedir);

  if (!proc)
    return "";

  while (dirent* entry = readdir(proc.get()))
  {
    std::ifstream comm(std::string("/proc/") + entry->d_name + "/comm");
    std::string name;

    if (comm && std::getline(comm, name))
    {
      for (const auto& n : names)
      {
        if (name == n)
          return name;
      }
    }
  }

  return "";
}

// Every object of the extent map, its free list and its index, whatever slot.
void removeExtentMapSegments()
{
  ShmKeys keys;
  std::vector<std::string> prefixes;

  for (unsigned base :
       {keys.KEYRANGE_EXTENTMAP_BASE, keys.KEYRANGE_EMFREELIST_BASE, keys.KEYRANGE_EXTENTMAP_INDEX_BASE})
    prefixes.push_back(ShmKeys::keyToName(base).substr(0, std::string("MCS-shm-").size() + 4));

  std::unique_ptr<DIR, int (*)(DIR*)> shm(opendir("/dev/shm"), closedir);

  if (!shm)
    return;

  std::vector<std::string> doomed;

  while (dirent* entry = readdir(shm.get()))
  {
    std::string name = entry->d_name;

    for (const auto& prefix : prefixes)
    {
      if (name.compare(0, prefix.size(), prefix) == 0)
        doomed.push_back(name);
    }
  }

  for (const auto& name : doomed)
    boost::interprocess::shared_memory_object::remove(name.c_str());
}

std::string freeListText(ExtentMap& em)
{
  std::ostringstream os;
  os << "free list slots (start, size x 1024):";

  for (const auto& range : em.getFreeListEntries())
  {
    if (range.size != 0)
      os << " (" << range.start << ", " << range.size << ")";
    else
      os << " -";
  }

  return os.str();
}

class ExtentMapFreeListTest : public ::testing::Test
{
 protected:
  // Before the first ExtentMap of the process maps anything.
  static void SetUpTestSuite()
  {
    std::string live = liveColumnStoreProcess();

    if (!live.empty())
    {
      sSkipReason = live + " is running and uses the same extent map segments";
      return;
    }

    // Creating an extent asks OamCache for the DBRoot count, which needs an
    // installed config and module file. A package build has neither.
    for (const char* path : {"/etc/columnstore/Columnstore.xml", "/var/lib/columnstore/local/module"})
    {
      if (access(path, R_OK) != 0)
      {
        sSkipReason = std::string("no ") + path + " (ExtentMap needs an installed config)";
        return;
      }
    }

    removeExtentMapSegments();
    idbdatafile::IDBPolicy::init(false, false, "", 0);
  }

  void SetUp() override
  {
    if (!sSkipReason.empty())
      GTEST_SKIP() << sSkipReason;

    fEM.reset(new ExtentMap());
    deleteTestOids();

    char dir[] = "/tmp/mcol6605-XXXXXX";
    ASSERT_NE(mkdtemp(dir), nullptr);
    fDir = dir;

    // Each test starts from an empty extent map: one free range, all of it.
    auto ranges = usedRanges();
    ASSERT_EQ(ranges.size(), 1u) << freeListText(*fEM);
    ASSERT_EQ(ranges[0].start, 0);
  }

  void TearDown() override
  {
    if (!fEM)
      return;

    deleteTestOids();
    unlink((fDir + "/em").c_str());
    rmdir(fDir.c_str());
  }

  // One column extent for oid; returns its first LBID.
  LBID_t createExtent(int oid)
  {
    uint32_t partition = 0;
    uint16_t segment = 0;
    LBID_t lbid = -1;
    int allocated = 0;
    uint32_t startBlock = 0;

    fEM->createColumnExtent_DBroot(oid, ColWidth, DBRoot, execplan::CalpontSystemCatalog::BIGINT, partition,
                                   segment, lbid, allocated, startBlock);
    fEM->confirmChanges();
    return lbid;
  }

  void deleteOid(int oid)
  {
    fEM->deleteOID(oid);
    fEM->confirmChanges();
  }

  void deleteTestOids()
  {
    for (int oid : TestOids)
    {
      std::vector<EMEntry> entries;
      fEM->getExtents(oid, entries, false, false, true);

      if (!entries.empty())
        deleteOid(oid);
    }
  }

  std::vector<InlineLBIDRange> usedRanges()
  {
    std::vector<InlineLBIDRange> ranges;

    for (const auto& range : fEM->getFreeListEntries())
    {
      if (range.size != 0)
        ranges.push_back(range);
    }

    return ranges;
  }

  static std::string sSkipReason;
  std::unique_ptr<ExtentMap> fEM;
  std::string fDir;
};

std::string ExtentMapFreeListTest::sSkipReason;

}  // namespace

// A live delete leaves the freed range in a slot after the tail range. The next
// extent must still come from the freed range, the lowest one that fits.
TEST_F(ExtentMapFreeListTest, ReusesLowestFreeRange)
{
  createExtent(OidA);
  LBID_t b = createExtent(OidB);
  createExtent(OidC);
  deleteOid(OidB);
  std::string afterDelete = freeListText(*fEM);

  EXPECT_EQ(createExtent(OidD), b) << "after the delete, " << afterDelete;
}

// A node that loads a saved image (hole first in the free list) and a node that
// did the delete live (hole last) must hand out the same LBIDs.
TEST_F(ExtentMapFreeListTest, SameChoiceAfterSaveLoad)
{
  createExtent(OidA);
  LBID_t b = createExtent(OidB);
  createExtent(OidC);
  deleteOid(OidB);
  fEM->save(fDir + "/em");

  std::string live = freeListText(*fEM);
  LBID_t liveLbid = createExtent(OidD);

  fEM->load(fDir + "/em");
  std::string loaded = freeListText(*fEM);
  LBID_t loadedLbid = createExtent(OidD);

  EXPECT_EQ(loadedLbid, liveLbid) << "live: " << live << "\nloaded: " << loaded;
  EXPECT_EQ(liveLbid, b);
}

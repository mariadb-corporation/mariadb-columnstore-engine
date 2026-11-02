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

   The extent map lives in shared memory under keys derived from ShmKeys, so
   these tests give their ExtentMap keys of their own: a key range picked from
   the pid of the test process, away from the one a ColumnStore on this host
   uses. Nothing of the real extent map is touched, and the segments are
   removed again when the suite ends.
 */

#include <dirent.h>
#include <unistd.h>

#include <cstdlib>
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

// Keys of this run's own segments: the usual ranges under a BRM_UID taken from
// the pid, so no segment of a ColumnStore on this host is shared.
ShmKeys testShmKeys()
{
  ShmKeys keys;
  const uint32_t uid = 0x800 | (static_cast<uint32_t>(getpid()) & 0x7ff);

  for (uint32_t* base :
       {&keys.KEYRANGE_EXTENTMAP_BASE, &keys.KEYRANGE_EMFREELIST_BASE, &keys.KEYRANGE_EXTENTMAP_INDEX_BASE})
    *base = (*base & 0xfffff) | (uid << 20);

  return keys;
}

// Every object of the test's extent map, its free list and its index, whatever slot.
void removeExtentMapSegments()
{
  const ShmKeys keys = testShmKeys();
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
    // Creating an extent reads the files-per-partition and extents-per-file
    // settings, which need an installed config. A package build has none. The
    // DBRoots don't come from OamCache (see SetUp), so no module file is needed.
    for (const char* path : {"/etc/columnstore/Columnstore.xml"})
    {
      if (access(path, R_OK) != 0)
      {
        sSkipReason = std::string("no ") + path + " (ExtentMap needs an installed config)";
        return;
      }
    }

    removeExtentMapSegments();
    idbdatafile::IDBPolicy::init(false, false, "", 0);
    sSegmentsMade = true;
  }

  static void TearDownTestSuite()
  {
    if (sSegmentsMade)
      removeExtentMapSegments();
  }

  void SetUp() override
  {
    if (!sSkipReason.empty())
      GTEST_SKIP() << sSkipReason;

    fEM.reset(new ExtentMap(testShmKeys()));
    fEM->setDbRoots({DBRoot});
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
    if (!fDir.empty())
    {
      unlink((fDir + "/em").c_str());
      rmdir(fDir.c_str());
    }
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
  static bool sSegmentsMade;
  std::unique_ptr<ExtentMap> fEM;
  std::string fDir;
};

std::string ExtentMapFreeListTest::sSkipReason;
bool ExtentMapFreeListTest::sSegmentsMade = false;

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

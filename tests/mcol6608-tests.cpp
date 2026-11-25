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

/* MCOL-6608: load_brm must fail when the journal replay fails, instead of
   starting the node on a shorter history than the one that was journaled.

   A journal is a run of records, each a 4-byte length and that many bytes of
   command. A cut journal (a partial copy, a torn write) used to replay as the
   records before the cut plus whatever the short read left in the buffer, and
   a record whose command failed counted as replayed.

   printJournal() parses every record without touching the BRM, so the
   truncation tests need no shared memory. The failed-command test replays for
   real, so it uses the fixed-key BRM segments: it refuses to run next to a
   live ColumnStore and must not run in parallel with anything else that uses
   them (RUN_SERIAL in tests/CMakeLists.txt).
 */

#include <dirent.h>
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <boost/interprocess/shared_memory_object.hpp>

#include "IDBPolicy.h"
#include "brmtypes.h"
#include "bytestream.h"
#include "shmkeys.h"
#include "slavecomm.h"
#include "vbbm.h"
#include "vss.h"

using namespace BRM;

namespace
{
constexpr uint32_t MissingOid = 3999;

// One journal record as SlaveComm::saveDelta() writes it: the length, then the command.
std::string journalRecord(const messageqcpp::ByteStream& cmd)
{
  uint32_t len = cmd.length();
  std::string record(reinterpret_cast<const char*>(&len), sizeof(len));
  record.append(reinterpret_cast<const char*>(cmd.buf()), cmd.length());
  return record;
}

// deleteOID, harmless to parse: printJournal() only prints it.
std::string deleteOidRecord(uint32_t oid)
{
  messageqcpp::ByteStream cmd;
  cmd << DELETE_OID << oid;
  return journalRecord(cmd);
}

class JournalDir
{
 public:
  JournalDir()
  {
    char dir[] = "/tmp/mcol6608-XXXXXX";

    if (mkdtemp(dir))
      fDir = dir;
  }

  ~JournalDir()
  {
    unlink(journal().c_str());
    rmdir(fDir.c_str());
  }

  bool ok() const
  {
    return !fDir.empty();
  }

  // What load_brm passes: the save file name, from which it derives the journal's.
  std::string prefix() const
  {
    return fDir + "/BRM_saves";
  }

  std::string journal() const
  {
    return prefix() + "_journal";
  }

  void write(const std::string& bytes) const
  {
    std::ofstream out(journal(), std::ios::binary | std::ios::trunc);
    out.write(bytes.data(), bytes.size());
  }

 private:
  std::string fDir;
};

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

// Every object of the extent map (with its free list and index), the VBBM and the VSS.
void removeBrmSegments()
{
  ShmKeys keys;
  std::vector<std::string> prefixes;

  for (unsigned base : {keys.KEYRANGE_EXTENTMAP_BASE, keys.KEYRANGE_EMFREELIST_BASE,
                        keys.KEYRANGE_EXTENTMAP_INDEX_BASE, keys.KEYRANGE_VBBM_BASE, keys.KEYRANGE_VSS_BASE})
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

// SlaveComm's constructor reads the installed config. A package build has none.
std::string noConfigReason()
{
  const char* path = "/etc/columnstore/Columnstore.xml";

  if (access(path, R_OK) != 0)
    return std::string("no ") + path + " (SlaveComm needs an installed config)";

  return "";
}

}  // namespace

class JournalParseTest : public ::testing::Test
{
 protected:
  static void SetUpTestSuite()
  {
    sSkipReason = noConfigReason();
    idbdatafile::IDBPolicy::init(false, false, "", 0);
  }

  void SetUp() override
  {
    if (!sSkipReason.empty())
      GTEST_SKIP() << sSkipReason;

    ASSERT_TRUE(fDir.ok());
  }

  static std::string sSkipReason;
  JournalDir fDir;
};

std::string JournalParseTest::sSkipReason;

// The contract load_brm builds on: a whole journal gives its record count.
TEST_F(JournalParseTest, WholeJournalCountsRecords)
{
  fDir.write(deleteOidRecord(3001) + deleteOidRecord(3002));
  SlaveComm comm;

  EXPECT_EQ(comm.printJournal(fDir.prefix()), 2);
}

// An empty journal is not an error: nothing has happened since the last save.
TEST_F(JournalParseTest, EmptyJournalIsZero)
{
  fDir.write("");
  SlaveComm comm;

  EXPECT_EQ(comm.printJournal(fDir.prefix()), 0);
}

TEST_F(JournalParseTest, MissingJournalFails)
{
  SlaveComm comm;

  EXPECT_EQ(comm.printJournal(fDir.prefix()), -1);
}

// The second record is cut in its body: its length is there, its command isn't
// all there. Replaying the first one alone would be a shorter history.
TEST_F(JournalParseTest, TruncatedJournalFails)
{
  std::string whole = deleteOidRecord(3001) + deleteOidRecord(3002);
  fDir.write(whole.substr(0, whole.size() - 2));
  SlaveComm comm;

  EXPECT_EQ(comm.printJournal(fDir.prefix()), -1);
}

// A journal cut inside the first length field.
TEST_F(JournalParseTest, ShortLengthFieldFails)
{
  fDir.write(deleteOidRecord(3001).substr(0, 2));
  SlaveComm comm;

  EXPECT_EQ(comm.printJournal(fDir.prefix()), -1);
}

class JournalReplayTest : public ::testing::Test
{
 protected:
  // Before anything in the process maps a BRM segment.
  static void SetUpTestSuite()
  {
    std::string live = liveColumnStoreProcess();

    if (!live.empty())
    {
      sSkipReason = live + " is running and uses the same BRM segments";
      return;
    }

    sSkipReason = noConfigReason();

    if (!sSkipReason.empty())
      return;

    removeBrmSegments();
    idbdatafile::IDBPolicy::init(false, false, "", 0);
  }

  void SetUp() override
  {
    if (!sSkipReason.empty())
      GTEST_SKIP() << sSkipReason;

    ASSERT_TRUE(fDir.ok());

    // load_brm loads the saved image before it replays, so the VBBM and the VSS
    // have a published data area by then. Publish empty ones the same way: a
    // rollback of the first update a table ever had crashes (setPointers() on
    // the null data area discardUpdate() goes back to), which load_brm never hits.
    VBBM vbbm;
    vbbm.lock(VBBM::WRITE);
    vbbm.release(VBBM::WRITE);
    VSS vss;
    vss.lock(VSS::WRITE);
    vss.release(VSS::WRITE);
  }

  static std::string sSkipReason;
  JournalDir fDir;
};

std::string JournalReplayTest::sSkipReason;

// The record parses, but the command fails: there is no such OID, so deleteOID
// answers with an error. A replay that leaves out a journaled change must not
// report success.
TEST_F(JournalReplayTest, FailedRecordFailsReplay)
{
  fDir.write(deleteOidRecord(MissingOid));
  SlaveComm comm;

  EXPECT_EQ(comm.replayJournal(fDir.prefix()), -1);
}

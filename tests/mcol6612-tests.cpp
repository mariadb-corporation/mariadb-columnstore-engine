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

/* MCOL-6612: the joins and aggregates spill directories sit in a per-host
   subtree of SystemTempFileDir, because ExeMgr wipes them on every start and
   the directory may be on storage that several nodes share.

   Config::makeConfig(path) reads a config file of our own, so these tests
   need no installed Columnstore.xml. The gethostname() failure path is not
   tested: the host name can't be injected without a production change.
 */

#include <unistd.h>

#include <climits>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

#include "configcpp.h"

using config::Config;

namespace
{
std::string hostName()
{
  char host[HOST_NAME_MAX + 1] = {};
  if (gethostname(host, sizeof(host) - 1) != 0)
    return {};
  return host;
}

// Writes a config file with the given SystemConfig body and returns its Config.
// Each test uses its own path, because makeConfig() keeps one instance per path.
Config* configWith(const std::string& name, const std::string& systemConfig)
{
  std::string path = testing::TempDir() + "mcol6612-" + name + "-" + std::to_string(getpid()) + ".xml";
  std::ofstream out(path);
  out << "<Columnstore Version=\"V1.0.0\">\n<SystemConfig>\n"
      << systemConfig << "\n</SystemConfig>\n</Columnstore>\n";
  out.close();
  return Config::makeConfig(path);
}
}  // namespace

TEST(TempFileDirTest, ConfiguredDirGetsHostSubtree)
{
  std::string host = hostName();
  ASSERT_FALSE(host.empty());
  Config* cf = configWith("configured", "<SystemTempFileDir>/data/spill</SystemTempFileDir>");

  EXPECT_EQ(cf->getTempFileDir(Config::TempDirPurpose::Joins), "/data/spill/" + host + "/joins/");
  EXPECT_EQ(cf->getTempFileDir(Config::TempDirPurpose::Aggregates), "/data/spill/" + host + "/aggregates/");
}

TEST(TempFileDirTest, EmptySettingUsesDefaultWithHostSubtree)
{
  std::string host = hostName();
  ASSERT_FALSE(host.empty());
  Config* cf = configWith("empty", "<SystemTempFileDir></SystemTempFileDir>");

  EXPECT_EQ(cf->getTempFileDir(Config::TempDirPurpose::Joins),
            "/tmp/columnstore_tmp_files/" + host + "/joins/");
  EXPECT_EQ(cf->getTempFileDir(Config::TempDirPurpose::Aggregates),
            "/tmp/columnstore_tmp_files/" + host + "/aggregates/");
}

TEST(TempFileDirTest, MissingSettingUsesDefaultWithHostSubtree)
{
  std::string host = hostName();
  ASSERT_FALSE(host.empty());
  Config* cf = configWith("missing", "");

  EXPECT_EQ(cf->getTempFileDir(Config::TempDirPurpose::Joins),
            "/tmp/columnstore_tmp_files/" + host + "/joins/");
}

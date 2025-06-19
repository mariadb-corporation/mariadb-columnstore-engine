/* Copyright (C) 2024 MariaDB Corporation.

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

#include <cstdio>
#include <fstream>
#include <sys/stat.h>
#include <gtest/gtest.h>
#include "BufferedFile.h"

std::string message(const std::string& fname, int err)
{
  return std::string("unable to open file: ") + fname + ", exception: " + strerror(err);
}

TEST(BufferedFileTest, NoError)
{
  const char fname[] = "/tmp/foo.bar";
  std::ofstream out(fname);

  EXPECT_NO_THROW(idbdatafile::BufferedFile bf(fname, "r", 0));
  remove(fname);
}

TEST(BufferedFileTest, NoEnt)
{
  const char fname[] = "/home/BufferedFile_test";

  EXPECT_THROW(
      {
        try
        {
          idbdatafile::BufferedFile bf(fname, "r", 0);
        }
        catch (const std::runtime_error& e)
        {
          EXPECT_EQ(message(fname, ENOENT), e.what());
          throw;
        }
      },
      std::runtime_error);
}

TEST(BufferedFileTest, NotDir)
{
  std::string fake_dir_name = "/tmp/foo";
  std::ofstream out(fake_dir_name);
  std::string fname = fake_dir_name + "/bar";

  EXPECT_THROW(
      {
        try
        {
          idbdatafile::BufferedFile bf(fname.c_str(), "r", 0);
        }
        catch (const std::runtime_error& e)
        {
          EXPECT_EQ(message(fname, ENOTDIR), e.what());
          throw;
        }
      },
      std::runtime_error);
  remove(fake_dir_name.c_str());
}

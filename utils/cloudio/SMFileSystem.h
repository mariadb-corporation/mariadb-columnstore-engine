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

#pragma once

#include <list>
#include <string>
#include <boost/utility.hpp>
#include "IDBFileSystem.h"

namespace idbdatafile
{
class SMFileSystem : public IDBFileSystem, boost::noncopyable
{
 public:
  SMFileSystem();
  ~SMFileSystem() override;

  // why are some of these const and some not const in IDBFileSystem?
  int mkdir(const char* pathname) override;
  off64_t size(const char* path) const override;
  off64_t compressedSize(const char* path) const override;
  int remove(const char* pathname) override;
  int rename(const char* oldpath, const char* newpath) override;
  bool exists(const char* pathname) const override;
  int listDirectory(const char* pathname, std::list<std::string>& contents) const override;
  bool isDir(const char* pathname) const override;
  int copyFile(const char* srcPath, const char* destPath) const override;
  bool filesystemIsUp() const override;
  bool filesystemSync() const override;
};

}  // namespace idbdatafile

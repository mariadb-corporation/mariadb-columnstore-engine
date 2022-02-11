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

#ifndef SMFILESYSTEM_H_
#define SMFILESYSTEM_H_

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
  virtual ~SMFileSystem();

  // why are some of these const and some not const in IDBFileSystem?
  int mkdir(const char* pathname);
  off64_t size(const char* path) const;
  off64_t compressedSize(const char* path) const;
  int remove(const char* pathname);
  int rename(const char* oldpath, const char* newpath);
  bool exists(const char* pathname) const;
  int listDirectory(const char* pathname, std::list<std::string>& contents) const;
  bool isDir(const char* pathname) const;
  int copyFile(const char* srcPath, const char* destPath) const;
  bool filesystemIsUp() const;
  bool filesystemSync() const;
};

}  // namespace idbdatafile

#endif

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

#ifndef POSIXFILESYSTEM_H_
#define POSIXFILESYSTEM_H_

#include "IDBFileSystem.h"

namespace idbdatafile
{
class PosixFileSystem : public IDBFileSystem
{
 public:
  PosixFileSystem();
  ~PosixFileSystem();

  int mkdir(const char* pathname) override;
  off64_t size(const char* path) const override;
  off64_t compressedSize(const char* path) const override;
  int remove(const char* pathname) override;
  int rename(const char* oldpath, const char* newpath) override;
  bool exists(const char* pathname) const override;
  int listDirectory(const char* pathname, std::list<std::string>& contents) const override;
  bool isDir(const char* pathname) const override;
  int copyFile(const char* srcPath, const char* destPath) const override;
  int chown(const char* objectName, const uid_t p_uid, const gid_t p_pid, int& funcErrno) const override;
};

}  // namespace idbdatafile

#endif /* POSIXFILESYSTEM_H_ */

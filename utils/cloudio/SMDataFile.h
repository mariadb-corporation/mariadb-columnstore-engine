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

#include <string>
#include <boost/utility.hpp>
#include "IDBDataFile.h"
#include "SMComm.h"

namespace idbdatafile
{
class SMDataFile : public IDBDataFile
{
 public:
  ~SMDataFile() override;

  ssize_t pread(void* ptr, off64_t offset, size_t count) override;
  ssize_t read(void* ptr, size_t count) override;
  ssize_t write(const void* ptr, size_t count) override;
  int seek(off64_t offset, int whence) override;
  int truncate(off64_t length) override;
  int fallocate(int mode, off64_t offset, off64_t length) override;
  off64_t size() override;
  off64_t tell() override;
  int flush() override;
  time_t mtime() override;
  int close() override;

  // for testing only
  SMDataFile(const char* fname, int openmode, size_t fake_size);

 private:
  SMDataFile();
  SMDataFile(const char* fname, int openmode, const struct stat&);
  off64_t position;
  int openmode;
  SMComm* comm;

  friend class SMFileFactory;
};

}  // namespace idbdatafile

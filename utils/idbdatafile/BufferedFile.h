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

#pragma once

#include <stdexcept>
#include <boost/utility.hpp>
#include "IDBDataFile.h"
#include <unistd.h>

namespace idbdatafile
{
/**
 * BufferedFile implements the IDBDataFile for I/O to a C library FILE*
 * (via fopen, fwrite, fread, etc.).  See IDBDataFile.h for more documentation
 * on member functions.
 */
class BufferedFile : public IDBDataFile, boost::noncopyable
{
 public:
  BufferedFile(const char* fname, const char* mode, unsigned opts);
  /* virtual */ ~BufferedFile() override;

  /* virtual */ ssize_t pread(void* ptr, off64_t offset, size_t count) override;
  /* virtual */ ssize_t read(void* ptr, size_t count) override;
  /* virtual */ ssize_t write(const void* ptr, size_t count) override;
  /* virtual */ int seek(off64_t offset, int whence) override;
  /* virtual */ int truncate(off64_t length) override;
  /* virtual */ off64_t size() override;
  /* virtual */ off64_t tell() override;
  /* virtual */ int flush() override;
  /* virtual */ time_t mtime() override;
  /* virtual */ int fallocate(int mode, off64_t offset, off64_t length) override;

 protected:
  /* virtual */
  int close() override;

 private:
  void applyOptions(unsigned opts);

  FILE* m_fp;
  char* m_buffer;
};

}  // namespace idbdatafile

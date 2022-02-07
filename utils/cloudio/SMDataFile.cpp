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

#include <fcntl.h>

#include "SMDataFile.h"

using namespace std;

namespace idbdatafile
{
SMDataFile::~SMDataFile()
{
}

SMDataFile::SMDataFile(const char* name, int _openmode, const struct stat& _stat) : IDBDataFile(name)
{
  openmode = _openmode;
  // the 'a' file open mode is the only one that starts at EOF
  if ((openmode & O_APPEND) && !(openmode & O_RDWR))
    position = _stat.st_size;
  else
    position = 0;
  comm = SMComm::get();
}

ssize_t SMDataFile::pread(void* buf, off64_t offset, size_t count)
{
  return comm->pread(name(), buf, count, offset);
}

ssize_t SMDataFile::read(void* buf, size_t count)
{
  ssize_t ret = comm->pread(name(), buf, count, position);
  if (ret < 0)
    return ret;
  position += ret;
  return ret;
}

ssize_t SMDataFile::write(const void* buf, size_t count)
{
  if (openmode & O_APPEND)
    return comm->append(name(), buf, count);
  ssize_t ret = comm->pwrite(name(), buf, count, position);
  if (ret < 0)
    return ret;
  position += ret;
  return ret;
}

int SMDataFile::seek(off64_t offset, int whence)
{
  switch (whence)
  {
    case SEEK_SET: position = offset; break;
    case SEEK_CUR: position += offset; break;
    case SEEK_END:
    {
      struct stat _stat;
      int err = comm->stat(name(), &_stat);
      if (err)
        return err;
      position = _stat.st_size + offset;
      break;
    }
    default: errno = EINVAL; return -1;
  }
  return 0;
}

int SMDataFile::truncate(off64_t length)
{
  return comm->truncate(name(), length);
}

int SMDataFile::fallocate(int mode, off64_t offset, off64_t length)
{
  idbassert_s(mode == 0, "SMDataFile::fallocate() does not support mode != 0 right now.");
  return comm->truncate(name(), offset + length);
}

off64_t SMDataFile::size()
{
  struct stat _stat;
  int err = comm->stat(name(), &_stat);

  if (err)
    return err;
  return _stat.st_size;
}

off64_t SMDataFile::tell()
{
  return position;
}

int SMDataFile::flush()
{
  return 0;  // writes are synchronous b/c of replication.  If we allow asynchronous replication,
             // then we need to implement a flush() cmd in SMComm.
}

time_t SMDataFile::mtime()
{
  struct stat _stat;
  int err = comm->stat(name(), &_stat);

  if (err)
    return (time_t)err;
  return _stat.st_mtime;
}

int SMDataFile::close()
{
  return 0;
}

// constructor used for testing
SMDataFile::SMDataFile(const char* fname, int _openmode, size_t fake_size) : IDBDataFile(fname)
{
  openmode = _openmode;
  // the 'a' file open mode is the only one that starts at EOF
  if ((openmode & O_APPEND) && !(openmode & O_RDWR))
    position = fake_size;
  else
    position = 0;
  comm = SMComm::get();
}

}  // namespace idbdatafile

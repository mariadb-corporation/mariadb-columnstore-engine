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

#include "UnbufferedFile.h"
#include "IDBLogger.h"
#include "utility.h"

#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sstream>

using namespace std;

namespace idbdatafile
{
UnbufferedFile::UnbufferedFile(const char* fname, const char* mode, unsigned opts) : IDBDataFile(fname)
{
  int flags = modeStrToFlags(mode);

  if (flags == -1)
  {
    ostringstream oss;
    oss << "Error opening file - unsupported mode " << mode;
    throw std::runtime_error(oss.str());
  }

  // special flags we set by convention due to InfiniDB usage
  flags |= O_LARGEFILE | O_NOATIME;

  if (opts & IDBDataFile::USE_ODIRECT)
  {
    flags |= O_DIRECT;
  }

  m_fd = ::open(fname, flags, S_IRWXU);

  if (m_fd == -1)
  {
    m_fd = INVALID_HANDLE_VALUE;
    throw std::runtime_error("unable to open Unbuffered file ");
  }

}

UnbufferedFile::~UnbufferedFile()
{
  close();
}

ssize_t UnbufferedFile::pread(void* ptr, off64_t offset, size_t count)
{
  ssize_t ret;
  int savedErrno;

  if (m_fd == INVALID_HANDLE_VALUE)
    return -1;

  ret = ::pread(m_fd, ptr, count, offset);
  savedErrno = errno;

  if (IDBLogger::isEnabled())
    IDBLogger::logRW("pread", m_fname, this, offset, count, ret);

  errno = savedErrno;
  return ret;
}

ssize_t UnbufferedFile::read(void* ptr, size_t count)
{
  ssize_t ret = 0;
  ssize_t offset = tell();
  int savedErrno;

  ret = ::read(m_fd, ptr, count);
  savedErrno = errno;

  if (IDBLogger::isEnabled())
    IDBLogger::logRW("read", m_fname, this, offset, count, ret);

  errno = savedErrno;
  return ret;
}

ssize_t UnbufferedFile::write(const void* ptr, size_t count)
{
  ssize_t ret = 0;
  ssize_t offset = tell();
  int savedErrno;

  ret = ::write(m_fd, ptr, count);
  savedErrno = errno;

  if (IDBLogger::isEnabled())
    IDBLogger::logRW("write", m_fname, this, offset, count, ret);

  errno = savedErrno;
  return ret;
}

int UnbufferedFile::seek(off64_t offset, int whence)
{
  int ret;
  int savedErrno;

  ret = (lseek(m_fd, offset, whence) >= 0) ? 0 : -1;
  savedErrno = errno;

  if (IDBLogger::isEnabled())
    IDBLogger::logSeek(m_fname, this, offset, whence, ret);

  errno = savedErrno;
  return ret;
}

int UnbufferedFile::truncate(off64_t length)
{
  int ret;
  int savedErrno;

  ret = ftruncate(m_fd, length);
  savedErrno = errno;

  if (IDBLogger::isEnabled())
    IDBLogger::logTruncate(m_fname, this, length, ret);

  errno = savedErrno;
  return ret;
}

off64_t UnbufferedFile::size()
{
  off64_t ret = 0;
  int savedErrno;

  struct stat statBuf;
  int rc = ::fstat(m_fd, &statBuf);
  ret = ((rc == 0) ? statBuf.st_size : -1);
  savedErrno = errno;

  if (IDBLogger::isEnabled())
    IDBLogger::logSize(m_fname, this, ret);

  errno = savedErrno;
  return ret;
}

off64_t UnbufferedFile::tell()
{
  off64_t ret;
  ret = lseek(m_fd, 0, SEEK_CUR);
  return ret;
}

int UnbufferedFile::flush()
{
  int ret;
  int savedErrno;

  ret = fsync(m_fd);
  savedErrno = errno;

  if (IDBLogger::isEnabled())
    IDBLogger::logNoArg(m_fname, this, "flush", ret);

  errno = savedErrno;
  return ret;
}

time_t UnbufferedFile::mtime()
{
  time_t ret = 0;
  struct stat statbuf;

  if (::fstat(m_fd, &statbuf) == 0)
    ret = statbuf.st_mtime;
  else
    ret = (time_t)-1;

  return ret;
}

int UnbufferedFile::close()
{
  int ret = -1;
  int savedErrno = EINVAL;  // corresponds to INVALID_HANDLE_VALUE

  if (m_fd != INVALID_HANDLE_VALUE)
  {
    ret = ::close(m_fd);
    savedErrno = errno;
  }

  if (IDBLogger::isEnabled())
    IDBLogger::logNoArg(m_fname, this, "close", ret);

  errno = savedErrno;
  return ret;
}

/**
     @brief
    The wrapper for fallocate function.
     @see
    This one is used in shared/we_fileop.cpp to skip expensive file preallocation.
*/
int UnbufferedFile::fallocate(int mode, off64_t offset, off64_t length)
{
  int ret = 0;
  int savedErrno = 0;

  ret = ::fallocate(m_fd, mode, offset, length);
  savedErrno = errno;

  if (IDBLogger::isEnabled())
  {
    IDBLogger::logNoArg(m_fname, this, "fallocate", errno);
  }

  errno = savedErrno;
  return ret;
}

}  // namespace idbdatafile

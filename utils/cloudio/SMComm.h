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

#ifndef SMCOMM_H_
#define SMCOMM_H_

#include <sys/stat.h>
#include <string>
#include "SocketPool.h"
#include "bytestream.h"
#include "bytestreampool.h"

namespace idbdatafile
{
class SMComm : public boost::noncopyable
{
 public:
  // This is a singleton.  Get it with get()
  static SMComm* get();

  /* Open currently returns a stat struct so SMDataFile can set its initial position, otherwise
     behaves how you'd think. */
  int open(const std::string& filename, const int mode, struct stat* statbuf);

  ssize_t pread(const std::string& filename, void* buf, const size_t count, const off_t offset);

  ssize_t pwrite(const std::string& filename, const void* buf, const size_t count, const off_t offset);

  /* append exists for cases where the file is open in append mode.  A normal write won't work
  because the file position/size may be out of date if there are multiple writers. */
  ssize_t append(const std::string& filename, const void* buf, const size_t count);

  int unlink(const std::string& filename);

  int stat(const std::string& filename, struct stat* statbuf);

  // added this one because it should be trivial to implement in SM, and prevents a large
  // operation in SMDataFile.
  int truncate(const std::string& filename, const off64_t length);

  int listDirectory(const std::string& path, std::list<std::string>* entries);

  // health indicator.  0 = processes are talking to each other and SM has read/write access to
  // the specified S3 bucket.  Need to define specific error codes.
  int ping();

  int sync();

  int copyFile(const std::string& file1, const std::string& file2);

  virtual ~SMComm();

 private:
  SMComm();

  std::string getAbsFilename(const std::string& filename);

  SocketPool sockets;
  messageqcpp::ByteStreamPool buffers;
  std::string cwd;
};

}  // namespace idbdatafile

#endif

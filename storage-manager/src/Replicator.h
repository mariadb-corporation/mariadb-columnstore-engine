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

#ifndef REPLICATOR_H_
#define REPLICATOR_H_

//#include "ThreadPool.h"
#include "MetadataFile.h"
#include <boost/filesystem.hpp>
#include <sys/types.h>
#include <stdint.h>

#define JOURNAL_ENTRY_HEADER_SIZE 16

namespace storagemanager
{
//	64-bit offset
//	64-bit length
//	<length-bytes of data to write at specified offset>

class Replicator
{
 public:
  static Replicator* get();
  virtual ~Replicator();

  enum Flags
  {
    NONE = 0,
    LOCAL_ONLY = 0x1,
    NO_LOCAL = 0x2
  };

  int addJournalEntry(const boost::filesystem::path& filename, const uint8_t* data, off_t offset,
                      size_t length);
  int newObject(const boost::filesystem::path& filename, const uint8_t* data, off_t offset, size_t length);
  int newNullObject(const boost::filesystem::path& filename, size_t length);

  int remove(const boost::filesystem::path& file, Flags flags = NONE);

  int updateMetadata(MetadataFile& meta);

  void printKPIs() const;

 private:
  Replicator();

  // a couple helpers
  ssize_t _write(int fd, const void* data, size_t len);
  ssize_t _pwrite(int fd, const void* data, size_t len, off_t offset);

  Config* mpConfig;
  SMLogging* mpLogger;
  std::string msJournalPath;
  std::string msCachePath;
  // ThreadPool threadPool;

  size_t repUserDataWritten, repHeaderDataWritten;
  size_t replicatorObjectsCreated, replicatorJournalsCreated;
};

}  // namespace storagemanager

#endif

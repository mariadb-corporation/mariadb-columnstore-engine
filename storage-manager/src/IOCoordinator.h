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

#ifndef IOCOORDINATOR_H_
#define IOCOORDINATOR_H_

#include <sys/types.h>
#include <stdint.h>
#include <sys/stat.h>
#include <vector>
#include <string>
#include <boost/utility.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/shared_array.hpp>
#include <boost/filesystem.hpp>

#include "Config.h"
#include "Cache.h"
#include "SMLogging.h"
#include "RWLock.h"
#include "Replicator.h"
#include "Utilities.h"
#include "Ownership.h"

namespace storagemanager
{
boost::shared_array<char> seekToEndOfHeader1(int fd, size_t* bytesRead);

class IOCoordinator : public boost::noncopyable
{
 public:
  static IOCoordinator* get();
  virtual ~IOCoordinator();

  ssize_t read(const char* filename, uint8_t* data, off_t offset, size_t length);
  ssize_t write(const char* filename, const uint8_t* data, off_t offset, size_t length);
  ssize_t append(const char* filename, const uint8_t* data, size_t length);
  int open(const char* filename, int openmode, struct stat* out);
  int listDirectory(const char* filename, std::vector<std::string>* listing);
  int stat(const char* path, struct stat* out);
  int truncate(const char* path, size_t newsize);
  int unlink(const char* path);
  int copyFile(const char* filename1, const char* filename2);

  // The shared logic for merging a journal file with its base file.
  // len should be set to the length of the data requested
  boost::shared_array<uint8_t> mergeJournal(const char* objectPath, const char* journalPath, off_t offset,
                                            size_t len, size_t* sizeRead) const;

  // this version modifies object data in memory, given the journal filename.  Processes the whole object
  // and whole journal file.
  int mergeJournalInMem(boost::shared_array<uint8_t>& objData, size_t len, const char* journalPath,
                        size_t* sizeRead) const;

  // this version of MJIM has a higher IOPS requirement and lower mem usage.
  int mergeJournalInMem_bigJ(boost::shared_array<uint8_t>& objData, size_t len, const char* journalPath,
                             size_t* sizeRead) const;

  // this version takes already-open file descriptors, and an already-allocated buffer as input.
  // file descriptor are positioned, eh, best not to assume anything about their positions
  // on return.
  // Not implemented yet.  At the point of this writing, we will use the existing versions, even though
  // it's wasteful, and will leave this as a likely future optimization.
  int mergeJournal(int objFD, int journalFD, uint8_t* buf, off_t offset, size_t* len) const;

  /* Lock manipulation fcns.  They can lock on any param given to them.  For convention's sake,
     the parameter should mostly be the abs filename being accessed. */
  void readLock(const std::string& filename);
  void writeLock(const std::string& filename);
  void readUnlock(const std::string& filename);
  void writeUnlock(const std::string& filename);

  // some accessors...
  const boost::filesystem::path& getCachePath() const;
  const boost::filesystem::path& getJournalPath() const;
  const boost::filesystem::path& getMetadataPath() const;

  void printKPIs() const;

 private:
  IOCoordinator();
  Config* config;
  Cache* cache;
  SMLogging* logger;
  Replicator* replicator;
  Ownership ownership;  // ACK!  Need a new name for this!

  size_t objectSize;
  boost::filesystem::path journalPath;
  boost::filesystem::path cachePath;
  boost::filesystem::path metaPath;

  std::map<std::string, RWLock*> locks;
  boost::mutex lockMutex;  // lol

  void remove(const boost::filesystem::path& path);
  void deleteMetaFile(const boost::filesystem::path& file);

  int _truncate(const boost::filesystem::path& path, size_t newsize, ScopedFileLock* lock);
  ssize_t _write(const boost::filesystem::path& filename, const uint8_t* data, off_t offset, size_t length,
                 const boost::filesystem::path& firstDir);

  int loadObjectAndJournal(const char* objFilename, const char* journalFilename, uint8_t* data, off_t offset,
                           size_t length);
  int loadObject(int fd, uint8_t* data, off_t offset, size_t length);

  // some KPIs
  // from the user's POV...
  size_t bytesRead, bytesWritten, filesOpened, filesCreated, filesCopied;
  size_t filesDeleted, bytesCopied, filesTruncated, listingCount, callsToWrite;

  // from IOC's pov...
  size_t iocFilesOpened, iocObjectsCreated, iocJournalsCreated, iocFilesDeleted;
  size_t iocBytesRead, iocBytesWritten;
};

}  // namespace storagemanager

#endif

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

#ifndef CACHE_H_
#define CACHE_H_

/* PrefixCache manages the cache for one prefix managed by SM.
   Cache is a map of prefix -> Cache, and holds the items
   that should be centralized like the Downloader
*/

#include "Downloader.h"
#include "SMLogging.h"
#include "PrefixCache.h"
#include "Config.h"

#include <string>
#include <vector>
#include <map>
#include <boost/utility.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/thread/mutex.hpp>

// Setting to min possible based on
// using 1k in config file. If wrong
// letter is used will not value be
// set to less than 1k
#define MIN_CACHE_SIZE 1024

namespace storagemanager
{
class Cache : public boost::noncopyable, public ConfigListener
{
 public:
  static Cache* get();
  virtual ~Cache();

  // reading fcns
  // read() marks objects to be read s.t. they do not get flushed.
  // after reading them, unlock the 'logical file', and call doneReading().
  void read(const boost::filesystem::path& prefix, const std::vector<std::string>& keys);
  void doneReading(const boost::filesystem::path& prefix, const std::vector<std::string>& keys);
  bool exists(const boost::filesystem::path& prefix, const std::string& key);
  void exists(const boost::filesystem::path& prefix, const std::vector<std::string>& keys,
              std::vector<bool>* out);

  // writing fcns
  // new*() fcns tell the Cache data was added.  After writing a set of objects,
  // unlock the 'logical file', and call doneWriting().
  void newObject(const boost::filesystem::path& prefix, const std::string& key, size_t size);
  void newJournalEntry(const boost::filesystem::path& prefix, size_t size);
  void doneWriting(const boost::filesystem::path& prefix);
  void deletedObject(const boost::filesystem::path& prefix, const std::string& key, size_t size);
  void deletedJournal(const boost::filesystem::path& prefix, size_t size);

  // an 'atomic' existence check & delete.  Covers the object and journal.  Does not delete the files.
  // returns 0 if it didn't exist, 1 if the object exists, 2 if the journal exists, and 3 (1 | 2) if both
  // exist This should be called while holding the file lock for key because it touches the journal file.
  int ifExistsThenDelete(const boost::filesystem::path& prefix, const std::string& key);

  // rename is used when an old obj gets merged with its journal file
  // the size will change in that process; sizediff is by how much
  void rename(const boost::filesystem::path& prefix, const std::string& oldKey, const std::string& newKey,
              ssize_t sizediff);
  void setMaxCacheSize(size_t size);
  void makeSpace(const boost::filesystem::path& prefix, size_t size);
  size_t getCurrentCacheSize();
  size_t getCurrentCacheElementCount();
  size_t getCurrentCacheSize(const boost::filesystem::path& prefix);
  size_t getCurrentCacheElementCount(const boost::filesystem::path& prefix);
  size_t getMaxCacheSize() const;

  Downloader* getDownloader() const;
  void newPrefix(const boost::filesystem::path& prefix);
  void dropPrefix(const boost::filesystem::path& prefix);
  void shutdown();
  void printKPIs() const;

  // test helpers
  const boost::filesystem::path& getCachePath() const;
  const boost::filesystem::path& getJournalPath() const;
  const boost::filesystem::path getCachePath(const boost::filesystem::path& prefix) const;
  const boost::filesystem::path getJournalPath(const boost::filesystem::path& prefix) const;
  // this will delete everything in the Cache and journal paths, and empty all Cache structures.
  void reset();
  void validateCacheSize();

  virtual void configListener() override;

 private:
  Cache();

  SMLogging* logger;
  boost::filesystem::path cachePrefix;
  boost::filesystem::path journalPrefix;
  size_t maxCacheSize;
  size_t objectSize;
  boost::scoped_ptr<Downloader> downloader;

  PrefixCache& getPCache(const boost::filesystem::path& prefix);

  std::map<boost::filesystem::path, PrefixCache*> prefixCaches;
  mutable boost::mutex lru_mutex;  // protects the prefixCaches
};

}  // namespace storagemanager

#endif

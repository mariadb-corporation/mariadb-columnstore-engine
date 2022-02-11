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

#ifndef PREFIXCACHE_H_
#define PREFIXCACHE_H_

/* PrefixCache manages the cache for one prefix managed by SM.
   Cache is a map of prefix -> PrefixCache, and holds the items
   that should be centralized like the Downloader
*/

#include "Downloader.h"
#include "SMLogging.h"
#include "Replicator.h"

#include <string>
#include <vector>
#include <list>
#include <unordered_set>
#include <boost/utility.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/thread/mutex.hpp>

namespace storagemanager
{
class PrefixCache : public boost::noncopyable
{
 public:
  PrefixCache(const boost::filesystem::path& prefix);
  virtual ~PrefixCache();

  // reading fcns
  // read() marks objects to be read s.t. they do not get flushed.
  // after reading them, unlock the 'logical file', and call doneReading().
  void read(const std::vector<std::string>& keys);
  void doneReading(const std::vector<std::string>& keys);
  bool exists(const std::string& key) const;
  void exists(const std::vector<std::string>& keys, std::vector<bool>* out) const;

  // writing fcns
  // new*() fcns tell the PrefixCache data was added.  After writing a set of objects,
  // unlock the 'logical file', and call doneWriting().
  void newObject(const std::string& key, size_t size);
  void newJournalEntry(size_t size);
  void doneWriting();
  void deletedObject(const std::string& key, size_t size);
  void deletedJournal(size_t size);

  // an 'atomic' existence check & delete.  Covers the object and journal.  Does not delete the files.
  // returns 0 if it didn't exist, 1 if the object exists, 2 if the journal exists, and 3 (1 | 2) if both
  // exist This should be called while holding the file lock for key because it touches the journal file.
  int ifExistsThenDelete(const std::string& key);

  // rename is used when an old obj gets merged with its journal file
  // the size will change in that process; sizediff is by how much
  void rename(const std::string& oldKey, const std::string& newKey, ssize_t sizediff);
  void setMaxCacheSize(size_t size);
  void makeSpace(size_t size);
  size_t getCurrentCacheSize() const;
  size_t getCurrentCacheElementCount() const;
  size_t getMaxCacheSize() const;
  void shutdown();

  // test helpers
  const boost::filesystem::path& getCachePath();
  const boost::filesystem::path& getJournalPath();
  // this will delete everything in the PrefixCache and journal paths, and empty all PrefixCache structures.
  void reset();
  void validateCacheSize();

 private:
  PrefixCache();

  boost::filesystem::path cachePrefix;
  boost::filesystem::path journalPrefix;
  boost::filesystem::path firstDir;
  size_t maxCacheSize;
  size_t objectSize;
  size_t currentCacheSize;
  Replicator* replicator;
  SMLogging* logger;
  Downloader* downloader;

  void populate();
  void _makeSpace(size_t size);

  /* The main PrefixCache structures */
  // lru owns the string memory for the filenames it manages.  m_lru and DNE point to those strings.
  typedef std::list<std::string> LRU_t;
  LRU_t lru;

  struct M_LRU_element_t
  {
    M_LRU_element_t(const std::string&);
    M_LRU_element_t(const std::string*);
    M_LRU_element_t(const LRU_t::iterator&);
    const std::string* key;
    LRU_t::iterator lit;
  };
  struct KeyHasher
  {
    size_t operator()(const M_LRU_element_t& l) const;
  };

  struct KeyEquals
  {
    bool operator()(const M_LRU_element_t& l1, const M_LRU_element_t& l2) const;
  };

  typedef std::unordered_set<M_LRU_element_t, KeyHasher, KeyEquals> M_LRU_t;
  M_LRU_t m_lru;  // the LRU entries as a hash table

  /* The do-not-evict list stuff. */
  struct DNEElement
  {
    DNEElement(const LRU_t::iterator&);
    DNEElement(const std::string&);
    LRU_t::iterator key;
    std::string sKey;
    uint refCount;
  };

  struct DNEHasher
  {
    size_t operator()(const DNEElement& d) const;
  };

  struct DNEEquals
  {
    bool operator()(const DNEElement& d1, const DNEElement& d2) const;
  };

  typedef std::unordered_set<DNEElement, DNEHasher, DNEEquals> DNE_t;
  DNE_t doNotEvict;
  void addToDNE(const DNEElement&);
  void removeFromDNE(const DNEElement&);

  // the to-be-deleted set.  Elements removed from the LRU but not yet deleted will be here.
  // Elements are inserted and removed by makeSpace().  If read() references a file that is in this,
  // it will remove it, signalling to makeSpace that it should not be deleted
  struct TBDLess
  {
    bool operator()(const LRU_t::iterator&, const LRU_t::iterator&) const;
  };

  typedef std::set<LRU_t::iterator, TBDLess> TBD_t;
  TBD_t toBeDeleted;

  mutable boost::mutex lru_mutex;  // protects the main PrefixCache structures & the do-not-evict set
};

}  // namespace storagemanager

#endif

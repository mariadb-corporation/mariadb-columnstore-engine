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

#include "PrefixCache.h"
#include "Cache.h"
#include "Config.h"
#include "Downloader.h"
#include "Synchronizer.h"
#include <iostream>
#include <syslog.h>
#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;
namespace bf = boost::filesystem;

namespace storagemanager
{
PrefixCache::PrefixCache(const bf::path& prefix) : firstDir(prefix), currentCacheSize(0)
{
  Config* conf = Config::get();
  logger = SMLogging::get();
  replicator = Replicator::get();
  downloader = Cache::get()->getDownloader();

  string stmp = conf->getValue("Cache", "cache_size");
  if (stmp.empty())
  {
    logger->log(LOG_CRIT, "Cache/cache_size is not set");
    throw runtime_error("Please set Cache/cache_size in the storagemanager.cnf file");
  }
  try
  {
    maxCacheSize = stoul(stmp);
  }
  catch (invalid_argument&)
  {
    logger->log(LOG_CRIT, "Cache/cache_size is not a number");
    throw runtime_error("Please set Cache/cache_size to a number");
  }
  // cout << "Cache got cache size " << maxCacheSize << endl;

  stmp = conf->getValue("ObjectStorage", "object_size");
  if (stmp.empty())
  {
    logger->log(LOG_CRIT, "ObjectStorage/object_size is not set");
    throw runtime_error("Please set ObjectStorage/object_size in the storagemanager.cnf file");
  }
  try
  {
    objectSize = stoul(stmp);
  }
  catch (invalid_argument&)
  {
    logger->log(LOG_CRIT, "ObjectStorage/object_size is not a number");
    throw runtime_error("Please set ObjectStorage/object_size to a number");
  }

  cachePrefix = conf->getValue("Cache", "path");
  if (cachePrefix.empty())
  {
    logger->log(LOG_CRIT, "Cache/path is not set");
    throw runtime_error("Please set Cache/path in the storagemanager.cnf file");
  }
  cachePrefix /= firstDir;

  try
  {
    bf::create_directories(cachePrefix);
  }
  catch (exception& e)
  {
    logger->log(LOG_CRIT, "Failed to create %s, got: %s", cachePrefix.string().c_str(), e.what());
    throw e;
  }

  stmp = conf->getValue("ObjectStorage", "journal_path");
  if (stmp.empty())
  {
    logger->log(LOG_CRIT, "ObjectStorage/journal_path is not set");
    throw runtime_error("Please set ObjectStorage/journal_path in the storagemanager.cnf file");
  }
  journalPrefix = stmp;
  journalPrefix /= firstDir;

  bf::create_directories(journalPrefix);
  try
  {
    bf::create_directories(journalPrefix);
  }
  catch (exception& e)
  {
    logger->log(LOG_CRIT, "Failed to create %s, got: %s", journalPrefix.string().c_str(), e.what());
    throw e;
  }

  lru_mutex.lock();  // unlocked by populate() when it's done
  // Ideally put this in background but has to be synchronous with write calls
  populate();
  // boost::thread t([this] { this->populate(); });
  // t.detach();
}

PrefixCache::~PrefixCache()
{
  /*  This and shutdown() need to do whatever is necessary to leave cache contents in a safe
      state on disk.  Does anything need to be done toward that?
  */
}

void PrefixCache::populate()
{
  Synchronizer* sync = Synchronizer::get();
  bf::directory_iterator dir(cachePrefix);
  bf::directory_iterator dend;
  vector<string> newObjects;
  while (dir != dend)
  {
    // put everything in lru & m_lru
    const bf::path& p = dir->path();
    if (bf::is_regular_file(p))
    {
      lru.push_back(p.filename().string());
      auto last = lru.end();
      m_lru.insert(--last);
      currentCacheSize += bf::file_size(*dir);
      newObjects.push_back(p.filename().string());
    }
    else if (p != cachePrefix / downloader->getTmpPath())
      logger->log(LOG_WARNING, "Cache: found something in the cache that does not belong '%s'",
                  p.string().c_str());
    ++dir;
  }
  sync->newObjects(firstDir, newObjects);
  newObjects.clear();

  // account for what's in the journal dir
  vector<pair<string, size_t> > newJournals;
  dir = bf::directory_iterator(journalPrefix);
  while (dir != dend)
  {
    const bf::path& p = dir->path();
    if (bf::is_regular_file(p))
    {
      if (p.extension() == ".journal")
      {
        size_t s = bf::file_size(*dir);
        currentCacheSize += s;
        newJournals.push_back(pair<string, size_t>(p.stem().string(), s));
      }
      else
        logger->log(LOG_WARNING, "Cache: found a file in the journal dir that does not belong '%s'",
                    p.string().c_str());
    }
    else
      logger->log(LOG_WARNING, "Cache: found something in the journal dir that does not belong '%s'",
                  p.string().c_str());
    ++dir;
  }
  lru_mutex.unlock();
  sync->newJournalEntries(firstDir, newJournals);
}

// be careful using this!  SM should be idle.  No ongoing reads or writes.
void PrefixCache::validateCacheSize()
{
  boost::unique_lock<boost::mutex> s(lru_mutex);

  if (!doNotEvict.empty() || !toBeDeleted.empty())
  {
    cout << "Not safe to use validateCacheSize() at the moment." << endl;
    return;
  }

  size_t oldSize = currentCacheSize;
  currentCacheSize = 0;
  m_lru.clear();
  lru.clear();
  populate();

  if (oldSize != currentCacheSize)
    logger->log(LOG_DEBUG,
                "PrefixCache::validateCacheSize(): found a discrepancy.  Actual size is %lld, had %lld.",
                currentCacheSize, oldSize);
  else
    logger->log(LOG_DEBUG,
                "PrefixCache::validateCacheSize(): Cache size accounting agrees with reality for now.");
}

void PrefixCache::read(const vector<string>& keys)
{
  /*  Move existing keys to the back of the LRU, start downloading nonexistant keys.
   */
  vector<const string*> keysToFetch;
  vector<int> dlErrnos;
  vector<size_t> dlSizes;

  boost::unique_lock<boost::mutex> s(lru_mutex);

  M_LRU_t::iterator mit;
  for (const string& key : keys)
  {
    mit = m_lru.find(key);
    if (mit != m_lru.end())
    {
      addToDNE(mit->lit);
      lru.splice(lru.end(), lru, mit->lit);  // move them to the back so they are last to pick for eviction
    }
    else
    {
      // There's window where the file has been downloaded but is not yet
      // added to the lru structs.  However it is in the DNE.  If it is in the DNE, then it is also
      // in Downloader's map.  So, this thread needs to start the download if it's not in the
      // DNE or if there's an existing download that hasn't finished yet.  Starting the download
      // includes waiting for an existing download to finish, which from this class's pov is the
      // same thing.
      if (doNotEvict.find(key) == doNotEvict.end() || downloader->inProgress(key))
        keysToFetch.push_back(&key);
      else
        cout << "Cache: detected and stopped a racey download" << endl;
      addToDNE(key);
    }
  }
  if (keysToFetch.empty())
    return;

  downloader->download(keysToFetch, &dlErrnos, &dlSizes, cachePrefix, &lru_mutex);

  size_t sum_sizes = 0;
  for (uint i = 0; i < keysToFetch.size(); ++i)
  {
    // downloads with size 0 didn't actually happen, either because it
    // was a preexisting download (another read() call owns it), or because
    // there was an error downloading it.  Use size == 0 as an indication of
    // what to add to the cache.  Also needs to verify that the file was not deleted,
    // indicated by existence in doNotEvict.
    if (dlSizes[i] != 0)
    {
      if (doNotEvict.find(*keysToFetch[i]) != doNotEvict.end())
      {
        sum_sizes += dlSizes[i];
        lru.push_back(*keysToFetch[i]);
        LRU_t::iterator lit = lru.end();
        m_lru.insert(--lit);  // I dislike this way of grabbing the last iterator in a list.
      }
      else  // it was downloaded, but a deletion happened so we have to toss it
      {
        cout << "removing a file that was deleted by another thread during download" << endl;
        bf::remove(cachePrefix / (*keysToFetch[i]));
      }
    }
  }

  // move everything in keys to the back of the lru (yes, again)
  for (const string& key : keys)
  {
    mit = m_lru.find(key);
    if (mit != m_lru.end())  // all of the files exist, just not all of them are 'owned by' this thread.
      lru.splice(lru.end(), lru, mit->lit);
  }

  // fix cache size
  //_makeSpace(sum_sizes);
  currentCacheSize += sum_sizes;
}

void PrefixCache::doneReading(const vector<string>& keys)
{
  boost::unique_lock<boost::mutex> s(lru_mutex);
  for (const string& key : keys)
  {
    removeFromDNE(key);
    // most should be in the map.
    // debateable whether it's faster to look up the list iterator and use it
    // or whether it's faster to bypass that and use strings only.

    // const auto &it = m_lru.find(key);
    // if (it != m_lru.end())
    //    removeFromDNE(it->lit);
  }
  _makeSpace(0);
}

void PrefixCache::doneWriting()
{
  makeSpace(0);
}

PrefixCache::DNEElement::DNEElement(const LRU_t::iterator& k) : key(k), refCount(1)
{
}
PrefixCache::DNEElement::DNEElement(const string& k) : sKey(k), refCount(1)
{
}

void PrefixCache::addToDNE(const DNEElement& key)
{
  DNE_t::iterator it = doNotEvict.find(key);
  if (it != doNotEvict.end())
  {
    DNEElement& dnee = const_cast<DNEElement&>(*it);
    ++(dnee.refCount);
  }
  else
    doNotEvict.insert(key);
}

void PrefixCache::removeFromDNE(const DNEElement& key)
{
  DNE_t::iterator it = doNotEvict.find(key);
  if (it == doNotEvict.end())
    return;
  DNEElement& dnee = const_cast<DNEElement&>(*it);
  if (--(dnee.refCount) == 0)
    doNotEvict.erase(it);
}

const bf::path& PrefixCache::getCachePath()
{
  return cachePrefix;
}

const bf::path& PrefixCache::getJournalPath()
{
  return journalPrefix;
}

void PrefixCache::exists(const vector<string>& keys, vector<bool>* out) const
{
  out->resize(keys.size());
  boost::unique_lock<boost::mutex> s(lru_mutex);
  for (uint i = 0; i < keys.size(); i++)
    (*out)[i] = (m_lru.find(keys[i]) != m_lru.end());
}

bool PrefixCache::exists(const string& key) const
{
  boost::unique_lock<boost::mutex> s(lru_mutex);
  return m_lru.find(key) != m_lru.end();
}

void PrefixCache::newObject(const string& key, size_t size)
{
  boost::unique_lock<boost::mutex> s(lru_mutex);
  assert(m_lru.find(key) == m_lru.end());
  if (m_lru.find(key) != m_lru.end())
  {
    // This should never happen but was in MCOL-3499
    // Remove this when PrefixCache ctor can call populate() synchronous with write calls
    logger->log(LOG_ERR, "PrefixCache::newObject(): key exists in m_lru already %s", key.c_str());
  }
  //_makeSpace(size);
  lru.push_back(key);
  LRU_t::iterator back = lru.end();
  m_lru.insert(--back);
  currentCacheSize += size;
}

void PrefixCache::newJournalEntry(size_t size)
{
  boost::unique_lock<boost::mutex> s(lru_mutex);
  //_makeSpace(size);
  currentCacheSize += size;
}

void PrefixCache::deletedJournal(size_t size)
{
  boost::unique_lock<boost::mutex> s(lru_mutex);

  if (currentCacheSize >= size)
    currentCacheSize -= size;
  else
  {
    ostringstream oss;
    oss << "PrefixCache::deletedJournal(): Detected an accounting error.";
    logger->log(LOG_WARNING, oss.str().c_str());
    currentCacheSize = 0;
  }
}

void PrefixCache::deletedObject(const string& key, size_t size)
{
  boost::unique_lock<boost::mutex> s(lru_mutex);

  M_LRU_t::iterator mit = m_lru.find(key);
  assert(mit != m_lru.end());

  // if it's being flushed, let makeSpace() do the deleting
  if (toBeDeleted.find(mit->lit) == toBeDeleted.end())
  {
    doNotEvict.erase(mit->lit);
    lru.erase(mit->lit);
    m_lru.erase(mit);
    if (currentCacheSize >= size)
      currentCacheSize -= size;
    else
    {
      ostringstream oss;
      oss << "PrefixCache::deletedObject(): Detected an accounting error.";
      logger->log(LOG_WARNING, oss.str().c_str());
      currentCacheSize = 0;
    }
  }
}

void PrefixCache::setMaxCacheSize(size_t size)
{
  boost::unique_lock<boost::mutex> s(lru_mutex);
  if (size < maxCacheSize)
    _makeSpace(maxCacheSize - size);
  maxCacheSize = size;
}

void PrefixCache::makeSpace(size_t size)
{
  boost::unique_lock<boost::mutex> s(lru_mutex);
  _makeSpace(size);
}

size_t PrefixCache::getMaxCacheSize() const
{
  return maxCacheSize;
}

// call this holding lru_mutex
void PrefixCache::_makeSpace(size_t size)
{
  ssize_t thisMuch = currentCacheSize + size - maxCacheSize;
  if (thisMuch <= 0)
    return;

  LRU_t::iterator it;
  while (thisMuch > 0 && !lru.empty())
  {
    it = lru.begin();
    // find the first element not being either read() right now or being processed by another
    // makeSpace() call.
    while (it != lru.end())
    {
      // make sure it's not currently being read or being flushed by another _makeSpace() call
      if ((doNotEvict.find(it) == doNotEvict.end()) && (toBeDeleted.find(it) == toBeDeleted.end()))
        break;
      ++it;
    }
    if (it == lru.end())
    {
      // nothing can be deleted right now
      return;
    }

    // ran into this a couple times, still happens as of commit 948ee1aa5
    // BT: made this more visable in logging.
    //     likely related to MCOL-3499 and lru containing double entries.
    if (!bf::exists(cachePrefix / *it))
      logger->log(LOG_WARNING, "PrefixCache::makeSpace(): doesn't exist, %s/%s", cachePrefix.string().c_str(),
                  ((string)(*it)).c_str());
    assert(bf::exists(cachePrefix / *it));
    /*
        tell Synchronizer that this key will be evicted
        delete the file
        remove it from our structs
        update current size
    */

    // logger->log(LOG_WARNING, "Cache:  flushing!");
    toBeDeleted.insert(it);

    string key = *it;  // need to make a copy; it could get changed after unlocking.

    lru_mutex.unlock();
    try
    {
      Synchronizer::get()->flushObject(firstDir, key);
    }
    catch (...)
    {
      // it gets logged by Sync
      lru_mutex.lock();
      toBeDeleted.erase(it);
      continue;
    }
    lru_mutex.lock();

    // check doNotEvict again in case this object is now being read
    if (doNotEvict.find(it) == doNotEvict.end())
    {
      bf::path cachedFile = cachePrefix / *it;
      m_lru.erase(*it);
      toBeDeleted.erase(it);
      lru.erase(it);
      size_t newSize = bf::file_size(cachedFile);
      replicator->remove(cachedFile, Replicator::LOCAL_ONLY);
      if (newSize < currentCacheSize)
      {
        currentCacheSize -= newSize;
        thisMuch -= newSize;
      }
      else
      {
        logger->log(LOG_WARNING,
                    "PrefixCache::makeSpace(): accounting error.  Almost wrapped currentCacheSize on flush.");
        currentCacheSize = 0;
        thisMuch = 0;
      }
    }
    else
      toBeDeleted.erase(it);
  }
}

void PrefixCache::rename(const string& oldKey, const string& newKey, ssize_t sizediff)
{
  // rename it in the LRU
  // erase/insert to rehash it everywhere else

  boost::unique_lock<boost::mutex> s(lru_mutex);
  auto it = m_lru.find(oldKey);
  if (it == m_lru.end())
    return;

  auto lit = it->lit;
  m_lru.erase(it);
  int refCount = 0;
  auto dne_it = doNotEvict.find(lit);
  if (dne_it != doNotEvict.end())
  {
    refCount = dne_it->refCount;
    doNotEvict.erase(dne_it);
  }

  auto tbd_it = toBeDeleted.find(lit);
  bool hasTBDEntry = (tbd_it != toBeDeleted.end());
  if (hasTBDEntry)
    toBeDeleted.erase(tbd_it);

  *lit = newKey;

  if (hasTBDEntry)
    toBeDeleted.insert(lit);
  if (refCount != 0)
  {
    pair<DNE_t::iterator, bool> dne_tmp = doNotEvict.insert(lit);
    const_cast<DNEElement&>(*(dne_tmp.first)).refCount = refCount;
  }

  m_lru.insert(lit);
  currentCacheSize += sizediff;
}

int PrefixCache::ifExistsThenDelete(const string& key)
{
  bf::path cachedPath = cachePrefix / key;
  bf::path journalPath = journalPrefix / (key + ".journal");

  boost::unique_lock<boost::mutex> s(lru_mutex);
  bool objectExists = false;

  auto it = m_lru.find(key);
  if (it != m_lru.end())
  {
    if (toBeDeleted.find(it->lit) == toBeDeleted.end())
    {
      doNotEvict.erase(it->lit);
      lru.erase(it->lit);
      m_lru.erase(it);
      objectExists = true;
    }
    else  // let makeSpace() delete it if it's already in progress
      return 0;
  }
  bool journalExists = bf::exists(journalPath);
  // assert(objectExists == bf::exists(cachedPath));

  size_t objectSize = (objectExists ? bf::file_size(cachedPath) : 0);
  // size_t objectSize = (objectExists ? MetadataFile::getLengthFromKey(key) : 0);
  size_t journalSize = (journalExists ? bf::file_size(journalPath) : 0);
  currentCacheSize -= (objectSize + journalSize);

  // assert(!objectExists || objectSize == bf::file_size(cachedPath));

  return (objectExists ? 1 : 0) | (journalExists ? 2 : 0);
}

size_t PrefixCache::getCurrentCacheSize() const
{
  return currentCacheSize;
}

size_t PrefixCache::getCurrentCacheElementCount() const
{
  boost::unique_lock<boost::mutex> s(lru_mutex);
  assert(m_lru.size() == lru.size());
  return m_lru.size();
}

void PrefixCache::reset()
{
  boost::unique_lock<boost::mutex> s(lru_mutex);
  m_lru.clear();
  lru.clear();
  toBeDeleted.clear();
  doNotEvict.clear();

  bf::directory_iterator dir;
  bf::directory_iterator dend;
  for (dir = bf::directory_iterator(cachePrefix); dir != dend; ++dir)
    bf::remove_all(dir->path());

  for (dir = bf::directory_iterator(journalPrefix); dir != dend; ++dir)
    bf::remove_all(dir->path());
  currentCacheSize = 0;
}

void PrefixCache::shutdown()
{
  /* Does this need to do something anymore? */
}

/* The helper classes */

PrefixCache::M_LRU_element_t::M_LRU_element_t(const string* k) : key(k)
{
}

PrefixCache::M_LRU_element_t::M_LRU_element_t(const string& k) : key(&k)
{
}

PrefixCache::M_LRU_element_t::M_LRU_element_t(const LRU_t::iterator& i) : key(&(*i)), lit(i)
{
}

inline size_t PrefixCache::KeyHasher::operator()(const M_LRU_element_t& l) const
{
  return hash<string>()(*(l.key));
}

inline bool PrefixCache::KeyEquals::operator()(const M_LRU_element_t& l1, const M_LRU_element_t& l2) const
{
  return (*(l1.key) == *(l2.key));
}

inline size_t PrefixCache::DNEHasher::operator()(const DNEElement& l) const
{
  return (l.sKey.empty() ? hash<string>()(*(l.key)) : hash<string>()(l.sKey));
}

inline bool PrefixCache::DNEEquals::operator()(const DNEElement& l1, const DNEElement& l2) const
{
  const string *s1, *s2;
  s1 = l1.sKey.empty() ? &(*(l1.key)) : &(l1.sKey);
  s2 = l2.sKey.empty() ? &(*(l2.key)) : &(l2.sKey);

  return (*s1 == *s2);
}

inline bool PrefixCache::TBDLess::operator()(const LRU_t::iterator& i1, const LRU_t::iterator& i2) const
{
  return *i1 < *i2;
}

}  // namespace storagemanager

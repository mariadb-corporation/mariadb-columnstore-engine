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

namespace
{
boost::mutex m;
storagemanager::Cache* inst = NULL;
}  // namespace

namespace storagemanager
{
Cache* Cache::get()
{
  if (inst)
    return inst;
  boost::unique_lock<boost::mutex> s(m);
  if (inst)
    return inst;
  inst = new Cache();
  return inst;
}

Cache::Cache()
{
  Config* conf = Config::get();
  logger = SMLogging::get();

  configListener();
  conf->addConfigListener(this);
  // cout << "Cache got cache size " << maxCacheSize << endl;

  string stmp = conf->getValue("ObjectStorage", "object_size");
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

  try
  {
    bf::create_directories(cachePrefix);
  }
  catch (exception& e)
  {
    logger->log(LOG_CRIT, "Failed to create %s, got: %s", cachePrefix.string().c_str(), e.what());
    throw e;
  }
  // cout << "Cache got cachePrefix " << cachePrefix << endl;
  downloader.reset(new Downloader());

  stmp = conf->getValue("ObjectStorage", "journal_path");
  if (stmp.empty())
  {
    logger->log(LOG_CRIT, "ObjectStorage/journal_path is not set");
    throw runtime_error("Please set ObjectStorage/journal_path in the storagemanager.cnf file");
  }
  journalPrefix = stmp;
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
}

Cache::~Cache()
{
  Config::get()->removeConfigListener(this);
  for (auto it = prefixCaches.begin(); it != prefixCaches.end(); ++it)
    delete it->second;
}

// be careful using this!  SM should be idle.  No ongoing reads or writes.
void Cache::validateCacheSize()
{
  boost::unique_lock<boost::mutex> s(lru_mutex);

  for (auto it = prefixCaches.begin(); it != prefixCaches.end(); ++it)
    it->second->validateCacheSize();
}

// new simplified version
void Cache::read(const bf::path& prefix, const vector<string>& keys)
{
  getPCache(prefix).read(keys);
}

void Cache::doneReading(const bf::path& prefix, const vector<string>& keys)
{
  getPCache(prefix).doneReading(keys);
}

void Cache::doneWriting(const bf::path& prefix)
{
  getPCache(prefix).makeSpace(0);
}

const bf::path& Cache::getCachePath() const
{
  return cachePrefix;
}

const bf::path& Cache::getJournalPath() const
{
  return journalPrefix;
}

const bf::path Cache::getCachePath(const bf::path& prefix) const
{
  return cachePrefix / prefix;
}

const bf::path Cache::getJournalPath(const bf::path& prefix) const
{
  return journalPrefix / prefix;
}

void Cache::exists(const bf::path& prefix, const vector<string>& keys, vector<bool>* out)
{
  getPCache(prefix).exists(keys, out);
}

bool Cache::exists(const bf::path& prefix, const string& key)
{
  return getPCache(prefix).exists(key);
}

void Cache::newObject(const bf::path& prefix, const string& key, size_t size)
{
  getPCache(prefix).newObject(key, size);
}

void Cache::newJournalEntry(const bf::path& prefix, size_t size)
{
  getPCache(prefix).newJournalEntry(size);
}

void Cache::deletedJournal(const bf::path& prefix, size_t size)
{
  getPCache(prefix).deletedJournal(size);
}

void Cache::deletedObject(const bf::path& prefix, const string& key, size_t size)
{
  getPCache(prefix).deletedObject(key, size);
}

void Cache::setMaxCacheSize(size_t size)
{
  boost::unique_lock<boost::mutex> s(lru_mutex);

  maxCacheSize = size;
  for (auto it = prefixCaches.begin(); it != prefixCaches.end(); ++it)
    it->second->setMaxCacheSize(size);
}

void Cache::makeSpace(const bf::path& prefix, size_t size)
{
  getPCache(prefix).makeSpace(size);
}

size_t Cache::getMaxCacheSize() const
{
  return maxCacheSize;
}

void Cache::rename(const bf::path& prefix, const string& oldKey, const string& newKey, ssize_t sizediff)
{
  getPCache(prefix).rename(oldKey, newKey, sizediff);
}

int Cache::ifExistsThenDelete(const bf::path& prefix, const string& key)
{
  return getPCache(prefix).ifExistsThenDelete(key);
}

void Cache::printKPIs() const
{
  downloader->printKPIs();
}
size_t Cache::getCurrentCacheSize()
{
  size_t totalSize = 0;

  boost::unique_lock<boost::mutex> s(lru_mutex);

  for (auto it = prefixCaches.begin(); it != prefixCaches.end(); ++it)
    totalSize += it->second->getCurrentCacheSize();
  return totalSize;
}

size_t Cache::getCurrentCacheElementCount()
{
  size_t totalCount = 0;

  for (auto it = prefixCaches.begin(); it != prefixCaches.end(); ++it)
    totalCount += it->second->getCurrentCacheElementCount();
  return totalCount;
}

size_t Cache::getCurrentCacheSize(const bf::path& prefix)
{
  return getPCache(prefix).getCurrentCacheSize();
}

size_t Cache::getCurrentCacheElementCount(const bf::path& prefix)
{
  return getPCache(prefix).getCurrentCacheElementCount();
}

void Cache::reset()
{
  boost::unique_lock<boost::mutex> s(lru_mutex);

  for (auto it = prefixCaches.begin(); it != prefixCaches.end(); ++it)
    it->second->reset();
}

void Cache::newPrefix(const bf::path& prefix)
{
  boost::unique_lock<boost::mutex> s(lru_mutex);

  // cerr << "Cache: making new prefix " << prefix.string() << endl;
  assert(prefixCaches.find(prefix) == prefixCaches.end());
  prefixCaches[prefix] = NULL;
  s.unlock();
  PrefixCache* pCache = new PrefixCache(prefix);
  s.lock();
  prefixCaches[prefix] = pCache;
}

void Cache::dropPrefix(const bf::path& prefix)
{
  boost::unique_lock<boost::mutex> s(lru_mutex);

  auto* pCache = prefixCaches[prefix];
  prefixCaches.erase(prefix);
  s.unlock();
  delete pCache;
}

inline PrefixCache& Cache::getPCache(const bf::path& prefix)
{
  boost::unique_lock<boost::mutex> s(lru_mutex);

  // cerr << "Getting pcache for " << prefix.string() << endl;
  PrefixCache* ret;
  auto it = prefixCaches.find(prefix);
  assert(it != prefixCaches.end());

  ret = it->second;
  while (ret == NULL)  // wait for the new PC to init
  {
    s.unlock();
    sleep(1);
    s.lock();
    ret = prefixCaches[prefix];
  }

  return *ret;
}

Downloader* Cache::getDownloader() const
{
  return downloader.get();
}

void Cache::shutdown()
{
  boost::unique_lock<boost::mutex> s(lru_mutex);
  downloader.reset();
}

void Cache::configListener()
{
  Config* conf = Config::get();
  SMLogging* logger = SMLogging::get();

  if (maxCacheSize == 0)
    maxCacheSize = 2147483648;
  string stmp = conf->getValue("Cache", "cache_size");
  if (stmp.empty())
  {
    logger->log(LOG_CRIT, "Cache/cache_size is not set. Using current value = %zi", maxCacheSize);
  }
  try
  {
    size_t newMaxCacheSize = stoull(stmp);
    if (newMaxCacheSize != maxCacheSize)
    {
      if (newMaxCacheSize >= MIN_CACHE_SIZE)
      {
        setMaxCacheSize(newMaxCacheSize);
        logger->log(LOG_INFO, "Cache/cache_size = %zi", maxCacheSize);
      }
      else
      {
        logger->log(LOG_CRIT,
                    "Cache/cache_size is below %u. Check value and suffix are correct in configuration file. "
                    "Using current value = %zi",
                    MIN_CACHE_SIZE, maxCacheSize);
      }
    }
  }
  catch (invalid_argument&)
  {
    logger->log(LOG_CRIT, "Cache/cache_size is not a number. Using current value = %zi", maxCacheSize);
  }
}
}  // namespace storagemanager

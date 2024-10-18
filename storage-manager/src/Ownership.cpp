/* Copyright (C) 2024 MariaDB Corporation

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

#include "Ownership.h"
#include "Config.h"
#include "Cache.h"
#include "Synchronizer.h"
#include "KVStorageInitializer.h"
#include "KVPrefixes.hpp"
#include "fdbcs.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <boost/filesystem.hpp>

using namespace std;
namespace bf = boost::filesystem;

namespace storagemanager
{
// FDB recommends keep the key size up to 32 bytes.
const char* ownerShipStates[3] = {/*OWNED*/ "_O", /*FLUSHING*/ "_F", /*REQUEST_TRANSFER*/ "_RT"};

inline std::string getKeyName(const std::string& fileName, OwnershipStateId state)
{
  return KVPrefixes[static_cast<uint32_t>(KVPrefixId::SM_OWNERSHIP)] + fileName +
         ownerShipStates[static_cast<uint32_t>(state)];
}

inline void Ownership::removeKeys(const std::string& fileName, const std::vector<OwnershipStateId>& states)
{
  auto kvStorage = KVStorageInitializer::getStorageInstance();
  auto tnx = kvStorage->createTransaction();

  for (const auto state : states)
    tnx->remove(getKeyName(fileName, state));

  if (!tnx->commit())
  {
    const char* msg = "Ownership: commit `removeKeys` failed ";
    logger->log(LOG_CRIT, msg);
    throw runtime_error(msg);
  }
}

Ownership::Ownership()
{
  Config* config = Config::get();
  logger = SMLogging::get();

  string sPrefixDepth = config->getValue("ObjectStorage", "common_prefix_depth");
  if (sPrefixDepth.empty())
  {
    const char* msg =
        "Ownership: Need to specify ObjectStorage/common_prefix_depth in the storagemanager.cnf file";
    logger->log(LOG_CRIT, msg);
    throw runtime_error(msg);
  }
  try
  {
    prefixDepth = stoul(sPrefixDepth, NULL, 0);
  }
  catch (invalid_argument& e)
  {
    const char* msg = "Ownership: Invalid value in ObjectStorage/common_prefix_depth";
    logger->log(LOG_CRIT, msg);
    throw runtime_error(msg);
  }

  metadataPrefix = config->getValue("ObjectStorage", "metadata_path");
  if (metadataPrefix.empty())
  {
    const char* msg = "Ownership: Need to specify ObjectStorage/metadata_path in the storagemanager.cnf file";
    logger->log(LOG_CRIT, msg);
    throw runtime_error(msg);
  }

  monitor = new Monitor(this);
}

Ownership::~Ownership()
{
  delete monitor;
  for (auto& it : ownedPrefixes)
    releaseOwnership(it.first, true);
}

bf::path Ownership::get(const bf::path& p, bool getOwnership)
{
  bf::path ret, prefix, normalizedPath(p);
  bf::path::const_iterator pit;
  int i, levels;

  normalizedPath.normalize();
  // cerr << "Ownership::get() param = " << normalizedPath.string() << endl;
  if (prefixDepth > 0)
  {
    for (i = 0, pit = normalizedPath.begin(); i <= prefixDepth && pit != normalizedPath.end(); ++i, ++pit)
      ;
    if (pit != normalizedPath.end())
      prefix = *pit;
    // cerr << "prefix is " << prefix.string() << endl;
    for (levels = 0; pit != normalizedPath.end(); ++levels, ++pit)
      ret /= *pit;
    if (!getOwnership)
      return ret;
    if (levels <= 1)
      throw runtime_error("Ownership: given path " + normalizedPath.string() +
                          " does not have minimum number of directories");
  }
  else
  {
    ret = normalizedPath;
    prefix = *(normalizedPath.begin());
  }

  if (!getOwnership)
    return ret;

  mutex.lock();
  if (ownedPrefixes.find(prefix) == ownedPrefixes.end())
  {
    mutex.unlock();
    takeOwnership(prefix);
  }
  else
  {
    // todo...  replace this polling, and the similar polling in Cache, with proper condition vars.
    while (ownedPrefixes[prefix] == false)
    {
      mutex.unlock();
      sleep(1);
      mutex.lock();
    }
    mutex.unlock();
  }
  // cerr << "returning " << ret.string() << endl;
  return ret;
}

void Ownership::touchFlushing(const bf::path& prefix, volatile bool* doneFlushing) const
{
  while (!*doneFlushing)
  {
    {
      auto kvStorage = KVStorageInitializer::getStorageInstance();
      auto tnx = kvStorage->createTransaction();
      const std::string flushingKey = getKeyName(prefix.string(), OwnershipStateId::FLUSHING);
      tnx->set(flushingKey, "");
      if (!tnx->commit())
      {
        const char* msg = "Ownership: commit `touchFlushing` failed ";
        logger->log(LOG_CRIT, msg);
        throw runtime_error(msg);
      }
    }
    try
    {
      boost::this_thread::sleep_for(boost::chrono::seconds(1));
    }
    catch (boost::thread_interrupted&)
    {
    }
  }
}

void Ownership::releaseOwnership(const bf::path& p, bool isDtor)
{
  logger->log(LOG_DEBUG, "Ownership: releasing ownership of %s", p.string().c_str());
  boost::unique_lock<boost::mutex> s(mutex);

  auto it = ownedPrefixes.find(p);
  if (it == ownedPrefixes.end())
  {
    logger->log(LOG_DEBUG, "Ownership::releaseOwnership(): told to disown %s, but do not own it",
                p.string().c_str());
    return;
  }

  if (isDtor)
  {
    removeKeys(p.string(), {OwnershipStateId::OWNED, OwnershipStateId::FLUSHING});
    return;
  }
  else
    ownedPrefixes.erase(it);

  s.unlock();

  volatile bool done = false;

  // start flushing
  boost::thread xfer([this, &p, &done] { this->touchFlushing(p, &done); });
  Synchronizer::get()->dropPrefix(p);
  Cache::get()->dropPrefix(p);
  done = true;
  xfer.interrupt();
  xfer.join();
  removeKeys(p.string(), {OwnershipStateId::OWNED, OwnershipStateId::FLUSHING});
}

void Ownership::_takeOwnership(const bf::path& p)
{
  logger->log(LOG_DEBUG, "Ownership: taking ownership of %s", p.string().c_str());
  {
    auto kvStorage = KVStorageInitializer::getStorageInstance();
    auto tnx = kvStorage->createTransaction();
    const std::string ownedKey = getKeyName(p.string(), OwnershipStateId::OWNED);
    const std::string flushingKey = getKeyName(p.string(), OwnershipStateId::FLUSHING);
    const std::string requestTransferKey = getKeyName(p.string(), OwnershipStateId::REQUEST_TRANSFER);
    tnx->remove(flushingKey);
    tnx->remove(requestTransferKey);
    tnx->set(ownedKey, "");
    if (!tnx->commit())
    {
      const char* msg = "Ownership: commit `_takeOwnership` transaction failed";
      logger->log(LOG_CRIT, msg);
      throw runtime_error(msg);
    }
  }
  mutex.lock();
  ownedPrefixes[p] = true;
  mutex.unlock();
  Synchronizer::get()->newPrefix(p);
  Cache::get()->newPrefix(p);
}

void Ownership::takeOwnership(const bf::path& p)
{
  boost::unique_lock<boost::mutex> s(mutex);

  auto it = ownedPrefixes.find(p);
  if (it != ownedPrefixes.end())
    return;
  ownedPrefixes[p] = NULL;
  s.unlock();

  bool ownedKeyExists;
  {
    auto kvStorage = KVStorageInitializer::getStorageInstance();
    auto tnx = kvStorage->createTransaction();
    const std::string ownedKey = getKeyName(p.string(), OwnershipStateId::OWNED);
    ownedKeyExists = tnx->get(ownedKey).first;
  }

  // if it's not already owned, then we can take possession
  if (!ownedKeyExists)
  {
    _takeOwnership(p);
    return;
  }

  {
    auto kvStorage = KVStorageInitializer::getStorageInstance();
    auto tnx = kvStorage->createTransaction();
    const std::string requestTransferKey = getKeyName(p.string(), OwnershipStateId::REQUEST_TRANSFER);
    tnx->set(requestTransferKey, "");
    if (!tnx->commit())
    {
      const char* msg = "Ownership: commit `requestTransfer` failed ";
      logger->log(LOG_CRIT, msg);
      throw runtime_error(msg);
    }
  }

  bool okToTransfer = false;
  time_t lastFlushTime = time(NULL);
  while (!okToTransfer && time(NULL) < lastFlushTime + 10)
  {
    // if the OWNED file is deleted or if the flushing file isn't touched after 10 secs
    // it is ok to take possession.
    bool ownedKeyExists;
    {
      auto kvStorage = KVStorageInitializer::getStorageInstance();
      auto tnx = kvStorage->createTransaction();
      const std::string ownedKey = getKeyName(p.string(), OwnershipStateId::OWNED);
      ownedKeyExists = tnx->get(ownedKey).first;
    }
    if (!ownedKeyExists)
      okToTransfer = true;

    bool flushingKeyExists;
    {
      auto kvStorage = KVStorageInitializer::getStorageInstance();
      auto tnx = kvStorage->createTransaction();
      const std::string flushingKey = getKeyName(p.string(), OwnershipStateId::FLUSHING);
      flushingKeyExists = tnx->get(flushingKey).first;
    }
    if (flushingKeyExists)
    {
      logger->log(LOG_DEBUG, "Ownership: waiting to get %s", p.string().c_str());
      // Since notice the flushing key.
      lastFlushTime = time(NULL);
    }
    if (!okToTransfer)
      sleep(1);
  }
  _takeOwnership(p);
}

Ownership::Monitor::Monitor(Ownership* _owner) : owner(_owner), stop(false)
{
  thread = boost::thread([this] { this->watchForInterlopers(); });
}

Ownership::Monitor::~Monitor()
{
  stop = true;
  thread.interrupt();
  thread.join();
}

void Ownership::Monitor::watchForInterlopers()
{
  // look for requests to transfer ownership
  vector<bf::path> releaseList;

  while (!stop)
  {
    releaseList.clear();
    boost::unique_lock<boost::mutex> s(owner->mutex);

    for (auto& prefix : owner->ownedPrefixes)
    {
      if (stop)
        break;
      if (prefix.second == false)
        continue;

      bool requestKeyExists;
      {
        auto kvStorage = KVStorageInitializer::getStorageInstance();
        auto tnx = kvStorage->createTransaction();
        const std::string requestTransferKey =
            getKeyName(prefix.first.string(), OwnershipStateId::REQUEST_TRANSFER);

        requestKeyExists = tnx->get(requestTransferKey).first;
      }
      // release it if there's a release request only.  Log it if there's an error other than
      // that the file isn't there.
      if (requestKeyExists)
        releaseList.push_back(prefix.first);
    }
    s.unlock();

    for (auto& prefix : releaseList)
      owner->releaseOwnership(prefix);
    if (stop)
      break;
    try
    {
      boost::this_thread::sleep_for(boost::chrono::seconds(1));
    }
    catch (boost::thread_interrupted&)
    {
    }
  }
}
}  // namespace storagemanager
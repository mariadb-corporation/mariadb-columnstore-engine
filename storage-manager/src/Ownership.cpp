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

#include "Ownership.h"
#include "Config.h"
#include "Cache.h"
#include "Synchronizer.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <boost/filesystem.hpp>

using namespace std;
namespace bf = boost::filesystem;

namespace storagemanager
{
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

// minor timesaver
#define TOUCH(p, f)                                                                                       \
  {                                                                                                       \
    int fd = ::open((metadataPrefix / p / f).string().c_str(), O_TRUNC | O_CREAT | O_WRONLY, 0660);       \
    if (fd >= 0)                                                                                          \
      ::close(fd);                                                                                        \
    else                                                                                                  \
    {                                                                                                     \
      char buf[80];                                                                                       \
      int saved_errno = errno;                                                                            \
      cerr << "failed to touch " << metadataPrefix / p / f << " got " << strerror_r(saved_errno, buf, 80) \
           << endl;                                                                                       \
    }                                                                                                     \
  }

#define DELETE(p, f) ::unlink((metadataPrefix / p / f).string().c_str());

void Ownership::touchFlushing(const bf::path& prefix, volatile bool* doneFlushing) const
{
  while (!*doneFlushing)
  {
    TOUCH(prefix, "FLUSHING");
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
    // This is a quick release.  If this is being destroyed, then it is through the graceful
    // shutdown mechanism, which will flush data separately.
    DELETE(p, "OWNED");
    DELETE(p, "FLUSHING");
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

  // update state
  DELETE(p, "OWNED");
  DELETE(p, "FLUSHING");
}

void Ownership::_takeOwnership(const bf::path& p)
{
  logger->log(LOG_DEBUG, "Ownership: taking ownership of %s", p.string().c_str());
  DELETE(p, "FLUSHING");
  DELETE(p, "REQUEST_TRANSFER");
  // TODO: need to consider errors taking ownership
  TOUCH(p, "OWNED");
  mutex.lock();
  ownedPrefixes[p] = true;
  mutex.unlock();
  Synchronizer::get()->newPrefix(p);
  Cache::get()->newPrefix(p);
}

void Ownership::takeOwnership(const bf::path& p)
{
  // If the prefix doesn't exist, ownership isn't possible yet.
  if (!bf::is_directory(metadataPrefix / p))
    return;

  boost::unique_lock<boost::mutex> s(mutex);

  auto it = ownedPrefixes.find(p);
  if (it != ownedPrefixes.end())
    return;
  ownedPrefixes[p] = NULL;
  s.unlock();

  bool okToTransfer = false;
  struct stat statbuf;
  int err;
  char buf[80];
  bf::path ownedPath = metadataPrefix / p / "OWNED";
  bf::path flushingPath = metadataPrefix / p / "FLUSHING";

  // if it's not already owned, then we can take possession
  err = ::stat(ownedPath.string().c_str(), &statbuf);
  if (err && errno == ENOENT)
  {
    _takeOwnership(p);
    return;
  }

  TOUCH(p, "REQUEST_TRANSFER");
  time_t lastFlushTime = time(NULL);
  while (!okToTransfer && time(NULL) < lastFlushTime + 10)
  {
    // if the OWNED file is deleted or if the flushing file isn't touched after 10 secs
    // it is ok to take possession.
    err = ::stat(ownedPath.string().c_str(), &statbuf);
    if (err)
    {
      if (errno == ENOENT)
        okToTransfer = true;
      else
        logger->log(LOG_CRIT, "Ownership::takeOwnership(): got '%s' doing stat of %s",
                    strerror_r(errno, buf, 80), ownedPath.string().c_str());
    }
    err = ::stat(flushingPath.string().c_str(), &statbuf);
    if (err && errno != ENOENT)
      logger->log(LOG_CRIT, "Ownership::takeOwnership(): got '%s' doing stat of %s",
                  strerror_r(errno, buf, 80), flushingPath.string().c_str());
    else
    {
      logger->log(LOG_DEBUG, "Ownership: waiting to get %s", p.string().c_str());
      if (!err)
        lastFlushTime = statbuf.st_mtime;
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
  struct stat statbuf;
  int err;
  char buf[80];
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
      bf::path p(owner->metadataPrefix / (prefix.first) / "REQUEST_TRANSFER");
      const char* cp = p.string().c_str();

      err = ::stat(cp, &statbuf);
      // release it if there's a release request only.  Log it if there's an error other than
      // that the file isn't there.
      if (err == 0)
        releaseList.push_back(prefix.first);
      if (err < 0 && errno != ENOENT)
        owner->logger->log(LOG_ERR, "Runner::watchForInterlopers(): failed to stat %s, got %s", cp,
                           strerror_r(errno, buf, 80));
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

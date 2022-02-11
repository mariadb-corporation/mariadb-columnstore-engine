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

#ifndef DOWNLOADER_H_
#define DOWNLOADER_H_

#include "ThreadPool.h"
#include "CloudStorage.h"
#include "SMLogging.h"
#include "Config.h"

#include <unordered_set>
#include <vector>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/filesystem/path.hpp>

namespace storagemanager
{
class Downloader : public ConfigListener
{
 public:
  Downloader();
  virtual ~Downloader();

  // caller owns the memory for the strings.
  // errors are reported through errnos
  void download(const std::vector<const std::string*>& keys, std::vector<int>* errnos,
                std::vector<size_t>* sizes, const boost::filesystem::path& prefix, boost::mutex* lock);
  bool inProgress(const std::string&);  // call this holding the cache's lock
  const boost::filesystem::path& getTmpPath() const;

  void printKPIs() const;

  virtual void configListener() override;

 private:
  uint maxDownloads;
  // boost::filesystem::path downloadPath;
  boost::mutex lock;

  class DownloadListener
  {
   public:
    DownloadListener(uint* counter, boost::condition* condvar);
    void downloadFinished();

   private:
    uint* counter;
    boost::condition* cond;
  };

  /* Possible optimization.  Downloads used to use pointers to strings to avoid an extra copy.
     Out of paranoid during debugging, I made it copy the strings instead for a clearer lifecycle.
     However, it _should_ be safe to do.
  */
  struct Download : public ThreadPool::Job
  {
    Download(const std::string& source, const boost::filesystem::path& _dlPath, boost::mutex*, Downloader*);
    Download(const std::string& source);
    virtual ~Download();
    void operator()();
    boost::filesystem::path dlPath;
    const std::string key;
    int dl_errno;  // to propagate errors from the download job to the caller
    size_t size;
    boost::mutex* lock;
    bool finished, itRan;
    Downloader* dl;
    std::vector<DownloadListener*> listeners;
  };

  struct DLHasher
  {
    size_t operator()(const boost::shared_ptr<Download>& d) const;
  };

  struct DLEquals
  {
    bool operator()(const boost::shared_ptr<Download>& d1, const boost::shared_ptr<Download>& d2) const;
  };

  typedef std::unordered_set<boost::shared_ptr<Download>, DLHasher, DLEquals> Downloads_t;
  Downloads_t downloads;
  boost::filesystem::path tmpPath;

  ThreadPool workers;
  CloudStorage* storage;
  SMLogging* logger;

  // KPIs
  size_t bytesDownloaded;
};

}  // namespace storagemanager

#endif

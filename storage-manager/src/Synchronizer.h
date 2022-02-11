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

#ifndef SYNCHRONIZER_H_
#define SYNCHRONIZER_H_

#include <string>
#include <map>
#include <deque>
#include <boost/utility.hpp>
#include <boost/filesystem.hpp>
#include <boost/chrono.hpp>
#include <boost/scoped_ptr.hpp>

#include "SMLogging.h"
#include "Replicator.h"
#include "ThreadPool.h"
#include "CloudStorage.h"
#include "Config.h"

namespace storagemanager
{
class Cache;  // break circular dependency in header files
class IOCoordinator;

class Synchronizer : public boost::noncopyable, public ConfigListener
{
 public:
  static Synchronizer* get();
  virtual ~Synchronizer();

  // these take keys as parameters, not full path names, ex, pass in '12345' not
  // 'cache/12345'.
  void newJournalEntry(const boost::filesystem::path& firstDir, const std::string& key, size_t len);
  void newJournalEntries(const boost::filesystem::path& firstDir,
                         const std::vector<std::pair<std::string, size_t> >& keys);
  void newObjects(const boost::filesystem::path& firstDir, const std::vector<std::string>& keys);
  void deletedObjects(const boost::filesystem::path& firstDir, const std::vector<std::string>& keys);
  void flushObject(const boost::filesystem::path& firstDir, const std::string& key);
  void forceFlush();  // ideally, make a version of this that takes a firstDir parameter
  void syncNow();     // synchronous version of force for SyncTask

  void newPrefix(const boost::filesystem::path& p);
  void dropPrefix(const boost::filesystem::path& p);

  // for testing primarily
  boost::filesystem::path getJournalPath();
  boost::filesystem::path getCachePath();
  void printKPIs() const;

  virtual void configListener() override;

 private:
  Synchronizer();

  void _newJournalEntry(const boost::filesystem::path& firstDir, const std::string& key, size_t len);
  void process(std::list<std::string>::iterator key);
  void synchronize(const std::string& sourceFile, std::list<std::string>::iterator& it);
  void synchronizeDelete(const std::string& sourceFile, std::list<std::string>::iterator& it);
  void synchronizeWithJournal(const std::string& sourceFile, std::list<std::string>::iterator& it);
  void rename(const std::string& oldkey, const std::string& newkey);
  void makeJob(const std::string& key);

  // this struct kind of got sloppy.  Need to clean it at some point.
  struct PendingOps
  {
    PendingOps(int flags);
    ~PendingOps();
    int opFlags;
    int waiters;
    bool finished;
    boost::condition condvar;
    void wait(boost::mutex*);
    void notify();
  };

  struct Job : public ThreadPool::Job
  {
    virtual ~Job(){};
    Job(Synchronizer* s, std::list<std::string>::iterator i);
    void operator()();
    Synchronizer* sync;
    std::list<std::string>::iterator it;
  };

  uint maxUploads;
  boost::scoped_ptr<ThreadPool> threadPool;
  std::map<std::string, boost::shared_ptr<PendingOps> > pendingOps;
  std::map<std::string, boost::shared_ptr<PendingOps> > opsInProgress;

  // this is a bit of a kludge to handle objects being renamed.  Jobs can be issued
  // against name1, but when the job starts running, the target may be name2.
  // some consolidation should be possible between this and the two maps above, tbd.
  // in general the code got kludgier b/c of renaming, needs a cleanup pass.
  std::list<std::string> objNames;

  // this thread will start jobs for entries in pendingOps every 10 seconds
  bool die;
  boost::thread syncThread;
  const boost::chrono::seconds syncInterval = boost::chrono::seconds(10);
  void periodicSync();
  std::map<boost::filesystem::path, size_t> uncommittedJournalSize;
  size_t journalSizeThreshold;
  bool blockNewJobs;

  void syncNow(const boost::filesystem::path& prefix);  // a synchronous version of forceFlush()

  // some KPIs
  size_t numBytesRead, numBytesWritten, numBytesUploaded, numBytesDownloaded, flushesTriggeredBySize,
      flushesTriggeredByTimer, journalsMerged, objectsSyncedWithNoJournal, bytesReadBySync,
      bytesReadBySyncWithJournal;
  ssize_t mergeDiff;

  SMLogging* logger;
  Cache* cache;
  Replicator* replicator;
  IOCoordinator* ioc;
  CloudStorage* cs;

  boost::filesystem::path cachePath;
  boost::filesystem::path journalPath;
  boost::mutex mutex;
};

}  // namespace storagemanager
#endif

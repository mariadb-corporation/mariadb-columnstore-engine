
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

namespace storagemanager
{

class Cache;  // break circular dependency in header files
class IOCoordinator;

class Synchronizer : public boost::noncopyable
{
    public:
        static Synchronizer *get();
        virtual ~Synchronizer();
        
        // these take keys as parameters, not full path names, ex, pass in '12345' not
        // 'cache/12345'.
        void newJournalEntry(const boost::filesystem::path &firstDir, const std::string &key, size_t len);
        void newJournalEntries(const boost::filesystem::path &firstDir, const std::vector<std::pair<std::string, size_t> > &keys);
        void newObjects(const boost::filesystem::path &firstDir, const std::vector<std::string> &keys);
        void deletedObjects(const boost::filesystem::path &firstDir, const std::vector<std::string> &keys);        
        void flushObject(const boost::filesystem::path &firstDir, const std::string &key);
        void forceFlush();  // ideally, make a version of this that takes a firstDir parameter
        
        
        void newPrefix(const boost::filesystem::path &p);
        void dropPrefix(const boost::filesystem::path &p);
        
        // for testing primarily
        boost::filesystem::path getJournalPath();
        boost::filesystem::path getCachePath();
    private:
        Synchronizer();
        
        void _newJournalEntry(const boost::filesystem::path &firstDir, const std::string &key, size_t len);
        void process(std::list<std::string>::iterator key);
        void synchronize(const std::string &sourceFile, std::list<std::string>::iterator &it);
        void synchronizeDelete(const std::string &sourceFile, std::list<std::string>::iterator &it);
        void synchronizeWithJournal(const std::string &sourceFile, std::list<std::string>::iterator &it);
        void rename(const std::string &oldkey, const std::string &newkey);
        void makeJob(const std::string &key);
        
        // this struct kind of got sloppy.  Need to clean it at some point.
        struct PendingOps
        {
            PendingOps(int flags);
            ~PendingOps();
            int opFlags;
            int waiters;
            bool finished;
            boost::condition condvar;
            void wait(boost::mutex *);
            void notify();
        };
        
        struct Job : public ThreadPool::Job
        {
            Job(Synchronizer *s, std::list<std::string>::iterator i);
            void operator()();
            Synchronizer *sync;
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
        
        void syncNow(const boost::filesystem::path &prefix);  // a synchronous version of forceFlush()
        
        SMLogging *logger;
        Cache *cache;
        Replicator *replicator;
        IOCoordinator *ioc;
        CloudStorage *cs;
        
        boost::filesystem::path cachePath;
        boost::filesystem::path journalPath;
        boost::mutex mutex;
};

}
#endif


#ifndef SYNCHRONIZER_H_
#define SYNCHRONIZER_H_

#include <string>
#include <map>
#include <deque>
#include <boost/utility.hpp>
#include <boost/filesystem.hpp>

#include "SMLogging.h"
#include "Cache.h"
#include "Replicator.h"
#include "IOCoordinator.h"
#include "ThreadPool.h"

namespace storagemanager
{

class Cache;

/* TODO: Need to think about how errors are handled / propagated */
class Synchronizer : public boost::noncopyable
{
    public:
        static Synchronizer *get();
        virtual ~Synchronizer();
        
        // these take keys as parameters, not full path names, ex, pass in '12345' not
        // 'cache/12345.obj'.
        void newJournalEntry(const std::string &key);
        void newObjects(const std::vector<std::string> &keys);
        void deletedObjects(const std::vector<std::string> &keys);        
        void flushObject(const std::string &key);
        
        // for testing primarily
        boost::filesystem::path getJournalPath();
        boost::filesystem::path getCachePath();
    private:
        Synchronizer();
        
        void process(std::list<std::string>::iterator key, bool use_lock=true);
        void synchronize(const std::string &sourceFile, std::list<std::string>::iterator &it);
        void synchronizeDelete(const std::string &sourceFile, std::list<std::string>::iterator &it);
        void synchronizeWithJournal(const std::string &sourceFile, std::list<std::string>::iterator &it);
        void rename(const std::string &oldkey, const std::string &newkey);
        void makeJob(const std::string &key);
        
        // this struct kind of got sloppy.  Need to clean it at some point.
        struct PendingOps
        {
            PendingOps(int flags);
            int opFlags;
            bool finished;
            boost::condition condvar;
            void wait(boost::mutex *);
            void notify(boost::mutex *);
        };
        
        struct Job : public ThreadPool::Job
        {
            Job(Synchronizer *s, std::list<std::string>::iterator i) : sync(s), it(i) { }
            void operator()() { sync->process(it); }
            Synchronizer *sync;
            std::list<std::string>::iterator it;
        };
        
        uint maxUploads;
        ThreadPool threadPool;
        std::map<std::string, boost::shared_ptr<PendingOps> > pendingOps;
        std::map<std::string, boost::shared_ptr<PendingOps> > opsInProgress;
        
        // this is a bit of a kludge to handle objects being renamed.  Jobs can be issued
        // against name1, but when the job starts running, the target may be name2.
        // some consolidation should be possible between this and the two maps above, tbd.
        // in general the code got kludgier b/c of renaming, needs a cleanup pass.
        std::list<std::string> objNames;
        
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

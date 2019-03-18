
#ifndef SYNCHRONIZER_H_
#define SYNCHRONIZER_H_

#include <string>
#include <map>
#include <deque>
#include <boost/utility.hpp>

#include "SMLogging.h"
#include "Cache.h"
#include "Replicator.h"
#include "IOCoordinator.h"
#include "ThreadPool.h"

namespace storagemanager
{

/* TODO: Need to think about how errors are handled / propagated */
class Synchronizer : public boost::noncopyable
{
    public:
        static Synchronizer *get();
        virtual ~Synchronizer();
        
        void newJournalEntry(const std::string &key);
        void newObjects(const std::vector<std::string> &keys);
        void deletedObjects(const std::vector<std::string> &keys);        
        void flushObject(const std::string &key);
        
    private:
        Synchronizer();
        
        void process(const std::string &key);
        void synchronize(const std::string &key, bool isFlush);
        void synchronizeDelete(const std::string &key);
        void synchronizeWithJournal(const std::string &key, bool isFlush);
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
            Job(Synchronizer *s, const std::string &k) : sync(s), key(k) { }
            void operator()() { sync->process(key); }
            Synchronizer *sync;
            std::string key;
        };
        
        ThreadPool threadPool;
        std::map<std::string, boost::shared_ptr<PendingOps> > pendingOps;
        std::map<std::string, boost::shared_ptr<PendingOps> > opsInProgress;
        SMLogging *logger;
        Cache *cache;
        Replicator *replicator;
        IOCoordinator *ioc;
        boost::mutex mutex;
};

}
#endif

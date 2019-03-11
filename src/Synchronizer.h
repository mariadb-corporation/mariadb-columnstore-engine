
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
        
        struct FlushListener
        {
            FlushListener(boost::mutex *m, boost::condvar *c);
            boost::mutex *mutex;
            boost::condition *condvar;
            void flushed();
        }
        
        struct PendingOps
        {
            PendingOps(int flags, std::list<std::string>::iterator pos);
            int opFlags;
            bool finished;
            std::list<std::string>::iterator queueEntry;
            boost::condition condvar;
            void wait();
            void notify();
        };
        
        ThreadPool threadPool;
        std::map<std::string, boost::shared_ptr<PendingOps> > pendingOps;
        std::list<std::string> workQueue;
        SMLogging *logger;
        Cache *cache;
        Replicator *replicator;
        IOCoordinator *ioc;
        boost::mutex mutex;
};

}
#endif

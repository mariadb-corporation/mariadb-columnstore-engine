
#include "Synchronizer.h"
#include <boost/thread/mutex.hpp>

using namespace std;

namespace
{
    storagemanager::Synchronizer *instance = NULL;
    boost::mutex inst_mutex;
}

namespace storagemanager
{

Synchronizer * Synchronizer::get()
{
    if (instance)
        return instance;
    boost::unique_lock<boost::mutex> lock(inst_mutex);
    if (instance)
        return instance;
    instance = new Synchronizer();
    return instance;
}

Synchronizer::Synchronizer() : maxUploads(0)
{
    Config *config = Config::get();
    logger = SMLogging::get();
    cache = Cache::get();
    replicator = Replicator::get();
    ioc = IOCoordinator::get();
    
    string stmp = config->getValue("ObjectStorage", "max_concurrent_uploads")
    try
    {
        maxUploads = stoul(stmp);
    }
    catch(invalid_argument)
    {
        logger->log(LOG_WARNING, "Downloader: Invalid arg for ObjectStorage/max_concurrent_uploads, using default of 20");
    }
    if (maxUploads == 0)
        maxUploads = 20;
    threadPool.setMaxThreads(maxUploads);
}

Synchronizer::~Synchronizer()
{
    /* should this wait until all pending work is done,
        or save the list it's working on.....
        For milestone 2, this will do the safe thing and finish working first.
        Later we can get fancy. */
}

enum OpFlags
{
    NOOP = 0,
    JOURNAL = 0x1,
    DELETE = 0x2,
    NEW_OBJECT = 0x4,
    IN_PROGRESS = 0x8,
    IS_FLUSH = 0x10
};

void Synchronizer::newJournalEntry(const string &key)
{
    boost::unique_lock<boost::mutex> s(mutex);
    
    auto it = pendingOps.find(key);
    if (it != pendingOps.end())
    {
        it->second->opFlags |= JOURNAL;
        return;
    }
    workQueue.push_back(key);
    pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(JOURNAL, workQueue.end() - 1));
}

void Synchronizer::newObjects(const vector<string> &keys)
{
    boost::unique_lock<boost::mutex> s(mutex);

    for (string &key : keys)
    {
        assert(pendingOps.find(key) == pendingOps.end());
        workQueue.push_back(key);
        pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(NEW_OBJECT, workQueue.end() - 1));
    }
}

void Synchronizer::deletedObjects(const vector<string> &keys)
{
    boost::unique_lock<boost::mutex> s(mutex);

    auto it = pendingOps.find(key);
    if (it != pendingOps.end())
    {
        it->second->opFlags |= DELETE;
        return;
    }
    workQueue.push_back(key);
    pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(DELETE, workQueue.end() - 1));
    
}

void Synchronizer::flushObject(const string &key)
{
    /*  move the work queue entry for key to the front of the queue / create if not exists
        mark the pending ops as a flush
        wait for the op to finish
    */
    boost::unique_lock<boost::mutex> s(mutex);
    
    auto &it = pendingOps.find(key);
    if (it != pendingOps.end())
    {   
        workQueue.splice(workQueue.begin(), workQueue, it->second.queueEntry);
        it->second->opFlags |= IS_FLUSH;
        it->second->wait();
    }
    else
    {
        workQueue.push_front(key);
        pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(IS_FLUSH, workQueue.begin()));
        pendingOps[key]->wait();
    }
}

void Synchronizer::process(const string &key)
{
    boost::unique_lock<boost::mutex> s(mutex);
    
    auto it = pendingOps.find(key);
    assert(it != pendingOps.end());
    boost::shared_ptr<PendingOps> pending = it->second;
    pendingOps.erase(it);
    s.unlock();
    
    if (pending->opFlags & DELETE)
        synchronizeDelete(key);
    else if (pending->opFlags & JOURNAL)
        synchronizerWithJournal(key, pending->opFlags & IS_FLUSH);
    else if (pending->opFlags & NEW_OBJECT)
        synchronize(key, pending->opFlags & IS_FLUSH);
    else
        // complain
        ;
    
    if (pending->opFlags & IS_FLUSH)
        pending->notify();
}

struct ScopedReadLock
{
    ScopedReadLock(IOCoordinator *i, string &key)
    {
        ioc = i;
        ioc->getReadLock(key);
    }
    ~ScopedReadLock()
    {
        ioc->releaseReadLock(key);
    }
    IOCoordinator *ioc;
};

struct ScopedWriteLock
{
    ScopedWriteLock(IOCoordinator *i, string &key)
    {
        ioc = i;
        ioc->getWriteLock(key);
    }
    ~ScopedReadLock()
    {
        ioc->releaseWriteLock(key);
    }
    IOCoordinator *ioc;
};

void Synchronizer::synchronize(const std::string &key, bool isFlush)
{
    ScopedReadLock s(ioc, key);
    
    bool exists = false;
    cs->exists(key, &exists);
    if (!exists)
    {
        cs->putObject(cache->getCachePath() / key, key);
        replicator->delete(key, Replicator::NO_LOCAL);
    }
    if (isFlush)
        replicator->delete(key, Replicator::LOCAL_ONLY);
}

void Synchronizer::synchronizeWithJournal(const string &key, bool isFlush)
{
    

    


/* The helper objects & fcns */

Synchronizer::PendingOps(int flags, list<string>::iterator pos) : opFlags(flags), finished(false), queueEntry(pos)
{
}

Synchronizer::PendingOps::notify()
{
    boost::unique_lock<boost::mutex> s(mutex);
    finished = true;
    condvar.notify_all();
}

Synchronizer::PendingOps::wait()
{
    while (!finished)
        condvar.wait(mutex);
}

}

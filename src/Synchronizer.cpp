
#include "Synchronizer.h"
#include <boost/thread/mutex.hpp>

using namespace std;

namespace
{
    storagemanager::Synchronizer *instance = NULL;
    boost::mutex inst_mutex;
}

namespace bf = boost::filesystem;
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
    
    try {
        if (pending->opFlags & DELETE)
            synchronizeDelete(key);
        else if (pending->opFlags & JOURNAL)
            synchronizerWithJournal(key, pending->opFlags & IS_FLUSH);
        else if (pending->opFlags & NEW_OBJECT)
            synchronize(key, pending->opFlags & IS_FLUSH);
        else
            throw logic_error("Synchronizer::process(): got an unknown op flag");
    }
    catch(exception &e) {
        logger->log(LOG_CRIT, "Synchronizer::process(): error sync'ing %s opFlags=%d, got '%s'.  Requeueing it.", key.c_str(),
            pending->opFlags, e.what());
        s.lock();
        workQueue.push_back(key);
        pendingOps[key] = pending;
        return;
    }
    if (pending->opFlags & IS_FLUSH)
        pending->notify();
}

struct ScopedReadLock
{
    ScopedReadLock(IOCoordinator *i, string &key)
    {
        ioc = i;
        ioc->readLock(key.c_str());
    }
    ~ScopedReadLock()
    
        ioc->readUnlock(key.c_str());
    }
    IOCoordinator *ioc;
};

struct ScopedWriteLock
{
    ScopedWriteLock(IOCoordinator *i, string &key)
    {
        ioc = i;
        ioc->writeLock(key.c_str());
        locked = true;
    }
    ~ScopedReadLock()
    {
        if (locked)
            ioc->writeUnlock(key.c_str());
    }
    
    void unlock()
    {
        if (locked)
        {
            ioc->writeUnlock(key.c_str());
            locked = false;
        }
    }
    IOCoordinator *ioc;
    bool locked;
};

void Synchronizer::synchronize(const string &key, bool isFlush)
{
    ScopedReadLock s(ioc, key);
    
    char buf[80];
    bool exists = false;
    int err;
    err = cs->exists(key, &exists);
    if (err)
        throw runtime_error(strerror_r(errno, buf, 80));
    if (!exists)
    {
        err = cs->putObject(cache->getCachePath() / key, key);
        if (err)
            throw runtime_error(strerror_r(errno, buf, 80));
        replicator->delete(key, Replicator::NO_LOCAL);
    }
    if (isFlush)
        replicator->delete(key, Replicator::LOCAL_ONLY);
}

void Synchronizer::synchronizeDelete(const string &key)
{
    /* Right now I think this is being told to delete key from cloud storage, 
       and that it has already been deleted everywhere locally. */
    cs->delete(key);
}

void Synchronizer::synchronizeWithJournal(const string &key, bool isFlush)
{
    // interface to Metadata TBD
    //string sourceFilename = Metadata::getSourceFromKey(key);
    ScopedWriteLock s(ioc, sourceFilename);
    bf::path oldCachePath = cache->getCachePath() / key;
    string journalName = oldCachePath.string() + ".journal";
    int err;
    boost::shared_array<uint8_t> data;
    size_t count = 0, size = 0;
    char buf[80];
    bool oldObjIsCached = cache->exists(key);
    
    // get the base object if it is not already cached
    // merge it with its journal file
    if (!oldObjIsCached)
    {
        err = cs->getObject(key, data, &size);
        if (err)
            throw runtime_error(string("Synchronizer: getObject() failed: ") + strerror_r(errno, buf, 80));
        err = ios->mergeJournalInMem(data, journalName, &size);
        assert(!err);
    }
    else
        data = ios->mergeJournal(oldCachePath.string(), journalName, 0, &size);
    assert(data);
    
    // get a new key for the resolved version & upload it
    string newKey = ios->newKeyFromOldKey(key);
    err = cs->putObject(data, size, newKey);
    if (err)
        throw runtime_error(string("Synchronizer: putObject() failed: ") + strerror_r(errno, buf, 80));
    
    // if this isn't a flush operation..
    //   write the new data to disk,
    //   tell the cache about the rename
    //   rename the file in any pending ops in Synchronizer
    
    if (!isFlush && oldObjIsCached)
    {
        // Is this the only thing outside of Replicator that writes files?
        // If so move this write loop to Replicator.
        bf::path newCachePath = cache->getCachePath() / newKey;
        int newFD = ::open(newCachePath.string().c_str(), O_WRONLY, 0600);
        if (newFD < 0)
            throw runtime_error(string("Synchronizer: Failed to open a new object in local storage!  Got ") 
              + strerror_r(errno, buf, 80));
        ScopedCloser s(newFD);
        
        while (count < size)
        {
            err = ::write(newFD, data.get(), size - count);
            if (err < 0)
                throw runtime_error(string("Synchronizer: Failed to write to a new object in local storage!  Got ") 
                  + strerror_r(errno, buf, 80));
            count += err;
        }
        
        // the total size difference is the new obj size - (old obj size + journal size)
        // might be wise to add things like getting a file size to a utility class.
        // TBD how often we need to do it.
        struct stat statbuf;
        err = stat(oldCachePath.string().c_str(), &statbuf);
        assert(!err);
        size_t oldObjSize = statbuf.st_size;
        err = stat((oldCachePath.string() + ".journal").c_str(), &statbuf);
        assert(!err);
        size_t journalSize = statbuf.st_size;
        
        cache->rename(key, newKey, size - oldObjSize - journalSize);
        rename(key, newKey);
    }
    
    // update the metadata for the source file
    // waiting for stubs to see what these calls look like
    /*
    Metadata md(sourceFilename);
    md.rename(key, newKey);
    replicator->updateMetadata(sourceFilename, md);
    */
    
    s.unlock();
    
    // delete the old object & journal file
    vector<string> files;
    files.push_back(key);
    files.push_back(journalName);
    replicator->delete(files);
    cs->delete(key);
}


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

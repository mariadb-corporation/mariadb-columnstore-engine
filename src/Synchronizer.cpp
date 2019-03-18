
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
    makeJob(key);
    pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(JOURNAL));
}

void Synchronizer::newObjects(const vector<string> &keys)
{
    boost::unique_lock<boost::mutex> s(mutex);

    for (string &key : keys)
    {
        assert(pendingOps.find(key) == pendingOps.end());
        makeJob(key);
        pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(NEW_OBJECT));
    }
}

void Synchronizer::deletedObjects(const vector<string> &keys)
{
    boost::unique_lock<boost::mutex> s(mutex);

    for (string &key : keys)
    {
        auto it = pendingOps.find(key);
        if (it != pendingOps.end())
        {
            it->second->opFlags |= DELETE;
            return;
        }
        makeJob(key);
        pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(DELETE));
    }
}

void Synchronizer::flushObject(const string &key)
{
    process(key);
}

void Synchronizer::makeJob(const string &key)
{
    boost::shared_ptr<Job> j(new Job(this, key));
    threadPool.addJob(j);
}

void Synchronizer::process(const string &key)
{
    boost::unique_lock<boost::mutex> s(mutex);
    
    auto op = opsInProgress.find(key);
    // it's already in progress
    if (op != opsInProgress.end())
    {
        op->second->wait(&mutex);
        return;
    }
    auto it = pendingOps.find(key);
    // no work to be done on this key
    if (it == pendingOps.end())
        return;
    
    boost::shared_ptr<PendingOps> pending = it->second;
    opsInProgress[key] = *it;
    pendingOps.erase(it);
    s.unlock();
    
    bool success = false;
    while (!success)
    {
        try {
            if (pending->opFlags & DELETE)
                synchronizeDelete(key);
            else if (pending->opFlags & JOURNAL)
                synchronizerWithJournal(key);
            else if (pending->opFlags & NEW_OBJECT)
                synchronize(key);
            else
                throw logic_error("Synchronizer::process(): got an unknown op flag");
            pending->notify(&mutex);
            success = true;
        }
        catch(exception &e) {
            logger->log(LOG_CRIT, "Synchronizer::process(): error sync'ing %s opFlags=%d, got '%s'.  Requeueing it.", key.c_str(),
                pending->opFlags, e.what());
            sleep(1);
        }
    }
    // TBD:  On a network outage or S3 outage, it might not be a bad idea to keep retrying
    // until the end of time.  This will (?) naturally make the system unusable until the blockage
    // is cleared, which is what we want, right?  Is there a way to nicely tell the user what
    // is happening, or are the logs good enough?
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

void Synchronizer::synchronize(const string &key)
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
}

void Synchronizer::synchronizeDelete(const string &key)
{
    /* Right now I think this is being told to delete key from cloud storage, 
       and that it has already been deleted everywhere locally. If that's not right,
       we'll have to add some sync around this. */
    cs->delete(key);
}

void Synchronizer::synchronizeWithJournal(const string &key)
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
    
    // if the object was cached...
    //   write the new data to disk,
    //   tell the cache about the rename
    //   rename the file in any pending ops in Synchronizer
    
    if (oldObjIsCached)
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
        
        // might be wise to add things like getting a file size to a utility class.
        // TBD how often we need to do it.
        struct stat statbuf;
        err = stat(oldCachePath.string().c_str(), &statbuf);
        assert(!err);
        cache->rename(key, newKey, size - statbuf.st_size);
        rename(key, newKey);
        replicator->delete(key);
    }
    
    // update the metadata for the source file
    // waiting for stubs to see what these calls look like
    /*
    Metadata md(sourceFilename);
    md.rename(key, newKey);
    replicator->updateMetadata(sourceFilename, md);
    */
    
    ioc->renameObject(oldkey, newkey);   
    s.unlock();
    
    struct stat statbuf;
    err = stat(journalName.string().c_str(), &statbuf);
    assert(!err);
    
    // delete the old object & journal file
    replicator->delete(journalName);
    cache->deletedJournal(statbuf.st_size);
    cs->delete(key);
}

void Synchronizer::rename(const string &oldKey, const string &newKey)
{
    boost::unique_lock<boost::mutex> s(mutex);

    auto it = pendingOps.find(oldKey);
    if (it == pendingOps.end())
        return;
    pendingOps.erase(it);
    pendingOps[newKey] = it->second;
}

/* The helper objects & fcns */

Synchronizer::PendingOps(int flags) : opFlags(flags), finished(false)
{
}

Synchronizer::~PendingOps()
{
}

Synchronizer::PendingOps::notify(boost::mutex *m)
{
    boost::unique_lock<boost::mutex> s(*m);
    finished = true;
    condvar.notify_all();
}

Synchronizer::PendingOps::wait(boost::mutex *m)
{
    while (!finished)
        condvar.wait(*m);
}

}

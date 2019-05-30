
#include "Synchronizer.h"
#include "Cache.h"
#include "IOCoordinator.h"
#include "MetadataFile.h"
#include "Utilities.h"
#include <boost/thread/mutex.hpp>

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

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
    cs = CloudStorage::get();
    
    string stmp = config->getValue("ObjectStorage", "max_concurrent_uploads");
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
        
    journalPath = cache->getJournalPath();    
    cachePath = cache->getCachePath();
    threadPool.setMaxThreads(maxUploads);
    die = false;
    syncThread = boost::thread([this] () { this->periodicSync(); });
}

Synchronizer::~Synchronizer()
{
    /* should this wait until all pending work is done,
        or save the list it's working on.....
        For milestone 2, this will do the safe thing and finish working first.
        Later we can get fancy. */
    boost::unique_lock<boost::mutex> lock(mutex);
    die = true;
    syncThread.interrupt();
    lock.unlock();
    syncThread.join();
}

enum OpFlags
{
    NOOP = 0,
    JOURNAL = 0x1,
    DELETE = 0x2,
    NEW_OBJECT = 0x4,
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
    //makeJob(key);
    pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(JOURNAL));
}

void Synchronizer::newObjects(const vector<string> &keys)
{
    boost::unique_lock<boost::mutex> s(mutex);

    for (const string &key : keys)
    {
        assert(pendingOps.find(key) == pendingOps.end());
        //makeJob(key);
        pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(NEW_OBJECT));
    }
}

void Synchronizer::deletedObjects(const vector<string> &keys)
{
    boost::unique_lock<boost::mutex> s(mutex);

    for (const string &key : keys)
    {
        auto it = pendingOps.find(key);
        if (it != pendingOps.end())
        {
            it->second->opFlags |= DELETE;
            return;
        }
        //makeJob(key);
        pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(DELETE));
    }
    // would be good to signal to the things in opsInProgress that these were deleted.  That would
    // quiet down the logging somewhat.  How to do that efficiently, and w/o gaps or deadlock...
}

void Synchronizer::flushObject(const string &key)
{
    boost::unique_lock<boost::mutex> s(mutex);

    // if there is something to do on key, it should be either in pendingOps or opsInProgress
    // if it is is pending ops, start the job now.  If it is in progress, wait for it to finish.
    // If there are no jobs to do on key, it will verify it exists in cloud storage.
    // The existence check is currently necessary on a startup where the cache is populated but
    // synchronizer isn't.

    bool noExistingJob = false;
    auto it = pendingOps.find(key);
    if (it != pendingOps.end())
    {
        objNames.push_front(key);
        auto nameIt = objNames.begin();
        s.unlock();
        process(nameIt);
        s.lock();
    }
    else
    {
        auto op = opsInProgress.find(key);
        // it's already in progress
        if (op != opsInProgress.end()) {
            boost::shared_ptr<PendingOps> tmp = op->second;
            tmp->wait(&mutex);
        }
        else
        {
            // it's not in either one, trigger existence check
            noExistingJob = true;
        }
    }
    
    if (!noExistingJob)
        return;
        
    // check whether this key is in cloud storage
    bool keyExists, journalExists;
    int err;
    do {
        err = cs->exists(key.c_str(), &keyExists);
        if (err)
        {
            char buf[80];
            logger->log(LOG_CRIT, "Sync::flushObject(): cloud existence check failed, got '%s'", strerror_r(errno, buf, 80));
            sleep(5);
        }
    } while (err);
    journalExists = bf::exists(journalPath/(key + ".journal"));
    
    if (journalExists)
    {
        logger->log(LOG_DEBUG, "Sync::flushObject(): %s has a journal, "
            "and there is no job for it.  Merging & uploading now.", key.c_str());
        pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(JOURNAL));
        objNames.push_front(key);
        auto nameIt = objNames.begin();
        s.unlock();
        process(nameIt);
    }
    else if (!keyExists)
    {
        logger->log(LOG_DEBUG, "Sync::flushObject(): %s does not exist in cloud storage, "
            "and there is no job for it.  Uploading it now.", key.c_str());
        pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(NEW_OBJECT));
        objNames.push_front(key);
        auto nameIt = objNames.begin();
        s.unlock();
        process(nameIt);
    }
}

void Synchronizer::periodicSync()
{
    boost::unique_lock<boost::mutex> lock(mutex);
    while (!die)
    {
        lock.unlock();
        try
        {
            boost::this_thread::sleep_for(syncInterval);
        }
        catch (const boost::thread_interrupted)
        {
            //logger->log(LOG_DEBUG,"Synchronizer Force Flush.");
        }
        lock.lock();
        //cout << "Sync'ing " << pendingOps.size() << " objects" << " queue size is " << 
        //    threadPool.currentQueueSize() << endl;
        for (auto &job : pendingOps)
            makeJob(job.first);
    }
}

void Synchronizer::forceFlush()
{
    boost::unique_lock<boost::mutex> lock(mutex);
    syncThread.interrupt();
    lock.unlock();
}

void Synchronizer::makeJob(const string &key)
{
    objNames.push_front(key);
    
    boost::shared_ptr<Job> j(new Job(this, objNames.begin()));
    threadPool.addJob(j);
}

void Synchronizer::process(list<string>::iterator name)
{
    /*
        check if there is a pendingOp for name
        if yes, start processing it
        if no,
            check if there is an ongoing op and block on it
            if not, return
    */

    boost::unique_lock<boost::mutex> s(mutex);
    
    string &key = *name;
    auto it = pendingOps.find(key);
    if (it == pendingOps.end())
    {
        auto op = opsInProgress.find(key);
        // it's already in progress
        if (op != opsInProgress.end())
        {
            boost::shared_ptr<PendingOps> tmp = op->second;
            tmp->wait(&mutex);
            objNames.erase(name);
            return;
        }
        else
        {
            // it's not in pending or opsinprogress, nothing to do
            objNames.erase(name);
            return;
        }
    }
    
    boost::shared_ptr<PendingOps> pending = it->second;
    bool inserted = opsInProgress.insert(*it).second;
    if (!inserted)
    {
        objNames.erase(name);
        return;    // the one in pending will have to wait until the next time to avoid clobbering waiting threads
    }
    string sourceFile = MetadataFile::getSourceFromKey(*name);
    pendingOps.erase(it);
    s.unlock();
    
    bool success = false;
    while (!success)
    {
        assert(!s.owns_lock());
        try {
            // Exceptions should only happen b/c of cloud service errors that can't be retried.
            // This code is intentionally racy to avoid having to grab big locks.
            // In particular, it's possible that by the time synchronize() runs,
            // the file to sync has already been deleted.  When one of these functions
            // encounters a state that doesn't make sense, such as being told to upload a file
            // that doesn't exist, it will return successfully under the assumption that
            // things are working as they should upstream, and a syncDelete() call will be coming
            // shortly.
            if (pending->opFlags & DELETE)
                synchronizeDelete(sourceFile, name);
            else if (pending->opFlags & JOURNAL)
                synchronizeWithJournal(sourceFile, name);
            else if (pending->opFlags & NEW_OBJECT)
                synchronize(sourceFile, name);
            else
                throw logic_error("Synchronizer::process(): got an unknown op flag");
            s.lock();
            pending->notify();
            success = true;
        }
        catch(exception &e) {
            logger->log(LOG_CRIT, "Synchronizer::process(): error sync'ing %s opFlags=%d, got '%s'.  Retrying...", key.c_str(),
                pending->opFlags, e.what());
            success = false;
            sleep(1);
            continue;
            /*  TODO:  Need to think this about this requeue logic again.  The potential problem is that
                there may be threads waiting for this job to finish.  If the insert doesn't happen because
                there is already a job in pendingOps for the same file, then the threads waiting on this
                job never get woken, right??  Or, can that never happen for some reason?
            */
            s.lock();
            auto inserted = pendingOps.insert(pair<string, boost::shared_ptr<PendingOps> >(key, pending));
            if (!inserted.second)
                inserted.first->second->opFlags |= pending->opFlags;
            opsInProgress.erase(key);
            makeJob(key);
            objNames.erase(name);
            return;
        }
    }
        
    opsInProgress.erase(*name);
    objNames.erase(name);
}

void Synchronizer::synchronize(const string &sourceFile, list<string>::iterator &it)
{
    ScopedReadLock s(ioc, sourceFile);
    
    string &key = *it;
    char buf[80];
    bool exists = false;
    int err;
    MetadataFile md(sourceFile.c_str(), MetadataFile::no_create_t());
    
    if (!md.exists())
    {
        logger->log(LOG_DEBUG, "synchronize(): no metadata found for %s.  It must have been deleted.", sourceFile.c_str());
        return;
    }
    
    const metadataObject *mdEntry;
    bool entryExists = md.getEntry(MetadataFile::getOffsetFromKey(key), &mdEntry);
    if (!entryExists)
    {
        logger->log(LOG_DEBUG, "synchronize(): %s does not exist in metadata for %s.  This suggests truncation.", key.c_str(), sourceFile.c_str());
        return;
    }
    assert(key == mdEntry->key);
    
    err = cs->exists(key, &exists);
    if (err)
        throw runtime_error(string("synchronize(): checking existence of ") + key + ", got " + 
            strerror_r(errno, buf, 80));
    if (exists)
        return;

    // TODO: should be safe to check with Cache instead of a file existence check
    exists = bf::exists(cachePath / key);
    if (!exists)
    {
        logger->log(LOG_DEBUG, "synchronize(): was told to upload %s but it does not exist locally", key.c_str());
        return;
    }

    err = cs->putObject((cachePath / key).string(), key);
    if (err)
        throw runtime_error(string("synchronize(): uploading ") + key + ", got " + strerror_r(errno, buf, 80));
    replicator->remove((cachePath/key).string().c_str(), Replicator::NO_LOCAL);
}

void Synchronizer::synchronizeDelete(const string &sourceFile, list<string>::iterator &it)
{
    ScopedWriteLock s(ioc, sourceFile);
    cs->deleteObject(*it);
}

void Synchronizer::synchronizeWithJournal(const string &sourceFile, list<string>::iterator &lit)
{
    ScopedWriteLock s(ioc, sourceFile);
    
    char buf[80];
    string key = *lit;
    MetadataFile md(sourceFile.c_str(), MetadataFile::no_create_t());
    
    if (!md.exists())
    {
        logger->log(LOG_DEBUG, "synchronizeWithJournal(): no metadata found for %s.  It must have been deleted.", sourceFile.c_str());
        return;
    }
    
    const metadataObject *mdEntry;
    bool metaExists = md.getEntry(MetadataFile::getOffsetFromKey(key), &mdEntry);
    if (!metaExists)
    {
        logger->log(LOG_DEBUG, "synchronizeWithJournal(): %s does not exist in metadata for %s.  This suggests truncation.", key.c_str(), sourceFile.c_str());
        return;
    }
    assert(key == mdEntry->key);
    
    bf::path oldCachePath = cachePath / key;
    string journalName = (journalPath/ (key + ".journal")).string();
    
    if (!bf::exists(journalName))
    {
        logger->log(LOG_DEBUG, "synchronizeWithJournal(): no journal file found for %s", key.c_str());
        
        // sanity check + add'l info.  Test whether the object exists in cloud storage.  If so, complain,
        // and run synchronize() instead.
        bool existsOnCloud;
        int err = cs->exists(key, &existsOnCloud);
        if (err)
            throw runtime_error(string("Synchronizer: cs->exists() failed: ") + strerror_r(errno, buf, 80));
        if (!existsOnCloud)
        {
            if (bf::exists(oldCachePath))
            {
                logger->log(LOG_DEBUG, "synchronizeWithJournal(): %s has no journal and does not exist in the cloud, calling "
                    "synchronize() instead.  Need to explain how this happens.", key.c_str());
                s.unlock();
                synchronize(sourceFile, lit);
            }
            else
                logger->log(LOG_DEBUG, "synchronizeWithJournal(): %s has no journal, and does not exist in the cloud or in "
                    " the local cache.  Need to explain how this happens.", key.c_str());
            return;
        }
        else
            logger->log(LOG_DEBUG, "synchronizeWithJournal(): %s has no journal, but it does exist in the cloud. "
                " This indicates that an overlapping syncWithJournal() call handled it properly.", key.c_str());

        return;
    }
    
    int err;
    boost::shared_array<uint8_t> data;
    size_t count = 0, size = mdEntry->length;
    
    bool oldObjIsCached = cache->exists(key);
    
    // get the base object if it is not already cached
    // merge it with its journal file
    if (!oldObjIsCached)
    {
        err = cs->getObject(key, &data, &size);
        if (err)
        {
            if (errno == ENOENT)
            {
                logger->log(LOG_DEBUG, "synchronizeWithJournal(): %s does not exist in cache nor in cloud storage", key.c_str());
                return;
            }
            throw runtime_error(string("Synchronizer: getObject() failed: ") + strerror_r(errno, buf, 80));
        }

        //TODO!!  This sucks.  Need a way to pass in a larger array to cloud storage, and also have it not
        // do any add'l alloc'ing or copying
        if (size < mdEntry->length)
        {
            boost::shared_array<uint8_t> tmp(new uint8_t[mdEntry->length]());
            memcpy(tmp.get(), data.get(), size);
            memset(&tmp[size], 0, mdEntry->length - size);
            data.swap(tmp);
        }
        size = mdEntry->length;    // reset length.  Using the metadata as a source of truth (truncation), not actual file length.
        
        err = ioc->mergeJournalInMem(data, size, journalName.c_str());
        if (err)
        {   
            if (!bf::exists(journalName))
                logger->log(LOG_DEBUG, "synchronizeWithJournal(): journal %s was deleted mid-operation, check locking",
                    journalName.c_str());
            else
                logger->log(LOG_ERR, "synchronizeWithJournal(): unexpected error merging journal for %s", key.c_str());
            return;
        }
    }
    else
    {
        data = ioc->mergeJournal(oldCachePath.string().c_str(), journalName.c_str(), 0, size);
        if (!data)
        {
            if (!bf::exists(journalName))
                logger->log(LOG_DEBUG, "synchronizeWithJournal(): journal %s was deleted mid-operation, check locking",
                    journalName.c_str());
            else
                logger->log(LOG_ERR, "synchronizeWithJournal(): unexpected error merging journal for %s", key.c_str());
            return;
        }
    }
    
    // get a new key for the resolved version & upload it
    string newKey = MetadataFile::getNewKeyFromOldKey(key, size);
    err = cs->putObject(data, size, newKey);
    if (err)
    {
        // try to delete it in cloud storage... unlikely it is there in the first place, and if it is
        // this probably won't work
        int l_errno = errno;
        cs->deleteObject(newKey);    
        throw runtime_error(string("Synchronizer: putObject() failed: ") + strerror_r(l_errno, buf, 80));
    }
    
    // if the object was cached...
    //   write the new data to disk,
    //   tell the cache about the rename
    //   rename the file in any pending ops in Synchronizer
    
    if (oldObjIsCached)
    {
        // Is this the only thing outside of Replicator that writes files?
        // If so move this write loop to Replicator.
        bf::path newCachePath = cachePath / newKey;
        int newFD = ::open(newCachePath.string().c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (newFD < 0)
            throw runtime_error(string("Synchronizer: Failed to open a new object in local storage!  Got ") 
              + strerror_r(errno, buf, 80));
        ScopedCloser scloser(newFD);
        
        while (count < size)
        {
            err = ::write(newFD, data.get(), size - count);
            if (err < 0)
            {
                ::unlink(newCachePath.string().c_str());
                throw runtime_error(string("Synchronizer: Failed to write to a new object in local storage!  Got ") 
                  + strerror_r(errno, buf, 80));
            }
            count += err;
        }
        cache->rename(key, newKey, size - bf::file_size(oldCachePath));
        replicator->remove(oldCachePath);
    }
    
    // update the metadata for the source file
    
    md.updateEntry(MetadataFile::getOffsetFromKey(key), newKey, size);
    replicator->updateMetadata(sourceFile.c_str(), md);

    rename(key, newKey);
    ioc->renameObject(key, newKey);
    
    // delete the old object & journal file
    cache->deletedJournal(bf::file_size(journalName));
    replicator->remove(journalName);
    cs->deleteObject(key);
}

void Synchronizer::rename(const string &oldKey, const string &newKey)
{
    boost::unique_lock<boost::mutex> s(mutex);

    auto it = pendingOps.find(oldKey);
    if (it != pendingOps.end())
    {
        pendingOps[newKey] = it->second;
        pendingOps.erase(it);
    }
    it = opsInProgress.find(oldKey);
    if (it != opsInProgress.end())
    {
        opsInProgress[newKey] = it->second;
        opsInProgress.erase(it);
    }
    
    for (auto &name: objNames)
        if (name == oldKey)
            name = newKey;
}

bf::path Synchronizer::getJournalPath()
{
    return journalPath;
}

bf::path Synchronizer::getCachePath()
{
    return cachePath;
}

/* The helper objects & fcns */

Synchronizer::PendingOps::PendingOps(int flags) : opFlags(flags), waiters(0), finished(false)
{
}

Synchronizer::PendingOps::~PendingOps()
{
    assert(waiters == 0);
}

void Synchronizer::PendingOps::notify()
{
    finished = true;
    condvar.notify_all();
}

void Synchronizer::PendingOps::wait(boost::mutex *m)
{
    while (!finished)
    {
        waiters++;
        condvar.wait(*m);
        waiters--;
    }
}

}

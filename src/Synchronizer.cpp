
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

    for (const string &key : keys)
    {
        assert(pendingOps.find(key) == pendingOps.end());
        makeJob(key);
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
        makeJob(key);
        pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(DELETE));
    }
}

void Synchronizer::flushObject(const string &key)
{
    boost::unique_lock<boost::mutex> s(mutex);

    // if there is something to do on key, it should be in the objNames list
    // and either in pendingOps or opsInProgress.
    // The sanity check at the end was intended for debugging / development
    // I'm inclined to make it permanent.  An existence check on S3 is quick.

    bool noExistingJob = false;
    auto it = pendingOps.find(key);
    if (it != pendingOps.end())
        // find the object name and call process() to start it right away
        for (auto name = objNames.begin(); name != objNames.end(); ++it)
        {
            if (*name == key)
            {
                process(name, false);
                break;
            }
        }
    else
    {
        auto op = opsInProgress.find(key);
        // it's already in progress
        if (op != opsInProgress.end())
            op->second->wait(&mutex);
        else
        {
            // it's not in either one, trigger existence check
            noExistingJob = true;
        }
    }
    
    if (!noExistingJob)
        return;
        
    // check whether this key is in cloud storage
    bool exists;
    int err;
    do {
        err = cs->exists(key.c_str(), &exists);
        if (err)
        {
            char buf[80];
            logger->log(LOG_CRIT, "Sync::flushObject(): cloud existence check failed, got '%s'", strerror_r(errno, buf, 80));
            sleep(5);
        }
    } while (err);
    if (!exists)
    {
        logger->log(LOG_DEBUG, "Sync::flushObject(): %s does not exist in cloud storage, "
            "and there is no job for it.  Uploading it now.", key.c_str());
        pendingOps[key] = boost::shared_ptr<PendingOps>(new PendingOps(NEW_OBJECT));
        objNames.push_front(key);
        process(objNames.begin(), true);
    }
}

void Synchronizer::makeJob(const string &key)
{
    objNames.push_front(key);
    
    boost::shared_ptr<Job> j(new Job(this, objNames.begin()));
    threadPool.addJob(j);
}

void Synchronizer::process(list<string>::iterator name, bool callerHoldsLock)
{
    /*
        check if there is a pendingOp for name
        if yes, start processing it
        if no,
            check if there is an ongoing op and block on it
            if not, return
    */

    // had to use this 'callerHoldsLock' kludge to let flush() start processing a job w/o unlocking first
    // and introducing a race.  It means this fcn should not use its own scoped lock.  It sucks, need to rework it.
    boost::unique_lock<boost::mutex> s(mutex, boost::defer_lock);
    
    if (!callerHoldsLock)
        s.lock();
    
    string &key = *name;
    auto it = pendingOps.find(key);
    if (it == pendingOps.end())
    {
        auto op = opsInProgress.find(key);
        // it's already in progress
        if (op != opsInProgress.end())
        {
            op->second->wait(&mutex);
            return;
        }
        else
            // it's not in pending or opsinprogress, nothing to do
            return;
    }
    
    boost::shared_ptr<PendingOps> pending = it->second;
    opsInProgress[key] = pending;
    pendingOps.erase(it);
    string sourceFile = MetadataFile::getSourceFromKey(*name);
    
    if (!callerHoldsLock)
        s.unlock();
    else
        mutex.unlock();
    
    bool success = false;
    while (!success)
    {    
        try {
            // Exceptions should only happen b/c of cloud service errors that can't be retried.
            // This code is intentionally racy to avoid having to grab big locks.
            // In particular, it's possible that by the time synchronize() runs,
            // the file to sync has already been deleted.  When one of these functions
            // encounters a state that doesn't make sense, such as being told to upload a file
            // that doesn't exist, it will return silently under the assumption that
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
            pending->notify(&mutex);
            success = true;
        }
        catch(exception &e) {
            logger->log(LOG_CRIT, "Synchronizer::process(): error sync'ing %s opFlags=%d, got '%s'.  Retrying...", key.c_str(),
                pending->opFlags, e.what());
            success = false;
            sleep(1);
            // TODO: requeue the job instead of looping infinitely
        }
    }
    
    if (!callerHoldsLock)
        s.lock();
    else
        mutex.lock();
        
    opsInProgress.erase(key);
    objNames.erase(name);
}



void Synchronizer::synchronize(const string &sourceFile, list<string>::iterator &it)
{
    ScopedReadLock s(ioc, sourceFile);
    
    string &key = *it;
    char buf[80];
    bool exists = false;
    int err;
    
    err = cs->exists(key, &exists);
    if (err)
        throw runtime_error(string("synchronize(): checking existence of ") + key + ", got " + 
            strerror_r(errno, buf, 80));
    if (exists)
        return;

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
    /* Don't think it's necessary to lock here.  Sync is being told to delete a file on cloud storage.
       Presumably the caller has removed it from metadata & the cache, so it is no longer referencable.  
    */
    //ScopedWriteLock s(ioc, sourceFile);
    cs->deleteObject(*it);
}

void Synchronizer::synchronizeWithJournal(const string &sourceFile, list<string>::iterator &lit)
{
    ScopedWriteLock s(ioc, sourceFile);
    
    string &key = *lit;
    bf::path oldCachePath = cachePath / key;
    string journalName = (journalPath/ (key + ".journal")).string();
    
    if (!bf::exists(journalName))
    {
        logger->log(LOG_DEBUG, "synchronizeWithJournal(): no journal file found for %s", key.c_str());
        // I don't think this should happen, maybe throw a logic_error here.
        // Revision ^^.  It can happen if the object was deleted after the op was latched but before it runs.
        return;
    }

    int err;
    boost::shared_array<uint8_t> data;
    size_t count = 0, size = 0;
    char buf[80];
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
        err = ioc->mergeJournalInMem(data, &size, journalName.c_str());
        assert(!err);
    }
    else
        data = ioc->mergeJournal(oldCachePath.string().c_str(), journalName.c_str(), 0, &size);
    assert(data);
    
    // get a new key for the resolved version & upload it
    string newKey = MetadataFile::getNewKeyFromOldKey(key, size);
    err = cs->putObject(data, size, newKey);
    if (err)
    {
        // try to delete it in cloud storage... unlikely it is there in the first place, and if it is
        // this probably won't work
        cs->deleteObject(newKey);    
        throw runtime_error(string("Synchronizer: putObject() failed: ") + strerror_r(errno, buf, 80));
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
        ScopedCloser s(newFD);
        
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
        replicator->remove(oldCachePath.string().c_str());
    }
    
    // update the metadata for the source file
    MetadataFile md(sourceFile.c_str());
    md.updateEntry(MetadataFile::getOffsetFromKey(key), newKey, size);
    replicator->updateMetadata(sourceFile.c_str(), md);

    rename(key, newKey);
    ioc->renameObject(key, newKey);   
    s.unlock();
    
    // delete the old object & journal file
    cache->deletedJournal(bf::file_size(journalName));
    replicator->remove(journalName.c_str());
    cs->deleteObject(key);
}

void Synchronizer::rename(const string &oldKey, const string &newKey)
{
    boost::unique_lock<boost::mutex> s(mutex);

    auto it = pendingOps.find(oldKey);
    if (it == pendingOps.end())
        return;
    pendingOps[newKey] = it->second;
    pendingOps.erase(it);
    
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

Synchronizer::PendingOps::PendingOps(int flags) : opFlags(flags), finished(false)
{
}

void Synchronizer::PendingOps::notify(boost::mutex *m)
{
    boost::unique_lock<boost::mutex> s(*m);
    finished = true;
    condvar.notify_all();
}

void Synchronizer::PendingOps::wait(boost::mutex *m)
{
    while (!finished)
        condvar.wait(*m);
}

}

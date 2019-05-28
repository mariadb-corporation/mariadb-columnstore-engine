#include "Downloader.h"
#include "Config.h"
#include "SMLogging.h"
#include <string>
#include <errno.h>
#include <iostream>
#include <boost/filesystem.hpp>

using namespace std;
namespace storagemanager
{

Downloader::Downloader() : maxDownloads(0)
{
    storage = CloudStorage::get();
    string sMaxDownloads = Config::get()->getValue("ObjectStorage", "max_concurrent_downloads");
    try
    {
        maxDownloads = stoul(sMaxDownloads);
    }
    catch(invalid_argument)
    {
        logger->log(LOG_WARNING, "Downloader: Invalid arg for ObjectStorage/max_concurrent_downloads, using default of 20");
    }
    if (maxDownloads == 0)
        maxDownloads = 20;
    workers.setMaxThreads(maxDownloads);
    workers.setName("Downloader");
    logger = SMLogging::get();
}

Downloader::~Downloader()
{
}

void Downloader::useThisLock(boost::mutex *mutex)
{
    lock = mutex;
}

/*
inline boost::mutex * Downloader::getDownloadMutex()
{
    return &download_mutex;
}
*/

#if 0
/*  This is another fcn with a bug too subtle to identify right now.  Writing up a simplified
    version; we can revisit later if necessary */
void Downloader::download(const vector<const string *> &keys, vector<int> *errnos, vector<size_t> *sizes)
{
    uint counter = keys.size();
    boost::condition condvar;
    boost::mutex m;
    DownloadListener listener(&counter, &condvar, &m);
    
    boost::shared_ptr<Download> dls[keys.size()];
    bool inserted[keys.size()];
    Downloads_t::iterator iterators[keys.size()];
    
    uint i;
    
    for (i = 0; i < keys.size(); i++)
        dls[i].reset(new Download(keys[i], this));
    
    boost::unique_lock<boost::mutex> s(download_mutex);
    for (i = 0; i < keys.size(); i++)
    {
        auto &dl = dls[i];
        pair<Downloads_t::iterator, bool> ret = downloads.insert(dl);
        iterators[i] = ret.first;
        inserted[i] = ret.second;
        
        if (inserted[i])
        {
            dl->listeners.push_back(&listener);
            workers.addJob(dl);
        }
        else
        {
            dl = *(ret.first);  // point to the existing download.  Redundant with the iterators array.  Don't care yet.
            (*iterators[i])->listeners.push_back(&listener);
        }
    }
    
    // wait for the downloads to finish
    boost::unique_lock<boost::mutex> dl_lock(m);
    s.unlock();
    while (counter > 0)
        condvar.wait(dl_lock);
    dl_lock.unlock();
    
    // remove the entries inserted by this call
    sizes->resize(keys.size());
    s.lock();
    for (i = 0; i < keys.size(); i++)
    {
        if (inserted[i])
        {
            (*sizes)[i] = (*iterators[i])->size;
            downloads.erase(iterators[i]);
        }
        else
            (*sizes)[i] = 0;
    }
    s.unlock();
            
    // check for errors & propagate
    errnos->resize(keys.size());
    char buf[80];
    for (i = 0; i < keys.size(); i++)
    {
        auto &dl = dls[i];
        (*errnos)[i] = dl->dl_errno;
        if (dl->dl_errno != 0)
            logger->log(LOG_ERR, "Downloader: failed to download %s, got '%s'", keys[i]->c_str(), strerror_r(dl->dl_errno, buf, 80));
    }
}
#endif

void Downloader::download(const vector<const string *> &keys, vector<int> *errnos, vector<size_t> *sizes)
{
    uint counter = keys.size();
    boost::condition condvar;
    DownloadListener listener(&counter, &condvar);
    vector<boost::shared_ptr<Download> > ownedDownloads(keys.size());
    
    // the caller is holding a mutex
    /*  if a key is already being downloaded, attach listener to that Download instance.
        if it is not already being downloaded, make a new Download instance.
        wait for the listener to tell us that it's done.
    */
    for (uint i = 0; i < keys.size(); i++)
    {
        boost::shared_ptr<Download> newDL(new Download(*keys[i], downloadPath, lock));
        auto it = downloads.find(newDL);   // kinda sucks to have to search this way.
        if (it == downloads.end())
        {
            newDL->listeners.push_back(&listener);
            ownedDownloads[i] = newDL;
            downloads.insert(newDL);
            workers.addJob(newDL);
        }
        else
        {
            //assert((*it)->key == *keys[i]);
            //cout << "Waiting for the existing download of " << *keys[i] << endl;
            
            // a Download is technically in the map until all of the Downloads issued by its owner
            // have finished.  So, we have to test whether this existing download has already finished
            // or not before waiting for it.  We could have Downloads remove themselves on completion.
            // TBD.  Need to just get this working first.
            if ((*it)->finished)
                --counter;
            else
                (*it)->listeners.push_back(&listener);
        }
    }
    
    // wait for the downloads to finish
    while (counter > 0)
        condvar.wait(*lock);
    
    // check success, gather sizes from downloads started by this thread
    sizes->resize(keys.size());
    errnos->resize(keys.size());
    char buf[80];    
    for (uint i = 0; i < keys.size(); i++)
    {
        if (ownedDownloads[i])
        {
            assert(ownedDownloads[i]->finished);
            (*sizes)[i] = ownedDownloads[i]->size;
            (*errnos)[i] = ownedDownloads[i]->dl_errno;
            if ((*errnos)[i])
                logger->log(LOG_ERR, "Downloader: failed to download %s, got '%s'", keys[i]->c_str(), 
                    strerror_r((*errnos)[i], buf, 80));
            downloads.erase(ownedDownloads[i]);
        }
        else 
        {
            (*sizes)[i] = 0;
            (*errnos)[i] = 0;
        }
    }
}

void Downloader::setDownloadPath(const string &path)
{
    downloadPath = path;
}
           
/* The helper fcns */
Downloader::Download::Download(const string &source, const string &_dlPath, boost::mutex *_lock) : 
                dlPath(_dlPath), key(source), dl_errno(0), size(0), lock(_lock), finished(false), itRan(false)
{
}

Downloader::Download::~Download()
{
    assert(!itRan || finished);
}

void Downloader::Download::operator()()
{
    itRan = true;
    CloudStorage *storage = CloudStorage::get();
    // TODO: we should have it download to a tmp path, then mv it to the cache dir to avoid
    // Cache seeing an incomplete download after a crash
    int err = storage->getObject(key, (dlPath / key).string(), &size);
    if (err != 0)
    {
        dl_errno = errno;
        boost::filesystem::remove(dlPath / key);
        size = 0;
    }
    
    lock->lock();
    finished = true;
    for (uint i = 0; i < listeners.size(); i++)
        listeners[i]->downloadFinished();
    lock->unlock();
}

Downloader::DownloadListener::DownloadListener(uint *_counter, boost::condition *condvar) : counter(_counter), cond(condvar)
{
}

void Downloader::DownloadListener::downloadFinished()
{
    (*counter)--;
    if ((*counter) == 0)
        cond->notify_all();
}

inline size_t Downloader::DLHasher::operator()(const boost::shared_ptr<Download> &d) const
{
    // since the keys start with a uuid, we can probably get away with just returning the first 8 chars
    // or as a compromise, hashing only the first X chars.  For later.
    return hash<string>()(d->key);
}

inline bool Downloader::DLEquals::operator()(const boost::shared_ptr<Download> &d1, const boost::shared_ptr<Download> &d2) const
{
    return (d1->key == d2->key);
}

}

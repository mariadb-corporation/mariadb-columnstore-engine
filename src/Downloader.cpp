#include "Downloader.h"
#include "Config.h"
#include "SMLogging.h"
#include <string>
#include <errno.h>
#include <iostream>
#include <boost/filesystem.hpp>

using namespace std;
namespace bf = boost::filesystem;
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
    tmpPath = "downloading";
}

Downloader::~Downloader()
{
}

void Downloader::download(const vector<const string *> &keys, vector<int> *errnos, vector<size_t> *sizes,
    const bf::path &prefix, boost::mutex *cache_lock)
{
    uint counter = keys.size();
    boost::condition condvar;
    DownloadListener listener(&counter, &condvar);
    vector<boost::shared_ptr<Download> > ownedDownloads(keys.size());
    
    // the caller is holding cache_lock
    /*  if a key is already being downloaded, attach listener to that Download instance.
        if it is not already being downloaded, make a new Download instance.
        wait for the listener to tell us that it's done.
    */
    boost::unique_lock<boost::mutex> s(lock);
    for (uint i = 0; i < keys.size(); i++)
    {
        boost::shared_ptr<Download> newDL(new Download(*keys[i], prefix, cache_lock, this));
        
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
    s.unlock();

    // wait for the downloads to finish
    while (counter > 0)
        condvar.wait(*cache_lock);
    
    // check success, gather sizes from downloads started by this thread
    sizes->resize(keys.size());
    errnos->resize(keys.size());
    char buf[80];
    s.lock();
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

bool Downloader::inProgress(const string &key)
{
    boost::shared_ptr<Download> tmp(new Download(key));
    boost::unique_lock<boost::mutex> s(lock);
    
    auto it = downloads.find(tmp);
    if (it != downloads.end())
        return !(*it)->finished;
    return false;
}

inline const bf::path & Downloader::getTmpPath() const
{
    return tmpPath;
}

/* The helper fcns */
Downloader::Download::Download(const string &source, const bf::path &_dlPath, boost::mutex *_lock, Downloader *_dl) : 
                dlPath(_dlPath), key(source), dl_errno(0), size(0), lock(_lock), finished(false), itRan(false), dl(_dl)
{
}

Downloader::Download::Download(const string &source) : 
    key(source), dl_errno(0), size(0), lock(NULL), finished(false), itRan(false), dl(NULL)
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

    // download to a tmp path
    if (!bf::exists(dlPath / dl->getTmpPath()))
        bf::create_directories(dlPath / dl->getTmpPath());
    bf::path tmpFile = dlPath / dl->getTmpPath() / key;
    int err = storage->getObject(key, tmpFile.string(), &size);
    if (err != 0)
    {
        dl_errno = errno;
        bf::remove(tmpFile);
        size = 0;
    }
    
    // move it to its proper place
    boost::system::error_code berr;
    bf::rename(tmpFile, dlPath / key, berr);
    if (berr)
    {
        dl_errno = berr.value();
        bf::remove(tmpFile);
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

#include "Downloader.h"
#include "Config.h"
#include <string>
#include <errno.h>

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
        // log something
    }
    if (maxDownloads == 0)
        maxDownloads = 20;
    workers.reset(new ThreadPool(maxDownloads));
}

Downloader::~Downloader()
{
}

int Downloader::download(const vector<const string *> &keys, vector<int> *errnos)
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
            workers->addJob(dl);
        }
        else
        {
            dl = *(ret.first);  // point to the existing download.  Redundant with the iterators array.  Don't care yet.
            (*iterators[i])->listeners.push_back(&listener);
        }
    }
    s.unlock();
    
    // wait for the downloads to finish
    boost::unique_lock<boost::mutex> dl_lock(m);
    while (counter > 0)
        condvar.wait(dl_lock);
    dl_lock.unlock();
    
    // remove the entries inserted by this call
    s.lock();
    for (i = 0; i < keys.size(); i++)
        if (inserted[i])
            downloads.erase(iterators[i]);

    // check for errors & propagate
    int ret = 0;
    errnos->resize(keys.size());
    for (i = 0; i < keys.size(); i++)
    {
        auto &dl = dls[i];
        (*errnos)[i] = dl->dl_errno;
        if (dl->dl_errno != 0)
            ret = -1;
    }
}

void Downloader::setDownloadPath(const string &path)
{
    downloadPath = path;
}

inline const string & Downloader::getDownloadPath() const
{
    return downloadPath;
}
           
/* The helper fcns */
Downloader::Download::Download(const string *source, Downloader *dl) : key(source), dler(dl), dl_errno(0)
{
}

void Downloader::Download::operator()()
{
    CloudStorage *storage = CloudStorage::get();
    int err = storage->getObject(*key, dler->getDownloadPath() + "/" + *key);
    if (err != 0)
        dl_errno = errno;
        
    for (auto &listener : listeners)
        listener->downloadFinished();
}

Downloader::DownloadListener::DownloadListener(uint *counter, boost::condition *condvar, boost::mutex *m) : count(counter), cond(condvar), mutex(m)
{
}

void Downloader::DownloadListener::downloadFinished()
{
    boost::unique_lock<boost::mutex> u(*mutex);
    if (--(*count) == 0)
        cond->notify_all();
}

inline size_t Downloader::DLHasher::operator()(const boost::shared_ptr<Download> &d) const
{
    return hash<string>()(*(d->key));
}

inline bool Downloader::DLEquals::operator()(const boost::shared_ptr<Download> &d1, const boost::shared_ptr<Download> &d2) const
{
    return (*(d1->key) == *(d2->key));
}

}

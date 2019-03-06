#ifndef DOWNLOADER_H_
#define DOWNLOADER_H_

#include "ThreadPool.h"
#include "CloudStorage.h"
#include <unordered_set>
#include <vector>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

namespace storagemanager
{

class Downloader
{
    public:
        Downloader();
        virtual ~Downloader();
        
        void download(const std::vector<const std::string *> &keys);
        void setDownloadPath(const std::string &path);
        const std::string & getDownloadPath() const;
        
    private:
        uint maxDownloads;
        std::string downloadPath;
    
        class DownloadListener
        {
        public:
            DownloadListener(uint *counter, boost::condition *condvar, boost::mutex *m);
            void downloadFinished();
        private:
            uint *count;
            boost::condition *cond;
            boost::mutex *mutex;
        };
    
        struct Download : public ThreadPool::Job
        {
            Download(const std::string *source, Downloader *);
            void operator()();
            Downloader *dler;
            const std::string *key;
            int dl_errno;   // to propagate errors from the download job to the caller
            std::vector<DownloadListener *> listeners;
        };
    
        struct DLHasher
        {
            size_t operator()(const boost::shared_ptr<Download> &d) const;
        };
        
        struct DLEquals
        {
            bool operator()(const boost::shared_ptr<Download> &d1, const boost::shared_ptr<Download> &d2) const;
        };
    
        typedef std::unordered_set<boost::shared_ptr<Download>, DLHasher, DLEquals> Downloads_t;
        Downloads_t downloads;
        boost::mutex download_mutex;
        
        boost::scoped_ptr<ThreadPool> downloaders;
        CloudStorage *storage;
};

}

#endif

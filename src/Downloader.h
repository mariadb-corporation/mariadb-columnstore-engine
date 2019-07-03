#ifndef DOWNLOADER_H_
#define DOWNLOADER_H_

#include "ThreadPool.h"
#include "CloudStorage.h"
#include "SMLogging.h"
#include <unordered_set>
#include <vector>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/filesystem/path.hpp>

namespace storagemanager
{

class Downloader
{
    public:
        Downloader();
        virtual ~Downloader();
        
        // caller owns the memory for the strings.
        // errors are reported through errnos
        void download(const std::vector<const std::string *> &keys, std::vector<int> *errnos, std::vector<size_t> *sizes);
        void setDownloadPath(const std::string &path);
        void useThisLock(boost::mutex *);
        bool inProgress(const std::string &);
        
    private:
        uint maxDownloads;
        std::string downloadPath;
        boost::mutex *lock;
    
        class DownloadListener
        {
        public:
            DownloadListener(uint *counter, boost::condition *condvar);
            void downloadFinished();
        private:
            uint *counter;
            boost::condition *cond;
        };
    
        /* Possible optimization.  Downloads used to use pointers to strings to avoid an extra copy.
           Out of paranoid during debugging, I made it copy the strings instead for a clearer lifecycle.
           However, it _should_ be safe to do.
        */
        struct Download : public ThreadPool::Job
        {
            Download(const std::string &source, const std::string &_dlPath, boost::mutex *_lock);
            Download(const std::string &source);
            ~Download();
            void operator()();
            boost::filesystem::path dlPath;
            const std::string key;
            int dl_errno;   // to propagate errors from the download job to the caller
            size_t size;
            boost::mutex *lock;
            bool finished, itRan;
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
        
        // something is not working right with this lock design, need to simplify.
        // for now, download will use Cache's lock for everything.
        //boost::mutex download_mutex;
        //boost::mutex *getDownloadMutex();
        ThreadPool workers;
        CloudStorage *storage;
        SMLogging *logger;
};

}

#endif

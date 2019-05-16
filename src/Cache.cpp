
#include "Cache.h"
#include "Config.h"
#include "Downloader.h"
#include <iostream>
#include <syslog.h>
#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;
namespace bf = boost::filesystem;

namespace
{
    boost::mutex m;
    storagemanager::Cache *inst = NULL;
}

namespace storagemanager
{

Cache * Cache::get()
{
    if (inst)
        return inst;
    boost::unique_lock<boost::mutex> s(m);
    if (inst)
        return inst;
    inst = new Cache();
    return inst;
}

Cache::Cache() : currentCacheSize(0)
{
    Config *conf = Config::get();
    logger = SMLogging::get();
    replicator = Replicator::get();
    
    string stmp = conf->getValue("Cache", "cache_size");
    if (stmp.empty()) 
    {
        logger->log(LOG_CRIT, "Cache/cache_size is not set");
        throw runtime_error("Please set Cache/cache_size in the storagemanager.cnf file");
    }
    try
    {
        maxCacheSize = stoul(stmp);
    }
    catch (invalid_argument &)
    {
        logger->log(LOG_CRIT, "Cache/cache_size is not a number");
        throw runtime_error("Please set Cache/cache_size to a number");
    }
    //cout << "Cache got cache size " << maxCacheSize << endl;
        
    stmp = conf->getValue("ObjectStorage", "object_size");
    if (stmp.empty()) 
    {
        logger->log(LOG_CRIT, "ObjectStorage/object_size is not set");
        throw runtime_error("Please set ObjectStorage/object_size in the storagemanager.cnf file");
    }
    try
    {
        objectSize = stoul(stmp);
    }
    catch (invalid_argument &)
    {
        logger->log(LOG_CRIT, "ObjectStorage/object_size is not a number");
        throw runtime_error("Please set ObjectStorage/object_size to a number");
    }
    
    prefix = conf->getValue("Cache", "path");
    if (prefix.empty())
    {
        logger->log(LOG_CRIT, "Cache/path is not set");
        throw runtime_error("Please set Cache/path in the storagemanager.cnf file");
    }
    
    try 
    {
        bf::create_directories(prefix);
    }
    catch (exception &e)
    {
        logger->log(LOG_CRIT, "Failed to create %s, got: %s", prefix.string().c_str(), e.what());
        throw e;
    }
    //cout << "Cache got prefix " << prefix << endl;
    
    downloader.setDownloadPath(prefix.string());
    downloader.useThisLock(&lru_mutex);
    
    stmp = conf->getValue("ObjectStorage", "journal_path");
    if (stmp.empty())
    {
        logger->log(LOG_CRIT, "ObjectStorage/journal_path is not set");
        throw runtime_error("Please set ObjectStorage/journal_path in the storagemanager.cnf file");
    }
    journalPrefix = stmp;
    bf::create_directories(journalPrefix);
    try 
    {
        bf::create_directories(journalPrefix);
    }
    catch (exception &e)
    {
        syslog(LOG_CRIT, "Failed to create %s, got: %s", journalPrefix.string().c_str(), e.what());
        throw e;
    }
    
    populate();
}

Cache::~Cache()
{
}

void Cache::populate()
{
    bf::directory_iterator dir(prefix);
    bf::directory_iterator dend;
    while (dir != dend)
    {
        // put everything in lru & m_lru
        const bf::path &p = dir->path();
        if (bf::is_regular_file(p))
        {
            lru.push_back(p.filename().string());
            auto last = lru.end();
            m_lru.insert(--last);
            currentCacheSize += bf::file_size(*dir);
        }
        else
            logger->log(LOG_WARNING, "Cache: found something in the cache that does not belong '%s'", p.string().c_str());
        ++dir;
    }
    
    // account for what's in the journal dir
    dir = bf::directory_iterator(journalPrefix);
    while (dir != dend)
    {
        const bf::path &p = dir->path();
        if (bf::is_regular_file(p))
        {
            if (p.extension() == ".journal")   // need to decide whether objects should have an extension
                currentCacheSize += bf::file_size(*dir);
            else
                logger->log(LOG_WARNING, "Cache: found a file in the journal dir that does not belong '%s'", p.string().c_str());
        }
        else
            logger->log(LOG_WARNING, "Cache: found something in the journal dir that does not belong '%s'", p.string().c_str());
        ++dir;
    }
}

#if 0
/*  Need to simplify this, we keep running into sneaky problems, and I just spotted a couple more.
    Just going to rewrite it.  We can revisit later if the simplified version needs improvement */
void Cache::read(const vector<string> &keys)
{
/*
    move existing keys to a do-not-evict map
    fetch keys that do not exist
    after fetching, move all keys from do-not-evict to the back of the LRU
*/
    boost::unique_lock<boost::recursive_mutex> s(lru_mutex);
    vector<const string *> keysToFetch;
    
    uint i;
    M_LRU_t::iterator mit;
    
    for (const string &key : keys)
    {
        mit = m_lru.find(key);
        if (mit != m_lru.end())
            // it's in the cache, add the entry to the do-not-evict set
            addToDNE(mit->lit);
        else
            // not in the cache, put it in the list to download
            keysToFetch.push_back(&key);
    }    
    //s.unlock();
    
    // start downloading the keys to fetch
    // TODO: there should be a path for the common case, which is that there is nothing
    // to download.
    vector<int> dl_errnos;
    vector<size_t> sizes;
    if (!keysToFetch.empty())
        downloader.download(keysToFetch, &dl_errnos, &sizes, lru_mutex);
    
    size_t sum_sizes = 0;
    for (size_t &size : sizes)
        sum_sizes += size;
    
    //s.lock();
    // do makespace() before downloading.  Problem is, until the download is finished, this fcn can't tell which
    // downloads it was responsible for.  Need Downloader to make the call...?
    _makeSpace(sum_sizes);      
    currentCacheSize += sum_sizes;

    // move all keys to the back of the LRU
    for (i = 0; i < keys.size(); i++)
    {
        mit = m_lru.find(keys[i]);
        if (mit != m_lru.end())
        {
            lru.splice(lru.end(), lru, mit->lit);
            LRU_t::iterator lit = lru.end();
            removeFromDNE(--lit);
        }
        else if (dl_errnos[i] == 0)   // successful download
        {
            lru.push_back(keys[i]);
            LRU_t::iterator lit = lru.end();
            m_lru.insert(M_LRU_element_t(--lit));
        }
        else
        {
            // Downloader already logged it, anything to do here?
            /* brainstorming options for handling it.
             1) Should it be handled?  The caller will log a file-not-found error, and there will be a download
                failure in the log already.  
             2) Can't really DO anything can it?
            */
        }
    }
}
#endif

// new simplified version
void Cache::read(const vector<string> &keys)
{
    /*  Move existing keys to the back of the LRU, start downloading nonexistant keys.
        Going to skip the do-not-evict set in this version, and will assume that the cache
        is large enough that an entry at the back of the LRU won't be evicted before the
        caller can open the corresponding file 
    */
    vector<const string *> keysToFetch;
    vector<int> dlErrnos;
    vector<size_t> dlSizes;
    
    boost::unique_lock<boost::recursive_mutex> s(lru_mutex);
    
    M_LRU_t::iterator mit;
    for (const string &key : keys)
    {
        mit = m_lru.find(key);
        if (mit != m_lru.end())
            lru.splice(lru.end(), lru, mit->lit);
        else
            keysToFetch.push_back(&key);
    }
    if (keysToFetch.empty())
        return;
        
    assert(s.owns_lock());
    downloader.download(keysToFetch, &dlErrnos, &dlSizes);
    assert(s.owns_lock());
    
    size_t sum_sizes = 0;
    for (uint i = 0; i < keysToFetch.size(); ++i)
    {
        // downloads with size 0 didn't actually happen, either because it
        // was a preexisting download (another read() call owns it), or because
        // there was an error downloading it.  Use size == 0 as an indication of
        // what to add to the cache
        if (dlSizes[i] != 0)
        {
            sum_sizes += dlSizes[i];
            lru.push_back(*keysToFetch[i]);
            LRU_t::iterator lit = lru.end();
            m_lru.insert(M_LRU_element_t(--lit));  // I dislike this way of grabbing the last iterator in a list.
        }
    }
    
    // fix cache size
    _makeSpace(sum_sizes);
    currentCacheSize += sum_sizes;
}

Cache::DNEElement::DNEElement(const LRU_t::iterator &k) : key(k), refCount(1)
{
}
        
void Cache::addToDNE(const LRU_t::iterator &key)
{
    DNEElement e(key);
    DNE_t::iterator it = doNotEvict.find(e);
    if (it != doNotEvict.end())
    {
        DNEElement &dnee = const_cast<DNEElement &>(*it);
        ++(dnee.refCount);
    }
    else
        doNotEvict.insert(e);
}
        
void Cache::removeFromDNE(const LRU_t::iterator &key)
{
    DNEElement e(key);
    DNE_t::iterator it = doNotEvict.find(e);
    if (it == doNotEvict.end())
        return;
    DNEElement &dnee = const_cast<DNEElement &>(*it);
    if (--(dnee.refCount) == 0)
        doNotEvict.erase(it);
}
    
const bf::path & Cache::getCachePath()
{
    return prefix;
}

const bf::path & Cache::getJournalPath()
{
    return journalPrefix;
}
        
void Cache::exists(const vector<string> &keys, vector<bool> *out) const
{
    out->resize(keys.size());
    boost::unique_lock<boost::recursive_mutex> s(lru_mutex);
    for (uint i = 0; i < keys.size(); i++)
        (*out)[i] = (m_lru.find(keys[i]) != m_lru.end());
}

bool Cache::exists(const string &key) const
{
    boost::unique_lock<boost::recursive_mutex> s(lru_mutex);
    return m_lru.find(key) != m_lru.end();
}

void Cache::newObject(const string &key, size_t size)
{
    boost::unique_lock<boost::recursive_mutex> s(lru_mutex);
    assert(m_lru.find(key) == m_lru.end());
    _makeSpace(size);
    lru.push_back(key);
    LRU_t::iterator back = lru.end();
    m_lru.insert(--back);
    currentCacheSize += size;
}

void Cache::newJournalEntry(size_t size)
{
    boost::unique_lock<boost::recursive_mutex> s(lru_mutex);
    _makeSpace(size);
    currentCacheSize += size;
}

void Cache::deletedJournal(size_t size)
{
    boost::unique_lock<boost::recursive_mutex> s(lru_mutex);
    assert(currentCacheSize >= size);
    currentCacheSize -= size;
}

void Cache::deletedObject(const string &key, size_t size)
{
    boost::unique_lock<boost::recursive_mutex> s(lru_mutex);
    assert(currentCacheSize >= size);
    M_LRU_t::iterator mit = m_lru.find(key);
    assert(mit != m_lru.end());          // TODO: 5/16/19 - got this assertion using S3 by running test000, then test000 again.
    assert(doNotEvict.find(mit->lit) == doNotEvict.end());
    lru.erase(mit->lit);
    m_lru.erase(mit);
    currentCacheSize -= size;
}

void Cache::setMaxCacheSize(size_t size)
{
    boost::unique_lock<boost::recursive_mutex> s(lru_mutex);
    if (size < maxCacheSize)
        _makeSpace(maxCacheSize - size);
    maxCacheSize = size;
}

void Cache::makeSpace(size_t size)
{
    boost::unique_lock<boost::recursive_mutex> s(lru_mutex);
    _makeSpace(size);
}

size_t Cache::getMaxCacheSize() const
{
    return maxCacheSize;
}

// call this holding lru_mutex
void Cache::_makeSpace(size_t size)
{
    ssize_t thisMuch = currentCacheSize + size - maxCacheSize;
    if (thisMuch <= 0)
        return;

    struct stat statbuf;
    LRU_t::iterator it = lru.begin();
    while (it != lru.end() && thisMuch > 0)
    {
        if (doNotEvict.find(it) != doNotEvict.end())
        {
            ++it;
            continue;   // it's in the do-not-evict list
        }
        
        bf::path cachedFile = prefix / *it;
        int err = stat(cachedFile.string().c_str(), &statbuf);
        if (err)
        {
            logger->log(LOG_WARNING, "Cache::makeSpace(): There seems to be a cached file that couldn't be stat'ed: %s", 
                cachedFile.string().c_str());
            ++it;
            continue;
        }
        
        /* 
            tell Synchronizer that this key will be evicted
            delete the file
            remove it from our structs
            update current size
        */
        assert(currentCacheSize >= (size_t) statbuf.st_size);
        currentCacheSize -= statbuf.st_size;
        thisMuch -= statbuf.st_size;
        //logger->log(LOG_WARNING, "Cache:  flushing!  Try to avoid this, it may deadlock!");
        Synchronizer::get()->flushObject(*it);
        replicator->remove(cachedFile.string().c_str(), Replicator::LOCAL_ONLY);
        LRU_t::iterator toRemove = it++;
        m_lru.erase(*toRemove);
        lru.erase(toRemove);
    }
}

void Cache::rename(const string &oldKey, const string &newKey, ssize_t sizediff)
{
    boost::unique_lock<boost::recursive_mutex> s(lru_mutex);
    auto it = m_lru.find(oldKey);
    //assert(it != m_lru.end());
    if (it == m_lru.end())
    {
        logger->log(LOG_WARNING, "Cache: was told to rename %s, but it is not in the cache", oldKey.c_str());
        return;
    }
    
    auto lit = it->lit;
    m_lru.erase(it);
    *lit = newKey;
    m_lru.insert(lit);
    currentCacheSize += sizediff;
}

size_t Cache::getCurrentCacheSize() const
{
    return currentCacheSize;
}

void Cache::reset()
{
    boost::unique_lock<boost::recursive_mutex> s(lru_mutex);
    m_lru.clear();
    lru.clear();
    
    bf::directory_iterator dir;
    bf::directory_iterator dend;
    for (dir = bf::directory_iterator(prefix); dir != dend; ++dir)
        bf::remove_all(dir->path());
        
    for (dir = bf::directory_iterator(journalPrefix); dir != dend; ++dir)
        bf::remove_all(dir->path());
    currentCacheSize = 0;
}

/* The helper classes */

Cache::M_LRU_element_t::M_LRU_element_t(const string *k) : key(k)
{}

Cache::M_LRU_element_t::M_LRU_element_t(const string &k) : key(&k)
{}

Cache::M_LRU_element_t::M_LRU_element_t(const LRU_t::iterator &i) : key(&(*i)), lit(i)
{}

inline size_t Cache::KeyHasher::operator()(const M_LRU_element_t &l) const
{
    return hash<string>()(*(l.key));
}

inline bool Cache::KeyEquals::operator()(const M_LRU_element_t &l1, const M_LRU_element_t &l2) const
{
    return (*(l1.key) == *(l2.key));
}

inline size_t Cache::DNEHasher::operator()(const DNEElement &l) const
{
    return hash<string>()(*(l.key));
}

inline bool Cache::DNEEquals::operator()(const DNEElement &l1, const DNEElement &l2) const
{
    return (*(l1.key) == *(l2.key));
}

}





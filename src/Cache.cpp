
#include "Cache.h"
#include "Config.h"
#include "Downloader.h"
#include <iostream>
#include <syslog.h>
#include <boost/filesystem.hpp>
#include <boost/thread.hpp>

using namespace std;
using namespace boost::filesystem;

namespace storagemanager
{

Cache::Cache()
{
    Config *conf = Config::get();

    string ssize = conf->getValue("Cache", "cache_size");
    if (ssize.empty()) 
    {
        syslog(LOG_CRIT, "Cache/cache_size is not set");
        throw runtime_error("Please set Cache/cache_size in the storagemanager.cnf file");
    }
    try
    {
        maxCacheSize = stol(ssize);
    }
    catch (invalid_argument &)
    {
        syslog(LOG_CRIT, "Cache/cache_size is not a number");
        throw runtime_error("Please set Cache/cache_size to a number");
    }
    cout << "Cache got cache size " << maxCacheSize << endl;
        
    prefix = conf->getValue("Cache", "path");
    if (prefix.empty())
    {
        syslog(LOG_CRIT, "Cache/path is not set");
        throw runtime_error("Please set Cache/path in the storagemanager.cnf file");
    }
    try 
    {
        boost::filesystem::create_directories(prefix);
    }
    catch (exception &e)
    {
        syslog(LOG_CRIT, "Failed to create %s, got: %s", prefix.string().c_str(), e.what());
        throw e;
    }
    cout << "Cache got prefix " << prefix << endl;
    
    downloader.setDownloadPath(prefix.string());
}

Cache::~Cache()
{
}

void Cache::read(const vector<string> &keys)
{
/*
    move existing keys to a do-not-evict map
    fetch keys that do not exist
    after fetching, move all keys from do-not-evict to the back of the LRU
*/
    boost::unique_lock<boost::mutex> s(lru_mutex);
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
    s.unlock();
    
    // start downloading the keys to fetch
    if (!keysToFetch.empty())
        downloader.download(keysToFetch);
    
    s.lock();
    
    // move all keys to the back of the LRU
    for (const string &key : keys)
    {
        mit = m_lru.find(key);
        if (mit != m_lru.end())
            lru.splice(lru.end(), lru, mit->lit);
        else
        {
            lru.push_back(key);
            m_lru.insert(M_LRU_element_t(&(lru.back()), lru.end()--));
        }
        removeFromDNE(lru.end());
    }
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
        ++dnee.refCount;
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
    if (--dnee.refCount == 0)
        doNotEvict.erase(it);
}
    
        

        
void Cache::exists(const vector<string> &keys, vector<bool> *out)
{
}

void Cache::newObject(const string &key, size_t size)
{
}

void Cache::deletedObject(const string &key, size_t size)
{
}

void Cache::setCacheSize(size_t size)
{
}

void Cache::makeSpace(size_t size)
{
}


/* The helper classes */

Cache::M_LRU_element_t::M_LRU_element_t(const string *k) : key(k)
{}

Cache::M_LRU_element_t::M_LRU_element_t(const string &k) : key(&k)
{}

Cache::M_LRU_element_t::M_LRU_element_t(const string *k, const LRU_t::iterator &i) : key(k), lit(i)
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





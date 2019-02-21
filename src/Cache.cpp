
#include "Cache.h"
#include "Config.h"
#include <iostream>
#include <syslog.h>
#include <boost/filesystem.hpp>

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
    
}

Cache::~Cache()
{
}

void Cache::read(const vector<string> &keys)
{
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

}

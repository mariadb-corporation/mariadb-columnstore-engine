
#ifndef CACHE_H_
#define CACHE_H_

#include <string>
#include <vector>
#include <boost/utility.hpp>
#include <boost/filesystem/path.hpp>

namespace storagemanager
{

class Cache : public boost::noncopyable
{
    public:
        Cache();
        virtual ~Cache();
        
        void read(const std::vector<std::string> &keys);
        void exists(const std::vector<std::string> &keys, std::vector<bool> *out);
        void newObject(const std::string &key, size_t size);
        void deletedObject(const std::string &key, size_t size);
        void setCacheSize(size_t size);
        void makeSpace(size_t size);

    private:
        boost::filesystem::path prefix;
        size_t maxCacheSize;
        
};

}

#endif

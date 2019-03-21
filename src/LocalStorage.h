#ifndef LOCALSTORAGE_H_
#define LOCALSTORAGE_H_

#include <string>
#include "CloudStorage.h"
#include "SMLogging.h"
#include <boost/filesystem/path.hpp>

namespace storagemanager
{

class LocalStorage : public CloudStorage
{
    public:
        LocalStorage();
        virtual ~LocalStorage();

        int getObject(const std::string &sourceKey, const std::string &destFile, size_t *size = NULL);
        int getObject(const std::string &sourceKey, boost::shared_array<uint8_t> &data, size_t *size = NULL);
        int putObject(const std::string &sourceFile, const std::string &destKey);
        int putObject(const boost::shared_array<uint8_t> data, size_t len, const std::string &destKey);
        void deleteObject(const std::string &key);
        int copyObject(const std::string &sourceKey, const std::string &destKey);
        int exists(const std::string &key, bool *out);
        
        const boost::filesystem::path & getPrefix() const;

    private:
        boost::filesystem::path prefix;
        
        int copy(const boost::filesystem::path &sourceKey, const boost::filesystem::path &destKey);
};

}

#endif

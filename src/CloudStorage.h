
#ifndef CLOUDSTORAGE_H_
#define CLOUDSTORAGE_H_

#include <string>
#include <boost/shared_array.hpp>
#include "SMLogging.h"

namespace storagemanager
{

class CloudStorage
{
    public:
    
        /* These behave like syscalls.  return code -1 means an error, and errno is set */
        virtual int getObject(const std::string &sourceKey, const std::string &destFile, size_t *size = NULL) = 0;
        virtual int getObject(const std::string &sourceKey, boost::shared_array<uint8_t> *data, size_t *size = NULL) = 0;
        virtual int putObject(const std::string &sourceFile, const std::string &destKey) = 0;
        virtual int putObject(const boost::shared_array<uint8_t> data, size_t len, const std::string &destKey) = 0;
        virtual void deleteObject(const std::string &key) = 0;
        virtual int copyObject(const std::string &sourceKey, const std::string &destKey) = 0;
        virtual int exists(const std::string &key, bool *out) = 0;
        
        // this will return a CloudStorage instance of the type specified in StorageManager.cnf
        static CloudStorage *get();
        
    protected:
        SMLogging *logger;
        CloudStorage();
        
    private:
        
};

}

#endif

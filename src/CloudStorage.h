
#ifndef CLOUDSTORAGE_H_
#define CLOUDSTORAGE_H_

#include <string>

namespace storagemanager
{

class CloudStorage
{
    public:
        virtual int getObject(const std::string &sourceKey, const std::string &destFile) = 0;
        virtual int putObject(const std::string &sourceFile, const std::string &destKey) = 0;
        virtual void deleteObject(const std::string &key) = 0;
        
        // this will return a CloudStorage instance of the type specified in StorageManager.cnf
        static CloudStorage *get();
    private:
};

}

#endif


#ifndef S3STORAGE_H_
#define S3STORAGE_H_

#include <string>
#include "CloudStorage.h"

namespace storagemanager
{

class S3Storage : public CloudStorage
{
    public:
        S3Storage();
        virtual ~S3Storage();

        int getObject(const std::string &sourceKey, const std::string &destFile, size_t *size = NULL);
        int putObject(const std::string &sourceFile, const std::string &destKey);
        void deleteObject(const std::string &key);
        int copyObject(const std::string &sourceKey, const std::string &destKey);
        int exists(const std::string &key, bool *out);

    private:
        
};




}

#endif

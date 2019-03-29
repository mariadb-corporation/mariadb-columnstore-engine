/*
 * MetadataFile.h
 */
#ifndef METADATAFILE_H_
#define METADATAFILE_H_

#include "Config.h"
#include "SMLogging.h"
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>
#include <vector>
#include <iostream>
#include <set>

namespace storagemanager
{

struct metadataObject {
    uint64_t offset;
    mutable uint64_t length;
    mutable std::string key;
    bool operator < (const metadataObject &b) const { return offset < b.offset; }
};



class MetadataFile
{
    public:
        struct no_create_t {};
        MetadataFile();
        MetadataFile(const char* filename);
        MetadataFile(const char* filename, no_create_t);   // this one won't create it if it doesn't exist
        ~MetadataFile();

        bool exists() const;
        void printObjects();
        int stat(struct stat *) const;
        // returns the objects needed to update
        std::vector<metadataObject> metadataRead(off_t offset, size_t length);
        // updates the metadatafile with new object
        int writeMetadata(const char *filename);
        
        // updates the name and length fields of an entry, given the offset
        void updateEntry(off_t offset, const std::string &newName, size_t newLength);
        void updateEntryLength(off_t offset, size_t newLength);
        metadataObject addMetadataObject(const char *filename, size_t length);

        // TBD: this may have to go; there may be no use case where only the uuid needs to change.
        static std::string getNewKeyFromOldKey(const std::string &oldKey, size_t length=0);
        static std::string getNewKey(std::string sourceName, size_t offset, size_t length);
        static off_t getOffsetFromKey(const std::string &key);
        static std::string getSourceFromKey(const std::string &key);
        static size_t getLengthFromKey(const std::string &key);
        static void setOffsetInKey(std::string &key, off_t newOffset);
        static void setLengthInKey(std::string &key, size_t newLength);
        
        // this will be a singleton, which stores the config used
        // by all MetadataFile instances so we don't have to keep bothering Config.
        // members are public b/c i don't want to write accessors right now.  Also who cares.
        class MetadataConfig
        {
            public:
                static MetadataConfig *get();
                size_t mObjectSize;
                std::string msMetadataPath;
            
            private:
                MetadataConfig();
        };
        
    private:
        MetadataConfig *mpConfig;
        SMLogging *mpLogger;
        int mVersion;
        int mRevision;
        std::string mFilename;
        std::set<metadataObject> mObjects;
        bool _exists;
};

}

#endif /* METADATAFILE_H_ */

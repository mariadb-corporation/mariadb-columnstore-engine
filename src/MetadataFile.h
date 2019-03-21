/*
 * MetadataFile.h
 */
#ifndef METADATAFILE_H_
#define METADATAFILE_H_

#include "Config.h"
#include "SMLogging.h"
#include <string>
#include <sys/types.h>
#include <stdint.h>
#include <vector>
#include <iostream>
#include <set>

using namespace std;

namespace storagemanager
{

struct metadataObject {
    uint64_t offset;
    uint64_t length;
    string name;
    bool operator < (const metadataObject &b) const { return offset < b.offset; }
};

class MetadataFile
{
    public:
        MetadataFile();
        MetadataFile(const char* filename);
        ~MetadataFile();

        void printObjects();
        // returns the objects needed to update
        vector<metadataObject> metadataRead(off_t offset, size_t length);
        // updates the metadatafile with new object
        int updateMetadata(const char *filename);
        
        // updates the name and length fields of an entry, given the offset
        void updateEntry(off_t offset, const std::string &newName, size_t newLength); 
        metadataObject addMetadataObject(const char *filename, size_t length);

        // TBD: this may have to go; there may be no use case where only the uuid needs to change.
        static std::string getNewKeyFromOldKey(const std::string &oldKey, size_t length=0);
        static std::string getNewKey(std::string sourceName, size_t offset, size_t length);
        static off_t getOffsetFromKey(const std::string &key);
        static std::string getSourceFromKey(const std::string &key);
        static size_t getLengthFromKey(const std::string &key);
        static void setOffsetInKey(std::string &key, off_t newOffset);
        static void setLengthInKey(std::string &key, size_t newLength);
        

    private:
        Config *mpConfig;
        SMLogging *mpLogger;
        int mVersion;
        int mRevision;
        size_t mObjectSize;
        //set<metadataObject> mObjects;
        vector<metadataObject> mObjects;
};

}

#endif /* METADATAFILE_H_ */

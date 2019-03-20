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
        metadataObject addMetadataObject(const char *filename, size_t length);

        // TBD: this may have to go; there may be no use case where only the uuid needs to change.
        std::string getNewKeyFromOldKey(const std::string &oldKey);
        std::string getNewKey(std::string sourceName, size_t offset, size_t length);

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

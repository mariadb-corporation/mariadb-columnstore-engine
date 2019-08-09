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
#include <unordered_map>
#include <boost/filesystem/path.hpp>

namespace storagemanager
{

struct metadataObject {
    metadataObject();
    metadataObject(uint64_t offset);   // so we can search mObjects by integer
    metadataObject(uint64_t offset, uint64_t length, const std::string &key);
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
        MetadataFile(const boost::filesystem::path &filename);
        MetadataFile(const boost::filesystem::path &path, no_create_t,bool appendExt);   // this one won't create it if it doesn't exist
        
        // this ctor is 'special'.  It will take an absolute path, and it will assume it points to a metafile
        // meaning, that it doesn't need the metadata prefix prepended, or the .meta extension appended.
        // aside from that, it will behave like the no_create ctor variant above
        //MetadataFile(const boost::filesystem::path &path);
        ~MetadataFile();

        bool exists() const;
        void printObjects() const;
        int stat(struct stat *) const;
        size_t getLength() const;
        // returns the objects needed to update
        std::vector<metadataObject> metadataRead(off_t offset, size_t length) const;
        // updates the metadatafile with new object
        int writeMetadata(const boost::filesystem::path &filename);
        
        // updates the name and length fields of an entry, given the offset
        void updateEntry(off_t offset, const std::string &newName, size_t newLength);
        void updateEntryLength(off_t offset, size_t newLength);
        metadataObject addMetadataObject(const boost::filesystem::path &filename, size_t length);
        bool getEntry(off_t offset, metadataObject *out) const;
        void removeEntry(off_t offset);
        void removeAllEntries();
        
        // removes p from the json cache.  p should include the .meta extension
        static void deletedMeta(const boost::filesystem::path &p);
        
        // TBD: this may have to go; there may be no use case where only the uuid needs to change.
        static std::string getNewKeyFromOldKey(const std::string &oldKey, size_t length=0);
        static std::string getNewKey(std::string sourceName, size_t offset, size_t length);
        static off_t getOffsetFromKey(const std::string &key);
        static std::string getSourceFromKey(const std::string &key);
        static size_t getLengthFromKey(const std::string &key);
        static void setOffsetInKey(std::string &key, off_t newOffset);
        static void setLengthInKey(std::string &key, size_t newLength);
        
        // breaks a key into its consitituent fields
        static void breakout(const std::string &key, std::vector<std::string> &out);
        
        off_t getMetadataNewObjectOffset();
        // this will be a singleton, which stores the config used
        // by all MetadataFile instances so we don't have to keep bothering Config.
        // members are public b/c i don't want to write accessors right now.  Also who cares.
        class MetadataConfig
        {
            public:
                static MetadataConfig *get();
                size_t mObjectSize;
                boost::filesystem::path msMetadataPath;
            
            private:
                MetadataConfig();
        };
  
        static void printKPIs();
              
        typedef boost::shared_ptr<boost::property_tree::ptree> Jsontree_t;
    private:
        MetadataConfig *mpConfig;
        SMLogging *mpLogger;
        int mVersion;
        int mRevision;
        boost::filesystem::path mFilename;
        Jsontree_t jsontree;
        //std::set<metadataObject> mObjects;
        bool _exists;
        void makeEmptyJsonTree();
};



}

#endif /* METADATAFILE_H_ */

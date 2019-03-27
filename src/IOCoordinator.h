
#ifndef IOCOORDINATOR_H_
#define IOCOORDINATOR_H_

#include <sys/types.h>
#include <stdint.h>
#include <sys/stat.h>
#include <vector>
#include <string>
#include <boost/utility.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/shared_array.hpp>
#include <boost/filesystem.hpp>

#include "Config.h"
#include "Cache.h"
#include "SMLogging.h"
#include "RWLock.h"
#include "Replicator.h"

namespace storagemanager
{

boost::shared_array<char> seekToEndOfHeader1(int fd);

class IOCoordinator : public boost::noncopyable
{
    public:
        static IOCoordinator *get();
        virtual ~IOCoordinator();

        void willRead(const char *filename, off_t offset, size_t length);
        int read(const char *filename, uint8_t *data, off_t offset, size_t length);
        int write(const char *filename, const uint8_t *data, off_t offset, size_t length);
        int append(const char *filename, const uint8_t *data, size_t length);
        int open(const char *filename, int openmode, struct stat *out);
        int listDirectory(const char *filename, std::vector<std::string> *listing);
        int stat(const char *path, struct stat *out);
        int truncate(const char *path, size_t newsize);
        int unlink(const char *path);
        int copyFile(const char *filename1, const char *filename2);
        
        // The shared logic for merging a journal file with its base file.
        // *len should be set to the length of the data requested (0 means read the whole file),
        // on return *len will be the actual length returned.
        boost::shared_array<uint8_t> mergeJournal(const char *objectPath, const char *journalPath, off_t offset, size_t *len) const;
        
        // this version modifies object data in memory, given the journal filename
        int mergeJournalInMem(boost::shared_array<uint8_t> &objData, size_t *len, const char *journalPath) const;
        
        // this version takes already-open file descriptors, and an already-allocated buffer as input.
        // file descriptor are positioned, eh, best not to assume anything about their positions
        // on return.  
        // Not implemented yet.  At the point of this writing, we will use the existing versions, even though
        // it's wasteful, and will leave this as a likely future optimization.
        int mergeJournal(int objFD, int journalFD, uint8_t *buf, off_t offset, size_t *len) const;
        
        /* Lock manipulation fcns.  They can lock on any param given to them. */
        void renameObject(const std::string &oldKey, const std::string &newKey);
        bool readLock(const std::string &filename);
        bool writeLock(const std::string &filename);
        void readUnlock(const std::string &filename);
        void writeUnlock(const std::string &filename);

    private:
        IOCoordinator();
        Config *config;
        Cache *cache;
        SMLogging *logger;
        Replicator *replicator;
        size_t objectSize;
        boost::filesystem::path journalPath;
        boost::filesystem::path cachePath;
        
        std::map<std::string, RWLock *> locks;
        boost::mutex lockMutex;  // lol
        
        int loadObjectWithJournal(const char *objFilename, const char *journalFilename, 
            uint8_t *data, off_t offset, size_t length);
        int loadObject(int fd, uint8_t *data, off_t offset, size_t length);
};

}


#endif


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

#include "Config.h"
#include "SMLogging.h"

namespace storagemanager
{

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
        
        void getNewKeyFromOldKey(const std::string &oldKey, std::string *newKey);
        void getNewKeyFromSourceName(const std::string &sourceName, std::string *newKey);
        
        // The shared logic for merging a journal file with its base file.
        // The default values for offset and len mean 'process the whole file'.  Otherwise,
        // offset is relative to the object.
        boost::shared_array<uint8_t> mergeJournal(const char *objectPath, const char *journalPath, off_t offset = 0, size_t len = 0) const;
        int mergeJournalInMem(uint8_t *objData, const char *journalPath);
        
    private:
        IOCoordinator();
        Config *config;
        SMLogging *logger;
        size_t objectSize;
};

}


#endif

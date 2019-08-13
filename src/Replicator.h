#ifndef REPLICATOR_H_
#define REPLICATOR_H_

//#include "ThreadPool.h"
#include "MetadataFile.h"
#include <boost/filesystem.hpp>
#include <sys/types.h>
#include <stdint.h>

#define JOURNAL_ENTRY_HEADER_SIZE 16

namespace storagemanager
{

//	64-bit offset
//	64-bit length
//	<length-bytes of data to write at specified offset>

class Replicator
{
    public:
        static Replicator *get();
        virtual ~Replicator();

        enum Flags
        {
            NONE = 0,
            LOCAL_ONLY = 0x1,
            NO_LOCAL = 0x2
        };
        
        int addJournalEntry(const boost::filesystem::path &filename, const uint8_t *data, off_t offset, size_t length);
        int newObject(const boost::filesystem::path &filename, const uint8_t *data, off_t offset, size_t length);
        int remove(const boost::filesystem::path &file, Flags flags = NONE);
        
        int updateMetadata(MetadataFile &meta);

        void printKPIs() const;

    private:
        Replicator();
        Config *mpConfig;
        SMLogging *mpLogger;
        std::string msJournalPath;
        std::string msCachePath;
        //ThreadPool threadPool;

        size_t repUserDataWritten, repHeaderDataWritten;
        size_t replicatorObjectsCreated, replicatorJournalsCreated;

};

}

#endif

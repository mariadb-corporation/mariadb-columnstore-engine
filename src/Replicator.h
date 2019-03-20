#ifndef REPLICATOR_H_
#define REPLICATOR_H_

//#include "ThreadPool.h"
#include <sys/types.h>
#include <stdint.h>

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

        int addJournalEntry(const char *filename, const uint8_t *data, off_t offset, size_t length);
        int newObject(const char *filename, const uint8_t *data, size_t length);
        int remove(const char *key ,uint8_t flags);


    private:
        Replicator();
        //ThreadPool threadPool;
};

}

#endif

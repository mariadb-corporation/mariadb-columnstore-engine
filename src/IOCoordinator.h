
#ifndef IOCOORDINATOR_H_
#define IOCOORDINATOR_H_

#include <sys/types.h>
#include <stdint.h>
#include <sys/stat.h>
#include <vector>
#include <string>
#include <boost/utility.hpp>
#include <boost/thread/mutex.hpp>

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
        
    private:
        IOCoordinator();
};

}


#endif

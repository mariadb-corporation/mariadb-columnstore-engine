
#include "IOCoordinator.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <boost/filesystem.hpp>

using namespace std;

namespace storagemanager
{

IOCoordinator::IOCoordinator()
{
}

IOCoordinator::~IOCoordinator()
{
}

void IOCoordinator::willRead(const char *, off_t, size_t)
{
    // no cache yet
}

struct scoped_closer {
    scoped_closer(int f) : fd(f) { }
    ~scoped_closer() { 
        int s_errno = errno;
        ::close(fd);
        errno = s_errno; 
    }
    int fd;
};

#define OPEN(name, mode) \
    fd = ::open(filename, mode); \
    if (fd < 0) \
        return fd; \
    scoped_closer sc(fd);

int IOCoordinator::read(const char *filename, uint8_t *data, off_t offset, size_t length)
{
    int fd, err;
    
    OPEN(filename, O_RDONLY);
    
    size_t count = 0;
    ::lseek(fd, offset, SEEK_SET);
    while (count < length) {
        err = ::read(fd, &data[count], length - count);
        if (err <= 0)
            if (count > 0)   // return what was successfully read
                return count;
            else
                return err;
        count += err;
    }
    
    return count;    
}

int IOCoordinator::write(const char *filename, const uint8_t *data, off_t offset, size_t length)
{
    int fd, err;
    
    OPEN(filename, O_WRONLY);
    size_t count = 0;
    ::lseek(fd, offset, SEEK_SET);
    while (count < length) {
        err = ::write(fd, &data[count], length - count);
        if (err <= 0)
            if (count > 0)   // return what was successfully written
                return count;
            else
                return err;
        count += err;
    }
    
    return count;
}

int IOCoordinator::append(const char *filename, const uint8_t *data, size_t length)
{
    int fd, err;
    
    OPEN(filename, O_WRONLY | O_APPEND);
    size_t count = 0;
    while (count < length) {
        err = ::write(fd, &data[count], length - count);
        if (err <= 0)
            if (count > 0)   // return what was successfully written
                return count;
            else
                return err;
        count += err;
    }
    
    return count;
}

int IOCoordinator::open(const char *filename, int openmode, struct stat *out)
{
    int fd, err;
    
    OPEN(filename, openmode);
    return fstat(fd, out);
}

int IOCoordinator::listDirectory(const char *filename, vector<string> *listing)
{
    boost::filesystem::path p(filename);
    
    listing->clear();
    if (!boost::filesystem::exists(p))
    {
        errno = ENOENT;
        return -1;
    }
    if (!boost::filesystem::is_directory(p))
    {
        errno = ENOTDIR;
        return -1;
    }
    
    boost::filesystem::directory_iterator it(p), end;
    for (boost::filesystem::directory_iterator it(p); it != end; it++)
        listing->push_back(it->path().filename().string());
    return 0;
}

int IOCoordinator::stat(const char *path, struct stat *out)
{
    return ::stat(path, out);
}

int IOCoordinator::truncate(const char *path, size_t newsize)
{
    return ::truncate(path, newsize);
}

int IOCoordinator::unlink(const char *path)
{
    return ::unlink(path);
}

}

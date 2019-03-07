
#include "IOCoordinator.h"
#include "SMLogging.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <boost/filesystem.hpp>
#include <iostream>

using namespace std;

namespace
{
    storagemanager::IOCoordinator *ioc = NULL;
    boost::mutex m;
}

namespace storagemanager
{

IOCoordinator::IOCoordinator()
{
}

IOCoordinator::~IOCoordinator()
{
}

IOCoordinator * IOCoordinator::get()
{
    if (ioc)
        return ioc;
    boost::mutex::scoped_lock s(m);
    if (ioc)
        return ioc;
    ioc = new IOCoordinator();
    return ioc;
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
    fd = ::open(filename, mode, 0660); \
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
    
    /* create all subdirs if necessary.  We don't care if directories actually get created. */
    if (openmode & O_CREAT)  {
        boost::filesystem::path p(filename);
        boost::system::error_code ec;
        boost::filesystem::create_directories(p.parent_path(), ec);
    }
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

/* Might need to rename this one.  The corresponding fcn in IDBFileSystem specifies that it
deletes files, but also entire directories, like an 'rm -rf' command.  Implementing that here
will no longer make it like the 'unlink' syscall. */
int IOCoordinator::unlink(const char *path)
{
    int ret = 0;
    boost::filesystem::path p(path);

    try
    {
        boost::filesystem::remove_all(path);
    }
    catch(boost::filesystem::filesystem_error &e)
    {
        errno = e.code().value();
        ret = -1;
    }
    return ret;
    
    //return ::unlink(path);
}

int IOCoordinator::copyFile(const char *filename1, const char *filename2)
{
    SMLogging* logger = SMLogging::get();
    int err = 0, l_errno;
    try {
        boost::filesystem::copy_file(filename1, filename2);
    }
    catch (boost::filesystem::filesystem_error &e) {
        err = -1;
        l_errno = e.code().value();   // why not.
        // eh, not going to translate all of boost's errors into our errors for this.
        // log the error
        logger->log(LOG_ERR,"IOCoordinator::copy(): got %s",e.what());
    }
    catch (...) {
        err = -1;
        l_errno = EIO;
    }
    return err;
}

}

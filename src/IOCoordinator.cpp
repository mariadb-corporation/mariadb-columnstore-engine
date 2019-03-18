
#include "IOCoordinator.h"
#include "SMLogging.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <boost/filesystem.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/uuid/uuid.hpp>
#include <iostream>

#define max(x, y) (x > y ? x : y)
#define min(x, y) (x < y ? x : y)

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
    config = Config::get();
    logger = SMLogging::get();
    objectSize = 5 * (1<<20);
    try 
    {
        objectSize = stoul(config->getValue("ObjectStorage", "object_size"));
    }
    catch (...)
    {
        cerr << "ObjectStorage/object_size must be set to a numeric value" << endl;
        throw;
    }
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

// this is not generic by any means.  This is assuming a version 1 journal header, and is looking
// for the end of it, which is the first \0 char.  It returns with fd pointing at the 
// first byte after the header.
// update: had to make it also return the header; the boost json parser does not stop at either
// a null char or the end of an object.
boost::shared_array<char> seekToEndOfHeader1(int fd)
{
    ::lseek(fd, 0, SEEK_SET);
    boost::shared_array<char> ret(new char[100]);
    int err;
    
    err = ::read(fd, ret.get(), 100);
    if (err < 0)
    {
        char buf[80];
        throw runtime_error("seekToEndOfHeader1 got: " + string(strerror_r(errno, buf, 80)));
    }
    for (int i = 0; i < err; i++)
    {
        if (ret[i] == 0)
        {
            ::lseek(fd, i+1, SEEK_SET);
            return ret;
        }
    }
    throw runtime_error("seekToEndOfHeader1: did not find the end of the header");
}
    

boost::shared_array<uint8_t> IOCoordinator::mergeJournal(const char *object, const char *journal, off_t offset, size_t len) const
{
    int objFD, journalFD;
    boost::shared_array<uint8_t> ret;
    
    objFD = ::open(object, O_RDONLY);
    if (objFD < 0)
        return NULL;
    scoped_closer s1(objFD);
    journalFD = ::open(journal, O_RDONLY);
    if (journalFD < 0)
        return NULL;
    scoped_closer s2(journalFD);
    
    // TODO: Right now this assumes that max object size has not been changed.
    // ideally, we would have a way to look up the size of a specific object.
    if (len == 0)
        len = objectSize - offset;
    ret.reset(new uint8_t[len]); 
    
    // read the object into memory
    size_t count = 0;
    ::lseek(objFD, offset, SEEK_SET);
    while (count < len) {
        int err = ::read(objFD, &ret[count], len - count);
        if (err < 0)
        {
            char buf[80];
            logger->log(LOG_CRIT, "IOC::mergeJournal(): failed to read %s, got '%s'", object, strerror_r(errno, buf, 80));
            int l_errno = errno;
            ret.reset();
            errno = l_errno;
            return ret;
        }
        else if (err == 0) 
        {
            // at the EOF of the object.  The journal may contain entries that append to the data,
            // so 0-fill the remaining bytes.
            #ifdef DEBUG
            memset(&ret[count], 0, len-count);
            #endif
            break;
        }
        count += err;
    }
    
    // grab the journal header and make sure the version is 1
    boost::shared_array<char> headertxt = seekToEndOfHeader1(journalFD);
    stringstream ss;
    ss << headertxt.get();
    boost::property_tree::ptree header;
    boost::property_tree::json_parser::read_json(ss, header);
    assert(header.get<string>("version") == "1");
    
    // start processing the entries
    while (1)
    {
        uint64_t offlen[2];
        int err = ::read(journalFD, &offlen, 16);
        if (err != 16)   // got EOF
            break;
        
        // if this entry overlaps, read the overlapping section
        uint64_t lastJournalOffset = offlen[0] + offlen[1];
        uint64_t lastBufOffset = offset + len;
        if (offlen[0] <= lastBufOffset && lastJournalOffset >= offset)
        {
            uint64_t startReadingAt = max(offlen[0], offset);
            uint64_t lengthOfRead = min(lastBufOffset, lastJournalOffset) - startReadingAt;
            
            if (startReadingAt != offlen[0])
                ::lseek(journalFD, startReadingAt - offlen[0], SEEK_CUR);
                
            uint count = 0;
            while (count < lengthOfRead)
            {
                err = ::read(journalFD, &ret[startReadingAt - offset + count], lengthOfRead - count);
                if (err < 0)
                {
                    char buf[80];
                    logger->log(LOG_ERR, "mergeJournal: got %s", strerror_r(errno, buf, 80));
                    ret.reset();
                    return ret;
                }
                else if (err == 0)
                {
                    logger->log(LOG_ERR, "mergeJournal: got early EOF");
                    ret.reset();
                    return ret;
                }
                count += err;
            }
            
            if (lengthOfRead != offlen[1])
                ::lseek(journalFD, offlen[1] - lengthOfRead, SEEK_CUR);
        }
        else
            // skip over this journal entry
            ::lseek(journalFD, offlen[1], SEEK_CUR);
    }
    
    return ret;
}

// MergeJournalInMem is a specialized version of mergeJournal().  TODO: refactor if possible.
int IOCoordinator::mergeJournalInMem(uint8_t *objData, const char *journalPath)
{
    int journalFD = ::open(journalPath, O_RDONLY);
    if (journalFD < 0)
        return -1;
    scoped_closer s(journalFD);

    // grab the journal header and make sure the version is 1
    boost::shared_array<char> headertxt = seekToEndOfHeader1(journalFD);
    stringstream ss;
    ss << headertxt.get();
    boost::property_tree::ptree header;
    boost::property_tree::json_parser::read_json(ss, header);
    assert(header.get<string>("version") == "1");
    
    // start processing the entries
    while (1)
    {
        uint64_t offlen[2];
        int err = ::read(journalFD, &offlen, 16);
        if (err != 16)   // got EOF
            break;
        
        uint64_t startReadingAt = offlen[0];
        uint64_t lengthOfRead = offlen[1];

        ::lseek(journalFD, startReadingAt, SEEK_CUR);
            
        uint count = 0;
        while (count < lengthOfRead)
        {
            err = ::read(journalFD, &objData[startReadingAt + count], lengthOfRead - count);
            if (err < 0)
            {
                char buf[80];
                int l_errno = errno;
                logger->log(LOG_ERR, "mergeJournal: got %s", strerror_r(errno, buf, 80));
                errno = l_errno;
                return -1;
            }
            else if (err == 0)
            {
                logger->log(LOG_ERR, "mergeJournal: got early EOF");
                errno = ENODATA;  // is there a better errno for early EOF?
                return -1;
            }
            count += err;
        }
        ::lseek(journalFD, offlen[1] - lengthOfRead, SEEK_CUR);
    }
    return 0;
}

string IOCoordinator::getNewKeyFromOldKey(const string &oldKey)
{
    boost::uuids::uuid u;
    string ret(oldKey);
    strcpy(&(*newKey)[0], u.to_string().c_str());
}

string IOCoordinator::getNewKey(string sourceName, size_t offset, size_t length)
{
    boost::uuids::uuid u;
    stringstream ss;
    
    for (int i = 0; i < sourceName.length(); i++)
        if (sourceName[i] == '/')
            sourceName[i] = '-';
    
    ss << u << "_" << offset << "_" << length << "_" << sourceName;
    return ss.str();
}
    
    
    
}

}

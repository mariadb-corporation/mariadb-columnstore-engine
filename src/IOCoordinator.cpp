
#include "IOCoordinator.h"
#include "MetadataFile.h"
#include "Utilities.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <boost/filesystem.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <iostream>

#define max(x, y) (x > y ? x : y)
#define min(x, y) (x < y ? x : y)

using namespace std;

namespace
{
    storagemanager::IOCoordinator *ioc = NULL;
    boost::mutex m;
}

namespace bf = boost::filesystem;

namespace storagemanager
{

IOCoordinator::IOCoordinator()
{
    config = Config::get();
    cache = Cache::get();
    logger = SMLogging::get();
    replicator = Replicator::get();
    
    try 
    {
        objectSize = stoul(config->getValue("ObjectStorage", "object_size"));
    }
    catch (...)
    {
        logger->log(LOG_ERR, "ObjectStorage/object_size must be set to a numeric value");
        throw runtime_error("Please set ObjectStorage/object_size in the storagemanager.cnf file");
    }

    try
    {
        metaPath = config->getValue("ObjectStorage", "metadata_path");
        if (metaPath.empty())
        {
            logger->log(LOG_ERR, "ObjectStorage/journal_path is not set");
            throw runtime_error("Please set ObjectStorage/journal_path in the storagemanager.cnf file");
        }
    }
    catch (...)
    {
        logger->log(LOG_ERR, "ObjectStorage/metadata_path is not set");
        throw runtime_error("Please set ObjectStorage/metadata_path in the storagemanager.cnf file");
    }
    
    cachePath = cache->getCachePath();
    journalPath = cache->getJournalPath();
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
    // not sure we will implement this.
}

#define OPEN(name, mode) \
    fd = ::open(filename, mode, 0660); \
    if (fd < 0) \
        return fd; \
    ScopedCloser sc(fd);

int IOCoordinator::loadObject(int fd, uint8_t *data, off_t offset, size_t length) const
{
    size_t count = 0;
    int err;
    
    ::lseek(fd, offset, SEEK_SET);
    while (count < length)
    {
        err = ::read(fd, &data[count], length - count);
        if (err < 0)
            return err;
        else if (err == 0)
        {
            errno = ENODATA;   // better errno for early EOF?
            return -1;
        }
        count += err;
    }
    return 0;
}

int IOCoordinator::loadObjectAndJournal(const char *objFilename, const char *journalFilename, 
    uint8_t *data, off_t offset, size_t length) const
{
    boost::shared_array<uint8_t> argh;
    
    argh = mergeJournal(objFilename, journalFilename, offset, &length);
    if (!argh)
        return -1;
    else
        memcpy(data, argh.get(), length);
    return 0;
}

int IOCoordinator::read(const char *filename, uint8_t *data, off_t offset, size_t length)
{
    /*
        This is a bit complex and verbose, so for the first cut, it will only return a partial
        result where that is easy to do.  Otherwise it will return an error.
    */

    /*
        Get the read lock on filename
        Figure out which objects are relevant to the request
        call Cache::read(objects)
        Open any journal files that exist to prevent deletion
        open all object files to prevent deletion
        release read lock
        put together the response in data
    */
    
    ScopedReadLock fileLock(this, filename);
    MetadataFile meta(filename, MetadataFile::no_create_t());
    
    if (!meta.exists())
    {
        errno = ENOENT;
        return -1;
    }
    
    vector<metadataObject> relevants = meta.metadataRead(offset, length);
    map<string, int> journalFDs, objectFDs;
    map<string, string> keyToJournalName, keyToObjectName;
    vector<SharedCloser> fdMinders;
    char buf[80];
    
    // load them into the cache
    vector<string> keys;
    for (const auto &object : relevants)
        keys.push_back(object.key);
    cache->read(keys);
    
    // open the journal files and objects that exist to prevent them from being 
    // deleted mid-operation
    for (const auto &key : keys)
    {
        // trying not to think about how expensive all these type conversions are.
        // need to standardize on type.  Or be smart and build filenames in a char [].
        // later.  not thinking about it for now.
        
        // open all of the journal files that exist
        string filename = (journalPath/(key + ".journal")).string();
        int fd = ::open(filename.c_str(), O_RDONLY);
        if (fd >= 0)
        {
            keyToJournalName[key] = filename;
            journalFDs[key] = fd;
            fdMinders.push_back(SharedCloser(fd));
        }
        else if (errno != ENOENT)
        {
            int l_errno = errno;
            logger->log(LOG_CRIT, "IOCoordinator::read(): Got an unexpected error opening %s, error was '%s'",
                filename.c_str(), strerror_r(l_errno, buf, 80));
            errno = l_errno;
            return -1;
        }
        
        // open all of the objects
        filename = (cachePath/key).string();
        fd = ::open(filename.c_str(), O_RDONLY);
        if (fd < 0)
        {
            int l_errno = errno;
            logger->log(LOG_CRIT, "IOCoordinator::read(): Got an unexpected error opening %s, error was '%s'",
                filename.c_str(), strerror_r(l_errno, buf, 80));
            errno = l_errno;
            return -1;
        }
        keyToObjectName[key] = filename;
        objectFDs[key] = fd;
        fdMinders.push_back(SharedCloser(fd));
    }
    fileLock.unlock();
    
    // copy data from each object + journal into the returned data
    size_t count = 0;
    int err;
    boost::shared_array<uint8_t> mergedData;
    for (auto &object : relevants)
    {
        const auto &jit = journalFDs.find(object.key);
        
        // if this is the first object, the offset to start reading at is offset - object->offset
        off_t thisOffset = (count == 0 ? offset - object.offset : 0);
        
        // if this is the last object, the length of the read is length - count,
        // otherwise it is the length of the object - starting offset
        
        size_t thisLength = min(object.length - thisOffset, length - count);
        if (jit == journalFDs.end()) 
            err = loadObject(objectFDs[object.key], &data[count], thisOffset, thisLength);
        else 
            err = loadObjectAndJournal(keyToObjectName[object.key].c_str(), keyToJournalName[object.key].c_str(), 
                &data[count], thisOffset, thisLength);
        if (err) 
        {
            if (count == 0)
                return -1;
            else
                return count;
        }
            
        count += thisLength;
    }
    
    // all done
    return count;
}

int IOCoordinator::write(const char *filename, const uint8_t *data, off_t offset, size_t length)
{
    int err = 0;
    uint64_t count = 0;
    uint64_t writeLength = 0;
    uint64_t dataRemaining = length;
    uint64_t journalOffset = 0;
    vector<metadataObject> objects;

    //writeLock(filename);

    MetadataFile metadata = MetadataFile(filename);

    //read metadata determine how many objects overlap
    objects = metadata.metadataRead(offset,length);

    // if there are objects append the journalfile in replicator
    if(!objects.empty())
    {
        for (std::vector<metadataObject>::const_iterator i = objects.begin(); i != objects.end(); ++i)
        {
            // figure out how much data to write to this object
            if (count == 0 && offset > i->offset)
            {
                // first object in the list so start at offset and
                // write to end of oject or all the data
                journalOffset = offset - i->offset;
                writeLength = min((objectSize - journalOffset),dataRemaining);
            }
            else
            {
                // starting at beginning of next object write the rest of data
                // or until object length is reached
                writeLength = min(objectSize,dataRemaining);
                journalOffset = 0;
            }
            err = replicator->addJournalEntry(i->key.c_str(),&data[count],journalOffset,writeLength);
            if (err <= 0)
            {
                metadata.updateEntryLength(i->offset, count);
                metadata.writeMetadata(filename);
                logger->log(LOG_ERR,"IOCoordinator::write(): newObject failed to complete write, %u of %u bytes written.",count,length);
                return count;
            }
            if ((writeLength + journalOffset) > i->length)
                metadata.updateEntryLength(i->offset, (writeLength + journalOffset));

            count += err;
            dataRemaining -= err;
        }
        //cache.makeSpace(journal_data_size)
        //Synchronizer::newJournalData(journal_file);
    }

    // there is no overlapping data, or data goes beyond end of last object
    while (dataRemaining > 0 && err >= 0)
    {
        //add a new metaDataObject
        writeLength = min(objectSize,dataRemaining);
        //cache.makeSpace(size)
        // add a new metadata object, this will get a new objectKey NOTE: probably needs offset too
        metadataObject newObject = metadata.addMetadataObject(filename,writeLength);
        // write the new object
        err = replicator->newObject(newObject.key.c_str(),data,writeLength);
        if (err <= 0)
        {
            // update metadataObject length to reflect what awas actually written
            metadata.updateEntryLength(newObject.offset, count);
            metadata.writeMetadata(filename);
            logger->log(LOG_ERR,"IOCoordinator::write(): newObject failed to complete write, %u of %u bytes written.",count,length);
            return count;
            //log error and abort
        }
        // sync
        //Synchronizer::newObject(newname)
        count += err;
        dataRemaining -= err;
    }

    metadata.writeMetadata(filename);
    
    //writeUnlock(filename);

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

// TODO: might need to support more open flags, ex: O_EXCL
int IOCoordinator::open(const char *filename, int openmode, struct stat *out)
{
    ScopedReadLock s(this, filename);

    MetadataFile meta(filename, MetadataFile::no_create_t());
    
    if ((openmode & O_CREAT) && !meta.exists())
        replicator->updateMetadata(filename, meta);   // this will end up creating filename
    return meta.stat(out);
}

int IOCoordinator::listDirectory(const char *filename, vector<string> *listing)
{
    bf::path p(metaPath / filename);
    
    listing->clear();
    if (!bf::exists(p))
    {
        errno = ENOENT;
        return -1;
    }
    if (!bf::is_directory(p))
    {
        errno = ENOTDIR;
        return -1;
    }
    
    bf::directory_iterator it(p), end;
    for (bf::directory_iterator it(p); it != end; it++)
        listing->push_back(it->path().filename().string());
    return 0;
}

int IOCoordinator::stat(const char *path, struct stat *out)
{
    ScopedReadLock s(this, path);
    MetadataFile meta(path, MetadataFile::no_create_t());
    return meta.stat(out);
}

int IOCoordinator::truncate(const char *path, size_t newSize)
{
    /*
        grab the write lock.
        get the relevant metadata.
        truncate the metadata.
        tell replicator to write the new metadata
        release the lock
        
        tell replicator to delete all of the objects that no longer exist & their journal files
        tell cache they were deleted
        tell synchronizer they were deleted
    */
    
    synchronizer = Synchronizer::get();  // need to init sync here to break circular dependency...
    
    int err;
    ScopedWriteLock lock(this, path);
    MetadataFile meta(path, MetadataFile::no_create_t());
    if (!meta.exists())
    {
        errno = ENOENT;
        return -1;
    }
    
    size_t filesize = meta.getLength();
    if (filesize == newSize)
        return 0;
    
    // extend the file, going to make IOC::write() do it
    if (filesize < newSize)
    {
        lock.unlock();
        uint8_t zero = 0;
        err = write(path, &zero, newSize - 1, 1);
        if (err < 0)
            return -1;
        return 0;
    }
    
    vector<metadataObject> objects = meta.metadataRead(newSize, filesize);
    
    // truncate the file
    if (newSize == objects[0].offset)
        meta.removeEntry(objects[0].offset);
    else
        meta.updateEntryLength(objects[0].offset, newSize - objects[0].offset);
    for (uint i = 1; i < objects.size(); i++)
        meta.removeEntry(objects[i].offset);
    
    err = replicator->updateMetadata(path, meta);
    if (err)
        return err;
    lock.unlock();
    
    uint i = (newSize == objects[0].offset ? 0 : 1);
    vector<string> deletedObjects;
    while (i < objects.size())
    {
        bf::path cached = cachePath / objects[i].key;
        bf::path journal = journalPath / (objects[i].key + ".journal");
        if (bf::exists(journal))
        {
            size_t jsize = bf::file_size(journal);
            replicator->remove(journal);
            cache->deletedJournal(jsize);
        }
            
        size_t fsize = bf::file_size(cached);
        replicator->remove(cached);
        cache->deletedObject(objects[i].key, fsize);
        deletedObjects.push_back(objects[i].key);
        ++i;
    }
    if (!deletedObjects.empty())
        synchronizer->deletedObjects(deletedObjects);
    return 0;
}

/* Might need to rename this one.  The corresponding fcn in IDBFileSystem specifies that it
deletes files, but also entire directories, like an 'rm -rf' command.  Implementing that here
will no longer make it like the 'unlink' syscall. */
int IOCoordinator::unlink(const char *path)
{
    int ret = 0;
    bf::path p(path);

    try
    {
        bf::remove_all(path);
    }
    catch(bf::filesystem_error &e)
    {
        errno = e.code().value();
        ret = -1;
    }
    return ret;
}

int IOCoordinator::copyFile(const char *filename1, const char *filename2)
{
    SMLogging* logger = SMLogging::get();
    int err = 0, l_errno;
    try {
        bf::copy_file(filename1, filename2);
    }
    catch (bf::filesystem_error &e) {
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

const bf::path &IOCoordinator::getCachePath() const
{
    return cachePath;
}

const bf::path &IOCoordinator::getJournalPath() const
{
    return journalPath;
}

const bf::path &IOCoordinator::getMetadataPath() const
{
    return metaPath;
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

int IOCoordinator::mergeJournal(int objFD, int journalFD, uint8_t *buf, off_t offset, size_t *len) const
{
    throw runtime_error("IOCoordinator::mergeJournal(int, int, etc) is not implemented yet.");
}

boost::shared_array<uint8_t> IOCoordinator::mergeJournal(const char *object, const char *journal, off_t offset,
    size_t *len) const
{
    int objFD, journalFD;
    boost::shared_array<uint8_t> ret;
    
    objFD = ::open(object, O_RDONLY);
    if (objFD < 0)
        return ret;
    ScopedCloser s1(objFD);
    journalFD = ::open(journal, O_RDONLY);
    if (journalFD < 0)
        return ret;
    ScopedCloser s2(journalFD);
    
    // grab the journal header, make sure the version is 1, and get the max offset
    boost::shared_array<char> headertxt = seekToEndOfHeader1(journalFD);
    stringstream ss;
    ss << headertxt.get();
    boost::property_tree::ptree header;
    boost::property_tree::json_parser::read_json(ss, header);
    assert(header.get<string>("version") == "1");
    string stmp = header.get<string>("max_offset");
    size_t maxJournalOffset = strtoul(stmp.c_str(), NULL, 0);
    
    struct stat objStat;
    fstat(objFD, &objStat);
    
    if (*len == 0)
        // read to the end of the file
        *len = max(maxJournalOffset, objStat.st_size) - offset;
    else
        // make sure len is within the bounds of the data
        *len = min(*len, (max(maxJournalOffset, objStat.st_size) - offset));
    ret.reset(new uint8_t[*len]);
    
    // read the object into memory
    size_t count = 0;
    ::lseek(objFD, offset, SEEK_SET);
    while (count < *len) {
        int err = ::read(objFD, &ret[count], *len - count);
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
            memset(&ret[count], 0, *len-count);
            #endif
            break;
        }
        count += err;
    }
    
    // start processing the entries
    while (1)
    {
        uint64_t offlen[2];
        int err = ::read(journalFD, &offlen, 16);
        if (err != 16)   // got EOF
            break;
        
        // if this entry overlaps, read the overlapping section
        uint64_t lastJournalOffset = offlen[0] + offlen[1];
        uint64_t lastBufOffset = offset + *len;
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
int IOCoordinator::mergeJournalInMem(boost::shared_array<uint8_t> &objData, size_t *len, const char *journalPath) const
{
    int journalFD = ::open(journalPath, O_RDONLY);
    if (journalFD < 0)
        return -1;
    ScopedCloser s(journalFD);

    // grab the journal header and make sure the version is 1
    boost::shared_array<char> headertxt = seekToEndOfHeader1(journalFD);
    stringstream ss;
    ss << headertxt.get();
    boost::property_tree::ptree header;
    boost::property_tree::json_parser::read_json(ss, header);
    assert(header.get<string>("version") == "1");
    string stmp = header.get<string>("max_offset");
    size_t maxJournalOffset = strtoul(stmp.c_str(), NULL, 0);    
    
    if (maxJournalOffset > *len)
    {
        uint8_t *newbuf = new uint8_t[maxJournalOffset];
        memcpy(newbuf, objData.get(), *len);
        objData.reset(newbuf);
        *len = maxJournalOffset;
    }
    
    // start processing the entries
    while (1)
    {
        uint64_t offlen[2];
        int err = ::read(journalFD, &offlen, 16);
        if (err != 16)   // got EOF
            break;
        
        uint64_t startReadingAt = offlen[0];
        uint64_t lengthOfRead = offlen[1];
            
        uint count = 0;
        while (count < lengthOfRead)
        {
            err = ::read(journalFD, &objData[startReadingAt + count], lengthOfRead - count);
            if (err < 0)
            {
                char buf[80];
                int l_errno = errno;
                logger->log(LOG_ERR, "mergeJournalInMem: got %s", strerror_r(errno, buf, 80));
                errno = l_errno;
                return -1;
            }
            else if (err == 0)
            {
                logger->log(LOG_ERR, "mergeJournalInMem: got early EOF");
                errno = ENODATA;  // is there a better errno for early EOF?
                return -1;
            }
            count += err;
        }
    }
    return 0;
}

void IOCoordinator::renameObject(const string &oldKey, const string &newKey)
{
    // does anything need to be done here?
}


bool IOCoordinator::readLock(const string &filename)
{
    boost::unique_lock<boost::mutex> s(lockMutex);

    auto ins = locks.insert(pair<string, RWLock *>(filename, NULL));
    if (ins.second)
        ins.first->second = new RWLock();
    ins.first->second->readLock(s);
}

void IOCoordinator::readUnlock(const string &filename)
{
    boost::unique_lock<boost::mutex> s(lockMutex);
    
    auto it = locks.find(filename);
    it->second->readUnlock();
    if (!it->second->inUse())
    {
        delete it->second;
        locks.erase(it);
    }
}

bool IOCoordinator::writeLock(const string &filename)
{
    boost::unique_lock<boost::mutex> s(lockMutex);
    
    auto ins = locks.insert(pair<string, RWLock *>(filename, NULL));
    if (ins.second)
        ins.first->second = new RWLock();
    ins.first->second->writeLock(s);
}

void IOCoordinator::writeUnlock(const string &filename)
{
    boost::unique_lock<boost::mutex> s(lockMutex);
    
    auto it = locks.find(filename);
    it->second->writeUnlock();
    if (!it->second->inUse())
    {
        delete it->second;
        locks.erase(it);
    }
}

}

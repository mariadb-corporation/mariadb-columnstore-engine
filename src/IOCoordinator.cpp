
#include "IOCoordinator.h"
#include "MetadataFile.h"
#include "Synchronizer.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <boost/filesystem.hpp>
#define BOOST_SPIRIT_THREADSAFE
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
    // not sure we will implement this.
}

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
    
    argh = mergeJournal(objFilename, journalFilename, offset, length);
    if (!argh)
        return -1;
    else
        memcpy(data, argh.get(), length);
    return 0;
}

ssize_t IOCoordinator::read(const char *_filename, uint8_t *data, off_t offset, size_t length)
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
    bf::path p(_filename);
    const char *filename = p.string().c_str();
    
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
    ScopedCloser fdMinders[relevants.size() * 2];
    int mindersIndex = 0;
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
            fdMinders[mindersIndex++].fd = fd;
            //fdMinders.push_back(SharedCloser(fd));
        }
        else if (errno != ENOENT)
        {
            int l_errno = errno;
            fileLock.unlock();
            cache->doneReading(keys);
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
            fileLock.unlock();
            cache->doneReading(keys);
            logger->log(LOG_CRIT, "IOCoordinator::read(): Got an unexpected error opening %s, error was '%s'",
                filename.c_str(), strerror_r(l_errno, buf, 80));
            errno = l_errno;
            return -1;
        }
        keyToObjectName[key] = filename;
        objectFDs[key] = fd;
        fdMinders[mindersIndex++].fd = fd;
        //fdMinders.push_back(SharedCloser(fd));
    }
    //fileLock.unlock();
    
    // ^^^ TODO:  The original version unlocked the file at the above position.  On second glance,
    // I'm not sure how we can guarantee the journal files won't be modified during the loads below.
    // If we do have a guarantee, and I'm just not seeing it now, add the explanation here and uncomment
    // the unlock() call above.  For now I will let this hold the read lock for the duration.
    
    // copy data from each object + journal into the returned data
    size_t count = 0;
    int err;
    boost::shared_array<uint8_t> mergedData;
    for (auto &object : relevants)
    {
        const auto &jit = journalFDs.find(object.key);
        
        // if this is the first object, the offset to start reading at is offset - object->offset
        off_t thisOffset = (count == 0 ? offset - object.offset : 0);
        // This checks and returns if the read is starting past EOF
        if (thisOffset >= (off_t) object.length)
            goto out;
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
            fileLock.unlock();
            cache->doneReading(keys);
            if (count == 0)
                return -1;
            else
                return count;
        }
            
        count += thisLength;
    }
    
out:
    fileLock.unlock();
    cache->doneReading(keys);
    // all done
    return count;
}

ssize_t IOCoordinator::write(const char *_filename, const uint8_t *data, off_t offset, size_t length)
{
    bf::path p(_filename);
    const char *filename = p.string().c_str();
    
    ScopedWriteLock lock(this, filename);
    int ret = _write(filename, data, offset, length);
    lock.unlock();
    cache->doneWriting();
    return ret;
}

ssize_t IOCoordinator::_write(const char *filename, const uint8_t *data, off_t offset, size_t length)
{
    int err = 0;
    ssize_t count = 0;
    uint64_t writeLength = 0;
    uint64_t dataRemaining = length;
    uint64_t objectOffset = 0;
    vector<metadataObject> objects;
    vector<string> newObjectKeys;
    Synchronizer *synchronizer = Synchronizer::get();  // need to init sync here to break circular dependency...

    MetadataFile metadata = MetadataFile(filename, MetadataFile::no_create_t());
    if (!metadata.exists())
    {
        errno = ENOENT;
        return -1;
    }

    //read metadata determine how many objects overlap
    objects = metadata.metadataRead(offset,length);

    // if there are objects append the journalfile in replicator
    if(!objects.empty())
    {
        for (std::vector<metadataObject>::const_iterator i = objects.begin(); i != objects.end(); ++i)
        {
            // figure out how much data to write to this object
            if (count == 0 && (uint64_t) offset > i->offset)
            {
                // first object in the list so start at offset and
                // write to end of oject or all the data
                objectOffset = offset - i->offset;
                writeLength = min((objectSize - objectOffset),dataRemaining);
            }
            else
            {
                // starting at beginning of next object write the rest of data
                // or until object length is reached
                writeLength = min(objectSize,dataRemaining);
                objectOffset = 0;
            }
            //cache->makeSpace(writeLength+JOURNAL_ENTRY_HEADER_SIZE);

            err = replicator->addJournalEntry(i->key.c_str(),&data[count],objectOffset,writeLength);
            assert((uint) err == writeLength);
            
            if (err <= 0)
            {
                // XXXPAT: Count hasn't been updated yet, so I'm not sure what we're trying to do here.
                // There's another block below that looks similar.  Also similar blocks in append().
                if ((count + objectOffset) > i->length)
                    metadata.updateEntryLength(i->offset, (count + objectOffset));
                replicator->updateMetadata(filename, metadata);
                logger->log(LOG_ERR,"IOCoordinator::write(): object failed to complete write, %u of %u bytes written.",count,length);
                return count;
            }

            if ((writeLength + objectOffset) > i->length)
                metadata.updateEntryLength(i->offset, (writeLength + objectOffset));

            cache->newJournalEntry(writeLength+JOURNAL_ENTRY_HEADER_SIZE);

            synchronizer->newJournalEntry(i->key, writeLength+JOURNAL_ENTRY_HEADER_SIZE);
            count += writeLength;
            dataRemaining -= writeLength;
        }
    }
    // there is no overlapping data, or data goes beyond end of last object
    while (dataRemaining > 0)
    {
        off_t newObjectOffset = metadata.getMetadataNewObjectOffset();
        // count is 0 so first write is beyond current end of file.
        // offset is > than newObject.offset so we need to adjust offset for object
        // unless offset is beyond newObject.offset + objectSize then we need to write null data to this object
        if (count == 0 && offset > newObjectOffset)
        {
            //this is starting beyond last object in metadata
            //figure out if the offset is in this object
            objectOffset = offset - newObjectOffset;
            writeLength = min((objectSize - objectOffset),dataRemaining);
        }
        else
        {
            // count != 0 we've already started writing and are going to new object
            // start at beginning of the new object
            writeLength = min(objectSize,dataRemaining);
            objectOffset = 0;
        }
        // writeLength is the data length passed to write()
        // objectOffset is 0 unless the write starts beyond the end of data
        // in that case need to add the null data to cachespace
        //cache->makeSpace(writeLength + objectOffset);

        metadataObject newObject = metadata.addMetadataObject(filename,(writeLength + objectOffset));

        // send to replicator
        err = replicator->newObject(newObject.key.c_str(),&data[count],objectOffset,writeLength);
        assert((uint) err == writeLength);
        if (err <= 0)
        {
            // update metadataObject length to reflect what awas actually written
            if ((count + objectOffset) > newObject.length)
                metadata.updateEntryLength(newObject.offset, (count + objectOffset));
            replicator->updateMetadata(filename, metadata);
            logger->log(LOG_ERR,"IOCoordinator::write(): newObject failed to complete write, %u of %u bytes written.",count,length);
            return count;
            //log error and abort
        }

        cache->newObject(newObject.key,writeLength + objectOffset);
        newObjectKeys.push_back(newObject.key);

        count += writeLength;
        dataRemaining -= writeLength;
    }
    synchronizer->newObjects(newObjectKeys);

    replicator->updateMetadata(filename, metadata);

    return count;
}

ssize_t IOCoordinator::append(const char *_filename, const uint8_t *data, size_t length)
{
    bf::path p(_filename);
    const char *filename = p.string().c_str();

    int err;
    ssize_t count = 0;
    uint64_t writeLength = 0;
    uint64_t dataRemaining = length;
    vector<metadataObject> objects;
    vector<string> newObjectKeys;
    Synchronizer *synchronizer = Synchronizer::get();  // need to init sync here to break circular dependency...
    
    ScopedWriteLock lock(this, filename);

    MetadataFile metadata = MetadataFile(filename, MetadataFile::no_create_t());
    if (!metadata.exists())
    {
        errno = EBADF;
        return -1;
    }

    uint64_t offset = metadata.getLength();

    //read metadata determine if this fits in the last object
    objects = metadata.metadataRead(offset,length);

    if(!objects.empty() && objects.size() == 1)
    {
        std::vector<metadataObject>::const_iterator i = objects.begin();

        // XXXPAT: Need to handle the case where objectSize has been reduced since i was created
        // ie, i->length may be > objectSize here, so objectSize - i->length may be a huge positive #
        if ((objectSize - i->length) > 0) // if this is zero then we can't put anything else in this object
        {
            // figure out how much data to write to this object
            writeLength = min((objectSize - i->length),dataRemaining);

            //cache->makeSpace(writeLength+JOURNAL_ENTRY_HEADER_SIZE);

            err = replicator->addJournalEntry(i->key.c_str(),&data[count],i->length,writeLength);
            assert((uint) err == writeLength);
            if (err <= 0)
            {
                metadata.updateEntryLength(i->offset, (count + i->length));
                replicator->updateMetadata(filename, metadata);
                logger->log(LOG_ERR,"IOCoordinator::append(): journal failed to complete write, %u of %u bytes written.",count,length);
                goto out;
            }
            metadata.updateEntryLength(i->offset, (writeLength + i->length));

            cache->newJournalEntry(writeLength+JOURNAL_ENTRY_HEADER_SIZE);

            synchronizer->newJournalEntry(i->key, writeLength+JOURNAL_ENTRY_HEADER_SIZE);
            count += writeLength;
            dataRemaining -= writeLength;
        }
    }
    else if (objects.size() > 1)
    {
        //Something went wrong this shouldn't overlap objects
        logger->log(LOG_ERR,"IOCoordinator::append(): overlapping objects found on append.",count,length);
        assert(0);
        return -1;
    }
    // append is starting or adding to a new object
    while (dataRemaining > 0)
    {
        //add a new metaDataObject
        writeLength = min(objectSize,dataRemaining);

        //cache->makeSpace(writeLength);
        // add a new metadata object, this will get a new objectKey NOTE: probably needs offset too
        metadataObject newObject = metadata.addMetadataObject(filename,writeLength);

        // write the new object
        err = replicator->newObject(newObject.key.c_str(),&data[count],0,writeLength);
        assert((uint) err == writeLength);
        if (err <= 0)
        {
            // update metadataObject length to reflect what awas actually written
            metadata.updateEntryLength(newObject.offset, (count));
            replicator->updateMetadata(filename, metadata);
            logger->log(LOG_ERR,"IOCoordinator::append(): newObject failed to complete write, %u of %u bytes written.",count,length);
            goto out;
            //log error and abort
        }
        cache->newObject(newObject.key,writeLength);
        newObjectKeys.push_back(newObject.key);

        count += writeLength;
        dataRemaining -= writeLength;
    }
    synchronizer->newObjects(newObjectKeys);
    replicator->updateMetadata(filename, metadata);
    
    // had to add this hack to prevent deadlock
out:
    // need to release the file lock before telling Cache that we're done writing.
    lock.unlock();
    cache->doneWriting();
    
    return count;
}

// TODO: might need to support more open flags, ex: O_EXCL
int IOCoordinator::open(const char *_filename, int openmode, struct stat *out)
{
    bf::path p = _filename;  // normalize it
    const char *filename = p.string().c_str();
    ScopedReadLock s(this, filename);

    MetadataFile meta(filename, MetadataFile::no_create_t());
    
    if ((openmode & O_CREAT) && !meta.exists())
        replicator->updateMetadata(filename, meta);   // this will end up creating filename
    if ((openmode & O_TRUNC) && meta.exists())
    {
        s.unlock();
        truncate(filename, 0);
        s.lock();
    }
    return meta.stat(out);
}

int IOCoordinator::listDirectory(const char *dirname, vector<string> *listing)
{
    bf::path p(metaPath / dirname);
    
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
    
    bf::directory_iterator end;
    for (bf::directory_iterator it(p); it != end; it++)
    {
        if (bf::is_directory(it->path()))
            listing->push_back(it->path().filename().string());
        else
            listing->push_back(it->path().stem().string());
    }
    return 0;
}

int IOCoordinator::stat(const char *_path, struct stat *out)
{
    bf::path p(_path);
    const char *path = p.string().c_str();
    
    if (bf::is_directory(metaPath/p))
        return ::stat((metaPath/p).string().c_str(), out);

    ScopedReadLock s(this, path);
    MetadataFile meta(path, MetadataFile::no_create_t());
    return meta.stat(out);
}

int IOCoordinator::truncate(const char *_path, size_t newSize)
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
    bf::path p(_path);
    const char *path = p.string().c_str();
    
    Synchronizer *synchronizer = Synchronizer::get();  // needs to init sync here to break circular dependency...
    
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
        uint8_t zero = 0;
        err = _write(path, &zero, newSize - 1, 1);
        lock.unlock();
        cache->doneWriting();
        if (err < 0)
            return -1;
        return 0;
    }
    
    vector<metadataObject> objects = meta.metadataRead(newSize, filesize - newSize);
    
    // truncate the file
    if (newSize == objects[0].offset)
        meta.removeEntry(objects[0].offset);
    else
    {
        meta.updateEntryLength(objects[0].offset, newSize - objects[0].offset);
        assert(objects[0].offset >= 0 && objectSize > (newSize - objects[0].offset));
    }
    for (uint i = 1; i < objects.size(); i++)
        meta.removeEntry(objects[i].offset);
    
    err = replicator->updateMetadata(path, meta);
    if (err)
        return err;
    //lock.unlock();   <-- ifExistsThenDelete() needs the file lock held during the call
    
    uint i = (newSize == objects[0].offset ? 0 : 1);
    vector<string> deletedObjects;
    for (; i < objects.size(); ++i)
    {
        int result = cache->ifExistsThenDelete(objects[i].key);
        if (result & 0x1)
            replicator->remove(cachePath / objects[i].key);
        if (result & 0x2)
            replicator->remove(journalPath / (objects[i].key + ".journal"));
        deletedObjects.push_back(objects[i].key);
    }
    if (!deletedObjects.empty())
        synchronizer->deletedObjects(deletedObjects);
    return 0;
}



void IOCoordinator::deleteMetaFile(const bf::path &file)
{
    /*
        lock for writing
        tell replicator to delete every object
        tell cache they were deleted
        tell synchronizer to delete them in cloud storage
    */
    //cout << "deleteMetaFile called on " << file << endl;
    
    Synchronizer *synchronizer = Synchronizer::get();

    // this is kind of ugly.  We need to lock on 'file' relative to metaPath, and without the .meta extension
    string pita = file.string().substr(metaPath.string().length());   // get rid of metapath
    pita = pita.substr(0, pita.length() - 5);   // get rid of the extension
    ScopedWriteLock lock(this, pita);
    //cout << "file is " << file.string() << " locked on " << pita << endl;
    
    MetadataFile meta(file);
    replicator->remove(file);
    //lock.unlock();      <-- ifExistsThenDelete() needs the file lock held during the call
        
    vector<metadataObject> objects = meta.metadataRead(0, meta.getLength());
    vector<string> deletedObjects;
    for (auto &object : objects)
    {
        //cout << "deleting " << object.key << endl;
        int result = cache->ifExistsThenDelete(object.key);
        if (result & 0x1)
            replicator->remove(cachePath/object.key);
        if (result & 0x2)
            replicator->remove(journalPath/(object.key + ".journal"));
        deletedObjects.push_back(object.key);
    }
    synchronizer->deletedObjects(deletedObjects);
}

void IOCoordinator::remove(const bf::path &p)
{
    // recurse on dirs
    if (bf::is_directory(p))
    {
        bf::directory_iterator dend;
        bf::directory_iterator entry(p);
        while (entry != dend) 
        {
            remove(*entry);     
            ++entry;
        }
        //cout << "removing dir " << p << endl;
        replicator->remove(p);
        return;
    }
    
    // if p is a metadata file call deleteMetaFile
    if (p.extension() == ".meta" && bf::is_regular_file(p))
        deleteMetaFile(p);
    else
    {
        // if we were passed a 'logical' file, it needs to have the meta extension added
        bf::path possibleMetaFile = p.string() + ".meta";
        if (bf::is_regular_file(possibleMetaFile))
            deleteMetaFile(possibleMetaFile);
        else if (bf::exists(p))
            replicator->remove(p);  // if p.meta doesn't exist, and it's not a dir, then just throw it out
    }
    
}

/* Need to rename this one.  The corresponding fcn in IDBFileSystem specifies that it
deletes files, but also entire directories, like an 'rm -rf' command.  Implementing that here
will no longer make it like the 'unlink' syscall. */
int IOCoordinator::unlink(const char *path)
{
    /*  recursively iterate over path
        open every metadata file
        lock for writing
        tell replicator to delete every object
        tell cache they were deleted
        tell synchronizer to delete them in cloud storage
    */
    
    /* TODO!  We need to make sure the input params to IOC fcns don't go up to parent dirs,
        ex, if path = '../../../blahblah'. */
    bf::path p(metaPath/path);
    
    try 
    {
        remove(p);
    }
    catch (bf::filesystem_error &e)
    {
        cout << "IOC::unlink caught an error: " << e.what() << endl;
        errno = e.code().value();
        return -1;
    }
    return 0;
}

// a helper to de-uglify error handling in copyFile
struct CFException
{
    CFException(int err, const string &logEntry) : l_errno(err), entry(logEntry) { }
    int l_errno;
    string entry;
};
    
int IOCoordinator::copyFile(const char *_filename1, const char *_filename2)
{
    /*
        if filename2 exists, delete it
        if filename1 does not exist return ENOENT
        if filename1 is not a meta file return EINVAL
        
        get a new metadata object
        get a read lock on filename1
        
        for every object in filename1
            get a new key for the object
            tell cloudstorage to copy obj1 to obj2
                if it errors out b/c obj1 doesn't exist
                    upload it with the new name from the cache
            copy the journal file if any
            add the new key to the new metadata
        
        on error, delete all of the newly created objects
        
        write the new metadata object
    */
    
    const bf::path p1(_filename1);
    const bf::path p2(_filename2);
    const char *filename1 = p1.string().c_str();
    const char *filename2 = p2.string().c_str();
    
    CloudStorage *cs = CloudStorage::get();
    Synchronizer *sync = Synchronizer::get();
    bf::path metaFile1 = metaPath/(p1.string() + ".meta");
    bf::path metaFile2 = metaPath/(p2.string() + ".meta");
    int err;
    char errbuf[80];
    
    if (!bf::exists(metaFile1))
    {
        errno = ENOENT;
        return -1;
    }
    
    if (bf::exists(metaFile2))
        deleteMetaFile(metaFile2);
    // since we don't implement mkdir(), assume the caller did that and
    // create any necessary parent dirs for filename2
    try
    {
        bf::create_directories(metaFile2.parent_path());
    }
    catch(bf::filesystem_error &e)
    {
        logger->log(LOG_CRIT, "IOCoordinator::copyFile(): failed to create directory %s.  Got %s", 
            metaFile2.parent_path().string().c_str(), strerror_r(e.code().value(), errbuf, 80));
        errno = e.code().value();
        return -1;
    }

    vector<pair<string, size_t> > newJournalEntries;
    ScopedReadLock lock(this, filename1);
    ScopedWriteLock lock2(this, filename2);
    MetadataFile meta1(metaFile1);
    MetadataFile meta2(metaFile2.string().c_str(), MetadataFile::no_create_t());
    vector<metadataObject> objects = meta1.metadataRead(0, meta1.getLength());
    
    if (meta2.exists()) {
        cout << "copyFile: overwriting a file" << endl;
        meta2.removeAllEntries();
    }
    
    // TODO.  I dislike large try-catch blocks, and large loops.  Maybe a little refactoring is in order.
    try 
    {
        for (const auto &object : objects)
        {
            bf::path journalFile = journalPath/(object.key + ".journal");
            metadataObject newObj = meta2.addMetadataObject(filename2, object.length);
            assert(newObj.offset == object.offset);
            // TODO: failure here makes a lot of noise in the log file
            // even though it's less efficient, we need to add an existence check, and copy on existence instead.
            err = cs->copyObject(object.key, newObj.key);
            if (err)
            {
                if (errno == ENOENT)
                {
                    // it's not in cloudstorage, see if it's in the cache
                    bf::path cachedObjPath = cachePath/object.key;
                    bool objExists = bf::exists(cachedObjPath);
                    if (!objExists)
                        throw CFException(ENOENT, string("IOCoordinator::copyFile(): source = ") + filename1 + 
                            ", dest = " + filename2 + ".  Object " + object.key + " does not exist in either "
                            "cloud storage or the cache!");

                    // put the copy in cloudstorage
                    err = cs->putObject(cachedObjPath.string(), newObj.key);
                    if (err)
                        throw CFException(errno, string("IOCoordinator::copyFile(): source = ") + filename1 + 
                            ", dest = " + filename2 + ".  Got an error uploading object " + object.key + ": " +
                            strerror_r(errno, errbuf, 80));
                }
                else      // the problem was something other than it not existing in cloud storage
                    throw CFException(errno, string("IOCoordinator::copyFile(): source = ") + filename1 + 
                        ", dest = " + filename2 + ".  Got an error copying object " + object.key + ": " +
                        strerror_r(errno, errbuf, 80));
            }
            
            // if there's a journal file for this object, make a copy
            if (bf::exists(journalFile))
            {
                bf::path newJournalFile = journalPath/(newObj.key + ".journal");
                try 
                {
                    bf::copy_file(journalFile, newJournalFile);
                    size_t tmp = bf::file_size(newJournalFile);
                    cache->newJournalEntry(tmp);
                    newJournalEntries.push_back(pair<string, size_t>(newObj.key, tmp));
                }
                catch (bf::filesystem_error &e)
                {
                    throw CFException(e.code().value(), string("IOCoordinator::copyFile(): source = ") + filename1 + 
                        ", dest = " + filename2 + ".  Got an error copying " + journalFile.string() + ": " +
                        strerror_r(e.code().value(), errbuf, 80));
                }
            }
        }
    }
    catch (CFException &e)
    {
        logger->log(LOG_CRIT, e.entry.c_str());
        for (const auto &newObject : meta2.metadataRead(0, meta2.getLength()))
            cs->deleteObject(newObject.key);
        for (auto &jEntry : newJournalEntries)
        {
            bf::path fullJournalPath = journalPath/(jEntry.first + ".journal");
            cache->deletedJournal(bf::file_size(fullJournalPath));
            bf::remove(fullJournalPath);
        }
        errno = e.l_errno;
        return -1;
    }
    lock.unlock();
    replicator->updateMetadata(filename2, meta2);
    lock2.unlock();
    
    for (auto &jEntry : newJournalEntries)
        sync->newJournalEntry(jEntry.first, jEntry.second);
    return 0;
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
    size_t len) const
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
    assert(header.get<int>("version") == 1);
    //size_t maxJournalOffset = header.get<size_t>("max_offset");

    #if 0
    struct stat objStat;
    fstat(objFD, &objStat);
    
    if (len == 0)
        // read to the end of the file
        len = max(maxJournalOffset + 1, (size_t) objStat.st_size) - offset;
    else
        // make sure len is within the bounds of the data
        len = min(*len, (max(maxJournalOffset + 1, (size_t) objStat.st_size) - offset));
    ret.reset(new uint8_t[*len]);
    #endif
    
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
            memset(&ret[count], 0, len-count);
            break;
        }
        count += err;
    }
    
    // start processing the entries
    while (1)
    {
        uint64_t offlen[2];
        int err = ::read(journalFD, &offlen, 16);
        if (err == 0)   // got EOF
            break;
        assert(err == 16);
        
        //cout << "MJ: got offset " << offlen[0] << " length " << offlen[1] << endl;
        // if this entry overlaps, read the overlapping section
        uint64_t lastJournalOffset = offlen[0] + offlen[1];
        uint64_t lastBufOffset = offset + len;
        if (offlen[0] <= lastBufOffset && lastJournalOffset >= (uint64_t) offset)
        {
            uint64_t startReadingAt = max(offlen[0], (uint64_t) offset);
            uint64_t lengthOfRead = min(lastBufOffset, lastJournalOffset) - startReadingAt;
            
            //cout << "MJ: startReadingAt = " << startReadingAt << " lengthOfRead = " << lengthOfRead << endl;
            
            // seek to the portion of the entry to start reading at
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
                    logger->log(LOG_ERR, "mergeJournal: got early EOF. offset=%ld, len=%ld, jOffset=%ld, jLen=%ld,"
                        " startReadingAt=%ld, lengthOfRead=%ld", offset, len, offlen[0], offlen[1], startReadingAt, lengthOfRead);
                    ret.reset();
                    return ret;
                }
                count += err;
            }
            
            // advance the file pos if we didn't read to the end of the entry
            if (startReadingAt - offlen[0] + lengthOfRead != offlen[1])
                ::lseek(journalFD, offlen[1] - (lengthOfRead + startReadingAt - offlen[0]), SEEK_CUR);
        }
        else
            // skip over this journal entry
            ::lseek(journalFD, offlen[1], SEEK_CUR);
    }
    
    return ret;
}

// MergeJournalInMem is a specialized version of mergeJournal().  TODO: refactor if possible.
int IOCoordinator::mergeJournalInMem(boost::shared_array<uint8_t> &objData, size_t len, const char *journalPath) const
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
    assert(header.get<int>("version") == 1);
    //size_t maxJournalOffset = header.get<size_t>("max_offset"); 
    
    #if 0
    if (maxJournalOffset > *len)
    {
        uint8_t *newbuf = new uint8_t[maxJournalOffset + 1];
        memcpy(newbuf, objData.get(), *len);
        memset(&newbuf[*len], 0, maxJournalOffset - *len + 1);
        objData.reset(newbuf);
        *len = maxJournalOffset + 1;
    }
    #endif
    
    // start processing the entries
    while (1)
    {
        uint64_t offlen[2];
        int err = ::read(journalFD, &offlen, 16);
        if (err == 0)   // got EOF
            break;
        else if (err < 16)
        {
            // punting on this
            cout << "mergeJournalInMem: failed to read a journal entry header in one attempt.  fixme..." << endl;
            errno = ENODATA;
            return -1;
        }
        
        uint64_t startReadingAt = offlen[0];
        uint64_t lengthOfRead = offlen[1];

        // XXXPAT: Speculative change.  Got mem errors from writing past the end of objData.  The length
        // in the metadata is shorter than this journal entry, and not because it got crazy values.
        // I think the explanation is a truncation.
        if (startReadingAt > len)
        {
            //logger->log(LOG_CRIT, "mergeJournalInMem: skipping a theoretically irrelevant journal entry in %s.  "
            //    "jstart = %llu, max = %llu", journalPath, startReadingAt, len);
            ::lseek(journalFD, offlen[1], SEEK_CUR);
            continue;
        }
        
        if (startReadingAt + lengthOfRead > len)
        {
            //logger->log(LOG_CRIT, "mergeJournalInMem: possibly bad journal entry in %s.  jStart = %llu, jEnd = %llu, max = %llu",
            //    journalPath, startReadingAt, startReadingAt + lengthOfRead, len);
            lengthOfRead = len - startReadingAt;
        }
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
        if (lengthOfRead < offlen[1])
            ::lseek(journalFD, offlen[1] - lengthOfRead, SEEK_CUR);
    }
    return 0;
}

void IOCoordinator::renameObject(const string &oldKey, const string &newKey)
{
    // does anything need to be done here?
}


void IOCoordinator::readLock(const string &filename)
{
    boost::unique_lock<boost::mutex> s(lockMutex);

    //cout << "read-locking " << filename << endl;
    //assert(filename[0] == '/');
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

void IOCoordinator::writeLock(const string &filename)
{
    boost::unique_lock<boost::mutex> s(lockMutex);
    
    //cout << "write-locking " << filename << endl;
    //assert(filename[0] == '/');
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

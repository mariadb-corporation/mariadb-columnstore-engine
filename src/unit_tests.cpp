#include "OpenTask.h"
#include "WriteTask.h"
#include "AppendTask.h"
#include "UnlinkTask.h"
#include "StatTask.h"
#include "TruncateTask.h"
#include "ListDirectoryTask.h"
#include "PingTask.h"
#include "CopyTask.h"
#include "messageFormat.h"
#include "Config.h"
#include "Cache.h"
#include "LocalStorage.h"
#include "MetadataFile.h"
#include "Replicator.h"
#include "S3Storage.h"
#include "Utilities.h"

#include <iostream>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <string.h>
#include <fcntl.h>
#include <boost/filesystem.hpp>
#include <boost/thread.hpp>
#include <boost/format.hpp>
#include <algorithm>
#include <random>


#undef NDEBUG
#include <cassert>

using namespace storagemanager;
using namespace std;
namespace bf = boost::filesystem;

struct scoped_closer {
    scoped_closer(int f) : fd(f) { }
    ~scoped_closer() { 
        int s_errno = errno;
        ::close(fd);
        errno = s_errno; 
    }
    int fd;
};

// (ints) 0 1 2 3 ... 2048
void makeTestObject(const char *dest)
{
    int objFD = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert(objFD >= 0);
    scoped_closer s1(objFD);
    
    for (int i = 0; i < 2048; i++)
        assert(write(objFD, &i, 4) == 4);
}

// the merged version should look like
// (ints)  0 1 2 3 4 0 1 2 3 4 10 11 12 13...
void makeTestJournal(const char *dest)
{
    int journalFD = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert(journalFD >= 0);
    scoped_closer s2(journalFD);
    
    char header[] = "{ \"version\" : 1, \"max_offset\": 39 }";
    write(journalFD, header, strlen(header) + 1);
    
    uint64_t offlen[2] = { 20, 20 };
    write(journalFD, offlen, 16);
    for (int i = 0; i < 5; i++)
        assert(write(journalFD, &i, 4) == 4);
}

const char *testObjKey = "12345_0_8192_test-file";
const char *testFile = "test-file";
const char *_metadata = 
"{ \n\
    \"version\" : 1, \n\
    \"revision\" : 1, \n\
    \"objects\" : \n\
    [ \n\
        { \n\
            \"offset\" : 0, \n\
            \"length\" : 8192, \n\
            \"key\" : \"12345_0_8192_test-file\" \n\
        } \n\
    ] \n\
}\n";


void makeTestMetadata(const char *dest)
{
    int metaFD = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    assert(metaFD >= 0);
    scoped_closer sc(metaFD);
    
    // need to parameterize the object name in the objects list
    write(metaFD, _metadata, strlen(_metadata));
}

int getSocket()
{
    int sock = ::socket(AF_UNIX, SOCK_STREAM, 0);
    assert(sock >= 0);
    return sock;
}

int sessionSock = -1;  // tester uses this end of the connection
int serverSock = -1;   
int clientSock = -1;   // have the Tasks use this end of the connection

void acceptConnection()
{
    int err;
    if (serverSock == -1) {
        serverSock = getSocket();

        struct sockaddr_un sa;
        memset(&sa, 0, sizeof(sa));
        sa.sun_family = AF_UNIX;
        memcpy(&sa.sun_path[1], "testing", 7);
        
        int err = ::bind(serverSock, (struct sockaddr *) &sa, sizeof(sa));
        assert(err == 0);
        err = ::listen(serverSock, 2);
        assert(err == 0);
    }
    
    sessionSock = ::accept(serverSock, NULL, NULL);
    assert(sessionSock > 0);
}

// connects sessionSock to clientSock
void makeConnection()
{
    boost::thread t(acceptConnection);
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    memcpy(&sa.sun_path[1], "testing", 7);
    
    clientSock = ::socket(AF_UNIX, SOCK_STREAM, 0);
    assert(clientSock > 0);
    sleep(1);   // let server thread get to accept()
    int err = ::connect(clientSock, (struct sockaddr *) &sa, sizeof(sa));
    assert(err == 0);
    t.join();
}
    
bool opentask()
{
    // going to rely on msgs being smaller than the buffer here
    uint8_t buf[1024];
    sm_msg_header *hdr = (sm_msg_header *) buf;
    open_cmd *cmd = (open_cmd *) &hdr[1];
    
    // open/create a file named 'opentest1'
    const char *filename = "opentest1";
    hdr->type = SM_MSG_START;
    hdr->flags = 0;
    hdr->payloadLen = sizeof(*cmd) + 9;
    cmd->opcode = OPEN;
    cmd->openmode = O_WRONLY | O_CREAT;
    cmd->flen = 9;
    strcpy((char *) cmd->filename, filename);
    
    ::unlink(filename);
    ::write(sessionSock, cmd, hdr->payloadLen);
    OpenTask o(clientSock, hdr->payloadLen);
    o.run();
    
    // read the response
    int err = ::recv(sessionSock, buf, 1024, MSG_DONTWAIT);
    sm_response *resp = (sm_response *) buf;
    assert(err == sizeof(struct stat) + sizeof(sm_response));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.payloadLen == sizeof(struct stat) + 4);
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 0);
    struct stat *_stat = (struct stat *) resp->payload;
    
    // what can we verify about the stat...
    assert(_stat->st_uid == getuid());
    assert(_stat->st_gid == getgid());
    assert(_stat->st_size == 0);
    
    /* verify the file is there */
    string metaPath = Config::get()->getValue("ObjectStorage", "metadata_path");
    assert(!metaPath.empty());
    metaPath += string("/") + filename + ".meta";
    
    assert(boost::filesystem::exists(metaPath));
    ::unlink(metaPath.c_str());
    cout << "opentask OK" << endl;
    return true;
}

bool replicatorTest()
{
    Config* config = Config::get();
    string metaPath = config->getValue("ObjectStorage", "metadata_path");
    string journalPath = config->getValue("ObjectStorage", "journal_path");
    Replicator *repli = Replicator::get();
    int err,fd;
    const char *newobject = "newobjectTest";
    const char *newobjectJournal = "newobjectTest.journal";
    string newObjectJournalFullPath = journalPath + "/" + "newobjectTest.journal";
    uint8_t buf[1024];
    uint8_t data[1024];
    int version = 1;
    uint64_t max_offset = 0;
    memcpy(data,"1234567890",10);
    string header = (boost::format("{ \"version\" : \"%03i\", \"max_offset\" : \"%011u\" }") % version % max_offset).str();

    // test newObject
    repli->newObject(newobject,data,10);
    
    //check file contents
    fd = ::open(newobject, O_RDONLY);
    err = ::read(fd, buf, 1024);
    assert(err == 10);
    buf[10] = 0;
    assert(!strcmp("1234567890", (const char *) buf));
    cout << "replicator newObject OK" << endl;
    ::close(fd);

    // test addJournalEntry
    repli->addJournalEntry(newobject,data,0,10);

    fd = ::open(newObjectJournalFullPath.c_str(), O_RDONLY);
    err = ::read(fd, buf, 1024);
    assert(err == (header.length() + 1 + 16 + 10));
    buf[err] = 0;
    assert(!strcmp("1234567890", (const char *) buf + header.length() + 1 + 16));
    cout << "replicator addJournalEntry OK" << endl;
    ::close(fd);

    repli->remove(newobject);
    repli->remove(newObjectJournalFullPath.c_str());
    assert(!boost::filesystem::exists(newobject));
    cout << "replicator remove OK" << endl;
    return true;
}

bool metadataJournalTest(std::size_t size, off_t offset)
{
    // make an empty file to write to
    const char *filename = "metadataJournalTest";
    uint8_t buf[(sizeof(write_cmd)+std::strlen(filename)+size)];
    uint64_t *data;

    sm_msg_header *hdr = (sm_msg_header *) buf;
    write_cmd *cmd = (write_cmd *) &hdr[1];
    cmd->opcode = WRITE;
    cmd->offset = offset;
    cmd->count = size;
    cmd->flen = std::strlen(filename);
    memcpy(&cmd->filename, filename, cmd->flen);
    data = (uint64_t *) &cmd->filename[cmd->flen];
    int count = 0;
    for (uint64_t i = 0; i < (size/sizeof(uint64_t)); i++)
    {
        data[i] = i;
        count++;
    }
    hdr->type = SM_MSG_START;
    hdr->payloadLen = sizeof(*cmd) + cmd->flen + cmd->count;
    WriteTask w(clientSock, hdr->payloadLen);
    int error = ::write(sessionSock, cmd, hdr->payloadLen);

    w.run();

    // verify response
    int err = ::recv(sessionSock, buf, 1024, MSG_DONTWAIT);
    sm_response *resp = (sm_response *) buf;
    assert(err == sizeof(*resp));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.payloadLen == 4);
    assert(resp->header.flags == 0);
    assert(resp->returnCode == size);

    MetadataFile mdfTest(filename);
    mdfTest.printObjects();

}

void metadataJournalTestCleanup(std::size_t size)
{
    Config* config = Config::get();
    string metaPath = config->getValue("ObjectStorage", "metadata_path");
    string journalPath = config->getValue("ObjectStorage", "journal_path");
    const char *filename = "metadataJournalTest";
    MetadataFile mdfTest(filename);
    std::vector<metadataObject> objects = mdfTest.metadataRead(0,size);
    for (std::vector<metadataObject>::const_iterator i = objects.begin(); i != objects.end(); ++i)
    {
        string keyJournal = journalPath + "/" + i->key + ".journal";
        if(boost::filesystem::exists(i->key.c_str()))
            ::unlink(i->key.c_str());
        if(boost::filesystem::exists(keyJournal.c_str()))
            ::unlink(keyJournal.c_str());
    }
    string mdfFile = metaPath + "/" + "metadataJournalTest.meta";
    ::unlink(mdfFile.c_str());
}

bool writetask()
{
    // make an empty file to write to
    const char *filename = "writetest1";
    ::unlink(filename);
    int fd = ::open(filename, O_CREAT | O_RDWR, 0666);
    assert(fd > 0);
    scoped_closer f(fd);
 
    uint8_t buf[1024];
    sm_msg_header *hdr = (sm_msg_header *) buf;
    write_cmd *cmd = (write_cmd *) &hdr[1];
    uint8_t *data;

    cmd->opcode = WRITE;
    cmd->offset = 0;
    cmd->count = 9;
    cmd->flen = 10;
    memcpy(&cmd->filename, filename, cmd->flen);
    data = (uint8_t *) &cmd->filename[cmd->flen];
    memcpy(data, "123456789", cmd->count);

    hdr->type = SM_MSG_START;
    hdr->payloadLen = sizeof(*cmd) + cmd->flen + cmd->count;

    WriteTask w(clientSock, hdr->payloadLen);
    ::write(sessionSock, cmd, hdr->payloadLen);
    w.run();
    
    // verify response
    int err = ::recv(sessionSock, buf, 1024, MSG_DONTWAIT);
    sm_response *resp = (sm_response *) buf;
    assert(err == sizeof(*resp));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.payloadLen == 4);
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 9);
    
    //check file contents
    err = ::read(fd, buf, 1024);
    assert(err == 9);
    buf[9] = 0;
    assert(!strcmp("123456789", (const char *) buf));
    ::unlink(filename);
    cout << "write task OK" << endl;
    return true;
}

bool appendtask()
{
    // make a file and put some stuff in it
    const char *filename = "appendtest1";
    ::unlink(filename);
    int fd = ::open(filename, O_CREAT | O_RDWR, 0666);
    assert(fd > 0);
    scoped_closer f(fd);
    int err = ::write(fd, "testjunk", 8);
    assert(err == 8);
 
    uint8_t buf[1024];
    append_cmd *cmd = (append_cmd *) buf;
    uint8_t *data;
    
    cmd->opcode = APPEND;
    cmd->count = 9;
    cmd->flen = 11;
    memcpy(&cmd->filename, filename, cmd->flen);
    data = (uint8_t *) &cmd->filename[cmd->flen];
    memcpy(data, "123456789", cmd->count);
    
    int payloadLen = sizeof(*cmd) + cmd->flen + cmd->count;
    
    AppendTask a(clientSock, payloadLen);
    ::write(sessionSock, cmd, payloadLen);
    a.run();
    
    // verify response
    err = ::recv(sessionSock, buf, 1024, MSG_DONTWAIT);
    sm_response *resp = (sm_response *) buf;
    assert(err == sizeof(*resp));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.payloadLen == 4);
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 9);
    
    //check file contents
    ::lseek(fd, 0, SEEK_SET);
    err = ::read(fd, buf, 1024);
    assert(err == 17);
    buf[17] = 0;
    assert(!strcmp("testjunk123456789", (const char *) buf));
    ::unlink(filename);
    cout << "append task OK" << endl;
    return true;
}

bool unlinktask()
{
    // make a meta file and delete it
    const char *filename = "unlinktest1";
    IOCoordinator *ioc = IOCoordinator::get();
    bf::path fullPath = ioc->getMetadataPath()/(string(filename) + ".meta");
    bf::remove(fullPath);
    
    MetadataFile meta(filename);
    assert(bf::exists(fullPath));
    
    uint8_t buf[1024];
    unlink_cmd *cmd = (unlink_cmd *) buf;
    uint8_t *data;
    
    cmd->opcode = UNLINK;
    cmd->flen = strlen(filename);
    memcpy(&cmd->filename, filename, cmd->flen);
    
    UnlinkTask u(clientSock, sizeof(unlink_cmd) + cmd->flen);
    ::write(sessionSock, cmd, sizeof(unlink_cmd) + cmd->flen);
    u.run();
    
    // verify response
    int err = ::recv(sessionSock, buf, 1024, MSG_DONTWAIT);
    sm_response *resp = (sm_response *) buf;
    assert(err == sizeof(*resp));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.payloadLen == 4);
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 0);
    
    // confirm it no longer exists
    assert(!bf::exists(fullPath));
    
    // delete it again, make sure we get an error message & reasonable error code
    // Interesting.  boost::filesystem::remove() doesn't consider it an error if the file doesn't
    // exist.  Need to look into the reasoning for that, and decide whether IOC
    // should return an error anyway.  For now, this test below doesn't get 
    // an error msg.
    #if 0
    memset(buf, 0, 1024);
    cmd->opcode = UNLINK;
    cmd->flen = strlen(filename);
    memcpy(&cmd->filename, filename, cmd->flen);
    
    UnlinkTask u2(clientSock, sizeof(unlink_cmd) + cmd->flen);
    ::write(sessionSock, cmd, sizeof(unlink_cmd) + cmd->flen);
    u2.run();
    
    // verify response
    err = ::recv(sessionSock, buf, 1024, MSG_DONTWAIT);
    resp = (sm_response *) buf;
    assert(err == sizeof(*resp) + 4);
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.payloadLen == 8);
    assert(resp->header.flags == 0);
    assert(resp->returnCode == -1);
    err = (*(int *) resp->payload);
    assert(err == ENOENT);
    #endif
    
    cout << "unlink task OK" << endl;
}

bool stattask()
{
    string filename = "stattest1";
    string fullFilename = Config::get()->getValue("ObjectStorage", "metadata_path") + "/" + filename + ".meta";
    
    ::unlink(fullFilename.c_str());
    makeTestMetadata(fullFilename.c_str());

    uint8_t buf[1024];
    stat_cmd *cmd = (stat_cmd *) buf;
    
    cmd->opcode = STAT;
    cmd->flen = filename.length();
    strcpy((char *) cmd->filename, filename.c_str());
    
    ::write(sessionSock, cmd, sizeof(*cmd) + cmd->flen);
    StatTask s(clientSock, sizeof(*cmd) + cmd->flen);
    s.run();
    
    // read the response
    int err = ::recv(sessionSock, buf, 1024, MSG_DONTWAIT);
    sm_response *resp = (sm_response *) buf;
    assert(err == sizeof(struct stat) + sizeof(sm_response));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.flags == 0);
    assert(resp->header.payloadLen == sizeof(struct stat) + 4);
    assert(resp->returnCode == 0);
    struct stat *_stat = (struct stat *) resp->payload;
    
    // what can we verify about the stat...
    assert(_stat->st_uid == getuid());
    assert(_stat->st_gid == getgid());
    assert(_stat->st_size == 8192);
    
    ::unlink(fullFilename.c_str());
    cout << "stattask OK" << endl;
    return true;
}

bool IOCTruncate()
{

    IOCoordinator *ioc = IOCoordinator::get();
    CloudStorage *cs = CloudStorage::get();
    LocalStorage *ls = dynamic_cast<LocalStorage *>(cs);
    if (!ls)
    {
        cout << "IOCTruncate() currently requires using Local storage" << endl;
        return true;
    }
    Cache *cache = Cache::get();
    cache->reset();
    
    bf::path cachePath = ioc->getCachePath();
    bf::path journalPath = ioc->getJournalPath();
    bf::path metaPath = ioc->getMetadataPath();
    bf::path cloudPath = ls->getPrefix(); 
    
    // metaPath doesn't necessarily exist until a MetadataFile instance is created
    bf::create_directories(metaPath);
    
    /*  start with one object in cloud storage
        truncate past the end of the object
        verify nothing changed & got success
        truncate at 4000 bytes
        verify everything sees the 'file' as 4000 bytes
            - IOC + meta
        truncate at 0 bytes
        verify file now looks empty
        verify the object was deleted
        
        add 2 8k test objects and a journal against the second one
        truncate @ 10000 bytes
        verify all files still exist
        truncate @ 6000 bytes, 2nd object & journal were deleted
        truncate @ 0 bytes, verify no files are left
    */
    
    bf::path metadataFile = metaPath/"test-file.meta";
    bf::path objectPath = cloudPath/testObjKey;
    bf::path cachedObjectPath = cachePath/testObjKey;
    makeTestMetadata(metadataFile.string().c_str());
    makeTestObject(objectPath.string().c_str());
    int err;
    uint8_t buf[1<<14];
    
    // Extending a file doesn't quite work yet, punting on that part of the test for now
    
    err = ioc->truncate(testFile, 4000);
    assert(!err);
    MetadataFile meta(testFile);
    assert(meta.getLength() == 4000);
    
    // read the data, make sure there are only 4000 bytes & the object still exists
    err = ioc->read(testFile, buf, 0, 8192);
    assert(err == 4000);
    err = ioc->read(testFile, buf, 4000, 1);
    assert(err == 0);
    assert(bf::exists(objectPath));
    
    // truncate to 0 bytes, make sure everything is consistent with that, and the object no longer exists
    err = ioc->truncate(testFile, 0);
    assert(!err);
    meta = MetadataFile(testFile);
    assert(meta.getLength() == 0);
    err = ioc->read(testFile, buf, 0, 1);
    assert(err == 0);
    err = ioc->read(testFile, buf, 4000, 1);
    assert(err == 0);
    sleep(1);  // give Sync a chance to delete the object from the cloud
    assert(!bf::exists(objectPath));
    
    // recreate the meta file, make a 2-object version
    ::unlink(metadataFile.string().c_str());
    makeTestMetadata(metadataFile.string().c_str());
    makeTestObject(objectPath.string().c_str());
    
    meta = MetadataFile(testFile);
    bf::path secondObjectPath = cloudPath / meta.addMetadataObject(testFile, 8192).key;
    bf::path cachedSecondObject = cachePath/secondObjectPath.filename();
    makeTestObject(secondObjectPath.string().c_str());
    meta.writeMetadata(testFile);
    
    // make sure there are 16k bytes, and the data is valid before going forward
    memset(buf, 0, 16384);
    err = ioc->read(testFile, buf, 0, 16384);
    assert(err == 16384);
    int *buf32 = (int *) buf;
    for (int i = 0; i < 16384/4; i++)
        assert(buf32[i] == (i % 2048));
    assert(bf::exists(cachedSecondObject));
    assert(bf::exists(cachedObjectPath));
    
    // truncate to 10k, make sure everything looks right
    err = ioc->truncate(testFile, 10240);
    assert(!err);
    meta = MetadataFile(testFile);
    assert(meta.getLength() == 10240);
    memset(buf, 0, 16384);
    err = ioc->read(testFile, buf, 0, 10240);
    for (int i = 0; i < 10240/4; i++)
        assert(buf32[i] == (i % 2048));
    err = ioc->read(testFile, buf, 10239, 10);
    assert(err == 1);
    
    // truncate to 6000 bytes, make sure second object got deleted
    err = ioc->truncate(testFile, 6000);
    meta = MetadataFile(testFile);
    assert(meta.getLength() == 6000);
    err = ioc->read(testFile, buf, 0, 8192);
    assert(err == 6000);
    sleep(1);   // give Synchronizer a chance to delete the file from the 'cloud'
    assert(!bf::exists(secondObjectPath));
    assert(!bf::exists(cachedSecondObject));
    
    bf::remove(metadataFile);
    bf::remove(objectPath);
    cache->reset();
    cout << "IOCTruncate OK" << endl;
    return true;
}


bool truncatetask()
{
    IOCoordinator *ioc = IOCoordinator::get();
    Cache *cache = Cache::get();
    bf::path metaPath = ioc->getMetadataPath();
    
    const char *filename = "trunctest1";
    // get the metafile created
    string metaFullName = (metaPath/filename).string() + ".meta";
    ::unlink(metaFullName.c_str());
    MetadataFile meta(filename);

    uint8_t buf[1024];
    truncate_cmd *cmd = (truncate_cmd *) buf;
    
    cmd->opcode = TRUNCATE;
    cmd->length = 1000;
    cmd->flen = strlen(filename);
    strcpy((char *) cmd->filename, filename);
    
    ::write(sessionSock, cmd, sizeof(*cmd) + cmd->flen);
    TruncateTask t(clientSock, sizeof(*cmd) + cmd->flen);
    t.run();
    
    // read the response
    int err = ::recv(sessionSock, buf, 1024, MSG_DONTWAIT);
    sm_response *resp = (sm_response *) buf;
    assert(err == sizeof(sm_response));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.flags == 0);
    assert(resp->header.payloadLen == 4);
    assert(resp->returnCode == 0);
    
    // reload the metadata, check that it is 1000 bytes
    meta = MetadataFile(filename);
    assert(meta.getLength() == 1000);
    
    cache->reset();
    ::unlink(metaFullName.c_str());
    cout << "truncate task OK" << endl;
    return true;
}

bool listdirtask()
{
    IOCoordinator *ioc = IOCoordinator::get();
    const bf::path metaPath = ioc->getMetadataPath();
    const char *relPath = "listdirtask";
    bf::path tmpPath = metaPath/relPath;
    
    // make some dummy files, make sure they are in the list returned.
    set<string> files;
    int err;
    vector<SharedCloser> fdMinders;
    
    bf::create_directories(tmpPath);
    for (int i = 0; i < 10; i++) {
        
        string file(tmpPath.string() + "/dummy" + to_string(i));
        files.insert(file);
        file += ".meta";
        err = ::open(file.c_str(), O_CREAT | O_WRONLY, 0600);
        assert(err >= 0);
        fdMinders.push_back(err);
    }
    
    uint8_t buf[8192];
    listdir_cmd *cmd = (listdir_cmd *) buf;
    
    cmd->opcode = LIST_DIRECTORY;
    cmd->plen = strlen(relPath);
    memcpy(cmd->path, relPath, cmd->plen);
    
    ::write(sessionSock, cmd, sizeof(*cmd) + cmd->plen);
    ListDirectoryTask l(clientSock, sizeof(*cmd) + cmd->plen);
    l.run();

    /* going to keep this simple. Don't run this in a big dir. */
    /* maybe later I'll make a dir, put a file in it, and etc.  For now run it in a small dir. */
    err = ::recv(sessionSock, buf, 8192, MSG_DONTWAIT);
    sm_response *resp = (sm_response *) buf;
    assert(err > 0);
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 0);
    listdir_resp *r = (listdir_resp *) resp->payload;
    assert(r->elements == 10);
    int off = sizeof(sm_response) + sizeof(listdir_resp);
    int fileCounter = 0;
    while (off < err)
    {
        listdir_resp_entry *e = (listdir_resp_entry *) &buf[off];
        //cout << "len = " << e->flen << endl;
        assert(off + e->flen + sizeof(listdir_resp_entry) < 8192);
        string file(e->filename, e->flen);
        assert(files.find((tmpPath/file).string()) != files.end());
        fileCounter++;
        //cout << "name = " << file << endl;
        off += e->flen + sizeof(listdir_resp_entry);
    }
    assert(fileCounter == r->elements);
    bf::remove_all(tmpPath);
    return true;
}

bool pingtask()
{
    uint8_t buf[1024];
    ping_cmd *cmd = (ping_cmd *) buf;
    cmd->opcode = PING;
    
    ::write(sessionSock, cmd, sizeof(*cmd));
    PingTask p(clientSock, sizeof(*cmd));
    p.run();
    
    // read the response
    int err = ::recv(sessionSock, buf, 1024, MSG_DONTWAIT);
    sm_response *resp = (sm_response *) buf;
    assert(err == sizeof(sm_response));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.payloadLen == 4);
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 0);
    cout << "pingtask OK" << endl;
}

bool copytask()
{
    /*  
        make a file
        copy it
        verify it exists
    */
    const char *source = "dummy1";
    const char *dest = "dummy2";
    MetadataFile meta1(source);
    
    uint8_t buf[1024];
    copy_cmd *cmd = (copy_cmd *) buf;
    cmd->opcode = COPY;
    cmd->file1.flen = strlen(source);
    strncpy(cmd->file1.filename, source, cmd->file1.flen);
    f_name *file2 = (f_name *) &cmd->file1.filename[cmd->file1.flen];
    file2->flen = strlen(dest);
    strncpy(file2->filename, dest, file2->flen);
    
    uint len = (uint64_t) &file2->filename[file2->flen] - (uint64_t) buf;
    ::write(sessionSock, buf, len);
    CopyTask c(clientSock, len);
    c.run();
    
    // read the response
    int err = ::recv(sessionSock, buf, 1024, MSG_DONTWAIT);
    sm_response *resp = (sm_response *) buf;
    assert(err == sizeof(sm_response));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.payloadLen == 4);
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 0);
    
    // verify copytest2 is there
    MetadataFile meta2(dest, MetadataFile::no_create_t());
    assert(meta2.exists());
    
    bf::path metaPath = IOCoordinator::get()->getMetadataPath();
    bf::remove(metaPath/(string(source) + ".meta"));
    bf::remove(metaPath/(string(dest) + ".meta"));
    cout << "copytask OK " << endl;
    return true;
}

bool localstorageTest1()
{
    LocalStorage ls;
    
    /* TODO: Some stuff */
    cout << "local storage test 1 OK" << endl;
    return true;
}

bool cacheTest1()
{

    Cache *cache = Cache::get();
    CloudStorage *cs = CloudStorage::get();
    LocalStorage *ls = dynamic_cast<LocalStorage *>(cs);
    if (ls == NULL) {
        cout << "Cache test 1 requires using local storage" << endl;
        return false;
    }
    
    cache->reset();
    assert(cache->getCurrentCacheSize() == 0);
    
    bf::path storagePath = ls->getPrefix();
    bf::path cachePath = cache->getCachePath();
    vector<string> v_bogus;
    vector<bool> exists;
    
    // make sure nothing shows up in the cache path for files that don't exist
    v_bogus.push_back("does-not-exist");
    cache->read(v_bogus);
    assert(!bf::exists(cachePath / "does-not-exist"));
    cache->exists(v_bogus, &exists);
    assert(exists.size() == 1);
    assert(!exists[0]);
    
    // make sure a file that does exist does show up in the cache path
    string realFile("storagemanager.cnf");
    bf::copy_file(realFile, storagePath / realFile, bf::copy_option::overwrite_if_exists);
    v_bogus[0] = realFile;
    cache->read(v_bogus);
    assert(bf::exists(cachePath / realFile));
    exists.clear();
    cache->exists(v_bogus, &exists);
    assert(exists.size() == 1);
    assert(exists[0]);
    ssize_t currentSize = cache->getCurrentCacheSize();
    assert(currentSize == bf::file_size(cachePath / realFile));
    
    // lie about the file being deleted and then replaced
    cache->deletedObject(realFile, currentSize);
    assert(cache->getCurrentCacheSize() == 0);
    cache->newObject(realFile, currentSize);
    assert(cache->getCurrentCacheSize() == currentSize);
    cache->exists(v_bogus, &exists);
    assert(exists.size() == 1);
    assert(exists[0]);
    
    // cleanup
    bf::remove(cachePath / realFile);
    bf::remove(storagePath / realFile);
    cout << "cache test 1 OK" << endl;
}



bool mergeJournalTest()
{
    /*
        create a dummy object and a dummy journal
        call mergeJournal to process it with various params
        verify the expected values
    */
    
    makeTestObject("test-object");
    makeTestJournal("test-journal");
    
    int i;
    IOCoordinator *ioc = IOCoordinator::get();
    size_t len = 0;
    boost::shared_array<uint8_t> data = ioc->mergeJournal("test-object", "test-journal", 0, &len);
    assert(data);
    int *idata = (int *) data.get();
    for (i = 0; i < 5; i++)
        assert(idata[i] == i);
    for (; i < 10; i++)
        assert(idata[i] == i-5);
    for (; i < 2048; i++)
        assert(idata[i] == i);
        
    // try different range parameters
    // read at the beginning of the change
    len = 40;
    data = ioc->mergeJournal("test-object", "test-journal", 20, &len);
    assert(data);
    idata = (int *) data.get();
    for (i = 0; i < 5; i++)
        assert(idata[i] == i);
    for (; i < 10; i++)
        assert(idata[i] == i+5);
    
    // read s.t. beginning of the change is in the middle of the range
    len = 24;
    data = ioc->mergeJournal("test-object", "test-journal", 8, &len);
    assert(data);
    idata = (int *) data.get();
    for (i = 0; i < 3; i++)
        assert(idata[i] == i + 2);
    for (; i < 6; i++)
        assert(idata[i] == i - 3);
    
    // read s.t. end of the change is in the middle of the range
    len = 20;
    data = ioc->mergeJournal("test-object", "test-journal", 28, &len);
    assert(data);
    idata = (int *) data.get();
    for (i = 0; i < 3; i++)
        assert(idata[i] == i + 2);
    for (; i < 3; i++)
        assert(idata[i] == i + 7);
    
    // cleanup
    bf::remove("test-object");
    bf::remove("test-journal");
    cout << "mergeJournalTest OK" << endl;
    return true;
}

bool syncTest1()
{
    Config *config = Config::get();
    Synchronizer *sync = Synchronizer::get();
    Cache *cache = Cache::get();
    CloudStorage *cs = CloudStorage::get();
    LocalStorage *ls = dynamic_cast<LocalStorage *>(cs);
    if (!ls)
    {
        cout << "syncTest1() requires using local storage at the moment." << endl;
        return true;
    }
    
    cache->reset();
    
    // delete everything in the fake cloud to make it easier to list later
    bf::path fakeCloudPath = ls->getPrefix();
    for (bf::directory_iterator dir(fakeCloudPath); dir != bf::directory_iterator(); ++dir)
        bf::remove(dir->path());
    
    bf::path cachePath = sync->getCachePath();
    bf::path journalPath = sync->getJournalPath();
    
    string stmp = config->getValue("ObjectStorage", "metadata_path");
    assert(!stmp.empty());
    bf::path metaPath = stmp;
    // nothing creates the dir yet
    bf::create_directories(metaPath);
    
    // make the test obj, journal, and metadata
    string key = "12345_0_8192_test-file";
    string journalName = key + ".journal";
    
    makeTestObject((cachePath/key).string().c_str());
    makeTestJournal((journalPath/journalName).string().c_str());
    makeTestMetadata((metaPath/"test-file.meta").string().c_str());
    
    cache->newObject(key, bf::file_size(cachePath/key));
    cache->newJournalEntry(bf::file_size(journalPath/journalName));

    vector<string> vObj;
    vObj.push_back(key);
    
    sync->newObjects(vObj);
    sleep(1);  // wait for the job to run
    
    // make sure that it made it to the cloud
    bool exists = false;
    int err = cs->exists(key, &exists);
    assert(!err);
    assert(exists);
    
    sync->newJournalEntry(key);
    sleep(1);  // let it do what it does
    
    // check that the original objects no longer exist
    assert(!cache->exists(key));
    assert(!bf::exists(journalPath/journalName));
    
    // Replicator doesn't implement all of its functionality yet, need to delete key from the cache manually for now
    bf::remove(cachePath/key);
    
    // check that a new version of object exists in cloud storage
    // D'oh, this would have to list the objects to find it, not going to implement 
    // that everywhere just now.  For now, making this test require LocalStorage.
    bool foundIt = false;
    string newKey;
    for (bf::directory_iterator dir(fakeCloudPath); dir != bf::directory_iterator() && !foundIt; ++dir)
    {
        newKey = dir->path().filename().string();
        foundIt = (MetadataFile::getSourceFromKey(newKey) == "test-file");
        if (foundIt)
        {
            size_t fsize = bf::file_size(dir->path());
            assert(cache->exists(newKey));
            cs->deleteObject(newKey);
            break;
        }
    }
    
    assert(foundIt);
    cache->makeSpace(cache->getMaxCacheSize());   // clear the cache & make it call sync->flushObject() 
    
    // the key should now be back in cloud storage and deleted from the cache
    assert(!cache->exists(newKey));
    err = cs->exists(newKey, &exists);
    assert(!err && exists);
    
    // make the journal again, call sync->newJournalObject()
    makeTestJournal((journalPath / (newKey + ".journal")).string().c_str());
    cache->newJournalEntry(bf::file_size(journalPath / (newKey + ".journal")));
    sync->newJournalEntry(newKey);
    sleep(1);
    
    // verify that newkey is no longer in cloud storage, and that another permutation is
    err = cs->exists(newKey, &exists);
    assert(!err && !exists);
    foundIt = false;
    for (bf::directory_iterator dir(fakeCloudPath); dir != bf::directory_iterator() && !foundIt; ++dir)
    {
        key = dir->path().filename().string();
        foundIt = (MetadataFile::getSourceFromKey(key) == "test-file");
    }
    assert(foundIt);
    
    // TODO test error paths, pass in some junk
    
    // cleanup, just blow away everything for now
    cache->reset();
    vector<string> keys;
    for (bf::directory_iterator dir(fakeCloudPath); dir != bf::directory_iterator(); ++dir)
        keys.push_back(dir->path().filename().string());
    sync->deletedObjects(keys);
    ::unlink((metaPath/"test-file.meta").string().c_str());
    
    cout << "Sync test 1 OK" << endl;
    return true;
}

void metadataUpdateTest()
{
    Config* config = Config::get();
    string metaPath = config->getValue("ObjectStorage", "metadata_path");
    MetadataFile mdfTest("metadataUpdateTest");
    mdfTest.addMetadataObject("metadataUpdateTest",100);
    mdfTest.printObjects();
    mdfTest.updateEntryLength(0,200);
    mdfTest.printObjects();
    //mdfTest.updateEntryLength(0,100);
    //mdfTest.printObjects();
    string metaFilePath = metaPath + "/" + "metadataUpdateTest.meta";
    ::unlink(metaFilePath.c_str());

}    

void s3storageTest1()
{
    if (!getenv("AWS_ACCESS_KEY_ID") || !getenv("AWS_SECRET_ACCESS_KEY"))
    {
        cout << "s3storageTest1 requires exporting your AWS creds, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY" << endl;
        return;
    }

    S3Storage s3;
    bool exists;
    int err;
    string testFile = "storagemanager.cnf";
    string testFile2 = testFile + "2";
    
    exists = bf::exists(testFile);
    if (!exists)
    {
        cout << "s3storageTest1() requires having " << testFile << " in the current directory.";
        return;
    }
    
    try {
        err = s3.exists(testFile, &exists);
        assert(!err);
        if (exists)
            s3.deleteObject(testFile);
            
        err = s3.exists(testFile2, &exists);
        assert(!err);
        if (exists)
            s3.deleteObject(testFile2);
            
        // put it & get it
        err = s3.putObject(testFile, testFile);
        assert(!err);
        err = s3.exists(testFile, &exists);
        assert(!err);
        assert(exists);
        err = s3.getObject(testFile, testFile2);
        assert(!err);
        exists = bf::exists(testFile2);
        assert(bf::file_size(testFile) == bf::file_size(testFile2));
        
        // do a deep compare testFile vs testFile2
        size_t len = bf::file_size(testFile);
        int fd1 = open(testFile.c_str(), O_RDONLY);
        assert(fd1 >= 0);
        int fd2 = open(testFile2.c_str(), O_RDONLY);
        assert(fd2 >= 0);
        
        uint8_t *data1 = new uint8_t[len];
        uint8_t *data2 = new uint8_t[len];
        err = read(fd1, data1, len);
        assert(err == len);
        err = read(fd2, data2, len);
        assert(err == len);
        assert(!memcmp(data1, data2, len));
        close(fd1);
        close(fd2);
        delete [] data1;
        delete [] data2;
        
        err = s3.copyObject(testFile, testFile2);
        assert(!err);
        err = s3.exists(testFile2, &exists);
        assert(!err);
        assert(exists);
        s3.deleteObject(testFile);
        s3.deleteObject(testFile2);
    }
    catch(exception &e)
    {
        cout << __FUNCTION__ << " caught " << e.what() << endl;
        assert(0);
    }
    cout << "S3Storage Test 1 OK" << endl;
}

void IOCReadTest1()
{
    /*  Generate the test object & metadata
        read it, verify result
        
        Generate the journal object
        read it, verify the merged result
        
        TODO: do partial reads with an offset similar to what the mergeJournal tests do
        TODO: some error path testing
    */
    Cache *cache = Cache::get();
    CloudStorage *cs = CloudStorage::get();
    IOCoordinator *ioc = IOCoordinator::get();
    Config *config = Config::get();
    LocalStorage *ls = dynamic_cast<LocalStorage *>(cs);
    if (!ls)
    {
        cout << "IOC read test 1 requires LocalStorage for now." << endl;
        return;
    }
        
    bf::path storagePath = ls->getPrefix();
    bf::path cachePath = cache->getCachePath();
    bf::path journalPath = cache->getJournalPath();
    bf::path metaPath = config->getValue("ObjectStorage", "metadata_path");
    assert(!metaPath.empty());
    bf::create_directories(metaPath);
    
    string objFilename = (storagePath/testObjKey).string();
    string journalFilename = (journalPath/testObjKey).string() + ".journal";
    string metaFilename = (metaPath/testFile).string() + ".meta";
    
    cache->reset();
    bf::remove(objFilename);
    bf::remove(journalFilename);
    bf::remove(metaFilename);
    
    int err;
    boost::scoped_array<uint8_t> data(new uint8_t[1<<20]);
    memset(data.get(), 0, 1<<20);
    err = ioc->read(testFile, data.get(), 0, 1<<20);
    assert(err < 0);
    assert(errno == ENOENT);
    
    makeTestObject(objFilename.c_str());
    makeTestMetadata(metaFilename.c_str());
    size_t objSize = bf::file_size(objFilename);
    err = ioc->read(testFile, data.get(), 0, 1<<20);
    assert(err == objSize);
    
    // verify the data
    int *data32 = (int *) data.get();
    int i;
    for (i = 0; i < 2048; i++)
        assert(data32[i] == i);
    for (; i < (1<<20)/4; i++)
        assert(data32[i] == 0);
        
    makeTestJournal(journalFilename.c_str());
    
    err = ioc->read(testFile, data.get(), 0, 1<<20);
    assert(err == objSize);
    for (i = 0; i < 5; i++)
        assert(data32[i] == i);
    for (; i < 10; i++)
        assert(data32[i] == i-5);
    for (; i < 2048; i++)
        assert(data32[i] == i);
    for (; i < (1<<20)/4; i++)
        assert(data32[i] == 0);
    
    cache->reset();
    bf::remove(objFilename);
    bf::remove(journalFilename);
    bf::remove(metaFilename);
    
    cout << "IOC read test 1 OK" << endl;
}

void IOCUnlink()
{
    IOCoordinator *ioc = IOCoordinator::get();
    CloudStorage *cs = CloudStorage::get();
    Cache *cache = Cache::get();
    Synchronizer *sync = Synchronizer::get();
    
    cache->reset();
    
    /* 
        Make a metadata file with a complex path
        make the test object and test journal
        delete it at the parent dir level
        make sure the parent dir was deleted
        make sure the object and journal were deleted
    */
    
    bf::path metaPath = ioc->getMetadataPath();
    bf::path cachePath = ioc->getCachePath();
    bf::path journalPath = ioc->getJournalPath();
    bf::path cachedObjPath = cachePath/testObjKey;
    bf::path cachedJournalPath = journalPath/(string(testObjKey) + ".journal");
    bf::path basedir = "unlinktest";
    bf::path metadataFile = metaPath/basedir/(string(testFile) + ".meta");
    bf::create_directories(metaPath/basedir);
    makeTestMetadata(metadataFile.string().c_str());
    makeTestObject(cachedObjPath.string().c_str());
    makeTestJournal(cachedJournalPath.string().c_str());
    
    cache->newObject(cachedObjPath.filename().string(), bf::file_size(cachedObjPath));
    cache->newJournalEntry(bf::file_size(cachedJournalPath));
    vector<string> keys;
    keys.push_back(cachedObjPath.filename().string());
    sync->newObjects(keys);
    //sync->newJournalEntry(keys[0]);    don't want to end up renaming it
    sleep(1);
    
    // ok, they should be fully 'in the system' now.
    // verify that they are
    assert(bf::exists(metaPath/basedir));
    assert(bf::exists(cachedObjPath));
    assert(bf::exists(cachedJournalPath));
    bool exists;
    cs->exists(cachedObjPath.filename().string(), &exists);
    assert(exists);
    
    int err = ioc->unlink(basedir.string().c_str());
    assert(err == 0);
    
    assert(!bf::exists(metaPath/basedir));
    assert(!bf::exists(cachedObjPath));
    assert(!bf::exists(cachedJournalPath));
    sleep(1);   // stall for sync
    cs->exists(cachedObjPath.filename().string(), &exists);
    assert(!exists);
    assert(cache->getCurrentCacheSize() == 0);
    
    cout << "IOC unlink test OK" << endl;
}

void IOCCopyFile1()
{
    /*
        Make our usual test files
            with metadata in a subdir
            with object in cloud storage
        call ioc::copyFile()
            with dest in a different subdir
        verify the contents
    */
    IOCoordinator *ioc = IOCoordinator::get();
    Cache *cache = Cache::get();
    CloudStorage *cs = CloudStorage::get();
    LocalStorage *ls = dynamic_cast<LocalStorage *>(cs);
    if (!ls)
    {
        cout << "IOCCopyFile1 requires local storage at the moment" << endl;
        return;
    }
    
    bf::path metaPath = ioc->getMetadataPath();
    bf::path csPath = ls->getPrefix();
    bf::path journalPath = ioc->getJournalPath();
    bf::path sourcePath = metaPath/"copyfile1"/"source.meta";
    bf::path destPath = metaPath/"copyfile2"/"dest.meta";
    const char *l_sourceFile = "copyfile1/source";
    const char *l_destFile = "copyfile2/dest";
    
    cache->reset();
    
    bf::create_directories(sourcePath.parent_path());
    makeTestMetadata(sourcePath.string().c_str());
    makeTestObject((csPath/testObjKey).string().c_str());
    makeTestJournal((journalPath/(string(testObjKey) + ".journal")).string().c_str());
    cache->newJournalEntry(bf::file_size(journalPath/(string(testObjKey) + ".journal")));
    
    int err = ioc->copyFile("copyfile1/source", "copyfile2/dest");
    assert(!err);
    uint8_t buf1[8192], buf2[8192];
    err = ioc->read(l_sourceFile, buf1, 0, 8192);
    assert(err == 8192);
    err = ioc->read(l_destFile, buf2, 0, 8192);
    assert(err == 8192);
    assert(memcmp(buf1, buf2, 8192) == 0);
    
    ioc->unlink("copyfile1");
    ioc->unlink("copyfile2");
    assert(cache->getCurrentCacheSize() == 0);
    cout << "IOC copy file 1 OK" << endl;
}

void IOCCopyFile2()
{
    // call IOC::copyFile() with non-existant file
    IOCoordinator *ioc = IOCoordinator::get();
    
    bf::path metaPath = ioc->getMetadataPath();
    bf::remove(metaPath/"not-there.meta");
    bf::remove(metaPath/"not-there2.meta");
    
    int err = ioc->copyFile("not-there", "not-there2");
    assert(err);
    assert(errno == ENOENT);
    assert(!bf::exists(metaPath/"not-there.meta"));
    assert(!bf::exists(metaPath/"not-there2.meta"));
    
    cout << "IOC copy file 2 OK" << endl;
}

void IOCCopyFile3()
{
    /*
        Make our usual test files
            with object in the cache not in CS
        call ioc::copyFile()
        verify dest file exists
    */
        IOCoordinator *ioc = IOCoordinator::get();
    Cache *cache = Cache::get();
    CloudStorage *cs = CloudStorage::get();
    
    bf::path metaPath = ioc->getMetadataPath();
    bf::path journalPath = ioc->getJournalPath();
    bf::path cachePath = ioc->getCachePath();
    bf::path sourcePath = metaPath/"copyfile1"/"source.meta";
    bf::path destPath = metaPath/"copyfile2"/"dest.meta";
    const char *l_sourceFile = "copyfile1/source";
    const char *l_destFile = "copyfile2/dest";
    
    cache->reset();
    
    bf::create_directories(sourcePath.parent_path());
    makeTestMetadata(sourcePath.string().c_str());
    makeTestObject((cachePath/testObjKey).string().c_str());
    makeTestJournal((journalPath/(string(testObjKey) + ".journal")).string().c_str());
    cache->newObject(testObjKey, bf::file_size(cachePath/testObjKey));
    cache->newJournalEntry(bf::file_size(journalPath/(string(testObjKey) + ".journal")));
    
    int err = ioc->copyFile("copyfile1/source", "copyfile2/dest");
    assert(!err);
    uint8_t buf1[8192], buf2[8192];
    err = ioc->read(l_sourceFile, buf1, 0, 8192);
    assert(err == 8192);
    err = ioc->read(l_destFile, buf2, 0, 8192);
    assert(err == 8192);
    assert(memcmp(buf1, buf2, 8192) == 0);
    
    ioc->unlink("copyfile1");
    ioc->unlink("copyfile2");
    assert(cache->getCurrentCacheSize() == 0);
    cout << "IOC copy file 3 OK" << endl;
}

void IOCCopyFile()
{
    IOCCopyFile1();
    IOCCopyFile2();
    IOCCopyFile3();
}

int main()
{

    std::size_t sizeKB = 1024;
    cout << "connecting" << endl;
    makeConnection();
    cout << "connected" << endl;
    scoped_closer sc1(serverSock), sc2(sessionSock), sc3(clientSock);
    
    opentask();
    metadataUpdateTest();
    // requires 8K object size to test boundries
    //Case 1 new write that spans full object
    metadataJournalTest((10*sizeKB),0);
    //Case 2 write data beyond end of data in object 2 that still ends in object 2
    metadataJournalTest((4*sizeKB),(8*sizeKB));
    //Case 3 write spans 2 journal objects
    metadataJournalTest((8*sizeKB),(4*sizeKB));
    //Case 4 write starts object1 ends object3
    metadataJournalTest((10*sizeKB),(7*sizeKB));
    //Case 5 write starts in new object at offset >0
    //TODO add zero padding to writes in this scenario
    //metadataJournalTest((8*sizeKB),4*sizeKB);
    metadataJournalTestCleanup(17*sizeKB);

    //writetask();
    appendtask();
    unlinktask();
    stattask();
    //truncatetask();   // currently waiting on IOC::write() to be completed.
    listdirtask();
    pingtask();
    copytask();
    
    localstorageTest1();
    cacheTest1();
    mergeJournalTest();
    replicatorTest();
    syncTest1();
    
    s3storageTest1();
    IOCReadTest1();
    IOCTruncate();
    IOCUnlink();
    IOCCopyFile();

    return 0;
}

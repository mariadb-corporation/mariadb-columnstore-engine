/* Copyright (C) 2019 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

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
#include "Synchronizer.h"
#include "ProcessTask.h"

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
#include <boost/algorithm/string/replace.hpp>
#include <algorithm>
#include <random>

#undef NDEBUG
#include <cassert>

using namespace storagemanager;
using namespace std;
namespace bf = boost::filesystem;

void printUsage()
{
  cout << "MariaDB Columnstore Storage Manager Unit Test\n"
       << endl
       << "Usage unit_test [OPTION] " << endl
       << "-d [test_data]          Location of test_data included with source code" << endl
       << "                          Default = ./" << endl
       << "-p [prefix]            This directory will be used as scratch space for tests run" << endl
       << "                          Default = unittest" << endl;
}

struct scoped_closer
{
  scoped_closer(int f) : fd(f)
  {
  }
  ~scoped_closer()
  {
    int s_errno = errno;
    ::close(fd);
    errno = s_errno;
  }
  int fd;
};

// (ints) 0 1 2 3 ... 2048
void makeTestObject(const char* dest)
{
  int objFD = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  assert(objFD >= 0);
  scoped_closer s1(objFD);

  for (int i = 0; i < 2048; i++)
    assert(write(objFD, &i, 4) == 4);
}

// the merged version should look like
// (ints)  0 1 2 3 4 0 1 2 3 4 10 11 12 13...
void makeTestJournal(const char* dest)
{
  int journalFD = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  assert(journalFD >= 0);
  scoped_closer s2(journalFD);

  char header[] = "{ \"version\" : 1, \"max_offset\": 39 }";
  size_t result = write(journalFD, header, strlen(header) + 1);
  assert(result == (strlen(header) + 1));
  uint64_t offlen[2] = {20, 20};
  result = write(journalFD, offlen, 16);
  for (int i = 0; i < 5; i++)
    assert(write(journalFD, &i, 4) == 4);
}

bf::path testDirPath = "./";
bf::path homepath = getenv("HOME");
string prefix = "unittest";

string testObjKey = "12345_0_8192_" + prefix + "~test-file";
string copyfileObjKey = "12345_0_8192_" + prefix + "~source";
string metaTestFile = prefix + "/test-file";
bf::path testFilePath = homepath / metaTestFile;
const char* testFile = testFilePath.string().c_str();

string _metadata =
    "{ \n\
    \"version\" : 1, \n\
    \"revision\" : 1, \n\
    \"objects\" : \n\
    [ \n\
        { \n\
            \"offset\" : 0, \n\
            \"length\" : 8192, \n\
            \"key\" : \"xxx\" \n\
        } \n\
    ] \n\
}\n";

void makeTestMetadata(const char* dest, string& key)
{
  int metaFD = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0600);
  assert(metaFD >= 0);
  scoped_closer sc(metaFD);
  boost::algorithm::replace_all(_metadata, "xxx", key);
  // need to parameterize the object name in the objects list
  size_t result = write(metaFD, _metadata.c_str(), _metadata.length());
  assert(result == _metadata.length());
  boost::algorithm::replace_all(_metadata, key, "xxx");
}

int getSocket()
{
  int sock = ::socket(AF_UNIX, SOCK_STREAM, 0);
  assert(sock >= 0);
  return sock;
}

int sessionSock = -1;  // tester uses this end of the connection
int serverSock = -1;
int clientSock = -1;  // have the Tasks use this end of the connection

void acceptConnection()
{
  int err;
  if (serverSock == -1)
  {
    serverSock = getSocket();

    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    memcpy(&sa.sun_path[1], "testing", 7);

    err = ::bind(serverSock, (struct sockaddr*)&sa, sizeof(sa));
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
  sleep(1);  // let server thread get to accept()
  int err = ::connect(clientSock, (struct sockaddr*)&sa, sizeof(sa));
  assert(err == 0);
  t.join();
}

bool opentask(bool connectionTest = false)
{
  // going to rely on msgs being smaller than the buffer here
  int err = 0;
  uint8_t buf[1024];
  sm_msg_header* hdr = (sm_msg_header*)buf;
  open_cmd* cmd = (open_cmd*)&hdr[1];
  string testFile = "metadataJournalTest";
  // open/create a file named 'opentest1'
  std::string filename = homepath.string() + "/" + prefix + "/" + testFile;
  hdr->type = SM_MSG_START;
  hdr->flags = 0;
  hdr->payloadLen = sizeof(*cmd) + filename.size();
  cmd->opcode = OPEN;
  cmd->openmode = O_WRONLY | O_CREAT;
  cmd->flen = 19;
  strcpy((char*)cmd->filename, filename.c_str());

  cout << "open file " << filename << endl;
  ::unlink(filename.c_str());

  // set payload to be shorter than actual message lengh
  // and send a shortened message.
  if (connectionTest)
    hdr->payloadLen -= 2;

  size_t result = ::write(sessionSock, cmd, hdr->payloadLen);
  assert(result == (hdr->payloadLen));

  // set payload to be correct length again
  if (connectionTest)
    hdr->payloadLen += 2;

  // process task will look for the full length and
  // will wait on the rest of the message.
  ProcessTask pt(clientSock, hdr->payloadLen);
  boost::thread t(pt);

  if (connectionTest)
  {
    // make sure the thread is waiting for the rest of the data
    // then kill the connection. This will trigger the task thread
    // to exit on an error handling path
    sleep(1);
    close(sessionSock);
    close(clientSock);
    err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
    assert(err == -1);
    t.join();
  }
  else
  {
    t.join();
    // read the response
    err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
    sm_response* resp = (sm_response*)buf;
    assert(err == sizeof(struct stat) + sizeof(sm_response));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.payloadLen == sizeof(struct stat) + sizeof(ssize_t));
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 0);
    struct stat* _stat = (struct stat*)resp->payload;

    // what can we verify about the stat...
    assert(_stat->st_size == 0);
    /* verify the file is there */
    string metaPath = Config::get()->getValue("ObjectStorage", "metadata_path");
    assert(!metaPath.empty());
    metaPath += string("/" + prefix + "/" + testFile + ".meta");

    assert(boost::filesystem::exists(metaPath));
  }

  cout << "opentask OK" << endl;
  return true;
}

bool replicatorTest()
{
  Config* config = Config::get();
  string metaPath = config->getValue("ObjectStorage", "metadata_path");
  string journalPath = config->getValue("ObjectStorage", "journal_path");
  string cacehPath = config->getValue("Cache", "path");

  Replicator* repli = Replicator::get();
  int err, fd;
  string newobject = prefix + "/newobjectTest";
  string newObjectJournalFullPath = journalPath + "/" + prefix + "/newobjectTest.journal";
  string newObjectCacheFullPath = cacehPath + "/" + prefix + "/newobjectTest";
  uint8_t buf[1024];
  uint8_t data[1024];
  int version = 1;
  uint64_t max_offset = 0;
  memcpy(data, "1234567890", 10);
  string header =
      (boost::format("{ \"version\" : \"%03i\", \"max_offset\" : \"%011u\" }") % version % max_offset).str();

  // test newObject
  repli->newObject(newobject, data, 0, 10);

  // check file contents
  fd = ::open(newObjectCacheFullPath.c_str(), O_RDONLY);
  err = ::read(fd, buf, sizeof(buf));
  assert(err == 10);
  buf[10] = 0;
  assert(!strcmp("1234567890", (const char*)buf));
  cout << "replicator newObject OK" << endl;
  ::close(fd);

  // test addJournalEntry
  repli->addJournalEntry(newobject, data, 0, 10);

  fd = ::open(newObjectJournalFullPath.c_str(), O_RDONLY);
  err = ::read(fd, buf, sizeof(buf));
  assert((uint)err == (header.length() + 1 + 16 + 10));
  buf[err] = 0;
  assert(!strcmp("1234567890", (const char*)buf + header.length() + 1 + 16));
  cout << "replicator addJournalEntry OK" << endl;
  ::close(fd);

  repli->remove(newObjectCacheFullPath.c_str());
  repli->remove(newObjectJournalFullPath.c_str());
  assert(!boost::filesystem::exists(newObjectCacheFullPath.c_str()));
  cout << "replicator remove OK" << endl;
  return true;
}

void metadataJournalTest(std::size_t size, off_t offset)
{
  // make an empty file to write to
  bf::path fullPath = homepath / prefix / "metadataJournalTest";
  const char* filename = fullPath.string().c_str();
  std::vector<uint8_t> buf(sizeof(write_cmd) + std::strlen(filename) + size);
  uint64_t* data;

  sm_msg_header* hdr = (sm_msg_header*)buf.data();
  write_cmd* cmd = (write_cmd*)&hdr[1];
  cmd->opcode = WRITE;
  cmd->offset = offset;
  cmd->count = size;
  cmd->flen = std::strlen(filename);
  memcpy(&cmd->filename, filename, cmd->flen);
  data = (uint64_t*)&cmd->filename[cmd->flen];
  int count = 0;
  for (uint64_t i = 0; i < (size / sizeof(uint64_t)); i++)
  {
    data[i] = i;
    count++;
  }
  hdr->type = SM_MSG_START;
  hdr->payloadLen = sizeof(*cmd) + cmd->flen + cmd->count;
  WriteTask w(clientSock, hdr->payloadLen);
  int err = ::write(sessionSock, cmd, hdr->payloadLen);

  w.run();

  // verify response
  uint8_t bufRead[1024];
  err = ::recv(sessionSock, bufRead, sizeof(bufRead), MSG_DONTWAIT);
  sm_response* resp = (sm_response*)bufRead;
  assert(err == sizeof(*resp));
  assert(resp->header.type == SM_MSG_START);
  assert(resp->header.payloadLen == sizeof(ssize_t));
  assert(resp->header.flags == 0);
  assert(resp->returnCode == (int)size);
}

void metadataJournalTest_append(std::size_t size)
{
  // make an empty file to write to
  bf::path fullPath = homepath / prefix / "metadataJournalTest";
  const char* filename = fullPath.string().c_str();

  std::vector<uint8_t> buf(sizeof(write_cmd) + std::strlen(filename) + size);
  uint64_t* data;

  sm_msg_header* hdr = (sm_msg_header*)buf.data();
  append_cmd* cmd = (append_cmd*)&hdr[1];
  cmd->opcode = APPEND;
  cmd->count = size;
  cmd->flen = std::strlen(filename);
  memcpy(&cmd->filename, filename, cmd->flen);
  data = (uint64_t*)&cmd->filename[cmd->flen];
  int count = 0;
  for (uint64_t i = 0; i < (size / sizeof(uint64_t)); i++)
  {
    data[i] = i;
    count++;
  }
  hdr->type = SM_MSG_START;
  hdr->payloadLen = sizeof(*cmd) + cmd->flen + cmd->count;
  AppendTask a(clientSock, hdr->payloadLen);
  int err = ::write(sessionSock, cmd, hdr->payloadLen);

  a.run();

  // verify response
  uint8_t bufRead[1024];
  err = ::recv(sessionSock, bufRead, sizeof(bufRead), MSG_DONTWAIT);
  sm_response* resp = (sm_response*)bufRead;
  assert(err == sizeof(*resp));
  assert(resp->header.type == SM_MSG_START);
  assert(resp->header.payloadLen == sizeof(ssize_t));
  assert(resp->header.flags == 0);
  assert(resp->returnCode == (int)size);
}

void metadataJournalTestCleanup()
{
  IOCoordinator* ioc = IOCoordinator::get();
  Synchronizer* sync = Synchronizer::get();
  bf::path fullPath = homepath / prefix / "metadataJournalTest";
  ioc->unlink(fullPath.string().c_str());
  sync->forceFlush();
}

bool writetask()
{
  // make an empty file to write to
  bf::path fullPath = homepath / prefix / "writetest1";
  const char* filename = fullPath.string().c_str();
  ::unlink(filename);
  int fd = ::open(filename, O_CREAT | O_RDWR, 0666);
  assert(fd > 0);
  scoped_closer f(fd);

  uint8_t buf[1024];
  sm_msg_header* hdr = (sm_msg_header*)buf;
  write_cmd* cmd = (write_cmd*)&hdr[1];
  uint8_t* data;

  cmd->opcode = WRITE;
  cmd->offset = 0;
  cmd->count = 9;
  cmd->flen = 10;
  memcpy(&cmd->filename, filename, cmd->flen);
  data = (uint8_t*)&cmd->filename[cmd->flen];
  memcpy(data, "123456789", cmd->count);

  hdr->type = SM_MSG_START;
  hdr->payloadLen = sizeof(*cmd) + cmd->flen + cmd->count;

  WriteTask w(clientSock, hdr->payloadLen);
  ssize_t result = ::write(sessionSock, cmd, hdr->payloadLen);
  assert(result == static_cast<ssize_t>(hdr->payloadLen));

  w.run();

  // verify response
  int err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
  sm_response* resp = (sm_response*)buf;
  assert(err == sizeof(*resp));
  assert(resp->header.type == SM_MSG_START);
  assert(resp->header.payloadLen == sizeof(ssize_t));
  assert(resp->header.flags == 0);
  assert(resp->returnCode == 9);

  // check file contents
  err = ::read(fd, buf, sizeof(buf));
  assert(err == 9);
  buf[9] = 0;
  assert(!strcmp("123456789", (const char*)buf));
  ::unlink(filename);
  cout << "write task OK" << endl;
  return true;
}

bool appendtask()
{
  // make a file and put some stuff in it
  bf::path fullPath = homepath / prefix / "appendtest1";
  const char* filename = fullPath.string().c_str();
  ::unlink(filename);
  int fd = ::open(filename, O_CREAT | O_RDWR, 0666);
  assert(fd > 0);
  scoped_closer f(fd);
  int err = ::write(fd, "testjunk", 8);
  assert(err == 8);

  uint8_t buf[1024];
  append_cmd* cmd = (append_cmd*)buf;
  uint8_t* data;

  cmd->opcode = APPEND;
  cmd->count = 9;
  cmd->flen = 11;
  memcpy(&cmd->filename, filename, cmd->flen);
  data = (uint8_t*)&cmd->filename[cmd->flen];
  memcpy(data, "123456789", cmd->count);

  int payloadLen = sizeof(*cmd) + cmd->flen + cmd->count;

  AppendTask a(clientSock, payloadLen);
  ssize_t result = ::write(sessionSock, cmd, payloadLen);
  assert(result == (payloadLen));

  a.run();

  // verify response
  err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
  sm_response* resp = (sm_response*)buf;
  assert(err == sizeof(*resp));
  assert(resp->header.type == SM_MSG_START);
  assert(resp->header.payloadLen == sizeof(ssize_t));
  assert(resp->header.flags == 0);
  assert(resp->returnCode == 9);

  // check file contents
  ::lseek(fd, 0, SEEK_SET);
  err = ::read(fd, buf, sizeof(buf));
  assert(err == 17);
  buf[17] = 0;
  assert(!strcmp("testjunk123456789", (const char*)buf));
  ::unlink(filename);
  cout << "append task OK" << endl;
  return true;
}

void unlinktask(bool connectionTest = false)
{
  int err = 0;
  // make a meta file and delete it
  bf::path fullPath = homepath / prefix / "unlinktest1";
  string pathMeta = prefix + "/unlinktest1";
  const char* Metafilename = pathMeta.c_str();
  const char* filename = fullPath.string().c_str();

  IOCoordinator* ioc = IOCoordinator::get();
  bf::path fullPathMeta = ioc->getMetadataPath() / (string(Metafilename) + ".meta");
  bf::remove(fullPathMeta);

  MetadataFile meta(Metafilename);
  meta.writeMetadata();

  assert(bf::exists(fullPathMeta));

  uint8_t buf[1024];
  unlink_cmd* cmd = (unlink_cmd*)buf;

  cmd->opcode = UNLINK;
  cmd->flen = strlen(filename);
  memcpy(&cmd->filename, filename, cmd->flen);

  // set payload to be shorter than actual message lengh
  // and send a shortened message.
  if (connectionTest)
    cmd->flen -= 2;

  size_t result = ::write(sessionSock, cmd, sizeof(unlink_cmd) + cmd->flen);
  assert(result == (sizeof(unlink_cmd) + cmd->flen));

  // set payload to be correct length again
  if (connectionTest)
    cmd->flen += 2;

  // process task will look for the full length and
  // will wait on the rest of the message.
  ProcessTask pt(clientSock, sizeof(unlink_cmd) + cmd->flen);
  boost::thread t(pt);

  if (connectionTest)
  {
    // make sure the thread is waiting for the rest of the data
    // then kill the connection. This will trigger the task thread
    // to exit on an error handling path
    sleep(1);
    close(sessionSock);
    close(clientSock);
    err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
    assert(err == -1);
    t.join();
  }
  else
  {
    t.join();
    // read the response
    err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
    sm_response* resp = (sm_response*)buf;
    assert(err == sizeof(*resp));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.payloadLen == sizeof(ssize_t));
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 0);
    // confirm it no longer exists
    assert(!bf::exists(fullPathMeta));
  }

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
    ssize_t result = ::write(sessionSock, cmd, sizeof(unlink_cmd) + cmd->flen);
    assert(result==(sizeof(unlink_cmd) + cmd->flen));

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

bool stattask(bool connectionTest = false)
{
  int err = 0;
  bf::path fullPath = homepath / prefix / "stattest1";
  string filename = fullPath.string();
  string Metafilename = prefix + "/stattest1";

  string fullFilename =
      Config::get()->getValue("ObjectStorage", "metadata_path") + "/" + Metafilename + ".meta";

  ::unlink(fullFilename.c_str());
  makeTestMetadata(fullFilename.c_str(), testObjKey);

  uint8_t buf[1024];
  stat_cmd* cmd = (stat_cmd*)buf;

  cmd->opcode = STAT;
  cmd->flen = filename.length();
  strcpy((char*)cmd->filename, filename.c_str());

  // set payload to be shorter than actual message lengh
  // and send a shortened message.
  if (connectionTest)
    cmd->flen -= 2;

  size_t result = ::write(sessionSock, cmd, sizeof(*cmd) + cmd->flen);
  assert(result == (sizeof(*cmd) + cmd->flen));

  // set payload to be correct length again
  if (connectionTest)
    cmd->flen += 2;

  // process task will look for the full length and
  // will wait on the rest of the message.
  ProcessTask pt(clientSock, sizeof(*cmd) + cmd->flen);
  boost::thread t(pt);

  if (connectionTest)
  {
    // make sure the thread is waiting for the rest of the data
    // then kill the connection. This will trigger the task thread
    // to exit on an error handling path
    sleep(1);
    close(sessionSock);
    close(clientSock);
    err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
    assert(err == -1);
    t.join();
  }
  else
  {
    t.join();
    // read the response
    err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
    sm_response* resp = (sm_response*)buf;
    assert(err == sizeof(struct stat) + sizeof(sm_response));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.flags == 0);
    assert(resp->header.payloadLen == sizeof(struct stat) + sizeof(ssize_t));
    assert(resp->returnCode == 0);
    struct stat* _stat = (struct stat*)resp->payload;

    // what can we verify about the stat...
    assert(_stat->st_uid == getuid());
    assert(_stat->st_gid == getgid());
    assert(_stat->st_size == 8192);
  }

  ::unlink(fullFilename.c_str());
  cout << "stattask OK" << endl;
  return true;
}

bool IOCTruncate()
{
  IOCoordinator* ioc = IOCoordinator::get();
  CloudStorage* cs = CloudStorage::get();
  Synchronizer* sync = Synchronizer::get();
  LocalStorage* ls = dynamic_cast<LocalStorage*>(cs);
  if (!ls)
  {
    cout << "IOCTruncate() currently requires using Local storage" << endl;
    return true;
  }
  Cache* cache = Cache::get();
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

  bf::path metadataFile = metaPath / prefix / "test-file.meta";
  bf::path objectPath = cloudPath / testObjKey;
  bf::path cachedObjectPath = cachePath / prefix / testObjKey;
  makeTestMetadata(metadataFile.string().c_str(), testObjKey);
  makeTestObject(objectPath.string().c_str());

  int err;
  uint8_t buf[1 << 14];
  int* buf32 = (int*)buf;

  /* Need to enable this later.
  // Extend the test file to 10000 bytes
  err = ioc->truncate(testFile, 10000);
  assert(!err);
  err = ioc->read(testFile, buf, 0, 10000);
  assert(err == 10000);
  // verity the data is what it should be
  for (int i = 0; i < 2048; i++)
      assert(buf32[i] == i);
  for (int i = 2048; i < 2500; i++)
      assert(buf32[i] == 0);
  */

  err = ioc->truncate(testFile, 4000);
  assert(!err);
  MetadataFile meta(metaTestFile);
  assert(meta.getLength() == 4000);

  // read the data, make sure there are only 4000 bytes & the object still exists
  err = ioc->read(testFile, buf, 0, 8192);
  assert(err == 4000);
  err = ioc->read(testFile, buf, 4000, 1);
  assert(err == 0);
  err = ioc->read(testFile, buf, 4005, 1);
  assert(err == 0);
  assert(bf::exists(objectPath));

  // truncate to 0 bytes, make sure everything is consistent with that, and the object no longer exists
  err = ioc->truncate(testFile, 0);
  assert(!err);
  meta = MetadataFile(metaTestFile);
  assert(meta.getLength() == 0);
  err = ioc->read(testFile, buf, 0, 1);
  assert(err == 0);
  err = ioc->read(testFile, buf, 4000, 1);
  assert(err == 0);
  sync->forceFlush();
  sleep(1);  // give Sync a chance to delete the object from the cloud
  assert(!bf::exists(objectPath));

  // recreate the meta file, make a 2-object version
  ioc->unlink(testFile);
  makeTestMetadata(metadataFile.string().c_str(), testObjKey);
  makeTestObject(objectPath.string().c_str());

  meta = MetadataFile(metaTestFile);
  bf::path secondObjectPath = cloudPath / meta.addMetadataObject(testFile, 8192).key;
  bf::path cachedSecondObject = cachePath / prefix / secondObjectPath.filename();
  makeTestObject(secondObjectPath.string().c_str());
  meta.writeMetadata();

  // make sure there are 16k bytes, and the data is valid before going forward
  memset(buf, 0, sizeof(buf));
  err = ioc->read(testFile, buf, 0, sizeof(buf));
  assert(err == sizeof(buf));
  for (int i = 0; i < (int)sizeof(buf) / 4; i++)
    assert(buf32[i] == (i % 2048));
  assert(bf::exists(cachedSecondObject));
  assert(bf::exists(cachedObjectPath));

  // truncate to 10k, make sure everything looks right
  err = ioc->truncate(testFile, 10240);
  assert(!err);
  meta = MetadataFile(metaTestFile);
  assert(meta.getLength() == 10240);
  memset(buf, 0, sizeof(buf));
  err = ioc->read(testFile, buf, 0, 10240);
  for (int i = 0; i < 10240 / 4; i++)
    assert(buf32[i] == (i % 2048));
  err = ioc->read(testFile, buf, 10239, 10);
  assert(err == 1);

  // truncate to 6000 bytes, make sure second object got deleted
  err = ioc->truncate(testFile, 6000);
  meta = MetadataFile(metaTestFile);
  assert(meta.getLength() == 6000);
  err = ioc->read(testFile, buf, 0, 8192);
  assert(err == 6000);
  sync->forceFlush();
  sleep(1);  // give Synchronizer a chance to delete the file from the 'cloud'
  assert(!bf::exists(secondObjectPath));
  assert(!bf::exists(cachedSecondObject));

  cache->reset();
  ioc->unlink(testFile);

  cout << "IOCTruncate OK" << endl;
  return true;
}

bool truncatetask(bool connectionTest = false)
{
  IOCoordinator* ioc = IOCoordinator::get();
  Cache* cache = Cache::get();

  bf::path metaPath = ioc->getMetadataPath();
  bf::path fullPath = homepath / prefix / "trunctest1";
  string metaStr = prefix + "/trunctest1";
  const char* filename = fullPath.string().c_str();
  const char* Metafilename = metaStr.c_str();
  int err = 0;
  // get the metafile created
  string metaFullName = (metaPath / Metafilename).string() + ".meta";
  ::unlink(metaFullName.c_str());
  MetadataFile meta(Metafilename);

  uint8_t buf[1024];
  truncate_cmd* cmd = (truncate_cmd*)buf;

  cmd->opcode = TRUNCATE;
  cmd->length = 1000;
  cmd->flen = strlen(filename);
  strcpy((char*)cmd->filename, filename);

  // set payload to be shorter than actual message lengh
  // and send a shortened message.
  if (connectionTest)
    cmd->flen -= 2;

  size_t result = ::write(sessionSock, cmd, sizeof(*cmd) + cmd->flen);
  assert(result == (sizeof(*cmd) + cmd->flen));

  // set payload to be correct length again
  if (connectionTest)
    cmd->flen += 2;

  // process task will look for the full length and
  // will wait on the rest of the message.
  ProcessTask pt(clientSock, sizeof(*cmd) + cmd->flen);
  boost::thread t(pt);

  if (connectionTest)
  {
    // make sure the thread is waiting for the rest of the data
    // then kill the connection. This will trigger the task thread
    // to exit on an error handling path
    sleep(1);
    close(sessionSock);
    close(clientSock);
    err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
    assert(err == -1);
    t.join();
  }
  else
  {
    t.join();
    // read the response
    err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
    sm_response* resp = (sm_response*)buf;
    assert(err == sizeof(sm_response));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.flags == 0);
    assert(resp->header.payloadLen == sizeof(ssize_t));
    assert(resp->returnCode == 0);
    // reload the metadata, check that it is 1000 bytes
    meta = MetadataFile(Metafilename);
    assert(meta.getLength() == 1000);
  }

  cache->reset();
  ::unlink(metaFullName.c_str());
  cout << "truncate task OK" << endl;
  return true;
}

bool listdirtask(bool connectionTest = false)
{
  IOCoordinator* ioc = IOCoordinator::get();
  const bf::path metaPath = ioc->getMetadataPath();
  bf::path fullPath = homepath / prefix / "listdirtask";
  string metaStr = prefix + "/listdirtask";
  const char* relPath = fullPath.string().c_str();
  const char* MetarelPath = metaStr.c_str();

  bf::path tmpPath = metaPath / MetarelPath;

  // make some dummy files, make sure they are in the list returned.
  set<string> files;
  int err;
  vector<SharedCloser> fdMinders;

  bf::create_directories(tmpPath);
  for (int i = 0; i < 10; i++)
  {
    string file(tmpPath.string() + "/dummy" + to_string(i));
    files.insert(file);
    file += ".meta";
    err = ::open(file.c_str(), O_CREAT | O_WRONLY, 0600);
    assert(err >= 0);
    fdMinders.push_back(err);
  }

  uint8_t buf[8192];
  memset(buf, 0, sizeof(buf));
  listdir_cmd* cmd = (listdir_cmd*)buf;

  cmd->opcode = LIST_DIRECTORY;
  cmd->plen = strlen(relPath);
  memcpy(cmd->path, relPath, cmd->plen);

  // set payload to be shorter than actual message lengh
  // and send a shortened message.
  if (connectionTest)
    cmd->plen -= 2;

  size_t result = ::write(sessionSock, cmd, sizeof(*cmd) + cmd->plen);
  assert(result == (sizeof(*cmd) + cmd->plen));

  // set payload to be correct length again
  if (connectionTest)
    cmd->plen += 2;

  // process task will look for the full length and
  // will wait on the rest of the message.
  ProcessTask pt(clientSock, sizeof(*cmd) + cmd->plen);
  boost::thread t(pt);

  if (connectionTest)
  {
    // make sure the thread is waiting for the rest of the data
    // then kill the connection. This will trigger the task thread
    // to exit on an error handling path
    sleep(1);
    close(sessionSock);
    close(clientSock);
    err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
    assert(err == -1);
    t.join();
  }
  else
  {
    t.join();
    /* going to keep this simple. Don't run this in a big dir. */
    /* maybe later I'll make a dir, put a file in it, and etc.  For now run it in a small dir. */
    err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
    sm_response* resp = (sm_response*)buf;
    assert(err > 0);
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 0);
    listdir_resp* r = (listdir_resp*)resp->payload;
    assert(r->elements == 10);
    int off = sizeof(sm_response) + sizeof(listdir_resp);
    uint fileCounter = 0;
    while (off < err)
    {
      listdir_resp_entry* e = (listdir_resp_entry*)&buf[off];
      // cout << "len = " << e->flen << endl;
      assert(off + e->flen + sizeof(listdir_resp_entry) < 8192);
      string file(e->filename, e->flen);
      assert(files.find((tmpPath / file).string()) != files.end());
      fileCounter++;
      // cout << "name = " << file << endl;
      off += e->flen + sizeof(listdir_resp_entry);
    }
    assert(fileCounter == r->elements);
  }

  bf::remove_all(tmpPath);

  cout << "listdir task OK" << endl;

  return true;
}

void pingtask()
{
  int err = 0;
  uint8_t buf[1024];
  ping_cmd* cmd = (ping_cmd*)buf;
  cmd->opcode = PING;

  size_t len = sizeof(*cmd);

  ssize_t result = ::write(sessionSock, cmd, sizeof(*cmd));
  assert(result == (sizeof(*cmd)));

  // process task will look for the full length and
  // will wait on the rest of the message.
  // don't test connection loss here since this message is only 1 byte
  ProcessTask pt(clientSock, len);
  boost::thread t(pt);

  t.join();
  // read the response
  err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
  sm_response* resp = (sm_response*)buf;
  assert(err == sizeof(sm_response));
  assert(resp->header.type == SM_MSG_START);
  assert(resp->header.payloadLen == sizeof(ssize_t));
  assert(resp->header.flags == 0);
  assert(resp->returnCode == 0);

  cout << "pingtask OK" << endl;
}

bool copytask(bool connectionTest = false)
{
  /*
      make a file
      copy it
      verify it exists
  */
  bf::path fullPathSrc = homepath / prefix / "dummy1";
  string metaStrSrc = prefix + "/dummy1";
  const char* source = fullPathSrc.string().c_str();
  const char* Metasource = metaStrSrc.c_str();

  bf::path fullPathDest = homepath / prefix / "dummy2";
  string metaStrDest = prefix + "/dummy2";
  const char* dest = fullPathDest.string().c_str();
  const char* Metadest = metaStrDest.c_str();

  MetadataFile meta1(Metasource);

  uint8_t buf[1024];
  copy_cmd* cmd = (copy_cmd*)buf;
  cmd->opcode = COPY;
  cmd->file1.flen = strlen(source);
  strncpy(cmd->file1.filename, source, cmd->file1.flen);
  f_name* file2 = (f_name*)&cmd->file1.filename[cmd->file1.flen];
  file2->flen = strlen(dest);
  strncpy(file2->filename, dest, file2->flen);

  uint len = (uint64_t)&file2->filename[file2->flen] - (uint64_t)buf;

  // set payload to be shorter than actual message lengh
  // and send a shortened message.
  if (connectionTest)
    len -= 2;

  ssize_t result = ::write(sessionSock, buf, len);
  assert(result == static_cast<ssize_t>(len));

  int err = 0;

  // set payload to be correct length again
  if (connectionTest)
    len += 2;

  // process task will look for the full length and
  // will wait on the rest of the message.
  ProcessTask pt(clientSock, len);
  boost::thread t(pt);

  if (connectionTest)
  {
    // make sure the thread is waiting for the rest of the data
    // then kill the connection. This will trigger the task thread
    // to exit on an error handling path
    sleep(1);
    close(sessionSock);
    close(clientSock);
    err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
    assert(err == -1);
    t.join();
  }
  else
  {
    t.join();
    // read the response
    err = ::recv(sessionSock, buf, sizeof(buf), MSG_DONTWAIT);
    sm_response* resp = (sm_response*)buf;
    assert(err == sizeof(sm_response));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.payloadLen == sizeof(ssize_t));
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 0);
    // verify copytest2 is there
    MetadataFile meta2(Metadest, MetadataFile::no_create_t(), true);
    assert(meta2.exists());
  }

  bf::path metaPath = IOCoordinator::get()->getMetadataPath();
  bf::remove(metaPath / (metaStrSrc + ".meta"));
  bf::remove(metaPath / (metaStrDest + ".meta"));
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
  Cache* cache = Cache::get();
  CloudStorage* cs = CloudStorage::get();
  LocalStorage* ls = dynamic_cast<LocalStorage*>(cs);
  if (ls == NULL)
  {
    cout << "Cache test 1 requires using local storage" << endl;
    return false;
  }

  cache->reset();
  assert(cache->getCurrentCacheSize() == 0);

  bf::path storagePath = ls->getPrefix();
  bf::path cachePath = cache->getCachePath() / prefix;
  vector<string> v_bogus;
  vector<bool> exists;

  // make sure nothing shows up in the cache path for files that don't exist
  v_bogus.push_back("does-not-exist");
  cache->read(prefix, v_bogus);
  assert(!bf::exists(cachePath / "does-not-exist"));
  cache->exists(prefix, v_bogus, &exists);
  assert(exists.size() == 1);
  assert(!exists[0]);

  // make sure a file that does exist does show up in the cache path

  makeTestObject((storagePath / testObjKey).string().c_str());

  v_bogus[0] = testObjKey.c_str();
  cache->read(prefix, v_bogus);
  assert(bf::exists(cachePath / testObjKey));
  exists.clear();
  cache->exists(prefix, v_bogus, &exists);
  assert(exists.size() == 1);
  assert(exists[0]);
  size_t currentSize = cache->getCurrentCacheSize();
  assert(currentSize == bf::file_size(cachePath / testObjKey));

  // lie about the file being deleted and then replaced
  cache->deletedObject(prefix, testObjKey, currentSize);
  assert(cache->getCurrentCacheSize() == 0);
  cache->newObject(prefix, testObjKey, currentSize);
  assert(cache->getCurrentCacheSize() == currentSize);
  cache->exists(prefix, v_bogus, &exists);
  assert(exists.size() == 1);
  assert(exists[0]);

  // cleanup
  bf::remove(cachePath / testObjKey);
  bf::remove(storagePath / testObjKey);
  cout << "cache test 1 OK" << endl;
  return true;
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
  IOCoordinator* ioc = IOCoordinator::get();
  size_t len = 8192, tmp;
  boost::shared_array<uint8_t> data = ioc->mergeJournal("test-object", "test-journal", 0, len, &tmp);
  assert(data);
  int* idata = (int*)data.get();
  for (i = 0; i < 5; i++)
    assert(idata[i] == i);
  for (; i < 10; i++)
    assert(idata[i] == i - 5);
  for (; i < 2048; i++)
    assert(idata[i] == i);

  // try different range parameters
  // read at the beginning of the change
  len = 40;
  data = ioc->mergeJournal("test-object", "test-journal", 20, len, &tmp);
  assert(data);
  idata = (int*)data.get();
  for (i = 0; i < 5; i++)
    assert(idata[i] == i);
  for (; i < 10; i++)
    assert(idata[i] == i + 5);

  // read s.t. beginning of the change is in the middle of the range
  len = 24;
  data = ioc->mergeJournal("test-object", "test-journal", 8, len, &tmp);
  assert(data);
  idata = (int*)data.get();
  for (i = 0; i < 3; i++)
    assert(idata[i] == i + 2);
  for (; i < 6; i++)
    assert(idata[i] == i - 3);

  // read s.t. end of the change is in the middle of the range
  len = 20;
  data = ioc->mergeJournal("test-object", "test-journal", 28, len, &tmp);
  assert(data);
  idata = (int*)data.get();
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
  IOCoordinator* ioc = IOCoordinator::get();
  Config* config = Config::get();
  Synchronizer* sync = Synchronizer::get();
  Cache* cache = Cache::get();
  CloudStorage* cs = CloudStorage::get();
  LocalStorage* ls = dynamic_cast<LocalStorage*>(cs);
  if (!ls)
  {
    cout << "syncTest1() requires using local storage at the moment." << endl;
    return true;
  }

  cache->reset();

  // delete everything in the fake cloud to make it easier to list later
  bf::path fakeCloudPath = ls->getPrefix();
  cout << "fakeCLoudPath = " << fakeCloudPath << endl;
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
  string journalName = prefix + "/" + testObjKey + ".journal";

  makeTestObject((cachePath / prefix / testObjKey).string().c_str());
  makeTestJournal((journalPath / journalName).string().c_str());
  makeTestMetadata((metaPath / string(metaTestFile + ".meta")).string().c_str(), testObjKey);

  cache->newObject(prefix, testObjKey, bf::file_size(cachePath / prefix / testObjKey));
  cache->newJournalEntry(prefix, bf::file_size(journalPath / journalName));

  vector<string> vObj;
  vObj.push_back(testObjKey);

  sync->newObjects(prefix, vObj);
  sync->forceFlush();
  sleep(2);  // wait for the job to run

  // make sure that it made it to the cloud
  bool exists = false;
  int err = cs->exists(testObjKey, &exists);
  assert(!err);
  assert(exists);

  sync->newJournalEntry(prefix, testObjKey, 0);
  sync->forceFlush();
  sleep(1);  // let it do what it does

  // check that the original objects no longer exist
  assert(!cache->exists(prefix, testObjKey));
  assert(!bf::exists(journalPath / journalName));

  // Replicator doesn't implement all of its functionality yet, need to delete key from the cache manually for
  // now
  bf::remove(cachePath / testObjKey);

  // check that a new version of object exists in cloud storage
  // D'oh, this would have to list the objects to find it, not going to implement
  // that everywhere just now.  For now, making this test require LocalStorage.
  bool foundIt = false;
  string newKey;
  for (bf::directory_iterator dir(fakeCloudPath); dir != bf::directory_iterator() && !foundIt; ++dir)
  {
    newKey = dir->path().filename().string();
    foundIt = (MetadataFile::getSourceFromKey(newKey) == metaTestFile);
    if (foundIt)
    {
      assert(cache->exists(prefix, newKey));
      cs->deleteObject(newKey);
      break;
    }
  }

  assert(foundIt);
  cache->makeSpace(prefix, cache->getMaxCacheSize());  // clear the cache & make it call sync->flushObject()

  // the key should now be back in cloud storage and deleted from the cache
  assert(!cache->exists(prefix, newKey));
  err = cs->exists(newKey, &exists);
  assert(!err && exists);

  // make the journal again, call sync->newJournalObject()
  makeTestJournal((journalPath / prefix / (newKey + ".journal")).string().c_str());
  cache->newJournalEntry(prefix, bf::file_size(journalPath / prefix / (newKey + ".journal")));
  sync->newJournalEntry(prefix, newKey, 0);
  sync->forceFlush();
  sleep(1);

  // verify that newkey is no longer in cloud storage, and that another permutation is
  err = cs->exists(newKey, &exists);
  assert(!err && !exists);
  foundIt = false;
  for (bf::directory_iterator dir(fakeCloudPath); dir != bf::directory_iterator() && !foundIt; ++dir)
  {
    testObjKey = dir->path().filename().string();
    foundIt = (MetadataFile::getSourceFromKey(testObjKey) == metaTestFile);
  }
  assert(foundIt);

  // TODO test error paths, pass in some junk

  // cleanup, just blow away everything for now
  cache->reset();
  vector<string> keys;
  for (bf::directory_iterator dir(fakeCloudPath); dir != bf::directory_iterator(); ++dir)
    keys.push_back(dir->path().filename().string());
  sync->deletedObjects(prefix, keys);
  sync->forceFlush();
  sleep(1);
  ioc->unlink(testFile);

  cout << "Sync test 1 OK" << endl;
  return true;
}

void metadataUpdateTest()
{
  Config* config = Config::get();
  string metaPath = config->getValue("ObjectStorage", "metadata_path");
  MetadataFile mdfTest("metadataUpdateTest");
  mdfTest.addMetadataObject("metadataUpdateTest", 100);
  mdfTest.printObjects();
  mdfTest.updateEntryLength(0, 200);
  mdfTest.printObjects();
  // mdfTest.updateEntryLength(0,100);
  // mdfTest.printObjects();
  string metaFilePath = metaPath + "/" + "metadataUpdateTest.meta";
  ::unlink(metaFilePath.c_str());
}

void s3storageTest1()
{
  try
  {
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

    try
    {
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

      uint8_t* data1 = new uint8_t[len];
      uint8_t* data2 = new uint8_t[len];
      err = read(fd1, data1, len);
      assert(err == (int)len);
      err = read(fd2, data2, len);
      assert(err == (int)len);
      assert(!memcmp(data1, data2, len));
      close(fd1);
      close(fd2);
      delete[] data1;
      delete[] data2;

      err = s3.copyObject(testFile, testFile2);
      assert(!err);
      err = s3.exists(testFile2, &exists);
      assert(!err);
      assert(exists);
      s3.deleteObject(testFile);
      s3.deleteObject(testFile2);

      err = s3.copyObject("this-does-not-exist", testFile2);
      assert(err < 0);
      assert(errno == ENOENT);
    }
    catch (exception& e)
    {
      cout << __FUNCTION__ << " caught " << e.what() << endl;
      assert(0);
    }
    cout << "S3Storage Test 1 OK" << endl;
  }
  catch (exception& e)
  {
    cout << e.what() << endl;
  }
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
  Cache* cache = Cache::get();
  CloudStorage* cs = CloudStorage::get();
  IOCoordinator* ioc = IOCoordinator::get();
  Config* config = Config::get();
  LocalStorage* ls = dynamic_cast<LocalStorage*>(cs);
  if (!ls)
  {
    cout << "IOC read test 1 requires LocalStorage for now." << endl;
    return;
  }
  testObjKey = "12345_0_8192_" + prefix + "~test-file";

  cache->reset();

  bf::path storagePath = ls->getPrefix();
  bf::path cachePath = cache->getCachePath();
  bf::path journalPath = cache->getJournalPath();
  bf::path metaPath = config->getValue("ObjectStorage", "metadata_path");
  assert(!metaPath.empty());
  bf::create_directories(metaPath / prefix);

  string objFilename = (storagePath / testObjKey).string();
  string journalFilename = (journalPath / prefix / testObjKey).string() + ".journal";
  string metaFilename = (metaPath / metaTestFile).string() + ".meta";

  boost::scoped_array<uint8_t> data(new uint8_t[1 << 20]);

  memset(data.get(), 0, 1 << 20);

  int err;
  err = ioc->read(testFile, data.get(), 0, 1 << 20);
  assert(err < 0);
  assert(errno == ENOENT);
  makeTestObject(objFilename.c_str());
  makeTestMetadata(metaFilename.c_str(), testObjKey);
  size_t objSize = bf::file_size(objFilename);
  err = ioc->read(testFile, data.get(), 0, 1 << 20);
  assert(err == (int)objSize);

  // verify the data
  int* data32 = (int*)data.get();
  int i;
  for (i = 0; i < 2048; i++)
    assert(data32[i] == i);
  for (; i < (1 << 20) / 4; i++)
    assert(data32[i] == 0);

  makeTestJournal(journalFilename.c_str());

  err = ioc->read(testFile, data.get(), 0, 1 << 20);
  assert(err == (int)objSize);
  for (i = 0; i < 5; i++)
    assert(data32[i] == i);
  for (; i < 10; i++)
    assert(data32[i] == i - 5);
  for (; i < 2048; i++)
    assert(data32[i] == i);
  for (; i < (1 << 20) / 4; i++)
    assert(data32[i] == 0);

  err = ioc->read(testFile, data.get(), 9000, 4000);
  assert(err == 0);

  cache->reset();
  err = ioc->unlink(testFile);
  assert(err >= 0);

  cout << "IOC read test 1 OK" << endl;
}

void IOCUnlink()
{
  IOCoordinator* ioc = IOCoordinator::get();
  CloudStorage* cs = CloudStorage::get();
  Cache* cache = Cache::get();
  Synchronizer* sync = Synchronizer::get();

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
  bf::path cachedObjPath = cachePath / prefix / testObjKey;
  bf::path cachedJournalPath = journalPath / prefix / (string(testObjKey) + ".journal");
  bf::path metadataFile = metaPath / (string(metaTestFile) + ".meta");
  makeTestMetadata(metadataFile.string().c_str(), testObjKey);
  makeTestObject(cachedObjPath.string().c_str());
  makeTestJournal(cachedJournalPath.string().c_str());

  cache->newObject(prefix, cachedObjPath.filename().string(), bf::file_size(cachedObjPath));
  cache->newJournalEntry(prefix, bf::file_size(cachedJournalPath));
  vector<string> keys;
  keys.push_back(cachedObjPath.filename().string());
  sync->newObjects(prefix, keys);
  // sync->newJournalEntry(keys[0]);    don't want to end up renaming it
  sync->forceFlush();
  sleep(1);

  // ok, they should be fully 'in the system' now.
  // verify that they are
  assert(bf::exists(metadataFile));
  assert(bf::exists(cachedObjPath));
  assert(bf::exists(cachedJournalPath));
  bool exists;
  cs->exists(cachedObjPath.filename().string(), &exists);
  assert(exists);

  int err = ioc->unlink(testFile);
  assert(err == 0);

  assert(!bf::exists(metadataFile));
  assert(!bf::exists(cachedObjPath));
  assert(!bf::exists(cachedJournalPath));
  sync->forceFlush();
  sleep(1);  // stall for sync
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
  IOCoordinator* ioc = IOCoordinator::get();
  Cache* cache = Cache::get();
  CloudStorage* cs = CloudStorage::get();
  LocalStorage* ls = dynamic_cast<LocalStorage*>(cs);
  Synchronizer* sync = Synchronizer::get();

  if (!ls)
  {
    cout << "IOCCopyFile1 requires local storage at the moment" << endl;
    return;
  }

  bf::path metaPath = ioc->getMetadataPath();
  bf::path cachePath = ioc->getCachePath();
  bf::path csPath = ls->getPrefix();
  bf::path journalPath = ioc->getJournalPath();
  bf::path cachedObjPath = cachePath / prefix / copyfileObjKey;
  bf::path sourcePath = metaPath / prefix / "source.meta";
  bf::path destPath = metaPath / prefix / "dest.meta";
  bf::path l_sourceFile = homepath / prefix / string("source");
  bf::path l_destFile = homepath / prefix / string("dest");

  cache->reset();

  bf::create_directories(sourcePath.parent_path());
  makeTestMetadata(sourcePath.string().c_str(), copyfileObjKey);
  makeTestObject((csPath / copyfileObjKey).string().c_str());
  makeTestJournal((journalPath / prefix / (string(copyfileObjKey) + ".journal")).string().c_str());
  cache->newJournalEntry(prefix, bf::file_size(journalPath / prefix / (string(copyfileObjKey) + ".journal")));

  int err = ioc->copyFile(l_sourceFile.string().c_str(), l_destFile.string().c_str());
  assert(!err);
  uint8_t buf1[8192], buf2[8192];
  err = ioc->read(l_sourceFile.string().c_str(), buf1, 0, sizeof(buf1));
  assert(err == sizeof(buf1));
  err = ioc->read(l_destFile.string().c_str(), buf2, 0, sizeof(buf2));
  assert(err == sizeof(buf2));
  assert(memcmp(buf1, buf2, 8192) == 0);

  assert(ioc->unlink(l_sourceFile.string().c_str()) == 0);
  assert(ioc->unlink(l_destFile.string().c_str()) == 0);
  sync->forceFlush();
  assert(cache->getCurrentCacheSize() == 0);

  cout << "IOC copy file 1 OK" << endl;
}

void IOCCopyFile2()
{
  // call IOC::copyFile() with non-existant file
  IOCoordinator* ioc = IOCoordinator::get();

  bf::path fullPath = homepath / prefix / "not-there";
  const char* source = fullPath.string().c_str();
  bf::path fullPath2 = homepath / prefix / "not-there2";
  const char* dest = fullPath2.string().c_str();

  bf::path metaPath = ioc->getMetadataPath();
  bf::remove(metaPath / prefix / "not-there.meta");
  bf::remove(metaPath / prefix / "not-there2.meta");

  int err = ioc->copyFile(source, dest);
  assert(err);
  assert(errno == ENOENT);
  assert(!bf::exists(metaPath / "not-there.meta"));
  assert(!bf::exists(metaPath / "not-there2.meta"));

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
  IOCoordinator* ioc = IOCoordinator::get();
  Cache* cache = Cache::get();
  Synchronizer* sync = Synchronizer::get();

  bf::path metaPath = ioc->getMetadataPath();
  bf::path journalPath = ioc->getJournalPath();
  bf::path cachePath = ioc->getCachePath();
  bf::path sourcePath = metaPath / prefix / "source.meta";
  bf::path destPath = metaPath / prefix / "dest.meta";
  bf::path l_sourceFile = homepath / prefix / string("source");
  bf::path l_destFile = homepath / prefix / string("dest");

  cache->reset();

  makeTestObject((cachePath / prefix / copyfileObjKey).string().c_str());
  makeTestJournal((journalPath / prefix / (string(copyfileObjKey) + ".journal")).string().c_str());
  makeTestMetadata(sourcePath.string().c_str(), copyfileObjKey);

  cache->newObject(prefix, copyfileObjKey, bf::file_size(cachePath / prefix / copyfileObjKey));
  cache->newJournalEntry(prefix, bf::file_size(journalPath / prefix / (string(copyfileObjKey) + ".journal")));

  int err = ioc->copyFile(l_sourceFile.string().c_str(), l_destFile.string().c_str());
  assert(!err);
  uint8_t buf1[8192], buf2[8192];
  err = ioc->read(l_sourceFile.string().c_str(), buf1, 0, sizeof(buf1));
  assert(err == sizeof(buf1));
  err = ioc->read(l_destFile.string().c_str(), buf2, 0, sizeof(buf2));
  assert(err == sizeof(buf2));
  assert(memcmp(buf1, buf2, 8192) == 0);

  assert(ioc->unlink(l_sourceFile.string().c_str()) == 0);
  assert(ioc->unlink(l_destFile.string().c_str()) == 0);
  sync->forceFlush();
  assert(cache->getCurrentCacheSize() == 0);

  cout << "IOC copy file 3 OK" << endl;
}

void IOCCopyFile()
{
  IOCCopyFile1();
  IOCCopyFile2();
  IOCCopyFile3();
}

/* Correctness was tested by inspecting debugging outputs in mergeJournal().  With a little more work
   we could capture the merged output and use that to confirm the expected result.  Later.
*/
void bigMergeJournal1()
{
  const char* jName =
      "test_data/e7a81ca3-0af8-48cc-b224-0f59c187e0c1_0_3436_~home~patrick~"
      "mariadb~columnstore~data1~systemFiles~dbrm~BRM_saves_em.journal";
  const char* fName =
      "test_data/e7a81ca3-0af8-48cc-b224-0f59c187e0c1_0_3436_~home~patrick~"
      "mariadb~columnstore~data1~systemFiles~dbrm~BRM_saves_em";
  bf::path jNamePath = testDirPath / jName;
  bf::path fNamePath = testDirPath / fName;

  if (!bf::is_directory(testDirPath / "test_data"))
  {
    cout << "bigMergeJournal1 test_data directory not found at " << (testDirPath / "test_data") << endl
         << "  Check if -d option needs to be provided or changed." << endl;
    return;
  }
  IOCoordinator* ioc = IOCoordinator::get();
  boost::shared_array<uint8_t> buf;
  size_t tmp;
  buf = ioc->mergeJournal(fNamePath.string().c_str(), jNamePath.string().c_str(), 0, 68332, &tmp);
  assert(buf);
  buf = ioc->mergeJournal(fNamePath.string().c_str(), jNamePath.string().c_str(), 100, 68232, &tmp);
  assert(buf);
  buf = ioc->mergeJournal(fNamePath.string().c_str(), jNamePath.string().c_str(), 0, 68232, &tmp);
  assert(buf);
  buf = ioc->mergeJournal(fNamePath.string().c_str(), jNamePath.string().c_str(), 100, 68132, &tmp);
  assert(buf);
  buf = ioc->mergeJournal(fNamePath.string().c_str(), jNamePath.string().c_str(), 100, 10, &tmp);
  assert(buf);
}

// This should write an incomplete msg(s) to make sure SM does the right thing.  Not
// done yet, handing this off to Ben.
void shortMsg()
{
  IOCoordinator* ioc = IOCoordinator::get();

  struct stat _stat;
  bf::path fullPath = homepath / prefix / "writetest1";
  const char* filename = fullPath.string().c_str();
  ::unlink(filename);
  ioc->open(filename, O_WRONLY | O_CREAT, &_stat);

  size_t size = 27;
  std::vector<uint8_t> bufWrite(sizeof(write_cmd) + std::strlen(filename) + size);

  sm_msg_header* hdrWrite = (sm_msg_header*)bufWrite.data();
  write_cmd* cmdWrite = (write_cmd*)&hdrWrite[1];
  uint8_t* dataWrite;

  cmdWrite->opcode = WRITE;
  cmdWrite->offset = 0;
  cmdWrite->count = size;
  cmdWrite->flen = std::strlen(filename);
  memcpy(&cmdWrite->filename, filename, cmdWrite->flen);
  dataWrite = (uint8_t*)&cmdWrite->filename[cmdWrite->flen];
  memcpy(dataWrite, "123456789123456789123456789", cmdWrite->count);

  hdrWrite->type = SM_MSG_START;
  hdrWrite->payloadLen = sizeof(*cmdWrite) + cmdWrite->flen + 9;

  WriteTask w(clientSock, hdrWrite->payloadLen);
  ssize_t result = ::write(sessionSock, cmdWrite, hdrWrite->payloadLen);
  assert(result == static_cast<ssize_t>(hdrWrite->payloadLen));

  w.run();

  // verify response
  uint8_t bufRead[1024];
  int err = ::recv(sessionSock, bufRead, sizeof(bufRead), MSG_DONTWAIT);
  sm_response* resp = (sm_response*)bufRead;
  assert(err == sizeof(*resp));
  assert(resp->header.type == SM_MSG_START);
  assert(resp->header.payloadLen == sizeof(ssize_t));
  assert(resp->header.flags == 0);
  assert(resp->returnCode == 9);

  std::vector<uint8_t> bufAppend(sizeof(append_cmd) + std::strlen(filename) + size);
  uint8_t* dataAppend;

  sm_msg_header* hdrAppend = (sm_msg_header*)bufAppend.data();
  append_cmd* cmdAppend = (append_cmd*)&hdrAppend[1];
  cmdAppend->opcode = APPEND;
  cmdAppend->count = size;
  cmdAppend->flen = std::strlen(filename);
  memcpy(&cmdAppend->filename, filename, cmdAppend->flen);
  dataAppend = (uint8_t*)&cmdAppend->filename[cmdAppend->flen];
  memcpy(dataAppend, "123456789123456789123456789", cmdAppend->count);
  hdrAppend->type = SM_MSG_START;
  hdrAppend->payloadLen = sizeof(*cmdAppend) + cmdAppend->flen + 9;

  AppendTask a(clientSock, hdrAppend->payloadLen);
  err = ::write(sessionSock, cmdAppend, hdrAppend->payloadLen);

  a.run();

  // verify response
  err = ::recv(sessionSock, bufRead, sizeof(bufRead), MSG_DONTWAIT);
  resp = (sm_response*)bufRead;
  assert(err == sizeof(*resp));
  assert(resp->header.type == SM_MSG_START);
  assert(resp->header.payloadLen == sizeof(ssize_t));
  assert(resp->header.flags == 0);
  assert(resp->returnCode == 9);
  ioc->unlink(fullPath.string().c_str());
  cout << "shortWriteMsg Test OK" << endl;
}

// write and append are the biggest vulnerabilities here b/c those msgs could be sent in multiple
// pieces, are much larger, and thus if there is a crash mid-message it's most likely to happen
// during a call to write/append().
// it may not even be possible for CS to write a partial open/stat/read/etc msg, but that should be
// tested as well.
void shortMsgTests()
{
  shortMsg();
}

int main(int argc, char* argv[])
{
  std::size_t sizeKB = 1024;
  int option;
  while ((option = getopt(argc, argv, "d:p:h:")) != EOF)
  {
    switch (option)
    {
      case 'd':
        testDirPath = optarg;
        cout << "test_dir path is " << testDirPath << endl;
        break;
      case 'p':
        prefix = optarg;
        testObjKey = "12345_0_8192_" + prefix + "~test-file";
        copyfileObjKey = "12345_0_8192_" + prefix + "~source";
        metaTestFile = prefix + "/test-file";
        testFilePath = homepath / metaTestFile;
        testFile = testFilePath.string().c_str();
        cout << "TestFile is " << testFile << endl;
        break;
      case 'h':
      default:
        printUsage();
        return 0;
        break;
    }
  }

  if (!bf::is_regular_file("test_data/storagemanager.cnf"))
  {
    cerr << "This should be run in a dir where ./test_data/storagemanager.cnf exists" << endl;
    exit(1);
  }
  Config* config = Config::get("test_data/storagemanager.cnf");
  cout << "Cleaning out debris from previous runs" << endl;
  bf::remove_all(config->getValue("ObjectStorage", "metadata_path"));
  bf::remove_all(config->getValue("ObjectStorage", "journal_path"));
  bf::remove_all(config->getValue("LocalStorage", "path"));
  bf::remove_all(config->getValue("Cache", "path"));

  cout << "connecting" << endl;
  makeConnection();
  cout << "connected" << endl;
  scoped_closer sc1(serverSock), sc2(sessionSock), sc3(clientSock);

  opentask();
  // metadataUpdateTest();
  // create the metadatafile to use
  // requires 8K object size to test boundries
  // Case 1 new write that spans full object
  metadataJournalTest((10 * sizeKB), 0);
  // Case 2 write data beyond end of data in object 2 that still ends in object 2
  metadataJournalTest((4 * sizeKB), (8 * sizeKB));
  // Case 3 write spans 2 journal objects
  metadataJournalTest((8 * sizeKB), (4 * sizeKB));
  // Case 4 write starts object1 ends object3
  metadataJournalTest((16 * sizeKB), (7 * sizeKB));
  // Case 5 write starts in new object at 0 offset after null objects
  metadataJournalTest((8 * sizeKB), (32 * sizeKB));
  // overwrite null objects
  metadataJournalTest((10 * sizeKB), (40 * sizeKB));
  metadataJournalTest((8 * sizeKB), (24 * sizeKB));
  // overwrite whole file and create new objects
  metadataJournalTest((96 * sizeKB), (0));

  // append test
  // first one appends file to end of 8K object
  metadataJournalTest_append((7 * sizeKB));
  // this apppends that starts on new object
  metadataJournalTest_append((7 * sizeKB));
  // this starts in one object and crosses into new object
  metadataJournalTest_append((7 * sizeKB));

  // writetask();
  // appendtask();
  unlinktask();
  stattask();
  truncatetask();
  listdirtask();
  pingtask();
  copytask();

  localstorageTest1();
  cacheTest1();
  mergeJournalTest();
  replicatorTest();
  syncTest1();

  IOCReadTest1();
  IOCTruncate();
  IOCUnlink();
  IOCCopyFile();
  shortMsgTests();

  // For the moment, this next one just verifies no error happens as reported by the fcns called.
  // It doesn't verify the result yet.
  bigMergeJournal1();

  // skip the s3 test if s3 is not configured
  if (config->getValue("S3", "region") != "")
  {
    s3storageTest1();
  }
  else
    cout << "To run the S3Storage unit tests, configure the S3 section of test-data/storagemanager.cnf"
         << endl;

  cout << "Cleanup";
  metadataJournalTestCleanup();
  cout << " DONE" << endl;

  cout << "Testing connection loss..." << endl << endl;

  cout << "Check log files for lines:" << endl;
  cout << "[NameTask read] caught an error: Bad file descriptor." << endl;
  cout << "****** socket error!" << endl;
  cout << "PosixTask::consumeMsg(): Discarding the tail end of a partial msg." << endl << endl;
  // opentask();
  cout << "OpenTask read2 connection test " << endl;
  opentask(true);
  makeConnection();
  cout << "UnlinkTask read connection test " << endl;
  unlinktask(true);
  makeConnection();
  cout << "StatTask read connection test " << endl;
  stattask(true);
  makeConnection();
  cout << "TruncateTask read connection test " << endl;
  truncatetask(true);
  makeConnection();
  cout << "ListDirextoryTask read connection test " << endl;
  listdirtask(true);
  makeConnection();
  cout << "CopyTask read connection test " << endl;
  copytask(true);

  (Cache::get())->shutdown();
  delete (Synchronizer::get());
  delete (Cache::get());
  delete (IOCoordinator::get());

  return 0;
}

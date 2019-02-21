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


#undef NDEBUG
#include <cassert>

using namespace storagemanager;
using namespace std;

struct scoped_closer {
    scoped_closer(int f) : fd(f) { }
    ~scoped_closer() { 
        int s_errno = errno;
        ::close(fd);
        errno = s_errno; 
    }
    int fd;
};

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
    assert(boost::filesystem::exists(filename));
    ::unlink(filename);
    cout << "opentask OK" << endl;
    return true;
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
    // make a file and delete it
    const char *filename = "unlinktest1";
    ::unlink(filename);
    int fd = ::open(filename, O_CREAT | O_RDWR, 0666);
    assert(fd > 0);
    scoped_closer f(fd);
    
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
    assert(!boost::filesystem::exists(filename));
    cout << "unlink task OK" << endl;
}

bool stattask()
{
    const char *filename = "stattest1";
    ::unlink(filename);
    int fd = ::open(filename, O_CREAT | O_RDWR, 0666);
    assert(fd > 0);
    scoped_closer f(fd);

    uint8_t buf[1024];
    stat_cmd *cmd = (stat_cmd *) buf;
    
    cmd->opcode = STAT;
    cmd->flen = strlen(filename);
    strcpy((char *) cmd->filename, filename);
    
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
    assert(_stat->st_size == 0);
    
    ::unlink(filename);
    cout << "stattask OK" << endl;
    return true;
}

bool truncatetask()
{
    const char *filename = "trunctest1";
    ::unlink(filename);
    int fd = ::open(filename, O_CREAT | O_RDWR, 0666);
    assert(fd > 0);
    scoped_closer f(fd);

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
    
    struct stat statbuf;
    ::stat(filename, &statbuf);
    assert(statbuf.st_size == 1000);
    ::unlink(filename);
    cout << "truncate task OK" << endl;
    return true;
}

bool listdirtask()
{
    // make a file, make sure it's in the list returned.
    const char *filename = "listdirtest1";
    ::unlink(filename);
    int fd = ::open(filename, O_CREAT | O_RDWR, 0666);
    assert(fd > 0);
    scoped_closer f(fd);
    
    uint8_t buf[1024];
    listdir_cmd *cmd = (listdir_cmd *) buf;
    
    cmd->opcode = LIST_DIRECTORY;
    cmd->plen = 1;
    cmd->path[0] = '.';
    
    ::write(sessionSock, cmd, sizeof(*cmd) + cmd->plen);
    ListDirectoryTask l(clientSock, sizeof(*cmd) + cmd->plen);
    l.run();

    /* going to keep this simple. Don't run this in a big dir. */
    /* maybe later I'll make a dir, put a file in it, and etc.  For now run it in a small dir. */
    int err = ::recv(sessionSock, buf, 1024, MSG_DONTWAIT);
    sm_response *resp = (sm_response *) buf;
    assert(err > 0);
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 0);
    listdir_resp *r = (listdir_resp *) resp->payload;
    //cout << "resp has " << r->elements << " elements" << endl;
    int off = sizeof(sm_response) + sizeof(listdir_resp);
    while (off < err)
    {
        listdir_resp_entry *e = (listdir_resp_entry *) &buf[off];
        //cout << "len = " << e->flen << endl;
        assert(off + e->flen + sizeof(listdir_resp_entry) < 1024);
        if (!strncmp(e->filename, filename, strlen(filename))) {
            cout << "listdirtask OK" << endl;
            ::unlink(filename);
            return true;
        }
        //string name(e->filename, e->flen);
        //cout << "name = " << name << endl;
        off += e->flen + sizeof(listdir_resp_entry);
    }
    cout << "listdirtask().  Didn't find '" << filename << " in the listing.  Dir too large for this test?" << endl;
    assert(1);
    return false;
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
    const char *filename = "copytest1";
    ::unlink(filename);
    int fd = ::open(filename, O_CREAT | O_RDWR, 0666);
    assert(fd > 0);
    scoped_closer f(fd);
    int err = ::write(fd, "testjunk", 8);
    assert(err == 8);
    
    uint8_t buf[1024];
    copy_cmd *cmd = (copy_cmd *) buf;
    cmd->opcode = COPY;
    cmd->file1.flen = strlen(filename);
    strncpy(cmd->file1.filename, filename, cmd->file1.flen);
    const char *filename2 = "copytest2";
    f_name *file2 = (f_name *) &cmd->file1.filename[cmd->file1.flen];
    file2->flen = strlen(filename2);
    strncpy(file2->filename, filename2, file2->flen);
    
    uint len = (uint64_t) &file2->filename[file2->flen] - (uint64_t) buf;
    ::write(sessionSock, buf, len);
    CopyTask c(clientSock, len);
    c.run();
    
    // read the response
    err = ::recv(sessionSock, buf, 1024, MSG_DONTWAIT);
    sm_response *resp = (sm_response *) buf;
    assert(err == sizeof(sm_response));
    assert(resp->header.type == SM_MSG_START);
    assert(resp->header.payloadLen == 4);
    assert(resp->header.flags == 0);
    assert(resp->returnCode == 0);
    
    // verify copytest2 is there
    assert(boost::filesystem::exists(filename2));
    ::unlink(filename);
    ::unlink(filename2);
    cout << "copytask OK " << endl;
    return true;
}

int main()
{
    cout << "connecting" << endl;
    makeConnection();
    cout << "connected" << endl;
    scoped_closer sc1(serverSock), sc2(sessionSock), sc3(clientSock);
    opentask();
    writetask();
    appendtask();
    unlinktask();
    stattask();
    truncatetask();
    listdirtask();
    pingtask();
    copytask();
    
    Config *conf = Config::get();
    LocalStorage ls;
    Cache cache;
    
    
    
    
    return 0;
}

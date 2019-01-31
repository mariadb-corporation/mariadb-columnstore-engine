#include "OpenTask.h"
#include "WriteTask.h"
#include "AppendTask.h"
#include "UnlinkTask.h"
#include "IOCoordinator.h"
#include "messageFormat.h"
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

IOCoordinator *ioc;

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
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    assert(err == sizeof(struct stat) + sizeof(sm_msg_resp));
    assert(resp->type == SM_MSG_START);
    assert(resp->payloadLen == sizeof(struct stat) + 4);
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
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    assert(err == sizeof(*resp));
    assert(resp->type == SM_MSG_START);
    assert(resp->payloadLen == 4);
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
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    assert(err == sizeof(*resp));
    assert(resp->type == SM_MSG_START);
    assert(resp->payloadLen == 4);
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
    sm_msg_resp *resp = (sm_msg_resp *) buf;
    assert(err == sizeof(*resp));
    assert(resp->type == SM_MSG_START);
    assert(resp->payloadLen == 4);
    assert(resp->returnCode == 0);
    
    // confirm it no longer exists
    assert(!boost::filesystem::exists(filename));
    cout << "unlink task OK" << endl;
}

int main()
{
    ioc = new IOCoordinator();
    cout << "connecting" << endl;
    makeConnection();
    cout << "connected" << endl;
    scoped_closer sc1(serverSock), sc2(sessionSock), sc3(clientSock);
    opentask();
    writetask();
    appendtask();
    unlinktask();
    return 0;
}

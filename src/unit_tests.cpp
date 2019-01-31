#include "OpenTask.h"
#include "IOCoordinator.h"
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
        cout << "closing " << fd << endl;
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

int sessionSock = -1;
int serverSock = -1;

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
    
bool opentask()
{
    boost::thread t(acceptConnection);
    struct sockaddr_un sa;
    memset(&sa, 0, sizeof(sa));
    sa.sun_family = AF_UNIX;
    memcpy(&sa.sun_path[1], "testing", 7);
    
    int clientSock = ::socket(AF_UNIX, SOCK_STREAM, 0);
    assert(clientSock > 0);
    scoped_closer s2(clientSock);
    sleep(1);   // let server thread get to accept()
    int err = ::connect(clientSock, (struct sockaddr *) &sa, sizeof(sa));
    assert(err == 0);
    t.join();
    scoped_closer s3(sessionSock);
    
    // going to rely on msgs being smaller than the buffer here
    uint8_t buf[1024];
    uint32_t *buf32 = (uint32_t *) buf;
    
    // open/create a file named 'opentest1'
    const char *filename = "opentest1";
    buf32[0] = O_WRONLY | O_CREAT;
    buf32[1] = 9;
    strcpy((char *) &buf32[2], "opentest1");
    uint msg_len = 17;
    
    ::unlink(filename);
    ::write(sessionSock, buf, msg_len);
    OpenTask o(clientSock, msg_len);
    o.run();
    
    // read the response
    
    
    /* verify the file is there */
    assert(boost::filesystem::exists(filename));
    ::unlink(filename);
}


int main()
{
    ioc = new IOCoordinator();
    opentask();
    return 0;
}

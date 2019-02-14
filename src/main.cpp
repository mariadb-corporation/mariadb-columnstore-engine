
#include <unistd.h>
#include <string>
#include <iostream>
#include <stdio.h>
#include <cstdlib>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>
#include <syslog.h>

using namespace std;

#include "SessionManager.h"

using namespace storagemanager;

int main(int argc, char** argv)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, NULL);
    
    int ret = 0;

    //TODO: make this configurable
    setlogmask (LOG_UPTO (LOG_DEBUG));

    openlog ("StorageManager", LOG_PID | LOG_NDELAY, LOG_LOCAL2);

    syslog(LOG_NOTICE, "StorageManager started.");

    SessionManager* sm = SessionManager::get();

    ret = sm->start();

    closelog ();

    return ret;
}


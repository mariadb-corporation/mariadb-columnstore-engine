
#include <unistd.h>
#include <string>
#include <iostream>
#include <stdio.h>
#include <cstdlib>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <signal.h>

using namespace std;

#include "SMLogging.h"
#include "SessionManager.h"

using namespace storagemanager;

#include "Cache.h"
void printCacheUsage(int sig)
{
    cout << "Current cache size = " << Cache::get()->getCurrentCacheSize() << endl;
    cout << "Cache element count = " << Cache::get()->getCurrentCacheElementCount() << endl;
}

int main(int argc, char** argv)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, NULL);
 
    sa.sa_handler = printCacheUsage;
    sigaction(SIGUSR1, &sa, NULL);
    
    int ret = 0;

    SMLogging* logger = SMLogging::get();

    logger->log(LOG_NOTICE,"StorageManager started.");

    SessionManager* sm = SessionManager::get();

    ret = sm->start();


    return ret;
}


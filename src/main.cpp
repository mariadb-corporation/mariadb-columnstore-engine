
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
#include "IOCoordinator.h"
#include "Cache.h"
#include "Synchronizer.h"
#include "Replicator.h"

using namespace storagemanager;

void printCacheUsage(int sig)
{
    Cache::get()->validateCacheSize();
    cout << "Current cache size = " << Cache::get()->getCurrentCacheSize() << endl;
    cout << "Cache element count = " << Cache::get()->getCurrentCacheElementCount() << endl;
}

int main(int argc, char** argv)
{

    /* Instantiate objects to have them verify config settings before continuing */
    IOCoordinator::get();
    Cache::get();
    Synchronizer::get();
    Replicator::get();

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


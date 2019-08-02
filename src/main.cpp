
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

bool signalCaught = false;

void printCacheUsage(int sig)
{
    Cache::get()->validateCacheSize();
    cout << "Current cache size = " << Cache::get()->getCurrentCacheSize() << endl;
    cout << "Cache element count = " << Cache::get()->getCurrentCacheElementCount() << endl;
}

void printKPIs(int sig)
{
    IOCoordinator::get()->printKPIs();
    Cache::get()->printKPIs();
    Synchronizer::get()->printKPIs();
    CloudStorage::get()->printKPIs();
}

void shutdownSM(int sig)
{
    if (!signalCaught)
        (SessionManager::get())->shutdownSM(sig);
    signalCaught = true;
}

int main(int argc, char** argv)
{

    /* Instantiate objects to have them verify config settings before continuing */
    IOCoordinator* ioc = IOCoordinator::get();
    Cache* cache = Cache::get();
    Synchronizer* sync = Synchronizer::get();
    Replicator* rep = Replicator::get();

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));

    for (int i=0; i<SIGRTMAX; i++)
    {
        sa.sa_handler = shutdownSM;
        if (i != SIGCONT && i != SIGKILL && i != SIGSTOP)
            sigaction(i, &sa, NULL);
    }

    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, NULL);
 
    sa.sa_handler = printCacheUsage;
    sigaction(SIGUSR1, &sa, NULL);
 
    sa.sa_handler = printKPIs;
    sigaction(SIGUSR2, &sa, NULL);
    
    int ret = 0;

    SMLogging* logger = SMLogging::get();

    logger->log(LOG_NOTICE,"StorageManager started.");

    SessionManager* sm = SessionManager::get();

    ret = sm->start();

    cache->shutdown();
    
    delete sync;
    delete cache;
    delete ioc;
    delete rep;
    logger->log(LOG_INFO,"StorageManager Shutdown Complete.");

    return ret;
}


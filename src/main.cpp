
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

#include "SessionManager.h"

using namespace storagemanager;

int main(int argc, char** argv)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, NULL);
    
    int ret = 0;
    SessionManager* sm = SessionManager::get();

    ret = sm->start();

    return ret;
}



#include <unistd.h>
#include <string>
#include <iostream>
#include <stdio.h>
#include <cstdlib>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
using namespace std;

#include "SessionManager.h"
#include "IOCoordinator.h"

using namespace storagemanager;

IOCoordinator *ioc;

int main(int argc, char** argv)
{
    int ret = 0;
    SessionManager sessionManager = SessionManager();
    ioc = new IOCoordinator();

    ret = sessionManager.start();

    return ret;
}


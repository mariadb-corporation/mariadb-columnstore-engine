
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

using namespace storagemanager;

int main(int argc, char** argv)
{
    int ret = 0;
    SessionManager* sm = SessionManager::get();

    ret = sm->start();

    return ret;
}


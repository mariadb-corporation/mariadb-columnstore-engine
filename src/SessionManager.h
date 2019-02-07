

#ifndef STORAGEMANGER_H_
#define STORAGEMANGER_H_

#include "ClientRequestProcessor.h"

#include <boost/thread/mutex.hpp>
#include <sys/poll.h>


namespace storagemanager
{

enum sessionCtrl {
    ADDFD,
    REMOVEFD
};

#define MAX_SM_SOCKETS 200

/**
 * @brief StorageManager class initializes process and handles incoming requests.
 */
class SessionManager
{
public:
    static SessionManager *get();
    ~SessionManager();

    /**
     * start and manage socket connections
     */
    int start();

    void returnSocket(int socket);
    void socketError(int socket);
    void CRPTest(int socket,uint length);

private:
    SessionManager();
    //SMConfig&  config;
    ClientRequestProcessor *crp;
    struct pollfd fds[MAX_SM_SOCKETS];
    int socketCtrl[2];
    boost::mutex ctrlMutex;
};

}

#endif

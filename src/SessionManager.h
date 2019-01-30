

#ifndef STORAGEMANGER_H_
#define STORAGEMANGER_H_

#include <boost/thread/mutex.hpp>
#include <sys/poll.h>


namespace storagemanager
{

enum sessionCtrl {
    ADDFD,
    REMOVEFD
};

/**
 * @brief StorageManager class initializes process and handles incoming requests.
 */
class SessionManager
{
public:
    /**
     * Constructor
     */
    SessionManager();

    /**
     * Default Destructor
     */
    ~SessionManager();

    /**
     * start and manage socket connections
     */
    int start();

    void returnSocket(int socket);
    void CRPTest(int socket,uint length);

private:
    //SMConfig&  config;
    struct pollfd fds[200];
    int socketCtrl[2];
};

}

#endif

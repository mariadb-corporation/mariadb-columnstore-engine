

#ifndef STORAGEMANGER_H_
#define STORAGEMANGER_H_

#include "ClientRequestProcessor.h"
#include "messageFormat.h"

#include <signal.h>
#include <boost/thread/mutex.hpp>
#include <sys/poll.h>
#include <tr1/unordered_map>


namespace storagemanager
{

enum sessionCtrl {
    ADDFD,
    REMOVEFD,
    SHUTDOWN
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
    void shutdownSM(int sig);

private:
    SessionManager();
    //SMConfig&  config;
    ClientRequestProcessor *crp;
    struct pollfd fds[MAX_SM_SOCKETS];
    int socketCtrl[2];
    boost::mutex ctrlMutex;
    
    // These map a socket fd to its state between read iterations if a message header could not be found in the data
    // available at the time.
    struct SockState {
        char remainingData[SM_HEADER_LEN];
        uint remainingBytes;
    };
    std::tr1::unordered_map<int, SockState> sockState;
    
};

}

#endif

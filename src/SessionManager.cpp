
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <errno.h>
#include <string>
#include <assert.h>
using namespace std;

#include <exception>

#include "messageFormat.h"
#include "SessionManager.h"
#include "ClientRequestProcessor.h"
#include <boost/thread.hpp>

namespace
{
    storagemanager::SessionManager *sm = NULL;
    boost::mutex m;
}

namespace storagemanager
{

SessionManager::SessionManager()
{
	crp = ClientRequestProcessor::get();
}

SessionManager::~SessionManager()
{
}

SessionManager * SessionManager::get()
{
    if (sm)
        return sm;
    boost::mutex::scoped_lock s(m);
    if (sm)
        return sm;
    sm = new SessionManager();
    return sm;
}

int SessionManager::start()
{
    int rc,listenSockfd,incomingSockfd,on = 1;
    struct sockaddr_un addr;
    int nfds;
    int pollTimeout = -1;
    int socketIncr;
    int current_size = 0;
    bool running = true;

	printf("SessionManager starting...\n");

    if (pipe(socketCtrl)==-1)
    {
        perror("Pipe Failed" );
        return 1;
    }

    listenSockfd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (listenSockfd < 0)
    {
        perror("socket() failed");
        return -1;
    }

    rc = ::setsockopt(listenSockfd, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on));
    if (rc < 0)
    {
        perror("setsockopt() failed");
        close(listenSockfd);
        return -1;
    }

    rc = ::ioctl(listenSockfd, FIONBIO, (char *)&on);
    if (rc < 0)
    {
        perror("ioctl() failed");
        close(listenSockfd);
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strcpy(&addr.sun_path[1], &storagemanager::socket_name[1]);   // first char is null...
    rc = ::bind(listenSockfd,(struct sockaddr *)&addr, sizeof(addr));
    if (rc < 0)
    {
        perror("bind() failed");
        close(listenSockfd);
        return -1;
    }

    rc = ::listen(listenSockfd, 32);
    if (rc < 0)
    {
        perror("listen() failed");
        close(listenSockfd);
        return -1;
    }

    memset(fds, 0 , sizeof(fds));
    fds[0].fd = listenSockfd;
    fds[0].events = POLLIN;
    fds[1].fd = socketCtrl[0];
    fds[1].events = POLLIN;
    nfds = 2;

	printf("SessionManager waiting for sockets....\n");
    while (running)
    {
        try
        {
            rc = ::poll(fds, nfds, pollTimeout);
            if (rc < 0)
            {
                perror("poll() failed");
                break;
            }
            current_size = nfds;
            for (socketIncr = 0; socketIncr < current_size; socketIncr++)
            {

                if(fds[socketIncr].revents == 0)
                    continue;

                if(fds[socketIncr].revents != POLLIN)
                {
                    //printf("Error! revents = %d\n", fds[socketIncr].revents);
                    close(fds[socketIncr].fd);
                    fds[socketIncr].fd = -1;
                    continue;
                }
                if (fds[socketIncr].fd == listenSockfd)
                {
                    //printf("  Listening socket is readable\n");
                    incomingSockfd = 0;
                    while (incomingSockfd != -1)
                    {
                        if (nfds >= MAX_SM_SOCKETS)
                            break; // try to free up some room

                        incomingSockfd = ::accept(listenSockfd, NULL, NULL);
                        if (incomingSockfd < 0)
                        {
                            if (errno != EWOULDBLOCK)
                            {
                                perror("accept() failed");
                                running = false;
                            }
                            break;
                        }
                        //printf("  New incoming connection - %d\n", incomingSockfd);
                        fds[nfds].fd = incomingSockfd;
                        fds[nfds].events = POLLIN;
                        nfds++;
                    }
                }
                else if (fds[socketIncr].fd == socketCtrl[0])
                {
                    //printf("  SocketControl is readable\n");
                    uint8_t ctrlCode;
                    int len,socket;

                    len = ::read(socketCtrl[0], &ctrlCode, 1);
                    if (len <= 0)
                    {
                        continue;
                    }
                    switch(ctrlCode)
                    {
                        case ADDFD:
                            len = ::read(socketCtrl[0], &socket, sizeof(socket));
                            if (len <= 0)
                            {
                                continue;
                            }
                            for (int i = 0; i < nfds; i++)
                            {
                                if(fds[i].fd == socket)
                                    fds[i].events = POLLIN;
                            }
                            break;
                        default:
                            break;
                    }
                    break;
                }
                else
                {
                    //printf("  socketIncr %d -- Descriptor %d is readable\n", socketIncr,fds[socketIncr].fd);

                    bool closeConn = false;
                    char recv_buffer[8192];
                    char send_buffer[8192];
                    memset(recv_buffer, 0 , sizeof(recv_buffer));
                    memset(send_buffer, 0 , sizeof(send_buffer));
                    uint recvMsgLength = 0;
                    uint recvMsgStart = 0;
                    uint remainingBytes = 0;
                    uint endOfData, i;
                    int peakLength,len;
                    while(true)
                    {

                        peakLength = ::recv(fds[socketIncr].fd, &recv_buffer[remainingBytes], sizeof(recv_buffer)-remainingBytes, MSG_PEEK | MSG_DONTWAIT);
                        if (peakLength < 0)
                        {
                            if (errno != EWOULDBLOCK)
                            {
                                perror("recv() failed");
                                closeConn = true;
                            }
                            break;
                        }

                        endOfData = remainingBytes + peakLength;
                        if (endOfData < 8)
                        {
                            //read this snippet and keep going
                            len = ::read(fds[socketIncr].fd, &recv_buffer[remainingBytes], peakLength);
                            remainingBytes = endOfData;
                            continue;
                        }

                        //Look for SM_MSG_START
                        for (i = 0; i <= endOfData - 8; i++)
                        {
                            if (*((uint *) &recv_buffer[i]) == storagemanager::SM_MSG_START)
                            {
                                //printf("Received SM_MSG_START\n");
                                //found it set msgLength and recvMsgStart offset of SM_MSG_START
                                recvMsgLength = *((uint *) &recv_buffer[i+4]);
                                recvMsgStart = i+8;
                                //printf("  recvMsgLength %d recvMsgStart %d endofData %d\n", recvMsgLength,recvMsgStart,endOfData);
                                // if >= endOfData then the start of the message data is the beginning of next message
                                if (recvMsgStart >= endOfData)
                                    recvMsgStart = 0;
                                break;
                            }
                        }

                        // didn't find SM_MSG_START in this message consume the data and loop back through on next message
                        if (recvMsgLength == 0)
                        {
                            //printf("No SM_MSG_START\n");
                            len = ::read(fds[socketIncr].fd, &recv_buffer[remainingBytes], peakLength);
                            remainingBytes = endOfData;
                        }
                        else
                        {
                            //found msg start
                            //remove the junk in front of the message
                            if (recvMsgStart > 0)
                            {
                                //printf("SM_MSG_START data is here\n");
                                len = ::read(fds[socketIncr].fd, &recv_buffer[remainingBytes], peakLength - recvMsgStart);
                            }
                            else
                            {
                                //printf("SM_MSG_START data is next message\n");
                                len = ::read(fds[socketIncr].fd, &recv_buffer[remainingBytes], peakLength);
                            }
                            //Disable polling on this socket
                            fds[socketIncr].events = 0;
                            //Pass the socket to CRP and the message length. next read will be start of message
                            crp->processRequest(fds[socketIncr].fd,recvMsgLength);

                            /*
                            //Doing this to work with cloudio_component_test
                            len = ::read(fds[socketIncr].fd, out, recvMsgLength);
                            printf("Read %d bytes.\n",len);
                            //Debug test lets send a reponse back
                            uint32_t response[4] = { storagemanager::SM_MSG_START, 8, (uint32_t ) -1, EINVAL };
                            len = ::send(fds[socketIncr].fd, response, 16, 0);
                            */
                        }
                        break;
                    }

                    if (closeConn)
                    {
                        close(fds[socketIncr].fd);
                        fds[socketIncr].fd = -1;
                    }
                }
            }
        }
        catch (std::exception& ex)
        {
            throw std::exception();
        }
        catch (...)
        {
            throw std::exception();
        }
        for (int i = 0; i < nfds; i++)
        {
            if (fds[i].fd == -1)
            {
                for (int j = i; j < nfds; j++)
                {
                    fds[j].fd = fds[j + 1].fd;
                    fds[j].events = fds[j + 1].events;
                    fds[j].revents = fds[j + 1].revents;
                }
                i--;
                nfds--;
            }
        }
    }

    return -1;
}

void SessionManager::returnSocket(int socket)
{
    int err;
    uint8_t ctrlCode = ADDFD;
    err = ::write(socketCtrl[1], &ctrlCode, 1);
    if (err <= 0)
    {
        return;
    }
    err = ::write(socketCtrl[1], &socket, sizeof(socket));
    if (err <= 0)
    {
        return;
    }
}

}


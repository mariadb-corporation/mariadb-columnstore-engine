/* Copyright (C) 2019 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

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
#include <iostream>
using namespace std;

#include <exception>

#include "messageFormat.h"
#include "SessionManager.h"
#include "ClientRequestProcessor.h"
#include "SMLogging.h"
#include <boost/thread.hpp>

namespace
{
storagemanager::SessionManager* sm = NULL;
boost::mutex m;
}  // namespace

namespace storagemanager
{
SessionManager::SessionManager()
{
  crp = ClientRequestProcessor::get();
  socketCtrl[0] = -1;
  socketCtrl[1] = -1;
}

SessionManager::~SessionManager()
{
}

SessionManager* SessionManager::get()
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
  SMLogging* logger = SMLogging::get();
  int rc, listenSockfd, incomingSockfd, on = 1;
  struct sockaddr_un addr;
  int nfds;
  int pollTimeout = -1;
  int socketIncr;
  int current_size = 0;

  bool shutdown = false;
  bool running = true;

  if (pipe(socketCtrl) == -1)
  {
    logger->log(LOG_CRIT, "Pipe Failed: %s", strerror(errno));
    return 1;
  }

  listenSockfd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (listenSockfd < 0)
  {
    logger->log(LOG_CRIT, "socket() failed: %s", strerror(errno));
    return -1;
  }

  rc = ::setsockopt(listenSockfd, SOL_SOCKET, SO_REUSEADDR, (char*)&on, sizeof(on));
  if (rc < 0)
  {
    logger->log(LOG_CRIT, "setsockopt() failed: %s", strerror(errno));
    close(listenSockfd);
    return -1;
  }

  rc = ::ioctl(listenSockfd, FIONBIO, (char*)&on);
  if (rc < 0)
  {
    logger->log(LOG_CRIT, "ioctl() failed: %s", strerror(errno));
    close(listenSockfd);
    return -1;
  }

  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strcpy(&addr.sun_path[1], &storagemanager::socket_name[1]);  // first char is null...
  rc = ::bind(listenSockfd, (struct sockaddr*)&addr, sizeof(addr));
  if (rc < 0)
  {
    logger->log(LOG_CRIT, "bind() failed: %s", strerror(errno));
    close(listenSockfd);
    return -1;
  }

  rc = ::listen(listenSockfd, 32);
  if (rc < 0)
  {
    logger->log(LOG_CRIT, "listen() failed: %s", strerror(errno));
    close(listenSockfd);
    return -1;
  }

  memset(fds, 0, sizeof(fds));
  fds[0].fd = listenSockfd;
  fds[0].events = POLLIN;
  fds[1].fd = socketCtrl[0];
  fds[1].events = POLLIN;
  nfds = 2;

  logger->log(LOG_INFO, "SessionManager waiting for sockets.");
  while (running)
  {
    try
    {
      // if (current_size != nfds)
      // logger->log(LOG_DEBUG,"polling %i fds %i", nfds,fds);
      // cout << "polling " << nfds << " fds" << endl;
      rc = ::poll(fds, nfds, pollTimeout);
      if (rc < 0 && errno != EINTR)
      {
        logger->log(LOG_CRIT, "poll() failed: %s", strerror(errno));
        break;
      }
      else if (rc < 0)
        continue;
      current_size = nfds;
      for (socketIncr = 0; socketIncr < current_size; socketIncr++)
      {
        if (fds[socketIncr].revents == 0)
          continue;
        // if (socketIncr >= 2)
        // logger->log(LOG_DEBUG,"got event on fd  %i index is %i", fds[socketIncr].fd,socketIncr);
        if (fds[socketIncr].revents != POLLIN)
        {
          // logger->log(LOG_DEBUG,"Error! revents = %d", fds[socketIncr].revents,);
          if (fds[socketIncr].fd == -1)
            logger->log(LOG_DEBUG, "!= POLLIN, closing fd -1");
          close(fds[socketIncr].fd);
          fds[socketIncr].fd = -1;
          continue;
        }
        if (fds[socketIncr].fd == listenSockfd)
        {
          // logger->log(LOG_DEBUG,"Listening socket is readable");
          incomingSockfd = 0;
          while (incomingSockfd != -1 && !shutdown)
          {
            if (nfds >= MAX_SM_SOCKETS)
              break;  // try to free up some room

            incomingSockfd = ::accept(listenSockfd, NULL, NULL);
            if (incomingSockfd < 0)
            {
              if (errno != EWOULDBLOCK)
              {
                logger->log(LOG_CRIT, "accept() failed: %s", strerror(errno));
                shutdown = true;
              }
              break;
            }
            // logger->log(LOG_DEBUG,"New incoming connection - %d",incomingSockfd);
            fds[nfds].fd = incomingSockfd;
            fds[nfds].events = POLLIN;
            nfds++;
          }
        }
        else if (fds[socketIncr].fd == socketCtrl[0])
        {
          // logger->log(LOG_DEBUG,"SocketControl is readable");
          uint8_t ctrlCode;
          int len, socket;

          len = ::read(socketCtrl[0], &ctrlCode, 1);
          if (len <= 0)
          {
            continue;
          }
          switch (ctrlCode)
          {
            case ADDFD:
              len = ::read(socketCtrl[0], &socket, sizeof(socket));
              if (len <= 0)
              {
                continue;
              }
              for (int i = 0; i < nfds; i++)
              {
                if (fds[i].fd == socket)
                {
                  if (shutdown)
                  {
                    logger->log(LOG_DEBUG, "Shutdown in progress, closed socket %i at index %i", fds[i].fd,
                                i);
                    close(socket);
                    break;
                  }
                  fds[i].events = (POLLIN | POLLPRI);
                  break;
                }
              }
              break;
            case REMOVEFD:
              len = ::read(socketCtrl[0], &socket, sizeof(socket));
              if (len <= 0)
              {
                continue;
              }
              for (int i = 0; i < nfds; i++)
              {
                if (fds[i].fd == socket)
                {
                  if (socket == -1)
                    logger->log(LOG_DEBUG, "REMOVEFD told to remove fd -1");
                  close(socket);
                  fds[i].fd = -1;
                  break;
                }
              }
              break;
            case SHUTDOWN:
              logger->log(LOG_DEBUG, "Shutdown StorageManager...");
              close(fds[0].fd);
              shutdown = true;
              break;
            default: break;
          }
        }
        else
        {
          // logger->log(LOG_DEBUG,"socketIncr %d -- Descriptor %d is
          // readable",socketIncr,fds[socketIncr].fd);
          bool closeConn = false;
          char recv_buffer[8192];
          uint recvMsgLength = 0;
          uint recvMsgStart = 0;
          uint remainingBytes = 0;
          uint endOfData, i;
          int peakLength, len;

          // logger->log(LOG_DEBUG,"reading from fd %i index is %i", fds[socketIncr].fd,socketIncr);
          if (sockState.find(fds[socketIncr].fd) != sockState.end())
          {
            SockState& state = sockState[fds[socketIncr].fd];
            remainingBytes = state.remainingBytes;
            memcpy(recv_buffer, state.remainingData, remainingBytes);
            sockState.erase(fds[socketIncr].fd);
          }
          while (true)
          {
            peakLength = ::recv(fds[socketIncr].fd, &recv_buffer[remainingBytes],
                                sizeof(recv_buffer) - remainingBytes, MSG_PEEK | MSG_DONTWAIT);
            if (peakLength < 0)
            {
              if (errno != EWOULDBLOCK)
              {
                closeConn = true;
                break;
              }

              // there's no data available at this point; go back to the poll loop.
              assert(remainingBytes < SM_HEADER_LEN);
              SockState state;
              state.remainingBytes = remainingBytes;
              memcpy(state.remainingData, recv_buffer, remainingBytes);
              sockState[fds[socketIncr].fd] = state;
              break;
            }
            // logger->log(LOG_DEBUG,"recv got %i bytes", peakLength);
            endOfData = remainingBytes + peakLength;
            if (endOfData < SM_HEADER_LEN)
            {
              // read this snippet and keep going
              len = ::read(fds[socketIncr].fd, &recv_buffer[remainingBytes], peakLength);
              remainingBytes = endOfData;
              if (len != peakLength)
                logger->log(LOG_ERR, "Read returned length != peakLength. ( %i != %i )", len, peakLength);
              continue;
            }

            // Look for SM_MSG_START
            for (i = 0; i <= endOfData - SM_HEADER_LEN; i++)
            {
              if (*((uint*)&recv_buffer[i]) == SM_MSG_START)
              {
                // logger->log(LOG_DEBUG,"Received SM_MSG_START");
                // found it set msgLength and recvMsgStart offset of SM_MSG_START
                recvMsgLength = *((uint*)&recv_buffer[i + 4]);
                // logger->log(LOG_DEBUG,"got length = %i", recvMsgLength);
                recvMsgStart = i + SM_HEADER_LEN;
                // logger->log(LOG_DEBUG,"recvMsgLength %d recvMsgStart %d endofData %d",
                // recvMsgLength,recvMsgStart,endOfData);
                // if >= endOfData then the start of the message data is the beginning of next message
                if (recvMsgStart >= endOfData)
                  recvMsgStart = 0;
                break;
              }
            }

            // didn't find SM_MSG_START in this message consume the data and loop back through on next message
            if (recvMsgLength == 0)
            {
              // logger->log(LOG_DEBUG,"No SM_MSG_START");
              len = ::read(fds[socketIncr].fd, &recv_buffer[remainingBytes], peakLength);
              if (len != peakLength)
                logger->log(LOG_ERR, "Read returned length != peakLength. ( %i != %i )", len, peakLength);
              // we know the msg header isn't in position [0, endOfData - i), so throw that out
              // and copy [i, endOfData) to the front of the buffer to be
              // checked by the next iteration.
              memcpy(recv_buffer, &recv_buffer[i], endOfData - i);
              remainingBytes = endOfData - i;
            }
            else
            {
              // found msg start
              // remove the junk in front of the message
              if (recvMsgStart > 0)
              {
                // logger->log(LOG_DEBUG,"SM_MSG_START data is here");
                // how many to consume here...
                // recvMsgStart is the position in the buffer
                // peakLength is the amount peeked this time
                // remainingBytes is the amount carried over from previous reads
                len = ::read(fds[socketIncr].fd, &recv_buffer[remainingBytes], recvMsgStart - remainingBytes);
              }
              else
              {
                // logger->log(LOG_DEBUG,"SM_MSG_START data is next message");
                len = ::read(fds[socketIncr].fd, &recv_buffer[remainingBytes], peakLength);
                if (len != peakLength)
                  logger->log(LOG_ERR, "Read returned length != peakLength. ( %i != %i )", len, peakLength);
              }
              // Disable polling on this socket
              fds[socketIncr].events = 0;
              // Pass the socket to CRP and the message length. next read will be start of message
              crp->processRequest(fds[socketIncr].fd, recvMsgLength);

              /*
              //Doing this to work with cloudio_component_test
              len = ::read(fds[socketIncr].fd, out, recvMsgLength);
              logger->log(LOG_DEBUG,"Read %d bytes.",len);
              //Debug test lets send a reponse back
              uint32_t response[4] = { storagemanager::SM_MSG_START, 8, (uint32_t ) -1, EINVAL };
              len = ::send(fds[socketIncr].fd, response, 16, 0);
              */
              break;
            }
          }

          if (closeConn)
          {
            if (fds[socketIncr].fd == -1)
              logger->log(LOG_DEBUG, "closeConn closing fd -1");
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

    int i, j;
    if (shutdown)
    {
      for (i = 2; i < nfds; ++i)
      {
        if (fds[i].events == (POLLIN | POLLPRI))
        {
          close(fds[i].fd);
          fds[i].fd = -1;
        }
      }
    }
    /* get rid of fds == -1 */
    for (i = 2; i < nfds; ++i)
    {
      if (fds[i].fd == -1)
        break;
    }
    for (j = i + 1; j < nfds; ++j)
    {
      if (fds[j].fd != -1)
      {
        fds[i].fd = fds[j].fd;
        fds[i].events = fds[j].events;
        ++i;
      }
    }
    nfds = i;

    if (shutdown)
    {
      if (nfds <= 2)
        running = false;
    }
  }

  // Shutdown Done
  crp->shutdown();

  return -1;
}

void SessionManager::returnSocket(int socket)
{
  boost::mutex::scoped_lock s(ctrlMutex);
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

void SessionManager::socketError(int socket)
{
  boost::mutex::scoped_lock s(ctrlMutex);
  SMLogging* logger = SMLogging::get();
  logger->log(LOG_CRIT, " ****** socket error!");
  int err;
  uint8_t ctrlCode = REMOVEFD;
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

void SessionManager::shutdownSM(int sig)
{
  boost::mutex::scoped_lock s(ctrlMutex);
  SMLogging* logger = SMLogging::get();
  logger->log(LOG_DEBUG, "SessionManager Caught Signal %i", sig);
  int err;
  uint8_t ctrlCode = SHUTDOWN;
  err = ::write(socketCtrl[1], &ctrlCode, 1);
  if (err <= 0)
  {
    return;
  }
}

}  // namespace storagemanager

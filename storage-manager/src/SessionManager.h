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
enum sessionCtrl
{
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
  static SessionManager* get();
  ~SessionManager();

  /**
   * start and manage socket connections
   */
  int start();

  void returnSocket(int socket);
  void socketError(int socket);
  void CRPTest(int socket, uint length);
  void shutdownSM(int sig);

 private:
  SessionManager();
  // SMConfig&  config;
  ClientRequestProcessor* crp;
  struct pollfd fds[MAX_SM_SOCKETS];
  int socketCtrl[2];
  boost::mutex ctrlMutex;

  // These map a socket fd to its state between read iterations if a message header could not be found in the
  // data available at the time.
  struct SockState
  {
    char remainingData[SM_HEADER_LEN];
    uint remainingBytes;
  };
  std::tr1::unordered_map<int, SockState> sockState;
};

}  // namespace storagemanager

#endif

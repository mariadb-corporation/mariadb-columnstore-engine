/* Copyright (C) 2020 MariaDB Corporation

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

#ifndef SERVICE_H_INCLUDED
#define SERVICE_H_INCLUDED

#include <signal.h>
#include "pipe.h"

class Service
{
 protected:
  // The service name, for logging
  const std::string m_name;
  // The pipe to send messages from the child to the parent
  Pipe m_pipe;

  static void common_signal_handler_CHLD(int sig)
  {
  }

  void InitCommonSignalHandlers()
  {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = common_signal_handler_CHLD;
    sigaction(SIGCHLD, &sa, NULL);
  }

  int RunForking()
  {
    int err;
    InitCommonSignalHandlers();

    if (m_pipe.open() || (err = fork()) < 0)
    {
      // Pipe or fork failed
      LogErrno();
      return 1;
    }

    if (err > 0)  // Parent
      return Parent();

    return Child();
  }

  virtual int Parent()
  {
    char str[100];
    // Read the message from the child
    ssize_t nbytes = m_pipe.readtm({120, 0}, str, sizeof(str));
    if (nbytes >= 0)
    {
      ParentLogChildMessage(std::string(str, nbytes));
    }
    else
    {
      // read() failed
      LogErrno();
      return 1;
    }
    return 0;
  }

 public:
  Service(const std::string& name) : m_name(name)
  {
  }
  virtual ~Service()
  {
  }

  void NotifyServiceStarted()
  {
    if (m_pipe.is_open_for_write())
    {
      std::ostringstream str;
      str << m_name << " main process has started";
      m_pipe.write(str.str());
    }
  }

  void NotifyServiceInitializationFailed()
  {
    if (m_pipe.is_open_for_write())
    {
      std::ostringstream str;
      str << m_name << " main process initialization failed";
      m_pipe.write(str.str());
    }
  }

  // Used by both Parent and Child to log errors
  virtual void LogErrno() = 0;
  // Used by Parent to log an initialization notification message from child
  virtual void ParentLogChildMessage(const std::string& str) = 0;
  // The main service process job
  virtual int Child() = 0;
};

#endif  // SERVICE_H_INCLUDED

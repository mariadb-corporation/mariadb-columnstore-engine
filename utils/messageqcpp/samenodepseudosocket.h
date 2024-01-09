/* Copyright (C) 2024 MariaDB Corp.

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

#pragma once

#include "../../dbcon/joblist/distributedenginecomm.h"

#include "socket.h"
#include "socketparms.h"
#include "bytestream.h"

namespace messageqcpp
{
class IOSocket;

// This class is a dummy replacement for a TCP socket
// wrapper to communicate with the same node.
class SameNodePseudoSocket : public Socket
{
 public:
  explicit SameNodePseudoSocket(joblist::DistributedEngineComm* exeMgrDecPtr);
  virtual ~SameNodePseudoSocket();
  virtual void write(SBS msg, Stats* stats = NULL, int senderType = 1);

 private:
  virtual void bind(const sockaddr* serv_addr);
  SameNodePseudoSocket(const SameNodePseudoSocket& rhs);
  virtual SameNodePseudoSocket& operator=(const SameNodePseudoSocket& rhs);

  virtual void connectionTimeout(const struct ::timespec* timeout)
  {
  }

  virtual void syncProto(bool use)
  {
  }

  int getConnectionNum() const
  {
    return 1;
  }

  inline virtual void socketParms(const SocketParms& socket)
  {
  }

  inline virtual const SocketParms socketParms() const
  {
    return SocketParms();
  }

  // all these virtual methods are to stay inaccessable.
  inline virtual void sa(const sockaddr* sa);
  virtual void open();
  virtual void close();
  inline virtual bool isOpen() const;
  virtual const SBS read(const struct timespec* timeout = 0, bool* isTimeOut = NULL,
                         Stats* stats = NULL) const;
  virtual void write(const ByteStream& msg, Stats* stats = NULL, int senderType = 1);
  virtual void write_raw(const ByteStream& msg, Stats* stats = NULL) const;
  virtual void listen(int backlog = 5);
  virtual const IOSocket accept(const struct timespec* timeout = 0);
  virtual void connect(const sockaddr* serv_addr);
  virtual Socket* clone() const;
  virtual const std::string toString() const;
  virtual const std::string addr2String() const;
  virtual bool isSameAddr(const Socket* rhs) const;
  virtual bool isSameAddr(const struct in_addr& ipv4Addr) const;
  static int ping(const std::string& ipaddr, const struct timespec* timeout = 0);
  virtual bool isConnected() const;
  virtual bool hasData() const;

  joblist::DistributedEngineComm* dec_ = nullptr;
};

inline bool SameNodePseudoSocket::isOpen() const
{
  return true;
}

inline void SameNodePseudoSocket::sa(const sockaddr* sa)
{
}

}  // namespace messageqcpp

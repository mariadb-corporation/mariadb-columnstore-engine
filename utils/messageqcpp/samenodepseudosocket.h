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
  ~SameNodePseudoSocket() override;
  void write(SBS msg, Stats* stats = nullptr) override;

 private:
  void bind(const sockaddr* serv_addr) override;
  SameNodePseudoSocket(const SameNodePseudoSocket& rhs);
  virtual SameNodePseudoSocket& operator=(const SameNodePseudoSocket& rhs);

  void connectionTimeout(const struct ::timespec* /*timeout*/) override
  {
  }

  void syncProto(bool /*use*/) override
  {
  }

  int getConnectionNum() const override
  {
    return 1;
  }

  inline void socketParms(const SocketParms& /*socket*/) override
  {
  }

  inline const SocketParms socketParms() const override
  {
    return SocketParms();
  }

  // all these virtual methods are to stay inaccessable.
  inline void sa(const sockaddr* sa) override;
  void open() override;
  void close() override;
  inline bool isOpen() const override;
  const SBS read(const struct timespec* timeout = nullptr, bool* isTimeOut = nullptr,
                 Stats* stats = nullptr) const override;
  void write(const ByteStream& msg, Stats* stats = nullptr) override;
  void write_raw(const ByteStream& msg, Stats* stats = nullptr) const override;
  void listen(int backlog = 5) override;
  const IOSocket accept(const struct timespec* timeout = nullptr) override;
  void connect(const sockaddr* serv_addr) override;
  Socket* clone() const override;
  virtual const std::string toString() const;
  const std::string addr2String() const override;
  bool isSameAddr(const Socket* rhs) const override;
  bool isSameAddr(const struct in_addr& ipv4Addr) const override;
  static int ping(const std::string& ipaddr, const struct timespec* timeout = nullptr);
  bool isConnected() const override;
  bool hasData() const override;

  joblist::DistributedEngineComm* dec_ = nullptr;
};

inline bool SameNodePseudoSocket::isOpen() const
{
  return true;
}

inline void SameNodePseudoSocket::sa(const sockaddr* /*sa*/)
{
}

}  // namespace messageqcpp

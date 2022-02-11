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

#ifndef _SOCKETPOOL_H_
#define _SOCKETPOOL_H_

#include <boost/utility.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include "bytestream.h"

namespace idbdatafile
{
/* This should be renamed; it's more than a pool, it also does the low-level communication.  TBD. */
class SocketPool : public boost::noncopyable
{
 public:
  SocketPool();

  // the dtor will immediately close all sockets
  virtual ~SocketPool();

  // 0 = success, -1 = failure.  Should this throw instead?
  int send_recv(messageqcpp::ByteStream& to_send, messageqcpp::ByteStream* to_recv);

 private:
  int getSocket();
  void returnSocket(const int sock);
  void remoteClosed(const int sock);

  std::vector<int> allSockets;
  std::deque<int> freeSockets;
  boost::mutex mutex;
  boost::condition_variable socketAvailable;
  uint maxSockets;
  static const uint defaultSockets = 20;
};

}  // namespace idbdatafile

#endif

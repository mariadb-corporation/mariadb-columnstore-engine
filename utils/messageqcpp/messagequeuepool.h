/* Copyright (C) 2017 MariaDB Corporation

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

#ifndef MESSAGEQCPP_MESSAGEQUEUECLIENT_H
#define MESSAGEQCPP_MESSAGEQUEUECLIENT_H

#include <map>
#include "messagequeue.h"

namespace messageqcpp
{
struct ClientObject
{
  MessageQueueClient* client;
  uint64_t lastUsed;
  bool inUse;

  ClientObject() : client(NULL), lastUsed(0), inUse(false)
  {
  }
};

class MessageQueueClientPool
{
 public:
  static MessageQueueClient* getInstance(const std::string& module);
  static MessageQueueClient* getInstance(const std::string& dnOrIp, uint64_t port);
  static void releaseInstance(MessageQueueClient* client);
  static void deleteInstance(MessageQueueClient* client);
  static MessageQueueClient* findInPool(const std::string& search);

 private:
  MessageQueueClientPool(){};
  ~MessageQueueClientPool(){};

  static std::multimap<std::string, ClientObject*> clientMap;
};

}  // namespace messageqcpp
#endif  // MESSAGEQCPP_MESSAGEQUEUECLIENT_H

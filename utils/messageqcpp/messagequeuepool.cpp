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

#include <sstream>
#include <map>
#include <boost/thread/mutex.hpp>
#include <time.h>
#include "messagequeuepool.h"
#include "messagequeue.h"

namespace messageqcpp
{
boost::mutex queueMutex;
// Make linker happy
std::multimap<std::string, ClientObject*> MessageQueueClientPool::clientMap;

// 300 seconds idle until cleanup
#define MAX_IDLE_TIME 300

static uint64_t TimeSpecToSeconds(struct timespec* ts)
{
  return (uint64_t)ts->tv_sec + (uint64_t)ts->tv_nsec / 1000000000;
}

MessageQueueClient* MessageQueueClientPool::getInstance(const std::string& dnOrIp, uint64_t port)
{
  boost::mutex::scoped_lock lock(queueMutex);

  std::ostringstream oss;
  oss << dnOrIp << "_" << port;
  std::string searchString = oss.str();

  MessageQueueClient* returnClient = MessageQueueClientPool::findInPool(searchString);

  // We found one, return it
  if (returnClient != NULL)
  {
    return returnClient;
  }

  // We didn't find one, create new one
  ClientObject* newClientObject = new ClientObject();
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  uint64_t nowSeconds = TimeSpecToSeconds(&now);

  newClientObject->client = new MessageQueueClient(dnOrIp, port);
  newClientObject->inUse = true;
  newClientObject->lastUsed = nowSeconds;
  clientMap.insert(std::pair<std::string, ClientObject*>(searchString, newClientObject));
  return newClientObject->client;
}

MessageQueueClient* MessageQueueClientPool::getInstance(const std::string& module)
{
  boost::mutex::scoped_lock lock(queueMutex);

  MessageQueueClient* returnClient = MessageQueueClientPool::findInPool(module);

  // We found one, return it
  if (returnClient != NULL)
  {
    return returnClient;
  }

  // We didn't find one, create new one
  ClientObject* newClientObject = new ClientObject();
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  uint64_t nowSeconds = TimeSpecToSeconds(&now);

  newClientObject->client = new MessageQueueClient(module);
  newClientObject->inUse = true;
  newClientObject->lastUsed = nowSeconds;
  clientMap.insert(std::pair<std::string, ClientObject*>(module, newClientObject));
  return newClientObject->client;
}

MessageQueueClient* MessageQueueClientPool::findInPool(const std::string& search)
{
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  uint64_t nowSeconds = TimeSpecToSeconds(&now);
  MessageQueueClient* returnClient = NULL;

  std::multimap<std::string, ClientObject*>::iterator it = clientMap.begin();

  // Scan pool
  while (it != clientMap.end())
  {
    ClientObject* clientObject = it->second;
    uint64_t elapsedTime = nowSeconds - clientObject->lastUsed;

    // If connection hasn't been used for MAX_IDLE_TIME we probably don't need it so drop it
    // Don't drop in use connections that have been in use a long time
    if ((elapsedTime >= MAX_IDLE_TIME) && (!clientObject->inUse))
    {
      delete clientObject->client;
      delete clientObject;
      // Do this so we don't invalidate current interator
      std::multimap<std::string, ClientObject*>::iterator toDelete = it;
      it++;
      clientMap.erase(toDelete);
      continue;
    }

    if (!clientObject->inUse)
    {
      MessageQueueClient* client = clientObject->client;

      // If the unused socket isn't connected or has data pending read, destroy it
      if (!client->isConnected() || client->hasData())
      {
        delete client;
        delete clientObject;
        // Do this so we don't invalidate current interator
        std::multimap<std::string, ClientObject*>::iterator toDelete = it;
        it++;
        clientMap.erase(toDelete);
        continue;
      }
    }

    // If connection matches store it for later, but keep scanning the pool for more timeout prunes
    if (it->first.compare(search) == 0)
    {
      if ((returnClient == NULL) && (!clientObject->inUse))
      {
        returnClient = clientObject->client;
        clientObject->inUse = true;
        return returnClient;
      }
    }

    it++;
  }

  return NULL;
}

void MessageQueueClientPool::releaseInstance(MessageQueueClient* client)
{
  // Scan pool for pointer and release
  // Set the last used and mark as not in use

  if (client == NULL)
    return;

  boost::mutex::scoped_lock lock(queueMutex);
  std::multimap<std::string, ClientObject*>::iterator it = clientMap.begin();

  while (it != clientMap.end())
  {
    if (it->second->client == client)
    {
      struct timespec now;
      clock_gettime(CLOCK_MONOTONIC, &now);
      uint64_t nowSeconds = TimeSpecToSeconds(&now);
      it->second->inUse = false;
      it->second->lastUsed = nowSeconds;
      return;
    }

    it++;
  }
}

// WriteEngine needs this as it forces connections closed and can't reuse. Also good for connection errors
void MessageQueueClientPool::deleteInstance(MessageQueueClient* client)
{
  // Scan pool for pointer and delete
  // Set the last used and mark as not in use

  if (client == NULL)
    return;

  boost::mutex::scoped_lock lock(queueMutex);
  std::multimap<std::string, ClientObject*>::iterator it = clientMap.begin();

  while (it != clientMap.end())
  {
    if (it->second->client == client)
    {
      delete it->second->client;
      delete it->second;
      clientMap.erase(it);
      return;
    }

    it++;
  }
}
}  // namespace messageqcpp

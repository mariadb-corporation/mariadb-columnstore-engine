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
#include <time.h>
#include <mutex>
#include "messagequeuepool.h"
#include "messagequeue.h"

#include <new>
#include <type_traits>

namespace messageqcpp
{

using ClientMapType = std::multimap<std::string, std::unique_ptr<ClientObject>>;

template <class T>
struct Immortal
{
  template <class... Args>
  Immortal(Args&&... args)
  {
    ::new (space) T(std::forward<Args>(args)...);
  }

  operator T&() & noexcept
  {
    return reinterpret_cast<T&>(space);
  }

 private:
  alignas(T) unsigned char space[sizeof(T)];
};

class LockedClientMap
{
  struct KeyToUsePrivateCtor
  {
    explicit KeyToUsePrivateCtor() = default;
  };

 public:
  LockedClientMap(const LockedClientMap&) = delete;
  LockedClientMap& operator=(const LockedClientMap&) = delete;
  ~LockedClientMap() = delete;

  static LockedClientMap& getInstance()
  {
    static Immortal<LockedClientMap> instance(KeyToUsePrivateCtor{});
    return instance;
  }

  LockedClientMap(KeyToUsePrivateCtor)
  {
  }

 private:
  ClientMapType clientMap;
  std::mutex queueMutex;

  friend class MessageQueueClientPool;
};

// 300 seconds idle until cleanup
#define MAX_IDLE_TIME 300

static uint64_t TimeSpecToSeconds(struct timespec* ts)
{
  return (uint64_t)ts->tv_sec + (uint64_t)ts->tv_nsec / 1000000000;
}

MessageQueueClient* MessageQueueClientPool::getInstance(const std::string& dnOrIp, uint64_t port)
{
  auto lock = std::scoped_lock(LockedClientMap::getInstance().queueMutex);

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

  newClientObject->client.reset(new MessageQueueClient(dnOrIp, port));
  newClientObject->inUse = true;
  newClientObject->lastUsed = nowSeconds;
  LockedClientMap::getInstance().clientMap.emplace(std::move(searchString), std::move(newClientObject));
  return newClientObject->client.get();
}

MessageQueueClient* MessageQueueClientPool::getInstance(const std::string& module)
{
  auto lock = std::scoped_lock(LockedClientMap::getInstance().queueMutex);

  MessageQueueClient* returnClient = MessageQueueClientPool::findInPool(module);

  // We found one, return it
  if (returnClient != NULL)
  {
    return returnClient;
  }

  // We didn't find one, create new one
  auto newClientObject = std::make_unique<ClientObject>();
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  uint64_t nowSeconds = TimeSpecToSeconds(&now);

  newClientObject->client.reset(new MessageQueueClient(module));
  newClientObject->inUse = true;
  newClientObject->lastUsed = nowSeconds;
  auto result = newClientObject->client.get();
  LockedClientMap::getInstance().clientMap.emplace(std::move(module), std::move(newClientObject));
  return result;
}

MessageQueueClient* MessageQueueClientPool::findInPool(const std::string& search)
{
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  uint64_t nowSeconds = TimeSpecToSeconds(&now);
  MessageQueueClient* returnClient = NULL;

  auto it = LockedClientMap::getInstance().clientMap.begin();

  // Scan pool
  while (it != LockedClientMap::getInstance().clientMap.end())
  {
    ClientObject* clientObject = it->second.get();
    uint64_t elapsedTime = nowSeconds - clientObject->lastUsed;

    // If connection hasn't been used for MAX_IDLE_TIME we probably don't need it so drop it
    // Don't drop in use connections that have been in use a long time
    if ((elapsedTime >= MAX_IDLE_TIME) && (!clientObject->inUse))
    {
      // Do this so we don't invalidate current interator
      auto toDelete = it;
      it++;
      LockedClientMap::getInstance().clientMap.erase(toDelete);
      continue;
    }

    if (!clientObject->inUse)
    {
      MessageQueueClient* client = clientObject->client.get();

      // If the unused socket isn't connected or has data pending read, destroy it
      if (!client->isConnected() || client->hasData())
      {
        // Do this so we don't invalidate current interator
        auto toDelete = it;
        it++;
        LockedClientMap::getInstance().clientMap.erase(toDelete);
        continue;
      }
    }

    // If connection matches store it for later, but keep scanning the pool for more timeout prunes
    if (it->first.compare(search) == 0)
    {
      if ((returnClient == NULL) && (!clientObject->inUse))
      {
        returnClient = clientObject->client.get();
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

  auto lock = std::scoped_lock(LockedClientMap::getInstance().queueMutex);
  auto it = LockedClientMap::getInstance().clientMap.begin();

  while (it != LockedClientMap::getInstance().clientMap.end())
  {
    if (it->second->client.get() == client)
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

  auto lock = std::scoped_lock(LockedClientMap::getInstance().queueMutex);
  auto it = LockedClientMap::getInstance().clientMap.begin();

  while (it != LockedClientMap::getInstance().clientMap.end())
  {
    if (it->second->client.get() == client)
    {
      LockedClientMap::getInstance().clientMap.erase(it);
      return;
    }

    it++;
  }
}
}  // namespace messageqcpp

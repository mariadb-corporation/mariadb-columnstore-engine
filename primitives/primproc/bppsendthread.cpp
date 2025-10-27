/* Copyright (C) 2014 InfiniDB, Inc.

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

/***********************************************************************
 *   $Id$
 *
 *
 ***********************************************************************/

#include <unistd.h>
#include <stdexcept>
#include <mutex>
#include "bppsendthread.h"
#include "resourcemanager.h"
#include "serviceexemgr.h"

namespace primitiveprocessor
{
extern uint32_t connectionsPerUM;
extern uint32_t BPPCount;

BPPSendThread::BPPSendThread()
{
  queueBytesThresh = joblist::ResourceManager::instance()->getBPPSendThreadBytesThresh();
  queueMsgThresh = joblist::ResourceManager::instance()->getBPPSendThreadMsgThresh();
  runner = boost::thread(Runner_t(this));
}

BPPSendThread::~BPPSendThread()
{
  abort();
  runner.join();
}

void BPPSendThread::sendResult(const Msg_t& msg, bool newConnection)
{
  // Wait for the queue to empty out a bit if it's stuffed full
  if (sizeTooBig())
  {
    std::unique_lock<std::mutex> sl1(respondLock);
    while (currentByteSize.load() >= queueBytesThresh && msgQueue.size() > 3 && !die.load())
    {
      fProcessorPool->incBlockedThreads();
      okToRespond.wait(sl1);
      fProcessorPool->decBlockedThreads();
    }
  }
  if (die.load())
    return;

  std::unique_lock<std::mutex> sl(msgQueueLock);

  if (gotException.load())
    throw std::runtime_error(exceptionString);

  currentByteSize.fetch_add(msg.msg->lengthWithHdrOverhead(), std::memory_order_relaxed);
  msgQueue.push(msg);

  if (!sawAllConnections && newConnection)
  {
    Connection_t ins(msg.sockLock, msg.sock);
    bool inserted = connections_s.insert(ins).second;

    if (inserted)
    {
      connections_v.push_back(ins);

      if (connections_v.size() == connectionsPerUM)
      {
        connections_s.clear();
        sawAllConnections = true;
      }
    }
  }

  if (mainThreadWaiting.load())
    queueNotEmpty.notify_one();
}

void BPPSendThread::sendResults(const std::vector<Msg_t>& msgs, bool newConnection)
{
  // Wait for the queue to empty out a bit if it's stuffed full
  if (sizeTooBig())
  {
    std::unique_lock<std::mutex> sl1(respondLock);
    while (currentByteSize.load() >= queueBytesThresh && msgQueue.size() > 3 && !die.load())
    {
      fProcessorPool->incBlockedThreads();
      okToRespond.wait(sl1);
      fProcessorPool->decBlockedThreads();
    }
  }
  if (die.load())
    return;

  std::unique_lock<std::mutex> sl(msgQueueLock);

  if (gotException.load())
    throw std::runtime_error(exceptionString);

  if (!sawAllConnections && newConnection)
  {
    idbassert(msgs.size() > 0);
    Connection_t ins(msgs[0].sockLock, msgs[0].sock);
    bool inserted = connections_s.insert(ins).second;

    if (inserted)
    {
      connections_v.push_back(ins);

      if (connections_v.size() == connectionsPerUM)
      {
        connections_s.clear();
        sawAllConnections = true;
      }
    }
  }

  for (uint32_t i = 0; i < msgs.size(); i++)
  {
    currentByteSize.fetch_add(msgs[i].msg->lengthWithHdrOverhead(), std::memory_order_relaxed);
    msgQueue.push(msgs[i]);
  }

  if (mainThreadWaiting.load())
    queueNotEmpty.notify_one();
}

void BPPSendThread::sendMore(int num)
{
  std::unique_lock<std::mutex> sl(ackLock);

  if (num == -1)
    fcEnabled.store(false, std::memory_order_relaxed);
  else if (num == 0)
  {
    fcEnabled.store(true, std::memory_order_relaxed);
    msgsLeft.store(0, std::memory_order_relaxed);
  }
  else
    msgsLeft.fetch_add(num, std::memory_order_relaxed);

  sl.unlock();
  if (waiting)
    okToSend.notify_one();
}

bool BPPSendThread::flowControlEnabled()
{
  return fcEnabled.load();
}

void BPPSendThread::mainLoop()
{
  const uint32_t msgCap = 20;
  boost::scoped_array<Msg_t> msg;
  uint32_t msgCount = 0, i, msgsSent;
  SP_UM_MUTEX lock;
  SP_UM_IOSOCK sock;
  bool doLoadBalancing = false;

  msg.reset(new Msg_t[msgCap]);

  while (!die.load())
  {
    std::unique_lock<std::mutex> sl(msgQueueLock);

    if (msgQueue.empty() && !die.load())
    {
      mainThreadWaiting.store(true, std::memory_order_relaxed);
      queueNotEmpty.wait(sl);
      mainThreadWaiting.store(false, std::memory_order_relaxed);
      continue;
    }

    msgCount = (msgQueue.size() > msgCap ? msgCap : msgQueue.size());

    for (i = 0; i < msgCount; i++)
    {
      msg[i] = msgQueue.front();
      msgQueue.pop();
    }

    doLoadBalancing = sawAllConnections;
    sl.unlock();

    /* In the send loop below, msgsSent tracks progress on sending the msg array,
     * i how many msgs are sent by 1 run of the loop, limited by msgCount or msgsLeft. */
    msgsSent = 0;

    while (msgsSent < msgCount && !die.load())
    {
      uint64_t bsSize;

      if (msgsLeft.load() <= 0 && fcEnabled.load() && !die.load())
      {
        std::unique_lock<std::mutex> sl2(ackLock);
        while (msgsLeft.load() <= 0 && fcEnabled.load() && !die.load())
        {
          waiting = true;
          okToSend.wait(sl2);
          waiting = false;
        }
      }

      for (i = 0; msgsSent < msgCount && ((fcEnabled.load() && msgsLeft.load() > 0) || !fcEnabled.load()) && !die.load(); msgsSent++, i++)
      {
        if (doLoadBalancing)
        {
          // Bug 4475 move control of sockIndex to batchPrimitiveProcessor
          lock = connections_v[msg[msgsSent].sockIndex].sockLock;
          sock = connections_v[msg[msgsSent].sockIndex].sock;
        }
        else
        {
          lock = msg[msgsSent].sockLock;
          sock = msg[msgsSent].sock;
        }

        bsSize = msg[msgsSent].msg->lengthWithHdrOverhead();

        // Same node processing path
        if (!lock)
        {
          msg[msgsSent].sock->write(msg[msgsSent].msg);
        }
        else
        {
          try
          {
            boost::mutex::scoped_lock sl2(*lock);
            sock->write(*msg[msgsSent].msg);
          }
          catch (std::exception& e)
          {
            sl.lock();
            exceptionString = e.what();
            gotException.store(true, std::memory_order_relaxed);
            return;
          }
        }

        msgsLeft.fetch_sub(1, std::memory_order_relaxed);
        currentByteSize.fetch_sub(bsSize, std::memory_order_relaxed);
        msg[msgsSent].msg.reset();
      }

      if (fProcessorPool->blockedThreadCount() > 0 && currentByteSize.load() < queueBytesThresh)
      {
        okToRespond.notify_one();
      }
    }
  }
}

void BPPSendThread::abort()
{
  std::lock_guard<std::mutex> sl(msgQueueLock);
  std::lock_guard<std::mutex> sl2(ackLock);
  std::lock_guard<std::mutex> sl3(respondLock);

  die.store(true, std::memory_order_relaxed);

  queueNotEmpty.notify_all();
  okToSend.notify_all();
  okToRespond.notify_all();
}

}  // namespace primitiveprocessor

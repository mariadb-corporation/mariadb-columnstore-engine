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
/** @file */

#pragma once

#include "fair_threadpool.h"
#include "umsocketselector.h"
#include <queue>
#include <set>
#include <condition_variable>
#include <utility>
#include "threadnaming.h"
#include "fair_threadpool.h"

namespace primitiveprocessor
{
class BPPSendThread
{
 public:
  BPPSendThread();  // starts unthrottled
  virtual ~BPPSendThread();

  struct Msg_t
  {
    messageqcpp::SBS msg;
    SP_UM_IOSOCK sock;
    SP_UM_MUTEX sockLock;
    int sockIndex;  // Socket index for round robin control. Bug 4475
    Msg_t() : sockIndex(0)
    {
    }
    Msg_t(const Msg_t& m) = default;
    Msg_t& operator=(const Msg_t& m) = default;
    Msg_t(messageqcpp::SBS m, SP_UM_IOSOCK so, SP_UM_MUTEX sl, int si)
     : msg(std::move(m)), sock(std::move(so)), sockLock(std::move(sl)), sockIndex(si)
    {
    }
  };

  bool sizeTooBig()
  {
    // keep the queue size below the 100 msg threshold & below the 250MB mark,
    // but at least 3 msgs so there is always 1 ready to be sent.
    return ((msgQueue.size() > queueMsgThresh) ||
            (currentByteSize >= queueBytesThresh && msgQueue.size() > 3)) &&
           !die;
  }

  void sendMore(int num);
  void sendResults(const std::vector<Msg_t>& msgs, bool newConnection);
  void sendResult(const Msg_t& msg, bool newConnection);
  void mainLoop();
  bool flowControlEnabled();
  void abort();
  inline bool aborted() const
  {
    return die;
  }
  void setProcessorPool(boost::shared_ptr<threadpool::FairThreadPool> processorPool)
  {
    fProcessorPool = std::move(processorPool);
  }

 private:
  BPPSendThread(const BPPSendThread&);
  BPPSendThread& operator=(const BPPSendThread&);

  struct Runner_t
  {
    BPPSendThread* bppst;
    Runner_t(BPPSendThread* b) : bppst(b)
    {
    }
    void operator()()
    {
      utils::setThreadName("BPPSendThread");
      bppst->mainLoop();
    }
  };

  boost::thread runner;
  std::queue<Msg_t> msgQueue;
  std::mutex msgQueueLock;
  std::condition_variable queueNotEmpty;
  volatile bool die = false;
  volatile bool gotException = false;
  volatile bool mainThreadWaiting = false;
  std::string exceptionString;
  uint32_t queueMsgThresh = 0;
  volatile int32_t msgsLeft = -1;
  bool waiting = false;
  std::mutex ackLock;
  std::condition_variable okToSend;
  // Condition to prevent run away queue
  std::mutex respondLock;
  std::condition_variable okToRespond;

  /* Load balancing structures */
  struct Connection_t
  {
    Connection_t(SP_UM_MUTEX lock, SP_UM_IOSOCK so) : sockLock(std::move(lock)), sock(std::move(so))
    {
    }
    SP_UM_MUTEX sockLock;
    SP_UM_IOSOCK sock;
    bool operator<(const Connection_t& t) const
    {
      return sockLock.get() < t.sockLock.get();
    }
    bool operator==(const Connection_t& t) const
    {
      return sockLock.get() == t.sockLock.get();
    }
  };
  std::set<Connection_t> connections_s;
  std::vector<Connection_t> connections_v;
  bool sawAllConnections = false;
  volatile bool fcEnabled = false;

  /* secondary queue size restriction based on byte size */
  volatile uint64_t currentByteSize = 0;
  uint64_t queueBytesThresh;
  // Used to tell the ThreadPool It should consider additional threads because a
  // queue full event has happened and a thread has been blocked.
  boost::shared_ptr<threadpool::FairThreadPool> fProcessorPool;
};

}  // namespace primitiveprocessor

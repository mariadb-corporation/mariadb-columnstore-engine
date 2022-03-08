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

#include "batchprimitiveprocessor.h"
#include "umsocketselector.h"
#include <mutex>
#include <condition_variable>
#include <queue>
#include <set>
#include <boost/thread/thread.hpp>
#include "threadnaming.h"
#include "prioritythreadpool.h"
#ifndef BPPSENDTHREAD_H
#define BPPSENDTHREAD_H

namespace primitiveprocessor
{
class BPPSendThread
{
 public:
  BPPSendThread();                       // starts unthrottled
  BPPSendThread(uint32_t initMsgsLeft);  // starts throttled
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
    Msg_t(const Msg_t& m) : msg(m.msg), sock(m.sock), sockLock(m.sockLock), sockIndex(m.sockIndex)
    {
    }
    Msg_t& operator=(const Msg_t& m)
    {
      msg = m.msg;
      sock = m.sock;
      sockLock = m.sockLock;
      sockIndex = m.sockIndex;
      return *this;
    }
    Msg_t(const messageqcpp::SBS& m, const SP_UM_IOSOCK& so, const SP_UM_MUTEX& sl, int si)
     : msg(m), sock(so), sockLock(sl), sockIndex(si)
    {
    }
  };

  bool sizeTooBig()
  {
    // keep the queue size below the 100 msg threshold & below the 250MB mark,
    // but at least 3 msgs so there is always 1 ready to be sent.
    return ((msgQueue.size() > sizeThreshold) || (currentByteSize >= maxByteSize && msgQueue.size() > 3)) &&
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
  void setProcessorPool(threadpool::PriorityThreadPool* processorPool)
  {
    fProcessorPool = processorPool;
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
  volatile bool die, gotException, mainThreadWaiting;
  std::string exceptionString;
  uint32_t sizeThreshold;
  volatile int32_t msgsLeft;
  bool waiting;
  std::mutex ackLock;
  std::condition_variable okToSend;
  // Condition to prevent run away queue
  std::mutex respondLock;
  std::condition_variable okToRespond;

  /* Load balancing structures */
  struct Connection_t
  {
    Connection_t(const SP_UM_MUTEX& lock, const SP_UM_IOSOCK& so) : sockLock(lock), sock(so)
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
  bool sawAllConnections;
  volatile bool fcEnabled;

  /* secondary queue size restriction based on byte size */
  volatile uint64_t currentByteSize;
  uint64_t maxByteSize;
  // Used to tell the PriorityThreadPool It should consider additional threads because a
  // queue full event has happened and a thread has been blocked.
  threadpool::PriorityThreadPool* fProcessorPool;
};

}  // namespace primitiveprocessor

#endif

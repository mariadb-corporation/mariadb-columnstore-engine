/* Copyright (C) 2014 InfiniDB, Inc.
 * Copyright (C) 2016-2022 MariaDB Corporation.

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

//
// $Id: distributedenginecomm.cpp 9655 2013-06-25 23:08:13Z xlou $
//
// C++ Implementation: distributedenginecomm
//
// Description:
//
//
// Author:  <pfigg@calpont.com>, (C) 2006
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <sstream>
#include <stdexcept>
#include <cassert>
#include <ctime>
#include <algorithm>
#include <unistd.h>
#include <chrono>
#include <thread>
#include <ifaddrs.h>
using namespace std;

#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "distributedenginecomm.h"

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "configcpp.h"
using namespace config;

#include "errorids.h"
#include "exceptclasses.h"
#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
using namespace logging;

#include "liboamcpp.h"
using namespace oam;

#include "jobstep.h"
using namespace joblist;

#include "atomicops.h"

namespace
{
void writeToLog(const char* file, int line, const string& msg, LOG_TYPE logto = LOG_TYPE_INFO)
{
  LoggingID lid(05);
  MessageLog ml(lid);
  Message::Args args;
  Message m(0);
  args.add(file);
  args.add("@");
  args.add(line);
  args.add(msg);
  m.format(args);

  switch (logto)
  {
    case LOG_TYPE_DEBUG: ml.logDebugMessage(m); break;

    case LOG_TYPE_INFO: ml.logInfoMessage(m); break;

    case LOG_TYPE_WARNING: ml.logWarningMessage(m); break;

    case LOG_TYPE_ERROR: ml.logWarningMessage(m); break;

    case LOG_TYPE_CRITICAL: ml.logCriticalMessage(m); break;
  }
}

// @bug 1463. this function is added for PM failover. for dual/more nic PM,
// this function is used to get the module name
string getModuleNameByIPAddr(oam::ModuleTypeConfig moduletypeconfig, string ipAddress)
{
  string modulename = "";
  DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();

  for (; pt != moduletypeconfig.ModuleNetworkList.end(); pt++)
  {
    modulename = (*pt).DeviceName;
    HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

    for (; pt1 != (*pt).hostConfigList.end(); pt1++)
    {
      if (ipAddress == (*pt1).IPAddr)
        return modulename;
    }
  }

  return modulename;
}

struct EngineCommRunner
{
  EngineCommRunner(joblist::DistributedEngineComm* jl, boost::shared_ptr<MessageQueueClient> cl,
                   uint32_t connectionIndex)
   : jbl(jl), client(cl), connIndex(connectionIndex)
  {
  }
  joblist::DistributedEngineComm* jbl;
  boost::shared_ptr<MessageQueueClient> client;
  uint32_t connIndex;
  void operator()()
  {
    // cout << "Listening on client at 0x" << hex << (ptrdiff_t)client << dec << endl;
    try
    {
      jbl->Listen(client, connIndex);
    }
    catch (std::exception& ex)
    {
      string what(ex.what());
      cerr << "exception caught in EngineCommRunner: " << what << endl;

      if (what.find("St9bad_alloc") != string::npos)
      {
        writeToLog(__FILE__, __LINE__, what, LOG_TYPE_CRITICAL);
        //           abort();
      }
      else
        writeToLog(__FILE__, __LINE__, what);
    }
    catch (...)
    {
      string msg("exception caught in EngineCommRunner.");
      writeToLog(__FILE__, __LINE__, msg);
      cerr << msg << endl;
    }
  }
};

template <typename T>
struct QueueShutdown
{
  void operator()(T& x)
  {
    x.shutdown();
  }
};

}  // namespace

/** Debug macro */
#define THROTTLE_DEBUG 0
#if THROTTLE_DEBUG
#define THROTTLEDEBUG std::cout
#else
#define THROTTLEDEBUG \
  if (false)          \
  std::cout
#endif

namespace joblist
{
DistributedEngineComm* DistributedEngineComm::fInstance = 0;

/*static*/
DistributedEngineComm* DistributedEngineComm::instance(ResourceManager* rm, bool isExeMgr)
{
  if (fInstance == 0)
  {
    fInstance = new DistributedEngineComm(rm, isExeMgr);
    if (isExeMgr && fInstance)
    {
      fInstance->getLocalNetIfacesSins();
    }
  }

  return fInstance;
}

/*static*/
void DistributedEngineComm::reset()
{
  delete fInstance;
  fInstance = 0;
}

DistributedEngineComm::DistributedEngineComm(ResourceManager* rm, bool isExeMgr)
 : fRm(rm), pmCount(0), fIsExeMgr(isExeMgr)
{
  if (fIsExeMgr)
  {
    getLocalNetIfacesSins();
  }
  Setup();
}

DistributedEngineComm::~DistributedEngineComm()
{
  Close();
  fInstance = 0;
}

int32_t DistributedEngineComm::Setup()
{
  // This is here to ensure that this function does not get invoked multiple times simultaneously.
  boost::mutex::scoped_lock setupLock(fSetupMutex);

  makeBusy(true);

  // This needs to be here to ensure that whenever Setup function is called, the lists are
  // empty. It's possible junk was left behind if exception.
  ClientList::iterator iter;

  for (iter = newClients.begin(); iter != newClients.end(); ++iter)
  {
    (*iter)->shutdown();
  }

  newClients.clear();
  newLocks.clear();

  uint32_t newPmCount = fRm->getPsCount();
  tbpsThreadCount = fRm->getJlNumScanReceiveThreads();
  fDECConnectionsPerQuery = fRm->getDECConnectionsPerQuery();
  unsigned numConnections = getNumConnections();
  flowControlEnableBytesThresh = fRm->getDECEnableBytesThresh();
  flowControlDisableBytesThresh = fRm->getDECDisableBytesThresh();
  oam::Oam oam;
  ModuleTypeConfig moduletypeconfig;

  try
  {
    oam.getSystemConfig("pm", moduletypeconfig);
  }
  catch (...)
  {
    writeToLog(__FILE__, __LINE__, "oam.getSystemConfig error, unknown exception", LOG_TYPE_ERROR);
    throw runtime_error("Setup failed");
  }

  if (newPmCount == 0)
    writeToLog(__FILE__, __LINE__, "Got a config file with 0 PMs", LOG_TYPE_CRITICAL);

  auto* config = fRm->getConfig();
  std::vector<messageqcpp::AddrAndPortPair> pmsAddressesAndPorts;
  for (size_t i = 1; i <= newPmCount; ++i)
  {
    std::string pmConfigNodeName("PMS");
    pmConfigNodeName.append(std::to_string(i));
    // The port returned by getAddressAndPort can be 0 but this will be handled by
    // MessageQueueClient::connect
    pmsAddressesAndPorts.push_back(messageqcpp::getAddressAndPort(config, pmConfigNodeName));
  }

  // numConnections must be calculated as number of PMs * number of connections per PM.
  // This happens earlier in getNumConnections().
  for (size_t i = 0; i < numConnections; ++i)
  {
    size_t connectionId = i % newPmCount;
    boost::shared_ptr<MessageQueueClient> cl(new MessageQueueClient(
        pmsAddressesAndPorts[connectionId].first, pmsAddressesAndPorts[connectionId].second));
    if (clientAtTheSameHost(cl))
    {
      cl->atTheSameHost(true);
      assert(connectionId <= std::numeric_limits<uint32_t>::max());
      localConnectionId_ = connectionId;
    }
    std::shared_ptr<std::mutex> nl(new std::mutex());

    try
    {
      if (cl->connect())
      {
        newClients.push_back(cl);
        // assign the module name
        cl->moduleName(getModuleNameByIPAddr(moduletypeconfig, cl->addr2String()));
        newLocks.push_back(nl);
        StartClientListener(cl, i);
      }
      else
      {
        throw runtime_error("Connection refused from PMS" + std::to_string(connectionId));
      }
    }
    catch (std::exception& ex)
    {
      if (i < newPmCount)
        newPmCount = newPmCount > 1 ? newPmCount - 1 : 1;  // We can't afford to reduce newPmCount to 0

      writeToLog(__FILE__, __LINE__,
                 "Could not connect to PMS" + std::to_string(connectionId) + ": " + ex.what(),
                 LOG_TYPE_ERROR);
      if (newPmCount == 0)
        writeToLog(__FILE__, __LINE__, "No more PMs to try to connect to", LOG_TYPE_ERROR);

      return 1;
    }
    catch (...)
    {
      if (i < newPmCount)
        newPmCount = newPmCount > 1 ? newPmCount - 1 : 1;  // We can't afford to reduce newPmCount to 0

      writeToLog(__FILE__, __LINE__, "Could not connect to PMS" + std::to_string(connectionId),
                 LOG_TYPE_ERROR);
      if (newPmCount == 0)
        writeToLog(__FILE__, __LINE__, "No more PMs to try to connect to", LOG_TYPE_ERROR);
      return 1;
    }
  }

  // for every entry in newClients up to newPmCount, scan for the same ip in the
  // first pmCount.  If there is no match, it's a new node,
  //    call the event listeners' newPMOnline() callbacks.
  boost::mutex::scoped_lock lock(eventListenerLock);

  for (uint32_t i = 0; i < newPmCount; i++)
  {
    uint32_t j;

    for (j = 0; j < pmCount; j++)
    {
      if (!fPmConnections.empty() && j < fPmConnections.size() &&
          newClients[i]->isSameAddr(*fPmConnections[j]))
      {
        break;
      }
    }

    if (j == pmCount)
      for (uint32_t k = 0; k < eventListeners.size(); k++)
        eventListeners[k]->newPMOnline(i);
  }

  lock.unlock();

  fWlock.swap(newLocks);
  fPmConnections.swap(newClients);
  // memory barrier to prevent the pmCount assignment migrating upward
  atomicops::atomicMb();
  pmCount = newPmCount;

  newLocks.clear();
  newClients.clear();
  return 0;
}

int DistributedEngineComm::Close()
{
  // cout << "DistributedEngineComm::Close() called" << endl;

  makeBusy(false);
  // for each MessageQueueClient in pmConnections delete the MessageQueueClient;
  fPmConnections.clear();
  fPmReader.clear();
  return 0;
}

void DistributedEngineComm::Listen(boost::shared_ptr<MessageQueueClient> client, uint32_t connIndex)
{
  SBS sbs;

  try
  {
    while (Busy())
    {
      Stats stats;
      // TODO: This call blocks so setting Busy() in another thread doesn't work here...
      sbs = client->read(0, NULL, &stats);

      if (sbs->length() != 0)
      {
        addDataToOutput(sbs, connIndex, &stats);
      }
      else  // got zero bytes on read, nothing more will come
        goto Error;
    }

    return;
  }
  catch (std::exception& e)
  {
    cerr << "DEC Caught EXCEPTION: " << e.what() << endl;
    goto Error;
  }
  catch (...)
  {
    cerr << "DEC Caught UNKNOWN EXCEPT" << endl;
    goto Error;
  }

Error:
  // @bug 488 - error condition! push 0 length bs to messagequeuemap and
  // eventually let jobstep error out.
  std::unique_lock lk(fMlock);
  MessageQueueMap::iterator map_tok;
  sbs.reset(new ByteStream(0));

  for (map_tok = fSessionMessages.begin(); map_tok != fSessionMessages.end(); ++map_tok)
  {
    map_tok->second->queue.clear();
    (void)atomicops::atomicInc(&map_tok->second->unackedWork[0]);
    map_tok->second->queue.push(sbs);
  }
  lk.unlock();

  if (fIsExeMgr)
  {
    decltype(pmCount) originalPMCount = pmCount;
    // Re-establish if a remote PM restarted.
    std::this_thread::sleep_for(std::chrono::seconds(3));
    auto rc = Setup();
    if (rc || originalPMCount != pmCount)
    {
      ostringstream os;
      os << "DEC: lost connection to " << client->addr2String();
      writeToLog(__FILE__, __LINE__, os.str(), LOG_TYPE_ERROR);
    }
  }
  return;
}

void DistributedEngineComm::addQueue(uint32_t key, bool sendACKs)
{
  bool b;

  boost::mutex* lock = new boost::mutex();
  condition* cond = new condition();
  uint32_t firstPMInterleavedConnectionId =
      key % (fPmConnections.size() / pmCount) * fDECConnectionsPerQuery * pmCount % fPmConnections.size();
  boost::shared_ptr<MQE> mqe(new MQE(pmCount, firstPMInterleavedConnectionId, flowControlEnableBytesThresh));

  mqe->queue = StepMsgQueue(lock, cond);
  mqe->sendACKs = sendACKs;
  mqe->throttled = false;

  std::lock_guard lk(fMlock);
  b = fSessionMessages.insert(pair<uint32_t, boost::shared_ptr<MQE> >(key, mqe)).second;

  if (!b)
  {
    ostringstream os;
    os << "DEC: attempt to add a queue with a duplicate ID " << key << endl;
    throw runtime_error(os.str());
  }
}

void DistributedEngineComm::removeQueue(uint32_t key)
{
  std::lock_guard lk(fMlock);
  MessageQueueMap::iterator map_tok = fSessionMessages.find(key);

  if (map_tok == fSessionMessages.end())
    return;

  map_tok->second->queue.shutdown();
  map_tok->second->queue.clear();
  fSessionMessages.erase(map_tok);
}

void DistributedEngineComm::shutdownQueue(uint32_t key)
{
  std::lock_guard lk(fMlock);
  MessageQueueMap::iterator map_tok = fSessionMessages.find(key);

  if (map_tok == fSessionMessages.end())
    return;

  map_tok->second->queue.shutdown();
  map_tok->second->queue.clear();
}

void DistributedEngineComm::read(uint32_t key, SBS& bs)
{
  boost::shared_ptr<MQE> mqe;

  // Find the StepMsgQueueList for this session
  std::unique_lock lk(fMlock);
  MessageQueueMap::iterator map_tok = fSessionMessages.find(key);

  if (map_tok == fSessionMessages.end())
  {
    ostringstream os;

    os << "DEC: attempt to read(bs) from a nonexistent queue\n";
    throw runtime_error(os.str());
  }

  mqe = map_tok->second;
  lk.unlock();

  // this method can block: you can't hold any locks here...
  TSQSize_t queueSize = mqe->queue.pop(&bs);

  if (bs && mqe->sendACKs)
  {
    std::unique_lock lk(ackLock);

    if (mqe->throttled && !mqe->hasBigMsgs && queueSize.size <= flowControlDisableBytesThresh)
      setFlowControl(false, key, mqe);

    vector<SBS> v;
    v.push_back(bs);
    sendAcks(key, v, mqe, queueSize.size);
  }

  if (!bs)
    bs.reset(new ByteStream());
}

const ByteStream DistributedEngineComm::read(uint32_t key)
{
  SBS sbs;
  boost::shared_ptr<MQE> mqe;

  // Find the StepMsgQueueList for this session
  std::unique_lock lk(fMlock);
  MessageQueueMap::iterator map_tok = fSessionMessages.find(key);

  if (map_tok == fSessionMessages.end())
  {
    ostringstream os;

    os << "DEC: read(): attempt to read from a nonexistent queue\n";
    throw runtime_error(os.str());
  }

  mqe = map_tok->second;
  lk.unlock();

  TSQSize_t queueSize = mqe->queue.pop(&sbs);

  if (sbs && mqe->sendACKs)
  {
    std::unique_lock lk(ackLock);

    if (mqe->throttled && !mqe->hasBigMsgs && queueSize.size <= flowControlDisableBytesThresh)
      setFlowControl(false, key, mqe);

    vector<SBS> v;
    v.push_back(sbs);
    sendAcks(key, v, mqe, queueSize.size);
  }

  if (!sbs)
    sbs.reset(new ByteStream());

  return *sbs;
}

void DistributedEngineComm::read_all(uint32_t key, vector<SBS>& v)
{
  boost::shared_ptr<MQE> mqe;

  std::unique_lock lk(fMlock);
  MessageQueueMap::iterator map_tok = fSessionMessages.find(key);

  if (map_tok == fSessionMessages.end())
  {
    ostringstream os;
    os << "DEC: read_all(): attempt to read from a nonexistent queue\n";
    throw runtime_error(os.str());
  }

  mqe = map_tok->second;
  lk.unlock();

  mqe->queue.pop_all(v);

  if (mqe->sendACKs)
  {
    std::unique_lock lk(ackLock);
    sendAcks(key, v, mqe, 0);
  }
}

void DistributedEngineComm::read_some(uint32_t key, uint32_t divisor, vector<SBS>& v, bool* flowControlOn)
{
  boost::shared_ptr<MQE> mqe;

  std::unique_lock lk(fMlock);
  MessageQueueMap::iterator map_tok = fSessionMessages.find(key);

  if (map_tok == fSessionMessages.end())
  {
    ostringstream os;

    os << "DEC: read_some(): attempt to read from a nonexistent queue\n";
    throw runtime_error(os.str());
  }

  mqe = map_tok->second;
  lk.unlock();

  TSQSize_t queueSize = mqe->queue.pop_some(divisor, v, 1);  // need to play with the min #

  if (flowControlOn)
    *flowControlOn = false;

  if (mqe->sendACKs)
  {
    std::unique_lock lk(ackLock);

    if (mqe->throttled && !mqe->hasBigMsgs && queueSize.size <= flowControlDisableBytesThresh)
      setFlowControl(false, key, mqe);

    sendAcks(key, v, mqe, queueSize.size);

    if (flowControlOn)
      *flowControlOn = mqe->throttled;
  }
}

void DistributedEngineComm::sendAcks(uint32_t uniqueID, const vector<SBS>& msgs, boost::shared_ptr<MQE> mqe,
                                     size_t queueSize)
{
  ISMPacketHeader* ism;
  uint32_t l_msgCount = msgs.size();

  /* If the current queue size > target, do nothing.
   * If the original queue size > target, ACK the msgs below the target.
   */
  if (!mqe->throttled || queueSize >= mqe->targetQueueSize)
  {
    /* no acks will be sent, but update unackedwork to keep the #s accurate */
    uint16_t numack = 0;
    uint32_t sockidx = 0;

    while (l_msgCount > 0)
    {
      nextPMToACK(mqe, l_msgCount, &sockidx, &numack);
      idbassert(numack <= l_msgCount);
      l_msgCount -= numack;
    }

    return;
  }

  size_t totalMsgSize = 0;

  for (uint32_t i = 0; i < msgs.size(); i++)
    totalMsgSize += msgs[i]->lengthWithHdrOverhead();

  if (queueSize + totalMsgSize > mqe->targetQueueSize)
  {
    /* update unackedwork for the overage that will never be acked */
    int64_t overage = queueSize + totalMsgSize - mqe->targetQueueSize;
    uint16_t numack = 0;
    uint32_t sockidx = 0;
    uint32_t msgsToIgnore;

    for (msgsToIgnore = 0; overage >= 0; msgsToIgnore++)
      overage -= msgs[msgsToIgnore]->lengthWithHdrOverhead();

    if (overage < 0)
      msgsToIgnore--;

    l_msgCount = msgs.size() - msgsToIgnore;  // this num gets acked

    while (msgsToIgnore > 0)
    {
      nextPMToACK(mqe, msgsToIgnore, &sockidx, &numack);
      idbassert(numack <= msgsToIgnore);
      msgsToIgnore -= numack;
    }
  }

  if (l_msgCount > 0)
  {
    SBS msg(new ByteStream(sizeof(ISMPacketHeader)));
    uint16_t* toAck;
    vector<bool> pmAcked(pmCount, false);

    ism = (ISMPacketHeader*)msg->getInputPtr();
    // The only var checked by ReadThread is the Command var.  The others
    // are wasted space.  We hijack the Size, & Flags fields for the
    // params to the ACK msg.

    ism->Interleave = uniqueID;
    ism->Command = BATCH_PRIMITIVE_ACK;
    toAck = &ism->Size;

    msg->advanceInputPtr(sizeof(ISMPacketHeader));
    // There must be only one local connection here.
    bool sendToLocal = false;
    while (l_msgCount > 0)
    {
      /* could have to send up to pmCount ACKs */
      uint32_t sockIndex = 0;

      /* This will reset the ACK field in the Bytestream directly, and nothing
       * else needs to change if multiple msgs are sent. */
      nextPMToACK(mqe, l_msgCount, &sockIndex, toAck);
      idbassert(*toAck <= l_msgCount);
      l_msgCount -= *toAck;
      if (sockIndex == localConnectionId_ && fIsExeMgr)
      {
        sendToLocal = true;
        continue;
      }
      pmAcked[sockIndex] = true;
      writeToClient(sockIndex, msg);
    }
    if (sendToLocal)
    {
      pmAcked[localConnectionId_] = true;
      writeToClient(localConnectionId_, msg);
    }

    // @bug4436, when no more unacked work, send an ack to all PMs that haven't been acked.
    // This is apply to the big message case only.  For small messages, the flow control is
    // disabled when the queue size is below the flowControlDisableBytesThresh.
    if (mqe->hasBigMsgs)
    {
      uint64_t totalUnackedWork = 0;

      for (uint32_t i = 0; i < pmCount; ++i)
        totalUnackedWork += mqe->unackedWork[i];

      if (totalUnackedWork == 0)
      {
        *toAck = 1;

        for (uint32_t i = 0; i < pmCount; ++i)
        {
          if (!pmAcked[i])
          {
            if (i == localConnectionId_ && fIsExeMgr)
            {
              continue;
            }
            writeToClient(i, msg);
          }
        }
        if (!pmAcked[localConnectionId_] && fIsExeMgr)
        {
          writeToClient(localConnectionId_, msg);
        }
      }
    }
  }
}

void DistributedEngineComm::nextPMToACK(boost::shared_ptr<MQE> mqe, uint32_t maxAck, uint32_t* sockIndex,
                                        uint16_t* numToAck)
{
  uint32_t& nextIndex = mqe->ackSocketIndex;

  /* Other threads can be touching mqe->unackedWork at the same time, but because of
   * the locking env, mqe->unackedWork can only grow; whatever gets latched in this fcn
   * is a safe minimum at the point of use. */

  if (mqe->unackedWork[nextIndex] >= maxAck)
  {
    (void)atomicops::atomicSub(&mqe->unackedWork[nextIndex], maxAck);
    *sockIndex = nextIndex;
    // FIXME: we're going to truncate here from 32 to 16 bits. Hopefully this will always fit...
    *numToAck = maxAck;

    if (pmCount > 0)
      nextIndex = (nextIndex + 1) % pmCount;

    return;
  }
  else
  {
    for (int i = pmCount - 1; i >= 0; --i)
    {
      uint32_t curVal = mqe->unackedWork[nextIndex];
      uint32_t unackedWork = (curVal > maxAck ? maxAck : curVal);

      if (unackedWork > 0)
      {
        (void)atomicops::atomicSub(&mqe->unackedWork[nextIndex], unackedWork);
        *sockIndex = nextIndex;
        *numToAck = unackedWork;

        if (pmCount > 0)
          nextIndex = (nextIndex + 1) % pmCount;

        return;
      }

      if (pmCount > 0)
        nextIndex = (nextIndex + 1) % pmCount;
    }

    cerr << "DEC::nextPMToACK(): Couldn't find a PM to ACK! ";

    for (int i = pmCount - 1; i >= 0; --i)
      cerr << mqe->unackedWork[i] << " ";

    cerr << " max: " << maxAck;
    cerr << endl;
    // make sure the returned vars are legitimate
    *sockIndex = nextIndex;
    *numToAck = maxAck / pmCount;

    if (pmCount > 0)
      nextIndex = (nextIndex + 1) % pmCount;

    return;
  }
}

void DistributedEngineComm::setFlowControl(bool enabled, uint32_t uniqueID, boost::shared_ptr<MQE> mqe)
{
  mqe->throttled = enabled;
  SBS msg(new ByteStream(sizeof(ISMPacketHeader)));
  ISMPacketHeader* ism = (ISMPacketHeader*)msg->getInputPtr();

  ism->Interleave = uniqueID;
  ism->Command = BATCH_PRIMITIVE_ACK;
  ism->Size = (enabled ? 0 : -1);

  msg->advanceInputPtr(sizeof(ISMPacketHeader));

  for (uint32_t i = 0; i < mqe->pmCount; ++i)
  {
    if (i == localConnectionId_ && fIsExeMgr)
    {
      continue;
    }
    writeToClient(i, msg);
  }
  if (fIsExeMgr)
    writeToClient(localConnectionId_, msg);
}

int32_t DistributedEngineComm::write(uint32_t senderID, const SBS& msg)
{
  ISMPacketHeader* ism = (ISMPacketHeader*)msg->buf();
  uint32_t dest;
  uint32_t numConn = fPmConnections.size();

  if (numConn > 0)
  {
    switch (ism->Command)
    {
      case BATCH_PRIMITIVE_CREATE:
        /* Disable flow control initially */
        *msg << (uint32_t)-1;
        /* FALLTHRU */

      case BATCH_PRIMITIVE_DESTROY:
      case BATCH_PRIMITIVE_ADD_JOINER:
      case BATCH_PRIMITIVE_END_JOINER:
      case BATCH_PRIMITIVE_ABORT:
      case DICT_CREATE_EQUALITY_FILTER:
      case DICT_DESTROY_EQUALITY_FILTER:
        /* XXXPAT: This relies on the assumption that the first pmCount "PMS*"
        entries in the config file point to unique PMs */
        {
          int32_t rc = 0;
          for (uint32_t i = 0; i < pmCount; ++i)
          {
            if (i == localConnectionId_ && fIsExeMgr)
            {
              continue;
            }

            if ((rc = writeToClient(i, msg, senderID)))
            {
              return rc;
            }
          }
          if (fIsExeMgr)
          {
            return writeToClient(localConnectionId_, msg);
          }
          return rc;
        }

      case BATCH_PRIMITIVE_RUN:
      case DICT_TOKEN_BY_SCAN_COMPARE:
      {
        // for efficiency, writeToClient() grabs the interleaving factor for the caller,
        // and decides the final connection index because it already grabs the
        // caller's queue information
        dest = ism->Interleave;
        return writeToClient(dest, msg, senderID, true);
      }

      default: idbassert_s(0, "Unknown message type");
    }
  }
  else
  {
    writeToLog(__FILE__, __LINE__, "No PrimProcs are running", LOG_TYPE_DEBUG);
    throw IDBExcept(ERR_NO_PRIMPROC);
  }
  return 0;
}

void DistributedEngineComm::write(messageqcpp::ByteStream& msg, uint32_t connection)
{
  ISMPacketHeader* ism = (ISMPacketHeader*)msg.buf();
  PrimitiveHeader* pm = (PrimitiveHeader*)(ism + 1);
  uint32_t senderID = pm->UniqueID;

  MessageQueueMap::iterator it;
  // This keeps mqe's stats from being freed until end of function
  boost::shared_ptr<MQE> mqe;
  Stats* senderStats = NULL;

  std::unique_lock lk(fMlock);
  it = fSessionMessages.find(senderID);

  if (it != fSessionMessages.end())
  {
    mqe = it->second;
    senderStats = &(mqe->stats);
  }

  lk.unlock();

  newClients[connection]->write(msg, NULL, senderStats);
}

void DistributedEngineComm::StartClientListener(boost::shared_ptr<MessageQueueClient> cl, uint32_t connIndex)
{
  boost::thread* thrd = new boost::thread(EngineCommRunner(this, cl, connIndex));
  fPmReader.push_back(thrd);
}

void DistributedEngineComm::addDataToOutput(SBS sbs)
{
  assert(localConnectionId_ < pmCount);
  return addDataToOutput(sbs, localConnectionId_, nullptr);
}

void DistributedEngineComm::addDataToOutput(SBS sbs, uint32_t connIndex, Stats* stats)
{
  ISMPacketHeader* hdr = (ISMPacketHeader*)(sbs->buf());
  PrimitiveHeader* p = (PrimitiveHeader*)(hdr + 1);
  uint32_t uniqueId = p->UniqueID;
  std::unique_lock lk(fMlock);
  MessageQueueMap::iterator map_tok = fSessionMessages.find(uniqueId);

  // The message for a session that doesn't exist.
  if (map_tok == fSessionMessages.end())
  {
    // Here gets the dead session ByteStream that is already removed
    // from DEC queue.
    return;
  }

  auto mqe = map_tok->second;
  lk.unlock();

  if (pmCount > 0)
  {
    (void)atomicops::atomicInc(&mqe->unackedWork[connIndex % pmCount]);
  }

  TSQSize_t queueSize = mqe->queue.push(sbs);

  if (mqe->sendACKs)
  {
    std::lock_guard lk(ackLock);
    uint64_t msgSize = sbs->lengthWithHdrOverhead();

    if (!mqe->throttled && msgSize > (flowControlEnableBytesThresh / 2))
      doHasBigMsgs(
          mqe, (bigMessageSize > 3 * msgSize ? bigMessageSize : 3 * msgSize));  // buffer at least 3 big msgs

    if (!mqe->throttled && queueSize.size >= mqe->targetQueueSize)
      setFlowControl(true, uniqueId, mqe);
  }

  if (stats)
    mqe->stats.dataRecvd(stats->dataRecvd());
}

void DistributedEngineComm::doHasBigMsgs(boost::shared_ptr<MQE> mqe, uint64_t targetSize)
{
  mqe->hasBigMsgs = true;

  if (mqe->targetQueueSize < targetSize)
    mqe->targetQueueSize = targetSize;
}

DistributedEngineComm::SBSVector& DistributedEngineComm::readLocalQueueMessagesOrWait(
    SBSVector& receivedMessages)
{
  for (;;)
  {
    std::unique_lock<std::mutex> exchangeLock(inMemoryEM2PPExchMutex_);
    if (inMemoryEM2PPExchQueue_.empty())
    {
      inMemoryEM2PPExchCV_.wait(exchangeLock);
      continue;
    }

    // Batch processing to reduce the crit section
    while (!inMemoryEM2PPExchQueue_.empty())
    {
      receivedMessages.push_back(inMemoryEM2PPExchQueue_.front());
      inMemoryEM2PPExchQueue_.pop();
    }
    exchangeLock.unlock();
    break;
  }

  return receivedMessages;
}

void DistributedEngineComm::pushToTheLocalQueueAndNotifyRecv(const messageqcpp::SBS& bs)
{
  std::unique_lock<std::mutex> exchangeLock(inMemoryEM2PPExchMutex_);
  inMemoryEM2PPExchQueue_.push(bs);
  exchangeLock.unlock();
  inMemoryEM2PPExchCV_.notify_one();
}

// This routine has an unexpected side-effect on its argument's SBS, namely
// SBS is cleared when it is sent to PP at the same host. This fact forces any
// code uses ::writeToClient to send to a local node in the last turn.
int DistributedEngineComm::writeToClient(size_t aPMIndex, const SBS& bs, uint32_t senderUniqueID,
                                         bool doInterleaving)
{
  MessageQueueMap::iterator it;
  // Keep mqe's stats from being freed early
  boost::shared_ptr<MQE> mqe;
  Stats* senderStats = NULL;

  if (fPmConnections.size() == 0)
    return 0;

  uint32_t connectionId = aPMIndex;
  assert(connectionId < fPmConnections.size());
  // EM-PP exchange via the queue.
  if (fPmConnections[connectionId]->atTheSameHost() && fIsExeMgr)
  {
    pushToTheLocalQueueAndNotifyRecv(bs);
    return 0;
  }

  if (senderUniqueID != numeric_limits<uint32_t>::max())
  {
    std::lock_guard lk(fMlock);
    it = fSessionMessages.find(senderUniqueID);

    if (it != fSessionMessages.end())
    {
      mqe = it->second;
      senderStats = &(mqe->stats);
      size_t pmIndex = aPMIndex % mqe->pmCount;
      connectionId = it->second->getNextConnectionId(pmIndex, fPmConnections.size(), fDECConnectionsPerQuery);
    }
  }

  ClientList::value_type client = fPmConnections[connectionId];
  try
  {
    if (!client->isAvailable())
      return 0;

    std::lock_guard lk(*(fWlock[connectionId]));
    client->write(bs, NULL, senderStats);
  }
  catch (...)
  {
    // @bug 488. error out under such condition instead of re-trying other connection,
    // by pushing 0 size bytestream to messagequeue and throw exception
    SBS sbs;
    std::unique_lock lk(fMlock);
    // std::cout << "WARNING: DEC WRITE BROKEN PIPE. PMS index = " << index << std::endl;
    MessageQueueMap::iterator map_tok;
    sbs.reset(new ByteStream(0));

    for (map_tok = fSessionMessages.begin(); map_tok != fSessionMessages.end(); ++map_tok)
    {
      map_tok->second->queue.clear();
      (void)atomicops::atomicInc(&map_tok->second->unackedWork[0]);
      map_tok->second->queue.push(sbs);
    }

    int tries = 0;
    // Try to setup connection with PS, it could be a situation that PS is starting.
    // MCOL-5263.
    while (tries < 10 && Setup())
    {
      ++tries;
      std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    lk.unlock();

    if (tries == 10)
    {
      ostringstream os;
      os << "DEC: lost connection to " << client->addr2String();
      writeToLog(__FILE__, __LINE__, os.str(), LOG_TYPE_ERROR);
      if (!fIsExeMgr)
        abort();
      else
        throw runtime_error("DistributedEngineComm::write: Broken Pipe error");
    }

    // Connection was established.
    return 1;
    /*
                    // reconfig the connection array
                    ClientList tempConns;
                    {
                            //cout << "WARNING: DEC WRITE BROKEN PIPE " <<
    fPmConnections[index]->otherEnd()<< endl; boost::mutex::scoped_lock onErrLock(fOnErrMutex); string
    moduleName = fPmConnections[index]->moduleName();
                            //cout << "module name = " << moduleName << endl;
                            if (index >= fPmConnections.size()) return 0;

                            for (uint32_t i = 0; i < fPmConnections.size(); i++)
                            {
                                    if (moduleName != fPmConnections[i]->moduleName())
                                            tempConns.push_back(fPmConnections[i]);
                            }
                            if (tempConns.size() == fPmConnections.size()) return 0;
                            fPmConnections.swap(tempConns);
                            pmCount = (pmCount == 0 ? 0 : pmCount - 1);
                    }
    // send alarm
    ALARMManager alarmMgr;
    string alarmItem("UNKNOWN");

    if (index < fPmConnections.size())
    {
        alarmItem = fPmConnections[index]->addr2String();
    }

    alarmItem.append(" PrimProc");
    alarmMgr.sendAlarmReport(alarmItem.c_str(), oam::CONN_FAILURE, SET);
    */
  }
  return 0;
}

uint32_t DistributedEngineComm::size(uint32_t key)
{
  std::unique_lock lk(fMlock);
  MessageQueueMap::iterator map_tok = fSessionMessages.find(key);

  if (map_tok == fSessionMessages.end())
    throw runtime_error("DEC::size() attempt to get the size of a nonexistant queue!");

  boost::shared_ptr<MQE> mqe = map_tok->second;
  // TODO: should probably check that this is a valid iter...
  lk.unlock();
  return mqe->queue.size().count;
}

void DistributedEngineComm::addDECEventListener(DECEventListener* l)
{
  boost::mutex::scoped_lock lk(eventListenerLock);
  eventListeners.push_back(l);
}

void DistributedEngineComm::removeDECEventListener(DECEventListener* l)
{
  boost::mutex::scoped_lock lk(eventListenerLock);
  std::vector<DECEventListener*> newListeners;
  uint32_t s = eventListeners.size();

  for (uint32_t i = 0; i < s; i++)
    if (eventListeners[i] != l)
      newListeners.push_back(eventListeners[i]);

  eventListeners.swap(newListeners);
}

Stats DistributedEngineComm::getNetworkStats(uint32_t uniqueID)
{
  std::lock_guard lk(fMlock);
  MessageQueueMap::iterator it;
  Stats empty;

  it = fSessionMessages.find(uniqueID);

  if (it != fSessionMessages.end())
    return it->second->stats;

  return empty;
}

DistributedEngineComm::MQE::MQE(const uint32_t pCount, const uint32_t initialInterleaverValue,
                                const uint64_t flowControlEnableBytesThresh)
 : ackSocketIndex(0), pmCount(pCount), hasBigMsgs(false), targetQueueSize(flowControlEnableBytesThresh)
{
  unackedWork.reset(new volatile uint32_t[pmCount]);
  interleaver.reset(new uint32_t[pmCount]);
  memset((void*)unackedWork.get(), 0, pmCount * sizeof(uint32_t));
  uint32_t interleaverValue = initialInterleaverValue;
  initialConnectionId = initialInterleaverValue;
  for (size_t pmId = 0; pmId < pmCount; ++pmId)
  {
    interleaver.get()[pmId] = interleaverValue++;
  }
}

// Here is the assumed connections distribution schema in a pmConnections vector.
// (PM1-PM2...-PMX)-(PM1-PM2..-PMX)...
uint32_t DistributedEngineComm::MQE::getNextConnectionId(const size_t pmIndex,
                                                         const size_t pmConnectionsNumber,
                                                         const uint32_t DECConnectionsPerQuery)

{
  uint32_t nextConnectionId = (interleaver[pmIndex] + pmCount) % pmConnectionsNumber;
  // Wrap around the connection id.
  if ((nextConnectionId - pmIndex) % DECConnectionsPerQuery == 0)
  {
    // The sum must be < connections vector size.
    nextConnectionId = initialConnectionId + pmIndex;
    interleaver[pmIndex] = nextConnectionId;
  }
  else
  {
    interleaver[pmIndex] = nextConnectionId;
  }
  return nextConnectionId;
}

template <typename T>
bool DistributedEngineComm::clientAtTheSameHost(T& client) const
{
  for (auto& sin : localNetIfaceSins_)
  {
    if (client->isSameAddr(sin))
    {
      return true;
    }
  }
  return false;
}

void DistributedEngineComm::getLocalNetIfacesSins()
{
  struct ifaddrs* netIfacesList = nullptr;
  struct ifaddrs* ifaceListMembPtr = nullptr;
  int success = 0;
  // retrieve the current interfaces - returns 0 on success
  success = getifaddrs(&netIfacesList);
  if (success == 0)
  {
    ifaceListMembPtr = netIfacesList;
    for (; ifaceListMembPtr; ifaceListMembPtr = ifaceListMembPtr->ifa_next)
    {
      if (ifaceListMembPtr->ifa_addr && ifaceListMembPtr->ifa_addr->sa_family == AF_INET)
      {
        localNetIfaceSins_.push_back(((struct sockaddr_in*)ifaceListMembPtr->ifa_addr)->sin_addr);
      }
    }
  }
  freeifaddrs(netIfacesList);
}
template bool DistributedEngineComm::clientAtTheSameHost<DistributedEngineComm::SharedPtrEMSock>(
    SharedPtrEMSock& client) const;
}  // namespace joblist

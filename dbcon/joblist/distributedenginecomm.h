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

//
// $Id: distributedenginecomm.h 9655 2013-06-25 23:08:13Z xlou $
//
// C++ Interface: distributedenginecomm
//
// Description:
//
//
// Author:  <pfigg@calpont.com>, (C) 2006
//
// Copyright: See COPYING file that comes with this distribution
//
//
/** @file */

#ifndef DISTENGINECOMM_H
#define DISTENGINECOMM_H

#include <iostream>
#include <vector>
#include <queue>
#include <string>
#include <map>
#include <mutex>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include <boost/scoped_array.hpp>

#include <oneapi/tbb/concurrent_hash_map.h>
#include <oneapi/tbb/concurrent_unordered_map.h>

#include "bytestream.h"
#include "primitivemsg.h"
#include "threadsafequeue.h"
#include "rwlock_local.h"
#include "resourcemanager.h"
#include "messagequeue.h"

#include "stopwatch.h"

class TestDistributedEngineComm;

#if defined(_MSC_VER) && defined(JOBLIST_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace messageqcpp
{
class MessageQueueClient;
}
namespace config
{
class Config;
}

/**
 * Namespace
 */
namespace joblist
{

class DECEventListener
{
public:
    virtual ~DECEventListener() { };

    /* Do whatever needs to be done to init the new PM */
    virtual void newPMOnline(uint32_t newConnectionNumber) = 0;
};

static constexpr uint32_t targetRecvQueueSize = 50000000;
static constexpr uint32_t disableThreshold = 10000000;
/* To keep some state associated with the connection.  These aren't copyable. */
//A queue of ByteStreams coming in from PrimProc heading for a JobStep
using StepMsgQueue = ThreadSafeQueueV2<messageqcpp::SBS>;

struct MQE : public boost::noncopyable
{
    MQE(uint32_t pmCount, uint32_t initialInterleaverValue);
    messageqcpp::Stats stats;
    StepMsgQueue queue;
    uint32_t ackSocketIndex;
    boost::scoped_array<volatile uint32_t> unackedWork;
    boost::scoped_array<uint32_t> interleaver;
    uint32_t pmCount;
    // non-BPP primitives don't do ACKs
    bool sendACKs;

    // This var will allow us to toggle flow control for BPP instances when
    // the UM is keeping up.  Send -1 as the ACK value to disable flow control
    // on the PM side, positive value to reenable it.  Not yet impl'd on the UM side.
    bool throttled;

    // This var signifies that the PM can return msgs big enough to keep toggling
    // FC on and off.  We force FC on in that case and maintain a larger buffer.
    bool hasBigMsgs;

    uint64_t targetQueueSize;
};

/**
 * class DistributedEngineComm
 */
class DistributedEngineComm
{
public:
    /**
     * Constructors
     */
    EXPORT virtual ~DistributedEngineComm();

    EXPORT static DistributedEngineComm* instance(ResourceManager* rm, bool isExeMgr = false);

    /** @brief delete the static instance
     *  This has the effect of causing the connection to be rebuilt
     */
    EXPORT static void reset();

    /** @brief currently a nop
     *
     */
    EXPORT int Open()
    {
        return 0;
    }

    void notifyClientsThatStreamEnds(); 

    EXPORT boost::shared_ptr<joblist::MQE> addQueue(uint32_t key, bool sendACKs = false);
    EXPORT void removeQueue(uint32_t key);
    EXPORT void shutdownQueue(uint32_t key, bool aErase = false);

    /** @brief read a primitive response
     *
     * Returns the next message in the inbound queue for session sessionId and step stepId.
     * @todo See if we can't save a copy by returning a const&
     */

    EXPORT const messageqcpp::ByteStream read(uint32_t key);
    /** @brief read a primitve response
     *
     * Returns the next message in the inbound queue for session sessionId and step stepId.
     * @param bs A pointer to the ByteStream to fill in.
     * @note: saves a copy vs read(uint32_t, uint32_t).
     */
    EXPORT void read(uint32_t key, messageqcpp::SBS&);
    bool read_one(uint32_t key, messageqcpp::SBS&);

    /** @brief read a primitve response
     *
     * Returns the next message in the inbound queue for session sessionId and step stepId.
     */
    EXPORT void read_all(uint32_t key, std::vector<messageqcpp::SBS>& v);

    /** reads queuesize/divisor msgs */
    EXPORT size_t read_some(uint32_t key,
                            uint32_t divisor,
                            std::vector<messageqcpp::SBS>& v,
                            bool* flowControlOn,
                            boost::shared_ptr<MQE> mqe,
                            logging::StopWatch* timer = nullptr);

    /** @brief Write a primitive message
     *
     * Writes a primitive message to a primitive server. Msg needs to conatin an ISMPacketHeader. The
     * LBID is extracted from the ISMPacketHeader and used to determine the actual P/M to send to.
     */
    EXPORT void write(uint32_t key, messageqcpp::ByteStream& msg);

    //EXPORT void throttledWrite(const messageqcpp::ByteStream& msg);

    /** @brief Special write function for use only by newPMOnline event handlers
    */
    EXPORT void write(messageqcpp::ByteStream& msg, uint32_t connection);

    /** @brief Shutdown this object
     *
     * Closes all the connections created during Setup() and cleans up other stuff.
     */
    EXPORT int Close();

    /** @brief Start listening for primitive responses
     *
     * Starts the current thread listening on the client socket for primitive response messages. Will not return
     * until busy() returns false or a zero-length response is received.
     */
    EXPORT void Listen(boost::shared_ptr<messageqcpp::MessageQueueClient> client, uint32_t connIndex);

    /** @brief set/unset busy flag
     *
     * Set or unset the busy flag so Listen() can return.
     */
    EXPORT void makeBusy(bool b)
    {
        fBusy = b;
    }

    /** @brief fBusy accessor
     *
     */
    EXPORT bool Busy() const
    {
        return fBusy;
    }

    /** @brief Returns the size of the queue for the specified jobstep
    */
    EXPORT uint32_t size(uint32_t key);

    EXPORT void Setup();

    EXPORT void addDECEventListener(DECEventListener*);
    EXPORT void removeDECEventListener(DECEventListener*);

    uint64_t connectedPmServers() const
    {
        return fPmConnections.size();
    }
    uint32_t getPmCount() const
    {
        return pmCount;
    }

    unsigned getNumConnections() const
    {
        unsigned cpp = (fIsExeMgr ? fRm->getPsConnectionsPerPrimProc() : 1);
        return fRm->getPsCount() * cpp;
    }

    messageqcpp::Stats getNetworkStats(uint32_t uniqueID);

    friend class ::TestDistributedEngineComm;

private:
    typedef std::vector<boost::thread*> ReaderList;
    typedef std::vector<boost::shared_ptr<messageqcpp::MessageQueueClient> > ClientList;

    //The mapping of session ids to StepMsgQueueLists
    using MessageQueueMap = tbb::concurrent_unordered_map<unsigned, boost::shared_ptr<MQE>>;

    explicit DistributedEngineComm(ResourceManager* rm, bool isExeMgr);

    void StartClientListener(boost::shared_ptr<messageqcpp::MessageQueueClient> cl, uint32_t connIndex);

    /** @brief Add a message to the queue
     *
     */
    void addDataToOutput(messageqcpp::SBS, uint32_t connIndex, messageqcpp::Stats* statsToAdd);

    /** @brief Writes data to the client at the index
    *
    * Continues trying to write data to the client at the next index until all clients have been tried.
    */
    int  writeToClient(const size_t index,
                       const messageqcpp::ByteStream& bs,
                       const uint32_t senderID = std::numeric_limits<uint32_t>::max(),
                       const bool doInterleaving = false);

    int  writeToClient(const size_t index,
                       const messageqcpp::ByteStream& bs,
                       MQE& mqe,
                       const uint32_t senderID = std::numeric_limits<uint32_t>::max(),
                       const bool doInterleaving = false);

    static DistributedEngineComm* fInstance;
    ResourceManager* fRm;

    ClientList fPmConnections; // all the pm servers
    ReaderList fPmReader;	// all the reader threads for the pm servers
    MessageQueueMap fSessionMessages; // place to put messages from the pm server to be returned by the Read method
    std::mutex fMlock; //sessionMessages mutex
    std::vector<boost::shared_ptr<boost::mutex> > fWlock; //PrimProc socket write mutexes
    bool fBusy;
    unsigned fLBIDShift;
    volatile uint32_t pmCount;
    boost::mutex fOnErrMutex;   // to lock function scope to reset pmconnections under error condition
    boost::mutex fSetupMutex;

    // event listener data
    std::vector<DECEventListener*> eventListeners;
    boost::mutex eventListenerLock;

    ClientList newClients;
    std::vector<boost::shared_ptr<boost::mutex> > newLocks;

    bool fIsExeMgr;

    // send-side throttling vars
    uint64_t throttleThreshold;
    uint32_t tbpsThreadCount;

    void sendAcks(uint32_t uniqueID, const std::vector<messageqcpp::SBS>& msgs,
                  boost::shared_ptr<MQE> mqe, size_t qSize);
    void nextPMToACK(boost::shared_ptr<MQE> mqe, uint32_t maxAck, uint32_t* sockIndex,
                     uint16_t* numToAck);
    void setFlowControl(bool enable, uint32_t uniqueID, MQE& mqe);
    void doHasBigMsgs(boost::shared_ptr<MQE> mqe, uint64_t targetSize);
    boost::mutex ackLock;
  public:
    inline boost::shared_ptr<MQE> getMqeForRead(const uint32_t key,
                                                const std::string& methodName)
    {
        auto mapIter = fSessionMessages.find(key);
        if (mapIter == fSessionMessages.end())
        {
            std::ostringstream os;
            os << "DEC: " << methodName << "() searches fom a nonexistent queue.\n";
            throw std::runtime_error(os.str());
        }
        return mapIter->second;
    }
};

}

#undef EXPORT

#endif // DISTENGINECOMM_H

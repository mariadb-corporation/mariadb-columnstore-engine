/* Copyright (C) 2014 InfiniDB, Inc.
 * Copyright (C) 2016-2021 MariaDB Corporation.

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
using namespace std;

#include <mutex>
#include <condition_variable>

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

void  writeToLog(const char* file, int line, const string& msg, LOG_TYPE logto = LOG_TYPE_INFO)
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
        case LOG_TYPE_DEBUG:
            ml.logDebugMessage(m);
            break;

        case LOG_TYPE_INFO:
            ml.logInfoMessage(m);
            break;

        case LOG_TYPE_WARNING:
            ml.logWarningMessage(m);
            break;

        case LOG_TYPE_ERROR:
            ml.logWarningMessage(m);
            break;

        case LOG_TYPE_CRITICAL:
            ml.logCriticalMessage(m);
            break;
    }
}

// @bug 1463. this function is added for PM failover. for dual/more nic PM,
// this function is used to get the module name
string getModuleNameByIPAddr(oam::ModuleTypeConfig moduletypeconfig,
                             string ipAddress)
{
    string modulename = "";
    DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();

    for ( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
    {
        modulename = (*pt).DeviceName;
        HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();

        for ( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
        {
            if (ipAddress == (*pt1).IPAddr)
                return modulename;
        }
    }

    return modulename;
}

struct EngineCommRunner
{
    EngineCommRunner(joblist::DistributedEngineComm* jl,
                     boost::shared_ptr<MessageQueueClient> cl, uint32_t connectionIndex) : jbl(jl), client(cl),
        connIndex(connectionIndex) {}
    joblist::DistributedEngineComm* jbl;
    boost::shared_ptr<MessageQueueClient> client;
    uint32_t connIndex;
    void operator()()
    {
        //cout << "Listening on client at 0x" << hex << (ptrdiff_t)client << dec << endl;
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
            else  writeToLog(__FILE__, __LINE__, what);
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
struct QueueShutdown : public unary_function<T&, void>
{
    void operator()(T& x)
    {
        x.shutdown();
    }
};

}

/** Debug macro */
#define THROTTLE_DEBUG 0
#if THROTTLE_DEBUG
#define THROTTLEDEBUG std::cout
#else
#define THROTTLEDEBUG if (false) std::cout
#endif

namespace joblist
{
DistributedEngineComm* DistributedEngineComm::fInstance = 0;

/*static*/
DistributedEngineComm* DistributedEngineComm::instance(ResourceManager* rm, bool isExeMgr)
{
    if (fInstance == 0)
        fInstance = new DistributedEngineComm(rm, isExeMgr);

    return fInstance;
}

/*static*/
void DistributedEngineComm::reset()
{
    delete fInstance;
    fInstance = 0;
}

DistributedEngineComm::DistributedEngineComm(ResourceManager* rm, bool isExeMgr) :
    fRm(rm),
    fLBIDShift(fRm->getPsLBID_Shift()),
    pmCount(0),
    fIsExeMgr(isExeMgr)
{

    Setup();
}

DistributedEngineComm::~DistributedEngineComm()
{
    Close();
    fInstance = 0;
}

void DistributedEngineComm::Setup()
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
    throttleThreshold = fRm->getDECThrottleThreshold();
    tbpsThreadCount = fRm->getJlNumScanReceiveThreads();
    unsigned numConnections = getNumConnections();
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
        writeToLog(__FILE__, __LINE__, "Got a config file with 0 PMs",
                   LOG_TYPE_CRITICAL);

    //This needs to make sense when compared to the extent size
    //     fLBIDShift = static_cast<unsigned>(config::Config::uFromText(fConfig->getConfig(section, "LBID_Shift")));

    for (unsigned i = 0; i < numConnections; i++)
    {
        ostringstream oss;
        oss << "PMS" << (i + 1);
        string fServer (oss.str());

        boost::shared_ptr<MessageQueueClient>
        cl(new MessageQueueClient(fServer, fRm->getConfig()));
        boost::shared_ptr<boost::mutex> nl(new boost::mutex());

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
                throw runtime_error("Connection refused");
            }
        }
        catch (std::exception& ex)
        {
            if (i < newPmCount)
                newPmCount--;

            writeToLog(__FILE__, __LINE__, "Could not connect to " + fServer + ": " + ex.what(), LOG_TYPE_ERROR);
            cerr << "Could not connect to " << fServer << ": " << ex.what() << endl;
        }
        catch (...)
        {
            if (i < newPmCount)
                newPmCount--;

            writeToLog(__FILE__, __LINE__, "Could not connect to " + fServer, LOG_TYPE_ERROR);
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
            if (newClients[i]->isSameAddr(*fPmConnections[j]))
                break;
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
}

int DistributedEngineComm::Close()
{
    //cout << "DistributedEngineComm::Close() called" << endl;

    makeBusy(false);
    // for each MessageQueueClient in pmConnections delete the MessageQueueClient;
    fPmConnections.clear();
    fPmReader.clear();
    return 0;
}

void DistributedEngineComm::notifyClientsThatStreamEnds()
{
    // @bug 488 - error condition! push 0 length bs to messagequeuemap and
    // eventually let jobstep error out.
    SBS sbs;
    // Concurrent hash map iterators getters aren't thread safe so MCS takes a lock.
    sbs.reset(new ByteStream(0));
    MessageQueueMap::iterator mapIter = fSessionMessages.begin();

    for (; mapIter != fSessionMessages.end(); ++mapIter)
    {
        // use map accessor to overcome the limitation and clean the map using iterator
        mapIter->second->queue.clear();
        (void)atomicops::atomicInc(&mapIter->second->unackedWork[0]);
        mapIter->second->queue.push(sbs);
    }
}

void DistributedEngineComm::Listen(boost::shared_ptr<MessageQueueClient> client, uint32_t connIndex)
{
    SBS sbs;
    try
    {
        while (Busy())
        {
            Stats stats;
            //TODO: This call blocks so setting Busy() in another thread doesn't work here...
            sbs = client->read(0, NULL, &stats);

            if (sbs->length() != 0)
                addDataToOutput(sbs, connIndex, &stats);
            else // got zero bytes on read, nothing more will come
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
    notifyClientsThatStreamEnds();
    if (fIsExeMgr)
    {
        //std::cout << "WARNING: DEC READ 0 LENGTH BS FROM "
        //    << client->otherEnd()<< " OR GOT AN EXCEPTION READING" << std::endl;
        decltype(pmCount) originalPMCount = pmCount;
        // Re-establish if a remote PM restarted.
        std::this_thread::sleep_for(std::chrono::seconds(3));
        Setup();
        if (originalPMCount != pmCount)
        {
            ostringstream os;
            os << "DEC: lost connection to " << client->addr2String();
            writeToLog(__FILE__, __LINE__, os.str(), LOG_TYPE_ERROR);
        }

/*
        // reset the pmconnection vector
        ClientList tempConns;
        boost::mutex::scoped_lock onErrLock(fOnErrMutex);
        string moduleName = client->moduleName();
        //cout << "moduleName=" << moduleName << endl;
        for ( uint32_t i = 0; i < fPmConnections.size(); i++)
        {
            if (moduleName != fPmConnections[i]->moduleName())
                tempConns.push_back(fPmConnections[i]);
            //else
            //cout << "DEC remove PM" << fPmConnections[i]->otherEnd() << " moduleName=" << fPmConnections[i]->moduleName() << endl;
        }

        if (tempConns.size() == fPmConnections.size()) return;

        fPmConnections.swap(tempConns);
        pmCount = (pmCount == 0 ? 0 : pmCount - 1);
        //cout << "PMCOUNT=" << pmCount << endl;

        // log it
        ostringstream os;
        os << "DEC: lost connection to " << client->addr2String();
        writeToLog(__FILE__, __LINE__, os.str(), LOG_TYPE_CRITICAL);
*/
    }
    return;
}

boost::shared_ptr<MQE> DistributedEngineComm::addQueue(uint32_t key, bool sendACKs)
{
    boost::shared_ptr<MQE> mqe(new MQE(pmCount, key % fPmConnections.size()));
    mqe->sendACKs = sendACKs;
    mqe->throttled = false;

    bool result = fSessionMessages.insert({key, mqe}).second;
    if (!result)
    {
        ostringstream os;
        os << "DEC: attempt to add a queue with a duplicate ID " << key << endl;
        throw runtime_error(os.str());
    }
    return mqe;
}

void DistributedEngineComm::removeQueue(uint32_t key)
{
    shutdownQueue(key, true);
}

void DistributedEngineComm::shutdownQueue(uint32_t key, bool aErase)
{
    auto mapIter = fSessionMessages.find(key);
    if (mapIter == fSessionMessages.end())
        return;
    mapIter->second->queue.shutdown();
    mapIter->second->queue.clear();
    if (aErase)
    {
        std::unique_lock<std::mutex> lk(fMlock);
        fSessionMessages.unsafe_erase(mapIter);
    }
}

bool DistributedEngineComm::read_one(uint32_t key, messageqcpp::SBS& bs)
{
    const std::string methodName("read_one");
    boost::shared_ptr<MQE> mqe = getMqeForRead(key, methodName);

    auto queueSizeAndQueueIsEmptyFlag = mqe->queue.pop_one(&bs);

    if (queueSizeAndQueueIsEmptyFlag.second)
        return false;

    if (bs && mqe->sendACKs)
    {
        boost::mutex::scoped_lock lk(ackLock);

        if (mqe->throttled && !mqe->hasBigMsgs &&
            queueSizeAndQueueIsEmptyFlag.first.size <= disableThreshold)
        {
            setFlowControl(false, key, *mqe);
        }

        vector<SBS> v;
        v.push_back(bs);
        sendAcks(key, v, mqe, queueSizeAndQueueIsEmptyFlag.first.size);
    }

    if (!bs)
        bs.reset(new ByteStream());
    return true;
}

void DistributedEngineComm::read_all(uint32_t key, vector<SBS>& v)
{
    const std::string methodName("read_all");
    boost::shared_ptr<MQE> mqe = getMqeForRead(key, methodName);

    mqe->queue.pop_all(v);

    if (mqe->sendACKs)
    {
        boost::mutex::scoped_lock lk(ackLock);
        sendAcks(key, v, mqe, 0);
    }
}

size_t DistributedEngineComm::read_some(uint32_t key,
                                        uint32_t divisor,
                                        vector<SBS>& v,
                                        bool* flowControlOn,
                                        boost::shared_ptr<MQE> mqe,
                                        logging::StopWatch *timer)
{
    static const std::string methodName("read_some");
    //boost::shared_ptr<MQE> mqe = getMqeForRead(key, methodName);
    TSQSize_t queueSize = mqe->queue.pop_some(divisor, v, 1);   // need to play with the min #

    if (flowControlOn)
        *flowControlOn = false;

    if (mqe->sendACKs)
    {
        //boost::mutex::scoped_lock lk(ackLock);

        //timer->start("read_some_other");
        if (mqe->throttled && !mqe->hasBigMsgs && queueSize.size <= disableThreshold)
            setFlowControl(false, key, *mqe);
        //timer->stop("read_some_other");

        // mqe->throttled must be false so we'll have an early quit from sendAcks
        sendAcks(key, v, mqe, queueSize.size);

        //if (flowControlOn)
        //    *flowControlOn = mqe->>throttled;
    }
    return mqe->queue.queueSize();
}

void DistributedEngineComm::sendAcks(uint32_t uniqueID, const vector<SBS>& msgs,
                                     boost::shared_ptr<MQE> mqe, size_t queueSize)
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
        ByteStream msg(sizeof(ISMPacketHeader));
        uint16_t* toAck;
        vector<bool> pmAcked(pmCount, false);

        ism = (ISMPacketHeader*) msg.getInputPtr();
        // The only var checked by ReadThread is the Command var.  The others
        // are wasted space.  We hijack the Size, & Flags fields for the
        // params to the ACK msg.

        ism->Interleave = uniqueID;
        ism->Command = BATCH_PRIMITIVE_ACK;
        toAck = &ism->Size;

        msg.advanceInputPtr(sizeof(ISMPacketHeader));

        while (l_msgCount > 0)
        {
            /* could have to send up to pmCount ACKs */
            uint32_t sockIndex = 0;

            /* This will reset the ACK field in the Bytestream directly, and nothing
             * else needs to change if multiple msgs are sent. */
            nextPMToACK(mqe, l_msgCount, &sockIndex, toAck);
            idbassert(*toAck <= l_msgCount);
            l_msgCount -= *toAck;
            pmAcked[sockIndex] = true;
            writeToClient(sockIndex, msg);
        }

        // @bug4436, when no more unacked work, send an ack to all PMs that haven't been acked.
        // This is apply to the big message case only.  For small messages, the flow control is
        // disabled when the queue size is below the disableThreshold.
        if (mqe->hasBigMsgs)
        {
            uint64_t totalUnackedWork = 0;

            for (uint32_t i = 0; i < pmCount; i++)
                totalUnackedWork += mqe->unackedWork[i];

            if (totalUnackedWork == 0)
            {
                *toAck = 1;

                for (uint32_t i = 0; i < pmCount; i++)
                {
                    if (!pmAcked[i])
                        writeToClient(i, msg);
                }
            }
        }
    }
}

void DistributedEngineComm::nextPMToACK(boost::shared_ptr<MQE> mqe, uint32_t maxAck,
                                        uint32_t* sockIndex, uint16_t* numToAck)
{
    uint32_t i;
    uint32_t& nextIndex = mqe->ackSocketIndex;

    /* Other threads can be touching mqe->unackedWork at the same time, but because of
     * the locking env, mqe->unackedWork can only grow; whatever gets latched in this fcn
     * is a safe minimum at the point of use. */

    if (mqe->unackedWork[nextIndex] >= maxAck)
    {
        (void)atomicops::atomicSub(&mqe->unackedWork[nextIndex], maxAck);
        *sockIndex = nextIndex;
        //FIXME: we're going to truncate here from 32 to 16 bits. Hopefully this will always fit...
        *numToAck = maxAck;

        if (pmCount > 0)
            nextIndex = (nextIndex + 1) % pmCount;

        return;
    }
    else
    {
        for (i = 0; i < pmCount; i++)
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

        for (i = 0; i < pmCount; i++)
            cerr << mqe->unackedWork[i] << " ";

        cerr << " max: " << maxAck;
        cerr << endl;
        //make sure the returned vars are legitimate
        *sockIndex = nextIndex;
        *numToAck = maxAck / pmCount;

        if (pmCount > 0)
            nextIndex = (nextIndex + 1) % pmCount;

        return;
    }
}

void DistributedEngineComm::setFlowControl(bool enabled, uint32_t uniqueID, MQE& mqe)
{
    mqe.throttled = enabled;
    ByteStream msg(sizeof(ISMPacketHeader));
    ISMPacketHeader* ism = (ISMPacketHeader*) msg.getInputPtr();

    ism->Interleave = uniqueID;
    ism->Command = BATCH_PRIMITIVE_ACK;
    ism->Size = (enabled ? 0 : -1);

#ifdef VALGRIND
    /* XXXPAT: For testing in valgrind, init the vars that don't get used */
    ism->Flags = 0;
    ism->Type = 0;
    ism->MsgCount = 0;
    ism->Status = 0;
#endif

    msg.advanceInputPtr(sizeof(ISMPacketHeader));

    for (uint32_t i = 0; i < mqe.pmCount; i++)
        writeToClient(i, msg, mqe);
}

void DistributedEngineComm::write(uint32_t senderID, ByteStream& msg)
{
    ISMPacketHeader* ism = (ISMPacketHeader*) msg.buf();
    uint32_t dest;
    uint32_t numConn = fPmConnections.size();

    if (numConn > 0)
    {
        switch (ism->Command)
        {
            case BATCH_PRIMITIVE_CREATE:
                /* Disable flow control initially */
                msg << (uint32_t) - 1;
		    /* FALLTHRU */

            case BATCH_PRIMITIVE_DESTROY:
            case BATCH_PRIMITIVE_ADD_JOINER:
            case BATCH_PRIMITIVE_END_JOINER:
            case BATCH_PRIMITIVE_ABORT:
            case DICT_CREATE_EQUALITY_FILTER:
            case DICT_DESTROY_EQUALITY_FILTER:
                /* XXXPAT: This relies on the assumption that the first pmCount "PMS*"
                entries in the config file point to unique PMs */
                uint32_t i;

                for (i = 0; i < pmCount; i++)
                    writeToClient(i, msg, senderID);

                return;

            case BATCH_PRIMITIVE_RUN:
            case DICT_TOKEN_BY_SCAN_COMPARE:
                // for efficiency, writeToClient() grabs the interleaving factor for the caller,
                // and decides the final connection index because it already grabs the
                // caller's queue information
                dest = ism->Interleave;
                writeToClient(dest, msg, senderID, true);
                break;

            default:
                idbassert_s(0, "Unknown message type");
        }
    }
    else
    {
        writeToLog(__FILE__, __LINE__, "No PrimProcs are running", LOG_TYPE_DEBUG);
        throw IDBExcept(ERR_NO_PRIMPROC);
    }
}

void DistributedEngineComm::write(messageqcpp::ByteStream& msg, uint32_t connection)
{
    ISMPacketHeader* ism = (ISMPacketHeader*) msg.buf();
    PrimitiveHeader* pm = (PrimitiveHeader*) (ism + 1);
    uint32_t senderID = pm->UniqueID;
/*
    boost::mutex::scoped_lock lk(fMlock, boost::defer_lock_t());
    MessageQueueMap::iterator it;
    // This keeps mqe's stats from being freed until end of function
    boost::shared_ptr<MQE> mqe;
    Stats* senderStats = NULL;

    lk.lock();
    it = fSessionMessages.find(senderID);

    if (it != fSessionMessages.end())
    {
        mqe = it->second;
        senderStats = &(mqe->stats);
    }

    lk.unlock();
*/
    boost::shared_ptr<MQE> mqe;
    Stats* senderStats = nullptr;
    auto mapIter = fSessionMessages.find(senderID);
    if (mapIter != fSessionMessages.end())
    {
        mqe = mapIter->second;
        senderStats = &(mqe->stats);
    }

    newClients[connection]->write(msg, nullptr, senderStats);
}

void DistributedEngineComm::StartClientListener(boost::shared_ptr<MessageQueueClient> cl, uint32_t connIndex)
{
    boost::thread* thrd = new boost::thread(EngineCommRunner(this, cl, connIndex));
    fPmReader.push_back(thrd);
}

void DistributedEngineComm::addDataToOutput(SBS sbs, uint32_t connIndex, Stats* stats)
{
    ISMPacketHeader* hdr = (ISMPacketHeader*)(sbs->buf());
    PrimitiveHeader* p = (PrimitiveHeader*)(hdr + 1);
    uint32_t uniqueId = p->UniqueID;
    MessageQueueMap::iterator mapIter = fSessionMessages.find(uniqueId);

    if (mapIter == fSessionMessages.end())
    {
        //std::cerr << "DistributedEngineComm::AddDataToOutput: tried to add a message to a dead session: " << uniqueId << ", size " << sbs->length() << ", step id " << p->StepID << endl;
        return;
    }
    boost::shared_ptr<MQE> mqe = mapIter->second;
/*
    boost::shared_ptr<MQE> mqe;

    boost::mutex::scoped_lock lk(fMlock);
    MessageQueueMap::iterator map_tok = fSessionMessages.find(uniqueId);

    if (map_tok == fSessionMessages.end())
    {
        // For debugging...
        //cerr << "DistributedEngineComm::AddDataToOutput: tried to add a message to a dead session: " << uniqueId << ", size " << sbs->length() << ", step id " << p->StepID << endl;
        // WIP
        // This doesn't look right if this method just returns w/o raising an exception.
        return;
    }

    mqe = map_tok->second;
    lk.unlock();
*/
    if (pmCount > 0)
    {
        (void)atomicops::atomicInc(&mqe->unackedWork[connIndex % pmCount]);
    }

    TSQSize_t queueSize = mqe->queue.push(sbs);
    //std::cerr << "DistributedEngineComm::addDataToOutput " << uniqueId << " count " << queueSize.count << std::endl;

    if (mqe->sendACKs)
    {
        //boost::mutex::scoped_lock lk(ackLock);
        uint64_t msgSize = sbs->lengthWithHdrOverhead();

        if (!mqe->throttled && msgSize > (targetRecvQueueSize / 2))
            doHasBigMsgs(mqe, (300 * 1024 * 1024 > 3 * msgSize ?
                               300 * 1024 * 1024 : 3 * msgSize)); //buffer at least 3 big msgs

        if (!mqe->throttled && queueSize.size >= mqe->targetQueueSize)
            setFlowControl(true, uniqueId, *mqe);
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

int DistributedEngineComm::writeToClient(const size_t index,
                                         const ByteStream& bs,
                                         const uint32_t sender,
                                         const bool doInterleaving)
{
    if (sender != numeric_limits<uint32_t>::max())
    {
        MessageQueueMap::iterator mapIter = fSessionMessages.find(sender);
        if (mapIter != fSessionMessages.end())
        {
            return writeToClient(index, bs, *mapIter->second.get(), sender, doInterleaving);
        }
    }
    return 0;
}

int DistributedEngineComm::writeToClient(const size_t aIndex,
                                         const ByteStream& bs,
                                         MQE& mqe,
                                         const uint32_t sender,
                                         const bool doInterleaving)
{
    size_t index = aIndex;
    if (fPmConnections.size() == 0)
        return 0;

    // Keep mqe's stats from being freed early
    Stats* senderStats = &(mqe.stats);
    uint32_t interleaver = 0;

    if (doInterleaving)
    {
        interleaver = mqe.interleaver[index % mqe.pmCount]++;
    }

    try
    {
        if (doInterleaving)
            index = (index + (interleaver * pmCount)) % fPmConnections.size();

        //cerr << "DEC::writeToClient index " << index << endl;
        ClientList::value_type client = fPmConnections[index];

        if (!client->isAvailable()) return 0;

        boost::mutex::scoped_lock lk(*(fWlock[index]));
        client->write(bs, NULL, senderStats);
        return 0;
    }
    catch (...)
    {
        // @bug 488. error out under such condition instead of re-trying other connection,
        // by pushing 0 size bytestream to messagequeue and throw exception
        SBS sbs;
        //std::cout << "WARNING: DEC WRITE BROKEN PIPE. PMS index = " << index << std::endl;
        notifyClientsThatStreamEnds();
        // Here was an example of reconnecting DEC on failure.
        throw runtime_error("DistributedEngineComm::write: Broken Pipe error");
    }
}

uint32_t DistributedEngineComm::size(uint32_t key)
{
    static const std::string methodName("size");
    boost::shared_ptr<MQE> mqe = getMqeForRead(key, methodName);
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
    MessageQueueMap::iterator mapIter = fSessionMessages.find(uniqueID);
    if (mapIter != fSessionMessages.end())
    {
        return mapIter->second->stats;
    }

    return Stats();
}

MQE::MQE(uint32_t pCount, uint32_t initialInterleaverValue) : ackSocketIndex(0), pmCount(pCount), hasBigMsgs(false),
    targetQueueSize(targetRecvQueueSize)
{
    unackedWork.reset(new volatile uint32_t[pmCount]);
    interleaver.reset(new uint32_t[pmCount]);
    memset((void*) unackedWork.get(), 0, pmCount * sizeof(uint32_t));
    memset((void*) interleaver.get(), initialInterleaverValue, pmCount * sizeof(uint32_t));
}

}
// vim:ts=4 sw=4:

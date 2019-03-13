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

//  $Id: jobstep.h 9636 2013-06-20 14:23:36Z rdempsey $


/** @file */

#ifndef JOBLIST_JOBSTEP_H_
#define JOBLIST_JOBSTEP_H_

#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <sys/time.h>
#include <stdexcept>

#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/uuid/uuid.hpp>

#include "calpontsystemcatalog.h"
#include "calpontselectexecutionplan.h"
#include "elementtype.h"
#include "errorinfo.h"
#include "jl_logger.h"
#include "timestamp.h"
#include "rowgroup.h"
#include "querytele.h"
#include "threadpool.h"
#include "atomicops.h"

#include "branchpred.h"

#ifndef __GNUC__
#  ifndef __attribute__
#    define __attribute__(x)
#  endif
#endif

using namespace threadpool;

namespace joblist
{

/** @brief class JobStepAssociation mediator class to connect/control JobSteps and DataLists
 *
 * Class JobStepAssociation connects/controls JobSteps and DalaLists
 */
class JobStepAssociation
{
public:
    JobStepAssociation() { }
    virtual ~JobStepAssociation() {}

    void inAdd(const AnyDataListSPtr& spdl) __attribute__((deprecated))
    {
        fInDataList.push_back(spdl);
    }
    void outAdd(const AnyDataListSPtr& spdl)
    {
        fOutDataList.push_back(spdl);
    }
    void outAdd(const AnyDataListSPtr& spdl, size_t pos)
    {
        if (pos > fOutDataList.size()) throw std::logic_error("Insert position is beyond end.");

        fOutDataList.insert(fOutDataList.begin() + pos, spdl);
    }
    void outAdd(const DataListVec& spdlVec, size_t pos)
    {
        if (pos > fOutDataList.size()) throw std::logic_error("Insert position is beyond end.");

        fOutDataList.insert(fOutDataList.begin() + pos, spdlVec.begin(), spdlVec.end());
    }
    size_t inSize() const __attribute__((deprecated))
    {
        return fInDataList.size();
    }
    size_t outSize() const
    {
        return fOutDataList.size();
    }
    const AnyDataListSPtr& inAt(size_t i) const __attribute__((deprecated))
    {
        return fInDataList.at(i);
    }
    const AnyDataListSPtr& outAt(size_t i) const
    {
        return fOutDataList.at(i);
    }
    AnyDataListSPtr& outAt(size_t i)
    {
        return fOutDataList.at(i);
    }

private:
    DataListVec fInDataList;
    DataListVec fOutDataList;
};


// forward reference
struct JobInfo;


/** @brief class JobStep abstract class describing a query execution step
 *
 * Class JobStep is an abstract class that describes a query execution step
 */
class JobStep
{
public:

    /** constructor
     */
    JobStep() { }
    JobStep(const JobInfo&);
    /** destructor
     */
    virtual ~JobStep() { /*pthread_mutex_destroy(&mutex);*/ }
    /** @brief virtual void Run method
     */
    virtual void run() = 0;
    virtual void abort()
    {
        fDie = true;
    }
    /** @brief virtual void join method
     */
    virtual void join() = 0;
    /** @brief virtual string toString method
     */
    virtual const std::string toString() const = 0;

    /** @brief virtual JobStepAssociation * inputAssociation method
     */
    virtual void  inputAssociation(const JobStepAssociation& inputAssociation)
    {
        fInputJobStepAssociation = inputAssociation;
    }
    virtual const JobStepAssociation& inputAssociation() const
    {
        return fInputJobStepAssociation;
    }
    /** @brief virtual JobStepAssociation * outputAssociation method
     */
    virtual void  outputAssociation(const JobStepAssociation& outputAssociation)
    {
        fOutputJobStepAssociation = outputAssociation;
    }
    virtual const JobStepAssociation& outputAssociation() const
    {
        return fOutputJobStepAssociation;
    }

    virtual void stepId(uint16_t stepId)
    {
        fStepId = stepId;
    }
    virtual uint16_t stepId()      const
    {
        return fStepId;
    }
    virtual uint32_t sessionId()   const
    {
        return fSessionId;
    }
    virtual uint32_t txnId()       const
    {
        return fTxnId;
    }
    virtual uint32_t statementId() const
    {
        return fStatementId;
    }
    virtual void logger(const SPJL& logger)
    {
        fLogger = logger;
    }

    virtual bool isDictCol() const
    {
        return 0;
    }
    virtual execplan::CalpontSystemCatalog::OID oid() const
    {
        return 0;
    }
    virtual execplan::CalpontSystemCatalog::OID tableOid() const
    {
        return 0;
    }
    // @bug 598 Added alias for self-join
    virtual std::string alias() const
    {
        return fAlias;
    }
    virtual void  alias(const std::string& alias)
    {
        fAlias = alias;
    }
    // @bug 3401 & 3402, view support
    virtual std::string view() const
    {
        return fView;
    }
    virtual void  view(const std::string& vw)
    {
        fView = vw;
    }
    // @bug 3438, stats with column name
    virtual std::string name() const
    {
        return fName;
    }
    virtual void  name(const std::string& nm)
    {
        fName = nm;
    }
    virtual std::string schema() const
    {
        return fSchema;
    }
    virtual void  schema(const std::string& s)
    {
        fSchema = s;
    }
    // @bug 3398, add columns' unique tuple ID to job step
    virtual uint64_t tupleId() const
    {
        return fTupleId;
    }
    virtual void tupleId(uint64_t id)
    {
        fTupleId = id;
    }

    //...Final I/O blk count, msg rcv count, etc for this job step. These
    //...methods do not use a mutex lock to acquire values, because it is
    //...assumed they are called after all processing is complete.
    virtual uint64_t phyIOCount    ( ) const
    {
        return 0;
    }
    virtual uint64_t cacheIOCount  ( ) const
    {
        return 0;
    }
    virtual uint64_t msgsRcvdCount ( ) const
    {
        return 0;
    }
    virtual uint64_t msgBytesIn    ( ) const
    {
        return 0;
    }
    virtual uint64_t msgBytesOut   ( ) const
    {
        return 0;
    }
    virtual uint64_t blockTouched  ( ) const
    {
        return 0;
    }
    virtual uint64_t cardinality   ( ) const
    {
        return fCardinality;
    }
    virtual void cardinality ( const uint64_t cardinality )
    {
        fCardinality = cardinality;
    }

    // functions to delay/control jobstep execution; decWaitToRunStepCnt() per-
    // forms atomic decrement op because it is accessed by multiple threads.
    bool     delayedRun() const
    {
        return fDelayedRunFlag;
    }
    int      waitToRunStepCnt()
    {
        return fWaitToRunStepCnt;
    }
    void     incWaitToRunStepCnt()
    {
        fDelayedRunFlag = true;
        ++fWaitToRunStepCnt;
    }
    int      decWaitToRunStepCnt()
    {
        return atomicops::atomicDec(&fWaitToRunStepCnt);
    }
    void resetDelayedRun()
    {
        fDelayedRunFlag = false;
        fWaitToRunStepCnt = 0;
    }

    void logEnd(const char* s)
    {
        fLogMutex.lock(); //pthread_mutex_lock(&mutex);
        std::cout << s << std::endl;
        fLogMutex.unlock(); //pthread_mutex_unlock(&mutex);
    }
    void syslogStartStep(uint32_t subSystem,
                         const std::string& stepName) const;
    void syslogEndStep  (uint32_t subSystem,
                         uint64_t blockedDLInput,
                         uint64_t blockedDLOutput,
                         uint64_t msgBytesInput = 0,
                         uint64_t msgBytesOutput = 0 )   const;
    void syslogReadBlockCounts (uint32_t subSystem,
                                uint64_t physicalReadCount,
                                uint64_t cacheReadCount,
                                uint64_t casualPartBlocks )   const;
    void syslogProcessingTimes (uint32_t subSystem,
                                const struct timeval&   firstReadTime,
                                const struct timeval&   lastReadTime,
                                const struct timeval&   firstWriteTime,
                                const struct timeval&   lastWriteTime) const;
    void setTrace(bool trace) __attribute__((deprecated));
    bool traceOn() const;
    void setTraceFlags(uint32_t flags)
    {
        fTraceFlags = flags;
    }
    JSTimeStamp dlTimes;

    const std::string& extendedInfo() const
    {
        return fExtendedInfo;
    }
    const std::string& miniInfo() const
    {
        return fMiniInfo;
    }

    uint32_t priority()
    {
        return fPriority;
    }
    void priority(uint32_t p)
    {
        fPriority = p;
    }

    uint32_t status() const
    {
        return fErrorInfo->errCode;
    }
    void  status(uint32_t s)
    {
        fErrorInfo->errCode = s;
    }
    std::string errorMessage()
    {
        return fErrorInfo->errMsg;
    }
    void errorMessage(const std::string& s)
    {
        fErrorInfo->errMsg = s;
    }
    const SErrorInfo& errorInfo() const
    {
        return fErrorInfo;
    }
    SErrorInfo& errorInfo()
    {
        return fErrorInfo;
    }
    void errorInfo(SErrorInfo& sp)
    {
        fErrorInfo = sp;
    }

    bool cancelled()
    {
        return (fErrorInfo->errCode > 0 || fDie);
    }

    virtual bool stringTableFriendly()
    {
        return false;
    }

    bool delivery() const
    {
        return fDelivery;
    }
    void delivery(bool b)
    {
        fDelivery = b;
    }

    const boost::uuids::uuid& queryUuid() const
    {
        return fQueryUuid;
    }

    //@bug5887, distinguish on clause filter and where clause filter
    bool onClauseFilter() const
    {
        return fOnClauseFilter;
    }
    void onClauseFilter(bool b)
    {
        fOnClauseFilter = b;
    }

    static ThreadPool jobstepThreadPool;
protected:

    //@bug6088, for telemetry posting
    static const int64_t STEP_TELE_INTERVAL = 5000;  // now, this is the browser refresh rate
    void postStepStartTele(querytele::StepTeleStats& sts)
    {
        fStartTime = fLastStepTeleTime = sts.start_time = querytele::QueryTeleClient::timeNowms();
        fQtc.postStepTele(sts);
    }
    void postStepProgressTele(querytele::StepTeleStats& sts)
    {
        int64_t crntTime = querytele::QueryTeleClient::timeNowms();

        if ((crntTime - fLastStepTeleTime) >= STEP_TELE_INTERVAL)
        {
            // interval between step telemetry
            sts.start_time = fStartTime;
            fQtc.postStepTele(sts);
            fLastStepTeleTime = crntTime;
        }
    }
    void postStepSummaryTele(querytele::StepTeleStats& sts)
    {
        sts.start_time = fStartTime;
        sts.end_time = fLastStepTeleTime = querytele::QueryTeleClient::timeNowms();
        fQtc.postStepTele(sts);
    }

    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;

    uint32_t fSessionId;
    uint32_t fTxnId;
    BRM::QueryContext fVerId;
    uint32_t fStatementId;

    uint32_t fStepId;
    uint64_t fTupleId;

    std::string fAlias;
    std::string fView;
    std::string fName;
    std::string fSchema;
    uint32_t    fTraceFlags;
    uint64_t    fCardinality;
    bool        fDelayedRunFlag;
    bool        fDelivery;
    bool        fOnClauseFilter;
    volatile bool fDie;
    volatile uint32_t fWaitToRunStepCnt;
    std::string fExtendedInfo;
    std::string fMiniInfo;

    uint32_t fPriority;

    SErrorInfo fErrorInfo;
    SPJL fLogger;

    uint32_t fLocalQuery;

    boost::uuids::uuid fQueryUuid;
    boost::uuids::uuid fStepUuid;
    querytele::QueryTeleClient fQtc;
    uint64_t fProgress;
    int64_t  fStartTime;
    int64_t  fLastStepTeleTime;

private:
    static boost::mutex fLogMutex;

    friend class CommandJL;
};


class TupleJobStep
{
public:
    TupleJobStep() { }
    virtual ~TupleJobStep() { }
    virtual void  setOutputRowGroup(const rowgroup::RowGroup&) = 0;
    virtual void  setFcnExpGroup3(const std::vector<execplan::SRCP>&) {}
    virtual void  setFE23Output(const rowgroup::RowGroup&) {}
    virtual const rowgroup::RowGroup& getOutputRowGroup() const = 0;
};


class TupleDeliveryStep : public TupleJobStep
{
public:
    virtual ~TupleDeliveryStep() { }
    virtual uint32_t  nextBand(messageqcpp::ByteStream& bs) = 0;
    virtual const rowgroup::RowGroup& getDeliveredRowGroup() const = 0;
    virtual void  deliverStringTableRowGroup(bool b) = 0;
    virtual bool  deliverStringTableRowGroup() const = 0;
};

class NullStep : public JobStep
{
public:
    /** @brief virtual void Run method
     */
    virtual void run() {}
    /** @brief virtual void join method
     */
    virtual void join() {}
    /** @brief virtual string toString method
     */
    virtual const std::string toString() const
    {
        return "NullStep";
    }
};

// calls rhs->toString()
std::ostream& operator<<(std::ostream& os, const JobStep* rhs);

typedef boost::shared_ptr<JobStepAssociation> SJSA;
typedef boost::shared_ptr<JobStepAssociation> JobStepAssociationSPtr;

typedef boost::shared_ptr<JobStep> SJSTEP;

}

#endif  // JOBLIST_JOBSTEP_H_
// vim:ts=4 sw=4:


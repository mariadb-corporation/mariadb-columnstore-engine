/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016 MariaDB Corporation

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

//  $Id: joblist.cpp 9655 2013-06-25 23:08:13Z xlou $

// Cross engine needs to be at the top due to MySQL includes
#include "crossenginestep.h"
#include "errorcodes.h"
#include <iterator>
#include <stdexcept>
//#define NDEBUG
#include <cassert>
using namespace std;

#include "joblist.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "errorids.h"
#include "jobstep.h"
#include "primitivestep.h"
#include "subquerystep.h"
#include "tupleaggregatestep.h"
#include "tupleannexstep.h"
#include "tuplehashjoin.h"
#include "tupleunion.h"
#include "tupleaggregatestep.h"
#include "windowfunctionstep.h"
#include "configcpp.h"
#include "oamcache.h"

#include "atomicops.h"

namespace joblist
{
int  JobList::fPmsConfigured = 0;

struct JSJoiner
{
    JSJoiner(JobStep* j): js(j) { }
    void operator()()
    {
        js->abort();
        js->join();
    }
    JobStep* js;
};

JobList::JobList(bool isEM) :
    fIsRunning(false),
    fIsExeMgr(isEM),
    fPmsConnected(0),
    projectingTableOID(0),
    fAborted(0),
    fPriority(50)
{
}

JobList::~JobList()
{
    try
    {
        if (fIsRunning)
        {
#if 0
            // This logic creates a set of threads to wind down the query
            vector<uint64_t> joiners;
            joiners.reserve(20);
            NullStep nullStep;  // For access to the static jobstepThreadPool.

            JobStepVector::iterator iter;
            JobStepVector::iterator end;

            iter = fQuery.begin();
            end = fQuery.end();

            // Wait for all the query steps to finish
            while (iter != end)
            {
                joiners.push_back(nullStep.jobstepThreadPool.invoke(JSJoiner(iter->get())));
                ++iter;
            }

            iter = fProject.begin();
            end = fProject.end();

            // wait for the projection steps
            while (iter != end)
            {
                joiners.push_back(nullStep.jobstepThreadPool.invoke(JSJoiner(iter->get())));
                ++iter;
            }

            nullStep.jobstepThreadPool.join(joiners);
#endif
            // This logic stops the query steps one at a time
            JobStepVector::iterator iter;
            JobStepVector::iterator end;
            end = fQuery.end();

            for (iter = fQuery.begin(); iter != end; ++iter)
            {
                (*iter)->abort();
            }

            // Stop all the projection steps
            end = fProject.end();

            for (iter = fProject.begin(); iter != end; ++iter)
            {
                (*iter)->abort();
            }

            // Wait for all the query steps to end
            end = fQuery.end();

            for (iter = fQuery.begin(); iter != end; ++iter)
            {
                (*iter)->join();
            }

            // Wait for all the projection steps to end
            end = fProject.end();

            for (iter = fProject.begin(); iter != end; ++iter)
            {
                (*iter)->join();
            }
        }
    }
    catch (exception& ex)
    {
        cerr << "JobList::~JobList: exception caught: " << ex.what() << endl;
    }
    catch (...)
    {
        cerr << "JobList::~JobList: exception caught!" << endl;
    }
}

int JobList::doQuery()
{
    // Don't start the steps if there is no PrimProc connection.
    if (fPmsConfigured < 1 || fPmsConnected < fPmsConfigured)
        return 0;

    JobStep* js;

    // Set the priority on the jobsteps
    for (uint32_t i = 0; i < fQuery.size(); i++)
        fQuery[i]->priority(fPriority);

    for (uint32_t i = 0; i < fProject.size(); i++)
        fProject[i]->priority(fPriority);

    // I put this logging in a separate loop rather than including it in the
    // other loop that calls run(), to insure that these logging msgs would
    // not be interleaved with any logging coming from the calls to run().
    JobStepVector::iterator iter2 = fQuery.begin();
    JobStepVector::iterator end2  = fQuery.end();
    int rc = -1;

    while (iter2 != end2)
    {
        js = iter2->get();

        if (js->traceOn())
        {
            if (js->delayedRun())
            {
                std::ostringstream oss;
                oss << "Session: " << js->sessionId() <<
                    "; delaying start of query step " << js->stepId() <<
                    "; waitStepCount-" << js->waitToRunStepCnt() << std::endl;
                std::cout << oss.str();
            }
        }

        ++iter2;
    }

    iter2 = fProject.begin();
    end2  = fProject.end();

    while (iter2 != end2)
    {
        js = iter2->get();

        if (js->traceOn())
        {
            if (js->delayedRun())
            {
                std::ostringstream oss;
                oss << "Session: " << js->sessionId() <<
                    "; delaying start of project step " << js->stepId() <<
                    "; waitStepCount-" << js->waitToRunStepCnt() << std::endl;
                std::cout << oss.str();
            }
        }

        ++iter2;
    }

    JobStepVector::iterator iter = fQuery.begin();
    JobStepVector::iterator end  = fQuery.end();

    // Start the query running
    while (iter != end)
    {
        js = iter->get();

        if (!js->delayedRun())
        {
            js->run();
        }

        ++iter;
    }

    iter = fProject.begin();
    end = fProject.end();

    // Fire up the projection steps
    while (iter != end)
    {
        if (!iter->get()->delayedRun())
        {
            iter->get()->run();
        }

        ++iter;
    }

    fIsRunning = true;
    rc = 0;
    return rc;
}

int JobList::putEngineComm(DistributedEngineComm* dec)
{
    int retryCnt = 0;

    if (fPmsConfigured == 0)
    {
        logging::LoggingID lid(05);
        logging::MessageLog ml(lid);
        logging::Message::Args args;
        logging::Message m(0);
        // We failed to get a connection
        args.add("There are no PMs configured. Can't perform Query");
        args.add(retryCnt);
        m.format(args);
        ml.logDebugMessage(m);

        if (!errInfo)
            errInfo.reset(new ErrorInfo);

        errInfo->errCode = logging::ERR_NO_PRIMPROC;
        errInfo->errMsg  = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_NO_PRIMPROC);
        return errInfo->errCode;
    }

    // Check to be sure all PrimProcs are attached.
    fPmsConnected = dec->connectedPmServers();

    while (fPmsConnected < fPmsConfigured)
    {
        sleep(1);
        fPmsConnected = dec->connectedPmServers();

        // Give up after 20 seconds. Primproc isn't coming back
        if (retryCnt >= 20)
        {
            break;
        }

        ++retryCnt;
        oam::OamCache* oamCache = oam::OamCache::makeOamCache();
        oamCache->forceReload();
        dec->Setup();
    }

    if (retryCnt > 0)
    {
        logging::LoggingID lid(05);
        logging::MessageLog ml(lid);
        logging::Message::Args args;
        logging::Message m(0);

        if (fPmsConnected < fPmsConfigured)
        {
            // We failed to get a connection
            args.add("Failed to get all PrimProc connections. Retry count");
            args.add(retryCnt);
            m.format(args);
            ml.logDebugMessage(m);

            if (!errInfo)
                errInfo.reset(new ErrorInfo);

            errInfo->errCode = logging::ERR_NO_PRIMPROC;
            errInfo->errMsg  = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_NO_PRIMPROC);
            return errInfo->errCode;
        }
        else
        {
            // Log that we had to reconnect
            args.add("PrimProc reconnected. Retry count");
            args.add(retryCnt);
            m.format(args);
            ml.logDebugMessage(m);
        }
    }

    JobStepVector::iterator iter = fQuery.begin();
    JobStepVector::iterator end = fQuery.end();

    while (iter != end)
    {
        SJSTEP sjstep = *iter;
        JobStep* jsp = sjstep.get();

        if (typeid(*jsp) == typeid(pDictionaryScan))
        {
            pDictionaryScan* step = dynamic_cast<pDictionaryScan*>(jsp);
            step->dec(dec);
        }
        else if (typeid(*jsp) == typeid(TupleBPS))
        {
            BatchPrimitive* step = dynamic_cast<BatchPrimitive*>(jsp);
            step->setBppStep();
            step->dec(dec);
        }

        ++iter;
    }

    iter = fProject.begin();
    end = fProject.end();

    while (iter != end)
    {
        SJSTEP sjstep = *iter;
        JobStep* jsp = sjstep.get();

        if (typeid(*jsp) == typeid(TupleBPS))
        {
            BatchPrimitive* step = dynamic_cast<BatchPrimitive*>(jsp);
            step->setBppStep();
            step->dec(dec);
        }

        ++iter;
    }

    return 0;
}

// -- TupleJobList
/* returns row count */
uint32_t TupleJobList::projectTable(CalpontSystemCatalog::OID, messageqcpp::ByteStream& bs)
{
    uint32_t ret = ds->nextBand(bs);
    moreData = (ret != 0);
    return ret;
}

const rowgroup::RowGroup& TupleJobList::getOutputRowGroup() const
{
    if (fDeliveredTables.empty())
        throw runtime_error("Empty delivery!");

    TupleDeliveryStep* tds =
        dynamic_cast<TupleDeliveryStep*>(fDeliveredTables.begin()->second.get());

    if (tds == NULL)
        throw runtime_error("Not a TupleDeliveryStep!!");

    return tds->getDeliveredRowGroup();
}

void TupleJobList::setDeliveryFlag(bool f)
{
    DeliveredTableMap::iterator iter = fDeliveredTables.begin();
    SJSTEP dstep = iter->second;
    ds = dynamic_cast<TupleDeliveryStep*>(dstep.get());

    if (ds) // if not dummy step
        dstep->delivery(f);
}

//------------------------------------------------------------------------------
// Retrieve, accumulate, and return a summary of stat totals for this joblist.
// Stat totals are determined by adding up the individual counts from each step.
// It is currently intended that this method only be called after the completion
// of the query, because no mutex locking is being employed when we access the
// data attributes from the jobsteps and datalists.
//------------------------------------------------------------------------------
void JobList::querySummary(bool extendedStats)
{
    fMiniInfo += "\n";

    try
    {
        // bug3438, print subquery info prior to outer query
        for (vector<SJLP>::iterator i = subqueryJoblists.begin(); i != subqueryJoblists.end(); i++)
        {
            i->get()->querySummary(extendedStats);
            fStats += i->get()->queryStats();
            fExtendedInfo += i->get()->extendedInfo();
            fMiniInfo += i->get()->miniInfo();
        }

        JobStepVector::const_iterator qIter = fQuery.begin();
        JobStepVector::const_iterator qEnd  = fQuery.end();
        JobStep* js;

        //
        //...Add up the I/O and msg counts for the query job steps
        //
        while ( qIter != qEnd )
        {
            js = qIter->get();

            fStats.fPhyIO     += js->phyIOCount();
            fStats.fCacheIO   += js->cacheIOCount();

            if (typeid(*js) == typeid(TupleBPS))
            {
                fStats.fMsgRcvCnt += js->blockTouched();
            }
            else
            {
                fStats.fMsgRcvCnt += js->msgsRcvdCount();
            }

            fStats.fMsgBytesIn += js->msgBytesIn();
            fStats.fMsgBytesOut += js->msgBytesOut();

            //...As long as we only have 2 job steps that involve casual
            //...partioning, we just define blkSkipped() in those 2 classes,
            //...and use typeid to find/invoke those methods.  If we start
            //...adding blkSkipped() to more classes, we should probably make
            //...blkSkipped() a pure virtual method of the base JobStep class
            //...so that we don't have to do this type checking and casting.
            uint64_t skipCnt = 0;

            if (typeid(*js) == typeid(pColStep))
                skipCnt = (dynamic_cast<pColStep*>(js))->blksSkipped ();
            else if (typeid(*js) == typeid(pColScanStep))
                skipCnt = (dynamic_cast<pColScanStep*>(js))->blksSkipped ();
            else if (typeid(*js) == typeid(TupleBPS))
                skipCnt = (dynamic_cast<BatchPrimitive*>(js))->blksSkipped ();

            fStats.fCPBlocksSkipped += skipCnt;

            if (extendedStats)
            {
                string ei;
                int max = 0;
                ei = js->extendedInfo();

                while (max < 4 && ei.size() <= 10)
                {
                    ei = js->extendedInfo();
                    max++;
                }

                fExtendedInfo += ei;
                fMiniInfo += js->miniInfo() + "\n";
            }

            ++qIter;
        }

        JobStepVector::const_iterator pIter = fProject.begin();
        JobStepVector::const_iterator pEnd  = fProject.end();

        //
        //...Add up the I/O and msg counts for the projection job steps
        //
        while ( pIter != pEnd )
        {
            js = pIter->get();

            fStats.fPhyIO   += js->phyIOCount();
            fStats.fCacheIO += js->cacheIOCount();

            if (typeid(*js) == typeid(TupleBPS))
            {
                fStats.fMsgRcvCnt += js->blockTouched();
            }
            else
            {
                fStats.fMsgRcvCnt += js->msgsRcvdCount();
            }

            fStats.fMsgBytesIn += js->msgBytesIn();
            fStats.fMsgBytesOut += js->msgBytesOut();

            uint64_t skipCnt = 0;

            if (typeid(*js) == typeid(pColStep))
                skipCnt = (dynamic_cast<pColStep*>(js))->blksSkipped ();
            else if (typeid(*js) == typeid(pColScanStep))
                skipCnt = (dynamic_cast<pColScanStep*>(js))->blksSkipped ();
            else if (typeid(*js) == typeid(TupleBPS))
                skipCnt = (dynamic_cast<BatchPrimitive*>(js))->blksSkipped ();

            fStats.fCPBlocksSkipped += skipCnt;
            ++pIter;
        }

        if ((!fProject.empty()) && extendedStats)
        {
            DeliveredTableMap::iterator dsi = fDeliveredTables.begin();

            while (dsi != fDeliveredTables.end())
            {
                js = dynamic_cast<JobStep*>(dsi->second.get());
                string ei;
                int max = 0;
                ei = js->extendedInfo();

                while (max < 4 && ei.size() <= 10)
                {
                    ei = js->extendedInfo();
                    max++;
                }

                fExtendedInfo += ei;
                fMiniInfo += js->miniInfo() + "\n";

                ++dsi;
            }
        }
    }
    catch (exception& ex)
    {
        cerr << "JobList::querySummary: exception caught: " << ex.what() << endl;
        return;
    }
    catch (...)
    {
        cerr << "JobList::querySummary: exception caught!" << endl;
        return;
    }

    return;
}

// @bug 828. Added additional information to the graph at the end of execution
void JobList::graph(uint32_t sessionID)
{
    // Graphic view draw
    ostringstream oss;
    struct timeval tvbuf;
    gettimeofday(&tvbuf, 0);
    struct tm tmbuf;
    localtime_r(reinterpret_cast<time_t*>(&tvbuf.tv_sec), &tmbuf);
    oss << "jobstep_results." << setfill('0')
        << setw(4) << (tmbuf.tm_year + 1900)
        << setw(2) << (tmbuf.tm_mon + 1)
        << setw(2) << (tmbuf.tm_mday)
        << setw(2) << (tmbuf.tm_hour)
        << setw(2) << (tmbuf.tm_min)
        << setw(2) << (tmbuf.tm_sec)
        << setw(6) << (tvbuf.tv_usec)
        << ".dot";
    string jsrname(oss.str());
    //it's too late to set this here. ExeMgr has already returned ei to dm...
    //fExtendedInfo += "Graphs are in " + jsrname;
    std::ofstream dotFile(jsrname.c_str(), std::ios::out);
    dotFile << "digraph G {" << std::endl;
    JobStepVector::iterator qsi;
    JobStepVector::iterator psi;
    DeliveredTableMap::iterator dsi;
    boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
    CalpontSystemCatalog::TableColName tcn;
    uint64_t outSize = 0;
    uint64_t msgs = 0;
    uint64_t pio = 0;
    int ctn = 0;
    bool diskIo = false;
    uint64_t saveTime = 0;
    uint64_t loadTime = 0;

    // merge in the subquery steps
    JobStepVector querySteps = fQuery;
    {
        SubQueryStep* subquery = NULL;
        qsi = querySteps.begin();

        while (qsi != querySteps.end())
        {
            if ((subquery = dynamic_cast<SubQueryStep*>(qsi->get())) != NULL)
            {
                querySteps.erase(qsi);
                JobStepVector subSteps = subquery->subJoblist()->querySteps();
                querySteps.insert(querySteps.end(), subSteps.begin(), subSteps.end());
                qsi = querySteps.begin();
            }
            else
            {
                qsi++;
            }
        }
    }

    for (qsi = querySteps.begin(); qsi != querySteps.end(); ctn++, qsi++)
    {
        //HashJoinStep* hjs = 0;

        //if (dynamic_cast<OrDelimiter*>(qsi->get()) != NULL)
        //	continue;

        // @bug 1042. clear column name first at each loop
        tcn.column = "";

        uint16_t stepidIn = qsi->get()->stepId();
        dotFile << stepidIn << " [label=\"st_" << stepidIn << " ";

        // @Bug 1033.  colName was being called for dictionary steps that don't have column names.
        //             Added if condition below.
        if ( typeid(*(qsi->get())) == typeid(pColScanStep) ||
                typeid(*(qsi->get())) == typeid(pColStep))
            tcn = csc->colName(qsi->get()->oid());

        dotFile << "(";

        if (!tcn.column.empty())
            dotFile << tcn.column << "/";

        if (typeid(*(qsi->get())) == typeid(TupleBPS))
        {
            BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(qsi->get());
            OIDVector projectOids = bps->getProjectOids();

            if ( projectOids.size() > 0 )
            {
                dotFile << "\\l";
                dotFile << "PC:";
                dotFile << "\\l";

                for ( unsigned int i = 0; i < projectOids.size(); i++ )
                {
                    tcn = csc->colName(projectOids[i]);

                    if (!tcn.column.empty())
                        dotFile << tcn.column << "   ";

                    if ( (i + 1) % 3 == 0 )
                        dotFile << "\\l";
                }
            }
            else
            {
                tcn = csc->colName(qsi->get()->oid());
                dotFile << tcn.column << "/";
            }
        }
        else if (typeid(*(qsi->get())) == typeid(CrossEngineStep))
        {
            tcn.schema = qsi->get()->schema();
            tcn.table = qsi->get()->alias();
        }

        dotFile << JSTimeStamp::tsdiffstr(qsi->get()->dlTimes.EndOfInputTime(),
                                          qsi->get()->dlTimes.FirstReadTime()) << "s";

        dotFile << ")";

        // oracle predict card
        dotFile << "\\l#: " << (*qsi)->cardinality();

        if (typeid(*(qsi->get())) == typeid(pColStep))
        {
            dotFile << "\"" << " shape=ellipse";
        }
        else if (typeid(*(qsi->get())) == typeid(pColScanStep))
        {
            dotFile << "\"" << " shape=box";
        }
        else if (typeid(*(qsi->get())) == typeid(TupleBPS))
        {
            BatchPrimitive* bps =
                dynamic_cast<BatchPrimitive*>(qsi->get());

            // if BPS not run, a BucketReuseStep was substituted, so draw dashed
            if (bps->wasStepRun())
            {
                dotFile << "\"" << " shape=box style=bold";

                if (typeid(*(qsi->get())) == typeid(TupleBPS))
                    dotFile << " peripheries=2";
            }
            else
                dotFile << "\"" << " shape=box style=dashed";
        }
        else if (typeid(*(qsi->get())) == typeid(CrossEngineStep))
        {
            dotFile << "\"" << " shape=box style=dashed";
        }
        else if (typeid(*(qsi->get())) == typeid(TupleAggregateStep))
        {
            dotFile << "\"" << " shape=triangle orientation=180";
        }
        else if (typeid(*(qsi->get())) == typeid(TupleAnnexStep))
        {
            dotFile << "\"" << " shape=star";
        }
        else if (typeid(*(qsi->get())) == typeid(WindowFunctionStep))
        {
            dotFile << "\"" << " shape=triangle orientation=180 peripheries=2";
        }
        else if (typeid(*(qsi->get())) == typeid(TupleHashJoinStep))
        {
            dotFile << "\"";
            dotFile << " shape=diamond style=dashed peripheries=2";
        }
        else if (typeid(*(qsi->get())) == typeid(TupleUnion) )
        {
            dotFile << "\"" << " shape=triangle";
        }
        else if (typeid(*(qsi->get())) == typeid(pDictionaryStep))
        {
            dotFile << "\"" << " shape=trapezium";
        }
        else if (typeid(*(qsi->get())) == typeid(FilterStep))
        {
            dotFile << "\"" << " shape=house orientation=180";
        }
        else if (typeid(*(qsi->get())) == typeid(TupleBPS))
        {
            dotFile << "\"" << " shape=box style=bold";
            dotFile << " peripheries=2";
        }
        else if (typeid(*(qsi->get())) == typeid(CrossEngineStep))
        {
            dotFile << "\"" << " shape=box style=bold";
            dotFile << " peripheries=2";
        }
        else
            dotFile << "\"";

        dotFile << "]" << endl;

        // msgsRecived, physicalIO, cacheIO
        msgs = qsi->get()->msgsRcvdCount();
        pio = qsi->get()->phyIOCount();

        for (unsigned int i = 0; i < qsi->get()->outputAssociation().outSize(); i++)
        {
            ptrdiff_t dloutptr = 0;
            DataList_t* dlout;
            StrDataList* sdl;
//			TupleDataList* tdl;

            if ((dlout = qsi->get()->outputAssociation().outAt(i)->dataList()))
            {
                dloutptr = (ptrdiff_t)dlout;
                outSize = dlout->totalSize();
                diskIo = dlout->totalDiskIoTime(saveTime, loadTime);
            }
            else if ((sdl = qsi->get()->outputAssociation().outAt(i)->stringDataList()))
            {
                dloutptr = (ptrdiff_t)sdl;
                outSize = sdl->totalSize();
                diskIo = sdl->totalDiskIoTime(saveTime, loadTime);
            }

            // if HashJoinStep, determine if output fifo was cached to disk
            bool hjTempDiskFlag = false;

            for (unsigned int k = 0; k < querySteps.size(); k++)
            {
                uint16_t stepidOut = querySteps[k].get()->stepId();
                JobStepAssociation queryInputSA = querySteps[k].get()->inputAssociation();

                for (unsigned int j = 0; j < queryInputSA.outSize(); j++)
                {
                    ptrdiff_t dlinptr = 0;
                    DataList_t* dlin = queryInputSA.outAt(j)->dataList();
                    StrDataList* sdl = 0;

                    if (dlin)
                        dlinptr = (ptrdiff_t)dlin;
                    else if ((sdl = queryInputSA.outAt(j)->stringDataList()))
                    {
                        dlinptr = (ptrdiff_t)sdl;
                    }

                    if (dloutptr == dlinptr)
                    {
                        dotFile << stepidIn << " -> " << stepidOut;

                        dotFile << " [label=\" r: " << outSize;

                        if (hjTempDiskFlag)
                        {
                            dotFile << "*";
                        }

                        dotFile << "\\l";

                        if (msgs != 0)
                        {
                            dotFile << " m: " << msgs << "\\l";

                            if (typeid(*(qsi->get())) == typeid(TupleBPS))
                            {
                                dotFile << " b: " << qsi->get()->blockTouched() << "\\l";
                            }

                            dotFile << " p: " << pio << "\\l";
                        }

                        if (diskIo == true)
                        {
                            dotFile << " wr: " << saveTime << "s\\l";
                            dotFile << " rd: " << loadTime << "s\\l";
                        }

                        dotFile << "\"]" << endl;
                    }
                }
            }

            for (psi = fProject.begin(); psi < fProject.end(); psi++)
            {
                uint16_t stepidOut = psi->get()->stepId();
                JobStepAssociation projectInputSA = psi->get()->inputAssociation();

                for (unsigned int j = 0; j < projectInputSA.outSize(); j++)
                {
                    ptrdiff_t dlinptr;
                    DataList_t* dlin = projectInputSA.outAt(j)->dataList();
                    StrDataList* sdl = 0;

                    if (dlin)
                    {
                        dlinptr = (ptrdiff_t)dlin;
                    }
                    else
                    {
                        sdl = projectInputSA.outAt(j)->stringDataList();
                        dlinptr = (ptrdiff_t)sdl;
                    }

                    if (dloutptr == dlinptr)
                    {
                        dotFile << stepidIn << " -> " << stepidOut;

                        dotFile << " [label=\" r: " << outSize;

                        if (hjTempDiskFlag)
                        {
                            dotFile << "*";
                        }

                        dotFile << "\\l";

                        if (msgs != 0)
                        {
                            dotFile << " m: " << msgs << "\\l";
                            dotFile << " p: " << pio << "\\l";
                        }

                        if (diskIo == true)
                        {
                            dotFile << " wr: " << saveTime << "s\\l";
                            dotFile << " rd: " << loadTime << "s\\l";
                        }

                        dotFile << "\"]" << endl;
                    }
                }
            }
        }

        //@Bug 921
        if (typeid(*(qsi->get())) == typeid(TupleBPS))
        {
            BatchPrimitive* bps = dynamic_cast<BatchPrimitive*>(qsi->get());
            CalpontSystemCatalog::OID tableOIDProject = bps->tableOid();

            if (bps->getOutputType() == TABLE_BAND || bps->getOutputType() == ROW_GROUP)
            {
                outSize  = bps->getRows();

                for (dsi = fDeliveredTables.begin(); dsi != fDeliveredTables.end(); dsi++)
                {
                    BatchPrimitive* bpsDelivery = dynamic_cast<BatchPrimitive*>((dsi->second).get());
                    TupleHashJoinStep* thjDelivery = dynamic_cast<TupleHashJoinStep*>((dsi->second).get());

                    if (bpsDelivery)
                    {
                        CalpontSystemCatalog::OID tableOID = bpsDelivery->tableOid();
                        dotFile << tableOID << " [label=" << bpsDelivery->alias() <<
                                " shape=plaintext]" << endl;
                        JobStepAssociation deliveryInputSA = bpsDelivery->inputAssociation();

                        if (tableOIDProject == tableOID)
                        {
                            dotFile << stepidIn << " -> " << tableOID;

                            dotFile << " [label=\" r: " << outSize << "\\l";
                            dotFile << " m: " << bpsDelivery->msgsRcvdCount() << "\\l";
                            dotFile << " b: " << bpsDelivery->blockTouched() << "\\l";
                            dotFile << " p: " << bpsDelivery->phyIOCount() << "\\l";
                            dotFile << "\"]" << endl;
                        }
                    }
                    else if (thjDelivery)
                    {
                        CalpontSystemCatalog::OID tableOID = thjDelivery->tableOid();
                        dotFile << tableOID << " [label=" << "vtable" << " shape=plaintext]" << endl;
                        JobStepAssociation deliveryInputSA = thjDelivery->inputAssociation();

                        if (tableOIDProject == tableOID)
                        {
                            dotFile << stepidIn << " -> " << tableOID;

                            dotFile << " [label=\" r: " << outSize << "\\l";
                            dotFile << " m: " << thjDelivery->msgsRcvdCount() << "\\l";
                            dotFile << " b: " << thjDelivery->blockTouched() << "\\l";
                            dotFile << " p: " << thjDelivery->phyIOCount() << "\\l";
                            dotFile << "\"]" << endl;
                        }
                    }
                }
            }
        }
        else if (typeid(*(qsi->get())) == typeid(CrossEngineStep))
        {
            outSize = dynamic_cast<CrossEngineStep*>(qsi->get())->getRows();
            dotFile << "0" << " [label=" << qsi->get()->alias() << " shape=plaintext]" << endl;
            dotFile << stepidIn << " -> 0";
            dotFile << " [label=\" r: " << outSize << "\\l";
            dotFile << "\"]" << endl;
        }
    }

    for (psi = fProject.begin(), ctn = 0; psi != fProject.end(); ctn++, psi++)
    {
        tcn.column = "";
        uint16_t stepidIn = psi->get()->stepId();
        dotFile << stepidIn << " [label=\"st_" << stepidIn << " ";
        tcn = csc->colName(psi->get()->oid());
        dotFile << "(";
        BatchPrimitive* bps = 0;

        if (typeid(*(psi->get())) == typeid(TupleBPS))
        {
            bps = dynamic_cast<BatchPrimitive*>(psi->get());
            OIDVector projectOids = bps->getProjectOids();

            for ( unsigned int i = 0; i < projectOids.size(); i++ )
            {
                tcn = csc->colName(projectOids[i]);

                if (!tcn.column.empty())
                {
                    dotFile << tcn.column;

                    if ( i != (projectOids.size() - 1) )
                        dotFile << "/  ";
                }

                if ( (i + 1) % 3 == 0 )
                    dotFile << "\\l";
            }
        }
        else
        {
            if (!tcn.column.empty())
                dotFile << tcn.column << "/";
        }

        dotFile << JSTimeStamp::tsdiffstr(psi->get()->dlTimes.EndOfInputTime(), psi->get()->dlTimes.FirstReadTime()) << "s";
        dotFile << ")";

        if (typeid(*(psi->get())) == typeid(pColStep))
        {
            dotFile << "\"" << " shape=ellipse";
        }
        else if (typeid(*(psi->get())) == typeid(pColScanStep))
        {
            dotFile << "\"" << " shape=box";
        }
        else if (typeid(*(psi->get())) == typeid(TupleBPS))
        {
            dotFile << "\"" << " shape=box style=bold";

            if (typeid(*(psi->get())) == typeid(TupleBPS))
                dotFile << " peripheries=2";
        }
        else if (typeid(*(psi->get())) == typeid(pDictionaryStep))
        {
            dotFile << "\"" << " shape=trapezium";
        }
        else if (typeid(*(psi->get())) == typeid(PassThruStep))
        {
            dotFile << "\"" << " shape=octagon";
        }
        else if (typeid(*(psi->get())) == typeid(FilterStep))
        {
            dotFile << "\"" << " shape=house orientation=180";
        }
        else
            dotFile << "\"";

        dotFile << "]" << endl;

        // msgsRecived, physicalIO, cacheIO
        msgs = psi->get()->msgsRcvdCount();
        pio = psi->get()->phyIOCount();

        CalpontSystemCatalog::OID tableOIDProject = 0;

        if (bps)
            tableOIDProject = bps->tableOid();

        //@Bug 921
        for (dsi = fDeliveredTables.begin(); dsi != fDeliveredTables.end(); dsi++)
        {
            BatchPrimitive* dbps = dynamic_cast<BatchPrimitive*>((dsi->second).get());

            if (dbps)
            {
                outSize = dbps->getRows();
                CalpontSystemCatalog::OID tableOID = dbps->tableOid();
                dotFile << tableOID << " [label=" << dbps->alias() << " shape=plaintext]" << endl;
                JobStepAssociation deliveryInputSA = dbps->inputAssociation();

                if ( tableOIDProject == tableOID )
                {
                    dotFile << stepidIn << " -> " << tableOID;

                    dotFile << " [label=\" r: " << outSize << "\\l";
                    dotFile << " m: " << dbps->msgsRcvdCount() << "\\l";
                    dotFile << " b: " << dbps->blockTouched() << "\\l";
                    dotFile << " p: " << dbps->phyIOCount() << "\\l";
                    dotFile << "\"]" << endl;

                }
            }
        }
    }

    dotFile << "}" << std::endl;
    dotFile.close();
}

void JobList::validate() const
{
//	uint32_t i;
//	DeliveredTableMap::const_iterator it;

    /* Make sure there's at least one query step and that they're the right type */
    idbassert(fQuery.size() > 0);
//	for (i = 0; i < fQuery.size(); i++)
//		idbassert(dynamic_cast<BatchPrimitiveStep *>(fQuery[i].get()) ||
//			dynamic_cast<HashJoinStep *>(fQuery[i].get()) ||
//			dynamic_cast<UnionStep *>(fQuery[i].get()) ||
//			dynamic_cast<AggregateFilterStep *>(fQuery[i].get()) ||
//			dynamic_cast<BucketReuseStep *>(fQuery[i].get()) ||
//			dynamic_cast<pDictionaryScan *>(fQuery[i].get()) ||
//			dynamic_cast<FilterStep *>(fQuery[i].get()) ||
//			dynamic_cast<OrDelimiter *>(fQuery[i].get())
//		);
//
//	/* Make sure there's at least one projected table and that they're the right type */
//	idbassert(fDeliveredTables.size() > 0);
//	for (i = 0; i < fProject.size(); i++)
//		idbassert(dynamic_cast<BatchPrimitiveStep *>(fProject[i].get()));
//
//	/* Check that all JobSteps use the right status pointer */
//	for (i = 0; i < fQuery.size(); i++) {
//		idbassert(fQuery[i]->errorInfo().get() == errorInfo().get());
//	}
//	for (i = 0; i < fProject.size(); i++) {
//		idbassert(fProject[i]->errorInfo().get() == errorInfo().get());
//	}
//	for (it = fDeliveredTables.begin(); it != fDeliveredTables.end(); ++it) {
//		idbassert(it->second->errorInfo().get() == errorInfo().get());
//	}
}

void TupleJobList::validate() const
{
    uint32_t i, j;
    DeliveredTableMap::const_iterator it;

    idbassert(fQuery.size() > 0);

    for (i = 0; i < fQuery.size(); i++)
    {
        idbassert(dynamic_cast<TupleBPS*>(fQuery[i].get()) ||
                  dynamic_cast<TupleHashJoinStep*>(fQuery[i].get()) ||
                  dynamic_cast<TupleAggregateStep*>(fQuery[i].get()) ||
                  dynamic_cast<TupleUnion*>(fQuery[i].get()) ||
                  dynamic_cast<pDictionaryScan*>(fQuery[i].get())
                 );
    }

    /* Duplicate check */
    for (i = 0; i < fQuery.size(); i++)
        for (j = i + 1; j < fQuery.size(); j++)
            idbassert(fQuery[i].get() != fQuery[j].get());

    /****  XXXPAT: An indication of a possible problem: The next assertion fails
    	occasionally */
// 	idbassert(fDeliveredTables.begin()->first == 100);  //fake oid for the vtable
// 	cout << "Delivered TableOID is " << fDeliveredTables.begin()->first << endl;
    idbassert(fProject.size() == 0);
    idbassert(fDeliveredTables.size() == 1);
    idbassert(dynamic_cast<TupleDeliveryStep*>(fDeliveredTables.begin()->second.get()));

    /* Check that all JobSteps use the right status pointer */
    for (i = 0; i < fQuery.size(); i++)
        idbassert(fQuery[i]->errorInfo().get() == errorInfo().get());

    for (i = 0; i < fProject.size(); i++)
        idbassert(fProject[i]->errorInfo().get() == errorInfo().get());

    for (it = fDeliveredTables.begin(); it != fDeliveredTables.end(); ++it)
        idbassert(it->second->errorInfo().get() == errorInfo().get());
}

void JobList::abort()
{
    uint32_t i;

    //If we're not currently aborting, then start aborting...
    if (atomicops::atomicCAS<uint32_t>(&fAborted, 0, 1))
    {
        for (i = 0; i < fQuery.size(); i++)
            fQuery[i]->abort();

        for (i = 0; i < fProject.size(); i++)
            fProject[i]->abort();


    }
}

void JobList::abortOnLimit(JobStep* js)
{
    //If we're not currently aborting, then start aborting...
    if (atomicops::atomicCAS<uint32_t>(&fAborted, 0, 1))
    {
        // @bug4848, enhance and unify limit handling.
        for (uint32_t i = 0; i < fQuery.size(); i++)
        {
            if (fQuery[i].get() == js)
                break;

            fQuery[i]->abort();
        }
    }
}

string JobList::toString() const
{
    uint32_t i;
    string ret;

    ret = "Filter Steps:\n";

    for (i = 0; i < fQuery.size(); i++)
        ret += fQuery[i]->toString();

    //ret += "\nProjection Steps:\n";
    //for (i = 0; i < fProject.size(); i++)
    //	ret += fProject[i]->toString();
    ret += "\n";
    return ret;
}

TupleJobList::TupleJobList(bool isEM) : JobList(isEM), ds(NULL), moreData(true)
{
}

TupleJobList::~TupleJobList()
{
    abort();
}

void TupleJobList::abort()
{
    if (fAborted == 0 && fIsRunning)
    {
        JobList::abort();
        messageqcpp::ByteStream bs;

        if (ds && moreData)
            while (ds->nextBand(bs));
    }
}


}

// vim:ts=4 sw=4:


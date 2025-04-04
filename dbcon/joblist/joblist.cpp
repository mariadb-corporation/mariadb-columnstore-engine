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
#include <algorithm>
#define PREFER_MY_CONFIG_H
#include "crossenginestep.h"
#include "errorcodes.h"
#include <iterator>
#include <stdexcept>
// #define NDEBUG
#include <cassert>
using namespace std;

#include "joblist.h"
#include "calpontsystemcatalog.h"
using namespace execplan;

#include "errorids.h"
#include "jlf_graphics.h"
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

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpotentially-evaluated-expression"
// for warnings on typeid :expression with side effects will be evaluated despite being used as an operand to
// 'typeid'
#endif

namespace joblist
{
int JobList::fPmsConfigured = 0;

struct JSJoiner
{
  JSJoiner(JobStep* j) : js(j)
  {
  }
  void operator()()
  {
    js->abort();
    js->join();
  }
  JobStep* js;
};

JobList::JobList(bool isEM)
 : fIsRunning(false), fIsExeMgr(isEM), fPmsConnected(0), projectingTableOID(0), fAborted(0), fPriority(50)
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
  JobStepVector::iterator end2 = fQuery.end();
  int rc = -1;

  while (iter2 != end2)
  {
    js = iter2->get();

    if (js->traceOn())
    {
      if (js->delayedRun())
      {
        std::ostringstream oss;
        oss << "Session: " << js->sessionId() << "; delaying start of query step " << js->stepId()
            << "; waitStepCount-" << js->waitToRunStepCnt() << std::endl;
        std::cout << oss.str();
      }
    }

    ++iter2;
  }

  iter2 = fProject.begin();
  end2 = fProject.end();

  while (iter2 != end2)
  {
    js = iter2->get();

    if (js->traceOn())
    {
      if (js->delayedRun())
      {
        std::ostringstream oss;
        oss << "Session: " << js->sessionId() << "; delaying start of project step " << js->stepId()
            << "; waitStepCount-" << js->waitToRunStepCnt() << std::endl;
        std::cout << oss.str();
      }
    }

    ++iter2;
  }

  JobStepVector::iterator iter = fQuery.begin();
  JobStepVector::iterator end = fQuery.end();

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
    errInfo->errMsg = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_NO_PRIMPROC);
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
      errInfo->errMsg = logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_NO_PRIMPROC);
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

  TupleDeliveryStep* tds = dynamic_cast<TupleDeliveryStep*>(fDeliveredTables.begin()->second.get());

  if (tds == NULL)
    throw runtime_error("Not a TupleDeliveryStep!!");

  return tds->getDeliveredRowGroup();
}

void TupleJobList::setDeliveryFlag(bool f)
{
  DeliveredTableMap::iterator iter = fDeliveredTables.begin();
  SJSTEP dstep = iter->second;
  ds = dynamic_cast<TupleDeliveryStep*>(dstep.get());

  if (ds)  // if not dummy step
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


    //
    //...Add up the I/O and msg counts for the query job steps
    //
    for (auto iter = fQuery.rbegin(); iter != fQuery.rend(); ++iter)
    {
      auto* js = iter->get();

      fStats.fPhyIO += js->phyIOCount();
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

      //...As long as we only have 2 job steps that involve casual
      //...partioning, we just define blkSkipped() in those 2 classes,
      //...and use typeid to find/invoke those methods.  If we start
      //...adding blkSkipped() to more classes, we should probably make
      //...blkSkipped() a pure virtual method of the base JobStep class
      //...so that we don't have to do this type checking and casting.
      uint64_t skipCnt = 0;

      if (typeid(*js) == typeid(pColStep))
        skipCnt = (dynamic_cast<pColStep*>(js))->blksSkipped();
      else if (typeid(*js) == typeid(pColScanStep))
        skipCnt = (dynamic_cast<pColScanStep*>(js))->blksSkipped();
      else if (typeid(*js) == typeid(TupleBPS))
        skipCnt = (dynamic_cast<BatchPrimitive*>(js))->blksSkipped();

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
    }

    //
    //...Add up the I/O and msg counts for the projection job steps
    //
    for (auto iter = fProject.rbegin(); iter != fProject.rend(); ++iter)
    {
      auto* js = iter->get();

      fStats.fPhyIO += js->phyIOCount();
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
        skipCnt = (dynamic_cast<pColStep*>(js))->blksSkipped();
      else if (typeid(*js) == typeid(pColScanStep))
        skipCnt = (dynamic_cast<pColScanStep*>(js))->blksSkipped();
      else if (typeid(*js) == typeid(TupleBPS))
        skipCnt = (dynamic_cast<BatchPrimitive*>(js))->blksSkipped();

      fStats.fCPBlocksSkipped += skipCnt;
    }

    if ((!fProject.empty()) && extendedStats)
    {
      DeliveredTableMap::iterator dsi = fDeliveredTables.begin();

      while (dsi != fDeliveredTables.end())
      {
        auto js = dynamic_cast<JobStep*>(dsi->second.get());
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
    // bug3438, print subquery info prior to outer query
    for (auto i = subqueryJoblists.rbegin(); i != subqueryJoblists.rend(); i++)
    {
      i->get()->querySummary(extendedStats);
      fStats += i->get()->queryStats();
      fExtendedInfo += i->get()->extendedInfo();
      fMiniInfo += i->get()->miniInfo();
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
  auto jsrname = jlf_graphics::generateDotFileName("jobstep_results.");
  std::ofstream dotFile(jsrname.c_str(), std::ios::out);
  dotFile << jlf_graphics::GraphGeneratorWStats(fQuery, fProject).writeDotCmds();
}

void JobList::validate() const
{
  /* Make sure there's at least one query step and that they're the right type */
  idbassert(fQuery.size() > 0);
}

void TupleJobList::validate() const
{
  uint32_t i, j;
  DeliveredTableMap::const_iterator it;

  idbassert(fQuery.size() > 0);

  for (i = 0; i < fQuery.size(); i++)
  {
    idbassert(dynamic_cast<TupleBPS*>(fQuery[i].get()) || dynamic_cast<TupleHashJoinStep*>(fQuery[i].get()) ||
              dynamic_cast<TupleAggregateStep*>(fQuery[i].get()) ||
              dynamic_cast<TupleUnion*>(fQuery[i].get()) || dynamic_cast<pDictionaryScan*>(fQuery[i].get()));
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

  // If we're not currently aborting, then start aborting...
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
  // If we're not currently aborting, then start aborting...
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

  ret += "\nProjection Steps:\n";
  for (i = 0; i < fProject.size(); i++)
    ret += fProject[i]->toString();
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
      while (ds->nextBand(bs))
        ;
  }
}

}  // namespace joblist

#ifdef __clang__
#pragma clang diagnostic pop
#endif

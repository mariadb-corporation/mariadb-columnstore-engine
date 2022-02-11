/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (c) 2016-2020 MariaDB

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

//  $Id: tuplehashjoin.cpp 9709 2013-07-20 06:08:46Z xlou $

#include <climits>
#include <cstdio>
#include <ctime>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <unistd.h>
//#define NDEBUG
#include <cassert>
#include <algorithm>
using namespace std;

#include "jlf_common.h"
#include "primitivestep.h"
#include "tuplehashjoin.h"
#include "calpontsystemcatalog.h"
#include "elementcompression.h"
#include "tupleaggregatestep.h"
#include "errorids.h"
#include "diskjoinstep.h"
#include "vlarray.h"

using namespace execplan;
using namespace joiner;
using namespace rowgroup;
using namespace boost;
using namespace funcexp;

#include "querytele.h"
using namespace querytele;

#include "atomicops.h"
#include "spinlock.h"

namespace joblist
{
TupleHashJoinStep::TupleHashJoinStep(const JobInfo& jobInfo)
 : JobStep(jobInfo)
 , joinType(INIT)
 , fTableOID1(0)
 , fTableOID2(0)
 , fOid1(0)
 , fOid2(0)
 , fDictOid1(0)
 , fDictOid2(0)
 , fSequence1(-1)
 , fSequence2(-1)
 , fTupleId1(-1)
 , fTupleId2(-1)
 , fCorrelatedSide(0)
 , resourceManager(jobInfo.rm)
 , fMemSizeForOutputRG(0)
 , runRan(false)
 , joinRan(false)
 , largeSideIndex(1)
 , joinIsTooBig(false)
 , isExeMgr(jobInfo.isExeMgr)
 , lastSmallOuterJoiner(-1)
 , fTokenJoin(-1)
 , fStatsMutexPtr(new boost::mutex())
 , fFunctionJoinKeys(jobInfo.keyInfo->functionJoinKeys)
 , sessionMemLimit(jobInfo.umMemLimit)
 , rgdLock(false)
{
  /* Need to figure out how much memory these use...
      Overhead storing 16 byte elements is about 32 bytes.  That
      should stay the same for other element sizes.
  */

  pmMemLimit = resourceManager->getHjPmMaxMemorySmallSide(fSessionId);
  uniqueLimit = resourceManager->getHjCPUniqueLimit();

  fExtendedInfo = "THJS: ";
  joinType = INIT;
  joinThreadCount = resourceManager->getJlNumScanReceiveThreads();
  largeBPS = NULL;
  moreInput = true;
  fQtc.stepParms().stepType = StepTeleStats::T_HJS;
  outputDL = NULL;
  ownsOutputDL = false;
  djsSmallUsage = jobInfo.smallSideUsage;
  djsSmallLimit = jobInfo.smallSideLimit;
  djsLargeLimit = jobInfo.largeSideLimit;
  djsPartitionSize = jobInfo.partitionSize;
  isDML = jobInfo.isDML;

  config::Config* config = config::Config::makeConfig();
  string str = config->getConfig("HashJoin", "AllowDiskBasedJoin");

  if (str.empty() || str == "y" || str == "Y")
    allowDJS = true;
  else
    allowDJS = false;

  numCores = resourceManager->numCores();
  if (numCores <= 0)
    numCores = 8;
  /* Debugging, rand() is used to simulate failures
  time_t t = time(NULL);
  srand(t);
  */
}

TupleHashJoinStep::~TupleHashJoinStep()
{
  delete fStatsMutexPtr;

  if (ownsOutputDL)
    delete outputDL;

  if (memUsedByEachJoin)
  {
    for (uint i = 0; i < smallDLs.size(); i++)
    {
      if (memUsedByEachJoin[i])
        resourceManager->returnMemory(memUsedByEachJoin[i], sessionMemLimit);
    }
  }
  returnMemory();
  // cout << "deallocated THJS, UM memory available: " << resourceManager.availableMemory() << endl;
}

void TupleHashJoinStep::run()
{
  uint32_t i;

  boost::mutex::scoped_lock lk(jlLock);

  if (runRan)
    return;

  runRan = true;

  deliverMutex.lock();

  // 	cout << "TupleHashJoinStep::run(): fOutputJobStepAssociation.outSize = " <<
  // fOutputJobStepAssociation.outSize() << ", fDelivery = " << boolalpha << fDelivery << endl;
  idbassert((fOutputJobStepAssociation.outSize() == 1 && !fDelivery) ||
            (fOutputJobStepAssociation.outSize() == 0 && fDelivery));
  idbassert(fInputJobStepAssociation.outSize() >= 2);

  largeDL = fInputJobStepAssociation.outAt(largeSideIndex)->rowGroupDL();
  largeIt = largeDL->getIterator();

  for (i = 0; i < fInputJobStepAssociation.outSize(); i++)
  {
    if (i != largeSideIndex)
    {
      smallDLs.push_back(fInputJobStepAssociation.outAt(i)->rowGroupDL());
      smallIts.push_back(smallDLs.back()->getIterator());
    }
  }

  if (!fDelivery)
    outputDL = fOutputJobStepAssociation.outAt(0)->rowGroupDL();
  else if (!largeBPS)
  {
    ownsOutputDL = true;
    outputDL = new RowGroupDL(1, 5);
    outputIt = outputDL->getIterator();
  }

  joiners.resize(smallDLs.size());
  mainRunner = jobstepThreadPool.invoke(HJRunner(this));
}

void TupleHashJoinStep::join()
{
  boost::mutex::scoped_lock lk(jlLock);

  if (joinRan)
    return;

  joinRan = true;
  jobstepThreadPool.join(mainRunner);

  if (djs)
  {
    for (int i = 0; i < (int)djsJoiners.size(); i++)
      djs[i].join();

    jobstepThreadPool.join(djsReader);
    jobstepThreadPool.join(djsRelay);
    // cout << "THJS: joined all DJS threads, shared usage = " << *djsSmallUsage << endl;
  }
}

// simple sol'n.  Poll mem usage of Joiner once per second.  Request mem
// increase after the fact.  Failure to get mem will be detected and handled by
// the threads inserting into Joiner.
void TupleHashJoinStep::trackMem(uint index)
{
  boost::shared_ptr<TupleJoiner> joiner = joiners[index];
  ssize_t memBefore = 0, memAfter = 0;
  bool gotMem;

  boost::unique_lock<boost::mutex> scoped(memTrackMutex);
  while (!stopMemTracking)
  {
    memAfter = joiner->getMemUsage();
    if (memAfter != memBefore)
    {
      gotMem = resourceManager->getMemory(memAfter - memBefore, sessionMemLimit, true);
      if (gotMem)
        atomicops::atomicAdd(&memUsedByEachJoin[index], memAfter - memBefore);
      else
        return;

      memBefore = memAfter;
    }
    memTrackDone.timed_wait(scoped, boost::posix_time::seconds(1));
  }

  // one more iteration to capture mem usage since last poll, for this one
  // raise an error if mem went over the limit
  memAfter = joiner->getMemUsage();
  if (memAfter == memBefore)
    return;
  gotMem = resourceManager->getMemory(memAfter - memBefore, sessionMemLimit, true);
  if (gotMem)
  {
    atomicops::atomicAdd(&memUsedByEachJoin[index], memAfter - memBefore);
  }
  else
  {
    if (!joinIsTooBig &&
        (isDML || !allowDJS || (fSessionId & 0x80000000) || (tableOid() < 3000 && tableOid() >= 1000)))
    {
      joinIsTooBig = true;
      ostringstream oss;
      oss << "(" << __LINE__ << ") "
          << logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_JOIN_TOO_BIG);
      fLogger->logMessage(logging::LOG_TYPE_INFO, oss.str());
      errorMessage(oss.str());
      status(logging::ERR_JOIN_TOO_BIG);
      cout << "Join is too big, raise the UM join limit for now (monitor thread)" << endl;
      abort();
    }
  }
}

void TupleHashJoinStep::startSmallRunners(uint index)
{
  utils::setThreadName("HJSStartSmall");
  string extendedInfo;
  JoinType jt;
  boost::shared_ptr<TupleJoiner> joiner;

  jt = joinTypes[index];
  extendedInfo += toString();

  if (typelessJoin[index])
  {
    joiner.reset(new TupleJoiner(smallRGs[index], largeRG, smallSideKeys[index], largeSideKeys[index], jt,
                                 &jobstepThreadPool));
  }
  else
  {
    joiner.reset(new TupleJoiner(smallRGs[index], largeRG, smallSideKeys[index][0], largeSideKeys[index][0],
                                 jt, &jobstepThreadPool));
  }

  joiner->setUniqueLimit(uniqueLimit);
  joiner->setTableName(smallTableNames[index]);
  joiners[index] = joiner;

  /* check for join types unsupported on the PM. */
  if (!largeBPS || !isExeMgr)
    joiner->setInUM(rgData[index]);

  /*
      start the small runners
      join them
      check status
      handle abort, out of memory, etc
  */

  /* To measure wall-time spent constructing the small-side tables...
  boost::posix_time::ptime end_time, start_time =
      boost::posix_time::microsec_clock::universal_time();
  */

  stopMemTracking = false;
  utils::VLArray<uint64_t> jobs(numCores);
  uint64_t memMonitor = jobstepThreadPool.invoke([this, index] { this->trackMem(index); });
  // starting 1 thread when in PM mode, since it's only inserting into a
  // vector of rows.  The rest will be started when converted to UM mode.
  if (joiner->inUM())
    for (int i = 0; i < numCores; i++)
      jobs[i] = jobstepThreadPool.invoke([this, i, index, &jobs] { this->smallRunnerFcn(index, i, jobs); });
  else
    jobs[0] = jobstepThreadPool.invoke([this, index, &jobs] { this->smallRunnerFcn(index, 0, jobs); });

  // wait for the first thread to join, then decide whether the others exist and need joining
  jobstepThreadPool.join(jobs[0]);
  if (joiner->inUM())
    for (int i = 1; i < numCores; i++)
      jobstepThreadPool.join(jobs[i]);

  // stop the monitor thread
  memTrackMutex.lock();
  stopMemTracking = true;
  memTrackDone.notify_one();
  memTrackMutex.unlock();
  jobstepThreadPool.join(memMonitor);

  /* If there was an error or an abort, drain the input DL,
      do endOfInput on the output */
  if (cancelled())
  {
    //		cout << "HJ stopping... status is " << status() << endl;
    if (largeBPS)
      largeBPS->abort();

    bool more = true;
    RGData oneRG;
    while (more)
      more = smallDLs[index]->next(smallIts[index], &oneRG);
  }

  /* To measure wall-time spent constructing the small-side tables...
  end_time = boost::posix_time::microsec_clock::universal_time();
  if (!(fSessionId & 0x80000000))
      cout << "hash table construction time = " << end_time - start_time <<
      " size = " << joiner->size() << endl;
  */

  extendedInfo += "\n";

  ostringstream oss;
  if (!joiner->onDisk())
  {
    // add extended info, and if not aborted then tell joiner
    // we're done reading the small side.
    if (joiner->inPM())
    {
      oss << "PM join (" << index << ")" << endl;
#ifdef JLF_DEBUG
      cout << oss.str();
#endif
      extendedInfo += oss.str();
    }
    else if (joiner->inUM())
    {
      oss << "UM join (" << index << ")" << endl;
#ifdef JLF_DEBUG
      cout << oss.str();
#endif
      extendedInfo += oss.str();
    }
    if (!cancelled())
      joiner->doneInserting();
  }

  boost::mutex::scoped_lock lk(*fStatsMutexPtr);
  fExtendedInfo += extendedInfo;
  formatMiniStats(index);
}

/* Index is which small input to read. */
void TupleHashJoinStep::smallRunnerFcn(uint32_t index, uint threadID, uint64_t* jobs)
{
  utils::setThreadName("HJSmallRunner");
  bool more = true;
  RGData oneRG;
  Row r;
  RowGroupDL* smallDL;
  uint32_t smallIt;
  RowGroup smallRG;
  boost::shared_ptr<TupleJoiner> joiner = joiners[index];

  smallDL = smallDLs[index];
  smallIt = smallIts[index];
  smallRG = smallRGs[index];

  smallRG.initRow(&r);
  try
  {
    ssize_t rgSize;
    bool gotMem;
    goto next;
    while (more && !cancelled())
    {
      smallRG.setData(&oneRG);
      if (smallRG.getRowCount() == 0)
        goto next;

      // TupleHJ owns the row memory
      utils::getSpinlock(rgdLock);
      rgData[index].push_back(oneRG);
      utils::releaseSpinlock(rgdLock);

      rgSize = smallRG.getSizeWithStrings();
      gotMem = resourceManager->getMemory(rgSize, sessionMemLimit, true);
      if (gotMem)
      {
        atomicops::atomicAdd(&memUsedByEachJoin[index], rgSize);
      }
      else
      {
        /*  Mem went over the limit.
            If DML or a syscat query, abort.
            if disk join is enabled, use it.
            else abort.
        */
        boost::unique_lock<boost::mutex> sl(saneErrMsg);
        if (cancelled())
          return;
        if (!allowDJS || isDML || (fSessionId & 0x80000000) || (tableOid() < 3000 && tableOid() >= 1000))
        {
          joinIsTooBig = true;
          ostringstream oss;
          oss << "(" << __LINE__ << ") "
              << logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_JOIN_TOO_BIG);
          fLogger->logMessage(logging::LOG_TYPE_INFO, oss.str());
          errorMessage(oss.str());
          status(logging::ERR_JOIN_TOO_BIG);
          cout << "Join is too big, raise the UM join limit for now (small runner)" << endl;
          abort();
        }
        else if (allowDJS)
          joiner->setConvertToDiskJoin();

        return;
      }
      joiner->insertRGData(smallRG, threadID);
      if (!joiner->inUM() && (memUsedByEachJoin[index] > pmMemLimit))
      {
        joiner->setInUM(rgData[index]);
        for (int i = 1; i < numCores; i++)
          jobs[i] =
              jobstepThreadPool.invoke([this, i, index, jobs] { this->smallRunnerFcn(index, i, jobs); });
      }
    next:
      dlMutex.lock();
      more = smallDL->next(smallIt, &oneRG);
      dlMutex.unlock();
    }
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_EXEMGR_MALFUNCTION, logging::ERR_JOIN_TOO_BIG,
                    "TupleHashJoinStep::smallRunnerFcn()");
    status(logging::ERR_EXEMGR_MALFUNCTION);
  }
  if (!joiner->inUM())
    joiner->setInPM();
}

void TupleHashJoinStep::forwardCPData()
{
  uint32_t i, col;

  if (largeBPS == NULL)
    return;

  for (i = 0; i < joiners.size(); i++)
  {
    if (joiners[i]->antiJoin() || joiners[i]->largeOuterJoin())
      continue;

    for (col = 0; col < joiners[i]->getSmallKeyColumns().size(); col++)
    {
      uint32_t idx = joiners[i]->getSmallKeyColumns()[col];

      if (smallRGs[i].isLongString(idx))
        continue;

      // @bug3683, not to add CP predicates if large side is not simple column
      if (fFunctionJoinKeys.find(largeRG.getKeys()[joiners[i]->getLargeKeyColumns()[col]]) !=
          fFunctionJoinKeys.end())
        continue;

      bool isSmallSideWideDecimal =
          datatypes::isWideDecimalType(smallRGs[i].getColType(idx), smallRGs[i].getColumnWidth(idx));

      largeBPS->addCPPredicates(largeRG.getOIDs()[joiners[i]->getLargeKeyColumns()[col]],
                                joiners[i]->getCPData()[col], !joiners[i]->discreteCPValues()[col],
                                isSmallSideWideDecimal);
    }
  }
}

void TupleHashJoinStep::djsRelayFcn()
{
  /*
      read from largeDL
      map to largeRG + outputRG format
      insert into fifos[0]
  */

  RowGroup djsInputRG = largeRG + outputRG;
  RowGroup l_largeRG = (tbpsJoiners.empty() ? largeRG : largeRG + outputRG);
  boost::shared_array<int> relayMapping = makeMapping(l_largeRG, djsInputRG);
  bool more;
  RGData inData, outData;
  Row l_largeRow, djsInputRow;
  int i;

  l_largeRG.initRow(&l_largeRow);
  djsInputRG.initRow(&djsInputRow);

  // cout << "Relay started" << endl;

  more = largeDL->next(largeIt, &inData);

  while (more && !cancelled())
  {
    l_largeRG.setData(&inData);

    // if (fSessionId < 0x80000000)
    //	cout << "got largeside data = " << l_largeRG.toString() << endl;
    if (l_largeRG.getRowCount() == 0)
      goto next;

    outData.reinit(djsInputRG, l_largeRG.getRowCount());
    djsInputRG.setData(&outData);
    djsInputRG.resetRowGroup(0);
    l_largeRG.getRow(0, &l_largeRow);
    djsInputRG.getRow(0, &djsInputRow);

    for (i = 0; i < (int)l_largeRG.getRowCount(); i++, l_largeRow.nextRow(), djsInputRow.nextRow())
    {
      applyMapping(relayMapping, l_largeRow, &djsInputRow);
      djsInputRG.incRowCount();
    }

    fifos[0]->insert(outData);
  next:
    more = largeDL->next(largeIt, &inData);
  }

  while (more)
    more = largeDL->next(largeIt, &inData);

  fifos[0]->endOfInput();
}

void TupleHashJoinStep::djsReaderFcn(int index)
{
  /*
      read from fifos[index]
         - incoming rgdata's have outputRG format
      do FE2 processing
      put into outputDL, to be picked up by the next JS or nextBand()
  */

  int it = fifos[index]->getIterator();
  bool more = true;
  RowGroup l_outputRG = outputRG;
  RGData rgData;
  vector<RGData> v_rgData;

  RowGroup l_fe2RG;
  Row fe2InRow, fe2OutRow;
  FuncExpWrapper l_fe;

  if (fe2)
  {
    l_fe2RG = fe2Output;
    l_outputRG.initRow(&fe2InRow);
    l_fe2RG.initRow(&fe2OutRow);
    l_fe = *fe2;
  }

  makeDupList(fe2 ? l_fe2RG : l_outputRG);

  while (!cancelled())
  {
    more = fifos[index]->next(it, &rgData);

    if (!more)
      break;

    l_outputRG.setData(&rgData);

    if (l_outputRG.getRowCount() == 0)
      continue;

    v_rgData.clear();
    v_rgData.push_back(rgData);

    if (fe2)
      processFE2(l_outputRG, l_fe2RG, fe2InRow, fe2OutRow, &v_rgData, &l_fe);

    processDupList(0, (fe2 ? l_fe2RG : l_outputRG), &v_rgData);
    sendResult(v_rgData);
  }

  while (more)
    more = fifos[index]->next(it, &rgData);

  for (int i = 0; i < (int)djsJoiners.size(); i++)
  {
    fExtendedInfo += djs[i].extendedInfo();
    fMiniInfo += djs[i].miniInfo();
  }

  outputDL->endOfInput();
}

void TupleHashJoinStep::hjRunner()
{
  uint32_t i;
  std::vector<uint64_t> smallRunners;  // thread handles from thread pool

  if (cancelled())
  {
    if (fOutputJobStepAssociation.outSize() > 0)
      fOutputJobStepAssociation.outAt(0)->rowGroupDL()->endOfInput();

    startAdjoiningSteps();
    deliverMutex.unlock();
    return;
  }

  StepTeleStats sts;

  if (fTableOID1 >= 3000)
  {
    sts.query_uuid = fQueryUuid;
    sts.step_uuid = fStepUuid;
    sts.msg_type = StepTeleStats::ST_START;
    sts.start_time = QueryTeleClient::timeNowms();
    sts.total_units_of_work = 1;
    postStepStartTele(sts);
  }

  idbassert(joinTypes.size() == smallDLs.size());
  idbassert(joinTypes.size() == joiners.size());

  /* Start the small-side runners */
  rgData.reset(new vector<RGData>[smallDLs.size()]);
  memUsedByEachJoin.reset(new ssize_t[smallDLs.size()]);

  for (i = 0; i < smallDLs.size(); i++)
    atomicops::atomicZero(&memUsedByEachJoin[i]);

  try
  {
    /* Note: the only join that can have a useful small outer table is the last small outer,
     * the others get clobbered by the join after it. Turn off small outer for 'the others'.
     * The last small outer can be:
     *     the last small side; or followed by large outer small sides */
    bool turnOffSmallouter = false;

    for (int j = smallDLs.size() - 1; j >= 0; j--)
    {
      if (joinTypes[j] & SMALLOUTER)
      {
        if (turnOffSmallouter)
        {
          joinTypes[j] &= ~SMALLOUTER;
        }
        else  // turnOffSmallouter == false, keep this one, but turn off any one in front
        {
          lastSmallOuterJoiner = j;
          turnOffSmallouter = true;
        }
      }
      else if (joinTypes[j] & INNER && turnOffSmallouter == false)
      {
        turnOffSmallouter = true;
      }
    }

    smallRunners.clear();
    smallRunners.reserve(smallDLs.size());

    for (i = 0; i < smallDLs.size(); i++)
      smallRunners.push_back(jobstepThreadPool.invoke(SmallRunner(this, i)));
  }
  catch (thread_resource_error&)
  {
    string emsg = "TupleHashJoin caught a thread resource error, aborting...\n";
    errorMessage("too many threads");
    status(logging::threadResourceErr);
    errorLogging(emsg, logging::threadResourceErr);
    fDie = true;
    deliverMutex.unlock();
  }

  jobstepThreadPool.join(smallRunners);
  smallRunners.clear();

  for (i = 0; i < feIndexes.size() && joiners.size() > 0; i++)
    joiners[feIndexes[i]]->setFcnExpFilter(fe[i]);

  /* segregate the Joiners into ones for TBPS and ones for DJS */
  segregateJoiners();

  /* Need to clean this stuff up.  If the query was cancelled before this, and this would have had
     a disk join, it's still necessary to construct the DJS objects to finish the abort.
     Update: Is this more complicated than scanning joiners for either ondisk() or (not isFinished())
     and draining the corresponding inputs & telling downstream EOF?  todo, think about it */
  if (!djsJoiners.empty())
  {
    joinIsTooBig = false;

    if (!cancelled())
      fLogger->logMessage(logging::LOG_TYPE_INFO, logging::INFO_SWITCHING_TO_DJS);

    uint32_t smallSideCount = djsJoiners.size();

    if (!outputDL)
    {
      ownsOutputDL = true;
      outputDL = new RowGroupDL(1, 5);
      outputIt = outputDL->getIterator();
    }

    djs.reset(new DiskJoinStep[smallSideCount]);
    fifos.reset(new boost::shared_ptr<RowGroupDL>[smallSideCount + 1]);

    for (i = 0; i <= smallSideCount; i++)
      fifos[i].reset(new RowGroupDL(1, 5));

    boost::mutex::scoped_lock sl(djsLock);

    for (i = 0; i < smallSideCount; i++)
    {
      // these link themselves fifos[0]->DSJ[0]->fifos[1]->DSJ[1] ... ->fifos[smallSideCount],
      // THJS puts data into fifos[0], reads it from fifos[smallSideCount]
      djs[i] = DiskJoinStep(this, i, djsJoinerMap[i], (i == smallSideCount - 1));
    }

    sl.unlock();

    try
    {
      for (i = 0; !cancelled() && i < smallSideCount; i++)
      {
        vector<RGData> empty;
        resourceManager->returnMemory(memUsedByEachJoin[djsJoinerMap[i]], sessionMemLimit);
        atomicops::atomicZero(&memUsedByEachJoin[i]);
        djs[i].loadExistingData(rgData[djsJoinerMap[i]]);
        rgData[djsJoinerMap[i]].swap(empty);
      }
    }
    catch (...)
    {
      handleException(std::current_exception(), logging::ERR_EXEMGR_MALFUNCTION, logging::ERR_JOIN_TOO_BIG,
                      "TupleHashJoinStep::hjRunner()");
      status(logging::ERR_EXEMGR_MALFUNCTION);
      abort();
    }

    if (fe2)
      fe2Mapping = makeMapping(outputRG, fe2Output);

    bool relay = false, reader = false;

    /* If an error happened loading the existing data, these threads are necessary
    to finish the abort */
    try
    {
      djsRelay = jobstepThreadPool.invoke(DJSRelay(this));
      relay = true;
      djsReader = jobstepThreadPool.invoke(DJSReader(this, smallSideCount));
      reader = true;

      for (i = 0; i < smallSideCount; i++)
        djs[i].run();
    }
    catch (thread_resource_error&)
    {
      /* This means there is a gap somewhere in the chain, need to identify
         where the gap is, drain the input, and close the output. */

      string emsg = "TupleHashJoin caught a thread resource error, aborting...\n";
      errorMessage("too many threads");
      status(logging::threadResourceErr);
      errorLogging(emsg, logging::threadResourceErr);
      abort();

      if (reader && relay)  // must have been thrown from the djs::run() loop
      {
        // fill the gap in the chain: drain input of the failed DJS (i), close the last fifo
        if (largeBPS)
          largeDL->endOfInput();

        int it = fifos[i]->getIterator();
        RGData rg;

        while (fifos[i]->next(it, &rg))
          ;

        fifos[smallSideCount]->endOfInput();
      }
      else  // no DJS's have been started
      {
        if (relay)
        {
          // drain Relay's output
          if (largeBPS)
            largeDL->endOfInput();

          int it = fifos[0]->getIterator();
          RGData rg;

          while (fifos[0]->next(it, &rg))
            ;
        }

        if (reader)
          // close Reader's input
          fifos[smallSideCount]->endOfInput();
        else  // close the next JobStep's input
          outputDL->endOfInput();
      }
    }
  }

  /* Final THJS configuration is settled here at the moment */
  deliverMutex.unlock();

  if (cancelled())
  {
    if (joinIsTooBig && !status())
    {
      ostringstream oss;
      oss << "(" << __LINE__ << ") "
          << logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_JOIN_TOO_BIG);
      fLogger->logMessage(logging::LOG_TYPE_INFO, oss.str());
      errorMessage(oss.str());
      status(logging::ERR_JOIN_TOO_BIG);
      cout << "Join is too big, raise the UM join limit for now" << endl;

      /* Drop memory */
      if (!fDelivery)
      {
        joiners.clear();
        tbpsJoiners.clear();
        rgData.reset();
        for (uint i = 0; i < smallDLs.size(); i++)
        {
          resourceManager->returnMemory(memUsedByEachJoin[i], sessionMemLimit);
          atomicops::atomicZero(&memUsedByEachJoin[i]);
        }
      }
    }
  }

  // todo: forwardCPData needs to grab data from djs
  if (!djs)
    forwardCPData();  // this fcn has its own exclusion list

  // decide if perform aggregation on PM
  if (dynamic_cast<TupleAggregateStep*>(fDeliveryStep.get()) != NULL && largeBPS)
  {
    bool pmAggregation = !(dynamic_cast<TupleAggregateStep*>(fDeliveryStep.get())->umOnly());

    for (i = 0; i < joiners.size() && pmAggregation; ++i)
      pmAggregation = pmAggregation && (joiners[i]->inPM() && !joiners[i]->smallOuterJoin());

    if (pmAggregation)
      dynamic_cast<TupleAggregateStep*>(fDeliveryStep.get())->setPmHJAggregation(largeBPS);
  }

  // can we sort the joiners?  Currently they all have to be inner joins.
  // Note, any vars that used to parallel the joiners list will be invalidated
  // (ie. smallTableNames)

  for (i = 0; i < tbpsJoiners.size(); i++)
    if (!tbpsJoiners[i]->innerJoin())
      break;

  if (i == tbpsJoiners.size())
    sort(tbpsJoiners.begin(), tbpsJoiners.end(), JoinerSorter());

  /* Each thread independently decides whether a given join can execute on the PM.
   * A PM join can't follow a UM join, so we fix that here.
   */
  bool doUM;

  for (i = 0, doUM = false; i < tbpsJoiners.size(); i++)
  {
    if (tbpsJoiners[i]->inUM())
      doUM = true;

    if (tbpsJoiners[i]->inPM() && doUM)
    {
#ifdef JLF_DEBUG
      cout << "moving join " << i << " to UM (PM join can't follow a UM join)\n";
#endif
      tbpsJoiners[i]->setInUM(rgData[i]);
    }
  }

  // there is an in-mem UM or PM join
  if (largeBPS && !tbpsJoiners.empty())
  {
    largeBPS->useJoiners(tbpsJoiners);

    if (djs)
      largeBPS->setJoinedResultRG(largeRG + outputRG);
    else
      largeBPS->setJoinedResultRG(outputRG);

    if (!feIndexes.empty())
      largeBPS->setJoinFERG(joinFilterRG);

    // 		cout << "join UM memory available is " << totalUMMemoryUsage << endl;

    /* Figure out whether fe2 can run with the tables joined on the PM.  If so,
    fe2 -> PM, otherwise fe2 -> UM.
    For now, the alg is "assume if any joins are done on the UM, fe2 has to go on
    the UM."  The structs and logic aren't in place yet to track all of the tables
    through a joblist. */
    if (fe2 && !djs)
    {
      /* Can't do a small outer join when the PM sends back joined rows */
      runFE2onPM = true;

      if (joinTypes[joiners.size() - 1] == SMALLOUTER)
        runFE2onPM = false;

      for (i = 0; i < joiners.size(); i++)
        if (joiners[i]->inUM())
        {
          runFE2onPM = false;
          break;
        }

#ifdef JLF_DEBUG
      if (runFE2onPM)
        cout << "PM runs FE2\n";
      else
        cout << "UM runs FE2\n";
#endif
      largeBPS->setFcnExpGroup2(fe2, fe2Output, runFE2onPM);
    }
    else if (fe2)
      runFE2onPM = false;

    if (!fDelivery && !djs)
    {
      /* connect the largeBPS directly to the next step */
      JobStepAssociation newJsa;
      newJsa.outAdd(fOutputJobStepAssociation.outAt(0));

      for (unsigned i = 1; i < largeBPS->outputAssociation().outSize(); i++)
        newJsa.outAdd(largeBPS->outputAssociation().outAt(i));

      largeBPS->outputAssociation(newJsa);
    }

    startAdjoiningSteps();
  }
  else if (largeBPS)
  {
    // there are no in-mem UM or PM joins, only disk-joins
    startAdjoiningSteps();
  }
  else if (!djs)
    // if there's no largeBPS, all joins are either done by DJS or join threads,
    // this clause starts the THJS join threads.
    startJoinThreads();

  if (fTableOID1 >= 3000)
  {
    sts.msg_type = StepTeleStats::ST_SUMMARY;
    sts.end_time = QueryTeleClient::timeNowms();
    sts.total_units_of_work = sts.units_of_work_completed = 1;
    postStepSummaryTele(sts);
  }
}

uint32_t TupleHashJoinStep::nextBand(messageqcpp::ByteStream& bs)
{
  RGData oneRG;
  bool more;
  uint32_t ret = 0;
  RowGroupDL* dl;
  uint64_t it;

  idbassert(fDelivery);

  boost::mutex::scoped_lock lk(deliverMutex);

  RowGroup* deliveredRG;

  if (fe2)
    deliveredRG = &fe2Output;
  else
    deliveredRG = &outputRG;

  if (largeBPS && !djs)
  {
    dl = largeDL;
    it = largeIt;
  }
  else
  {
    dl = outputDL;
    it = outputIt;
  }

  while (ret == 0)
  {
    if (cancelled())
    {
      oneRG.reinit(*deliveredRG, 0);
      deliveredRG->setData(&oneRG);
      deliveredRG->resetRowGroup(0);
      deliveredRG->setStatus(status());
      deliveredRG->serializeRGData(bs);
      more = true;

      while (more)
        more = dl->next(it, &oneRG);

      joiners.clear();
      rgData.reset();
      for (uint i = 0; i < smallDLs.size(); i++)
      {
        resourceManager->returnMemory(memUsedByEachJoin[i], sessionMemLimit);
        atomicops::atomicZero(&memUsedByEachJoin[i]);
      }
      return 0;
    }

    more = dl->next(it, &oneRG);

    if (!more)
    {
      joiners.clear();
      tbpsJoiners.clear();
      rgData.reset();
      oneRG.reinit(*deliveredRG, 0);
      deliveredRG->setData(&oneRG);
      deliveredRG->resetRowGroup(0);
      deliveredRG->setStatus(status());

      if (status() != 0)
        cout << " -- returning error status " << deliveredRG->getStatus() << endl;

      deliveredRG->serializeRGData(bs);
      for (uint i = 0; i < smallDLs.size(); i++)
      {
        resourceManager->returnMemory(memUsedByEachJoin[i], sessionMemLimit);
        atomicops::atomicZero(&memUsedByEachJoin[i]);
      }
      return 0;
    }

    deliveredRG->setData(&oneRG);
    ret = deliveredRG->getRowCount();
  }

  deliveredRG->serializeRGData(bs);
  return ret;
}

void TupleHashJoinStep::setLargeSideBPS(BatchPrimitive* b)
{
  largeBPS = dynamic_cast<TupleBPS*>(b);
}

void TupleHashJoinStep::startAdjoiningSteps()
{
  if (largeBPS)
    largeBPS->run();
}

/* TODO: update toString() with the multiple table join info */
const string TupleHashJoinStep::toString() const
{
  ostringstream oss;
  size_t idlsz = fInputJobStepAssociation.outSize();
  idbassert(idlsz > 1);
  oss << "TupleHashJoinStep    ses:" << fSessionId << " st:" << fStepId;
  oss << omitOidInDL;

  for (size_t i = 0; i < idlsz; ++i)
  {
    RowGroupDL* idl = fInputJobStepAssociation.outAt(i)->rowGroupDL();
    CalpontSystemCatalog::OID oidi = 0;

    if (idl)
      oidi = idl->OID();

    oss << " in ";

    if (largeSideIndex == i)
      oss << "*";

    oss << "tb/col:" << fTableOID1 << "/" << oidi;
    oss << " " << fInputJobStepAssociation.outAt(i);
  }

  idlsz = fOutputJobStepAssociation.outSize();

  if (idlsz > 0)
  {
    oss << endl << "					";
    RowGroupDL* dlo = fOutputJobStepAssociation.outAt(0)->rowGroupDL();
    CalpontSystemCatalog::OID oido = 0;

    if (dlo)
      oido = dlo->OID();

    oss << " out tb/col:" << fTableOID1 << "/" << oido;
    oss << " " << fOutputJobStepAssociation.outAt(0);
  }

  oss << endl;

  return oss.str();
}

//------------------------------------------------------------------------------
// Log specified error to stderr and the critical log
//------------------------------------------------------------------------------
void TupleHashJoinStep::errorLogging(const string& msg, int err) const
{
  ostringstream errMsg;
  errMsg << "Step " << stepId() << "; " << msg;
  cerr << errMsg.str() << endl;
  SErrorInfo errorInfo(new ErrorInfo);  // dummy, error info already set by caller.
  catchHandler(msg, err, errorInfo, fSessionId);
}

void TupleHashJoinStep::addSmallSideRG(const vector<rowgroup::RowGroup>& rgs, const vector<string>& tnames)
{
  smallTableNames.insert(smallTableNames.end(), tnames.begin(), tnames.end());
  smallRGs.insert(smallRGs.end(), rgs.begin(), rgs.end());
}

void TupleHashJoinStep::addJoinKeyIndex(const vector<JoinType>& jt, const vector<bool>& typeless,
                                        const vector<vector<uint32_t> >& smallkey,
                                        const vector<vector<uint32_t> >& largekey)
{
  joinTypes.insert(joinTypes.end(), jt.begin(), jt.end());
  typelessJoin.insert(typelessJoin.end(), typeless.begin(), typeless.end());
  smallSideKeys.insert(smallSideKeys.end(), smallkey.begin(), smallkey.end());
  largeSideKeys.insert(largeSideKeys.end(), largekey.begin(), largekey.end());
#ifdef JLF_DEBUG

  for (uint32_t i = 0; i < joinTypes.size(); i++)
    cout << "jointype[" << i << "] = 0x" << hex << joinTypes[i] << dec << endl;

#endif
}

void TupleHashJoinStep::configSmallSideRG(const vector<RowGroup>& rgs, const vector<string>& tnames)
{
  smallTableNames.insert(smallTableNames.begin(), tnames.begin(), tnames.end());
  smallRGs.insert(smallRGs.begin(), rgs.begin(), rgs.end());
}

void TupleHashJoinStep::configLargeSideRG(const RowGroup& rg)
{
  largeRG = rg;
}

void TupleHashJoinStep::configJoinKeyIndex(const vector<JoinType>& jt, const vector<bool>& typeless,
                                           const vector<vector<uint32_t> >& smallkey,
                                           const vector<vector<uint32_t> >& largekey)
{
  joinTypes.insert(joinTypes.begin(), jt.begin(), jt.end());
  typelessJoin.insert(typelessJoin.begin(), typeless.begin(), typeless.end());
  smallSideKeys.insert(smallSideKeys.begin(), smallkey.begin(), smallkey.end());
  largeSideKeys.insert(largeSideKeys.begin(), largekey.begin(), largekey.end());
#ifdef JLF_DEBUG

  for (uint32_t i = 0; i < joinTypes.size(); i++)
    cout << "jointype[" << i << "] = 0x" << hex << joinTypes[i] << dec << endl;

#endif
}

void TupleHashJoinStep::setOutputRowGroup(const RowGroup& rg)
{
  outputRG = rg;
}

execplan::CalpontSystemCatalog::OID TupleHashJoinStep::smallSideKeyOID(uint32_t s, uint32_t k) const
{
  return smallRGs[s].getOIDs()[smallSideKeys[s][k]];
}

execplan::CalpontSystemCatalog::OID TupleHashJoinStep::largeSideKeyOID(uint32_t s, uint32_t k) const
{
  return largeRG.getOIDs()[largeSideKeys[s][k]];
}

void TupleHashJoinStep::addFcnExpGroup2(const boost::shared_ptr<execplan::ParseTree>& fe)
{
  if (!fe2)
    fe2.reset(new funcexp::FuncExpWrapper());

  fe2->addFilter(fe);
}

void TupleHashJoinStep::setFcnExpGroup3(const vector<boost::shared_ptr<execplan::ReturnedColumn> >& v)
{
  if (!fe2)
    fe2.reset(new funcexp::FuncExpWrapper());

  for (uint32_t i = 0; i < v.size(); i++)
    fe2->addReturnedColumn(v[i]);
}

void TupleHashJoinStep::setFE23Output(const rowgroup::RowGroup& rg)
{
  fe2Output = rg;
}

const rowgroup::RowGroup& TupleHashJoinStep::getDeliveredRowGroup() const
{
  if (fe2)
    return fe2Output;

  return outputRG;
}

void TupleHashJoinStep::deliverStringTableRowGroup(bool b)
{
  if (fe2)
    fe2Output.setUseStringTable(b);

  outputRG.setUseStringTable(b);
}

bool TupleHashJoinStep::deliverStringTableRowGroup() const
{
  if (fe2)
    return fe2Output.usesStringTable();

  return outputRG.usesStringTable();
}

// Must hold the stats lock when calling this!
void TupleHashJoinStep::formatMiniStats(uint32_t index)
{
  ostringstream oss;
  oss << "HJS ";

  if (joiners[index]->inUM())
    oss << "UM ";
  else
    oss << "PM ";

  oss << alias() << "-" << joiners[index]->getTableName() << " ";

  if (fTableOID2 >= 3000)
    oss << fTableOID2;
  else
    oss << "- ";

  oss << " "
      << "- "
      << "- "
      << "- "
      << "- "
      //		<< JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << " "
      //		dlTimes are not timed in this step, using '--------' instead.
      << "-------- "
      << "-\n";
  fMiniInfo += oss.str();
}

void TupleHashJoinStep::addJoinFilter(boost::shared_ptr<execplan::ParseTree> pt, uint32_t index)
{
  boost::shared_ptr<funcexp::FuncExpWrapper> newfe(new funcexp::FuncExpWrapper());

  newfe->addFilter(pt);
  fe.push_back(newfe);
  feIndexes.push_back(index);
}

bool TupleHashJoinStep::hasJoinFilter(uint32_t index) const
{
  for (uint32_t i = 0; i < feIndexes.size(); i++)
    if (feIndexes[i] == static_cast<int>(index))
      return true;

  return false;
}

boost::shared_ptr<funcexp::FuncExpWrapper> TupleHashJoinStep::getJoinFilter(uint32_t index) const
{
  for (uint32_t i = 0; i < feIndexes.size(); i++)
    if (feIndexes[i] == static_cast<int>(index))
      return fe[i];

  return boost::shared_ptr<funcexp::FuncExpWrapper>();
}

void TupleHashJoinStep::setJoinFilterInputRG(const rowgroup::RowGroup& rg)
{
  joinFilterRG = rg;
}

void TupleHashJoinStep::startJoinThreads()
{
  uint32_t i;
  uint32_t smallSideCount = smallDLs.size();
  bool more = true;
  RGData oneRG;

  if (joinRunners.size() > 0)
    return;

  //@bug4836, in error case, stop process, and unblock the next step.
  if (cancelled())
  {
    outputDL->endOfInput();

    //@bug5785, memory leak on canceling complex queries
    while (more)
      more = largeDL->next(largeIt, &oneRG);

    return;
  }

  /* Init class-scope vars.
   *
   * Get a list of small RGs consistent with the joiners.
   * Generate small & large mappings for joinFERG and outputRG.
   * If fDelivery, create outputDL.
   */
  for (i = 0; i < smallSideCount; i++)
    smallRGs[i] = joiners[i]->getSmallRG();

  columnMappings.reset(new shared_array<int>[smallSideCount + 1]);

  for (i = 0; i < smallSideCount; i++)
    columnMappings[i] = makeMapping(smallRGs[i], outputRG);

  columnMappings[smallSideCount] = makeMapping(largeRG, outputRG);

  if (!feIndexes.empty())
  {
    fergMappings.reset(new shared_array<int>[smallSideCount + 1]);

    for (i = 0; i < smallSideCount; i++)
      fergMappings[i] = makeMapping(smallRGs[i], joinFilterRG);

    fergMappings[smallSideCount] = makeMapping(largeRG, joinFilterRG);
  }

  if (fe2)
    fe2Mapping = makeMapping(outputRG, fe2Output);

  smallNullMemory.reset(new scoped_array<uint8_t>[smallSideCount]);

  for (i = 0; i < smallSideCount; i++)
  {
    Row smallRow;
    smallRGs[i].initRow(&smallRow, true);
    smallNullMemory[i].reset(new uint8_t[smallRow.getSize()]);
    smallRow.setData(smallNullMemory[i].get());
    smallRow.initToNull();
  }

  for (i = 0; i < smallSideCount; i++)
    joiners[i]->setThreadCount(joinThreadCount);

  makeDupList(fe2 ? fe2Output : outputRG);

  /* Start join runners */
  joinRunners.reserve(joinThreadCount);

  for (i = 0; i < joinThreadCount; i++)
    joinRunners.push_back(jobstepThreadPool.invoke(JoinRunner(this, i)));

  /* Join them and call endOfInput */
  jobstepThreadPool.join(joinRunners);

  if (lastSmallOuterJoiner != (uint32_t)-1)
    finishSmallOuterJoin();

  outputDL->endOfInput();
}

void TupleHashJoinStep::finishSmallOuterJoin()
{
  vector<Row::Pointer> unmatched;
  uint32_t smallSideCount = smallDLs.size();
  uint32_t i, j, k;
  shared_array<uint8_t> largeNullMemory;
  RGData joinedData;
  Row joinedBaseRow, fe2InRow, fe2OutRow;
  shared_array<Row> smallRowTemplates;
  shared_array<Row> smallNullRows;
  Row largeNullRow;
  RowGroup l_outputRG = outputRG;
  RowGroup l_fe2Output = fe2Output;

  joiners[lastSmallOuterJoiner]->getUnmarkedRows(&unmatched);

  if (unmatched.empty())
    return;

  smallRowTemplates.reset(new Row[smallSideCount]);
  smallNullRows.reset(new Row[smallSideCount]);

  for (i = 0; i < smallSideCount; i++)
  {
    smallRGs[i].initRow(&smallRowTemplates[i]);
    smallRGs[i].initRow(&smallNullRows[i], true);
    smallNullRows[i].setData(smallNullMemory[i].get());
  }

  largeRG.initRow(&largeNullRow, true);
  largeNullMemory.reset(new uint8_t[largeNullRow.getSize()]);
  largeNullRow.setData(largeNullMemory.get());
  largeNullRow.initToNull();

  joinedData.reinit(l_outputRG);
  l_outputRG.setData(&joinedData);
  l_outputRG.resetRowGroup(0);
  l_outputRG.initRow(&joinedBaseRow);
  l_outputRG.getRow(0, &joinedBaseRow);

  if (fe2)
  {
    l_outputRG.initRow(&fe2InRow);
    fe2Output.initRow(&fe2OutRow);
  }

  for (j = 0; j < unmatched.size(); j++)
  {
    smallRowTemplates[lastSmallOuterJoiner].setPointer(unmatched[j]);

    for (k = 0; k < smallSideCount; k++)
    {
      if (k == lastSmallOuterJoiner)
        applyMapping(columnMappings[lastSmallOuterJoiner], smallRowTemplates[lastSmallOuterJoiner],
                     &joinedBaseRow);
      else
        applyMapping(columnMappings[k], smallNullRows[k], &joinedBaseRow);
    }

    applyMapping(columnMappings[smallSideCount], largeNullRow, &joinedBaseRow);
    joinedBaseRow.setRid(0);
    joinedBaseRow.nextRow();
    l_outputRG.incRowCount();

    if (l_outputRG.getRowCount() == 8192)
    {
      if (fe2)
      {
        vector<RGData> rgDatav;
        rgDatav.push_back(joinedData);
        processFE2(l_outputRG, l_fe2Output, fe2InRow, fe2OutRow, &rgDatav, fe2.get());
        outputDL->insert(rgDatav[0]);
      }
      else
      {
        outputDL->insert(joinedData);
      }

      joinedData.reinit(l_outputRG);
      l_outputRG.setData(&joinedData);
      l_outputRG.resetRowGroup(0);
      l_outputRG.getRow(0, &joinedBaseRow);
    }
  }

  if (l_outputRG.getRowCount() > 0)
  {
    if (fe2)
    {
      vector<RGData> rgDatav;
      rgDatav.push_back(joinedData);
      processFE2(l_outputRG, l_fe2Output, fe2InRow, fe2OutRow, &rgDatav, fe2.get());
      outputDL->insert(rgDatav[0]);
    }
    else
    {
      outputDL->insert(joinedData);
    }
  }
}

void TupleHashJoinStep::joinRunnerFcn(uint32_t threadID)
{
  RowGroup local_inputRG, local_outputRG, local_joinFERG;
  uint32_t smallSideCount = smallDLs.size();
  vector<RGData> inputData, joinedRowData;
  bool hasJoinFE = !fe.empty();
  uint32_t i;

  /* thread-local scratch space for join processing */
  shared_array<uint8_t> joinFERowData;
  Row largeRow, joinFERow, joinedRow, baseRow;
  shared_array<uint8_t> baseRowData;
  vector<vector<Row::Pointer> > joinMatches;
  shared_array<Row> smallRowTemplates;

  /* F & E vars */
  FuncExpWrapper local_fe;
  RowGroup local_fe2RG;
  Row fe2InRow, fe2OutRow;

  joinMatches.resize(smallSideCount);
  local_inputRG = largeRG;
  local_outputRG = outputRG;
  local_inputRG.initRow(&largeRow);
  local_outputRG.initRow(&joinedRow);
  local_outputRG.initRow(&baseRow, true);
  baseRowData.reset(new uint8_t[baseRow.getSize()]);
  baseRow.setData(baseRowData.get());

  if (hasJoinFE)
  {
    local_joinFERG = joinFilterRG;
    local_joinFERG.initRow(&joinFERow, true);
    joinFERowData.reset(new uint8_t[joinFERow.getSize()]);
    joinFERow.setData(joinFERowData.get());
  }

  if (fe2)
  {
    local_fe2RG = fe2Output;
    local_outputRG.initRow(&fe2InRow);
    local_fe2RG.initRow(&fe2OutRow);
    local_fe = *fe2;
  }

  smallRowTemplates.reset(new Row[smallSideCount]);

  for (i = 0; i < smallSideCount; i++)
    smallRGs[i].initRow(&smallRowTemplates[i]);

  grabSomeWork(&inputData);

  while (!inputData.empty() && !cancelled())
  {
    for (i = 0; i < inputData.size() && !cancelled(); i++)
    {
      local_inputRG.setData(&inputData[i]);

      if (local_inputRG.getRowCount() == 0)
        continue;

      joinOneRG(threadID, joinedRowData, local_inputRG, local_outputRG, largeRow, joinFERow, joinedRow,
                baseRow, joinMatches, smallRowTemplates, outputDL);
    }

    if (fe2)
      processFE2(local_outputRG, local_fe2RG, fe2InRow, fe2OutRow, &joinedRowData, &local_fe);

    processDupList(threadID, (fe2 ? local_fe2RG : local_outputRG), &joinedRowData);
    sendResult(joinedRowData);
    returnMemory();
    joinedRowData.clear();
    grabSomeWork(&inputData);
  }

  while (!inputData.empty())
    grabSomeWork(&inputData);
}

void TupleHashJoinStep::makeDupList(const RowGroup& rg)
{
  uint32_t i, j, cols = rg.getColumnCount();

  for (i = 0; i < cols; i++)
    for (j = i + 1; j < cols; j++)
      if (rg.getKeys()[i] == rg.getKeys()[j])
        dupList.push_back(make_pair(j, i));

  dupRows.reset(new Row[joinThreadCount]);

  for (i = 0; i < joinThreadCount; i++)
    rg.initRow(&dupRows[i]);
}

void TupleHashJoinStep::processDupList(uint32_t threadID, RowGroup& rg, vector<RGData>* rowData)
{
  uint32_t i, j, k;

  if (dupList.empty())
    return;

  for (i = 0; i < rowData->size(); i++)
  {
    rg.setData(&(*rowData)[i]);
    rg.getRow(0, &dupRows[threadID]);

    for (j = 0; j < rg.getRowCount(); j++, dupRows[threadID].nextRow())
      for (k = 0; k < dupList.size(); k++)
        dupRows[threadID].copyField(dupList[k].first, dupList[k].second);
  }
}

void TupleHashJoinStep::processFE2(RowGroup& input, RowGroup& output, Row& inRow, Row& outRow,
                                   vector<RGData>* rgData, funcexp::FuncExpWrapper* local_fe)
{
  vector<RGData> results;
  RGData result;
  uint32_t i, j;
  bool ret;

  result.reinit(output);
  output.setData(&result);
  output.resetRowGroup(0);
  output.getRow(0, &outRow);

  for (i = 0; i < rgData->size(); i++)
  {
    input.setData(&(*rgData)[i]);

    if (output.getRowCount() == 0)
    {
      output.resetRowGroup(input.getBaseRid());
      output.setDBRoot(input.getDBRoot());
    }

    input.getRow(0, &inRow);

    for (j = 0; j < input.getRowCount(); j++, inRow.nextRow())
    {
      ret = local_fe->evaluate(&inRow);

      if (ret)
      {
        applyMapping(fe2Mapping, inRow, &outRow);
        output.incRowCount();
        outRow.nextRow();

        if (output.getRowCount() == 8192)
        {
          results.push_back(result);
          result.reinit(output);
          output.setData(&result);
          output.resetRowGroup(input.getBaseRid());
          output.setDBRoot(input.getDBRoot());
          output.getRow(0, &outRow);
        }
      }
    }
  }

  if (output.getRowCount() > 0)
  {
    results.push_back(result);
  }

  rgData->swap(results);
}

void TupleHashJoinStep::sendResult(const vector<RGData>& res)
{
  boost::mutex::scoped_lock lock(outputDLLock);

  for (uint32_t i = 0; i < res.size(); i++)
    // INSERT_ADAPTER(outputDL, res[i]);
    outputDL->insert(res[i]);
}

void TupleHashJoinStep::grabSomeWork(vector<RGData>* work)
{
  boost::mutex::scoped_lock lock(inputDLLock);
  work->clear();

  if (!moreInput)
    return;

  RGData e;
  moreInput = largeDL->next(largeIt, &e);

  /* Tunable number here, but it probably won't change things much */
  for (uint32_t i = 0; i < 10 && moreInput; i++)
  {
    work->push_back(e);
    moreInput = largeDL->next(largeIt, &e);
  }

  if (moreInput)
    work->push_back(e);
}

/* This function is a port of the main join loop in TupleBPS::receiveMultiPrimitiveMessages().  Any
 * changes made here should also be made there and vice versa. */
void TupleHashJoinStep::joinOneRG(
    uint32_t threadID, vector<RGData>& out, RowGroup& inputRG, RowGroup& joinOutput, Row& largeSideRow,
    Row& joinFERow, Row& joinedRow, Row& baseRow, vector<vector<Row::Pointer> >& joinMatches,
    shared_array<Row>& smallRowTemplates, RowGroupDL* outputDL,
    // disk-join support vars.  This param list is insane; refactor attempt would be nice at some point.
    vector<boost::shared_ptr<joiner::TupleJoiner> >* tjoiners,
    boost::shared_array<boost::shared_array<int> >* rgMappings,
    boost::shared_array<boost::shared_array<int> >* feMappings,
    boost::scoped_array<boost::scoped_array<uint8_t> >* smallNullMem)
{
  /* Disk-join support.
     These dissociate the fcn from THJS's members & allow this fcn to be called from DiskJoinStep
  */
  if (!tjoiners)
    tjoiners = &joiners;

  if (!rgMappings)
    rgMappings = &columnMappings;

  if (!feMappings)
    feMappings = &fergMappings;

  if (!smallNullMem)
    smallNullMem = &smallNullMemory;

  RGData joinedData;
  uint32_t matchCount, smallSideCount = tjoiners->size();
  uint32_t j, k;

  joinedData.reinit(joinOutput);
  joinOutput.setData(&joinedData);
  joinOutput.resetRowGroup(inputRG.getBaseRid());
  joinOutput.setDBRoot(inputRG.getDBRoot());
  inputRG.getRow(0, &largeSideRow);

  // cout << "jointype = " << (*tjoiners)[0]->getJoinType() << endl;
  for (k = 0; k < inputRG.getRowCount() && !cancelled(); k++, largeSideRow.nextRow())
  {
    // cout << "THJS: Large side row: " << largeSideRow.toString() << endl;
    matchCount = 0;

    for (j = 0; j < smallSideCount; j++)
    {
      (*tjoiners)[j]->match(largeSideRow, k, threadID, &joinMatches[j]);
      /* Debugging code to print the matches
         Row r;
         smallRGs[j].initRow(&r);
         cout << joinMatches[j].size() << " matches: \n";
         for (uint32_t z = 0; z < joinMatches[j].size(); z++) {
             r.setData(joinMatches[j][z]);
             cout << "  " << r.toString() << endl;
         }
      */
      matchCount = joinMatches[j].size();

      if ((*tjoiners)[j]->hasFEFilter() && matchCount > 0)
      {
        // cout << "doing FE filter" << endl;
        vector<Row::Pointer> newJoinMatches;
        applyMapping((*feMappings)[smallSideCount], largeSideRow, &joinFERow);

        for (uint32_t z = 0; z < joinMatches[j].size(); z++)
        {
          smallRowTemplates[j].setPointer(joinMatches[j][z]);
          applyMapping((*feMappings)[j], smallRowTemplates[j], &joinFERow);

          if (!(*tjoiners)[j]->evaluateFilter(joinFERow, threadID))
            matchCount--;
          else
          {
            /* The first match includes it in a SEMI join result and excludes it from an ANTI join
             * result.  If it's SEMI & SCALAR however, it needs to continue.
             */
            newJoinMatches.push_back(joinMatches[j][z]);

            if ((*tjoiners)[j]->antiJoin() || ((*tjoiners)[j]->semiJoin() && !(*tjoiners)[j]->scalar()))
              break;
          }
        }

        // the filter eliminated all matches, need to join with the NULL row
        if (matchCount == 0 && (*tjoiners)[j]->largeOuterJoin())
        {
          newJoinMatches.clear();
          newJoinMatches.push_back(Row::Pointer((*smallNullMem)[j].get()));
          matchCount = 1;
        }

        joinMatches[j].swap(newJoinMatches);
      }

      /* If anti-join, reverse the result */
      if ((*tjoiners)[j]->antiJoin())
      {
        matchCount = (matchCount ? 0 : 1);
      }

      if (matchCount == 0)
      {
        joinMatches[j].clear();
        break;
      }
      else if (!(*tjoiners)[j]->scalar() && ((*tjoiners)[j]->semiJoin() || (*tjoiners)[j]->antiJoin()))
      {
        joinMatches[j].clear();
        joinMatches[j].push_back(Row::Pointer((*smallNullMem)[j].get()));
        matchCount = 1;
      }

      if (matchCount == 0 && (*tjoiners)[j]->innerJoin())
        break;

      if ((*tjoiners)[j]->scalar() && matchCount > 1)
      {
        errorMessage(logging::IDBErrorInfo::instance()->errorMsg(logging::ERR_MORE_THAN_1_ROW));
        status(logging::ERR_MORE_THAN_1_ROW);
        abort();
      }

      if ((*tjoiners)[j]->smallOuterJoin())
        (*tjoiners)[j]->markMatches(threadID, joinMatches[j]);
    }

    if (matchCount > 0)
    {
      /* TODO!!!  See TupleBPS for the fix for bug 3510! */
      applyMapping((*rgMappings)[smallSideCount], largeSideRow, &baseRow);
      baseRow.setRid(largeSideRow.getRelRid());
      generateJoinResultSet(joinMatches, baseRow, *rgMappings, 0, joinOutput, joinedData, out,
                            smallRowTemplates, joinedRow, outputDL);
    }
  }

  if (joinOutput.getRowCount() > 0)
    out.push_back(joinedData);
}

void TupleHashJoinStep::generateJoinResultSet(const vector<vector<Row::Pointer> >& joinerOutput, Row& baseRow,
                                              const shared_array<shared_array<int> >& mappings,
                                              const uint32_t depth, RowGroup& l_outputRG, RGData& rgData,
                                              vector<RGData>& outputData, const shared_array<Row>& smallRows,
                                              Row& joinedRow, RowGroupDL* dlp)
{
  uint32_t i;
  Row& smallRow = smallRows[depth];
  uint32_t smallSideCount = joinerOutput.size();

  if (depth < smallSideCount - 1)
  {
    for (i = 0; i < joinerOutput[depth].size(); i++)
    {
      smallRow.setPointer(joinerOutput[depth][i]);
      applyMapping(mappings[depth], smallRow, &baseRow);
      generateJoinResultSet(joinerOutput, baseRow, mappings, depth + 1, l_outputRG, rgData, outputData,
                            smallRows, joinedRow, dlp);
    }
  }
  else
  {
    l_outputRG.getRow(l_outputRG.getRowCount(), &joinedRow);

    for (i = 0; i < joinerOutput[depth].size(); i++, joinedRow.nextRow(), l_outputRG.incRowCount())
    {
      smallRow.setPointer(joinerOutput[depth][i]);

      if (UNLIKELY(l_outputRG.getRowCount() == 8192))
      {
        uint32_t dbRoot = l_outputRG.getDBRoot();
        uint64_t baseRid = l_outputRG.getBaseRid();
        outputData.push_back(rgData);
        // Count the memory
        if (UNLIKELY(!getMemory(l_outputRG.getMaxDataSize())))
        {
          // Don't let the join results buffer get out of control.
          sendResult(outputData);
          outputData.clear();
          returnMemory();
        }
        rgData.reinit(l_outputRG);
        l_outputRG.setData(&rgData);
        l_outputRG.resetRowGroup(baseRid);
        l_outputRG.setDBRoot(dbRoot);
        l_outputRG.getRow(0, &joinedRow);
      }

      applyMapping(mappings[depth], smallRow, &baseRow);
      copyRow(baseRow, &joinedRow);
    }
  }
}

void TupleHashJoinStep::segregateJoiners()
{
  uint32_t i;
  bool allInnerJoins = true;
  bool anyTooLarge = false;
  uint32_t smallSideCount = smallDLs.size();

  for (i = 0; i < smallSideCount; i++)
  {
    allInnerJoins &= (joinTypes[i] == INNER);
    anyTooLarge |= !joiners[i]->isFinished();
  }

  /* When DDL updates syscat, the syscat checks here are necessary */
  if (isDML || !allowDJS || (fSessionId & 0x80000000) || (tableOid() < 3000 && tableOid() >= 1000))
  {
    if (anyTooLarge)
    {
      joinIsTooBig = true;
      abort();
    }

    tbpsJoiners = joiners;
    return;
  }

#if 0
    // Debugging code, this makes all eligible joins disk-based.
    else {
    	cout << "making all joins disk-based" << endl;
		joinIsTooBig = true;
    	for (i = 0; i < smallSideCount; i++) {
            joiner[i]->setConvertToDiskJoin();
    		djsJoiners.push_back(joiners[i]);
    		djsJoinerMap.push_back(i);
    	}
    	return;
    }
#endif

  boost::mutex::scoped_lock sl(djsLock);
  /* For now if there is no largeBPS all joins need to either be DJS or not, not mixed */
  if (!largeBPS)
  {
    if (anyTooLarge)
    {
      joinIsTooBig = true;

      for (i = 0; i < smallSideCount; i++)
      {
        joiners[i]->setConvertToDiskJoin();
        djsJoiners.push_back(joiners[i]);
        djsJoinerMap.push_back(i);
      }
    }
    else
      tbpsJoiners = joiners;

    return;
  }

  /* If they are all inner joins they can be segregated w/o respect to
  ordering; if they're not, the ordering has to stay consistent therefore
  the first joiner that isn't finished and everything after has to be
  done by DJS. */

  if (allInnerJoins)
  {
    for (i = 0; i < smallSideCount; i++)
    {
      // if (joiners[i]->isFinished() && (rand() % 2)) {    // for debugging
      if (joiners[i]->isFinished())
      {
        // cout << "1joiner " << i << "  " << hex << (uint64_t) joiners[i].get() << dec << " -> TBPS" << endl;
        tbpsJoiners.push_back(joiners[i]);
      }
      else
      {
        joinIsTooBig = true;
        joiners[i]->setConvertToDiskJoin();
        // cout << "1joiner " << i << "  " << hex << (uint64_t) joiners[i].get() << dec << " -> DJS" << endl;
        djsJoiners.push_back(joiners[i]);
        djsJoinerMap.push_back(i);
      }
    }
  }
  else
  {
    // uint limit = rand() % smallSideCount;
    for (i = 0; i < smallSideCount; i++)
    {
      // if (joiners[i]->isFinished() && i < limit) {  // debugging
      if (joiners[i]->isFinished())
      {
        // cout << "2joiner " << i << "  " << hex << (uint64_t) joiners[i].get() << dec << " -> TBPS" << endl;
        tbpsJoiners.push_back(joiners[i]);
      }
      else
        break;
    }

    for (; i < smallSideCount; i++)
    {
      joinIsTooBig = true;
      joiners[i]->setConvertToDiskJoin();
      // cout << "2joiner " << i << "  " << hex << (uint64_t) joiners[i].get() << dec << " -> DJS" << endl;
      djsJoiners.push_back(joiners[i]);
      djsJoinerMap.push_back(i);
    }
  }
}

void TupleHashJoinStep::abort()
{
  JobStep::abort();
  boost::mutex::scoped_lock sl(djsLock);

  if (djs)
  {
    for (uint32_t i = 0; i < djsJoiners.size(); i++)
      djs[i].abort();
  }
}

}  // namespace joblist
// vim:ts=4 sw=4:

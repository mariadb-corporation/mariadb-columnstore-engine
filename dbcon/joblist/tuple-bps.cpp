/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019-2020 MariaDB Corporation

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

//  $Id: tuple-bps.cpp 9705 2013-07-17 20:06:07Z pleblanc $

#include <unistd.h>
//#define NDEBUG
#include <cassert>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <ctime>
#include <sys/time.h>
#include <deque>
using namespace std;

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include <boost/uuid/uuid_io.hpp>
using namespace boost;

#include "bpp-jl.h"
#include "distributedenginecomm.h"
#include "elementtype.h"
#include "jlf_common.h"
#include "primitivestep.h"
#include "unique32generator.h"
#include "rowestimator.h"
using namespace joblist;

#include "messagequeue.h"
using namespace messageqcpp;

#include "configcpp.h"
using namespace config;

#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
#include "errorcodes.h"
#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "liboamcpp.h"

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "brm.h"
using namespace BRM;

#include "oamcache.h"

#include "rowgroup.h"
using namespace rowgroup;

#include "threadnaming.h"

#include "querytele.h"
using namespace querytele;

#include "columnwidth.h"
#include "pseudocolumn.h"
//#define DEBUG 1

extern boost::mutex fileLock_g;

namespace
{
const uint32_t LOGICAL_EXTENT_CONVERTER = 10;  // 10 + 13.  13 to convert to logical blocks,
// 10 to convert to groups of 1024 logical blocks
const uint32_t DEFAULT_EXTENTS_PER_SEG_FILE = 2;

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
struct TupleBPSPrimitive
{
  TupleBPSPrimitive(TupleBPS* batchPrimitiveStep) : fBatchPrimitiveStep(batchPrimitiveStep)
  {
  }
  TupleBPS* fBatchPrimitiveStep;
  void operator()()
  {
    try
    {
      utils::setThreadName("BPSPrimitive");
      fBatchPrimitiveStep->sendPrimitiveMessages();
    }
    catch (std::exception& re)
    {
      cerr << "TupleBPS: send thread threw an exception: " << re.what() << "\t" << this << endl;
      catchHandler(re.what(), ERR_TUPLE_BPS, fBatchPrimitiveStep->errorInfo());
    }
    catch (...)
    {
      string msg("TupleBPS: send thread threw an unknown exception ");
      catchHandler(msg, ERR_TUPLE_BPS, fBatchPrimitiveStep->errorInfo());
      cerr << msg << this << endl;
    }
  }
};

struct TupleBPSAggregators
{
  TupleBPSAggregators(TupleBPS* batchPrimitiveStep) : fBatchPrimitiveStepCols(batchPrimitiveStep)
  {
  }
  TupleBPS* fBatchPrimitiveStepCols;

  void operator()()
  {
    try
    {
      utils::setThreadName("BPSAggregator");
      fBatchPrimitiveStepCols->receiveMultiPrimitiveMessages();
    }
    catch (std::exception& re)
    {
      cerr << fBatchPrimitiveStepCols->toString() << ": receive thread threw an exception: " << re.what()
           << endl;
      catchHandler(re.what(), ERR_TUPLE_BPS, fBatchPrimitiveStepCols->errorInfo());
    }
    catch (...)
    {
      string msg("TupleBPS: recv thread threw an unknown exception ");
      cerr << fBatchPrimitiveStepCols->toString() << msg << endl;
      catchHandler(msg, ERR_TUPLE_BPS, fBatchPrimitiveStepCols->errorInfo());
    }
  }
};

TupleBPS::JoinLocalData::JoinLocalData(TupleBPS* pTupleBPS, RowGroup& primRowGroup, RowGroup& outputRowGroup,
                                       boost::shared_ptr<funcexp::FuncExpWrapper>& fe2,
                                       rowgroup::RowGroup& fe2Output,
                                       std::vector<rowgroup::RowGroup>& joinerMatchesRGs,
                                       rowgroup::RowGroup& joinFERG,
                                       std::vector<std::shared_ptr<joiner::TupleJoiner>>& tjoiners,
                                       uint32_t smallSideCount, bool doJoin)
 : tbps(pTupleBPS)
 , local_primRG(primRowGroup)
 , local_outputRG(outputRowGroup)
 , fe2(fe2)
 , fe2Output(fe2Output)
 , joinerMatchesRGs(joinerMatchesRGs)
 , joinFERG(joinFERG)
 , tjoiners(tjoiners)
 , smallSideCount(smallSideCount)
 , doJoin(doJoin)
{
  if (doJoin || fe2)
  {
    local_outputRG.initRow(&postJoinRow);
  }

  if (fe2)
  {
    local_fe2Output = fe2Output;
    local_fe2Output.initRow(&local_fe2OutRow);
    local_fe2Data.reinit(fe2Output);
    local_fe2Output.setData(&local_fe2Data);
    local_fe2 = *fe2;
  }

  if (doJoin)
  {
    joinerOutput.resize(smallSideCount);
    smallSideRows.reset(new Row[smallSideCount]);
    smallNulls.reset(new Row[smallSideCount]);
    smallMappings.resize(smallSideCount);
    fergMappings.resize(smallSideCount + 1);
    smallNullMemory.reset(new std::shared_ptr<uint8_t[]>[smallSideCount]);
    local_primRG.initRow(&largeSideRow);
    local_outputRG.initRow(&joinedBaseRow, true);
    joinedBaseRowData.reset(new uint8_t[joinedBaseRow.getSize()]);
    joinedBaseRow.setData(rowgroup::Row::Pointer(joinedBaseRowData.get()));
    joinedBaseRow.initToNull();
    largeMapping = makeMapping(local_primRG, local_outputRG);

    bool hasJoinFE = false;

    for (uint32_t i = 0; i < smallSideCount; ++i)
    {
      joinerMatchesRGs[i].initRow(&(smallSideRows[i]));
      smallMappings[i] = makeMapping(joinerMatchesRGs[i], local_outputRG);

      if (tjoiners[i]->hasFEFilter())
      {
        fergMappings[i] = makeMapping(joinerMatchesRGs[i], joinFERG);
        hasJoinFE = true;
      }
    }

    if (hasJoinFE)
    {
      joinFERG.initRow(&joinFERow, true);
      joinFERowData.reset(new uint8_t[joinFERow.getSize()]);
      memset(joinFERowData.get(), 0, joinFERow.getSize());
      joinFERow.setData(rowgroup::Row::Pointer(joinFERowData.get()));
      fergMappings[smallSideCount] = makeMapping(local_primRG, joinFERG);
    }

    for (uint32_t i = 0; i < smallSideCount; ++i)
    {
      joinerMatchesRGs[i].initRow(&(smallNulls[i]), true);
      smallNullMemory[i].reset(new uint8_t[smallNulls[i].getSize()]);
      smallNulls[i].setData(rowgroup::Row::Pointer(smallNullMemory[i].get()));
      smallNulls[i].initToNull();
    }

    local_primRG.initRow(&largeNull, true);
    largeNullMemory.reset(new uint8_t[largeNull.getSize()]);
    largeNull.setData(rowgroup::Row::Pointer(largeNullMemory.get()));
    largeNull.initToNull();
  }
}

uint64_t TupleBPS::JoinLocalData::generateJoinResultSet(const uint32_t depth,
                                                        std::vector<rowgroup::RGData>& outputData,
                                                        RowGroupDL* dlp)
{
  uint32_t i;
  Row& smallRow = smallSideRows[depth];
  uint64_t memSizeForOutputRG = 0;

  if (depth < smallSideCount - 1)
  {
    for (i = 0; i < joinerOutput[depth].size() && !tbps->cancelled(); i++)
    {
      smallRow.setPointer(joinerOutput[depth][i]);
      applyMapping(smallMappings[depth], smallRow, &joinedBaseRow);
      memSizeForOutputRG += generateJoinResultSet(depth + 1, outputData, dlp);
    }
  }
  else
  {
    local_outputRG.getRow(local_outputRG.getRowCount(), &postJoinRow);

    for (i = 0; i < joinerOutput[depth].size() && !tbps->cancelled();
         i++, postJoinRow.nextRow(), local_outputRG.incRowCount())
    {
      smallRow.setPointer(joinerOutput[depth][i]);

      if (UNLIKELY(local_outputRG.getRowCount() == 8192))
      {
        uint32_t dbRoot = local_outputRG.getDBRoot();
        uint64_t baseRid = local_outputRG.getBaseRid();
        outputData.push_back(joinedData);
        // Don't let the join results buffer get out of control.
        if (tbps->resourceManager()->getMemory(local_outputRG.getMaxDataSize(), false))
        {
          memSizeForOutputRG += local_outputRG.getMaxDataSize();
        }
        else
        {
          // Don't wait for memory, just send the data on to DL.
          RowGroup out(local_outputRG);
          if (fe2 && !tbps->runFEonPM())
          {
            processFE2(outputData);
            tbps->rgDataVecToDl(outputData, local_fe2Output, dlp);
          }
          else
          {
            tbps->rgDataVecToDl(outputData, out, dlp);
          }
          tbps->resourceManager()->returnMemory(memSizeForOutputRG);
          memSizeForOutputRG = 0;
        }
        joinedData.reinit(local_outputRG);
        local_outputRG.setData(&joinedData);
        local_outputRG.resetRowGroup(baseRid);
        local_outputRG.setDBRoot(dbRoot);
        local_outputRG.getRow(0, &postJoinRow);
      }

      applyMapping(smallMappings[depth], smallRow, &joinedBaseRow);
      copyRow(joinedBaseRow, &postJoinRow);
    }
  }
  return memSizeForOutputRG;
}

void TupleBPS::JoinLocalData::processFE2(vector<rowgroup::RGData>& rgData)
{
  vector<RGData> results;
  RGData result;
  uint32_t i, j;
  bool ret;

  result = RGData(local_fe2Output);
  local_fe2Output.setData(&result);
  local_fe2Output.resetRowGroup(-1);
  local_fe2Output.getRow(0, &local_fe2OutRow);

  for (i = 0; i < rgData.size(); i++)
  {
    local_outputRG.setData(&(rgData)[i]);

    if (local_fe2Output.getRowCount() == 0)
    {
      local_fe2Output.resetRowGroup(local_outputRG.getBaseRid());
      local_fe2Output.setDBRoot(local_outputRG.getDBRoot());
    }

    local_outputRG.getRow(0, &postJoinRow);

    for (j = 0; j < local_outputRG.getRowCount(); j++, postJoinRow.nextRow())
    {
      ret = local_fe2.evaluate(&postJoinRow);

      if (ret)
      {
        applyMapping(tbps->fe2Mapping, postJoinRow, &local_fe2OutRow);
        local_fe2OutRow.setRid(postJoinRow.getRelRid());
        local_fe2Output.incRowCount();
        local_fe2OutRow.nextRow();

        if (local_fe2Output.getRowCount() == 8192 ||
            local_fe2Output.getDBRoot() != local_outputRG.getDBRoot() ||
            local_fe2Output.getBaseRid() != local_outputRG.getBaseRid())
        {
          results.push_back(result);
          result = RGData(local_fe2Output);
          local_fe2Output.setData(&result);
          local_fe2Output.resetRowGroup(local_outputRG.getBaseRid());
          local_fe2Output.setDBRoot(local_outputRG.getDBRoot());
          local_fe2Output.getRow(0, &local_fe2OutRow);
        }
      }
    }
  }

  if (local_fe2Output.getRowCount() > 0)
  {
    results.push_back(result);
  }

  rgData.swap(results);
}

struct ByteStreamProcessor
{
  ByteStreamProcessor(TupleBPS* tbps, vector<boost::shared_ptr<messageqcpp::ByteStream>>& bsv,
                      const uint32_t begin, const uint32_t end, vector<_CPInfo>& cpv, RowGroupDL* dlp,
                      const uint32_t threadID)
   : tbps(tbps), bsv(bsv), begin(begin), end(end), cpv(cpv), dlp(dlp), threadID(threadID)
  {
  }

  TupleBPS* tbps;
  vector<boost::shared_ptr<messageqcpp::ByteStream>>& bsv;
  uint32_t begin;
  uint32_t end;
  vector<_CPInfo>& cpv;
  RowGroupDL* dlp;
  uint32_t threadID;

  void operator()()
  {
    utils::setThreadName("ByteStreamProcessor");
    tbps->processByteStreamVector(bsv, begin, end, cpv, dlp, threadID);
  }
};

//------------------------------------------------------------------------------
// Initialize configurable parameters
//------------------------------------------------------------------------------
void TupleBPS::initializeConfigParms()
{
  string strVal;

  //...Get the tuning parameters that throttle msgs sent to primproc
  //...fFilterRowReqLimit puts a cap on how many rids we will request from
  //...    primproc, before pausing to let the consumer thread catch up.
  //...    Without this limit, there is a chance that PrimProc could flood
  //...    ExeMgr with thousands of messages that will consume massive
  //...    amounts of memory for a 100 gigabyte database.
  //...fFilterRowReqThreshold is the level at which the number of outstanding
  //...    rids must fall below, before the producer can send more rids.

  // These could go in constructor
  fRequestSize = fRm->getJlRequestSize();
  fMaxOutstandingRequests = fRm->getJlMaxOutstandingRequests();
  fProcessorThreadsPerScan = fRm->getJlProcessorThreadsPerScan();
  fNumThreads = 0;

  fExtentsPerSegFile = DEFAULT_EXTENTS_PER_SEG_FILE;

  if (fRequestSize >= fMaxOutstandingRequests)
    fRequestSize = 1;

  if ((fSessionId & 0x80000000) == 0)
  {
    fMaxNumThreads = fRm->getJlNumScanReceiveThreads();
    fMaxNumProcessorThreads = fMaxNumThreads;
  }
  else
  {
    fMaxNumThreads = 1;
    fMaxNumProcessorThreads = 1;
  }

  // Reserve the max number of thread space. A bit of an optimization.
  fProducerThreads.clear();
  fProducerThreads.reserve(fMaxNumThreads);
}

TupleBPS::TupleBPS(const pColStep& rhs, const JobInfo& jobInfo)
 : BatchPrimitive(jobInfo), pThread(0), fRm(jobInfo.rm)
{
  fInputJobStepAssociation = rhs.inputAssociation();
  fOutputJobStepAssociation = rhs.outputAssociation();
  fDec = 0;
  fSessionId = rhs.sessionId();
  fFilterCount = rhs.filterCount();
  fFilterString = rhs.filterString();
  isFilterFeeder = rhs.getFeederFlag();
  fOid = rhs.oid();
  fTableOid = rhs.tableOid();
  extentSize = rhs.extentSize;

  scannedExtents = rhs.extents;
  extentsMap[fOid] = tr1::unordered_map<int64_t, EMEntry>();
  tr1::unordered_map<int64_t, EMEntry>& ref = extentsMap[fOid];

  for (uint32_t z = 0; z < rhs.extents.size(); z++)
    ref[rhs.extents[z].range.start] = rhs.extents[z];

  lbidList = rhs.lbidList;
  rpbShift = rhs.rpbShift;
  divShift = rhs.divShift;
  modMask = rhs.modMask;
  numExtents = rhs.numExtents;
  ridsRequested = 0;
  ridsReturned = 0;
  recvExited = 0;
  totalMsgs = 0;
  msgsSent = 0;
  msgsRecvd = 0;
  fMsgBytesIn = 0;
  fMsgBytesOut = 0;
  fBlockTouched = 0;
  fExtentsPerSegFile = DEFAULT_EXTENTS_PER_SEG_FILE;
  recvWaiting = 0;
  fStepCount = 1;
  fCPEvaluated = false;
  fEstimatedRows = 0;
  fColType = rhs.colType();
  alias(rhs.alias());
  view(rhs.view());
  name(rhs.name());
  fColWidth = fColType.colWidth;
  fBPP.reset(new BatchPrimitiveProcessorJL(fRm));
  initializeConfigParms();
  fBPP->setSessionID(fSessionId);
  fBPP->setStepID(fStepId);
  fBPP->setQueryContext(fVerId);
  fBPP->setTxnID(fTxnId);
  fTraceFlags = rhs.fTraceFlags;
  fBPP->setTraceFlags(fTraceFlags);
  fBPP->setOutputType(ROW_GROUP);
  finishedSending = sendWaiting = false;
  fNumBlksSkipped = 0;
  fPhysicalIO = 0;
  fCacheIO = 0;
  BPPIsAllocated = false;
  uniqueID = UniqueNumberGenerator::instance()->getUnique32();
  fBPP->setUniqueID(uniqueID);
  fBPP->setUuid(fStepUuid);
  fCardinality = rhs.cardinality();
  doJoin = false;
  hasPMJoin = false;
  hasUMJoin = false;
  fRunExecuted = false;
  fSwallowRows = false;
  smallOuterJoiner = -1;
  hasAuxCol = false;

  // @1098 initialize scanFlags to be true
  scanFlags.assign(numExtents, true);
  runtimeCPFlags.assign(numExtents, true);
  bop = BOP_AND;

  runRan = joinRan = false;
  fDelivery = false;
  fExtendedInfo = "TBPS: ";
  fQtc.stepParms().stepType = StepTeleStats::T_BPS;

  hasPCFilter = hasPMFilter = hasRIDFilter = hasSegmentFilter = hasDBRootFilter = hasSegmentDirFilter =
      hasPartitionFilter = hasMaxFilter = hasMinFilter = hasLBIDFilter = hasExtentIDFilter = false;
}

TupleBPS::TupleBPS(const pColScanStep& rhs, const JobInfo& jobInfo) : BatchPrimitive(jobInfo), fRm(jobInfo.rm)
{
  fInputJobStepAssociation = rhs.inputAssociation();
  fOutputJobStepAssociation = rhs.outputAssociation();
  fDec = 0;
  fFilterCount = rhs.filterCount();
  fFilterString = rhs.filterString();
  isFilterFeeder = rhs.getFeederFlag();
  fOid = rhs.oid();
  fTableOid = rhs.tableOid();
  extentSize = rhs.extentSize;
  lbidRanges = rhs.lbidRanges;
  hasAuxCol = false;
  execplan::CalpontSystemCatalog::TableName tableName;

  if (fTableOid >= 3000)
  {
    try
    {
      tableName = jobInfo.csc->tableName(fTableOid);
      fOidAux = jobInfo.csc->tableAUXColumnOID(tableName);
    }
    catch (logging::IDBExcept& ie)
    {
      std::ostringstream oss;

      if (ie.errorCode() == logging::ERR_TABLE_NOT_IN_CATALOG)
      {
        oss << "Table " << tableName.toString();
        oss << " does not exist in the system catalog.";
      }
      else
      {
        oss << "Error getting AUX column OID for table " << tableName.toString();
        oss << " due to:  " << ie.what();
      }

      throw runtime_error(oss.str());
    }
    catch(std::exception& ex)
    {
      std::ostringstream oss;
      oss << "Error getting AUX column OID for table " << tableName.toString();
      oss << " due to:  " << ex.what();
      throw runtime_error(oss.str());
    }
    catch(...)
    {
      std::ostringstream oss;
      oss << "Error getting AUX column OID for table " << tableName.toString();
      throw runtime_error(oss.str());
    }

    if (fOidAux > 3000)
    {
      hasAuxCol = true;

      if (dbrm.getExtents(fOidAux, extentsAux))
        throw runtime_error("TupleBPS::TupleBPS BRM extent lookup failure (1)");

      sort(extentsAux.begin(), extentsAux.end(), BRM::ExtentSorter());

      tr1::unordered_map<int64_t, EMEntry>& refAux = extentsMap[fOidAux];

      for (uint32_t z = 0; z < extentsAux.size(); z++)
        refAux[extentsAux[z].range.start] = extentsAux[z];
    }
  }

  /* These lines are obsoleted by initExtentMarkers.  Need to remove & retest. */
  scannedExtents = rhs.extents;
  tr1::unordered_map<int64_t, EMEntry>& ref = extentsMap[fOid];

  for (uint32_t z = 0; z < rhs.extents.size(); z++)
    ref[rhs.extents[z].range.start] = rhs.extents[z];

  divShift = rhs.divShift;
  totalMsgs = 0;
  msgsSent = 0;
  msgsRecvd = 0;
  ridsReturned = 0;
  ridsRequested = 0;
  fNumBlksSkipped = 0;
  fMsgBytesIn = 0;
  fMsgBytesOut = 0;
  fBlockTouched = 0;
  fExtentsPerSegFile = DEFAULT_EXTENTS_PER_SEG_FILE;
  recvWaiting = 0;
  fSwallowRows = false;
  fStepCount = 1;
  fCPEvaluated = false;
  fEstimatedRows = 0;
  fColType = rhs.colType();
  alias(rhs.alias());
  view(rhs.view());
  name(rhs.name());

  fColWidth = fColType.colWidth;
  lbidList = rhs.lbidList;

  finishedSending = sendWaiting = false;
  firstRead = true;
  fSwallowRows = false;
  recvExited = 0;
  fBPP.reset(new BatchPrimitiveProcessorJL(fRm));
  initializeConfigParms();
  fBPP->setSessionID(fSessionId);
  fBPP->setQueryContext(fVerId);
  fBPP->setTxnID(fTxnId);
  fTraceFlags = rhs.fTraceFlags;
  fBPP->setTraceFlags(fTraceFlags);
  fBPP->setStepID(fStepId);
  fBPP->setOutputType(ROW_GROUP);
  fPhysicalIO = 0;
  fCacheIO = 0;
  BPPIsAllocated = false;
  uniqueID = UniqueNumberGenerator::instance()->getUnique32();
  fBPP->setUniqueID(uniqueID);
  fBPP->setUuid(fStepUuid);
  fCardinality = rhs.cardinality();
  doJoin = false;
  hasPMJoin = false;
  hasUMJoin = false;
  fRunExecuted = false;
  smallOuterJoiner = -1;
  bop = BOP_AND;

  runRan = joinRan = false;
  fDelivery = false;
  fExtendedInfo = "TBPS: ";

  initExtentMarkers();
  fQtc.stepParms().stepType = StepTeleStats::T_BPS;

  hasPCFilter = hasPMFilter = hasRIDFilter = hasSegmentFilter = hasDBRootFilter = hasSegmentDirFilter =
      hasPartitionFilter = hasMaxFilter = hasMinFilter = hasLBIDFilter = hasExtentIDFilter = false;
}

TupleBPS::TupleBPS(const PassThruStep& rhs, const JobInfo& jobInfo) : BatchPrimitive(jobInfo), fRm(jobInfo.rm)
{
  fInputJobStepAssociation = rhs.inputAssociation();
  fOutputJobStepAssociation = rhs.outputAssociation();
  fDec = 0;
  fFilterCount = 0;
  fOid = rhs.oid();
  fTableOid = rhs.tableOid();
  ridsReturned = 0;
  ridsRequested = 0;
  fMsgBytesIn = 0;
  fMsgBytesOut = 0;
  fBlockTouched = 0;
  fExtentsPerSegFile = DEFAULT_EXTENTS_PER_SEG_FILE;
  recvExited = 0;
  totalMsgs = 0;
  msgsSent = 0;
  msgsRecvd = 0;
  recvWaiting = 0;
  fStepCount = 1;
  fCPEvaluated = false;
  fEstimatedRows = 0;
  fColType = rhs.colType();
  alias(rhs.alias());
  view(rhs.view());
  name(rhs.name());
  fColWidth = fColType.colWidth;
  fBPP.reset(new BatchPrimitiveProcessorJL(fRm));
  initializeConfigParms();
  fBPP->setSessionID(fSessionId);
  fBPP->setStepID(fStepId);
  fBPP->setQueryContext(fVerId);
  fBPP->setTxnID(fTxnId);
  fTraceFlags = rhs.fTraceFlags;
  fBPP->setTraceFlags(fTraceFlags);
  fBPP->setOutputType(ROW_GROUP);
  finishedSending = sendWaiting = false;
  fSwallowRows = false;
  fNumBlksSkipped = 0;
  fPhysicalIO = 0;
  fCacheIO = 0;
  BPPIsAllocated = false;
  uniqueID = UniqueNumberGenerator::instance()->getUnique32();
  fBPP->setUniqueID(uniqueID);
  fBPP->setUuid(fStepUuid);
  doJoin = false;
  hasPMJoin = false;
  hasUMJoin = false;
  fRunExecuted = false;
  isFilterFeeder = false;
  smallOuterJoiner = -1;
  hasAuxCol = false;

  // @1098 initialize scanFlags to be true
  scanFlags.assign(numExtents, true);
  runtimeCPFlags.assign(numExtents, true);
  bop = BOP_AND;

  runRan = joinRan = false;
  fDelivery = false;
  fExtendedInfo = "TBPS: ";
  fQtc.stepParms().stepType = StepTeleStats::T_BPS;

  hasPCFilter = hasPMFilter = hasRIDFilter = hasSegmentFilter = hasDBRootFilter = hasSegmentDirFilter =
      hasPartitionFilter = hasMaxFilter = hasMinFilter = hasLBIDFilter = hasExtentIDFilter = false;
}

TupleBPS::TupleBPS(const pDictionaryStep& rhs, const JobInfo& jobInfo)
 : BatchPrimitive(jobInfo), fRm(jobInfo.rm)
{
  fInputJobStepAssociation = rhs.inputAssociation();
  fOutputJobStepAssociation = rhs.outputAssociation();
  fDec = 0;
  fOid = rhs.oid();
  fTableOid = rhs.tableOid();
  totalMsgs = 0;
  msgsSent = 0;
  msgsRecvd = 0;
  ridsReturned = 0;
  ridsRequested = 0;
  fNumBlksSkipped = 0;
  fBlockTouched = 0;
  fMsgBytesIn = 0;
  fMsgBytesOut = 0;
  fExtentsPerSegFile = DEFAULT_EXTENTS_PER_SEG_FILE;
  recvWaiting = 0;
  fSwallowRows = false;
  fStepCount = 1;
  fCPEvaluated = false;
  fEstimatedRows = 0;
  alias(rhs.alias());
  view(rhs.view());
  name(rhs.name());
  finishedSending = sendWaiting = false;
  recvExited = 0;
  fBPP.reset(new BatchPrimitiveProcessorJL(fRm));
  initializeConfigParms();
  fBPP->setSessionID(fSessionId);
  fBPP->setStepID(fStepId);
  fBPP->setQueryContext(fVerId);
  fBPP->setTxnID(fTxnId);
  fTraceFlags = rhs.fTraceFlags;
  fBPP->setTraceFlags(fTraceFlags);
  fBPP->setOutputType(ROW_GROUP);
  fPhysicalIO = 0;
  fCacheIO = 0;
  BPPIsAllocated = false;
  uniqueID = UniqueNumberGenerator::instance()->getUnique32();
  fBPP->setUniqueID(uniqueID);
  fBPP->setUuid(fStepUuid);
  fCardinality = rhs.cardinality();
  doJoin = false;
  hasPMJoin = false;
  hasUMJoin = false;
  fRunExecuted = false;
  isFilterFeeder = false;
  smallOuterJoiner = -1;
  // @1098 initialize scanFlags to be true
  scanFlags.assign(numExtents, true);
  runtimeCPFlags.assign(numExtents, true);
  bop = BOP_AND;
  hasAuxCol = false;

  runRan = joinRan = false;
  fDelivery = false;
  fExtendedInfo = "TBPS: ";
  fQtc.stepParms().stepType = StepTeleStats::T_BPS;

  hasPCFilter = hasPMFilter = hasRIDFilter = hasSegmentFilter = hasDBRootFilter = hasSegmentDirFilter =
      hasPartitionFilter = hasMaxFilter = hasMinFilter = hasLBIDFilter = hasExtentIDFilter = false;
}

TupleBPS::~TupleBPS()
{
  if (fDec)
  {
    fDec->removeDECEventListener(this);

    if (BPPIsAllocated)
    {
      SBS sbs{new ByteStream()};
      fBPP->destroyBPP(*sbs);

      try
      {
        fDec->write(uniqueID, sbs);
      }
      catch (const std::exception& e)
      {
        // log the exception
        cerr << "~TupleBPS caught: " << e.what() << endl;
        catchHandler(e.what(), ERR_TUPLE_BPS, fErrorInfo, fSessionId);
      }
      catch (...)
      {
        cerr << "~TupleBPS caught unknown exception" << endl;
        catchHandler("~TupleBPS caught unknown exception", ERR_TUPLE_BPS, fErrorInfo, fSessionId);
      }
    }

    fDec->removeQueue(uniqueID);
  }
}

void TupleBPS::setBPP(JobStep* jobStep)
{
  fCardinality = jobStep->cardinality();

  pColStep* pcsp = dynamic_cast<pColStep*>(jobStep);

  int colWidth = 0;

  if (pcsp != 0)
  {
    PseudoColStep* pseudo = dynamic_cast<PseudoColStep*>(jobStep);

    if (pseudo)
    {
      fBPP->addFilterStep(*pseudo);

      if (pseudo->filterCount() > 0)
      {
        hasPCFilter = true;

        switch (pseudo->pseudoColumnId())
        {
          case PSEUDO_EXTENTRELATIVERID: hasRIDFilter = true; break;

          case PSEUDO_DBROOT: hasDBRootFilter = true; break;

          case PSEUDO_PM: hasPMFilter = true; break;

          case PSEUDO_SEGMENT: hasSegmentFilter = true; break;

          case PSEUDO_SEGMENTDIR: hasSegmentDirFilter = true; break;

          case PSEUDO_EXTENTMIN: hasMinFilter = true; break;

          case PSEUDO_EXTENTMAX: hasMaxFilter = true; break;

          case PSEUDO_BLOCKID: hasLBIDFilter = true; break;

          case PSEUDO_EXTENTID: hasExtentIDFilter = true; break;

          case PSEUDO_PARTITION: hasPartitionFilter = true; break;
        }
      }
    }
    else
      fBPP->addFilterStep(*pcsp);

    extentsMap[pcsp->fOid] = tr1::unordered_map<int64_t, EMEntry>();
    tr1::unordered_map<int64_t, EMEntry>& ref = extentsMap[pcsp->fOid];

    for (uint32_t z = 0; z < pcsp->extents.size(); z++)
      ref[pcsp->extents[z].range.start] = pcsp->extents[z];

    colWidth = (pcsp->colType()).colWidth;
    isFilterFeeder = pcsp->getFeederFlag();

    // it17 does not allow combined AND/OR, this pcolstep is for hashjoin optimization.
    if (bop == BOP_OR && isFilterFeeder == false)
      fBPP->setForHJ(true);
  }
  else
  {
    pColScanStep* pcss = dynamic_cast<pColScanStep*>(jobStep);

    if (pcss != 0)
    {
      fBPP->addFilterStep(*pcss, lastScannedLBID, hasAuxCol, extentsAux, fOidAux);

      extentsMap[pcss->fOid] = tr1::unordered_map<int64_t, EMEntry>();
      tr1::unordered_map<int64_t, EMEntry>& ref = extentsMap[pcss->fOid];

      for (uint32_t z = 0; z < pcss->extents.size(); z++)
        ref[pcss->extents[z].range.start] = pcss->extents[z];

      colWidth = (pcss->colType()).colWidth;
      isFilterFeeder = pcss->getFeederFlag();
    }
    else
    {
      pDictionaryStep* pdsp = dynamic_cast<pDictionaryStep*>(jobStep);

      if (pdsp != 0)
      {
        fBPP->addFilterStep(*pdsp);
        colWidth = (pdsp->colType()).colWidth;
      }
      else
      {
        FilterStep* pfsp = dynamic_cast<FilterStep*>(jobStep);

        if (pfsp)
        {
          fBPP->addFilterStep(*pfsp);
        }
      }
    }
  }

  if (colWidth > fColWidth)
  {
    fColWidth = colWidth;
  }
}

void TupleBPS::setProjectBPP(JobStep* jobStep1, JobStep* jobStep2)
{
  int colWidth = 0;

  if (jobStep2 != NULL)
  {
    pDictionaryStep* pdsp = 0;
    pColStep* pcsp = dynamic_cast<pColStep*>(jobStep1);

    if (pcsp != 0)
    {
      pdsp = dynamic_cast<pDictionaryStep*>(jobStep2);
      fBPP->addProjectStep(*pcsp, *pdsp);

      //@Bug 961
      if (!pcsp->isExeMgr())
        fBPP->setNeedRidsAtDelivery(true);

      colWidth = (pcsp->colType()).colWidth;
      projectOids.push_back(jobStep1->oid());
    }
    else
    {
      PassThruStep* psth = dynamic_cast<PassThruStep*>(jobStep1);

      if (psth != 0)
      {
        pdsp = dynamic_cast<pDictionaryStep*>(jobStep2);
        fBPP->addProjectStep(*psth, *pdsp);

        //@Bug 961
        if (!psth->isExeMgr())
          fBPP->setNeedRidsAtDelivery(true);

        projectOids.push_back(jobStep1->oid());
        colWidth = (psth->colType()).colWidth;
      }
    }
  }
  else
  {
    pColStep* pcsp = dynamic_cast<pColStep*>(jobStep1);

    if (pcsp != 0)
    {
      PseudoColStep* pseudo = dynamic_cast<PseudoColStep*>(jobStep1);

      if (pseudo)
      {
        fBPP->addProjectStep(*pseudo);
      }
      else
        fBPP->addProjectStep(*pcsp);

      extentsMap[pcsp->fOid] = tr1::unordered_map<int64_t, EMEntry>();
      tr1::unordered_map<int64_t, EMEntry>& ref = extentsMap[pcsp->fOid];

      for (uint32_t z = 0; z < pcsp->extents.size(); z++)
        ref[pcsp->extents[z].range.start] = pcsp->extents[z];

      //@Bug 961
      if (!pcsp->isExeMgr())
        fBPP->setNeedRidsAtDelivery(true);

      colWidth = (pcsp->colType()).colWidth;
      projectOids.push_back(jobStep1->oid());
    }
    else
    {
      PassThruStep* passthru = dynamic_cast<PassThruStep*>(jobStep1);

      if (passthru != 0)
      {
        idbassert(!fBPP->getFilterSteps().empty());

        if (static_cast<CalpontSystemCatalog::OID>(fBPP->getFilterSteps().back()->getOID()) !=
            passthru->oid())
        {
          SJSTEP pts;

          if (passthru->pseudoType() == 0)
          {
            pts.reset(new pColStep(*passthru));
            pcsp = dynamic_cast<pColStep*>(pts.get());
          }
          else
          {
            pts.reset(new PseudoColStep(*passthru));
            pcsp = dynamic_cast<PseudoColStep*>(pts.get());
          }

          fBPP->addProjectStep(*pcsp);

          if (!passthru->isExeMgr())
            fBPP->setNeedRidsAtDelivery(true);

          colWidth = passthru->colType().colWidth;
          projectOids.push_back(pts->oid());
        }
        else
        {
          fBPP->addProjectStep(*passthru);

          //@Bug 961
          if (!passthru->isExeMgr())
            fBPP->setNeedRidsAtDelivery(true);

          colWidth = (passthru->colType()).colWidth;
          projectOids.push_back(jobStep1->oid());
        }
      }
    }
  }

  if (colWidth > fColWidth)
  {
    fColWidth = colWidth;
  }
}

void TupleBPS::storeCasualPartitionInfo(const bool estimateRowCounts)
{
  const vector<SCommand>& colCmdVec = fBPP->getFilterSteps();
  vector<ColumnCommandJL*> cpColVec;
  vector<SP_LBIDList> lbidListVec;
  ColumnCommandJL* colCmd = 0;
  bool defaultScanFlag = true;

  // @bug 2123.  We call this earlier in the process for the hash join estimation process now.  Return if
  // we've already done the work.
  if (fCPEvaluated)
  {
    return;
  }

  fCPEvaluated = true;

  if (colCmdVec.size() == 0)
  {
    defaultScanFlag = false;  // no reason to scan if there are no commands.
  }

  for (uint32_t i = 0; i < colCmdVec.size(); i++)
  {
    colCmd = dynamic_cast<ColumnCommandJL*>(colCmdVec[i].get());

    if (!colCmd || dynamic_cast<PseudoCCJL*>(colCmdVec[i].get()))
      continue;

    SP_LBIDList tmplbidList(new LBIDList(0));

    if (tmplbidList->CasualPartitionDataType(colCmd->getColType().colDataType, colCmd->getColType().colWidth))
    {
      lbidListVec.push_back(tmplbidList);
      cpColVec.push_back(colCmd);
    }

    // @Bug 3503. Use the total table size as the estimate for non CP columns.
    else if (fEstimatedRows == 0 && estimateRowCounts)
    {
      RowEstimator rowEstimator;
      fEstimatedRows = rowEstimator.estimateRowsForNonCPColumn(*colCmd);
    }
  }

  if (cpColVec.size() == 0)
  {
    defaultScanFlag = true;  // no reason to scan if there are no predicates to evaluate.
  }

  const bool ignoreCP = ((fTraceFlags & CalpontSelectExecutionPlan::IGNORE_CP) != 0);

  for (uint32_t idx = 0; idx < numExtents; idx++)
  {
    scanFlags[idx] = defaultScanFlag;

    for (uint32_t i = 0; scanFlags[idx] && i < cpColVec.size(); i++)
    {
      colCmd = cpColVec[i];
      const EMEntry& extent = colCmd->getExtents()[idx];

      /* If any column filter eliminates an extent, it doesn't get scanned */
      scanFlags[idx] =
          scanFlags[idx] && (extent.colWid <= utils::MAXCOLUMNWIDTH) &&  // XXX: change to named constant.
          (ignoreCP || extent.partition.cprange.isValid != BRM::CP_VALID ||
           colCmd->getColType().colWidth != extent.colWid ||
           lbidListVec[i]->CasualPartitionPredicate(extent.partition.cprange, &(colCmd->getFilterString()),
                                                    colCmd->getFilterCount(), colCmd->getColType(),
                                                    colCmd->getBOP(), colCmd->getIsDict()));
    }
  }

  // @bug 2123.  Use the casual partitioning information to estimate the number of rows that will be returned
  // for use in estimating the large side table for hashjoins.
  if (estimateRowCounts)
  {
    RowEstimator rowEstimator;
    fEstimatedRows = rowEstimator.estimateRows(cpColVec, scanFlags, dbrm, fOid);
  }
}

void TupleBPS::startPrimitiveThread()
{
  pThread = jobstepThreadPool.invoke(TupleBPSPrimitive(this));
}

void TupleBPS::startAggregationThread()
{
  //  This block of code starts all threads up front
  //     fMaxNumThreads = 1;
  //     fNumThreads = fMaxNumThreads;
  //     for (uint32_t i = 0; i < fMaxNumThreads; i++)
  //             fProducerThreads.push_back(jobstepThreadPool.invoke(TupleBPSAggregators(this, i)));

  fNumThreads++;
  fProducerThreads.push_back(jobstepThreadPool.invoke(TupleBPSAggregators(this)));
}

void TupleBPS::startProcessingThread(TupleBPS* tbps, vector<boost::shared_ptr<messageqcpp::ByteStream>>& bsv,
                                     const uint32_t start, const uint32_t end, vector<_CPInfo>& cpv,
                                     RowGroupDL* dlp, const uint32_t threadID)
{
  fProcessorThreads.push_back(
      jobstepThreadPool.invoke(ByteStreamProcessor(tbps, bsv, start, end, cpv, dlp, threadID)));
}

void TupleBPS::serializeJoiner()
{
  bool more = true;
  SBS sbs(new ByteStream());

  /* false from nextJoinerMsg means it's the last msg,
      it's not exactly the exit condition*/
  while (more)
  {
    {
      // code block to release the lock immediatly
      boost::mutex::scoped_lock lk(serializeJoinerMutex);
      more = fBPP->nextTupleJoinerMsg(*sbs);
    }
#ifdef JLF_DEBUG
    cout << "serializing joiner into " << bs.length() << " bytes" << endl;
#endif
    fDec->write(uniqueID, sbs);
    sbs.reset(new ByteStream());
  }
}

// Outdated method
void TupleBPS::serializeJoiner(uint32_t conn)
{
  // We need this lock for TupleBPS::serializeJoiner()
  boost::mutex::scoped_lock lk(serializeJoinerMutex);

  ByteStream bs;
  bool more = true;

  /* false from nextJoinerMsg means it's the last msg,
      it's not exactly the exit condition*/
  while (more)
  {
    more = fBPP->nextTupleJoinerMsg(bs);
    fDec->write(bs, conn);
    bs.restart();
  }
}

void TupleBPS::prepCasualPartitioning()
{
  uint32_t i;
  int64_t min, max, seq;
  int128_t bigMin, bigMax;
  boost::mutex::scoped_lock lk(cpMutex);

  for (i = 0; i < scannedExtents.size(); i++)
  {
    if (fOid >= 3000)
    {
      scanFlags[i] = scanFlags[i] && runtimeCPFlags[i];

      if (scanFlags[i] && lbidList->CasualPartitionDataType(fColType.colDataType, fColType.colWidth))
      {
        if (fColType.colWidth <= 8)
        {
          lbidList->GetMinMax(min, max, seq, (int64_t)scannedExtents[i].range.start, &scannedExtents,
                              fColType.colDataType);
        }
        else if (fColType.colWidth == 16)
        {
          lbidList->GetMinMax(bigMin, bigMax, seq, (int64_t)scannedExtents[i].range.start, &scannedExtents,
                              fColType.colDataType);
        }
      }
    }
    else
      scanFlags[i] = true;
  }
}

bool TupleBPS::goodExtentCount()
{
  uint32_t eCount = extentsMap.begin()->second.size();
  map<CalpontSystemCatalog::OID, tr1::unordered_map<int64_t, EMEntry>>::iterator it;

  for (it = extentsMap.begin(); it != extentsMap.end(); ++it)
    if (it->second.size() != eCount)
      return false;

  return true;
}

void TupleBPS::initExtentMarkers()
{
  numDBRoots = fRm->getDBRootCount();
  lastExtent.resize(numDBRoots);
  lastScannedLBID.resize(numDBRoots);

  tr1::unordered_map<int64_t, struct BRM::EMEntry>& ref = extentsMap[fOid];
  tr1::unordered_map<int64_t, struct BRM::EMEntry>::iterator it;

  // Map part# and seg# to an extent count per segment file.
  // Part# is 32 hi order bits of key, seg# is 32 lo order bits
  std::tr1::unordered_map<uint64_t, int> extentCountPerDbFile;

  scannedExtents.clear();

  for (it = ref.begin(); it != ref.end(); ++it)
  {
    scannedExtents.push_back(it->second);

    //@bug 5322: 0 HWM may not mean full extent if 1 extent in file.
    // Track how many extents are in each segment file
    if (fExtentsPerSegFile > 1)
    {
      EMEntry& e = it->second;
      uint64_t key = ((uint64_t)e.partitionNum << 32) + e.segmentNum;
      ++extentCountPerDbFile[key];
    }
  }

  sort(scannedExtents.begin(), scannedExtents.end(), ExtentSorter());

  numExtents = scannedExtents.size();
  // @1098 initialize scanFlags to be true
  scanFlags.assign(numExtents, true);
  runtimeCPFlags.assign(numExtents, true);

  for (uint32_t i = 0; i < numDBRoots; i++)
    lastExtent[i] = -1;

  for (uint32_t i = 0; i < scannedExtents.size(); i++)
  {
    uint32_t dbRoot = scannedExtents[i].dbRoot - 1;

    /* Kludge to account for gaps in the dbroot mapping. */
    if (scannedExtents[i].dbRoot > numDBRoots)
    {
      lastExtent.resize(scannedExtents[i].dbRoot);
      lastScannedLBID.resize(scannedExtents[i].dbRoot);

      for (uint32_t z = numDBRoots; z < scannedExtents[i].dbRoot; z++)
        lastExtent[z] = -1;

      numDBRoots = scannedExtents[i].dbRoot;
    }

    if ((scannedExtents[i].status == EXTENTAVAILABLE) && (lastExtent[dbRoot] < (int)i))
      lastExtent[dbRoot] = i;

    //@bug 5322: 0 HWM may not mean full extent if 1 extent in file.
    // Use special status (EXTENTSTATUSMAX+1) to denote a single extent
    // file with HWM 0; retrieve 1 block and not full extent.
    if ((fExtentsPerSegFile > 1) && (scannedExtents[i].HWM == 0))
    {
      uint64_t key = ((uint64_t)scannedExtents[i].partitionNum << 32) + scannedExtents[i].segmentNum;

      if (extentCountPerDbFile[key] == 1)
        scannedExtents[i].status = EXTENTSTATUSMAX + 1;
    }
  }

  // if only 1 block is written in the last extent, HWM is 0 and didn't get counted.
  for (uint32_t i = 0; i < numDBRoots; i++)
  {
    if (lastExtent[i] != -1)
      lastScannedLBID[i] = scannedExtents[lastExtent[i]].range.start +
                           (scannedExtents[lastExtent[i]].HWM - scannedExtents[lastExtent[i]].blockOffset);
    else
      lastScannedLBID[i] = -1;
  }
}

void TupleBPS::reloadExtentLists()
{
  /*
   * Iterate over each ColumnCommand instance
   *
   * 1) reload its extent array
   * 2) update TupleBPS's extent array
   * 3) update vars dependent on the extent layout (lastExtent, scanFlags, etc)
   */

  uint32_t i, j;
  ColumnCommandJL* cc;
  vector<SCommand>& filters = fBPP->getFilterSteps();
  vector<SCommand>& projections = fBPP->getProjectSteps();
  uint32_t oid;

  /* To reduce the race, make all CC's get new extents as close together
   * as possible, then rebuild the local copies.
   */

  for (i = 0; i < filters.size(); i++)
  {
    cc = dynamic_cast<ColumnCommandJL*>(filters[i].get());

    if (cc != NULL)
      cc->reloadExtents();
  }

  for (i = 0; i < projections.size(); i++)
  {
    cc = dynamic_cast<ColumnCommandJL*>(projections[i].get());

    if (cc != NULL)
      cc->reloadExtents();
  }

  extentsMap.clear();

  for (i = 0; i < filters.size(); i++)
  {
    cc = dynamic_cast<ColumnCommandJL*>(filters[i].get());

    if (cc == NULL)
      continue;

    const vector<EMEntry>& extents = cc->getExtents();
    oid = cc->getOID();

    extentsMap[oid] = tr1::unordered_map<int64_t, struct BRM::EMEntry>();
    tr1::unordered_map<int64_t, struct BRM::EMEntry>& mref = extentsMap[oid];

    for (j = 0; j < extents.size(); j++)
      mref[extents[j].range.start] = extents[j];

    if (cc->auxCol())
    {
      const vector<EMEntry>& extentsAux = cc->getExtentsAux();
      oid = cc->getOIDAux();

      extentsMap[oid] = tr1::unordered_map<int64_t, struct BRM::EMEntry>();
      tr1::unordered_map<int64_t, struct BRM::EMEntry>& mrefAux = extentsMap[oid];

      for (j = 0; j < extentsAux.size(); j++)
        mrefAux[extentsAux[j].range.start] = extentsAux[j];
    }
  }

  for (i = 0; i < projections.size(); i++)
  {
    cc = dynamic_cast<ColumnCommandJL*>(projections[i].get());

    if (cc == NULL)
      continue;

    const vector<EMEntry>& extents = cc->getExtents();
    oid = cc->getOID();

    extentsMap[oid] = tr1::unordered_map<int64_t, struct BRM::EMEntry>();
    tr1::unordered_map<int64_t, struct BRM::EMEntry>& mref = extentsMap[oid];

    for (j = 0; j < extents.size(); j++)
      mref[extents[j].range.start] = extents[j];
  }

  initExtentMarkers();
}

void TupleBPS::run()
{
  uint32_t i;
  boost::mutex::scoped_lock lk(jlLock);
  uint32_t retryCounter = 0;
  const uint32_t retryMax = 1000;       // 50s max; we've seen a 15s window so 50s should be 'safe'
  const uint32_t waitInterval = 50000;  // in us

  if (fRunExecuted)
    return;

  fRunExecuted = true;

  // make sure each numeric column has the same # of extents! See bugs 4564 and 3607
  try
  {
    while (!goodExtentCount() && retryCounter++ < retryMax)
    {
      usleep(waitInterval);
      reloadExtentLists();
    }
  }
  catch (std::exception& e)
  {
    ostringstream os;
    os << "TupleBPS: Could not get a consistent extent count for each column.  Got '" << e.what() << "'\n";
    catchHandler(os.str(), ERR_TUPLE_BPS, fErrorInfo, fSessionId);
    fOutputJobStepAssociation.outAt(0)->rowGroupDL()->endOfInput();
    return;
  }

  if (retryCounter == retryMax)
  {
    catchHandler("TupleBPS: Could not get a consistent extent count for each column.", ERR_TUPLE_BPS,
                 fErrorInfo, fSessionId);
    fOutputJobStepAssociation.outAt(0)->rowGroupDL()->endOfInput();
    return;
  }

  if (traceOn())
  {
    syslogStartStep(16,                        // exemgr subsystem
                    std::string("TupleBPS"));  // step name
  }

  SBS sbs{new ByteStream()};

  if (fDelivery)
  {
    deliveryDL.reset(new RowGroupDL(1, 5));
    deliveryIt = deliveryDL->getIterator();
  }

  fBPP->setThreadCount(fMaxNumProcessorThreads);

  if (doJoin)
  {
    for (i = 0; i < smallSideCount; i++)
      tjoiners[i]->setThreadCount(fMaxNumProcessorThreads);

    fBPP->setMaxPmJoinResultCount(fMaxPmJoinResultCount);
  }

  if (fe1)
    fBPP->setFEGroup1(fe1, fe1Input);

  if (fe2 && bRunFEonPM)
    fBPP->setFEGroup2(fe2, fe2Output);

  if (fe2)
  {
    primRowGroup.initRow(&fe2InRow);
    fe2Output.initRow(&fe2OutRow);
  }

  try
  {
    fDec->addDECEventListener(this);
    fBPP->priority(priority());
    fBPP->createBPP(*sbs);
    fDec->write(uniqueID, sbs);
    BPPIsAllocated = true;

    if (doJoin && tjoiners[0]->inPM())
      serializeJoiner();

    prepCasualPartitioning();
    startPrimitiveThread();
    fProducerThreads.clear();
    fProducerThreads.reserve(fMaxNumThreads);
    startAggregationThread();
  }
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_TUPLE_BPS, logging::ERR_ALWAYS_CRITICAL,
                    "TupleBPS::run()");
    fOutputJobStepAssociation.outAt(0)->rowGroupDL()->endOfInput();
  }
}

void TupleBPS::join()
{
  boost::mutex::scoped_lock lk(jlLock);

  if (joinRan)
    return;

  joinRan = true;

  if (fRunExecuted)
  {
    if (msgsRecvd < msgsSent)
    {
      // wake up the sending thread, it should drain the input dl and exit
      boost::unique_lock<boost::mutex> tplLock(tplMutex);
      condvarWakeupProducer.notify_all();
      tplLock.unlock();
    }

    if (pThread)
      jobstepThreadPool.join(pThread);

    jobstepThreadPool.join(fProducerThreads);

    if (BPPIsAllocated)
    {
      SBS sbs{new ByteStream()};
      fDec->removeDECEventListener(this);
      fBPP->destroyBPP(*sbs);

      try
      {
        fDec->write(uniqueID, sbs);
      }
      catch (...)
      {
        handleException(std::current_exception(), logging::ERR_TUPLE_BPS, logging::ERR_ALWAYS_CRITICAL,
                        "TupleBPS::join()");
      }

      BPPIsAllocated = false;
      fDec->removeQueue(uniqueID);
      tjoiners.clear();
    }
  }
}

void TupleBPS::sendError(uint16_t status)
{
  SBS msgBpp;
  fBPP->setCount(1);
  fBPP->setStatus(status);
  fBPP->runErrorBPP(*msgBpp);

  try
  {
    fDec->write(uniqueID, msgBpp);
  }
  catch (...)
  {
    // this fcn is only called in exception handlers
    // let the first error take precedence
  }

  fBPP->reset();
  finishedSending = true;
  condvar.notify_all();
  condvarWakeupProducer.notify_all();
}

uint32_t TupleBPS::nextBand(ByteStream& bs)
{
  bool more = true;
  RowGroup& realOutputRG = (fe2 ? fe2Output : primRowGroup);
  RGData rgData;
  uint32_t rowCount = 0;

  bs.restart();

  while (rowCount == 0 && more)
  {
    more = deliveryDL->next(deliveryIt, &rgData);

    if (!more)
      rgData = fBPP->getErrorRowGroupData(status());

    realOutputRG.setData(&rgData);
    rowCount = realOutputRG.getRowCount();

    if ((more && rowCount > 0) || !more)
      realOutputRG.serializeRGData(bs);
  }

  return rowCount;
}

/* The current interleaving rotates over PMs, clustering jobs for a single PM
 * by dbroot to keep block accesses adjacent & make best use of prefetching &
 * cache.
 */
void TupleBPS::interleaveJobs(vector<Job>* jobs) const
{
  vector<Job> newJobs;
  uint32_t i;
  uint32_t pmCount = 0;
  scoped_array<deque<Job>> bins;

  // the input is grouped by dbroot

  if (pmCount == 1)
    return;

  /* Need to get the 'real' PM count */
  for (i = 0; i < jobs->size(); i++)
    if (pmCount < (*jobs)[i].connectionNum + 1)
      pmCount = (*jobs)[i].connectionNum + 1;

  bins.reset(new deque<Job>[pmCount]);

  // group by connection number
  for (i = 0; i < jobs->size(); i++)
    bins[(*jobs)[i].connectionNum].push_back((*jobs)[i]);

  // interleave by connection num
  bool noWorkDone;

  while (newJobs.size() < jobs->size())
  {
    noWorkDone = true;

    for (i = 0; i < pmCount; i++)
    {
      if (!bins[i].empty())
      {
        newJobs.push_back(bins[i].front());
        bins[i].pop_front();
        noWorkDone = false;
      }
    }

    idbassert(!noWorkDone);
  }

#if 0
    /* Work in progress */
    // up to this point, only the first connection to a PM is used.
    // the last step is to round-robin on the connections of each PM.
    // ex: on a 3 PM system where connections per PM is 2,
    // connections 0 and 3 are for PM 1, 1 and 4 are PM2, 2 and 5 are PM3.
    uint32_t* jobCounters = (uint32_t*) alloca(pmCount * sizeof(uint32_t));
    memset(jobCounters, 0, pmCount * sizeof(uint32_t));

    for (i = 0; i < newJobs.size(); i++)
    {
        uint32_t& conn = newJobs[i].connectionNum;  // for readability's sake
        conn = conn + (pmCount * jobCounters[conn]);
        jobCounters[conn]++;
    }

#endif

  jobs->swap(newJobs);
  //	cout << "-------------\n";
  //	for (i = 0; i < jobs->size(); i++)
  //		cout << "job " << i+1 << ": dbroot " << (*jobs)[i].dbroot << ", PM "
  //				<< (*jobs)[i].connectionNum + 1 << endl;
}

void TupleBPS::sendJobs(const vector<Job>& jobs)
{
  uint32_t i;
  boost::unique_lock<boost::mutex> tplLock(tplMutex, boost::defer_lock);

  for (i = 0; i < jobs.size() && !cancelled(); i++)
  {
    fDec->write(uniqueID, jobs[i].msg);
    tplLock.lock();
    msgsSent += jobs[i].expectedResponses;

    if (recvWaiting)
      condvar.notify_all();

    while ((msgsSent - msgsRecvd > fMaxOutstandingRequests << LOGICAL_EXTENT_CONVERTER) && !fDie)
    {
      sendWaiting = true;
      condvarWakeupProducer.wait(tplLock);
      sendWaiting = false;
    }

    tplLock.unlock();
  }
}

template <typename T>
bool TupleBPS::compareSingleValue(uint8_t COP, T val1, T val2) const
{
  switch (COP)
  {
    case COMPARE_LT:
    case COMPARE_NGE: return (val1 < val2);

    case COMPARE_LE:
    case COMPARE_NGT: return (val1 <= val2);

    case COMPARE_GT:
    case COMPARE_NLE: return (val1 > val2);

    case COMPARE_GE:
    case COMPARE_NLT: return (val1 >= val2);

    case COMPARE_EQ: return (val1 == val2);

    case COMPARE_NE: return (val1 != val2);
  }

  return false;
}

/* (range COP val) comparisons */
bool TupleBPS::compareRange(uint8_t COP, int64_t min, int64_t max, int64_t val) const
{
  switch (COP)
  {
    case COMPARE_LT:
    case COMPARE_NGE: return (min < val);

    case COMPARE_LE:
    case COMPARE_NGT: return (min <= val);

    case COMPARE_GT:
    case COMPARE_NLE: return (max > val);

    case COMPARE_GE:
    case COMPARE_NLT: return (max >= val);

    case COMPARE_EQ:  // an 'in' comparison
      return (val >= min && val <= max);

    case COMPARE_NE:  //  'not in'
      return (val < min || val > max);
  }

  return false;
}

bool TupleBPS::processSingleFilterString_ranged(int8_t BOP, int8_t colWidth, int64_t min, int64_t max,
                                                const uint8_t* filterString, uint32_t filterCount) const
{
  uint j;
  bool ret = true;

  for (j = 0; j < filterCount; j++)
  {
    int8_t COP;
    int64_t val2;
    bool thisPredicate;
    COP = *filterString++;
    filterString++;  // skip the round var, don't think that applies here

    switch (colWidth)
    {
      case 1:
        val2 = *((int8_t*)filterString);
        filterString++;
        break;

      case 2:
        val2 = *((int16_t*)filterString);
        filterString += 2;
        break;

      case 4:
        val2 = *((int32_t*)filterString);
        filterString += 4;
        break;

      case 8:
        val2 = *((int64_t*)filterString);
        filterString += 8;
        break;

      default: throw logic_error("invalid column width");
    }

    thisPredicate = compareRange(COP, min, max, val2);

    if (j == 0)
      ret = thisPredicate;

    if (BOP == BOP_OR && thisPredicate)
      return true;
    else if (BOP == BOP_AND && !thisPredicate)
      return false;
  }

  return ret;
}

template <typename T>
bool TupleBPS::processSingleFilterString(int8_t BOP, int8_t colWidth, T val, const uint8_t* filterString,
                                         uint32_t filterCount) const
{
  uint j;
  bool ret = true;

  for (j = 0; j < filterCount; j++)
  {
    int8_t COP;
    int64_t val2;
    datatypes::TSInt128 bigVal2;
    bool thisPredicate;
    COP = *filterString++;
    filterString++;  // skip the round var, don't think that applies here

    switch (colWidth)
    {
      case 1:
        val2 = *((int8_t*)filterString);
        filterString++;
        break;

      case 2:
        val2 = *((int16_t*)filterString);
        filterString += 2;
        break;

      case 4:
        val2 = *((int32_t*)filterString);
        filterString += 4;
        break;

      case 8:
        val2 = *((int64_t*)filterString);
        filterString += 8;
        break;

      case 16:
        bigVal2 = reinterpret_cast<const int128_t*>(filterString);
        filterString += 16;
        break;

      default: throw logic_error("invalid column width");
    }

    // Assumption is that colWidth > 0
    if (static_cast<uint8_t>(colWidth) < datatypes::MAXDECIMALWIDTH)
      thisPredicate = compareSingleValue(COP, (int64_t)val, val2);
    else
      thisPredicate = compareSingleValue(COP, (int128_t)val, bigVal2.getValue());

    if (j == 0)
      ret = thisPredicate;

    if (BOP == BOP_OR && thisPredicate)
      return true;
    else if (BOP == BOP_AND && !thisPredicate)
      return false;
  }

  return ret;
}

template <typename T>
bool TupleBPS::processOneFilterType(int8_t colWidth, T value, uint32_t type) const
{
  const vector<SCommand>& filters = fBPP->getFilterSteps();
  uint i;
  bool ret = true;
  bool firstPseudo = true;

  for (i = 0; i < filters.size(); i++)
  {
    PseudoCCJL* pseudo = dynamic_cast<PseudoCCJL*>(filters[i].get());

    if (!pseudo || pseudo->getFunction() != type)
      continue;

    int8_t BOP = pseudo->getBOP();  // I think this is the same as TupleBPS's bop var...?

    /* 1-byte COP, 1-byte 'round', colWidth-bytes value */
    const uint8_t* filterString = pseudo->getFilterString().buf();
    uint32_t filterCount = pseudo->getFilterCount();
    bool thisPredicate = processSingleFilterString(BOP, colWidth, value, filterString, filterCount);

    if (firstPseudo)
    {
      firstPseudo = false;
      ret = thisPredicate;
    }

    if (bop == BOP_OR && thisPredicate)
      return true;
    else if (bop == BOP_AND && !thisPredicate)
      return false;
  }

  return ret;
}

bool TupleBPS::processLBIDFilter(const EMEntry& emEntry) const
{
  const vector<SCommand>& filters = fBPP->getFilterSteps();
  uint i;
  bool ret = true;
  bool firstPseudo = true;
  LBID_t firstLBID = emEntry.range.start;
  LBID_t lastLBID = firstLBID + (emEntry.range.size * 1024) - 1;

  for (i = 0; i < filters.size(); i++)
  {
    PseudoCCJL* pseudo = dynamic_cast<PseudoCCJL*>(filters[i].get());

    if (!pseudo || pseudo->getFunction() != PSEUDO_BLOCKID)
      continue;

    int8_t BOP = pseudo->getBOP();  // I think this is the same as TupleBPS's bop var...?

    /* 1-byte COP, 1-byte 'round', colWidth-bytes value */
    const uint8_t* filterString = pseudo->getFilterString().buf();
    uint32_t filterCount = pseudo->getFilterCount();
    bool thisPredicate =
        processSingleFilterString_ranged(BOP, 8, firstLBID, lastLBID, filterString, filterCount);

    if (firstPseudo)
    {
      firstPseudo = false;
      ret = thisPredicate;
    }

    if (bop == BOP_OR && thisPredicate)
      return true;
    else if (bop == BOP_AND && !thisPredicate)
      return false;
  }

  return ret;
}

bool TupleBPS::processPseudoColFilters(uint32_t extentIndex,
                                       boost::shared_ptr<map<int, int>> dbRootPMMap) const
{
  if (!hasPCFilter)
    return true;

  const EMEntry& emEntry = scannedExtents[extentIndex];

  if (bop == BOP_AND)
  {
    /* All Pseudocolumns have been promoted to 8-bytes except the casual partitioning filters */
    return (!hasPMFilter || processOneFilterType(8, (*dbRootPMMap)[emEntry.dbRoot], PSEUDO_PM)) &&
           (!hasSegmentFilter || processOneFilterType(8, emEntry.segmentNum, PSEUDO_SEGMENT)) &&
           (!hasDBRootFilter || processOneFilterType(8, emEntry.dbRoot, PSEUDO_DBROOT)) &&
           (!hasSegmentDirFilter || processOneFilterType(8, emEntry.partitionNum, PSEUDO_SEGMENTDIR)) &&
           (!hasExtentIDFilter || processOneFilterType(8, emEntry.range.start, PSEUDO_EXTENTID)) &&
           (!hasMaxFilter ||
            (emEntry.partition.cprange.isValid == BRM::CP_VALID
                 ? (!fColType.isWideDecimalType()
                        ? processOneFilterType(emEntry.range.size, emEntry.partition.cprange.hiVal,
                                               PSEUDO_EXTENTMAX)
                        : processOneFilterType(fColType.colWidth, emEntry.partition.cprange.bigHiVal,
                                               PSEUDO_EXTENTMAX))
                 : true)) &&
           (!hasMinFilter ||
            (emEntry.partition.cprange.isValid == BRM::CP_VALID
                 ? (!fColType.isWideDecimalType()
                        ? processOneFilterType(emEntry.range.size, emEntry.partition.cprange.loVal,
                                               PSEUDO_EXTENTMIN)
                        : processOneFilterType(fColType.colWidth, emEntry.partition.cprange.bigLoVal,
                                               PSEUDO_EXTENTMIN))
                 : true)) &&
           (!hasLBIDFilter || processLBIDFilter(emEntry));
  }
  else
  {
    return (hasPMFilter && processOneFilterType(8, (*dbRootPMMap)[emEntry.dbRoot], PSEUDO_PM)) ||
           (hasSegmentFilter && processOneFilterType(8, emEntry.segmentNum, PSEUDO_SEGMENT)) ||
           (hasDBRootFilter && processOneFilterType(8, emEntry.dbRoot, PSEUDO_DBROOT)) ||
           (hasSegmentDirFilter && processOneFilterType(8, emEntry.partitionNum, PSEUDO_SEGMENTDIR)) ||
           (hasExtentIDFilter && processOneFilterType(8, emEntry.range.start, PSEUDO_EXTENTID)) ||
           (hasMaxFilter &&
            (emEntry.partition.cprange.isValid == BRM::CP_VALID
                 ? (!fColType.isWideDecimalType()
                        ? processOneFilterType(emEntry.range.size, emEntry.partition.cprange.hiVal,
                                               PSEUDO_EXTENTMAX)
                        : processOneFilterType(fColType.colWidth, emEntry.partition.cprange.bigHiVal,
                                               PSEUDO_EXTENTMAX))
                 : false)) ||
           (hasMinFilter &&
            (emEntry.partition.cprange.isValid == BRM::CP_VALID
                 ? (!fColType.isWideDecimalType()
                        ? processOneFilterType(emEntry.range.size, emEntry.partition.cprange.loVal,
                                               PSEUDO_EXTENTMIN)
                        : processOneFilterType(fColType.colWidth, emEntry.partition.cprange.bigLoVal,
                                               PSEUDO_EXTENTMIN))
                 : false)) ||
           (hasLBIDFilter && processLBIDFilter(emEntry));
  }
}

void TupleBPS::makeJobs(vector<Job>* jobs)
{
  boost::shared_ptr<ByteStream> bs;
  uint32_t i;
  uint32_t lbidsToScan;
  uint32_t blocksToScan;
  uint32_t blocksPerJob;
  LBID_t startingLBID;
  oam::OamCache* oamCache = oam::OamCache::makeOamCache();
  boost::shared_ptr<map<int, int>> dbRootConnectionMap = oamCache->getDBRootToConnectionMap();
  boost::shared_ptr<map<int, int>> dbRootPMMap = oamCache->getDBRootToPMMap();
  int localPMId = oamCache->getLocalPMId();

  idbassert(ffirstStepType == SCAN);

  if (fOid >= 3000 && bop == BOP_AND)
    storeCasualPartitionInfo(false);

  totalMsgs = 0;

  for (i = 0; i < scannedExtents.size(); i++)
  {
    // the # of LBIDs to scan in this extent, if it will be scanned.
    //@bug 5322: status EXTENTSTATUSMAX+1 means single block extent.
    if ((scannedExtents[i].HWM == 0) && ((int)i < lastExtent[scannedExtents[i].dbRoot - 1]) &&
        (scannedExtents[i].status <= EXTENTSTATUSMAX))
      lbidsToScan = scannedExtents[i].range.size * 1024;
    else
      lbidsToScan = scannedExtents[i].HWM - scannedExtents[i].blockOffset + 1;

    // skip this extent if CP data rules it out or the scan has already passed
    // the last extent for that DBRoot (import may be adding extents that shouldn't
    // be read yet).  Also skip if there's a pseudocolumn with a filter that would
    // eliminate this extent

    bool inBounds = ((int)i <= lastExtent[scannedExtents[i].dbRoot - 1]);

    if (!inBounds)
    {
      continue;
    }

    if (!scanFlags[i])
    {
      fNumBlksSkipped += lbidsToScan;
      continue;
    }

    if (!processPseudoColFilters(i, dbRootPMMap))
    {
      fNumBlksSkipped += lbidsToScan;
      continue;
    }

    // if (!scanFlags[i] || !inBounds)
    //	continue;

    /* Figure out many blocks have data in this extent
     * Calc how many jobs to issue,
     * Get the dbroot,
     * construct the job msgs
     */

    // Bug5741 If we are local only and this doesn't belongs to us, skip it
    if (fLocalQuery == execplan::CalpontSelectExecutionPlan::LOCAL_QUERY)
    {
      if (localPMId == 0)
      {
        throw IDBExcept(ERR_LOCAL_QUERY_UM);
      }

      if (dbRootPMMap->find(scannedExtents[i].dbRoot)->second != localPMId)
        continue;
    }

    // a necessary DB root is offline
    if (dbRootConnectionMap->find(scannedExtents[i].dbRoot) == dbRootConnectionMap->end())
    {
      // MCOL-259 force a reload of the xml. This usualy fixes it.
      Logger log;
      log.logMessage(logging::LOG_TYPE_WARNING, "forcing reload of columnstore.xml for dbRootConnectionMap");
      oamCache->forceReload();
      dbRootConnectionMap = oamCache->getDBRootToConnectionMap();

      if (dbRootConnectionMap->find(scannedExtents[i].dbRoot) == dbRootConnectionMap->end())
      {
        log.logMessage(logging::LOG_TYPE_WARNING, "dbroot still not in dbRootConnectionMap");
        throw IDBExcept(ERR_DATA_OFFLINE);
      }
    }

    // the # of logical blocks in this extent
    if (lbidsToScan % fColType.colWidth)
      blocksToScan = lbidsToScan / fColType.colWidth + 1;
    else
      blocksToScan = lbidsToScan / fColType.colWidth;

    totalMsgs += blocksToScan;

    // how many logical blocks to process with a single job (& single thread on the PM)
    blocksPerJob = max(blocksToScan / fProcessorThreadsPerScan, 16U);

    startingLBID = scannedExtents[i].range.start;
    bool isExeMgrDEC = fDec->isExeMgrDEC();
    while (blocksToScan > 0)
    {
      uint32_t blocksThisJob = min(blocksToScan, blocksPerJob);

      fBPP->setLBID(startingLBID, scannedExtents[i]);
      fBPP->setCount(blocksThisJob);
      bs.reset(new ByteStream());
      fBPP->runBPP(*bs, (*dbRootConnectionMap)[scannedExtents[i].dbRoot], isExeMgrDEC);
      jobs->push_back(
          Job(scannedExtents[i].dbRoot, (*dbRootConnectionMap)[scannedExtents[i].dbRoot], blocksThisJob, bs));
      blocksToScan -= blocksThisJob;
      startingLBID += fColType.colWidth * blocksThisJob;
      fBPP->reset();
    }
  }
}

void TupleBPS::sendPrimitiveMessages()
{
  vector<Job> jobs;

  idbassert(ffirstStepType == SCAN);

  if (cancelled())
    goto abort;

  try
  {
    makeJobs(&jobs);
    interleaveJobs(&jobs);
    sendJobs(jobs);
  }
  catch (...)
  {
    sendError(logging::ERR_TUPLE_BPS);
    handleException(std::current_exception(), logging::ERR_TUPLE_BPS, logging::ERR_ALWAYS_CRITICAL,
                    "st: " + std::to_string(fStepId) + " TupleBPS::sendPrimitiveMessages()");
    abort_nolock();
  }

abort:
  boost::unique_lock<boost::mutex> tplLock(tplMutex);
  finishedSending = true;
  condvar.notify_all();
  tplLock.unlock();
}

void TupleBPS::processByteStreamVector(vector<boost::shared_ptr<messageqcpp::ByteStream>>& bsv,
                                       const uint32_t begin, const uint32_t end, vector<_CPInfo>& cpv,
                                       RowGroupDL* dlp, const uint32_t threadID)
{
  rowgroup::RGData rgData;
  vector<rowgroup::RGData> rgDatav;
  vector<rowgroup::RGData> fromPrimProc;
  auto data = getJoinLocalDataByIndex(threadID);

  bool validCPData = false;
  bool hasBinaryColumn = false;
  int128_t min;
  int128_t max;
  uint64_t lbid;
  uint32_t cachedIO;
  uint32_t physIO;
  uint32_t touchedBlocks;
  int32_t memAmount = 0;

  for (uint32_t i = begin; i < end; ++i)
  {
    messageqcpp::ByteStream* bs = bsv[i].get();

    // @bug 488. when PrimProc node is down. error out
    // An error condition.  We are not going to do anymore.
    ISMPacketHeader* hdr = (ISMPacketHeader*)(bs->buf());

#ifdef DEBUG_MPM
    cout << "BS length: " << bs->length() << endl;
#endif
    if (bs->length() == 0 || hdr->Status > 0)
    {
      // PM errors mean this should abort right away instead of draining the PM backlog
      // tplLock.lock();

      if (bs->length() == 0)
      {
        errorMessage(IDBErrorInfo::instance()->errorMsg(ERR_PRIMPROC_DOWN));
        status(ERR_PRIMPROC_DOWN);
      }
      else
      {
        string errMsg;

        bs->advance(sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
        *bs >> errMsg;
        status(hdr->Status);
        errorMessage(errMsg);
      }

      // Sets `fDie` to true, so other threads can check `cancelled()`.
      abort_nolock();
      return;
    }

    bool unused = false;
    bool fromDictScan = false;
    fromPrimProc.clear();
    fBPP->getRowGroupData(*bs, &fromPrimProc, &validCPData, &lbid, &fromDictScan, &min, &max, &cachedIO,
                          &physIO, &touchedBlocks, &unused, threadID, &hasBinaryColumn, fColType);

    // Another layer of messiness.  Need to refactor this fcn.
    while (!fromPrimProc.empty() && !cancelled())
    {
      rgData = fromPrimProc.back();
      fromPrimProc.pop_back();

      data->local_primRG.setData(&rgData);
      // TODO need the pre-join count even on PM joins... later
      data->ridsReturned_Thread += data->local_primRG.getRowCount();

      // TupleHashJoinStep::joinOneRG() is a port of the main join loop here.  Any
      // changes made here should also be made there and vice versa.
      if (hasUMJoin || !fBPP->pmSendsFinalResult())
      {
        data->joinedData = RGData(data->local_outputRG);
        data->local_outputRG.setData(&data->joinedData);
        data->local_outputRG.resetRowGroup(data->local_primRG.getBaseRid());
        data->local_outputRG.setDBRoot(data->local_primRG.getDBRoot());
        data->local_primRG.getRow(0, &data->largeSideRow);

        for (uint32_t k = 0; k < data->local_primRG.getRowCount() && !cancelled();
             k++, data->largeSideRow.nextRow())
        {
          uint32_t matchCount = 0;

          for (uint32_t j = 0; j < smallSideCount; j++)
          {
            tjoiners[j]->match(data->largeSideRow, k, threadID, &(data->joinerOutput[j]));
#ifdef JLF_DEBUG
            // Debugging code to print the matches
            Row r;
            joinerMatchesRGs[j].initRow(&r);
            cout << data->joinerOutput[j].size() << " matches: \n";
            for (uint32_t z = 0; z < data->joinerOutput[j].size(); z++)
            {
              r.setPointer(data->joinerOutput[j][z]);
              cout << "  " << r.toString() << endl;
            }
#endif
            matchCount = data->joinerOutput[j].size();

            if (tjoiners[j]->inUM())
            {
              // Count the # of rows that pass the join filter
              if (tjoiners[j]->hasFEFilter() && matchCount > 0)
              {
                vector<Row::Pointer> newJoinerOutput;
                applyMapping(data->fergMappings[smallSideCount], data->largeSideRow, &data->joinFERow);

                for (uint32_t z = 0; z < data->joinerOutput[j].size(); z++)
                {
                  data->smallSideRows[j].setPointer(data->joinerOutput[j][z]);
                  applyMapping(data->fergMappings[j], data->smallSideRows[j], &data->joinFERow);

                  if (!tjoiners[j]->evaluateFilter(data->joinFERow, threadID))
                    matchCount--;
                  else
                  {
                    // The first match includes it in a SEMI join result and excludes it
                    // from an ANTI join result.  If it's SEMI & SCALAR however, it needs
                    // to continue.
                    newJoinerOutput.push_back(data->joinerOutput[j][z]);

                    if (tjoiners[j]->antiJoin() || (tjoiners[j]->semiJoin() && !tjoiners[j]->scalar()))
                      break;
                  }
                }

                // the filter eliminated all matches, need to join with the NULL row
                if (matchCount == 0 && tjoiners[j]->largeOuterJoin())
                {
                  newJoinerOutput.push_back(Row::Pointer(data->smallNullMemory[j].get()));
                  matchCount = 1;
                }

                data->joinerOutput[j].swap(newJoinerOutput);
              }

              // XXXPAT: This has gone through enough revisions it would benefit
              // from refactoring

              // If anti-join, reverse the result
              if (tjoiners[j]->antiJoin())
              {
                matchCount = (matchCount ? 0 : 1);
              }

              if (matchCount == 0)
              {
                data->joinerOutput[j].clear();
                break;
              }
              else if (!tjoiners[j]->scalar() && (tjoiners[j]->antiJoin() || tjoiners[j]->semiJoin()))
              {
                data->joinerOutput[j].clear();
                data->joinerOutput[j].push_back(Row::Pointer(data->smallNullMemory[j].get()));
                matchCount = 1;
              }
            }

            if (matchCount == 0 && tjoiners[j]->innerJoin())
              break;

            // Scalar check
            if (tjoiners[j]->scalar() && matchCount > 1)
            {
              errorMessage(IDBErrorInfo::instance()->errorMsg(ERR_MORE_THAN_1_ROW));
              status(ERR_MORE_THAN_1_ROW);
              abort();
            }

            if (tjoiners[j]->smallOuterJoin())
              tjoiners[j]->markMatches(threadID, data->joinerOutput[j]);
          }

          if (matchCount > 0)
          {
            applyMapping(data->largeMapping, data->largeSideRow, &data->joinedBaseRow);
            data->joinedBaseRow.setRid(data->largeSideRow.getRelRid());
            memAmount += data->generateJoinResultSet(0, rgDatav, dlp);
          }
        }  // end of the for-loop in the join code

        if (data->local_outputRG.getRowCount() > 0)
        {
          rgDatav.push_back(data->joinedData);
        }
      }
      else
      {
        rgDatav.push_back(rgData);
      }
      if (memAmount)
      {
        resourceManager()->returnMemory(memAmount);
        memAmount = 0;
      }

      // Execute UM F & E group 2 on rgDatav
      if (fe2 && !bRunFEonPM && rgDatav.size() > 0 && !cancelled())
      {
        data->processFE2(rgDatav);
        rgDataVecToDl(rgDatav, data->local_fe2Output, dlp);
      }

      data->cachedIO_Thread += cachedIO;
      data->physIO_Thread += physIO;
      data->touchedBlocks_Thread += touchedBlocks;

      if (fOid >= 3000 && ffirstStepType == SCAN && bop == BOP_AND)
      {
        if (fColType.colWidth <= 8)
        {
          cpv.push_back(_CPInfo((int64_t)min, (int64_t)max, lbid, fromDictScan, validCPData));
        }
        else if (fColType.colWidth == 16)
        {
          cpv.push_back(_CPInfo(min, max, lbid, validCPData));
        }
      }
    }  // end of the per-rowgroup processing loop

    // insert the resulting rowgroup data from a single bytestream into dlp
    if (rgDatav.size() > 0)
    {
      if (fe2 && bRunFEonPM)
        rgDataVecToDl(rgDatav, data->local_fe2Output, dlp);
      else
        rgDataVecToDl(rgDatav, data->local_outputRG, dlp);
    }
  }
}

void TupleBPS::receiveMultiPrimitiveMessages()
{
  AnyDataListSPtr dl = fOutputJobStepAssociation.outAt(0);
  RowGroupDL* dlp = (fDelivery ? deliveryDL.get() : dl->rowGroupDL());

  StepTeleStats sts;
  sts.query_uuid = fQueryUuid;
  sts.step_uuid = fStepUuid;

  uint32_t size = 0;

  // Based on the type of `tupleBPS` operation - initialize the `JoinLocalDataPool`.
  // We initialize the max possible number of threads, because the right number of parallel threads will be
  // chosen right before the `vector of bytestream` processing based on `ThreadPool` workload.
  if (doJoin || fe2)
    initializeJoinLocalDataPool(fMaxNumProcessorThreads);
  else
    initializeJoinLocalDataPool(1);

  vector<boost::shared_ptr<messageqcpp::ByteStream>> bsv;
  boost::unique_lock<boost::mutex> tplLock(tplMutex, boost::defer_lock);

  try
  {
    tplLock.lock();

    while (1)
    {
      // sync with the send side
      while (!finishedSending && msgsSent == msgsRecvd)
      {
        recvWaiting++;
        condvar.wait(tplLock);
        recvWaiting--;
      }

      if (msgsSent == msgsRecvd && finishedSending)
      {
        break;
      }

      bool flowControlOn;
      fDec->read_some(uniqueID, fNumThreads, bsv, &flowControlOn);
      size = bsv.size();

      // @bug 4562
      if (traceOn() && fOid >= 3000 && dlTimes.FirstReadTime().tv_sec == 0)
        dlTimes.setFirstReadTime();

      if (fOid >= 3000 && sts.msg_type == StepTeleStats::ST_INVALID && size > 0)
      {
        sts.msg_type = StepTeleStats::ST_START;
        sts.total_units_of_work = totalMsgs;
        postStepStartTele(sts);
      }

      for (uint32_t z = 0; z < size; z++)
      {
        if (bsv[z]->length() > 0 && fBPP->countThisMsg(*(bsv[z])))
        {
          ++msgsRecvd;
        }
      }

      //@Bug 1424,1298

      if (sendWaiting && ((msgsSent - msgsRecvd) <= (fMaxOutstandingRequests << LOGICAL_EXTENT_CONVERTER)))
      {
        condvarWakeupProducer.notify_one();
        THROTTLEDEBUG << "receiveMultiPrimitiveMessages wakes up sending side .. "
                      << "  msgsSent: " << msgsSent << "  msgsRecvd = " << msgsRecvd << endl;
      }

      /* If there's an error and the joblist is being aborted, don't
          sit around forever waiting for responses.  */
      if (cancelled())
      {
        if (sendWaiting)
          condvarWakeupProducer.notify_one();

        break;
      }

      if (size == 0)
      {
        tplLock.unlock();
        usleep(2000 * fNumThreads);
        tplLock.lock();
        continue;
      }

      tplLock.unlock();

      vector<vector<_CPInfo>> cpInfos;
      // Calculate the work sizes.
      const uint32_t issuedThreads = jobstepThreadPool.getIssuedThreads();
      const uint32_t maxNumOfThreads = jobstepThreadPool.getMaxThreads();
      uint32_t numOfThreads = 1;

      if (issuedThreads < maxNumOfThreads && (doJoin || fe2))
      {
        // We cannot parallel more than allocated data we have.
        numOfThreads = std::min(maxNumOfThreads - issuedThreads, fNumProcessorThreads);
      }

      uint32_t workSize = size / numOfThreads;
      if (!workSize)
      {
        workSize = size;
        numOfThreads = 1;
      }

      vector<uint32_t> workSizes;
      // Calculate the work size for each thread.
      workSizes.reserve(numOfThreads);
      cpInfos.reserve(numOfThreads);

      for (uint32_t i = 0; i < numOfThreads; ++i)
      {
        workSizes.push_back(workSize);
        cpInfos.push_back(vector<_CPInfo>());
      }

      const uint32_t moreWork = size % numOfThreads;
      for (uint32_t i = 0; i < moreWork; ++i)
      {
        ++workSizes[i];
      }

      uint32_t start = 0;
#ifdef DEBUG_BSV
      cout << "Number of threads: " << workSizes.size() << endl;
#endif
      for (uint32_t i = 0, e = workSizes.size(); i < e; ++i)
      {
#ifdef DEBUG_BSV
        cout << "Thread # " << i << " work size " << workSizes[i] << endl;
#endif
        uint32_t end = start + workSizes[i];
        startProcessingThread(this, bsv, start, end, cpInfos[i], dlp, i);
        start = end;
      }

      jobstepThreadPool.join(fProcessorThreads);
      // Clear all.
      fProcessorThreads.clear();
      bsv.clear();

      // @bug 4562
      if (traceOn() && fOid >= 3000)
        dlTimes.setFirstInsertTime();

      // update casual partition
      for (const auto& cpv : cpInfos)
      {
        size = cpv.size();
        if (size > 0 && !cancelled())
        {
          for (uint32_t i = 0; i < size; i++)
          {
            if (fColType.colWidth > 8)
            {
              lbidList->UpdateMinMax(cpv[i].bigMin, cpv[i].bigMax, cpv[i].LBID, cpv[i].dictScan, fColType,
                                     cpv[i].valid);
            }
            else
            {
              lbidList->UpdateMinMax(cpv[i].min, cpv[i].max, cpv[i].LBID, cpv[i].dictScan, fColType,
                                     cpv[i].valid);
            }
          }
        }
      }

      tplLock.lock();

      if (fOid >= 3000)
      {
        uint64_t progress = msgsRecvd * 100 / totalMsgs;
        bool postProgress = (progress > fProgress);

        if (postProgress)
        {
          fProgress = progress;

          sts.msg_type = StepTeleStats::ST_PROGRESS;
          sts.total_units_of_work = totalMsgs;
          sts.units_of_work_completed = msgsRecvd;
          postStepProgressTele(sts);
        }
      }

    }  // done reading

  }  // try
  catch (...)
  {
    handleException(std::current_exception(), logging::ERR_TUPLE_BPS, logging::ERR_ALWAYS_CRITICAL,
                    "st: " + std::to_string(fStepId) + " TupleBPS::receiveMultiPrimitiveMessages()");
    abort_nolock();
  }

  // We have one thread here and do not need to notify any waiting producer threads, because we are done with
  // consuming messages from queue.
  tplLock.unlock();

  // Take just the first one.
  auto data = getJoinLocalDataByIndex(0);
  {
    if (doJoin && smallOuterJoiner != -1 && !cancelled())
    {
      vector<rowgroup::RGData> rgDatav;

      // If this was a left outer join, this needs to put the unmatched
      //  rows from the joiner into the output
      // XXXPAT: This might be a problem if later steps depend
      // on sensible rids and/or sensible ordering
      vector<Row::Pointer> unmatched;
#ifdef JLF_DEBUG
      cout << "finishing small-outer join output\n";
#endif
      uint32_t i = smallOuterJoiner;
      tjoiners[i]->getUnmarkedRows(&unmatched);
      data->joinedData = RGData(data->local_outputRG);
      data->local_outputRG.setData(&data->joinedData);
      data->local_outputRG.resetRowGroup(-1);
      data->local_outputRG.getRow(0, &data->joinedBaseRow);

      for (uint32_t j = 0; j < unmatched.size(); j++)
      {
        data->smallSideRows[i].setPointer(unmatched[j]);

        for (uint32_t k = 0; k < smallSideCount; k++)
        {
          if (i == k)
            applyMapping(data->smallMappings[i], data->smallSideRows[i], &data->joinedBaseRow);
          else
            applyMapping(data->smallMappings[k], data->smallNulls[k], &data->joinedBaseRow);
        }

        applyMapping(data->largeMapping, data->largeNull, &data->joinedBaseRow);
        data->joinedBaseRow.setRid(0);
        data->joinedBaseRow.nextRow();
        data->local_outputRG.incRowCount();

        if (data->local_outputRG.getRowCount() == 8192)
        {
          if (fe2)
          {
            rgDatav.push_back(data->joinedData);
            data->processFE2(rgDatav);

            if (rgDatav.size() > 0)
              rgDataToDl(rgDatav[0], data->local_fe2Output, dlp);

            rgDatav.clear();
          }
          else
            rgDataToDl(data->joinedData, data->local_outputRG, dlp);

          data->joinedData = RGData(data->local_outputRG);
          data->local_outputRG.setData(&data->joinedData);
          data->local_outputRG.resetRowGroup(-1);
          data->local_outputRG.getRow(0, &data->joinedBaseRow);
        }
      }

      if (data->local_outputRG.getRowCount() > 0)
      {
        if (fe2)
        {
          rgDatav.push_back(data->joinedData);
          data->processFE2(rgDatav);

          if (rgDatav.size() > 0)
            rgDataToDl(rgDatav[0], data->local_fe2Output, dlp);

          rgDatav.clear();
        }
        else
          rgDataToDl(data->joinedData, data->local_outputRG, dlp);
      }
    }

    if (traceOn() && fOid >= 3000)
    {
      //...Casual partitioning could cause us to do no processing.  In that
      //...case these time stamps did not get set.  So we set them here.
      if (dlTimes.FirstReadTime().tv_sec == 0)
      {
        dlTimes.setFirstReadTime();
        dlTimes.setLastReadTime();
        dlTimes.setFirstInsertTime();
      }

      dlTimes.setEndOfInputTime();
    }

    SBS sbs{new ByteStream()};

    try
    {
      if (BPPIsAllocated)
      {
        fDec->removeDECEventListener(this);
        fBPP->destroyBPP(*sbs);
        fDec->write(uniqueID, sbs);
        BPPIsAllocated = false;
      }
    }
    // catch and do nothing. Let it continue with the clean up and profiling
    catch (const std::exception& e)
    {
      cerr << "tuple-bps caught: " << e.what() << endl;
    }
    catch (...)
    {
      cerr << "tuple-bps caught unknown exception" << endl;
    }

    Stats stats = fDec->getNetworkStats(uniqueID);
    fMsgBytesIn = stats.dataRecvd();
    fMsgBytesOut = stats.dataSent();
    fDec->removeQueue(uniqueID);
    tjoiners.clear();
  }

  //@Bug 1099
  ridsReturned += data->ridsReturned_Thread;
  fPhysicalIO += data->physIO_Thread;
  fCacheIO += data->cachedIO_Thread;
  fBlockTouched += data->touchedBlocks_Thread;

  if (fTableOid >= 3000)
  {
    struct timeval tvbuf;
    gettimeofday(&tvbuf, 0);
    FIFO<std::shared_ptr<uint8_t[]>>* pFifo = 0;
    uint64_t totalBlockedReadCount = 0;
    uint64_t totalBlockedWriteCount = 0;

    //...Sum up the blocked FIFO reads for all input associations
    size_t inDlCnt = fInputJobStepAssociation.outSize();

    for (size_t iDataList = 0; iDataList < inDlCnt; iDataList++)
    {
      pFifo = dynamic_cast<FIFO<std::shared_ptr<uint8_t[]>>*>(
          fInputJobStepAssociation.outAt(iDataList)->rowGroupDL());

      if (pFifo)
      {
        totalBlockedReadCount += pFifo->blockedReadCount();
      }
    }

    //...Sum up the blocked FIFO writes for all output associations
    size_t outDlCnt = fOutputJobStepAssociation.outSize();

    for (size_t iDataList = 0; iDataList < outDlCnt; iDataList++)
    {
      pFifo = dynamic_cast<FIFO<std::shared_ptr<uint8_t[]>>*>(dlp);

      if (pFifo)
      {
        totalBlockedWriteCount += pFifo->blockedWriteCount();
      }
    }

    // Notify consumers as early as possible.
    dlp->endOfInput();

    //...Roundoff msg byte counts to nearest KB for display
    uint64_t msgBytesInKB = fMsgBytesIn >> 10;
    uint64_t msgBytesOutKB = fMsgBytesOut >> 10;

    if (fMsgBytesIn & 512)
      msgBytesInKB++;

    if (fMsgBytesOut & 512)
      msgBytesOutKB++;

    if (traceOn())
    {
      // @bug 828
      ostringstream logStr;
      logStr << "ses:" << fSessionId << " st: " << fStepId << " finished at " << JSTimeStamp::format(tvbuf)
             << "; PhyI/O-" << fPhysicalIO << "; CacheI/O-" << fCacheIO << "; MsgsSent-" << msgsSent
             << "; MsgsRvcd-" << msgsRecvd << "; BlocksTouched-" << fBlockTouched << "; BlockedFifoIn/Out-"
             << totalBlockedReadCount << "/" << totalBlockedWriteCount << "; output size-" << ridsReturned
             << endl
             << "\tPartitionBlocksEliminated-" << fNumBlksSkipped << "; MsgBytesIn-" << msgBytesInKB << "KB"
             << "; MsgBytesOut-" << msgBytesOutKB << "KB"
             << "; TotalMsgs-" << totalMsgs << endl
             << "\t1st read " << dlTimes.FirstReadTimeString() << "; EOI " << dlTimes.EndOfInputTimeString()
             << "; runtime-" << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime())
             << "s\n\tUUID " << uuids::to_string(fStepUuid) << "\n\tQuery UUID "
             << uuids::to_string(queryUuid()) << "\n\tJob completion status " << status() << endl;
      logEnd(logStr.str().c_str());

      syslogReadBlockCounts(16,                         // exemgr sybsystem
                            fPhysicalIO,                // # blocks read from disk
                            fCacheIO,                   // # blocks read from cache
                            fNumBlksSkipped);           // # casual partition block hits
      syslogProcessingTimes(16,                         // exemgr subsystem
                            dlTimes.FirstReadTime(),    // first datalist read
                            dlTimes.LastReadTime(),     // last  datalist read
                            dlTimes.FirstInsertTime(),  // first datalist write
                            dlTimes.EndOfInputTime());  // last (endOfInput) datalist write
      syslogEndStep(16,                                 // exemgr subsystem
                    totalBlockedReadCount,              // blocked datalist input
                    totalBlockedWriteCount,             // blocked datalist output
                    fMsgBytesIn,                        // incoming msg byte count
                    fMsgBytesOut);                      // outgoing msg byte count
      fExtendedInfo += toString() + logStr.str();
      formatMiniStats();
    }

    {
      sts.msg_type = StepTeleStats::ST_SUMMARY;
      sts.phy_io = fPhysicalIO;
      sts.cache_io = fCacheIO;
      sts.msg_rcv_cnt = sts.total_units_of_work = sts.units_of_work_completed = msgsRecvd;
      sts.cp_blocks_skipped = fNumBlksSkipped;
      sts.msg_bytes_in = fMsgBytesIn;
      sts.msg_bytes_out = fMsgBytesOut;
      sts.rows = ridsReturned;
      postStepSummaryTele(sts);
    }

    if (ffirstStepType == SCAN && bop == BOP_AND && !cancelled())
    {
      lbidList->UpdateAllPartitionInfo(fColType);
    }
  }
  else
  {
    // Notify consumers.
    dlp->endOfInput();
  }
}

const string TupleBPS::toString() const
{
  ostringstream oss;
  oss << "TupleBPS        ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId << " st:" << fStepId
      << " tb/col:" << fTableOid << "/" << fOid;

  if (alias().length())
    oss << " alias:" << alias();

  if (view().length())
    oss << " view:" << view();

#if 0

    // @bug 1282, don't have output datalist for delivery
    if (!fDelivery)
        oss << " " << omitOidInDL << fOutputJobStepAssociation.outAt(0) << showOidInDL;

#else

  if (fDelivery)
    oss << " is del ";
  else
    oss << " not del ";

  if (bop == BOP_OR)
    oss << " BOP_OR ";

  if (fDie)
    oss << " aborting " << msgsSent << "/" << msgsRecvd << " " << uniqueID << " ";

  if (fOutputJobStepAssociation.outSize() > 0)
  {
    oss << fOutputJobStepAssociation.outAt(0);

    if (fOutputJobStepAssociation.outSize() > 1)
      oss << " (too many outputs?)";
  }
  else
  {
    oss << " (no outputs?)";
  }

#endif
  oss << " nf:" << fFilterCount;
  oss << " in:";

  for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
  {
    oss << fInputJobStepAssociation.outAt(i);
  }

  oss << endl << "  UUID: " << uuids::to_string(fStepUuid) << endl;
  oss << "  Query UUID: " << uuids::to_string(queryUuid()) << endl;
  oss << "  " << fBPP->toString() << endl;
  return oss.str();
}

/* This exists to avoid a DBRM lookup for every rid. */
inline bool TupleBPS::scanit(uint64_t rid)
{
  uint64_t fbo;
  uint32_t extentIndex;

  if (fOid < 3000)
    return true;

  fbo = rid >> rpbShift;
  extentIndex = fbo >> divShift;
  return scanFlags[extentIndex] && runtimeCPFlags[extentIndex];
}

uint64_t TupleBPS::getFBO(uint64_t lbid)
{
  uint32_t i;
  uint64_t lastLBID;

  for (i = 0; i < numExtents; i++)
  {
    lastLBID = scannedExtents[i].range.start + (scannedExtents[i].range.size << 10) - 1;

    if (lbid >= (uint64_t)scannedExtents[i].range.start && lbid <= lastLBID)
      return (lbid - scannedExtents[i].range.start) + (i << divShift);
  }

  throw logic_error("TupleBPS: didn't find the FBO?");
}

void TupleBPS::useJoiner(std::shared_ptr<joiner::TupleJoiner> tj)
{
  vector<std::shared_ptr<joiner::TupleJoiner>> v;
  v.push_back(tj);
  useJoiners(v);
}

void TupleBPS::useJoiners(const vector<std::shared_ptr<joiner::TupleJoiner>>& joiners)
{
  uint32_t i;

  tjoiners = joiners;
  doJoin = (joiners.size() != 0);

  joinerMatchesRGs.clear();
  smallSideCount = tjoiners.size();
  hasPMJoin = false;
  hasUMJoin = false;

  for (i = 0; i < smallSideCount; i++)
  {
    joinerMatchesRGs.push_back(tjoiners[i]->getSmallRG());

    if (tjoiners[i]->inPM())
      hasPMJoin = true;
    else
      hasUMJoin = true;

    if (tjoiners[i]->getJoinType() & SMALLOUTER)
      smallOuterJoiner = i;
  }

  if (hasPMJoin)
    fBPP->useJoiners(tjoiners);
}

void TupleBPS::newPMOnline(uint32_t connectionNumber)
{
  ByteStream bs;

  fBPP->createBPP(bs);

  try
  {
    fDec->write(bs, connectionNumber);

    if (hasPMJoin)
      serializeJoiner(connectionNumber);
  }
  catch (IDBExcept& e)
  {
    abort();
    catchHandler(e.what(), e.errorCode(), fErrorInfo, fSessionId);
  }
}

void TupleBPS::setInputRowGroup(const rowgroup::RowGroup& rg)
{
  inputRowGroup = rg;
  fBPP->setInputRowGroup(rg);
}

void TupleBPS::setOutputRowGroup(const rowgroup::RowGroup& rg)
{
  outputRowGroup = rg;
  primRowGroup = rg;
  fBPP->setProjectionRowGroup(rg);
  checkDupOutputColumns(rg);

  if (fe2)
    fe2Mapping = makeMapping(outputRowGroup, fe2Output);
}

void TupleBPS::setJoinedResultRG(const rowgroup::RowGroup& rg)
{
  outputRowGroup = rg;
  checkDupOutputColumns(rg);
  fBPP->setJoinedRowGroup(rg);

  if (fe2)
    fe2Mapping = makeMapping(outputRowGroup, fe2Output);
}

const rowgroup::RowGroup& TupleBPS::getOutputRowGroup() const
{
  return outputRowGroup;
}

void TupleBPS::setAggregateStep(const rowgroup::SP_ROWAGG_PM_t& agg, const rowgroup::RowGroup& rg)
{
  if (rg.getColumnCount() > 0)
  {
    fAggRowGroupPm = rg;
    fAggregatorPm = agg;

    fBPP->addAggregateStep(agg, rg);
    fBPP->setNeedRidsAtDelivery(false);
  }
}

void TupleBPS::setBOP(uint8_t op)
{
  bop = op;
  fBPP->setBOP(bop);
}

void TupleBPS::setJobInfo(const JobInfo* jobInfo)
{
  fBPP->jobInfo(jobInfo);
}

uint64_t TupleBPS::getEstimatedRowCount()
{
  // Call function that populates the scanFlags array based on the extents that qualify based on casual
  // partitioning.
  storeCasualPartitionInfo(true);
  // TODO:  Strip out the cout below after a few days of testing.
#ifdef JLF_DEBUG
  cout << "OID-" << fOid << " EstimatedRowCount-" << fEstimatedRows << endl;
#endif
  return fEstimatedRows;
}

void TupleBPS::checkDupOutputColumns(const rowgroup::RowGroup& rg)
{
  // bug 1965, find if any duplicate columns selected
  map<uint32_t, uint32_t> keymap;  // map<unique col key, col index in the row group>
  dupColumns.clear();
  const vector<uint32_t>& keys = rg.getKeys();

  for (uint32_t i = 0; i < keys.size(); i++)
  {
    map<uint32_t, uint32_t>::iterator j = keymap.find(keys[i]);

    if (j == keymap.end())
      keymap.insert(make_pair(keys[i], i));  // map key to col index
    else
      dupColumns.push_back(make_pair(i, j->second));  // dest/src index pair
  }
}

void TupleBPS::dupOutputColumns(RGData& data, RowGroup& rg)
{
  rg.setData(&data);
  dupOutputColumns(rg);
}

void TupleBPS::dupOutputColumns(RowGroup& rg)
{
  Row workingRow;
  rg.initRow(&workingRow);
  rg.getRow(0, &workingRow);

  for (uint64_t i = 0; i < rg.getRowCount(); i++)
  {
    for (uint64_t j = 0; j < dupColumns.size(); j++)
      workingRow.copyField(dupColumns[j].first, dupColumns[j].second);

    workingRow.nextRow();
  }
}

void TupleBPS::stepId(uint16_t stepId)
{
  fStepId = stepId;
  fBPP->setStepID(stepId);
}

void TupleBPS::addFcnJoinExp(const vector<execplan::SRCP>& fe)
{
  if (!fe1)
    fe1.reset(new funcexp::FuncExpWrapper());

  for (uint32_t i = 0; i < fe.size(); i++)
    fe1->addReturnedColumn(fe[i]);
}

void TupleBPS::addFcnExpGroup1(const boost::shared_ptr<execplan::ParseTree>& fe)
{
  if (!fe1)
    fe1.reset(new funcexp::FuncExpWrapper());

  fe1->addFilter(fe);
}

void TupleBPS::setFE1Input(const RowGroup& feInput)
{
  fe1Input = feInput;
}

void TupleBPS::setFcnExpGroup2(const boost::shared_ptr<funcexp::FuncExpWrapper>& fe,
                               const rowgroup::RowGroup& rg, bool runFE2onPM)
{
  // the presence of fe2 changes rowgroup format which is used in PrimProc.
  // please be aware, if you are modifying several parts of the system.
  // the relevant part is in primimities/prim-proc/batchprimitiveprocessor,
  // execute() function, a branch where aggregation is handled.
  fe2 = fe;
  fe2Output = rg;
  checkDupOutputColumns(rg);
  fe2Mapping = makeMapping(outputRowGroup, fe2Output);
  bRunFEonPM = runFE2onPM;

  if (bRunFEonPM)
    fBPP->setFEGroup2(fe2, fe2Output);
}

void TupleBPS::setFcnExpGroup3(const vector<execplan::SRCP>& fe)
{
  if (!fe2)
    fe2.reset(new funcexp::FuncExpWrapper());

  for (uint32_t i = 0; i < fe.size(); i++)
    fe2->addReturnedColumn(fe[i]);

  // if this is called, there's no join, so it can always run on the PM
  bRunFEonPM = true;
  fBPP->setFEGroup2(fe2, fe2Output);
}

void TupleBPS::setFE23Output(const rowgroup::RowGroup& feOutput)
{
  fe2Output = feOutput;
  checkDupOutputColumns(feOutput);
  fe2Mapping = makeMapping(outputRowGroup, fe2Output);

  if (fe2 && bRunFEonPM)
    fBPP->setFEGroup2(fe2, fe2Output);
}

const rowgroup::RowGroup& TupleBPS::getDeliveredRowGroup() const
{
  if (fe2)
    return fe2Output;

  return outputRowGroup;
}

void TupleBPS::deliverStringTableRowGroup(bool b)
{
  if (fe2)
    fe2Output.setUseStringTable(b);
  else if (doJoin)
    outputRowGroup.setUseStringTable(b);
  else
  {
    outputRowGroup.setUseStringTable(b);
    primRowGroup.setUseStringTable(b);
  }

  fBPP->deliverStringTableRowGroup(b);
}

bool TupleBPS::deliverStringTableRowGroup() const
{
  if (fe2)
    return fe2Output.usesStringTable();

  return outputRowGroup.usesStringTable();
}

void TupleBPS::formatMiniStats()
{
  ostringstream oss;
  oss << "BPS "
      << "PM " << alias() << " " << fTableOid << " " << fBPP->toMiniString() << " " << fPhysicalIO << " "
      << fCacheIO << " " << fNumBlksSkipped << " "
      << JSTimeStamp::tsdiffstr(dlTimes.EndOfInputTime(), dlTimes.FirstReadTime()) << " " << ridsReturned
      << " ";

  fMiniInfo += oss.str();
}

void TupleBPS::rgDataToDl(RGData& rgData, RowGroup& rg, RowGroupDL* dlp)
{
  // bug 1965, populate duplicate columns if any.
  if (dupColumns.size() > 0)
    dupOutputColumns(rgData, rg);

  dlp->insert(rgData);
}

void TupleBPS::rgDataVecToDl(vector<RGData>& rgDatav, RowGroup& rg, RowGroupDL* dlp)
{
  uint64_t size = rgDatav.size();

  if (size > 0 && !cancelled())
  {
    dlMutex.lock();

    for (uint64_t i = 0; i < size; i++)
    {
      rgDataToDl(rgDatav[i], rg, dlp);
    }

    dlMutex.unlock();
  }

  rgDatav.clear();
}

void TupleBPS::setJoinFERG(const RowGroup& rg)
{
  joinFERG = rg;
  fBPP->setJoinFERG(rg);
}

void TupleBPS::addCPPredicates(uint32_t OID, const vector<int128_t>& vals, bool isRange,
                               bool isSmallSideWideDecimal)
{
  if (fTraceFlags & CalpontSelectExecutionPlan::IGNORE_CP || fOid < 3000)
    return;

  uint32_t i, j, k;
  int64_t min, max, seq;
  int128_t bigMin, bigMax;
  bool isValid, intersection;
  vector<SCommand> colCmdVec = fBPP->getFilterSteps();
  ColumnCommandJL* cmd;

  for (i = 0; i < fBPP->getProjectSteps().size(); i++)
    colCmdVec.push_back(fBPP->getProjectSteps()[i]);

  LBIDList ll(OID, 0);

  /* Find the columncommand with that OID.
   * Check that the column type is one handled by CP.
   * For each extent in that OID,
   *    grab the min & max,
   *    OR together all of the intersection tests,
   *    AND it with the current CP flag.
   */

  for (i = 0; i < colCmdVec.size(); i++)
  {
    cmd = dynamic_cast<ColumnCommandJL*>(colCmdVec[i].get());

    if (cmd != NULL && cmd->getOID() == OID)
    {
      const execplan::CalpontSystemCatalog::ColType& colType = cmd->getColType();

      if (!ll.CasualPartitionDataType(colType.colDataType, colType.colWidth) || cmd->isDict())
        return;

      // @bug 2989, use correct extents
      tr1::unordered_map<int64_t, struct BRM::EMEntry>* extentsPtr = NULL;
      vector<struct BRM::EMEntry> extents;  // in case the extents of OID is not in Map

      // TODO: store the sorted vectors from the pcolscans/steps as a minor optimization
      dbrm.getExtents(OID, extents);
      sort(extents.begin(), extents.end(), ExtentSorter());

      if (extentsMap.find(OID) != extentsMap.end())
      {
        extentsPtr = &extentsMap[OID];
      }
      else if (dbrm.getExtents(OID, extents) == 0)
      {
        extentsMap[OID] = tr1::unordered_map<int64_t, struct BRM::EMEntry>();
        tr1::unordered_map<int64_t, struct BRM::EMEntry>& mref = extentsMap[OID];

        for (uint32_t z = 0; z < extents.size(); z++)
          mref[extents[z].range.start] = extents[z];

        extentsPtr = &mref;
      }

      if (colType.colWidth <= 8)
      {
        for (j = 0; j < extents.size(); j++)
        {
          isValid = ll.GetMinMax(&min, &max, &seq, extents[j].range.start, *extentsPtr, colType.colDataType);

          if (isValid)
          {
            if (isRange)
            {
              if (!isSmallSideWideDecimal)
              {
                runtimeCPFlags[j] =
                    ll.checkRangeOverlap(min, max, (int64_t)vals[0], (int64_t)vals[1], colType) &&
                    runtimeCPFlags[j];
              }
              else
              {
                runtimeCPFlags[j] =
                    ll.checkRangeOverlap((int128_t)min, (int128_t)max, vals[0], vals[1], colType) &&
                    runtimeCPFlags[j];
              }
            }
            else
            {
              intersection = false;

              for (k = 0; k < vals.size(); k++)
              {
                if (!isSmallSideWideDecimal)
                {
                  intersection = intersection || ll.checkSingleValue(min, max, (int64_t)vals[k], colType);
                }
                else
                {
                  intersection =
                      intersection || ll.checkSingleValue((int128_t)min, (int128_t)max, vals[k], colType);
                }
              }

              runtimeCPFlags[j] = intersection && runtimeCPFlags[j];
            }
          }
        }
      }
      else
      {
        for (j = 0; j < extents.size(); j++)
        {
          isValid =
              ll.GetMinMax(&bigMin, &bigMax, &seq, extents[j].range.start, *extentsPtr, colType.colDataType);

          if (isValid)
          {
            if (isRange)
              runtimeCPFlags[j] =
                  ll.checkRangeOverlap(bigMin, bigMax, vals[0], vals[1], colType) && runtimeCPFlags[j];
            else
            {
              intersection = false;

              for (k = 0; k < vals.size(); k++)
                intersection = intersection || ll.checkSingleValue(bigMin, bigMax, vals[k], colType);

              runtimeCPFlags[j] = intersection && runtimeCPFlags[j];
            }
          }
        }
      }

      break;
    }
  }
}

void TupleBPS::dec(DistributedEngineComm* dec)
{
  if (fDec)
    fDec->removeQueue(uniqueID);

  fDec = dec;

  if (fDec)
    fDec->addQueue(uniqueID, true);
}

void TupleBPS::abort_nolock()
{
  if (fDie)
    return;

  JobStep::abort();

  if (fDec && BPPIsAllocated)
  {
    SBS sbs{new ByteStream()};
    fBPP->abortProcessing(sbs.get());

    try
    {
      fDec->write(uniqueID, sbs);
    }
    catch (...)
    {
      // this throws only if there are no PMs left.  If there are none,
      // that is the cause of the abort and that will be reported to the
      // front-end already.  Nothing to do here.
    }

    BPPIsAllocated = false;
    fDec->shutdownQueue(uniqueID);
  }

  condvarWakeupProducer.notify_all();
  condvar.notify_all();
}

void TupleBPS::abort()
{
  boost::mutex::scoped_lock scoped(boost::mutex);
  abort_nolock();
}

template bool TupleBPS::processOneFilterType<int64_t>(int8_t colWidth, int64_t value, uint32_t type) const;
template bool TupleBPS::processOneFilterType<int128_t>(int8_t colWidth, int128_t value, uint32_t type) const;

template bool TupleBPS::processSingleFilterString<int64_t>(int8_t BOP, int8_t colWidth, int64_t val,
                                                           const uint8_t* filterString,
                                                           uint32_t filterCount) const;
template bool TupleBPS::processSingleFilterString<int128_t>(int8_t BOP, int8_t colWidth, int128_t val,
                                                            const uint8_t* filterString,
                                                            uint32_t filterCount) const;

template bool TupleBPS::compareSingleValue<int64_t>(uint8_t COP, int64_t val1, int64_t val2) const;
template bool TupleBPS::compareSingleValue<int128_t>(uint8_t COP, int128_t val1, int128_t val2) const;

}  // namespace joblist

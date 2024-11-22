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
// $Id: batchprimitiveprocessor.h 2132 2013-07-17 20:06:10Z pleblanc $
// C++ Interface: batchprimitiveprocessor
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#pragma once

#include <boost/scoped_array.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <unordered_map>
#include <boost/thread.hpp>

#include "countingallocator.h"
#include "errorcodes.h"
#include "serializeable.h"
#include "messagequeue.h"
#include "primitiveprocessor.h"
#include "command.h"
#include "umsocketselector.h"
#include "tuplejoiner.h"
#include "rowgroup.h"
#include "rowaggregation.h"
#include "funcexpwrapper.h"
#include "bppsendthread.h"
#include "columnwidth.h"

#ifdef PRIMPROC_STOPWATCH
#include "stopwatch.h"
#endif

namespace primitiveprocessor
{
typedef std::tr1::unordered_map<int64_t, BRM::VSSData> VSSCache;
};

#include "primitiveserver.h"

namespace primitiveprocessor
{
typedef boost::shared_ptr<BatchPrimitiveProcessor> SBPP;

class scalar_exception : public std::exception
{
  const char* what() const noexcept override
  {
    return "Not a scalar subquery.";
  }
};

class NeedToRestartJob : public std::runtime_error
{
 public:
  NeedToRestartJob() : std::runtime_error("NeedToRestartJob")
  {
  }
  NeedToRestartJob(const std::string& s) : std::runtime_error(s)
  {
  }
};

class BatchPrimitiveProcessor
{
 public:
  BatchPrimitiveProcessor(messageqcpp::ByteStream&, double prefetchThresh, boost::shared_ptr<BPPSendThread>,
                          uint processorThreads);

  ~BatchPrimitiveProcessor();

  /* Interface used by primproc */
  void initBPP(messageqcpp::ByteStream&);
  void resetBPP(messageqcpp::ByteStream&, const SP_UM_MUTEX& wLock, const SP_UM_IOSOCK& outputSock);
  void addToJoiner(messageqcpp::ByteStream&);
  int endOfJoiner();
  int operator()();
  void setLBIDForScan(uint64_t rid);

  /* Duplicate() returns a deep copy of this object as it was init'd by initBPP.
      It's thread-safe wrt resetBPP. */
  SBPP duplicate();

  /* These need to be updated */
  // bool operator==(const BatchPrimitiveProcessor&) const;
  // inline bool operator!=(const BatchPrimitiveProcessor& bpp) const
  //{
  //	return !(*this == bpp);
  //}

  inline uint32_t getSessionID()
  {
    return sessionID;
  }
  inline uint32_t getStepID()
  {
    return stepID;
  }
  inline uint32_t getUniqueID()
  {
    return uniqueID;
  }
  inline bool busy()
  {
    return fBusy;
  }
  inline void busy(bool b)
  {
    fBusy = b;
  }

  size_t getWeight() const
  {
    return weight_;
  }

  uint16_t FilterCount() const
  {
    return filterCount;
  }
  uint16_t ProjectCount() const
  {
    return projectCount;
  }
  uint32_t PhysIOCount() const
  {
    return physIO;
  }
  uint32_t CachedIOCount() const
  {
    return cachedIO;
  }
  uint32_t BlocksTouchedCount() const
  {
    return touchedBlocks;
  }

  void setError(const std::string& error, logging::ErrorCodeValues errorCode)
  {
  }

  // these two functions are used by BPPV to create BPP instances
  // on demand.  TRY not to use unlock() for anything else.
  void unlock()
  {
    pthread_mutex_unlock(&objLock);
  }
  bool hasJoin()
  {
    return doJoin;
  }
  primitives::PrimitiveProcessor& getPrimitiveProcessor()
  {
    return pp;
  }
  uint32_t getOutMsgSize() const
  {
    return outMsgSize;
  }

 private:
  BatchPrimitiveProcessor();
  BatchPrimitiveProcessor(const BatchPrimitiveProcessor&);
  BatchPrimitiveProcessor& operator=(const BatchPrimitiveProcessor&);

  void initProcessor();
#ifdef PRIMPROC_STOPWATCH
  void execute(logging::StopWatch* stopwatch);
#else
  void execute(messageqcpp::SBS& bs);
#endif
  void writeProjectionPreamble(messageqcpp::SBS& bs);
  void makeResponse(messageqcpp::SBS& bs);
  void sendResponse(messageqcpp::SBS& bs);
  /* Used by scan operations to increment the LBIDs in successive steps */
  void nextLBID();

  /* these send relative rids, should this be abs rids? */
  void serializeElementTypes(messageqcpp::SBS& bs);
  void serializeStrings(messageqcpp::SBS& bs);

  void asyncLoadProjectColumns();
  void writeErrorMsg(messageqcpp::SBS& bs, const std::string& error, uint16_t errCode, bool logIt = true, bool critical = true);

  BPSOutputType ot;

  BRM::QueryContext versionInfo;
  uint32_t txnID;
  uint32_t sessionID;
  uint32_t stepID;
  uint32_t uniqueID;

  // # of times to loop over the command arrays
  // ...  This is 1, except when the first command is a scan, in which case
  // this single BPP object produces count responses.
  uint16_t count;
  uint64_t baseRid;  // first rid of the logical block

  uint16_t relRids[LOGICAL_BLOCK_RIDS];
  int64_t values[LOGICAL_BLOCK_RIDS];
  int128_t wide128Values[LOGICAL_BLOCK_RIDS];
  boost::scoped_array<uint64_t> absRids;
  boost::scoped_array<utils::NullString> strValues;
  uint16_t origRidCount;
  uint16_t ridCount;
  bool needStrValues;
  uint16_t wideColumnsWidths;

  /* Common space for primitive data */
  alignas(utils::MAXCOLUMNWIDTH) uint8_t blockData[BLOCK_SIZE * utils::MAXCOLUMNWIDTH];
  uint8_t blockDataAux[BLOCK_SIZE * execplan::AUX_COL_WIDTH];
  std::unique_ptr<uint8_t[], utils::AlignedDeleter> outputMsg;
  uint32_t outMsgSize;

  std::vector<SCommand> filterSteps;
  std::vector<SCommand> projectSteps;
  //@bug 1136
  uint16_t filterCount;
  uint16_t projectCount;
  bool sendRidsAtDelivery;
  uint16_t ridMap;
  bool gotAbsRids;
  bool gotValues;

  bool hasScan;
  bool validCPData;
  // CP data from a scanned column
  union
  {
    int128_t min128Val;
    int64_t minVal;
  };
  union
  {
    int128_t max128Val;
    int64_t maxVal;
  };
  bool cpDataFromDictScan;

  uint64_t lbidForCP;
  bool hasWideColumnOut;
  uint8_t wideColumnWidthOut;
  // IO counters
  boost::mutex counterLock;
  uint32_t busyLoaderCount;

  uint32_t physIO, cachedIO, touchedBlocks;

  SP_UM_IOSOCK sock;
  // messageqcpp::SBS serialized;
  SP_UM_MUTEX writelock;

  // MCOL-744 using pthread mutex instead of Boost mutex because
  // in it is possible that this lock could be unlocked when it is
  // already unlocked. In Ubuntu 16.04's Boost this triggers a
  // crash. Whilst it is very hard to hit this it is still bad.
  // Longer term TODO: fix/remove objLock and/or refactor BPP
  pthread_mutex_t objLock;
  bool LBIDTrace;
  bool fBusy;

  /* Join support TODO: Make join ops a seperate Command class. */
  bool doJoin;
  boost::scoped_array<boost::scoped_array<boost::mutex>> addToJoinerLocks;
  boost::scoped_array<boost::mutex> smallSideDataLocks;

  // 		uint32_t ridsIn, ridsOut;

  //@bug 1051 FilterStep on PM
  bool hasFilterStep;
  bool filtOnString;
  boost::scoped_array<uint16_t> fFiltCmdRids[2];
  boost::scoped_array<int64_t> fFiltCmdValues[2];
  boost::scoped_array<int128_t> fFiltCmdBinaryValues[2];
  boost::scoped_array<utils::NullString> fFiltStrValues[2];
  uint64_t fFiltRidCount[2];

  // query density threshold for prefetch & async loading
  double prefetchThreshold;

  /* RowGroup support */
  rowgroup::RowGroup outputRG;
  boost::scoped_ptr<rowgroup::RGData> outRowGroupData;
  std::shared_ptr<int[]> rgMap;          // maps input cols to output cols
  std::shared_ptr<int[]> projectionMap;  // maps the projection steps to the output RG
  bool hasRowGroup;

  /* Rowgroups + join */
  // typedef std::unordered_multimap<uint64_t, uint32_t, joiner::TupleJoiner::hasher,
  //                                      std::equal_to<uint64_t>,
  //                                      utils::STLPoolAllocator<std::pair<const uint64_t, uint32_t>>>
  //     TJoiner;
  using TJoiner =
      std::unordered_multimap<uint64_t, uint32_t, joiner::TupleJoiner::hasher, std::equal_to<uint64_t>,
                              allocators::CountingAllocator<std::pair<const uint64_t, uint32_t>>>;

  // typedef std::unordered_multimap<
  //     joiner::TypelessData, uint32_t, joiner::TupleJoiner::TypelessDataHasher,
  //     joiner::TupleJoiner::TypelessDataComparator,
  //     utils::STLPoolAllocator<std::pair<const joiner::TypelessData, uint32_t>>>
  //     TLJoiner;
  using TLJoiner =
      std::unordered_multimap<joiner::TypelessData, uint32_t, joiner::TupleJoiner::TypelessDataHasher,
                              joiner::TupleJoiner::TypelessDataComparator,
                              allocators::CountingAllocator<std::pair<const joiner::TypelessData, uint32_t>>>;

  bool generateJoinedRowGroup(rowgroup::Row& baseRow, const uint32_t depth = 0);
  /* generateJoinedRowGroup helper fcns & vars */
  void initGJRG();   // called once after joining
  void resetGJRG();  // called after every rowgroup returned by generateJRG
  boost::scoped_array<uint32_t> gjrgPlaceHolders;
  uint32_t gjrgRowNumber;
  bool gjrgFull;
  rowgroup::Row largeRow, joinedRow, baseJRow;
  boost::scoped_array<uint8_t> baseJRowMem;
  boost::scoped_ptr<rowgroup::RGData> joinedRGMem;
  boost::scoped_array<rowgroup::Row> smallRows;
  std::shared_ptr<std::shared_ptr<int[]>[]> gjrgMappings;

  std::shared_ptr<std::shared_ptr<boost::shared_ptr<TJoiner>[]>[]> tJoiners;
  typedef std::vector<uint32_t> MatchedData[LOGICAL_BLOCK_RIDS];
  std::shared_ptr<MatchedData[]> tSmallSideMatches;
  uint32_t executeTupleJoin(uint32_t startRid, rowgroup::RowGroup& largeSideRowGroup);
  bool getTupleJoinRowGroupData;
  std::vector<rowgroup::RowGroup> smallSideRGs;
  rowgroup::RowGroup largeSideRG;
  std::shared_ptr<rowgroup::RGData[]> smallSideRowData;
  std::shared_ptr<rowgroup::RGData[]> smallNullRowData;
  std::shared_ptr<rowgroup::Row::Pointer[]> smallNullPointers;
  std::shared_ptr<uint64_t[]> ssrdPos;  // this keeps track of position when building smallSideRowData
  std::shared_ptr<uint32_t[]> smallSideRowLengths;
  std::shared_ptr<joblist::JoinType[]> joinTypes;
  uint32_t joinerCount;
  std::shared_ptr<std::atomic<uint32_t>[]> tJoinerSizes;
  // LSKC[i] = the column in outputRG joiner i uses as its key column
  std::shared_ptr<uint32_t[]> largeSideKeyColumns;
  // KCPP[i] = true means a joiner uses projection step i as a key column
  std::shared_ptr<bool[]> keyColumnProj;
  rowgroup::Row oldRow, newRow;  // used by executeTupleJoin()
  std::shared_ptr<uint64_t[]> joinNullValues;
  std::shared_ptr<bool[]> doMatchNulls;
  boost::scoped_array<boost::scoped_ptr<funcexp::FuncExpWrapper>> joinFEFilters;
  bool hasJoinFEFilters;
  bool hasSmallOuterJoin;

  /* extra typeless join vars & fcns*/
  std::shared_ptr<bool[]> typelessJoin;
  std::shared_ptr<std::vector<uint32_t>[]> tlLargeSideKeyColumns;
  std::shared_ptr<std::vector<uint32_t>> tlSmallSideKeyColumns;
  std::shared_ptr<std::shared_ptr<boost::shared_ptr<TLJoiner>[]>[]> tlJoiners;
  std::shared_ptr<uint32_t[]> tlSmallSideKeyLengths;
  // True if smallSide and largeSide TypelessData key column differs,e.g BIGINT vs DECIMAL(38).
  bool mJOINHasSkewedKeyColumn;
  const rowgroup::RowGroup* mSmallSideRGPtr;
  const std::vector<uint32_t>* mSmallSideKeyColumnsPtr;

  inline void getJoinResults(const rowgroup::Row& r, uint32_t jIndex, std::vector<uint32_t>& v);
  // these allocators hold the memory for the keys stored in tlJoiners
  std::shared_ptr<utils::PoolAllocator[]> storedKeyAllocators;

  /* PM Aggregation */
  rowgroup::RowGroup joinedRG;  // if there's a join, the rows are formatted with this
  rowgroup::SP_ROWAGG_PM_t fAggregator;
  rowgroup::RowGroup fAggregateRG;
  rowgroup::RGData fAggRowGroupData;
  // boost::scoped_array<uint8_t> fAggRowGroupData;

  /* OR hacks */
  uint8_t bop;  // BOP_AND or BOP_OR
  bool hasPassThru;
  uint8_t forHJ;

  boost::scoped_ptr<funcexp::FuncExpWrapper> fe1, fe2;
  rowgroup::RowGroup fe1Input, fe2Output, *fe2Input;
  // note, joinFERG is only for metadata, and is shared between BPPs
  boost::shared_ptr<rowgroup::RowGroup> joinFERG;
  boost::scoped_array<uint8_t> joinFERowData;
  boost::scoped_ptr<rowgroup::RGData> fe1Data,
      fe2Data;  // can probably make these RGDatas not pointers to RGDatas
  std::shared_ptr<int[]> projectForFE1;
  std::shared_ptr<int[]> fe1ToProjection, fe2Mapping;  // RG mappings
  boost::scoped_array<std::shared_ptr<int[]>> joinFEMappings;
  rowgroup::Row fe1In, fe1Out, fe2In, fe2Out, joinFERow;

  bool hasDictStep;

  primitives::PrimitiveProcessor pp;

  /* VSS cache members */
  VSSCache vssCache;
  void buildVSSCache(uint32_t loopCount);

  /* To support limited DEC queues on the PM */
  boost::shared_ptr<BPPSendThread> sendThread;
  bool newConnection;  // to support the load balancing code in sendThread

  /* To support reentrancy */
  uint32_t currentBlockOffset;
  boost::scoped_array<uint64_t> relLBID;
  boost::scoped_array<bool> asyncLoaded;

  /* To support a smaller memory footprint when idle */
  static const uint64_t maxIdleBufferSize = 16 * 1024 * 1024;  // arbitrary
  void allocLargeBuffers();
  void freeLargeBuffers();

  /* To ensure all packets of an LBID go out the same socket */
  int sockIndex;

  /* Shared nothing vars */
  uint32_t dbRoot;

  bool endOfJoinerRan;
  /* Some addJoiner() profiling stuff */
  boost::posix_time::ptime firstCallTime;
  utils::Hasher_r bucketPicker;
  const uint32_t bpSeed = 0xf22df448;  // an arbitrary random #
  uint processorThreads;
  uint ptMask;
  bool firstInstance;
  uint64_t valuesLBID;
  bool initiatedByEM_;
  uint32_t weight_;

  uint32_t maxPmJoinResultCount = 1048576;
  friend class Command;
  friend class ColumnCommand;
  friend class DictStep;
  friend class PassThruCommand;
  friend class RTSCommand;
  friend class FilterCommand;
  friend class ScaledFilterCmd;
  friend class StrFilterCmd;
  friend class PseudoCC;
};

}  // namespace primitiveprocessor

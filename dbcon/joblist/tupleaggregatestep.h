/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporation.

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

//  $Id: tupleaggregatestep.h 9732 2013-08-02 15:56:15Z pleblanc $

#pragma once

#include "jobstep.h"
#include "rowaggregation.h"
#include "threadnaming.h"
#include "../../primitives/primproc/primitiveserverthreadpools.h"

#include <boost/thread.hpp>

namespace joblist
{
// forward reference
struct JobInfo;

struct cmpTuple
{
  bool operator()(boost::tuple<uint32_t, int, mcsv1sdk::mcsv1_UDAF*, std::vector<uint32_t>*> a,
                  boost::tuple<uint32_t, int, mcsv1sdk::mcsv1_UDAF*, std::vector<uint32_t>*> b) const
  {
    uint32_t keya = boost::get<0>(a);
    uint32_t keyb = boost::get<0>(b);
    int opa;
    int opb;
    mcsv1sdk::mcsv1_UDAF* pUDAFa;
    mcsv1sdk::mcsv1_UDAF* pUDAFb;

    // If key is less than
    if (keya < keyb)
      return true;
    if (keya == keyb)
    {
      // test Op
      opa = boost::get<1>(a);
      opb = boost::get<1>(b);
      if (opa < opb)
        return true;
      if (opa == opb)
      {
        // look at the UDAF object
        pUDAFa = boost::get<2>(a);
        pUDAFb = boost::get<2>(b);
        if (pUDAFa < pUDAFb)
          return true;
        if (pUDAFa == pUDAFb)
        {
          std::vector<uint32_t>* paramKeysa = boost::get<3>(a);
          std::vector<uint32_t>* paramKeysb = boost::get<3>(b);
          if (paramKeysa == NULL || paramKeysb == NULL)
            return false;
          if (paramKeysa->size() < paramKeysb->size())
            return true;
          if (paramKeysa->size() == paramKeysb->size())
          {
            for (uint64_t i = 0; i < paramKeysa->size(); ++i)
            {
              if ((*paramKeysa)[i] < (*paramKeysb)[i])
                return true;
            }
          }
        }
      }
    }
    return false;
  }
};

// The AGG_MAP type is used to maintain a list of aggregate functions in order to
// detect duplicates. Since all UDAF have the same op type (ROWAGG_UDAF), we add in
// the function pointer in order to ensure uniqueness.
using AGG_MAP =
    map<boost::tuple<uint32_t, int, mcsv1sdk::mcsv1_UDAF*, std::vector<uint32_t>*>, uint64_t, cmpTuple>;

/** @brief class TupleAggregateStep
 *
 */
class TupleAggregateStep : public JobStep, public TupleDeliveryStep
{
 public:
  /** @brief TupleAggregateStep constructor
   */
  TupleAggregateStep(const rowgroup::SP_ROWAGG_UM_t&, const rowgroup::RowGroup&, const rowgroup::RowGroup&,
                     const JobInfo&);

  /** @brief TupleAggregateStep destructor
   */
  ~TupleAggregateStep();

  /** @brief virtual void Run method
   */
  void run();
  void join();

  const std::string toString() const;

  void setOutputRowGroup(const rowgroup::RowGroup&);
  const rowgroup::RowGroup& getOutputRowGroup() const;
  const rowgroup::RowGroup& getDeliveredRowGroup() const;
  void deliverStringTableRowGroup(bool b);
  bool deliverStringTableRowGroup() const;
  uint32_t nextBand(messageqcpp::ByteStream& bs);
  uint32_t nextBand_singleThread(messageqcpp::ByteStream& bs);
  bool setPmHJAggregation(JobStep* step);
  void savePmHJData(rowgroup::SP_ROWAGG_t&, rowgroup::SP_ROWAGG_t&, rowgroup::RowGroup&);

  bool umOnly() const
  {
    return fUmOnly;
  }
  void umOnly(bool b)
  {
    fUmOnly = b;
  }

  void configDeliveredRowGroup(const JobInfo&);
  // void setEidMap(std::map<int, int>& m) { fIndexEidMap = m; }

  static SJSTEP prepAggregate(SJSTEP&, JobInfo&);

  // for multi-thread variables
  void initializeMultiThread();

 private:
  static void prep1PhaseDistinctAggregate(JobInfo&, std::vector<rowgroup::RowGroup>&,
                                          std::vector<rowgroup::SP_ROWAGG_t>&);
  static void prep1PhaseAggregate(JobInfo&, std::vector<rowgroup::RowGroup>&,
                                  std::vector<rowgroup::SP_ROWAGG_t>&);
  static void prep2PhasesAggregate(JobInfo&, std::vector<rowgroup::RowGroup>&,
                                   std::vector<rowgroup::SP_ROWAGG_t>&);
  static void prep2PhasesDistinctAggregate(JobInfo&, std::vector<rowgroup::RowGroup>&,
                                           std::vector<rowgroup::SP_ROWAGG_t>&);

  void prepExpressionOnAggregate(rowgroup::SP_ROWAGG_UM_t&, JobInfo&);
  void addConstangAggregate(std::vector<rowgroup::ConstantAggData>&);

  void doAggregate();
  void doAggregate_singleThread();
  uint64_t doThreadedAggregate(messageqcpp::ByteStream& bs, RowGroupDL* dlp);
  void aggregateRowGroups();
  void threadedAggregateRowGroups(uint32_t threadID);
  void threadedAggregateFinalize(uint32_t threadID);
  void doThreadedSecondPhaseAggregate(uint32_t threadID);
  bool nextDeliveredRowGroup();
  void pruneAuxColumns();
  bool cleanUpAndOutputRowGroup(messageqcpp::ByteStream& bs, RowGroupDL* dlp);
  void formatMiniStats();
  void printCalTrace();
  template <class GroupByMap>
  static bool tryToFindEqualFunctionColumnByTupleKey(JobInfo& jobInfo, GroupByMap& groupByMap,
                                                     const uint32_t tupleKey, uint32_t& foundTypleKey);
  // This functions are workaround for the function above. For some reason different parts of the code with same
  // semantics use different containers.
  static uint32_t getTupleKeyFromTuple(const boost::tuple<uint32_t, int, mcsv1sdk::mcsv1_UDAF*, std::vector<uint32_t>*>& tuple);
  static uint32_t getTupleKeyFromTuple(uint32_t key);

  boost::shared_ptr<execplan::CalpontSystemCatalog> fCatalog;
  uint64_t fRowsReturned;
  bool fDoneAggregate;
  bool fEndOfResult;

  rowgroup::SP_ROWAGG_UM_t fAggregator;
  rowgroup::RowGroup fRowGroupOut;
  rowgroup::RowGroup fRowGroupDelivered;
  rowgroup::RGData fRowGroupData;

  // for setting aggregate column eid in delivered rowgroup
  // std::map<int, int> fIndexEidMap;

  // data from RowGroupDL
  rowgroup::RowGroup fRowGroupIn;

  // for PM HashJoin
  // PM hashjoin is selected at runtime, prepare for it anyway.
  rowgroup::SP_ROWAGG_UM_t fAggregatorUM;
  rowgroup::SP_ROWAGG_PM_t fAggregatorPM;
  rowgroup::RowGroup fRowGroupPMHJ;

  // for run thread (first added for union)
  class Aggregator
  {
   public:
    Aggregator(TupleAggregateStep* step) : fStep(step)
    {
    }
    void operator()()
    {
      utils::setThreadName("TASAggr");
      fStep->doAggregate();
    }

    TupleAggregateStep* fStep;
  };

  class ThreadedAggregator
  {
   public:
    ThreadedAggregator(TupleAggregateStep* step, uint32_t threadID) : fStep(step), fThreadID(threadID)
    {
    }
    void operator()()
    {
      std::string t{"TASThrAggr"};
      t.append(std::to_string(fThreadID));
      utils::setThreadName(t.c_str());
      fStep->threadedAggregateRowGroups(fThreadID);
    }

    TupleAggregateStep* fStep;
    uint32_t fThreadID;
  };

  class ThreadedAggregateFinalizer
  {
   public:
    ThreadedAggregateFinalizer(TupleAggregateStep* step, uint32_t threadID) : fStep(step), fThreadID(threadID)
    {
    }

    void operator()()
    {
      std::string t{"TASThrFin"};
      t.append(std::to_string(fThreadID));
      utils::setThreadName(t.c_str());
      fStep->threadedAggregateFinalize(fThreadID);
    }

    TupleAggregateStep* fStep;
    uint32_t fThreadID;
  };

  class ThreadedSecondPhaseAggregator
  {
   public:
    ThreadedSecondPhaseAggregator(TupleAggregateStep* step, uint32_t threadID, uint32_t bucketsPerThread)
     : fStep(step), fThreadID(threadID), bucketCount(bucketsPerThread)
    {
    }
    void operator()()
    {
      utils::setThreadName("TASThr2ndPAggr");
      for (uint32_t i = 0; i < bucketCount; i++)
        fStep->doThreadedSecondPhaseAggregate(fThreadID + i);
    }
    TupleAggregateStep* fStep;
    uint32_t fThreadID;
    uint32_t bucketCount;
  };

  uint64_t fRunner;  // thread pool handle
  bool fUmOnly;
  ResourceManager* fRm;

  // multi-threaded
  uint32_t fNumOfThreads;
  uint32_t fNumOfBuckets;
  uint32_t fNumOfRowGroups;
  uint32_t fBucketNum;

  boost::mutex fMutex;
  std::vector<boost::mutex*> fAgg_mutex;
  std::vector<rowgroup::RGData> fRowGroupDatas;
  std::vector<rowgroup::SP_ROWAGG_UM_t> fAggregators;
  std::vector<rowgroup::RowGroup> fRowGroupIns;
  std::vector<rowgroup::RowGroup> fRowGroupOuts;
  std::vector<std::vector<rowgroup::RGData> > fRowGroupsDeliveredData;
  bool fIsMultiThread;
  int fInputIter;  // iterator
  boost::scoped_array<uint64_t> fMemUsage;

  boost::shared_ptr<int64_t> fSessionMemLimit;

  PrimitiveServerThreadPools fPrimitiveServerThreadPools;
};

}  // namespace joblist

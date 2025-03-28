/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporaton.

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

//  $Id: tupleannexstep.h 9596 2013-06-04 19:59:04Z xlou $

#pragma once

#include <queue>
#include <boost/thread/thread.hpp>

#include "jobstep.h"
#include "limitedorderby.h"

namespace joblist
{
class TupleConstantStep;
class LimitedOrderBy;
}  // namespace joblist

namespace joblist
{
/** @brief class TupleAnnexStep
 *
 */
class TupleAnnexStep : public JobStep, public TupleDeliveryStep
{
 public:
  /** @brief TupleAnnexStep constructor
   */
  explicit TupleAnnexStep(const JobInfo& jobInfo);
  // Copy ctor to have a class mutex
  TupleAnnexStep(const TupleAnnexStep& copy);

  /** @brief TupleAnnexStep destructor
   */
  ~TupleAnnexStep() override;

  // inherited methods
  void run() override;
  void join() override;
  const std::string toString() const override;

  /** @brief TupleJobStep's pure virtual methods
   */
  const rowgroup::RowGroup& getOutputRowGroup() const override;
  void setOutputRowGroup(const rowgroup::RowGroup&) override;

  /** @brief TupleDeliveryStep's pure virtual methods
   */
  uint32_t nextBand(messageqcpp::ByteStream& bs) override;
  const rowgroup::RowGroup& getDeliveredRowGroup() const override;
  void deliverStringTableRowGroup(bool b) override;
  bool deliverStringTableRowGroup() const override;

  void initialize(const rowgroup::RowGroup& rgIn, const JobInfo& jobInfo);

  void addOrderBy(LimitedOrderBy* lob)
  {
    fOrderBy = lob;
  }
  void addConstant(TupleConstantStep* tcs)
  {
    fConstant = tcs;
  }
  void setDistinct()
  {
    fDistinct = true;
  }
  void setLimit(uint64_t s, uint64_t c)
  {
    fLimitStart = s;
    fLimitCount = c;
  }
  void setParallelOp()
  {
    fParallelOp = true;
  }
  void setMaxThreads(uint32_t number)
  {
    fMaxThreads = number;
  }

  bool stringTableFriendly() override
  {
    return true;
  }

  rowgroup::Row row1, row2;  // scratch space for distinct comparisons todo: make them private

 protected:
  void execute();
  void execute(uint32_t);
  void executeNoOrderBy();
  void executeWithOrderBy();
  void executeParallelOrderBy(uint64_t id);
  void executeNoOrderByWithDistinct();
  void checkAndAllocateMemory4RGData(const rowgroup::RowGroup& rowGroup);
  void formatMiniStats();
  void printCalTrace();
  void finalizeParallelOrderBy();
  void finalizeParallelOrderByDistinct();

  // input/output rowgroup and row
  rowgroup::RowGroup fRowGroupIn;
  rowgroup::RowGroup fRowGroupOut;
  rowgroup::RowGroup fRowGroupDeliver;
  rowgroup::RGData fRgDataOut;
  rowgroup::Row fRowIn;
  rowgroup::Row fRowOut;

  // for datalist
  RowGroupDL* fInputDL;
  RowGroupDL* fOutputDL;
  uint64_t fInputIterator;
  std::vector<uint64_t> fInputIteratorsList;
  uint64_t fOutputIterator;

  class Runner
  {
   public:
    explicit Runner(TupleAnnexStep* step) : fStep(step), id(0)
    {
    }
    Runner(TupleAnnexStep* step, uint32_t id) : fStep(step), id(id)
    {
    }
    void operator()()
    {
      if (id)
        fStep->execute(id);
      else
        fStep->execute();
    }

    TupleAnnexStep* fStep;
    uint16_t id;
  };
  uint64_t fRunner;  // thread pool handle

  uint64_t fRowsProcessed;
  uint64_t fRowsReturned;
  uint64_t fLimitStart;
  uint64_t fLimitCount;
  uint64_t fMaxThreads;
  bool fLimitHit;
  bool fEndOfResult;
  bool fDistinct;
  bool fParallelOp;

  LimitedOrderBy* fOrderBy;
  TupleConstantStep* fConstant;

  funcexp::FuncExp* fFeInstance;
  JobList* fJobList;

  std::vector<LimitedOrderBy*> fOrderByList;
  std::vector<uint64_t> fRunnersList;
  uint16_t fFinishedThreads;
  boost::mutex fParallelFinalizeMutex;
  joblist::ResourceManager* fRm;
};

}  // namespace joblist

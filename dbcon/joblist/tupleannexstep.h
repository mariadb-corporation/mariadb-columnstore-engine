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

#ifndef JOBLIST_TUPLEANNEXSTEP_H
#define JOBLIST_TUPLEANNEXSTEP_H

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
  TupleAnnexStep(const JobInfo& jobInfo);
  // Copy ctor to have a class mutex
  TupleAnnexStep(const TupleAnnexStep& copy);

  /** @brief TupleAnnexStep destructor
   */
  ~TupleAnnexStep();

  // inherited methods
  void run();
  void join();
  const std::string toString() const;

  /** @brief TupleJobStep's pure virtual methods
   */
  const rowgroup::RowGroup& getOutputRowGroup() const;
  void setOutputRowGroup(const rowgroup::RowGroup&);

  /** @brief TupleDeliveryStep's pure virtual methods
   */
  uint32_t nextBand(messageqcpp::ByteStream& bs);
  const rowgroup::RowGroup& getDeliveredRowGroup() const;
  void deliverStringTableRowGroup(bool b);
  bool deliverStringTableRowGroup() const;

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

  virtual bool stringTableFriendly()
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
  void formatMiniStats();
  void printCalTrace();
  void finalizeParallelOrderBy();
  void finalizeParallelOrderByDistinct();

  // input/output rowgroup and row
  rowgroup::RowGroup fRowGroupIn;
  rowgroup::RowGroup fRowGroupOut;
  rowgroup::RowGroup fRowGroupDeliver;
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
    Runner(TupleAnnexStep* step) : fStep(step), id(0)
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
};

template <class T>
class reservablePQ : private std::priority_queue<T>
{
 public:
  typedef typename std::priority_queue<T>::size_type size_type;
  reservablePQ(size_type capacity = 0)
  {
    reserve(capacity);
  };
  void reserve(size_type capacity)
  {
    this->c.reserve(capacity);
  }
  size_type capacity() const
  {
    return this->c.capacity();
  }
};

}  // namespace joblist

#endif  // JOBLIST_TUPLEANNEXSTEP_H

// vim:ts=4 sw=4:

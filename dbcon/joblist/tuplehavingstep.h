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

//  $Id: tuplehavingstep.h 9596 2013-06-04 19:59:04Z xlou $

#pragma once

#include "jobstep.h"
#include "expressionstep.h"
#include "threadnaming.h"

// forward reference
namespace fucexp
{
class FuncExp;
}

namespace joblist
{
/** @brief class TupleHavingStep
 *
 */
class TupleHavingStep : public ExpressionStep, public TupleDeliveryStep
{
 public:
  /** @brief TupleHavingStep constructor
   */
  explicit TupleHavingStep(const JobInfo& jobInfo);

  /** @brief TupleHavingStep destructor
   */
  ~TupleHavingStep() override;

  /** @brief virtual void Run method
   */
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
  using ExpressionStep::expressionFilter;
  void expressionFilter(const execplan::ParseTree* filter, JobInfo& jobInfo) override;

  bool stringTableFriendly() override
  {
    return true;
  }

 protected:
  void execute();
  void doHavingFilters();
  void formatMiniStats();
  void printCalTrace();

  // input/output rowgroup and row
  rowgroup::RowGroup fRowGroupIn;
  rowgroup::RowGroup fRowGroupOut;
  rowgroup::Row fRowIn;
  rowgroup::Row fRowOut;

  // for datalist
  RowGroupDL* fInputDL;
  RowGroupDL* fOutputDL;
  uint64_t fInputIterator;

  class Runner
  {
   public:
    explicit Runner(TupleHavingStep* step) : fStep(step)
    {
    }
    void operator()()
    {
      utils::setThreadName("HVSRunner");
      fStep->execute();
    }

    TupleHavingStep* fStep;
  };

  uint64_t fRunner;  // thread pool handle

  uint64_t fRowsReturned;
  bool fEndOfResult;

  funcexp::FuncExp* fFeInstance;
};

}  // namespace joblist

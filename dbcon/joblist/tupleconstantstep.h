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

//  $Id: tupleconstantstep.h 9596 2013-06-04 19:59:04Z xlou $

#pragma once

#include "jobstep.h"
#include "threadnaming.h"

namespace joblist
{
/** @brief class TupleConstantStep
 *
 */
class TupleConstantStep : public JobStep, public TupleDeliveryStep
{
 public:
  /** @brief TupleConstantStep constructor
   */
  explicit TupleConstantStep(const JobInfo& jobInfo);

  /** @brief TupleConstantStep destructor
   */
  ~TupleConstantStep() override;

  /** @brief virtual void Run method
   */
  void run() override;

  /** @brief virtual void join method
   */
  void join() override;

  /** @brief virtual string toString method
   */
  const std::string toString() const override;

  void setOutputRowGroup(const rowgroup::RowGroup&) override;
  const rowgroup::RowGroup& getOutputRowGroup() const override;
  const rowgroup::RowGroup& getDeliveredRowGroup() const override;
  void deliverStringTableRowGroup(bool b) override;
  bool deliverStringTableRowGroup() const override;
  uint32_t nextBand(messageqcpp::ByteStream& bs) override;

  virtual void initialize(const JobInfo& jobInfo, const rowgroup::RowGroup* rgIn);
  virtual void fillInConstants(const rowgroup::Row& rowIn, rowgroup::Row& rowOut);
  static SJSTEP addConstantStep(const JobInfo& jobInfo, const rowgroup::RowGroup* rg = nullptr);

 protected:
  virtual void execute();
  virtual void fillInConstants();
  virtual void formatMiniStats();
  virtual void printCalTrace();
  virtual void constructContanstRow(const JobInfo& jobInfo);

  // for base
  uint64_t fRowsReturned;

  // input/output rowgroup and row
  rowgroup::RowGroup fRowGroupIn;
  rowgroup::RowGroup fRowGroupOut;
  rowgroup::Row fRowIn;
  rowgroup::Row fRowOut;

  // mapping
  std::vector<uint64_t> fIndexConst;    // consts in output row
  std::vector<uint64_t> fIndexMapping;  // from input row to output row

  // store constants
  rowgroup::Row fRowConst;
  boost::scoped_array<uint8_t> fConstRowData;

  // for datalist
  RowGroupDL* fInputDL;
  RowGroupDL* fOutputDL;
  uint64_t fInputIterator;

  class Runner
  {
   public:
    explicit Runner(TupleConstantStep* step) : fStep(step)
    {
    }
    void operator()()
    {
      utils::setThreadName("TCSRunner");
      fStep->execute();
    }

    TupleConstantStep* fStep;
  };

  uint64_t fRunner;  // thread pool handle
  bool fEndOfResult;
};

class TupleConstantOnlyStep : public TupleConstantStep
{
 public:
  /** @brief TupleConstantOnlyStep constructor
   */
  explicit TupleConstantOnlyStep(const JobInfo& jobInfo);

  /** @brief TupleConstantOnlyStep destructor
   */
  ~TupleConstantOnlyStep() override;

  /** @brief virtual void Run method
   */
  void run() override;

  /** @brief virtual void initialize method
   */
  void initialize(const JobInfo& jobInfo, const rowgroup::RowGroup* rgIn) override;

  const std::string toString() const override;
  uint32_t nextBand(messageqcpp::ByteStream& bs) override;

 protected:
  bool fEmptySet;
  using TupleConstantStep::fillInConstants;
  void fillInConstants() override;
};

class TupleConstantBooleanStep : public TupleConstantStep
{
 public:
  /** @brief TupleConstantBooleanStep constructor
   */
  TupleConstantBooleanStep(const JobInfo& jobInfo, bool value);

  /** @brief TupleConstantBooleanStep destructor
   */
  ~TupleConstantBooleanStep() override;

  /** @brief virtual void Run method
   */
  void run() override;

  /** @brief virtual void initialize method
      For some reason, this doesn't match the base class's virtual signature.
      However (for now), it's ok, because it's only called in one place and
      doesn't need to be virtual there.
   */
  using TupleConstantStep::initialize;
  void initialize(const rowgroup::RowGroup& rgIn, const JobInfo& jobInfo);

  const std::string toString() const override;
  uint32_t nextBand(messageqcpp::ByteStream& bs) override;

  virtual void boolValue(bool b)
  {
    fValue = b;
  }
  virtual bool boolValue() const
  {
    return fValue;
  }

 protected:
  void execute() override
  {
  }
  using TupleConstantStep::fillInConstants;
  void fillInConstants() override
  {
  }
  void constructContanstRow(const JobInfo& /*jobInfo*/) override
  {
  }
  // boolean value
  bool fValue;
};

}  // namespace joblist

/*
 Copyright (C) 2023 MariaDB Corporation
 This program is free software; you can redistribute it and/or modify it under the terms of the GNU General
 Public License as published by the Free Software Foundation; version 2 of the License. This program is
 distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 You should have received a copy of the GNU General Public License along with this program; if not, write to
 the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
*/

#pragma once

#include "jobstep.h"
#include "threadnaming.h"

namespace joblist
{
class TupleConstantBooleanStep : public JobStep, public TupleDeliveryStep
{
 public:
  /** @brief TupleConstantBooleanStep constructor
   */
  TupleConstantBooleanStep(const JobInfo& jobInfo, bool value);

  /** @brief TupleConstantBooleanStep destructor
   */
  ~TupleConstantBooleanStep();

  /** @brief virtual void Run method
   */
  void run();

  void join();

  /** @brief virtual void initialize method
      For some reason, this doesn't match the base class's virtual signature.
      However (for now), it's ok, because it's only called in one place and
      doesn't need to be virtual there.
   */
  void initialize(const rowgroup::RowGroup& rgIn, const JobInfo& jobInfo);

  void setOutputRowGroup(const rowgroup::RowGroup&);
  const rowgroup::RowGroup& getOutputRowGroup() const;
  const rowgroup::RowGroup& getDeliveredRowGroup() const;
  void deliverStringTableRowGroup(bool b);
  bool deliverStringTableRowGroup() const;
  uint32_t nextBand(messageqcpp::ByteStream& bs);

  const std::string toString() const;

  virtual void boolValue(bool b)
  {
    value_ = b;
  }
  virtual bool boolValue() const
  {
    return value_;
  }

 protected:
  void execute()
  {
  }
  void fillInConstants()
  {
  }

  virtual void formatMiniStats();
  virtual void printCalTrace();

  // input/output rowgroup and row
  rowgroup::RowGroup rowGroupOut_;
  rowgroup::Row rowOut_;

  // store constants
  rowgroup::Row rowConst_;

  // for datalist
  RowGroupDL* outputDL_ = nullptr;

  // boolean value
  bool value_;
};

}  // namespace joblist

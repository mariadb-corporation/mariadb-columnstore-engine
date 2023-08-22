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
SJSTEP addConstantStep(const JobInfo& jobInfo, const rowgroup::RowGroup* rg = nullptr);

class TupleConstantOnlyStep : public JobStep, public TupleDeliveryStep
{
 public:
  /** @brief TupleConstantOnlyStep constructor
   */
  TupleConstantOnlyStep(const JobInfo& jobInfo);

  /** @brief TupleConstantOnlyStep destructor
   */
  ~TupleConstantOnlyStep();

  /** @brief virtual void Run method
   */
  void run();

  void join();

  /** @brief virtual void initialize method
   */
  virtual void initialize(const JobInfo& jobInfo, const rowgroup::RowGroup* rgIn);

  void setOutputRowGroup(const rowgroup::RowGroup&);
  const rowgroup::RowGroup& getOutputRowGroup() const;
  const rowgroup::RowGroup& getDeliveredRowGroup() const;
  void deliverStringTableRowGroup(bool b);
  bool deliverStringTableRowGroup() const;
  uint32_t nextBand(messageqcpp::ByteStream& bs);

  const std::string toString() const;

  virtual void execute();
  void fillInConstants();

  virtual void formatMiniStats();
  virtual void printCalTrace();

  void constructContanstRow(const JobInfo& jobInfo);

  // for base
  uint64_t rowsReturned_ = 0;

  // input/output rowgroup and row
  rowgroup::RowGroup rowGroupIn_;
  rowgroup::RowGroup rowGroupOut_;

  rowgroup::Row rowOut_;

  // mapping
  std::vector<uint64_t> indexConst_;  // consts in output row

  // store constants
  rowgroup::Row rowConst_;
  boost::scoped_array<uint8_t> constRowData_;

  // for datalist
  RowGroupDL* inputDL_ = nullptr;
  RowGroupDL* outputDL_ = nullptr;
  uint64_t inputIterator_ = 0;

  bool endOfResult_ = false;
};

}  // namespace joblist
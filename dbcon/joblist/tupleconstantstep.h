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

#include <queue>
#include <boost/thread/thread.hpp>

#include "jobstep.h"

namespace joblist
{
class TupleConstantStep;
}  // namespace joblist

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
  TupleConstantStep(const JobInfo& jobInfo);

  /** @brief TupleConstantStep destructor
   */
  ~TupleConstantStep();

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

 protected:
  void execute();
  void fillInConstants(const rowgroup::Row& rowIn, rowgroup::Row& rowOut);
  void constInitialize(const JobInfo& jobInfo, const rowgroup::RowGroup* rgIn);
  void constructContanstRow(const JobInfo& jobInfo);
  void formatMiniStats();
  void printCalTrace();

  // input/output rowgroup and row
  rowgroup::RowGroup rowGroupIn_;
  rowgroup::RowGroup rowGroupOut_;
  rowgroup::RGData rgDataOut_;
  rowgroup::Row rowIn_;
  rowgroup::Row rowOut_;

  // mapping
  std::vector<uint64_t> indexConst_;    // consts in output row
  std::vector<uint64_t> indexMapping_;  // from input row to output row

  // store constants
  rowgroup::Row rowConst_;
  boost::scoped_array<uint8_t> constRowData_;

  // for datalist
  RowGroupDL* inputDL_ = nullptr;
  RowGroupDL* outputDL_ = nullptr;
  uint64_t fInputIterator = 0;
  uint64_t fOutputIterator = 0;

  uint64_t runner_ = 0;  // thread pool handle

  uint64_t rowsReturned_ = 0;

  bool endOfResult_ = false;

  JobList* jobList_;
};

}  // namespace joblist
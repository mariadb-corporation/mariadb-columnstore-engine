/* Copyright (C) 2022 MariaDB Corporation

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

/** @file */

#pragma once

#include <utility>
#include <set>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "groupconcat.h"

#define EXPORT

namespace joblist
{
// forward reference
class JsonArrayAggregator;
class ResourceManager;

class JsonArrayInfo : public GroupConcatInfo
{
 public:
  void prepJsonArray(JobInfo&);
  void mapColumns(const rowgroup::RowGroup&) override;

  const std::string toString() const override;

 protected:
  uint32_t getColumnKey(const execplan::SRCP& srcp, JobInfo& jobInfo) override;
  std::shared_ptr<int[]> makeMapping(const rowgroup::RowGroup&, const rowgroup::RowGroup&) override;
};

class JsonArrayAggregatAgUM : public GroupConcatAgUM
{
 public:
  EXPORT explicit JsonArrayAggregatAgUM(rowgroup::SP_GroupConcat&);
  EXPORT ~JsonArrayAggregatAgUM() override;

  using rowgroup::GroupConcatAg::merge;
  void initialize() override;
  void processRow(const rowgroup::Row&) override;
  EXPORT void merge(const rowgroup::Row&, int64_t) override;

  EXPORT void getResult(uint8_t*);
  EXPORT uint8_t* getResult() override;

 protected:
  void applyMapping(const std::shared_ptr<int[]>&, const rowgroup::Row&) override;
};

// JSON_ARRAYAGG base
class JsonArrayAggregator : public GroupConcator
{
 public:
  JsonArrayAggregator();
  ~JsonArrayAggregator() override;

  void initialize(const rowgroup::SP_GroupConcat&) override;
  void processRow(const rowgroup::Row&) override = 0;

  const std::string toString() const override;

 protected:
  bool concatColIsNull(const rowgroup::Row&) override;
  void outputRow(std::ostringstream&, const rowgroup::Row&) override;
  int64_t lengthEstimate(const rowgroup::Row&) override;
};

// For JSON_ARRAYAGG withour distinct or orderby
class JsonArrayAggNoOrder : public JsonArrayAggregator
{
 public:
  JsonArrayAggNoOrder();
  ~JsonArrayAggNoOrder() override;

  void initialize(const rowgroup::SP_GroupConcat&) override;
  void processRow(const rowgroup::Row&) override;

  using GroupConcator::merge;
  void merge(GroupConcator*) override;
  using GroupConcator::getResult;
  uint8_t* getResultImpl(const std::string& sep) override;

  const std::string toString() const override;

 protected:
  rowgroup::RowGroup fRowGroup;
  rowgroup::Row fRow;
  rowgroup::RGData fData;
  std::queue<rowgroup::RGData> fDataQueue;
  uint64_t fRowsPerRG;
  uint64_t fErrorCode;
  uint64_t fMemSize;
  ResourceManager* fRm;
  boost::shared_ptr<int64_t> fSessionMemLimit;
};

// ORDER BY used in JSON_ARRAYAGG class
class JsonArrayAggOrderBy : public JsonArrayAggregator, public ordering::IdbOrderBy
{
 public:
  JsonArrayAggOrderBy();
  ~JsonArrayAggOrderBy() override;

  using ordering::IdbOrderBy::initialize;
  void initialize(const rowgroup::SP_GroupConcat&) override;
  void processRow(const rowgroup::Row&) override;
  uint64_t getKeyLength() const override;

  using GroupConcator::merge;
  void merge(GroupConcator*) override;
  using GroupConcator::getResult;
  uint8_t* getResultImpl(const std::string& sep) override;

  const std::string toString() const override;

 protected:
};

}  // namespace joblist

#undef EXPORT

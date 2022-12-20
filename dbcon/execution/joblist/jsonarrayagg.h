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
#include <boost/shared_array.hpp>

#include "groupconcat.h"

#if defined(_MSC_VER) && defined(JOBLIST_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace joblist
{
// forward reference
class JsonArrayAggregator;
class ResourceManager;


class JsonArrayInfo : public GroupConcatInfo
{
 public:
  void prepJsonArray(JobInfo&);
  void mapColumns(const rowgroup::RowGroup&);

  const std::string toString() const;

 protected:
  uint32_t getColumnKey(const execplan::SRCP& srcp, JobInfo& jobInfo);
  boost::shared_array<int> makeMapping(const rowgroup::RowGroup&, const rowgroup::RowGroup&);

};


class JsonArrayAggregatAgUM : public GroupConcatAgUM
{
 public:
  EXPORT JsonArrayAggregatAgUM(rowgroup::SP_GroupConcat&);
  EXPORT ~JsonArrayAggregatAgUM();

  using rowgroup::GroupConcatAg::merge;
  void initialize();
  void processRow(const rowgroup::Row&);
  EXPORT void merge(const rowgroup::Row&, int64_t);

  EXPORT void getResult(uint8_t*);
  EXPORT uint8_t* getResult();

 protected:
  void applyMapping(const boost::shared_array<int>&, const rowgroup::Row&);
};

// JSON_ARRAYAGG base
class JsonArrayAggregator : public GroupConcator
{
 public:
  JsonArrayAggregator();
  virtual ~JsonArrayAggregator();

  virtual void initialize(const rowgroup::SP_GroupConcat&);
  virtual void processRow(const rowgroup::Row&) = 0;

  virtual const std::string toString() const;

 protected:
  virtual bool concatColIsNull(const rowgroup::Row&);
  virtual void outputRow(std::ostringstream&, const rowgroup::Row&);
  virtual int64_t lengthEstimate(const rowgroup::Row&);
};

// For JSON_ARRAYAGG withour distinct or orderby
class JsonArrayAggNoOrder : public JsonArrayAggregator
{
 public:
  JsonArrayAggNoOrder();
  virtual ~JsonArrayAggNoOrder();

  void initialize(const rowgroup::SP_GroupConcat&);
  void processRow(const rowgroup::Row&);

  using GroupConcator::merge;
  void merge(GroupConcator*);
  using GroupConcator::getResult;
  void getResult(uint8_t* buff, const std::string& sep);

  const std::string toString() const;

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
  virtual ~JsonArrayAggOrderBy();

  using ordering::IdbOrderBy::initialize;
  void initialize(const rowgroup::SP_GroupConcat&);
  void processRow(const rowgroup::Row&);
  uint64_t getKeyLength() const;

  using GroupConcator::merge;
  void merge(GroupConcator*);
  using GroupConcator::getResult;
  void getResult(uint8_t* buff, const std::string& sep);

  const std::string toString() const;

 protected:
};

}  // namespace joblist

#undef EXPORT

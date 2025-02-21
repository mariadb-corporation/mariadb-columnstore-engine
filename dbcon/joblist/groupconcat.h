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

//  $Id: groupconcat.h 9705 2013-07-17 20:06:07Z pleblanc $

/** @file */

#pragma once

#include <utility>
#include <set>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "returnedcolumn.h"  // SRCP
#include "rowgroup.h"        // RowGroup
#include "rowaggregation.h"  // SP_GroupConcat
#include "limitedorderby.h"  // IdbOrderBy

#define EXPORT

namespace joblist
{
// forward reference
struct JobInfo;
class GroupConcator;
class ResourceManager;

class GroupConcatInfo
{
 public:
  GroupConcatInfo();
  virtual ~GroupConcatInfo();

  void prepGroupConcat(JobInfo&);
  virtual void mapColumns(const rowgroup::RowGroup&);

  std::set<uint32_t>& columns()
  {
    return fColumns;
  }
  std::vector<rowgroup::SP_GroupConcat>& groupConcat()
  {
    return fGroupConcat;
  }

  virtual const std::string toString() const;

 protected:
  virtual uint32_t getColumnKey(const execplan::SRCP& srcp, JobInfo& jobInfo);
  virtual std::shared_ptr<int[]> makeMapping(const rowgroup::RowGroup&, const rowgroup::RowGroup&);

  std::set<uint32_t> fColumns;
  std::vector<rowgroup::SP_GroupConcat> fGroupConcat;
};

class GroupConcatAgUM : public rowgroup::GroupConcatAg
{
 public:
  EXPORT explicit GroupConcatAgUM(rowgroup::SP_GroupConcat&);
  EXPORT ~GroupConcatAgUM() override;

  void initialize() override;
  void processRow(const rowgroup::Row&) override;
  EXPORT void merge(const rowgroup::Row&, uint64_t) override;
  boost::scoped_ptr<GroupConcator>& concator()
  {
    return fConcator;
  }

  EXPORT uint8_t* getResult() override;


  void serialize(messageqcpp::ByteStream &bs) const override;
  void deserialize(messageqcpp::ByteStream &bs) override;

  rowgroup::RGDataSizeType getDataSize() const override;
  uint16_t getType() const override { return rowgroup::ROWAGG_GROUP_CONCAT; }

 protected:
  virtual void applyMapping(const std::shared_ptr<int[]>&, const rowgroup::Row&);

  boost::scoped_ptr<GroupConcator> fConcator;
  boost::scoped_array<uint8_t> fData;
  rowgroup::Row fRow;
  rowgroup::RGData fRowRGData;
  rowgroup::RowGroup fRowGroup;
  bool fNoOrder;
  rowgroup::RGDataSizeType fMemSize{0};
};

// GROUP_CONCAT base
class GroupConcator
{
 public:
  GroupConcator() = default;
  virtual ~GroupConcator() = default;

  virtual void initialize(const rowgroup::SP_GroupConcat&);
  virtual void processRow(const rowgroup::Row&) = 0;

  virtual void merge(GroupConcator*) = 0;
  virtual uint8_t* getResultImpl(const std::string& sep) = 0;
  virtual uint8_t* getResult(const std::string& sep);
  uint8_t* swapStreamWithStringAndReturnBuf(ostringstream& oss, bool isNull);

  virtual const std::string toString() const;

  virtual void serialize(messageqcpp::ByteStream &) const;
  virtual void deserialize(messageqcpp::ByteStream &);
  virtual rowgroup::RGDataSizeType getDataSize() const = 0;

 protected:
  virtual bool concatColIsNull(const rowgroup::Row&);
  virtual void outputRow(std::ostringstream&, const rowgroup::Row&);
  virtual int64_t lengthEstimate(const rowgroup::Row&);

  std::vector<uint32_t> fConcatColumns;
  std::vector<std::pair<utils::NullString, uint32_t> > fConstCols;
  int64_t fCurrentLength{0};
  int64_t fGroupConcatLen{0};
  int64_t fConstantLen{0};
  std::unique_ptr<std::string> outputBuf_;
  long fTimeZone{0};
};

// For GROUP_CONCAT withour distinct or orderby
class GroupConcatNoOrder : public GroupConcator
{
 public:
  GroupConcatNoOrder() = default;
  ~GroupConcatNoOrder() override;

  void initialize(const rowgroup::SP_GroupConcat&) override;
  void processRow(const rowgroup::Row&) override;

  void merge(GroupConcator*) override;
  using GroupConcator::getResult;
  uint8_t* getResultImpl(const std::string& sep) override;
  // uint8_t* getResult(const std::string& sep);

  void serialize(messageqcpp::ByteStream &) const override;
  void deserialize(messageqcpp::ByteStream &) override;

  rowgroup::RGDataSizeType getDataSize() const override { return fMemSize; }

  const std::string toString() const override;

 protected:
  void createNewRGData();
  rowgroup::RowGroup fRowGroup;
  rowgroup::Row fRow;
  std::vector<rowgroup::RGDataUnPtr> fDataVec;
  uint64_t fRowsPerRG{128};
  uint64_t fErrorCode{logging::ERR_AGGREGATION_TOO_BIG};
  rowgroup::RGDataSizeType fMemSize{0};
  rowgroup::RGDataSizeType fCurMemSize{0};
  ResourceManager* fRm{nullptr};
  boost::shared_ptr<int64_t> fSessionMemLimit;
};

// ORDER BY used in GROUP_CONCAT class
// This version is for GROUP_CONCAT, the size is limited by the group_concat_max_len.
class GroupConcatOrderBy : public GroupConcator, public ordering::IdbOrderBy
{
 public:
  GroupConcatOrderBy();
  ~GroupConcatOrderBy() override;

  using ordering::IdbOrderBy::initialize;
  void initialize(const rowgroup::SP_GroupConcat&) override;
  void processRow(const rowgroup::Row&) override;
  uint64_t getKeyLength() const override;

  void serialize(messageqcpp::ByteStream &) const override;
  void deserialize(messageqcpp::ByteStream &) override;

  rowgroup::RGDataSizeType getDataSize() const override;

  void merge(GroupConcator*) override;
  using GroupConcator::getResult;
  uint8_t* getResultImpl(const std::string& sep) override;
  // uint8_t* getResult(const std::string& sep);

  const std::string toString() const override;

 protected:
};

}  // namespace joblist

#undef EXPORT

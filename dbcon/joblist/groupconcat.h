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

#include "groupconcatcolumn.h"  // GroupConcatColumn
#include "returnedcolumn.h"     // SRCP
#include "rowgroup.h"           // RowGroup
#include "rowaggregation.h"     // SP_GroupConcat
#include "limitedorderby.h"     // IdbOrderBy

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
  ~GroupConcatInfo();

  void prepGroupConcat(JobInfo&);
  void mapColumns(const rowgroup::RowGroup&);

  std::set<uint32_t>& columns()
  {
    return fColumns;
  }
  std::vector<rowgroup::SP_GroupConcat>& groupConcat()
  {
    return fGroupConcat;
  }

  const std::string toString() const;

 protected:
  uint32_t getColumnKey(const execplan::SRCP& srcp, JobInfo& jobInfo) const;
  std::shared_ptr<int[]> makeMapping(const rowgroup::RowGroup&, const rowgroup::RowGroup&) const;

  std::set<uint32_t> fColumns;
  std::vector<rowgroup::SP_GroupConcat> fGroupConcat;
};

class GroupConcatAg
{
 public:
  explicit GroupConcatAg(rowgroup::SP_GroupConcat&, bool isJsonArrayAgg = false);
  ~GroupConcatAg();

  void initialize();
  void processRow(const rowgroup::Row&);
  void merge(const rowgroup::Row&, uint64_t);
  boost::scoped_ptr<GroupConcator>& concator()
  {
    return fConcator;
  }

  uint8_t* getResult();

  uint32_t getGroupConcatId() const
  {
    return fGroupConcat->id;
  }

  void serialize(messageqcpp::ByteStream& bs) const;
  void deserialize(messageqcpp::ByteStream& bs);

  rowgroup::RGDataSizeType getDataSize() const;

 protected:
  void applyMapping(const std::shared_ptr<int[]>&, const rowgroup::Row&);

  rowgroup::SP_GroupConcat fGroupConcat;
  bool fIsJsonArrayAgg{false};
  boost::scoped_ptr<GroupConcator> fConcator;
  boost::scoped_array<uint8_t> fData;
  rowgroup::Row fRow;
  rowgroup::RGData fRowRGData;
  rowgroup::RowGroup fRowGroup;
  bool fNoOrder;
  rowgroup::RGDataSizeType fMemSize{0};
};

using SP_GroupConcatAg = boost::shared_ptr<GroupConcatAg>;

// GROUP_CONCAT base
class GroupConcator
{
 public:
  explicit GroupConcator(bool isJsonArrayAgg) : fIsJsonArrayAgg(isJsonArrayAgg)
  {
  }
  virtual ~GroupConcator() = default;

  virtual void initialize(const rowgroup::SP_GroupConcat&);
  virtual void processRow(const rowgroup::Row&) = 0;

  virtual void merge(GroupConcator*) = 0;
  virtual uint8_t* getResultImpl(const std::string& sep) = 0;
  virtual uint8_t* getResult(const std::string& sep);
  uint8_t* swapStreamWithStringAndReturnBuf(std::ostringstream& oss, bool isNull);

  virtual const std::string toString() const;

  virtual void serialize(messageqcpp::ByteStream&) const;
  virtual void deserialize(messageqcpp::ByteStream&);
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
  bool fIsJsonArrayAgg{false};

  joblist::ResourceManager* fRm{nullptr};
  boost::shared_ptr<int64_t> fSessionMemLimit;
};

// For GROUP_CONCAT withour distinct or orderby
class GroupConcatNoOrder : public GroupConcator
{
 public:
  explicit GroupConcatNoOrder(bool isJsonArrayAgg) : GroupConcator(isJsonArrayAgg)
  {
  }
  ~GroupConcatNoOrder() override;

  void initialize(const rowgroup::SP_GroupConcat&) override;
  void processRow(const rowgroup::Row&) override;

  void merge(GroupConcator*) override;
  using GroupConcator::getResult;
  uint8_t* getResultImpl(const std::string& sep) override;
  // uint8_t* getResult(const std::string& sep);

  void serialize(messageqcpp::ByteStream&) const override;
  void deserialize(messageqcpp::ByteStream&) override;

  rowgroup::RGDataSizeType getDataSize() const override
  {
    return fMemSize;
  }

  const std::string toString() const override;

 protected:
  std::vector<rowgroup::RGDataUnPtr>& getRGDatas()
  {
    return fDataVec;
  }

  void createNewRGData();
  rowgroup::RowGroup fRowGroup;
  rowgroup::Row fRow;
  std::vector<rowgroup::RGDataUnPtr> fDataVec;
  uint64_t fRowsPerRG{128};
  rowgroup::RGDataSizeType fMemSize{0};
  rowgroup::RGDataSizeType fCurMemSize{0};
};

// ORDER BY used in GROUP_CONCAT class
// This version is for GROUP_CONCAT, the size is limited by the group_concat_max_len.
class GroupConcatOrderBy : public GroupConcator, public ordering::IdbCompare
{
 public:
  explicit GroupConcatOrderBy(bool isJsonArrayAgg);
  ~GroupConcatOrderBy() override;

  using ordering::IdbCompare::initialize;
  void initialize(const rowgroup::SP_GroupConcat&) override;
  void processRow(const rowgroup::Row&) override;
  uint64_t getKeyLength() const;

  void serialize(messageqcpp::ByteStream&) const override;
  void deserialize(messageqcpp::ByteStream&) override;

  rowgroup::RGDataSizeType getDataSize() const override;

  void merge(GroupConcator*) override;
  using GroupConcator::getResult;
  uint8_t* getResultImpl(const std::string& sep) override;
  // uint8_t* getResult(const std::string& sep);

  const std::string toString() const override;

 protected:
  struct Hasher
  {
    GroupConcatOrderBy* ts;
    utils::Hasher_r h;
    uint32_t colCount;

    Hasher(GroupConcatOrderBy* t, uint32_t c) : ts(t), colCount(c)
    {
    }
    uint64_t operator()(const rowgroup::Row::Pointer&) const;
  };

  struct Eq
  {
    GroupConcatOrderBy* ts;
    uint32_t colCount;

    Eq(GroupConcatOrderBy* t, uint32_t c) : ts(t), colCount(c)
    {
    }

    bool operator()(const rowgroup::Row::Pointer&, const rowgroup::Row::Pointer&) const;
  };

  using DistinctMap = std::unordered_map<rowgroup::Row::Pointer, uint64_t, Hasher, Eq>;

  class SortingPQ;

 protected:
  void createNewRGData();
  uint64_t getCurrentRowIdx() const;
  static uint64_t shiftGroupIdxBy(uint64_t idx, uint32_t shift);
  std::vector<rowgroup::RGDataUnPtr>& getRGDatas()
  {
    return fDataVec;
  }
  SortingPQ* getQueue()
  {
    return fOrderByQueue.get();
  }

  rowgroup::RGDataSizeType fMemSize{0};
  static constexpr uint64_t fRowsPerRG{128};

  std::vector<ordering::IdbSortSpec> fOrderByCond;
  rowgroup::Row fRow0;
  rowgroup::Row row1, row2;
  ordering::CompareRule fRule;
  std::vector<rowgroup::RGDataUnPtr> fDataVec;
  bool fDistinct;
  std::unique_ptr<DistinctMap> fDistinctMap;
  std::unique_ptr<SortingPQ> fOrderByQueue;
};

}  // namespace joblist

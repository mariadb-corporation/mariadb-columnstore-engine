/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (c) 2019 MariaDB Corporation

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

//  $Id: idborderby.h 4012 2013-07-24 21:04:45Z pleblanc $

/** @file */

#pragma once

#include <memory>
#include <queue>
#include <utility>
#include <vector>
#include <sstream>

#include <boost/scoped_ptr.hpp>

#include <unordered_set>

#include "countingallocator.h"
#include "resourcemanager.h"
#include "rowgroup.h"
#include "hasher.h"
// #include "stlpoolallocator.h"

// forward reference
namespace joblist
{
class ResourceManager;
}

namespace ordering
{
template <typename _Tp, typename _Sequence = std::vector<_Tp, allocators::CountingAllocator<_Tp>>,
          typename _Compare = std::less<typename _Sequence::value_type> >
class ReservablePQ : private std::priority_queue<_Tp, _Sequence, _Compare>
{
 public:
  typedef typename std::priority_queue<_Tp, _Sequence, _Compare>::size_type size_type;
  explicit ReservablePQ(size_type capacity, std::atomic<int64_t>* memoryLimit,
                    const int64_t checkPointStepSize = allocators::CheckPointStepSize,
                    const int64_t lowerBound = allocators::MemoryLimitLowerBound)
    : std::priority_queue<_Tp, _Sequence, _Compare>(_Compare(),
        _Sequence(allocators::CountingAllocator<_Tp>(memoryLimit, checkPointStepSize, lowerBound)))
  {
    reserve(capacity);
  }
  explicit ReservablePQ(size_type capacity, allocators::CountingAllocator<_Tp> alloc)
    : std::priority_queue<_Tp, _Sequence, _Compare>(_Compare(),
        _Sequence(alloc))
  {
    reserve(capacity);
  }
  void reserve(size_type capacity)
  {
    this->c.reserve(capacity);
  }
  size_type capacity() const
  {
    return this->c.capacity();
  }
  using std::priority_queue<_Tp, _Sequence, _Compare>::size;
  using std::priority_queue<_Tp, _Sequence, _Compare>::top;
  using std::priority_queue<_Tp, _Sequence, _Compare>::pop;
  using std::priority_queue<_Tp, _Sequence, _Compare>::push;
  using std::priority_queue<_Tp, _Sequence, _Compare>::empty;
};

// forward reference
class IdbCompare;
class OrderByRow;

using SortingPQ = ReservablePQ<OrderByRow>;

// order by specification
struct IdbSortSpec
{
  int fIndex;
  // TODO There are three ordering specs since 10.2
  int fAsc;  // <ordering specification> ::= ASC | DESC
  int fNf;   // <null ordering> ::= NULLS FIRST | NULLS LAST

  IdbSortSpec() : fIndex(-1), fAsc(1), fNf(1)
  {
  }
  IdbSortSpec(int i, bool b) : fIndex(i), fAsc(b ? 1 : -1), fNf(fAsc)
  {
  }
  IdbSortSpec(int i, bool b, bool n) : fIndex(i), fAsc(b ? 1 : -1), fNf(n ? 1 : -1)
  {
  }
};

// compare functor for different datatypes
// cannot use template because Row's getXxxField method.
class Compare
{
 public:
  explicit Compare(const IdbSortSpec& spec) : fSpec(spec)
  {
  }
  virtual ~Compare() = default;

  virtual int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) = 0;
  void revertSortSpec()
  {
    fSpec.fAsc = -fSpec.fAsc;
    fSpec.fNf = -fSpec.fNf;
  }

 protected:
  IdbSortSpec fSpec;
};

// Comparators for signed types

class TinyIntCompare : public Compare
{
 public:
  explicit TinyIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

class SmallIntCompare : public Compare
{
 public:
  explicit SmallIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

class IntCompare : public Compare
{
 public:
  explicit IntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

class BigIntCompare : public Compare
{
 public:
  explicit BigIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

class WideDecimalCompare : public Compare
{
  int keyColumnOffset;

 public:
  WideDecimalCompare(const IdbSortSpec& spec, int offset) : Compare(spec), keyColumnOffset(offset)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

// End of comparators for signed types
// Comparators for unsigned types

class UTinyIntCompare : public Compare
{
 public:
  explicit UTinyIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

class USmallIntCompare : public Compare
{
 public:
  explicit USmallIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

class UIntCompare : public Compare
{
 public:
  explicit UIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

class UBigIntCompare : public Compare
{
 public:
  explicit UBigIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

// end of comparators for unsigned types

// Comparators for float types

class DoubleCompare : public Compare
{
 public:
  explicit DoubleCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

class LongDoubleCompare : public Compare
{
 public:
  explicit LongDoubleCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

class FloatCompare : public Compare
{
 public:
  explicit FloatCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

// End of comparators for float types
// Comparators for temporal types

class DateCompare : public Compare
{
 public:
  explicit DateCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

class DatetimeCompare : public Compare
{
 public:
  explicit DatetimeCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

class TimeCompare : public Compare
{
 public:
  explicit TimeCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;
};

// End of comparators for temporal types

// Comparators for non-fixed size types

class StringCompare : public Compare
{
 public:
  explicit StringCompare(const IdbSortSpec& spec) : Compare(spec), cs(nullptr)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer) override;

  CHARSET_INFO* cs;
};

// End of comparators for variable sized types

class CompareRule
{
 public:
  explicit CompareRule(IdbCompare* c = nullptr) : fIdbCompare(c)
  {
  }

  bool less(rowgroup::Row::Pointer r1, rowgroup::Row::Pointer r2);

  void compileRules(const std::vector<IdbSortSpec>&, const rowgroup::RowGroup&);
  void revertRules();

  std::vector<Compare*> fCompares;
  IdbCompare* fIdbCompare;
};

class IdbCompare
{
 public:
  IdbCompare() = default;
  virtual ~IdbCompare() = default;

  virtual void initialize(const rowgroup::RowGroup&);
  void setStringTable(bool b);

  rowgroup::Row& row1()
  {
    return fRow1;
  }
  rowgroup::Row& row2()
  {
    return fRow2;
  }

  rowgroup::RowGroup* rowGroup()
  {
    return &fRowGroup;
  }

 protected:
  rowgroup::RowGroup fRowGroup;
  rowgroup::Row fRow1;
  rowgroup::Row fRow2;
};

class OrderByRow
{
 public:
  OrderByRow(const rowgroup::Row& r, CompareRule& c) : fData(r.getPointer()), fRule(&c)
  {
  }

  bool operator<(const OrderByRow& rhs) const
  {
    return fRule->less(fData, rhs.fData);
  }

  rowgroup::Row::Pointer fData;
  CompareRule* fRule;
};

class EqualCompData : public IdbCompare
{
 public:
  explicit EqualCompData(std::vector<uint64_t>& v) : fIndex(v)
  {
  }
  EqualCompData(std::vector<uint64_t>& v, const rowgroup::RowGroup& rg) : fIndex(v)
  {
    initialize(rg);
  }

  ~EqualCompData() override = default;

  bool operator()(rowgroup::Row::Pointer, rowgroup::Row::Pointer);

  std::vector<uint64_t> fIndex;
};

class OrderByData : public IdbCompare
{
 public:
  OrderByData(const std::vector<IdbSortSpec>&, const rowgroup::RowGroup&);
  ~OrderByData() override;

  bool operator()(rowgroup::Row::Pointer p1, rowgroup::Row::Pointer p2)
  {
    return fRule.less(p1, p2);
  }
  const CompareRule& rule() const
  {
    return fRule;
  }

 protected:
  CompareRule fRule;
};

// base classs for order by clause used in IDB
class IdbOrderBy : public IdbCompare
{
 public:
  IdbOrderBy();
  ~IdbOrderBy() override;

  void initialize(const rowgroup::RowGroup&) override;
  virtual void processRow(const rowgroup::Row&) = 0;
  virtual uint64_t getKeyLength() const = 0;
  virtual const std::string toString() const = 0;

  bool getData(rowgroup::RGData& data);

  void distinct(bool b)
  {
    fDistinct = b;
  }
  bool distinct() const
  {
    return fDistinct;
  }
  // INV fOrderByQueue is always a valid pointer that is instantiated in constructor
  SortingPQ& getQueue()
  {
    return *fOrderByQueue;
  }
  void returnAllRGDataMemory2RM()
  {
    while (!fOrderByQueue->empty())
    {
      fOrderByQueue->pop();
    }
    fRm->returnMemory(fMemSize, fSessionMemLimit);
    fMemSize = 0;
  }
   void returnRGDataMemory2RM(const size_t rgDataSize)
  {
    fRm->returnMemory(rgDataSize, fSessionMemLimit);
    fMemSize -= rgDataSize;
  }
  CompareRule& getRule()
  {
    return fRule;
  }

  std::unique_ptr<SortingPQ> fOrderByQueue = nullptr;

 protected:
  std::vector<IdbSortSpec> fOrderByCond;
  rowgroup::Row fRow0;
  CompareRule fRule;

  rowgroup::RGData fData;
  std::queue<rowgroup::RGData> fDataQueue;

  struct Hasher
  {
    IdbOrderBy* ts;
    utils::Hasher_r h;
    uint32_t colCount;
    Hasher(IdbOrderBy* t, uint32_t c) : ts(t), colCount(c)
    {
    }
    uint64_t operator()(const rowgroup::Row::Pointer&) const;
  };
  struct Eq
  {
    IdbOrderBy* ts;
    uint32_t colCount;
    Eq(IdbOrderBy* t, uint32_t c) : ts(t), colCount(c)
    {
    }
    bool operator()(const rowgroup::Row::Pointer&, const rowgroup::Row::Pointer&) const;
  };

  using DistinctMap_t = std::unordered_set<rowgroup::Row::Pointer, Hasher, Eq,
                                  allocators::CountingAllocator<rowgroup::Row::Pointer>>;
  boost::scoped_ptr<DistinctMap_t> fDistinctMap;
  rowgroup::Row row1, row2;  // scratch space for Hasher & Eq

  bool fDistinct;
  uint64_t fMemSize;
  uint64_t fRowsPerRG;
  uint64_t fErrorCode;
  joblist::ResourceManager* fRm;
  boost::shared_ptr<int64_t> fSessionMemLimit;
};

}  // namespace ordering

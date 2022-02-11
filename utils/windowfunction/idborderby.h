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

#ifndef IDB_ORDER_BY_H
#define IDB_ORDER_BY_H

#include <queue>
#include <utility>
#include <vector>
#include <sstream>
#include <boost/shared_array.hpp>
#include <boost/scoped_ptr.hpp>

#ifdef _MSC_VER
#include <unordered_set>
#else
#include <tr1/unordered_set>
#endif

#include "rowgroup.h"
#include "hasher.h"
#include "stlpoolallocator.h"

// forward reference
namespace joblist
{
class ResourceManager;
}

namespace ordering
{
template <typename _Tp, typename _Sequence = std::vector<_Tp>,
          typename _Compare = std::less<typename _Sequence::value_type> >
class reservablePQ : private std::priority_queue<_Tp, _Sequence, _Compare>
{
 public:
  typedef typename std::priority_queue<_Tp, _Sequence, _Compare>::size_type size_type;
  reservablePQ(size_type capacity = 0)
  {
    reserve(capacity);
  };
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

typedef reservablePQ<OrderByRow> SortingPQ;

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
  Compare(const IdbSortSpec& spec) : fSpec(spec)
  {
  }
  virtual ~Compare()
  {
  }

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
  TinyIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

class SmallIntCompare : public Compare
{
 public:
  SmallIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

class IntCompare : public Compare
{
 public:
  IntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

class BigIntCompare : public Compare
{
 public:
  BigIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

class WideDecimalCompare : public Compare
{
  int keyColumnOffset;

 public:
  WideDecimalCompare(const IdbSortSpec& spec, int offset) : Compare(spec), keyColumnOffset(offset)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

// End of comparators for signed types
// Comparators for unsigned types

class UTinyIntCompare : public Compare
{
 public:
  UTinyIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

class USmallIntCompare : public Compare
{
 public:
  USmallIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

class UIntCompare : public Compare
{
 public:
  UIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

class UBigIntCompare : public Compare
{
 public:
  UBigIntCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

// end of comparators for unsigned types

// Comparators for float types

class DoubleCompare : public Compare
{
 public:
  DoubleCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

class LongDoubleCompare : public Compare
{
 public:
  LongDoubleCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

class FloatCompare : public Compare
{
 public:
  FloatCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

// End of comparators for float types
// Comparators for temporal types

class DateCompare : public Compare
{
 public:
  DateCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

class DatetimeCompare : public Compare
{
 public:
  DatetimeCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

class TimeCompare : public Compare
{
 public:
  TimeCompare(const IdbSortSpec& spec) : Compare(spec)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);
};

// End of comparators for temporal types

// Comparators for non-fixed size types

class StringCompare : public Compare
{
 public:
  StringCompare(const IdbSortSpec& spec) : Compare(spec), cs(NULL)
  {
  }

  int operator()(IdbCompare*, rowgroup::Row::Pointer, rowgroup::Row::Pointer);

  CHARSET_INFO* cs;
};

// End of comparators for variable sized types

class CompareRule
{
 public:
  CompareRule(IdbCompare* c = NULL) : fIdbCompare(c)
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
  IdbCompare(){};
  virtual ~IdbCompare(){};

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
  EqualCompData(std::vector<uint64_t>& v) : fIndex(v)
  {
  }
  EqualCompData(std::vector<uint64_t>& v, const rowgroup::RowGroup& rg) : fIndex(v)
  {
    initialize(rg);
  }

  ~EqualCompData(){};

  bool operator()(rowgroup::Row::Pointer, rowgroup::Row::Pointer);

  std::vector<uint64_t> fIndex;
};

class OrderByData : public IdbCompare
{
 public:
  OrderByData(const std::vector<IdbSortSpec>&, const rowgroup::RowGroup&);
  virtual ~OrderByData();

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
  virtual ~IdbOrderBy();

  virtual void initialize(const rowgroup::RowGroup&);
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
  SortingPQ& getQueue()
  {
    return fOrderByQueue;
  }
  CompareRule& getRule()
  {
    return fRule;
  }

  SortingPQ fOrderByQueue;

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

  typedef std::tr1::unordered_set<rowgroup::Row::Pointer, Hasher, Eq,
                                  utils::STLPoolAllocator<rowgroup::Row::Pointer> >
      DistinctMap_t;
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

#endif  // IDB_ORDER_BY_H

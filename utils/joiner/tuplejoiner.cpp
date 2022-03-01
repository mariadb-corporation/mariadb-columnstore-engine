/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2019 MariaDB Corporation

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

#include "tuplejoiner.h"
#include <algorithm>
#include <vector>
#include <limits>
#ifndef _MSC_VER
#include <tr1/unordered_set>
#else
#include <unordered_set>
#endif

#include "hasher.h"
#include "lbidlist.h"
#include "spinlock.h"
#include "vlarray.h"

using namespace std;
using namespace rowgroup;
using namespace utils;
using namespace execplan;
using namespace joblist;

namespace joiner
{
// Typed joiner ctor
TupleJoiner::TupleJoiner(const rowgroup::RowGroup& smallInput, const rowgroup::RowGroup& largeInput,
                         uint32_t smallJoinColumn, uint32_t largeJoinColumn, JoinType jt,
                         threadpool::ThreadPool* jsThreadPool)
 : smallRG(smallInput)
 , largeRG(largeInput)
 , joinAlg(INSERTING)
 , joinType(jt)
 , threadCount(1)
 , typelessJoin(false)
 , bSignedUnsignedJoin(false)
 , uniqueLimit(100)
 , finished(false)
 , jobstepThreadPool(jsThreadPool)
 , _convertToDiskJoin(false)
{
  uint i;

  getBucketCount();
  m_bucketLocks.reset(new boost::mutex[bucketCount]);

  if (smallRG.getColTypes()[smallJoinColumn] == CalpontSystemCatalog::LONGDOUBLE)
  {
    ld.reset(new boost::scoped_ptr<ldhash_t>[bucketCount]);
    _pool.reset(new boost::shared_ptr<PoolAllocator>[bucketCount]);
    for (i = 0; i < bucketCount; i++)
    {
      STLPoolAllocator<pair<const long double, Row::Pointer>> alloc;
      _pool[i] = alloc.getPoolAllocator();
      ld[i].reset(new ldhash_t(10, hasher(), ldhash_t::key_equal(), alloc));
    }
  }
  else if (smallRG.usesStringTable())
  {
    sth.reset(new boost::scoped_ptr<sthash_t>[bucketCount]);
    _pool.reset(new boost::shared_ptr<PoolAllocator>[bucketCount]);
    for (i = 0; i < bucketCount; i++)
    {
      STLPoolAllocator<pair<const int64_t, Row::Pointer>> alloc;
      _pool[i] = alloc.getPoolAllocator();
      sth[i].reset(new sthash_t(10, hasher(), sthash_t::key_equal(), alloc));
    }
  }
  else
  {
    h.reset(new boost::scoped_ptr<hash_t>[bucketCount]);
    _pool.reset(new boost::shared_ptr<PoolAllocator>[bucketCount]);
    for (i = 0; i < bucketCount; i++)
    {
      STLPoolAllocator<pair<const int64_t, uint8_t*>> alloc;
      _pool[i] = alloc.getPoolAllocator();
      h[i].reset(new hash_t(10, hasher(), hash_t::key_equal(), alloc));
    }
  }

  smallRG.initRow(&smallNullRow);

  if (smallOuterJoin() || largeOuterJoin() || semiJoin() || antiJoin())
  {
    smallNullMemory = RGData(smallRG, 1);
    smallRG.setData(&smallNullMemory);
    smallRG.getRow(0, &smallNullRow);
    smallNullRow.initToNull();
  }

  smallKeyColumns.push_back(smallJoinColumn);
  largeKeyColumns.push_back(largeJoinColumn);
  discreteValues.reset(new bool[1]);
  cpValues.reset(new vector<int128_t>[1]);
  discreteValues[0] = false;

  if (smallRG.isUnsigned(smallKeyColumns[0]))
  {
    if (datatypes::isWideDecimalType(smallRG.getColType(smallKeyColumns[0]),
                                     smallRG.getColumnWidth(smallKeyColumns[0])))
    {
      cpValues[0].push_back((int128_t)-1);
      cpValues[0].push_back(0);
    }
    else
    {
      cpValues[0].push_back((int128_t)numeric_limits<uint64_t>::max());
      cpValues[0].push_back(0);
    }
  }
  else
  {
    if (datatypes::isWideDecimalType(smallRG.getColType(smallKeyColumns[0]),
                                     smallRG.getColumnWidth(smallKeyColumns[0])))
    {
      cpValues[0].push_back(utils::maxInt128);
      cpValues[0].push_back(utils::minInt128);
    }
    else
    {
      cpValues[0].push_back((int128_t)numeric_limits<int64_t>::max());
      cpValues[0].push_back((int128_t)numeric_limits<int64_t>::min());
    }
  }

  if (smallRG.isUnsigned(smallJoinColumn) != largeRG.isUnsigned(largeJoinColumn))
    bSignedUnsignedJoin = true;

  nullValueForJoinColumn = smallNullRow.getSignedNullValue(smallJoinColumn);
}

// Typeless joiner ctor
TupleJoiner::TupleJoiner(const rowgroup::RowGroup& smallInput, const rowgroup::RowGroup& largeInput,
                         const vector<uint32_t>& smallJoinColumns, const vector<uint32_t>& largeJoinColumns,
                         JoinType jt, threadpool::ThreadPool* jsThreadPool)
 : smallRG(smallInput)
 , largeRG(largeInput)
 , joinAlg(INSERTING)
 , joinType(jt)
 , threadCount(1)
 , typelessJoin(true)
 , smallKeyColumns(smallJoinColumns)
 , largeKeyColumns(largeJoinColumns)
 , bSignedUnsignedJoin(false)
 , uniqueLimit(100)
 , finished(false)
 , jobstepThreadPool(jsThreadPool)
 , _convertToDiskJoin(false)
{
  uint i;

  getBucketCount();

  _pool.reset(new boost::shared_ptr<PoolAllocator>[bucketCount]);
  ht.reset(new boost::scoped_ptr<typelesshash_t>[bucketCount]);
  for (i = 0; i < bucketCount; i++)
  {
    STLPoolAllocator<pair<const TypelessData, Row::Pointer>> alloc;
    _pool[i] = alloc.getPoolAllocator();
    ht[i].reset(new typelesshash_t(10, hasher(), typelesshash_t::key_equal(), alloc));
  }
  m_bucketLocks.reset(new boost::mutex[bucketCount]);

  smallRG.initRow(&smallNullRow);

  if (smallOuterJoin() || largeOuterJoin() || semiJoin() || antiJoin())
  {
    smallNullMemory = RGData(smallRG, 1);
    smallRG.setData(&smallNullMemory);
    smallRG.getRow(0, &smallNullRow);
    smallNullRow.initToNull();
  }

  keyLength = calculateKeyLength(smallKeyColumns, smallRG, &largeKeyColumns, &largeRG);

  discreteValues.reset(new bool[smallKeyColumns.size()]);
  cpValues.reset(new vector<int128_t>[smallKeyColumns.size()]);

  for (i = 0; i < smallKeyColumns.size(); ++i)
  {
    uint32_t smallKeyColumnsIdx = smallKeyColumns[i];
    auto smallSideColType = smallRG.getColTypes()[smallKeyColumnsIdx];
    // Set bSignedUnsignedJoin if one or more join columns are signed to unsigned compares.
    if (smallRG.isUnsigned(smallKeyColumnsIdx) != largeRG.isUnsigned(largeKeyColumns[i]))
    {
      bSignedUnsignedJoin = true;
    }

    discreteValues[i] = false;
    if (isUnsigned(smallSideColType))
    {
      cpValues[i].push_back((int128_t)numeric_limits<uint64_t>::max());
      cpValues[i].push_back(0);
    }
    else
    {
      if (datatypes::isWideDecimalType(smallSideColType, smallRG.getColumnWidth(smallKeyColumnsIdx)))
      {
        cpValues[i].push_back(utils::maxInt128);
        cpValues[i].push_back(utils::minInt128);
      }
      else
      {
        cpValues[i].push_back(numeric_limits<int64_t>::max());
        cpValues[i].push_back(numeric_limits<int64_t>::min());
      }
    }
  }

  // note, 'numcores' is implied by tuplehashjoin on calls to insertRGData().
  // TODO: make it explicit to avoid future confusion.
  storedKeyAlloc.reset(new FixedAllocator[numCores]);
  for (i = 0; i < (uint)numCores; i++)
    storedKeyAlloc[i].setAllocSize(keyLength);
}

TupleJoiner::TupleJoiner()
{
}

TupleJoiner::TupleJoiner(const TupleJoiner& j)
{
  throw runtime_error("TupleJoiner(TupleJoiner) shouldn't be called.");
}

TupleJoiner& TupleJoiner::operator=(const TupleJoiner& j)
{
  throw runtime_error("TupleJoiner::operator=() shouldn't be called.");
  return *this;
}

TupleJoiner::~TupleJoiner()
{
  smallNullMemory = RGData();
}

bool TupleJoiner::operator<(const TupleJoiner& tj) const
{
  return size() < tj.size();
}

void TupleJoiner::getBucketCount()
{
  // get the # of cores, round up to nearest power of 2
  // make the bucket mask
  numCores = sysconf(_SC_NPROCESSORS_ONLN);
  if (numCores <= 0)
    numCores = 8;
  bucketCount = (numCores == 1 ? 1 : (1 << (32 - __builtin_clz(numCores - 1))));
  bucketMask = bucketCount - 1;
}

template <typename buckets_t, typename hash_table_t>
void TupleJoiner::bucketsToTables(buckets_t* buckets, hash_table_t* tables)
{
  uint i;

  bool done = false, wasProductive;
  while (!done)
  {
    done = true;
    wasProductive = false;
    for (i = 0; i < bucketCount; i++)
    {
      if (buckets[i].empty())
        continue;
      bool gotIt = m_bucketLocks[i].try_lock();
      if (!gotIt)
      {
        done = false;
        continue;
      }
      for (auto& element : buckets[i])
        tables[i]->insert(element);
      m_bucketLocks[i].unlock();
      wasProductive = true;
      buckets[i].clear();
    }
    if (!done && !wasProductive)
      ::usleep(1000 * numCores);
  }
}

void TupleJoiner::um_insertTypeless(uint threadID, uint rowCount, Row& r)
{
  utils::VLArray<TypelessData> td(rowCount);
  utils::VLArray<vector<pair<TypelessData, Row::Pointer>>> v(bucketCount);
  uint i;
  FixedAllocator* alloc = &storedKeyAlloc[threadID];

  for (i = 0; i < rowCount; i++, r.nextRow())
  {
    td[i] = makeTypelessKey(r, smallKeyColumns, keyLength, alloc, largeRG, largeKeyColumns);
    if (td[i].len == 0)
      continue;
    uint bucket = bucketPicker((char*)td[i].data, td[i].len, bpSeed) & bucketMask;
    v[bucket].push_back(pair<TypelessData, Row::Pointer>(td[i], r.getPointer()));
  }
  bucketsToTables(&v[0], ht.get());
}

void TupleJoiner::um_insertLongDouble(uint rowCount, Row& r)
{
  utils::VLArray<vector<pair<long double, Row::Pointer>>> v(bucketCount);
  uint i;
  uint smallKeyColumn = smallKeyColumns[0];

  for (i = 0; i < rowCount; i++, r.nextRow())
  {
    long double smallKey = r.getLongDoubleField(smallKeyColumn);
    uint bucket = bucketPicker((char*)&smallKey, 10, bpSeed) &
                  bucketMask;  // change if we decide to support windows again
    if (UNLIKELY(smallKey == joblist::LONGDOUBLENULL))
      v[bucket].push_back(pair<long double, Row::Pointer>(joblist::LONGDOUBLENULL, r.getPointer()));
    else
      v[bucket].push_back(pair<long double, Row::Pointer>(smallKey, r.getPointer()));
  }
  bucketsToTables(&v[0], ld.get());
}

void TupleJoiner::um_insertInlineRows(uint rowCount, Row& r)
{
  uint i;
  int64_t smallKey;
  utils::VLArray<vector<pair<int64_t, uint8_t*>>> v(bucketCount);
  uint smallKeyColumn = smallKeyColumns[0];

  for (i = 0; i < rowCount; i++, r.nextRow())
  {
    if (!r.isUnsigned(smallKeyColumn))
      smallKey = r.getIntField(smallKeyColumn);
    else
      smallKey = (int64_t)r.getUintField(smallKeyColumn);
    uint bucket = bucketPicker((char*)&smallKey, sizeof(smallKey), bpSeed) & bucketMask;
    if (UNLIKELY(smallKey == nullValueForJoinColumn))
      v[bucket].push_back(pair<int64_t, uint8_t*>(getJoinNullValue(), r.getData()));
    else
      v[bucket].push_back(pair<int64_t, uint8_t*>(smallKey, r.getData()));
  }
  bucketsToTables(&v[0], h.get());
}

void TupleJoiner::um_insertStringTable(uint rowCount, Row& r)
{
  int64_t smallKey;
  uint i;
  utils::VLArray<vector<pair<int64_t, Row::Pointer>>> v(bucketCount);
  uint smallKeyColumn = smallKeyColumns[0];

  for (i = 0; i < rowCount; i++, r.nextRow())
  {
    if (!r.isUnsigned(smallKeyColumn))
      smallKey = r.getIntField(smallKeyColumn);
    else
      smallKey = (int64_t)r.getUintField(smallKeyColumn);
    uint bucket = bucketPicker((char*)&smallKey, sizeof(smallKey), bpSeed) & bucketMask;
    if (UNLIKELY(smallKey == nullValueForJoinColumn))
      v[bucket].push_back(pair<int64_t, Row::Pointer>(getJoinNullValue(), r.getPointer()));
    else
      v[bucket].push_back(pair<int64_t, Row::Pointer>(smallKey, r.getPointer()));
  }
  bucketsToTables(&v[0], sth.get());
}

void TupleJoiner::insertRGData(RowGroup& rg, uint threadID)
{
  uint i, rowCount;
  Row r;

  rg.initRow(&r);
  rowCount = rg.getRowCount();

  rg.getRow(0, &r);
  m_cpValuesLock.lock();
  for (i = 0; i < rowCount; i++, r.nextRow())
  {
    updateCPData(r);
    r.zeroRid();
  }
  m_cpValuesLock.unlock();
  rg.getRow(0, &r);

  if (joinAlg == UM)
  {
    if (typelessJoin)
      um_insertTypeless(threadID, rowCount, r);
    else if (r.getColType(smallKeyColumns[0]) == execplan::CalpontSystemCatalog::LONGDOUBLE)
      um_insertLongDouble(rowCount, r);
    else if (!smallRG.usesStringTable())
      um_insertInlineRows(rowCount, r);
    else
      um_insertStringTable(rowCount, r);
  }
  else
  {
    // while in PM-join mode, inserting is single-threaded
    for (i = 0; i < rowCount; i++, r.nextRow())
      rows.push_back(r.getPointer());
  }
}

void TupleJoiner::insert(Row& r, bool zeroTheRid)
{
  /* when doing a disk-based join, only the first iteration on the large side
  will 'zeroTheRid'.  The successive iterations will need it unchanged. */
  if (zeroTheRid)
    r.zeroRid();

  updateCPData(r);

  if (joinAlg == UM)
  {
    if (typelessJoin)
    {
      TypelessData td =
          makeTypelessKey(r, smallKeyColumns, keyLength, &storedKeyAlloc[0], largeRG, largeKeyColumns);
      if (td.len > 0)
      {
        uint bucket = bucketPicker((char*)td.data, td.len, bpSeed) & bucketMask;
        ht[bucket]->insert(pair<TypelessData, Row::Pointer>(td, r.getPointer()));
      }
    }
    else if (r.getColType(smallKeyColumns[0]) == execplan::CalpontSystemCatalog::LONGDOUBLE)
    {
      long double smallKey = r.getLongDoubleField(smallKeyColumns[0]);
      uint bucket = bucketPicker((char*)&smallKey, 10, bpSeed) &
                    bucketMask;  // change if we decide to support windows again
      if (UNLIKELY(smallKey == joblist::LONGDOUBLENULL))
        ld[bucket]->insert(pair<long double, Row::Pointer>(joblist::LONGDOUBLENULL, r.getPointer()));
      else
        ld[bucket]->insert(pair<long double, Row::Pointer>(smallKey, r.getPointer()));
    }
    else if (!smallRG.usesStringTable())
    {
      int64_t smallKey;

      if (!r.isUnsigned(smallKeyColumns[0]))
        smallKey = r.getIntField(smallKeyColumns[0]);
      else
        smallKey = (int64_t)r.getUintField(smallKeyColumns[0]);
      uint bucket = bucketPicker((char*)&smallKey, sizeof(smallKey), bpSeed) & bucketMask;
      if (UNLIKELY(smallKey == nullValueForJoinColumn))
        h[bucket]->insert(pair<int64_t, uint8_t*>(getJoinNullValue(), r.getData()));
      else
        h[bucket]->insert(pair<int64_t, uint8_t*>(smallKey, r.getData()));  // Normal path for integers
    }
    else
    {
      int64_t smallKey;

      if (!r.isUnsigned(smallKeyColumns[0]))
        smallKey = r.getIntField(smallKeyColumns[0]);
      else
        smallKey = (int64_t)r.getUintField(smallKeyColumns[0]);
      uint bucket = bucketPicker((char*)&smallKey, sizeof(smallKey), bpSeed) & bucketMask;
      if (UNLIKELY(smallKey == nullValueForJoinColumn))
        sth[bucket]->insert(pair<int64_t, Row::Pointer>(getJoinNullValue(), r.getPointer()));
      else
        sth[bucket]->insert(pair<int64_t, Row::Pointer>(smallKey, r.getPointer()));
    }
  }
  else
    rows.push_back(r.getPointer());
}

void TupleJoiner::match(rowgroup::Row& largeSideRow, uint32_t largeRowIndex, uint32_t threadID,
                        vector<Row::Pointer>* matches)
{
  uint32_t i;
  bool isNull = hasNullJoinColumn(largeSideRow);
  matches->clear();

  if (inPM())
  {
    vector<uint32_t>& v = pmJoinResults[threadID][largeRowIndex];
    uint32_t size = v.size();

    for (i = 0; i < size; i++)
      if (v[i] < rows.size())
        matches->push_back(rows[v[i]]);

    if (UNLIKELY((semiJoin() || antiJoin()) && matches->size() == 0))
      matches->push_back(smallNullRow.getPointer());
  }
  else if (LIKELY(!isNull))
  {
    if (UNLIKELY(typelessJoin))
    {
      TypelessData largeKey;
      thIterator it;
      pair<thIterator, thIterator> range;

      largeKey = makeTypelessKey(largeSideRow, largeKeyColumns, keyLength, &tmpKeyAlloc[threadID], smallRG,
                                 smallKeyColumns);
      if (largeKey.len == 0)
        return;

      uint bucket = bucketPicker((char*)largeKey.data, largeKey.len, bpSeed) & bucketMask;
      range = ht[bucket]->equal_range(largeKey);

      if (range.first == range.second && !(joinType & (LARGEOUTER | MATCHNULLS)))
        return;

      for (; range.first != range.second; ++range.first)
        matches->push_back(range.first->second);
    }
    else if (largeSideRow.getColType(largeKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE && ld)
    {
      // This is a compare of two long double
      long double largeKey;
      ldIterator it;
      pair<ldIterator, ldIterator> range;
      Row r;

      largeKey = largeSideRow.getLongDoubleField(largeKeyColumns[0]);
      uint bucket = bucketPicker((char*)&largeKey, 10, bpSeed) & bucketMask;
      range = ld[bucket]->equal_range(largeKey);

      if (range.first == range.second && !(joinType & (LARGEOUTER | MATCHNULLS)))
        return;
      for (; range.first != range.second; ++range.first)
      {
        matches->push_back(range.first->second);
      }
    }
    else if (!smallRG.usesStringTable())
    {
      int64_t largeKey;

      if (largeSideRow.getColType(largeKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE)
      {
        largeKey = (int64_t)largeSideRow.getLongDoubleField(largeKeyColumns[0]);
      }
      else if (largeSideRow.isUnsigned(largeKeyColumns[0]))
      {
        largeKey = (int64_t)largeSideRow.getUintField(largeKeyColumns[0]);
      }
      else
      {
        largeKey = largeSideRow.getIntField(largeKeyColumns[0]);
      }

      if (ld)
      {
        // Compare against long double
        long double ldKey = largeKey;
        uint bucket = bucketPicker((char*)&ldKey, 10, bpSeed) & bucketMask;
        auto range = ld[bucket]->equal_range(ldKey);

        if (range.first == range.second && !(joinType & (LARGEOUTER | MATCHNULLS)))
          return;

        for (; range.first != range.second; ++range.first)
          matches->push_back(range.first->second);
      }
      else
      {
        uint bucket = bucketPicker((char*)&largeKey, sizeof(largeKey), bpSeed) & bucketMask;
        auto range = h[bucket]->equal_range(largeKey);

        if (range.first == range.second && !(joinType & (LARGEOUTER | MATCHNULLS)))
          return;

        for (; range.first != range.second; ++range.first)
          matches->push_back(range.first->second);
      }
    }
    else
    {
      int64_t largeKey = largeSideRow.getIntField(largeKeyColumns[0]);
      uint bucket = bucketPicker((char*)&largeKey, sizeof(largeKey), bpSeed) & bucketMask;
      auto range = sth[bucket]->equal_range(largeKey);

      if (range.first == range.second && !(joinType & (LARGEOUTER | MATCHNULLS)))
        return;

      for (; range.first != range.second; ++range.first)
        matches->push_back(range.first->second);
    }
  }

  if (UNLIKELY(largeOuterJoin() && matches->size() == 0))
  {
    // cout << "Matched the NULL row: " << smallNullRow.toString() << endl;
    matches->push_back(smallNullRow.getPointer());
  }

  if (UNLIKELY(inUM() && (joinType & MATCHNULLS) && !isNull && !typelessJoin))
  {
    if (largeRG.getColType(largeKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE)
    {
      uint bucket = bucketPicker((char*)&(joblist::LONGDOUBLENULL), sizeof(joblist::LONGDOUBLENULL), bpSeed) &
                    bucketMask;
      pair<ldIterator, ldIterator> range = ld[bucket]->equal_range(joblist::LONGDOUBLENULL);

      for (; range.first != range.second; ++range.first)
        matches->push_back(range.first->second);
    }
    else if (!largeRG.usesStringTable())
    {
      auto nullVal = getJoinNullValue();
      uint bucket = bucketPicker((char*)&nullVal, sizeof(nullVal), bpSeed) & bucketMask;
      pair<iterator, iterator> range = h[bucket]->equal_range(nullVal);

      for (; range.first != range.second; ++range.first)
        matches->push_back(range.first->second);
    }
    else
    {
      auto nullVal = getJoinNullValue();
      uint bucket = bucketPicker((char*)&nullVal, sizeof(nullVal), bpSeed) & bucketMask;
      pair<sthash_t::iterator, sthash_t::iterator> range = sth[bucket]->equal_range(nullVal);

      for (; range.first != range.second; ++range.first)
        matches->push_back(range.first->second);
    }
  }

  /* Bug 3524.  For 'not in' queries this matches everything.
   */
  if (UNLIKELY(inUM() && isNull && antiJoin() && (joinType & MATCHNULLS)))
  {
    if (!typelessJoin)
    {
      if (smallRG.getColType(smallKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE)
      {
        ldIterator it;

        for (uint i = 0; i < bucketCount; i++)
          for (it = ld[i]->begin(); it != ld[i]->end(); ++it)
            matches->push_back(it->second);
      }
      else if (!smallRG.usesStringTable())
      {
        iterator it;

        for (uint i = 0; i < bucketCount; i++)
          for (it = h[i]->begin(); it != h[i]->end(); ++it)
            matches->push_back(it->second);
      }
      else
      {
        sthash_t::iterator it;

        for (uint i = 0; i < bucketCount; i++)
          for (it = sth[i]->begin(); it != sth[i]->end(); ++it)
            matches->push_back(it->second);
      }
    }
    else
    {
      thIterator it;

      for (uint i = 0; i < bucketCount; i++)
        for (it = ht[i]->begin(); it != ht[i]->end(); ++it)
          matches->push_back(it->second);
    }
  }
}

void TupleJoiner::doneInserting()
{
  // a minor textual cleanup
#ifdef TJ_DEBUG
#define CHECKSIZE                         \
  if (uniquer.size() > uniqueLimit)       \
  {                                       \
    cout << "too many discrete values\n"; \
    return;                               \
  }
#else
#define CHECKSIZE                   \
  if (uniquer.size() > uniqueLimit) \
    return;
#endif

  uint32_t col;

  /* Put together the discrete values for the runtime casual partitioning restriction */

  finished = true;

  for (col = 0; col < smallKeyColumns.size(); col++)
  {
    typedef std::tr1::unordered_set<int128_t, utils::Hash128, utils::Equal128> unordered_set_int128;
    unordered_set_int128 uniquer;
    unordered_set_int128::iterator uit;
    sthash_t::iterator sthit;
    hash_t::iterator hit;
    ldhash_t::iterator ldit;
    typelesshash_t::iterator thit;
    uint32_t i, pmpos = 0, rowCount;
    Row smallRow;
    auto smallSideColIdx = smallKeyColumns[col];
    auto smallSideColType = smallRG.getColType(smallSideColIdx);

    smallRG.initRow(&smallRow);

    if (smallRow.isCharType(smallSideColIdx))
      continue;

    rowCount = size();

    uint bucket = 0;
    if (joinAlg == PM)
      pmpos = 0;
    else if (typelessJoin)
      thit = ht[bucket]->begin();
    else if (isLongDouble(smallRG.getColType(smallKeyColumns[0])))
      ldit = ld[bucket]->begin();
    else if (!smallRG.usesStringTable())
      hit = h[bucket]->begin();
    else
      sthit = sth[bucket]->begin();

    for (i = 0; i < rowCount; i++)
    {
      if (joinAlg == PM)
        smallRow.setPointer(rows[pmpos++]);
      else if (typelessJoin)
      {
        while (thit == ht[bucket]->end())
          thit = ht[++bucket]->begin();
        smallRow.setPointer(thit->second);
        ++thit;
      }
      else if (isLongDouble(smallSideColType))
      {
        while (ldit == ld[bucket]->end())
          ldit = ld[++bucket]->begin();
        smallRow.setPointer(ldit->second);
        ++ldit;
      }
      else if (!smallRG.usesStringTable())
      {
        while (hit == h[bucket]->end())
          hit = h[++bucket]->begin();
        smallRow.setPointer(hit->second);
        ++hit;
      }
      else
      {
        while (sthit == sth[bucket]->end())
          sthit = sth[++bucket]->begin();
        smallRow.setPointer(sthit->second);
        ++sthit;
      }

      if (isLongDouble(smallSideColType))
      {
        double dval = (double)roundl(smallRow.getLongDoubleField(smallSideColIdx));
        switch (largeRG.getColType(largeKeyColumns[col]))
        {
          case CalpontSystemCatalog::DOUBLE:
          case CalpontSystemCatalog::UDOUBLE:
          case CalpontSystemCatalog::FLOAT:
          case CalpontSystemCatalog::UFLOAT:
          {
            uniquer.insert(*(int64_t*)&dval);
            break;
          }
          default:
          {
            uniquer.insert((int64_t)dval);
          }
        }
      }
      else if (datatypes::isWideDecimalType(smallSideColType, smallRow.getColumnWidth(smallSideColIdx)))
      {
        uniquer.insert(smallRow.getTSInt128Field(smallSideColIdx).getValue());
      }
      else if (smallRow.isUnsigned(smallSideColIdx))
      {
        uniquer.insert((int64_t)smallRow.getUintField(smallSideColIdx));
      }
      else
      {
        uniquer.insert(smallRow.getIntField(smallSideColIdx));
      }

      CHECKSIZE;
    }

    discreteValues[col] = true;
    cpValues[col].clear();
#ifdef TJ_DEBUG
    cout << "inserting " << uniquer.size() << " discrete values\n";
#endif

    for (uit = uniquer.begin(); uit != uniquer.end(); ++uit)
      cpValues[col].push_back(*uit);
  }
}

void TupleJoiner::setInPM()
{
  joinAlg = PM;
}

void TupleJoiner::umJoinConvert(size_t begin, size_t end)
{
  Row smallRow;
  smallRG.initRow(&smallRow);

  while (begin < end)
  {
    smallRow.setPointer(rows[begin++]);
    insert(smallRow);
  }
}

void TupleJoiner::setInUM()
{
  vector<Row::Pointer> empty;
  Row smallRow;
  uint32_t i, size;

  if (joinAlg == UM)
    return;

  joinAlg = UM;
  size = rows.size();
  size_t chunkSize =
      ((size / numCores) + 1 < 50000 ? 50000
                                     : (size / numCores) + 1);  // don't start a thread to process < 50k rows

  utils::VLArray<uint64_t> jobs(numCores);
  i = 0;
  for (size_t firstRow = 0; i < (uint)numCores && firstRow < size; i++, firstRow += chunkSize)
    jobs[i] = jobstepThreadPool->invoke(
        [this, firstRow, chunkSize, size]
        { this->umJoinConvert(firstRow, (firstRow + chunkSize < size ? firstRow + chunkSize : size)); });

  for (uint j = 0; j < i; j++)
    jobstepThreadPool->join(jobs[j]);

#ifdef TJ_DEBUG
  cout << "done\n";
#endif
  rows.swap(empty);

  if (typelessJoin)
  {
    tmpKeyAlloc.reset(new FixedAllocator[threadCount]);

    for (i = 0; i < threadCount; i++)
      tmpKeyAlloc[i] = FixedAllocator(keyLength, true);
  }
}

void TupleJoiner::umJoinConvert(uint threadID, vector<RGData>& rgs, size_t begin, size_t end)
{
  RowGroup l_smallRG(smallRG);

  while (begin < end)
  {
    l_smallRG.setData(&(rgs[begin++]));
    insertRGData(l_smallRG, threadID);
  }
}

void TupleJoiner::setInUM(vector<RGData>& rgs)
{
  Row smallRow;
  uint32_t i, size;

  if (joinAlg == UM)
    return;

  {  // don't need rows anymore, free the mem
    vector<Row::Pointer> empty;
    rows.swap(empty);
  }

  joinAlg = UM;
  size = rgs.size();
  size_t chunkSize =
      ((size / numCores) + 1 < 10 ? 10 : (size / numCores) + 1);  // don't issue jobs for < 10 rowgroups

  utils::VLArray<uint64_t> jobs(numCores);
  i = 0;
  for (size_t firstRow = 0; i < (uint)numCores && firstRow < size; i++, firstRow += chunkSize)
    jobs[i] = jobstepThreadPool->invoke(
        [this, firstRow, chunkSize, size, i, &rgs] {
          this->umJoinConvert(i, rgs, firstRow, (firstRow + chunkSize < size ? firstRow + chunkSize : size));
        });

  for (uint j = 0; j < i; j++)
    jobstepThreadPool->join(jobs[j]);

#ifdef TJ_DEBUG
  cout << "done\n";
#endif

  if (typelessJoin)
  {
    tmpKeyAlloc.reset(new FixedAllocator[threadCount]);

    for (i = 0; i < threadCount; i++)
      tmpKeyAlloc[i] = FixedAllocator(keyLength, true);
  }
}

void TupleJoiner::setPMJoinResults(boost::shared_array<vector<uint32_t>> jr, uint32_t threadID)
{
  pmJoinResults[threadID] = jr;
}

void TupleJoiner::markMatches(uint32_t threadID, uint32_t rowCount)
{
  boost::shared_array<vector<uint32_t>> matches = pmJoinResults[threadID];
  uint32_t i, j;

  for (i = 0; i < rowCount; i++)
    for (j = 0; j < matches[i].size(); j++)
    {
      if (matches[i][j] < rows.size())
      {
        smallRow[threadID].setPointer(rows[matches[i][j]]);
        smallRow[threadID].markRow();
      }
    }
}

void TupleJoiner::markMatches(uint32_t threadID, const vector<Row::Pointer>& matches)
{
  uint32_t rowCount = matches.size();
  uint32_t i;

  for (i = 0; i < rowCount; i++)
  {
    smallRow[threadID].setPointer(matches[i]);
    smallRow[threadID].markRow();
  }
}

boost::shared_array<std::vector<uint32_t>> TupleJoiner::getPMJoinArrays(uint32_t threadID)
{
  return pmJoinResults[threadID];
}

void TupleJoiner::setThreadCount(uint32_t cnt)
{
  threadCount = cnt;
  pmJoinResults.reset(new boost::shared_array<vector<uint32_t>>[cnt]);
  smallRow.reset(new Row[cnt]);

  for (uint32_t i = 0; i < cnt; i++)
    smallRG.initRow(&smallRow[i]);

  if (typelessJoin)
  {
    tmpKeyAlloc.reset(new FixedAllocator[threadCount]);

    for (uint32_t i = 0; i < threadCount; i++)
      tmpKeyAlloc[i] = FixedAllocator(keyLength, true);
  }

  if (fe)
  {
    fes.reset(new funcexp::FuncExpWrapper[cnt]);

    for (uint32_t i = 0; i < cnt; i++)
      fes[i] = *fe;
  }
}

void TupleJoiner::getUnmarkedRows(vector<Row::Pointer>* out)
{
  Row smallR;

  smallRG.initRow(&smallR);
  out->clear();

  if (inPM())
  {
    uint32_t i, size;

    size = rows.size();

    for (i = 0; i < size; i++)
    {
      smallR.setPointer(rows[i]);

      if (!smallR.isMarked())
        out->push_back(rows[i]);
    }
  }
  else
  {
    if (typelessJoin)
    {
      typelesshash_t::iterator it;

      for (uint i = 0; i < bucketCount; i++)
        for (it = ht[i]->begin(); it != ht[i]->end(); ++it)
        {
          smallR.setPointer(it->second);

          if (!smallR.isMarked())
            out->push_back(it->second);
        }
    }
    else if (smallRG.getColType(smallKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE)
    {
      ldIterator it;

      for (uint i = 0; i < bucketCount; i++)
        for (it = ld[i]->begin(); it != ld[i]->end(); ++it)
        {
          smallR.setPointer(it->second);

          if (!smallR.isMarked())
            out->push_back(it->second);
        }
    }
    else if (!smallRG.usesStringTable())
    {
      iterator it;

      for (uint i = 0; i < bucketCount; i++)
        for (it = h[i]->begin(); it != h[i]->end(); ++it)
        {
          smallR.setPointer(it->second);

          if (!smallR.isMarked())
            out->push_back(it->second);
        }
    }
    else
    {
      sthash_t::iterator it;

      for (uint i = 0; i < bucketCount; i++)
        for (it = sth[i]->begin(); it != sth[i]->end(); ++it)
        {
          smallR.setPointer(it->second);

          if (!smallR.isMarked())
            out->push_back(it->second);
        }
    }
  }
}

uint64_t TupleJoiner::getMemUsage() const
{
  if (inUM() && typelessJoin)
  {
    size_t ret = 0;
    for (uint i = 0; i < bucketCount; i++)
      ret += _pool[i]->getMemUsage();
    for (int i = 0; i < numCores; i++)
      ret += storedKeyAlloc[i].getMemUsage();
    return ret;
  }
  else if (inUM())
  {
    size_t ret = 0;
    for (uint i = 0; i < bucketCount; i++)
      ret += _pool[i]->getMemUsage();
    return ret;
  }
  else
    return (rows.size() * sizeof(Row::Pointer));
}

void TupleJoiner::setFcnExpFilter(boost::shared_ptr<funcexp::FuncExpWrapper> pt)
{
  fe = pt;

  if (fe)
    joinType |= WITHFCNEXP;
  else
    joinType &= ~WITHFCNEXP;
}

void TupleJoiner::updateCPData(const Row& r)
{
  uint32_t col;

  if (antiJoin() || largeOuterJoin())
    return;

  for (col = 0; col < smallKeyColumns.size(); col++)
  {
    auto colIdx = smallKeyColumns[col];
    if (r.isLongString(colIdx))
      continue;

    auto &min = cpValues[col][0], &max = cpValues[col][1];

    if (r.isCharType(colIdx))
    {
      datatypes::Charset cs(r.getCharset(colIdx));
      int64_t val = r.getIntField(colIdx);

      if (datatypes::TCharShort::strnncollsp(cs, val, min, r.getColumnWidth(smallKeyColumns[col])) < 0 ||
          ((int64_t)min) == numeric_limits<int64_t>::max())
      {
        min = val;
      }

      if (datatypes::TCharShort::strnncollsp(cs, val, max, r.getColumnWidth(smallKeyColumns[col])) > 0 ||
          ((int64_t)max) == numeric_limits<int64_t>::min())
      {
        max = val;
      }
    }
    else if (r.isUnsigned(colIdx))
    {
      uint128_t uval;

      if (r.getColType(colIdx) == CalpontSystemCatalog::LONGDOUBLE)
      {
        double dval = (double)roundl(r.getLongDoubleField(smallKeyColumns[col]));
        switch (largeRG.getColType(largeKeyColumns[col]))
        {
          case CalpontSystemCatalog::DOUBLE:
          case CalpontSystemCatalog::UDOUBLE:
          case CalpontSystemCatalog::FLOAT:
          case CalpontSystemCatalog::UFLOAT:
          {
            uval = *(uint64_t*)&dval;
            break;
          }
          default:
          {
            uval = (uint64_t)dval;
          }
        }
      }
      else if (datatypes::isWideDecimalType(r.getColType(colIdx), r.getColumnWidth(colIdx)))
      {
        uval = r.getTSInt128Field(colIdx).getValue();
      }
      else
      {
        uval = r.getUintField(colIdx);
      }

      if (uval > static_cast<uint128_t>(max))
        max = static_cast<int128_t>(uval);

      if (uval < static_cast<uint128_t>(min))
        min = static_cast<int128_t>(uval);
    }
    else
    {
      int128_t val = 0;

      if (r.getColType(colIdx) == CalpontSystemCatalog::LONGDOUBLE)
      {
        double dval = (double)roundl(r.getLongDoubleField(colIdx));
        switch (largeRG.getColType(largeKeyColumns[col]))
        {
          case CalpontSystemCatalog::DOUBLE:
          case CalpontSystemCatalog::UDOUBLE:
          case CalpontSystemCatalog::FLOAT:
          case CalpontSystemCatalog::UFLOAT:
          {
            val = *(int64_t*)&dval;
            break;
          }
          default:
          {
            val = (int64_t)dval;
          }
        }
      }
      else if (datatypes::isWideDecimalType(r.getColType(colIdx), r.getColumnWidth(colIdx)))
      {
        val = r.getTSInt128Field(colIdx).getValue();
      }
      else
      {
        val = r.getIntField(colIdx);
      }

      if (val > max)
        max = val;

      if (val < min)
        min = val;
    }
  }
}

size_t TupleJoiner::size() const
{
  if (joinAlg == UM || joinAlg == INSERTING)
  {
    size_t ret = 0;
    for (uint i = 0; i < bucketCount; i++)
      if (UNLIKELY(typelessJoin))
        ret += ht[i]->size();
      else if (smallRG.getColType(smallKeyColumns[0]) == CalpontSystemCatalog::LONGDOUBLE)
        ret += ld[i]->size();
      else if (!smallRG.usesStringTable())
        ret += h[i]->size();
      else
        ret += sth[i]->size();
    return ret;
  }

  return rows.size();
}

class TypelessDataStringEncoder
{
  const uint8_t* mStr;
  uint32_t mLength;

 public:
  TypelessDataStringEncoder(const uint8_t* str, uint32_t length) : mStr(str), mLength(length)
  {
  }
  TypelessDataStringEncoder(const utils::ConstString& str)
   : mStr((const uint8_t*)str.str()), mLength(str.length())
  {
  }
  bool store(uint8_t* to, uint32_t& off, uint32_t keylen) const
  {
    if (mLength > 0xFFFF)  // We encode length into two bytes below
    {
      throw runtime_error("Cannot join strings greater than 64KB");
    }

    if (off + mLength + 2 > keylen)
      return true;

    to[off++] = mLength / 0xFF;
    to[off++] = mLength % 0xFF;
    /*
      QQ: perhaps now when we put length,
      we don't need to stop at '\0' bytes any more.
      If so, the loop below can be replace to memcpy().
    */
    for (uint32_t j = 0; j < mLength && mStr[j] != 0; j++)
    {
      if (off >= keylen)
        return true;
      to[off++] = mStr[j];
    }

    return false;
  }
};

class WideDecimalKeyConverter
{
  const Row* mR;
  uint64_t convertedValue;
  const uint32_t mKeyColId;
  uint16_t width;

 public:
  WideDecimalKeyConverter(const Row& r, const uint32_t keyColId)
   : mR(&r), mKeyColId(keyColId), width(datatypes::MAXDECIMALWIDTH)
  {
  }
  bool isConvertedToSmallSideType() const
  {
    return width == datatypes::MAXLEGACYWIDTH;
  }
  int64_t getConvertedTInt64() const
  {
    return (int64_t)convertedValue;
  }
  // Returns true if the value doesn't fit into allowed range for a type.
  template <typename T, typename AT>
  bool numericRangeCheckAndConvert(const AT& value)
  {
    if (value > AT(std::numeric_limits<T>::max()) || value < AT(std::numeric_limits<T>::min()))
      return true;

    convertedValue = (uint64_t) static_cast<T>(value);
    return false;
  }
  // As of MCS 6.x there is an asumption MCS can't join having
  // INTEGER and non-INTEGER potentially fractional keys,
  // e.g. BIGINT to DECIMAL(38,1). It can only join BIGINT to DECIMAL(38).
  // convert() checks if wide-DECIMAL overflows INTEGER type range
  // and sets internal width to 0 if it is. If not width is set to 8
  // and convertedValue is casted to INTEGER type.
  // This convert() is called in EM to cast smallSide TypelessData
  // if the key columns has a skew, e.g. INT to DECIMAL(38).
  inline WideDecimalKeyConverter& convert(const bool otherSideIsIntOrNarrow,
                                          const execplan::CalpontSystemCatalog::ColDataType otherSideType)
  {
    if (otherSideIsIntOrNarrow)
    {
      datatypes::TSInt128 integralPart = mR->getTSInt128Field(mKeyColId);

      bool isUnsigned = datatypes::isUnsigned(otherSideType);
      if (isUnsigned)
      {
        width = (numericRangeCheckAndConvert<uint64_t>(integralPart)) ? 0 : datatypes::MAXLEGACYWIDTH;
        return *this;
      }
      width = (numericRangeCheckAndConvert<int64_t>(integralPart)) ? 0 : datatypes::MAXLEGACYWIDTH;
    }
    return *this;
  }
  // Stores the value that might had been converted.
  inline bool store(TypelessData& typelessData, uint32_t& off, const uint32_t keylen) const
  {
    // A note from convert() if there is otherSide column type range
    // overflow so store() returns TD with len=0. This tells EM to skip this
    // key b/c it won't match at PP. This happens it is possible to skip
    // smallSide TD but can't to do the same with largeSide b/c of OUTER joins.
    if (!width)
    {
      typelessData.len = 0;
      return true;
    }
    if (off + width > keylen)
      return true;
    switch (width)
    {
      case datatypes::MAXDECIMALWIDTH:
      {
        mR->storeInt128FieldIntoPtr(mKeyColId, &typelessData.data[off]);
        break;
      }
      default:
      {
        datatypes::TUInt64(convertedValue).store(&typelessData.data[off]);
      }
    }
    off += width;
    return false;
  }
};

// smallSideColWidths is non-nullptr valid pointer only
// if there is a skew b/w small and large side columns widths.
uint32 TypelessData::hash(const RowGroup& r, const std::vector<uint32_t>& keyCols,
                          const std::vector<uint32_t>* smallSideKeyColumnsIds,
                          const rowgroup::RowGroup* smallSideRG) const
{
  // This part is for largeSide hashing using Row at PP.
  if (!isSmallSide())
  {
    return mRowPtr->hashTypeless(keyCols, smallSideKeyColumnsIds,
                                 (smallSideRG) ? &smallSideRG->getColWidths() : nullptr);
  }
  // This part is for smallSide hashing at PP.
  TypelessDataDecoder decoder(*this);
  datatypes::MariaDBHasher hasher;
  for (auto keyColId : keyCols)
  {
    switch (r.getColTypes()[keyColId])
    {
      case CalpontSystemCatalog::VARCHAR:
      case CalpontSystemCatalog::CHAR:
      case CalpontSystemCatalog::TEXT:
      {
        CHARSET_INFO* cs = const_cast<RowGroup&>(r).getCharset(keyColId);
        hasher.add(cs, decoder.scanString());
        break;
      }
      case CalpontSystemCatalog::DECIMAL:
      {
        const uint32_t width = std::max(r.getColWidths()[keyColId], datatypes::MAXLEGACYWIDTH);
        if (isSmallSideWithSkewedData() || width == datatypes::MAXLEGACYWIDTH)
        {
          int64_t val = decoder.scanTInt64();
          hasher.add(&my_charset_bin, reinterpret_cast<const char*>(&val), datatypes::MAXLEGACYWIDTH);
        }
        else
          hasher.add(&my_charset_bin, decoder.scanGeneric(width));
        break;
      }
      default:
      {
        hasher.add(&my_charset_bin, decoder.scanGeneric(datatypes::MAXLEGACYWIDTH));
        break;
      }
    }
  }
  return hasher.finalize();
}

// this is smallSide, Row represents largeSide record.
int TypelessData::cmpToRow(const RowGroup& r, const std::vector<uint32_t>& keyCols, const rowgroup::Row& row,
                           const std::vector<uint32_t>* smallSideKeyColumnsIds,
                           const rowgroup::RowGroup* smallSideRG) const
{
  TypelessDataDecoder a(*this);

  for (uint32_t i = 0; i < keyCols.size(); i++)
  {
    auto largeSideKeyColRowIdx = keyCols[i];
    switch (r.getColType(largeSideKeyColRowIdx))
    {
      case CalpontSystemCatalog::VARCHAR:
      case CalpontSystemCatalog::CHAR:
      case CalpontSystemCatalog::TEXT:
      {
        datatypes::Charset cs(*const_cast<RowGroup&>(r).getCharset(largeSideKeyColRowIdx));
        ConstString ta = a.scanString();
        ConstString tb = row.getConstString(largeSideKeyColRowIdx);
        if (int rc = cs.strnncollsp(ta, tb))
          return rc;
        break;
      }
      case CalpontSystemCatalog::DECIMAL:
      {
        auto largeSideWidth = row.getColumnWidth(largeSideKeyColRowIdx);
        // First branch processes skewed JOIN, e.g. INT to DECIMAL(38)
        // else branch processes decimal with common width at both small- and largeSide.
        if (isSmallSideWithSkewedData() &&
            largeSideWidth != smallSideRG->getColumnWidth(smallSideKeyColumnsIds->operator[](i)))
        {
          if (largeSideWidth == datatypes::MAXLEGACYWIDTH)
          {
            if (int rc = a.scanTInt64() != row.getIntField(largeSideKeyColRowIdx))
              return rc;
          }
          else
          {
            WideDecimalKeyConverter cv(row, largeSideKeyColRowIdx);
            if (!cv.convert(true, smallSideRG->getColType(smallSideKeyColumnsIds->operator[](i)))
                     .isConvertedToSmallSideType())
              return 1;
            if (int rc = a.scanTInt64() != cv.getConvertedTInt64())
              return rc;
          }
        }
        else
        {
          // There is an assumption that both sides here are equal and are either 8 or 16 bytes.
          if (largeSideWidth == datatypes::MAXDECIMALWIDTH)
          {
            if (int rc = a.scanTInt128() != row.getTSInt128Field(largeSideKeyColRowIdx))
              return rc;
          }
          else
          {
            if (int rc = a.scanTInt64() != row.getIntField(largeSideKeyColRowIdx))
              return rc;
          }
        }
        break;
      }
      default:
      {
        ConstString ta = a.scanGeneric(datatypes::MAXLEGACYWIDTH);
        if (r.isUnsigned(largeSideKeyColRowIdx))
        {
          uint64_t tb = row.getUintField(largeSideKeyColRowIdx);
          if (int rc = memcmp(ta.str(), &tb, datatypes::MAXLEGACYWIDTH))
            return rc;
        }
        else
        {
          int64_t tb = row.getIntField(largeSideKeyColRowIdx);
          if (int rc = memcmp(ta.str(), &tb, datatypes::MAXLEGACYWIDTH))
            return rc;
        }
        break;
      }
    }
  }
  return 0;  // Equal
}

int TypelessData::cmp(const RowGroup& r, const std::vector<uint32_t>& keyCols, const TypelessData& da,
                      const TypelessData& db, const std::vector<uint32_t>* smallSideKeyColumnsIds,
                      const rowgroup::RowGroup* smallSideRG)
{
  idbassert(da.isSmallSide() || db.isSmallSide());
  if (!da.isSmallSide() && db.isSmallSide())
    return -db.cmpToRow(r, keyCols, da.mRowPtr[0], smallSideKeyColumnsIds, smallSideRG);
  if (da.isSmallSide() && !db.isSmallSide())
    return da.cmpToRow(r, keyCols, db.mRowPtr[0], smallSideKeyColumnsIds, smallSideRG);

  // This case happens in BPP::addToJoiner when it populates the final
  // hashmap with multiple smallSide TDs from temp hashmaps.
  idbassert(da.isSmallSide() && db.isSmallSide());

  TypelessDataDecoder a(da);
  TypelessDataDecoder b(db);

  for (uint32_t i = 0; i < keyCols.size(); ++i)
  {
    auto keyColIdx = keyCols[i];
    switch (r.getColTypes()[keyColIdx])
    {
      case CalpontSystemCatalog::VARCHAR:
      case CalpontSystemCatalog::CHAR:
      case CalpontSystemCatalog::TEXT:
      {
        datatypes::Charset cs(*const_cast<RowGroup&>(r).getCharset(keyColIdx));
        ConstString ta = a.scanString();
        ConstString tb = b.scanString();
        if (int rc = cs.strnncollsp(ta, tb))
          return rc;
        break;
      }
      case CalpontSystemCatalog::DECIMAL:
      {
        auto largeSideWidth = r.getColumnWidth(keyColIdx);
        // First and second branches processes skewed JOIN, e.g. INT to DECIMAL(38)
        // Third processes decimal with common width at both small- and largeSide.
        auto width = (da.isSmallSideWithSkewedData() &&
                      largeSideWidth != smallSideRG->getColumnWidth(smallSideKeyColumnsIds->operator[](i)))
                         ? datatypes::MAXLEGACYWIDTH
                         : std::max(r.getColWidths()[keyColIdx], datatypes::MAXLEGACYWIDTH);
        ConstString ta = a.scanGeneric(width);
        ConstString tb = b.scanGeneric(width);
        if (int rc = memcmp(ta.str(), tb.str(), width))
          return rc;
        break;
      }
      default:
      {
        ConstString ta = a.scanGeneric(datatypes::MAXLEGACYWIDTH);
        ConstString tb = b.scanGeneric(datatypes::MAXLEGACYWIDTH);
        idbassert(ta.length() == tb.length());
        // It is impossible to join signed to unsigned types now
        // but there is a potential error, e.g. uint64 vs negative int64.
        if (int rc = memcmp(ta.str(), tb.str(), ta.length()))
          return rc;
        break;
      }
    }
  }
  return 0;  // Equal
}

// Called in joblist code to produce SmallSide TypelessData to be sent to PP.
TypelessData makeTypelessKey(const Row& r, const vector<uint32_t>& keyCols, uint32_t keylen,
                             FixedAllocator* fa, const rowgroup::RowGroup& otherSideRG,
                             const std::vector<uint32_t>& otherKeyCols)
{
  TypelessData ret;
  uint32_t off = 0, i;
  execplan::CalpontSystemCatalog::ColDataType type;

  ret.data = (uint8_t*)fa->allocate();
  idbassert(keyCols.size() == otherKeyCols.size());

  for (i = 0; i < keyCols.size(); i++)
  {
    type = r.getColTypes()[keyCols[i]];

    if (datatypes::isCharType(type))
    {
      // this is a string, copy a normalized version
      const utils::ConstString str = r.getConstString(keyCols[i]);
      if (TypelessDataStringEncoder(str).store(ret.data, off, keylen))
        goto toolong;
    }
    else if (datatypes::isWideDecimalType(type, r.getColumnWidth(keyCols[i])))
    {
      bool otherSideIsIntOrNarrow = otherSideRG.getColumnWidth(otherKeyCols[i]) <= datatypes::MAXLEGACYWIDTH;
      // useless if otherSideIsInt is false
      auto otherSideType = (otherSideIsIntOrNarrow) ? otherSideRG.getColType(otherKeyCols[i])
                                                    : datatypes::SystemCatalog::UNDEFINED;
      if (WideDecimalKeyConverter(r, keyCols[i])
              .convert(otherSideIsIntOrNarrow, otherSideType)
              .store(ret, off, keylen))
      {
        goto toolong;
      }
    }
    else if (datatypes::isLongDouble(type))
    {
      if (off + sizeof(long double) > keylen)
        goto toolong;
      // Small side is a long double. Since CS can't store larger than DOUBLE,
      // we need to convert to whatever type large side is -- double or int64
      long double keyld = r.getLongDoubleField(keyCols[i]);
      switch (otherSideRG.getColType(otherKeyCols[i]))
      {
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
        {
          if (off + 8 > keylen)
            goto toolong;
          if (keyld > MAX_DOUBLE || keyld < MIN_DOUBLE)
          {
            ret.len = 0;
            return ret;
          }
          else
          {
            double d = (double)keyld;
            *((int64_t*)&ret.data[off]) = *(int64_t*)&d;
          }
          break;
        }
        case CalpontSystemCatalog::LONGDOUBLE:
        {
          if (off + sizeof(long double) > keylen)
            goto toolong;
          *((long double*)&ret.data[off]) = keyld;
          off += sizeof(long double);
          break;
        }
        default:
        {
          if (off + 8 > keylen)
            goto toolong;
          if (r.isUnsigned(keyCols[i]) && keyld > MAX_UBIGINT)
          {
            ret.len = 0;
            return ret;
          }
          else if (keyld > MAX_BIGINT || keyld < MIN_BIGINT)
          {
            ret.len = 0;
            return ret;
          }
          else
          {
            *((int64_t*)&ret.data[off]) = (int64_t)keyld;
            off += 8;
          }
          break;
        }
      }
    }
    else if (r.isUnsigned(keyCols[i]))
    {
      if (off + 8 > keylen)
        goto toolong;
      *((uint64_t*)&ret.data[off]) = r.getUintField(keyCols[i]);
      off += 8;
    }
    else
    {
      if (off + 8 > keylen)
        goto toolong;
      *((int64_t*)&ret.data[off]) = r.getIntField(keyCols[i]);
      off += 8;
    }
  }

  ret.len = off;
  fa->truncateBy(keylen - off);
  return ret;
toolong:
  fa->truncateBy(keylen);
  ret.len = 0;
  return ret;
}

// The method is used by disk-based JOIN and it is not collation or wide DECIMAL aware.
uint64_t getHashOfTypelessKey(const Row& r, const vector<uint32_t>& keyCols, uint32_t seed)
{
  Hasher_r hasher;
  uint64_t ret = seed, tmp;
  uint32_t i;
  uint32_t width = 0;
  char nullChar = '\0';
  execplan::CalpontSystemCatalog::ColDataType type;

  for (i = 0; i < keyCols.size(); i++)
  {
    type = r.getColTypes()[keyCols[i]];

    if (type == CalpontSystemCatalog::VARCHAR || type == CalpontSystemCatalog::CHAR ||
        type == CalpontSystemCatalog::TEXT)
    {
      // this is a string, copy a normalized version
      const utils::ConstString str = r.getConstString(keyCols[i]);
      ret = hasher(str.str(), str.length(), ret);
      ret = hasher(&nullChar, 1, ret);
      width += str.length() + 1;
    }
    else if (r.getColType(keyCols[i]) == CalpontSystemCatalog::LONGDOUBLE)
    {
      long double tmp = r.getLongDoubleField(keyCols[i]);
      ret = hasher((char*)&tmp, sizeof(long double), ret);
      width += sizeof(long double);
    }
    else if (r.isUnsigned(keyCols[i]))
    {
      tmp = r.getUintField(keyCols[i]);
      ret = hasher((char*)&tmp, 8, ret);
      width += 8;
    }
    else
    {
      tmp = r.getIntField(keyCols[i]);
      ret = hasher((char*)&tmp, 8, ret);
      width += 8;
    }
  }

  ret = hasher.finalize(ret, width);
  return ret;
}

string TypelessData::toString() const
{
  uint32_t i;
  ostringstream os;

  os << hex;

  for (i = 0; i < len; i++)
  {
    os << (uint32_t)data[i] << " ";
  }

  os << dec;
  return os.str();
}

void TypelessData::serialize(messageqcpp::ByteStream& b) const
{
  b << len;
  b.append(data, len);
  // Flags are not send b/c they are locally significant now.
}

void TypelessData::deserialize(messageqcpp::ByteStream& b, utils::PoolAllocator& fa)
{
  b >> len;
  data = (uint8_t*)fa.allocate(len);
  memcpy(data, b.buf(), len);
  b.advance(len);
}

bool TupleJoiner::hasNullJoinColumn(const Row& r) const
{
  uint64_t key;

  for (uint32_t i = 0; i < largeKeyColumns.size(); i++)
  {
    if (r.isNullValue(largeKeyColumns[i]))
      return true;

    if (UNLIKELY(bSignedUnsignedJoin))
    {
      // BUG 5628 If this is a signed/unsigned join column and the sign bit is set on either
      // side, then this row should not compare. Treat as NULL to prevent compare, even if
      // the bit patterns match.
      if (smallRG.isUnsigned(smallKeyColumns[i]) != largeRG.isUnsigned(largeKeyColumns[i]))
      {
        if (r.isUnsigned(largeKeyColumns[i]))
          key = r.getUintField(largeKeyColumns[i]);  // Does not propogate sign bit
        else
          key = r.getIntField(largeKeyColumns[i]);  // Propogates sign bit

        if (key & 0x8000000000000000ULL)
        {
          return true;
        }
      }
    }
  }

  return false;
}

string TupleJoiner::getTableName() const
{
  return tableName;
}

void TupleJoiner::setTableName(const string& tname)
{
  tableName = tname;
}

/* Disk based join support */

void TupleJoiner::clearData()
{
  _pool.reset(new boost::shared_ptr<utils::PoolAllocator>[bucketCount]);
  if (typelessJoin)
    ht.reset(new boost::scoped_ptr<typelesshash_t>[bucketCount]);
  else if (smallRG.getColTypes()[smallKeyColumns[0]] == CalpontSystemCatalog::LONGDOUBLE)
    ld.reset(new boost::scoped_ptr<ldhash_t>[bucketCount]);
  else if (smallRG.usesStringTable())
    sth.reset(new boost::scoped_ptr<sthash_t>[bucketCount]);
  else
    h.reset(new boost::scoped_ptr<hash_t>[bucketCount]);

  for (uint i = 0; i < bucketCount; i++)
  {
    STLPoolAllocator<pair<const TypelessData, Row::Pointer>> alloc;
    _pool[i] = alloc.getPoolAllocator();
    if (typelessJoin)
      ht[i].reset(new typelesshash_t(10, hasher(), typelesshash_t::key_equal(), alloc));
    else if (smallRG.getColTypes()[smallKeyColumns[0]] == CalpontSystemCatalog::LONGDOUBLE)
      ld[i].reset(new ldhash_t(10, hasher(), ldhash_t::key_equal(), alloc));
    else if (smallRG.usesStringTable())
      sth[i].reset(new sthash_t(10, hasher(), sthash_t::key_equal(), alloc));
    else
      h[i].reset(new hash_t(10, hasher(), hash_t::key_equal(), alloc));
  }

  std::vector<rowgroup::Row::Pointer> empty;
  rows.swap(empty);
  finished = false;
}

boost::shared_ptr<TupleJoiner> TupleJoiner::copyForDiskJoin()
{
  boost::shared_ptr<TupleJoiner> ret(new TupleJoiner());

  ret->smallRG = smallRG;
  ret->largeRG = largeRG;
  ret->smallNullMemory = smallNullMemory;
  ret->smallNullRow = smallNullRow;
  ret->joinType = joinType;
  ret->tableName = tableName;
  ret->typelessJoin = typelessJoin;
  ret->smallKeyColumns = smallKeyColumns;
  ret->largeKeyColumns = largeKeyColumns;
  ret->keyLength = keyLength;
  ret->bSignedUnsignedJoin = bSignedUnsignedJoin;
  ret->fe = fe;

  ret->nullValueForJoinColumn = nullValueForJoinColumn;
  ret->uniqueLimit = uniqueLimit;

  ret->discreteValues.reset(new bool[smallKeyColumns.size()]);
  ret->cpValues.reset(new vector<int128_t>[smallKeyColumns.size()]);

  for (uint32_t i = 0; i < smallKeyColumns.size(); i++)
  {
    ret->discreteValues[i] = false;
    if (isUnsigned(smallRG.getColTypes()[smallKeyColumns[i]]))
    {
      if (datatypes::isWideDecimalType(smallRG.getColType(smallKeyColumns[i]),
                                       smallRG.getColumnWidth(smallKeyColumns[i])))
      {
        ret->cpValues[i].push_back((int128_t)-1);
        ret->cpValues[i].push_back(0);
      }
      else
      {
        ret->cpValues[i].push_back((int128_t)numeric_limits<uint64_t>::max());
        ret->cpValues[i].push_back(0);
      }
    }
    else
    {
      if (datatypes::isWideDecimalType(smallRG.getColType(smallKeyColumns[i]),
                                       smallRG.getColumnWidth(smallKeyColumns[i])))
      {
        ret->cpValues[i].push_back(utils::maxInt128);
        ret->cpValues[i].push_back(utils::minInt128);
      }
      else
      {
        ret->cpValues[i].push_back(numeric_limits<int64_t>::max());
        ret->cpValues[i].push_back(numeric_limits<int64_t>::min());
      }
    }
  }

  if (typelessJoin)
  {
    ret->storedKeyAlloc.reset(new FixedAllocator[numCores]);
    for (int i = 0; i < numCores; i++)
      ret->storedKeyAlloc[i].setAllocSize(keyLength);
  }

  ret->numCores = numCores;
  ret->bucketCount = bucketCount;
  ret->bucketMask = bucketMask;
  ret->jobstepThreadPool = jobstepThreadPool;

  ret->setThreadCount(1);
  ret->clearData();
  ret->setInUM();
  return ret;
}

// Used for Typeless JOIN to detect if there is a JOIN when largeSide is wide-DECIMAL and
// smallSide is a smaller data type, e.g. INT or narrow-DECIMAL.
bool TupleJoiner::joinHasSkewedKeyColumn()
{
  std::vector<uint32_t>::const_iterator largeSideKeyColumnsIter = getLargeKeyColumns().begin();
  std::vector<uint32_t>::const_iterator smallSideKeyColumnsIter = getSmallKeyColumns().begin();
  idbassert(getLargeKeyColumns().size() == getSmallKeyColumns().size());
  while (largeSideKeyColumnsIter != getLargeKeyColumns().end())
  {
    auto smallSideColumnWidth = smallRG.getColumnWidth(*smallSideKeyColumnsIter);
    auto largeSideColumnWidth = largeRG.getColumnWidth(*largeSideKeyColumnsIter);
    bool widthIsDifferent = smallSideColumnWidth != largeSideColumnWidth;
    if (widthIsDifferent &&
        (datatypes::isWideDecimalType(smallRG.getColTypes()[*smallSideKeyColumnsIter],
                                      smallSideColumnWidth) ||
         datatypes::isWideDecimalType(largeRG.getColTypes()[*largeSideKeyColumnsIter], largeSideColumnWidth)))
    {
      return true;
    }
    ++largeSideKeyColumnsIter;
    ++smallSideKeyColumnsIter;
  }
  return false;
}

void TupleJoiner::setConvertToDiskJoin()
{
  _convertToDiskJoin = true;
}

// The method is made to reuse the code from Typeless TupleJoiner ctor.
// It is used in the mentioned ctor and in initBPP() to calculate
// Typeless key length in case of a JOIN when large side column is INT
// and small side column is wide-DECIMAL.
// An important assumption is that if the type is DECIMAL than it must
// be wide-DECIMAL b/c MCS calls the function running Typeless TupleJoiner
// ctor.
uint32_t calculateKeyLength(const std::vector<uint32_t>& aKeyColumnsIds,
                            const rowgroup::RowGroup& aSmallRowGroup,
                            const std::vector<uint32_t>* aLargeKeyColumnsIds,
                            const rowgroup::RowGroup* aLargeRowGroup)
{
  uint32_t keyLength = 0;
  for (size_t keyColumnIdx = 0; keyColumnIdx < aKeyColumnsIds.size(); ++keyColumnIdx)
  {
    auto smallSideKeyColumnId = aKeyColumnsIds[keyColumnIdx];
    auto largeSideKeyColumnId = (aLargeRowGroup) ? aLargeKeyColumnsIds->operator[](keyColumnIdx)
                                                 : std::numeric_limits<uint64_t>::max();
    const auto& smallKeyColumnType = aSmallRowGroup.getColTypes()[smallSideKeyColumnId];
    // Not used if aLargeRowGroup is 0 that happens in PrimProc.
    const auto& largeKeyColumntype = (aLargeRowGroup) ? aLargeRowGroup->getColTypes()[largeSideKeyColumnId]
                                                      : datatypes::SystemCatalog::UNDEFINED;
    if (datatypes::isCharType(smallKeyColumnType))
    {
      keyLength += aSmallRowGroup.getColumnWidth(smallSideKeyColumnId) + 2;  // +2 for encoded length

      // MCOL-698: if we don't do this LONGTEXT allocates 32TB RAM
      if (keyLength > 65536)
        return 65536;
    }
    else if (datatypes::isLongDouble(smallKeyColumnType))
    {
      keyLength += sizeof(long double);
    }
    else if (datatypes::isWideDecimalType(smallKeyColumnType,
                                          aSmallRowGroup.getColumnWidth(smallSideKeyColumnId)))
    {
      keyLength += (aLargeRowGroup &&
                    !datatypes::isWideDecimalType(largeKeyColumntype,
                                                  aLargeRowGroup->getColumnWidth(smallSideKeyColumnId)))
                       ? datatypes::MAXLEGACYWIDTH    // Small=Wide, Large=Narrow/xINT
                       : datatypes::MAXDECIMALWIDTH;  // Small=Wide, Large=Wide
    }
    else
    // The branch covers all datatypes left including skewed DECIMAL JOIN case
    // Small=Wide, Large=Narrow
    {
      keyLength += datatypes::MAXLEGACYWIDTH;
    }
  }

  return keyLength;
}

};  // namespace joiner

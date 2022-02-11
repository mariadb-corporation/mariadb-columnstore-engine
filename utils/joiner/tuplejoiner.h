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

#ifndef TJOINER_H_
#define TJOINER_H_

#include <iostream>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/scoped_array.hpp>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif

#include "rowgroup.h"
#include "joiner.h"
#include "fixedallocator.h"
#include "joblisttypes.h"
#include "../funcexp/funcexpwrapper.h"
#include "stlpoolallocator.h"
#include "hasher.h"
#include "threadpool.h"
#include "columnwidth.h"
#include "mcs_string.h"

namespace joiner
{
uint32_t calculateKeyLength(const std::vector<uint32_t>& aKeyColumnsIds, const rowgroup::RowGroup& aRowGroup,
                            const std::vector<uint32_t>* aLargeKeyColumnsIds = nullptr,
                            const rowgroup::RowGroup* aLargeRowGroup = nullptr);

constexpr uint8_t IS_SMALLSIDE = 0x01;         // SmallSide of a JOIN w/o a skew in key columns widths
constexpr uint8_t IS_SMALLSIDE_SKEWED = 0x02;  // SmallSide of a JOIN with a skew in key cols widths
class TypelessDataDecoder;

class TypelessData
{
 public:
  union
  {
    uint8_t* data;
    const rowgroup::Row* mRowPtr;
  };
  uint32_t len;
  // The flags are locally significant in PP now so serialize doesn't send it over the wire.
  uint32_t mFlags;

  TypelessData() : data(nullptr), len(0), mFlags(0)
  {
  }
  TypelessData(const rowgroup::Row* rowPtr) : mRowPtr(rowPtr), len(0), mFlags(0)
  {
  }
  TypelessData(messageqcpp::ByteStream& bs, utils::PoolAllocator& memAllocator)
   : data(nullptr), len(0), mFlags(0)
  {
    deserialize(bs, memAllocator);
  }
  inline bool operator==(const TypelessData&) const;
  void serialize(messageqcpp::ByteStream&) const;
  void deserialize(messageqcpp::ByteStream&, utils::FixedAllocator&);
  void deserialize(messageqcpp::ByteStream&, utils::PoolAllocator&);
  std::string toString() const;
  uint32_t hash(const rowgroup::RowGroup&, const std::vector<uint32_t>& keyCols,
                const std::vector<uint32_t>* smallSideKeyColumnsIds,
                const rowgroup::RowGroup* smallSideRG) const;
  static int cmp(const rowgroup::RowGroup&, const std::vector<uint32_t>& keyCols, const TypelessData& a,
                 const TypelessData& b, const std::vector<uint32_t>* smallSideKeyColumnsIds,
                 const rowgroup::RowGroup* smallSideRG);
  int cmpToRow(const rowgroup::RowGroup& r, const std::vector<uint32_t>& keyCols, const rowgroup::Row& row,
               const std::vector<uint32_t>* smallSideKeyColumnsIds,
               const rowgroup::RowGroup* smallSideRG) const;
  inline void setSmallSide()
  {
    mFlags |= IS_SMALLSIDE;
  }
  inline void setSmallSideWithSkewedData()
  {
    mFlags |= IS_SMALLSIDE_SKEWED;
  }
  inline bool isSmallSide() const
  {
    return mFlags & (IS_SMALLSIDE_SKEWED | IS_SMALLSIDE);
  }
  inline bool isSmallSideWithSkewedData() const
  {
    return mFlags & IS_SMALLSIDE_SKEWED;
  }
};

// This operator is used in EM only so it doesn't support TD cmp operation
// using Row pointers.
inline bool TypelessData::operator==(const TypelessData& t) const
{
  if (len != t.len)
    return false;

  if (len == 0)  // special value to force mismatches
    return false;

  return (memcmp(data, t.data, len) == 0);
}

class TypelessDataDecoder
{
  const uint8_t* mPtr;
  const uint8_t* mEnd;
  void checkAvailableData(uint32_t nbytes) const
  {
    if (mPtr + nbytes > mEnd)
      throw runtime_error("TypelessData is too short");
  }

 public:
  TypelessDataDecoder(const uint8_t* ptr, size_t length) : mPtr(ptr), mEnd(ptr + length)
  {
  }
  TypelessDataDecoder(const TypelessData& data) : TypelessDataDecoder(data.data, data.len)
  {
  }
  utils::ConstString scanGeneric(uint32_t length)
  {
    checkAvailableData(length);
    utils::ConstString res((const char*)mPtr, length);
    mPtr += length;
    return res;
  }
  uint32_t scanStringLength()
  {
    checkAvailableData(2);
    uint32_t res = ((uint32_t)mPtr[0]) * 255 + mPtr[1];
    mPtr += 2;
    return res;
  }
  utils::ConstString scanString()
  {
    return scanGeneric(scanStringLength());
  }
  int64_t scanTInt64()
  {
    checkAvailableData(sizeof(int64_t));
    int64_t res = *reinterpret_cast<const int64_t*>(mPtr);
    mPtr += sizeof(int64_t);
    return res;
  }
  datatypes::TSInt128 scanTInt128()
  {
    checkAvailableData(datatypes::MAXDECIMALWIDTH);
    datatypes::TSInt128 res(mPtr);
    mPtr += datatypes::MAXDECIMALWIDTH;
    return res;
  }
};

// Comparator for long double in the hash
class LongDoubleEq
{
 public:
  LongDoubleEq(){};
  inline bool operator()(const long double& pos1, const long double& pos2) const
  {
    return pos1 == pos2;
  }
};

/* This function makes the keys for string & compound joins.  The length of the
 * key is limited by keylen.  Keys that are longer are assigned a length of 0 on return,
 * signifying that it shouldn't match anything.
 */
// MCOL-1822 SUM/AVG as long double: pass in RG and col so we can determine type conversion
extern TypelessData makeTypelessKey(const rowgroup::Row&, const std::vector<uint32_t>&, uint32_t keylen,
                                    utils::FixedAllocator* fa, const rowgroup::RowGroup&,
                                    const std::vector<uint32_t>&);
extern uint64_t getHashOfTypelessKey(const rowgroup::Row&, const std::vector<uint32_t>&, uint32_t seed = 0);

class TypelessDataStructure
{
 public:
  const rowgroup::RowGroup* mRowGroup;
  const std::vector<uint32_t>* mMap;
  const std::vector<uint32_t>* mSmallSideKeyColumnsIds;
  const rowgroup::RowGroup* mSmallSideRG;
  TypelessDataStructure(const rowgroup::RowGroup* rg, const std::vector<uint32_t>* map,
                        const std::vector<uint32_t>* smallSideKeyColumnsIds,
                        const rowgroup::RowGroup* smallSideRG)
   : mRowGroup(rg), mMap(map), mSmallSideKeyColumnsIds(smallSideKeyColumnsIds), mSmallSideRG(smallSideRG)
  {
  }
};

class TupleJoiner
{
 public:
  struct hasher
  {
    inline size_t operator()(int64_t val) const
    {
      return fHasher((char*)&val, 8);
    }
    inline size_t operator()(uint64_t val) const
    {
      return fHasher((char*)&val, 8);
    }
    inline size_t operator()(const TypelessData& e) const
    {
      return fHasher((char*)e.data, e.len);
    }
    inline size_t operator()(long double val) const
    {
      if (sizeof(long double) == 8)  // Probably just MSC, but you never know.
      {
        return fHasher((char*)&val, sizeof(long double));
      }
      else
      {
        // For Linux x86_64, long double is stored in 128 bits, but only 80 are significant
        return fHasher((char*)&val, 10);
      }
    }

   private:
    utils::Hasher fHasher;
  };

  struct TypelessDataHasher : public TypelessDataStructure
  {
    TypelessDataHasher(const rowgroup::RowGroup* rg, const std::vector<uint32_t>* map,
                       const std::vector<uint32_t>* smallSideKeyColumnsIds,
                       const rowgroup::RowGroup* smallSideRG)
     : TypelessDataStructure(rg, map, smallSideKeyColumnsIds, smallSideRG)
    {
    }
    inline size_t operator()(const TypelessData& e) const
    {
      return e.hash(*mRowGroup, *mMap, mSmallSideKeyColumnsIds, mSmallSideRG);
    }
  };

  struct TypelessDataComparator : public TypelessDataStructure
  {
   public:
    TypelessDataComparator(const rowgroup::RowGroup* rg, const std::vector<uint32_t>* map,
                           const std::vector<uint32_t>* smallSideKeyColumnsIds,
                           const rowgroup::RowGroup* smallSideRG)
     : TypelessDataStructure(rg, map, smallSideKeyColumnsIds, smallSideRG)
    {
    }
    bool operator()(const TypelessData& a, const TypelessData& b) const
    {
      return !TypelessData::cmp(*mRowGroup, *mMap, a, b, mSmallSideKeyColumnsIds, mSmallSideRG);
    }
  };

  /* ctor to use for numeric join */
  TupleJoiner(const rowgroup::RowGroup& smallInput, const rowgroup::RowGroup& largeInput,
              uint32_t smallJoinColumn, uint32_t largeJoinColumn, joblist::JoinType jt,
              threadpool::ThreadPool* jsThreadPool);

  /* ctor to use for string & compound join */
  TupleJoiner(const rowgroup::RowGroup& smallInput, const rowgroup::RowGroup& largeInput,
              const std::vector<uint32_t>& smallJoinColumns, const std::vector<uint32_t>& largeJoinColumns,
              joblist::JoinType jt, threadpool::ThreadPool* jsThreadPool);

  ~TupleJoiner();

  size_t size() const;
  void insert(rowgroup::Row& r, bool zeroTheRid = true);  // not thread-safe
  void insertRGData(rowgroup::RowGroup& rg, uint threadID);
  void doneInserting();

  /* match() returns the small-side rows that match the large-side row.
      On a UM join, it uses largeSideRow,
      on a PM join, it uses index and threadID.
  */
  void match(rowgroup::Row& largeSideRow, uint32_t index, uint32_t threadID,
             std::vector<rowgroup::Row::Pointer>* matches);

  /* On a PM left outer join + aggregation, the result is already complete.
      No need to match, just mark.
  */
  void markMatches(uint32_t threadID, uint32_t rowCount);

  /* For small outer joins, this is how matches are marked now. */
  void markMatches(uint32_t threadID, const std::vector<rowgroup::Row::Pointer>& matches);

  /* Some accessors */
  inline bool inPM() const
  {
    return joinAlg == PM;
  }
  inline bool inUM() const
  {
    return joinAlg == UM;
  }
  inline bool onDisk() const
  {
    return _convertToDiskJoin;
  }
  void setInPM();
  void setInUM(std::vector<rowgroup::RGData>& rgs);
  void umJoinConvert(uint threadID, std::vector<rowgroup::RGData>& rgs, size_t begin, size_t end);

  // TODO: these are currently in use by edge cases, ex, converting to disk
  // join.  Would be nice to make those cases use the rgdata variants
  // above.
  void setInUM();
  void umJoinConvert(size_t begin, size_t end);

  void setThreadCount(uint32_t cnt);
  void setPMJoinResults(boost::shared_array<std::vector<uint32_t> >, uint32_t threadID);
  boost::shared_array<std::vector<uint32_t> > getPMJoinArrays(uint32_t threadID);
  std::vector<rowgroup::Row::Pointer>* getSmallSide()
  {
    return &rows;
  }
  inline bool smallOuterJoin()
  {
    return ((joinType & joblist::SMALLOUTER) != 0);
  }
  inline bool largeOuterJoin()
  {
    return ((joinType & joblist::LARGEOUTER) != 0);
  }
  inline bool innerJoin()
  {
    return joinType == joblist::INNER;
  }
  inline bool fullOuterJoin()
  {
    return (smallOuterJoin() && largeOuterJoin());
  }
  inline joblist::JoinType getJoinType()
  {
    return joinType;
  }
  inline const rowgroup::RowGroup& getSmallRG()
  {
    return smallRG;
  }
  inline const rowgroup::RowGroup& getLargeRG()
  {
    return largeRG;
  }
  inline uint32_t getSmallKeyColumn()
  {
    return smallKeyColumns[0];
  }
  inline uint32_t getLargeKeyColumn()
  {
    return largeKeyColumns[0];
  }
  bool hasNullJoinColumn(const rowgroup::Row& largeRow) const;
  void getUnmarkedRows(std::vector<rowgroup::Row::Pointer>* out);
  std::string getTableName() const;
  void setTableName(const std::string& tname);

  /* To allow sorting */
  bool operator<(const TupleJoiner&) const;

  uint64_t getMemUsage() const;

  /* Typeless join interface */
  inline bool isTypelessJoin()
  {
    return typelessJoin;
  }
  inline bool isSignedUnsignedJoin()
  {
    return bSignedUnsignedJoin;
  }
  inline const std::vector<uint32_t>& getSmallKeyColumns()
  {
    return smallKeyColumns;
  }
  inline const std::vector<uint32_t>& getLargeKeyColumns()
  {
    return largeKeyColumns;
  }
  inline uint32_t getKeyLength()
  {
    return keyLength;
  }

  /* Runtime casual partitioning support */
  inline const boost::scoped_array<bool>& discreteCPValues()
  {
    return discreteValues;
  }
  inline const boost::scoped_array<std::vector<int128_t> >& getCPData()
  {
    return cpValues;
  }
  inline void setUniqueLimit(uint32_t limit)
  {
    uniqueLimit = limit;
  }

  /* Semi-join interface */
  inline bool semiJoin()
  {
    return ((joinType & joblist::SEMI) != 0);
  }
  inline bool antiJoin()
  {
    return ((joinType & joblist::ANTI) != 0);
  }
  inline bool scalar()
  {
    return ((joinType & joblist::SCALAR) != 0);
  }
  inline bool matchnulls()
  {
    return ((joinType & joblist::MATCHNULLS) != 0);
  }
  inline bool hasFEFilter()
  {
    return fe.get();
  }
  inline boost::shared_ptr<funcexp::FuncExpWrapper> getFcnExpFilter()
  {
    return fe;
  }
  void setFcnExpFilter(boost::shared_ptr<funcexp::FuncExpWrapper> fe);
  inline bool evaluateFilter(rowgroup::Row& r, uint32_t index)
  {
    return fes[index].evaluate(&r);
  }
  inline uint64_t getJoinNullValue()
  {
    return joblist::BIGINTNULL;  // a normalized NULL value
  }
  inline uint64_t smallNullValue()
  {
    return nullValueForJoinColumn;
  }

  // Wide-DECIMAL JOIN
  bool joinHasSkewedKeyColumn();
  inline const vector<uint32_t>& getSmallSideColumnsWidths() const
  {
    return smallRG.getColWidths();
  }
  // Disk-based join support
  void clearData();
  boost::shared_ptr<TupleJoiner> copyForDiskJoin();
  bool isFinished()
  {
    return finished;
  }
  void setConvertToDiskJoin();

 private:
  typedef std::tr1::unordered_multimap<int64_t, uint8_t*, hasher, std::equal_to<int64_t>,
                                       utils::STLPoolAllocator<std::pair<const int64_t, uint8_t*> > >
      hash_t;
  typedef std::tr1::unordered_multimap<
      int64_t, rowgroup::Row::Pointer, hasher, std::equal_to<int64_t>,
      utils::STLPoolAllocator<std::pair<const int64_t, rowgroup::Row::Pointer> > >
      sthash_t;
  typedef std::tr1::unordered_multimap<
      TypelessData, rowgroup::Row::Pointer, hasher, std::equal_to<TypelessData>,
      utils::STLPoolAllocator<std::pair<const TypelessData, rowgroup::Row::Pointer> > >
      typelesshash_t;
  // MCOL-1822 Add support for Long Double AVG/SUM small side
  typedef std::tr1::unordered_multimap<
      long double, rowgroup::Row::Pointer, hasher, LongDoubleEq,
      utils::STLPoolAllocator<std::pair<const long double, rowgroup::Row::Pointer> > >
      ldhash_t;

  typedef hash_t::iterator iterator;
  typedef typelesshash_t::iterator thIterator;
  typedef ldhash_t::iterator ldIterator;

  TupleJoiner();
  TupleJoiner(const TupleJoiner&);
  TupleJoiner& operator=(const TupleJoiner&);
  void getBucketCount();

  rowgroup::RGData smallNullMemory;

  boost::scoped_array<boost::scoped_ptr<hash_t> > h;  // used for UM joins on ints
  boost::scoped_array<boost::scoped_ptr<sthash_t> >
      sth;  // used for UM join on ints where the backing table uses a string table
  boost::scoped_array<boost::scoped_ptr<ldhash_t> > ld;  // used for UM join on long double
  std::vector<rowgroup::Row::Pointer> rows;              // used for PM join

  /* This struct is rough.  The BPP-JL stores the parsed results for
  the logical block being processed.  There are X threads at once, so
  up to X logical blocks being processed.  For each of those there's a vector
  of matches.  Each match is an index into 'rows'. */
  boost::shared_array<boost::shared_array<std::vector<uint32_t> > > pmJoinResults;
  rowgroup::RowGroup smallRG, largeRG;
  boost::scoped_array<rowgroup::Row> smallRow;
  // boost::shared_array<uint8_t> smallNullMemory;
  rowgroup::Row smallNullRow;

  enum JoinAlg
  {
    INSERTING,
    PM,
    UM,
    LARGE
  };
  JoinAlg joinAlg;
  joblist::JoinType joinType;
  boost::shared_array<boost::shared_ptr<utils::PoolAllocator> > _pool;  // pools for the table and nodes
  uint32_t threadCount;
  std::string tableName;

  /* vars, & fcns for typeless join */
  bool typelessJoin;
  std::vector<uint32_t> smallKeyColumns, largeKeyColumns;
  boost::scoped_array<boost::scoped_ptr<typelesshash_t> > ht;  // used for UM join on strings
  uint32_t keyLength;
  boost::scoped_array<utils::FixedAllocator> storedKeyAlloc;
  boost::scoped_array<utils::FixedAllocator> tmpKeyAlloc;
  bool bSignedUnsignedJoin;  // Set if we have a signed vs unsigned compare in a join. When not set, we can
                             // save checking for the signed bit.

  /* semi-join vars & fcns */
  boost::shared_ptr<funcexp::FuncExpWrapper> fe;
  boost::scoped_array<funcexp::FuncExpWrapper> fes;  // holds X copies of fe, one per thread
  // this var is only used to normalize the NULL values for single-column joins,
  // will have to change when/if we need to support that for compound or string joins
  int64_t nullValueForJoinColumn;

  /* Runtime casual partitioning support */
  void updateCPData(const rowgroup::Row& r);
  boost::scoped_array<bool> discreteValues;
  boost::scoped_array<std::vector<int128_t> > cpValues;  // if !discreteValues, [0] has min, [1] has max
  uint32_t uniqueLimit;
  bool finished;

  // multithreaded UM hash table construction
  int numCores;
  uint bucketCount;
  uint bucketMask;
  boost::scoped_array<boost::mutex> m_bucketLocks;
  boost::mutex m_typelessLock, m_cpValuesLock;
  utils::Hasher_r bucketPicker;
  const uint32_t bpSeed = 0x4545e1d7;  // an arbitrary random #
  threadpool::ThreadPool* jobstepThreadPool;
  void um_insertTypeless(uint threadID, uint rowcount, rowgroup::Row& r);
  void um_insertLongDouble(uint rowcount, rowgroup::Row& r);
  void um_insertInlineRows(uint rowcount, rowgroup::Row& r);
  void um_insertStringTable(uint rowcount, rowgroup::Row& r);

  template <typename buckets_t, typename hash_table_t>
  void bucketsToTables(buckets_t*, hash_table_t*);

  bool _convertToDiskJoin;
};

}  // namespace joiner

#endif

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

/***********************************************************************
 *   $Id: lbidlist.h 9655 2013-06-25 23:08:13Z xlou $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef JOBLIST_LBIDLIST_H
#define JOBLIST_LBIDLIST_H

#include <boost/shared_ptr.hpp>
#include "joblisttypes.h"
#include "calpontsystemcatalog.h"
#include "brmtypes.h"
#include "bytestream.h"
#include <iostream>
#include "brm.h"
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif

namespace joblist
{
typedef BRM::LBIDRange_v LBIDRangeVector;

/** @brief struct MinMaxPartition
 *
 */
struct MinMaxPartition
{
  int64_t lbid;
  int64_t lbidmax;
  int64_t seq;
  int isValid;
  uint32_t blksScanned;
  union
  {
    int128_t bigMin;
    int64_t min;
  };
  union
  {
    int128_t bigMax;
    int64_t max;
  };
};

/** @brief class LBIDList
 *
 */
class LBIDList
{
 public:
  explicit LBIDList(const execplan::CalpontSystemCatalog::OID oid, const int debug);

  explicit LBIDList(const int debug);

  void init(const execplan::CalpontSystemCatalog::OID oid, const int debug);

  virtual ~LBIDList();

  void Dump(long Index, int Count) const;
  uint32_t GetRangeSize() const
  {
    return LBIDRanges.size() ? LBIDRanges.at(0).size : 0;
  }

  // Functions to handle min/max values per lbid for casual partitioning;
  // If pEMEntries is provided, then min/max will be extracted from that
  // vector, else extents in BRM will be searched. If type is unsigned, caller
  // should static cast returned min and max to uint64_t/int128_t
  template <typename T>
  bool GetMinMax(T& min, T& max, int64_t& seq, int64_t lbid,
                 const std::vector<struct BRM::EMEntry>* pEMEntries,
                 execplan::CalpontSystemCatalog::ColDataType type);

  template <typename T>
  bool GetMinMax(T* min, T* max, int64_t* seq, int64_t lbid,
                 const std::tr1::unordered_map<int64_t, BRM::EMEntry>& entries,
                 execplan::CalpontSystemCatalog::ColDataType type);

  template <typename T>
  void UpdateMinMax(T min, T max, int64_t lbid, const execplan::CalpontSystemCatalog::ColType& type,
                    bool validData = true);

  void UpdateAllPartitionInfo(const execplan::CalpontSystemCatalog::ColType& colType);

  bool IsRangeBoundary(uint64_t lbid);

  bool CasualPartitionPredicate(const BRM::EMCasualPartition_t& cpRange,
                                const messageqcpp::ByteStream* MsgDataPtr, const uint16_t NOPS,
                                const execplan::CalpontSystemCatalog::ColType& ct, const uint8_t BOP);

  template <typename T>
  bool checkSingleValue(T min, T max, T value, const execplan::CalpontSystemCatalog::ColType& type);

  template <typename T>
  bool checkRangeOverlap(T min, T max, T tmin, T tmax, const execplan::CalpontSystemCatalog::ColType& type);

  // check the column data type and the column size to determine if it
  // is a data type  to apply casual paritioning.
  bool CasualPartitionDataType(const execplan::CalpontSystemCatalog::ColDataType type,
                               const uint8_t size) const;

  LBIDList(const LBIDList& rhs)
  {
    copyLbidList(rhs);
  }

  LBIDList& operator=(const LBIDList& rhs)
  {
    copyLbidList(rhs);
    return *this;
  }

 private:
  LBIDList();

  void copyLbidList(const LBIDList& rhs);

  template <class T>
  inline bool compareVal(const T& Min, const T& Max, const T& value, char op, uint8_t lcf);

  template <typename T>
  int getMinMaxFromEntries(T& min, T& max, int32_t& seq, int64_t lbid,
                           const std::vector<struct BRM::EMEntry>& EMEntries);

  boost::shared_ptr<BRM::DBRM> em;
  std::vector<MinMaxPartition*> lbidPartitionVector;
  LBIDRangeVector LBIDRanges;
  int fDebug;

};  // LBIDList

}  // namespace joblist
#endif

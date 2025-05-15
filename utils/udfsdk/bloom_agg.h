/* Copyright (C) 2017 MariaDB Corporation

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
 *   $Id$
 *
 *   mcsv1_UDAF.h
 ***********************************************************************/

/**
 * Columnstore interface for writing a User Defined Aggregate
 * Functions (UDAF) and User Defined Analytic Functions (UDAnF)
 * or a function that can act as either - UDA(n)F
 *
 * The basic steps are:
 *
 * 1. Create a the UDA(n)F function interface in some .h file.
 * 2. Create the UDF function implementation in some .cpp file
 * 3. Create the connector stub (MariaDB UDAF definition) for
 * this UDF function.
 * 4. build the dynamic library using all of the source.
 * 5  Put the library in $COLUMNSTORE_INSTALL/lib of
 * all modules
 * 6. restart the Columnstore system.
 * 7. notify mysqld about the new function:
 *
 *    CREATE AGGREGATE FUNCTION ssq returns REAL soname
 *    'libudf_mysql.so';
 *
 * The UDAF function will run distributed in the Columnstore
 * engine. UDAnF do not run distributed.
 *
 * UDAF is User Defined Aggregate Function.
 * UDAnF is User Defined Analytic Function.
 * UDA(n)F is an acronym for a function that could be either. It
 * is also used to describe the interface that is used for
 * either.
 */
#pragma once

#include <cstdlib>
#include <string>
#include <vector>
#include <tr1/unordered_map>

#include "mcsv1_udaf.h"
#include "calpontsystemcatalog.h"
#include "windowfunctioncolumn.h"

#define EXPORT

namespace mcsv1sdk
{
using BloomFilter = std::vector<uint8_t>;

struct BloomAggData : public mcsv1sdk::UserData 
{
   BloomAggData() = default;
   BloomAggData(size_t hashFuncCount, size_t bloomFilterSize) : hashFuncCount(hashFuncCount), 
                                                                bloomFilterSize(bloomFilterSize) 
   {
      bloomFilter = BloomFilter(bloomFilterSize, 0);
   }

   virtual ~BloomAggData() {}

   void serialize(messageqcpp::ByteStream& stream) const override;
   void unserialize(messageqcpp::ByteStream& stream) override;

   BloomFilter bloomFilter;
   size_t hashFuncCount = 0;
   size_t bloomFilterSize = 0;

private:
   BloomAggData(UserData&) = delete;
};

class bloom_agg : public mcsv1_UDAF
{
public:

   bloom_agg() : mcsv1_UDAF(){};
   ~bloom_agg() override = default;

   ReturnCode init(mcsv1Context* context, ColumnDatum* colTypes) override;

   ReturnCode reset(mcsv1Context* context) override;

   ReturnCode nextValue(mcsv1Context* context, ColumnDatum* valsIn) override;

   ReturnCode subEvaluate(mcsv1Context* context, const UserData* userDataIn) override;

   ReturnCode evaluate(mcsv1Context* context, static_any::any& valOut) override;

   ReturnCode createUserData(UserData*& data, int32_t& length) override;

protected:
};

};  // namespace mcsv1sdk

#undef EXPORT

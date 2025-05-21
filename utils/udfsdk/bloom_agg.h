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

#pragma once

#include <cstdlib>
#include <string>
#include <vector>

#include "mcsv1_udaf.h"

#define EXPORT

namespace mcsv1sdk
{
using BloomFilter = std::vector<uint8_t>;

struct BloomAggData : public mcsv1sdk::UserData 
{
   BloomAggData() = default;
   BloomAggData(size_t hashFuncCount, size_t bloomFilterSize) : fHashFuncCount(hashFuncCount) 
   {
      bloomFilter = BloomFilter(bloomFilterSize, 0);
   }

   virtual ~BloomAggData() {}

   void serialize(messageqcpp::ByteStream& stream) const override;
   void unserialize(messageqcpp::ByteStream& stream) override;

   BloomFilter bloomFilter = BloomFilter();
   size_t fHashFuncCount = 0;

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

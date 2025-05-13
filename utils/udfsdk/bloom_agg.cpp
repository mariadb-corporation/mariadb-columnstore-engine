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

#include <sstream>
#include <cstring>
#include <typeinfo>
#include <algorithm>
#include <iostream>
#include "bloom_agg.h"
#include "bytestream.h"
#include "objectreader.h"
#include "xxhash.h"
#include "nullstring.h"

using namespace mcsv1sdk;

mcsv1_UDAF::ReturnCode bloom_agg::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() < 3)
  {
    context->setErrorMessage("bloom_agg() with less than 3 arguments");
    return mcsv1_UDAF::ERROR;
  }

  if (context->getParameterCount() > 3)
  {
    context->setErrorMessage("bloom_agg() with more than 3 arguments");
    return mcsv1_UDAF::ERROR;
  }

  context->setResultType(execplan::CalpontSystemCatalog::VARBINARY);
  context->setColWidth(256);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode bloom_agg::reset(mcsv1Context* context)
{
  BloomAggData* data = static_cast<BloomAggData*>(context->getUserData());

  if (data)
  {
    std::fill(data->bloomFilter.begin(), data->bloomFilter.end(), 0);
  }

  return mcsv1_UDAF::SUCCESS;
}

static inline void addValueToBloomFilter(const void* val, BloomAggData& data)
{
  uint64_t blockIdxHash = XXH3_64bits(val, sizeof(val)) % data.bloomFilterSize;
  
  // Currently using hard-coded prime numbers for seed values
  uint64_t valHash1 = XXH3_64bits_withSeed(&val, sizeof(val), 23);
  uint64_t valHash2 = XXH3_64bits_withSeed(&val, sizeof(val), 31);

  auto& block = data.bloomFilter[blockIdxHash];
  
  for (size_t i = 0; i < data.hashFuncCount; ++i) {
    size_t bitIdx = (valHash1 + i * valHash2) % 64;
    block |= (1 << (bitIdx % 64));
  }
}

mcsv1_UDAF::ReturnCode bloom_agg::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  BloomAggData* data = static_cast<BloomAggData*>(context->getUserData());

  if (context->isParamNull(0) || valsIn[0].columnData.empty())
  {
    return mcsv1_UDAF::SUCCESS;
  }
  
  // For now only numeric (non floating point) types are supported
  switch (valsIn[0].dataType)
  {
    case datatypes::SystemCatalog::TINYINT:
    case datatypes::SystemCatalog::SMALLINT:
    case datatypes::SystemCatalog::MEDINT:
    case datatypes::SystemCatalog::INT:
    case datatypes::SystemCatalog::BIGINT:
    case datatypes::SystemCatalog::UTINYINT:
    case datatypes::SystemCatalog::USMALLINT:
    case datatypes::SystemCatalog::UMEDINT:
    case datatypes::SystemCatalog::UINT:
    case datatypes::SystemCatalog::UBIGINT:
    {
      auto intVal = convertAnyTo<uint64_t>(valsIn[0].columnData);
      const void* val = static_cast<const void*>(&intVal);
      addValueToBloomFilter(val, *data);
      break;
    }    
  
  default:
    break;
  }

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode bloom_agg::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (context->isParamNull(0))
  {
    return mcsv1_UDAF::SUCCESS;
  }

  BloomAggData* outData = static_cast<BloomAggData*>(context->getUserData());
  const BloomAggData* inData = static_cast<const BloomAggData*>(userDataIn);

  for (size_t i = 0; i < outData->bloomFilter.size(); ++i)
  {
    outData->bloomFilter[i] |= inData->bloomFilter[i];
  }

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode bloom_agg::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  BloomAggData* data = static_cast<BloomAggData*>(context->getUserData());

  // Convert bloom filter to a string
  std::ostringstream oss;
  for (const auto& val : data->bloomFilter)
  {
    oss << val;
  }
  std::string result = oss.str();

  valOut = NullString(result);

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode bloom_agg::createUserData(UserData*& userdata, int32_t& length)
{
  userdata = new BloomAggData(3,32);
  length = sizeof(BloomAggData);
  return mcsv1_UDAF::SUCCESS;
}

// Override UserData methods
void BloomAggData::serialize(messageqcpp::ByteStream& bs) const
{
  bs << static_cast<uint64_t>(bloomFilterSize);
  bs << static_cast<uint64_t>(hashFuncCount);

  for (const auto& val : bloomFilter)
  {
    bs << val;
  }
}

void BloomAggData::unserialize(messageqcpp::ByteStream& bs)
{
  uint64_t size, hashCnt;
  bs >> size;
  bs >> hashCnt;
  bloomFilter.resize(size);
  bloomFilterSize = size;
  hashFuncCount = hashCnt;
  
  for (auto& val : bloomFilter)
  {
    bs >> val;
  }
}


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
#include <execution>
#include "bytestream.h"
#include "objectreader.h"
#include "nullstring.h"
#include "bloom_agg.h"

#include "bloom_funcs.h"
using namespace bloom_funcs;

using namespace mcsv1sdk;

// Only for debugging
#include <bitset>
#include <fstream>

static inline void logBloomFilter(auto& bloomFilter)
{
  std::ofstream log("/tmp/bloom_agglog", std::ios::app);
  
  log << "UDF Bloom filter: \n";
  for (const auto& v : bloomFilter)
  {
    log << std::bitset<8>(v);
  }
  log << "\n" << "bloom filter size: " << bloomFilter.size() << "\n";
  log << "\n\n";

}

mcsv1_UDAF::ReturnCode bloom_agg::init(mcsv1Context* context, [[maybe_unused]] ColumnDatum* colTypes)
{
  if (context->getParameterCount() != 3)
  {
    context->setErrorMessage("bloom_agg() requires 3 arguments: max_elem_count, false pos rate, column");
    return mcsv1_UDAF::ERROR;
  }

  context->setResultType(execplan::CalpontSystemCatalog::VARBINARY);
  context->setColWidth(70000);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode bloom_agg::reset(mcsv1Context* context)
{
  BloomAggData* data = static_cast<BloomAggData*>(context->getUserData());

  if (data)
  {
    std::fill(std::execution::par_unseq, data->bloomFilter.begin(), data->bloomFilter.end(), 0);
  }

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode bloom_agg::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  BloomAggData* data = static_cast<BloomAggData*>(context->getUserData());

  if (context->isParamNull(2) || valsIn[2].columnData.empty())
  {
    return mcsv1_UDAF::SUCCESS;
  }

  if (!data->isInitialized)
  {
    data->initialize(convertAnyTo<uint64_t>(valsIn[0].columnData), convertAnyTo<uint64_t>(valsIn[1].columnData));
  }

  // std::ofstream log("/tmp/bagg.log", std::ios::app);
  // log << "max el: " << convertAnyTo<uint64_t>(valsIn[0].columnData) << "\n";
  // log << "gp tsyr: " << convertAnyTo<uint64_t>(valsIn[1].columnData) << "\n";
  
  // For now only numeric (non floating point) types are supported
  switch (valsIn[2].dataType)
  {
    // abs(val) if two values are same (even tho one is negative), the hash will be the same
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
      auto intVal = convertAnyTo<uint64_t>(valsIn[2].columnData);
      addValueToBloomFilter(intVal, *data);
      break;
    }
    
    // TODO: support DECIMAL type (only with scale = 0)
    case datatypes::SystemCatalog::DECIMAL:
    case datatypes::SystemCatalog::UDECIMAL:
    {
      break;
    }

    // TODO: Support Textual types
    case datatypes::SystemCatalog::VARCHAR:
    case datatypes::SystemCatalog::VARBINARY:
    case datatypes::SystemCatalog::CLOB:
    case datatypes::SystemCatalog::BLOB:
    case datatypes::SystemCatalog::TEXT:
    {
      break;
    }
  
  default:
    context->setErrorMessage("bloom_agg() does not support this datatype.");
    return mcsv1_UDAF::ERROR;
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

  std::transform(
      std::execution::par_unseq,
      outData->bloomFilter.begin(), outData->bloomFilter.end(),
      inData->bloomFilter.begin(), outData->bloomFilter.begin(),
      std::bit_or<>()
  );

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode bloom_agg::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  BloomAggData* data = static_cast<BloomAggData*>(context->getUserData());

  valOut = utils::NullString(
      reinterpret_cast<const char*>(data->bloomFilter.data()),
      data->bloomFilter.size()
  );

  // std::string blob(data->bloomFilter.begin(), data->bloomFilter.end());
  // valOut = utils::NullString(blob);
  
  // logBloomFilter(data->bloomFilter);

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode bloom_agg::createUserData(UserData*& userdata, int32_t& length)
{
  userdata = new BloomAggData();
  length = sizeof(BloomAggData);
  return mcsv1_UDAF::SUCCESS;
}

// Override UserData methods
void BloomAggData::serialize(messageqcpp::ByteStream& bs) const
{
  bs << static_cast<uint64_t>(bloomFilter.size());
  bs << static_cast<uint64_t>(fHashFuncCount);

  for (const auto& val : bloomFilter)
  {
    bs << static_cast<uint8_t>(val);
  }
}

void BloomAggData::unserialize(messageqcpp::ByteStream& bs)
{
  uint64_t size, hashCnt;
  bs >> size;
  bs >> hashCnt;
  bloomFilter.resize(size);
  fHashFuncCount = hashCnt;
  
  for (auto& val : bloomFilter)
  {
    bs >> val;
  }
}


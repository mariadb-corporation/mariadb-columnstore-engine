#include "bloom_contains.h"
#include "xxhash.h"
#include "treenode.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "funcexp.h"
using namespace funcexp;

using namespace udfsdk;


/***************************************************************************
 * MCS_BLOOM_CONTAINS implementation
 *
 * OperationType() definition
 */
static const int defaultColWidth = 8;
CalpontSystemCatalog::ColType MCS_bloom_contains::operationType(FunctionParm& fp,
                                                     CalpontSystemCatalog::ColType& resultType)
{
  // operation type of MCS_bloom_contains is always a boolean.
  assert(fp.size() == 2);
  CalpontSystemCatalog::ColType rt;
  idbassert(fp[0]->data()->resultType().colDataType == execplan::CalpontSystemCatalog::VARBINARY);

  rt.colDataType = execplan::CalpontSystemCatalog::INT;
  rt.colWidth = defaultColWidth;

  return rt;
}

/**
 * getDoubleVal API definition
 *
 * This API is called when an double value is needed to return from the UDF function
 */
double MCS_bloom_contains::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  isNull = true;
  return 0;
}

long double MCS_bloom_contains::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& op_ct)
{
  return getDoubleVal(row, parm, isNull, op_ct);
}

float MCS_bloom_contains::getFloatVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return (float)MCS_bloom_contains::getDoubleVal(row, parm, isNull, op_ct);
}

// For testing purposes the following functionalities are not decoupled
namespace 
{
  uint64_t seed1 = 77557187;
  uint64_t seed2 = 8012791231;
}

template<typename T>
static inline bool bloomFilterContains(const T& val, const vector<uint8_t>& bloomFilter)
{
  uint64_t blockIdxHash = XXH3_64bits(reinterpret_cast<const void*>(&val), sizeof(val)) % bloomFilter.size();
  
  uint64_t valHash1 = XXH3_64bits_withSeed(reinterpret_cast<const void*>(&val), sizeof(val), seed1);
  uint64_t valHash2 = XXH3_64bits_withSeed(reinterpret_cast<const void*>(&val), sizeof(val), seed2);

  auto& block = bloomFilter[blockIdxHash];
  
    for (size_t i = 0; i < 2; ++i)
    {
        size_t bitIdx = (valHash1 + i * valHash2) % 8;
        if ((block & (1 << bitIdx)) == 0)
        {
            return false;
        }
    }

  return true;
}

/**
 * getIntVal API definition
 *
 * This API is called when an integer value is needed to return from the UDF function
 *
 * Because the result type MCS_bloom_contains is INTEGER, all the other API can simply call
 * getDoubleVal and apply the conversion. This method may not fit for all the UDF
 * implementation.
 */
int64_t MCS_bloom_contains::getIntVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
    // For some reason the isNull parameter is True
    utils::NullString bloomFilterStr = parm[0]->data()->getStrVal(row, isNull);
    if (isNull)
    {
        return 777;
    }

    vector<uint8_t> bloomFilter;
    for (const auto& c : bloomFilterStr.toString())
    {
        bloomFilter.push_back(c - '0');
    }

    [[maybe_unused]] bool result = false;
    switch(parm[1]->data()->resultType().colDataType)
    {
        case CalpontSystemCatalog::TINYINT:
        case CalpontSystemCatalog::SMALLINT:
        case CalpontSystemCatalog::MEDINT:
        case CalpontSystemCatalog::INT:
        case CalpontSystemCatalog::BIGINT:
        case CalpontSystemCatalog::UTINYINT:
        case CalpontSystemCatalog::USMALLINT:
        case CalpontSystemCatalog::UMEDINT:
        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UBIGINT:
        {
            auto val = parm[1]->data()->getIntVal();
            result = bloomFilterContains(val, bloomFilter);
        }
        default: break;
    }

    return static_cast<int64_t>(result);
    // return 999;
}

string MCS_bloom_contains::getStrVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return MCS_bloom_contains::getIntVal(row, parm, isNull, op_ct) ? "1" : "0";
}

IDB_Decimal MCS_bloom_contains::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal dec;
  dec.value = MCS_bloom_contains::getIntVal(row, parm, isNull, op_ct);
  dec.scale = 0;
  return dec;
}

/**
 * This API should never be called for MCS_bloom_contains, because the latter
 * is not for date/datetime values addition. In such case, one can
 * either not implement this API and an MCS5001 error will be thrown,
 * or throw a customized exception here.
 */
int32_t MCS_bloom_contains::getDateIntVal(Row& row, FunctionParm& parm, bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
  throw logic_error("Invalid API called for MCS_BLOOM_CONTAINS");
}

/**
 * This API should never be called for MCS_bloom_contains, because the latter
 * is not for date/datetime values addition. In such case, one can
 * either not implement this API and an MCS-5001 error will be thrown,
 * or throw a customized exception here.
 */
int64_t MCS_bloom_contains::getDatetimeIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  return (int64_t)MCS_bloom_contains::getDoubleVal(row, parm, isNull, op_ct);
}

bool MCS_bloom_contains::getBoolVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  isNull = true;
  return false;
}

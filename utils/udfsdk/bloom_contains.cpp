#include "bloom_contains.h"
#include "treenode.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "funcexp.h"
using namespace funcexp;

using namespace udfsdk;

#include "bloom_funcs.h"
using namespace bloom_funcs;


// Only for debugging
#include <bitset>
#include <fstream>

static inline void logBloomFilter(auto& bloomFilter)
{
  std::ofstream log("/tmp/bloom_udf.log", std::ios::app);
  
  log << "UDF Bloom filter: \n";
  for (const auto& v : bloomFilter)
  {
    log << std::bitset<8>(v);
  }
  log << "\n\n";

}

template<typename T>
static inline void lg(T data)
{
  std::ofstream log("/tmp/bc.log", std::ios::app);
  log << data << "\n";
}

CalpontSystemCatalog::ColType bloom_contains::operationType(FunctionParm& fp,
                                                     CalpontSystemCatalog::ColType& resultType)
{
  assert(fp.size() == 3);
  CalpontSystemCatalog::ColType rt;
  //idbassert(fp[0]->data()->resultType().colDataType == execplan::CalpontSystemCatalog::VARBINARY);

  rt.colDataType = execplan::CalpontSystemCatalog::INT;
  rt.colWidth = 8;

  return rt;
}

double bloom_contains::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  isNull = true;
  return 0;
}

long double bloom_contains::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& op_ct)
{
  return getDoubleVal(row, parm, isNull, op_ct);
}

float bloom_contains::getFloatVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return (float)bloom_contains::getDoubleVal(row, parm, isNull, op_ct);
}

int64_t bloom_contains::getIntVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
    utils::NullString bloomFilterStr = parm[0]->data()->getStrVal(row, isNull);
    if (isNull)
    {
        return 0;
    }

    vector<uint8_t> bloomFilter;
    for (const auto& c : bloomFilterStr.toString())
    {
        bloomFilter.push_back(c);
    }

    //logBloomFilter(bloomFilter);

    int64_t result = 0;
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
            result = bloomFilterContains(val, bloomFilter, parm[2]->data()->getIntVal());
        }
        default: break;
    }

    return result;
}

string bloom_contains::getStrVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return bloom_contains::getIntVal(row, parm, isNull, op_ct) ? "1" : "0";
}

IDB_Decimal bloom_contains::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal dec;
  dec.value = bloom_contains::getIntVal(row, parm, isNull, op_ct);
  dec.scale = 0;
  return dec;
}

int32_t bloom_contains::getDateIntVal(Row& row, FunctionParm& parm, bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
  throw logic_error("Invalid API called for BLOOM_CONTAINS");
}

int64_t bloom_contains::getDatetimeIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  return (int64_t)bloom_contains::getDoubleVal(row, parm, isNull, op_ct);
}

bool bloom_contains::getBoolVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return bloom_contains::getIntVal(row, parm, isNull, op_ct) > 0 ? true : false;
}

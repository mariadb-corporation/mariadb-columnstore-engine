#include "bloom_and.h"
#include "treenode.h"
using namespace execplan;

#include "rowgroup.h"
using namespace rowgroup;

#include "funcexp.h"
using namespace funcexp;

using namespace udfsdk;

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

static const int defaultColWidth = 3500000;
CalpontSystemCatalog::ColType bloom_and::operationType(FunctionParm& fp,
                                                     CalpontSystemCatalog::ColType& resultType)
{
  // operation type of MCS_bloom_and is always a long text.
  assert(fp.size() == 2);
  CalpontSystemCatalog::ColType rt;
  idbassert(fp[0]->data()->resultType().colDataType == execplan::CalpontSystemCatalog::VARBINARY);
  idbassert(fp[1]->data()->resultType().colDataType == execplan::CalpontSystemCatalog::VARBINARY);

  rt.colDataType = execplan::CalpontSystemCatalog::VARBINARY;
  rt.colWidth = defaultColWidth;

  return rt;
}

double bloom_and::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  isNull = true;
  return 0;
}

long double bloom_and::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& op_ct)
{
  return bloom_and::getDoubleVal(row, parm, isNull, op_ct);
}

float bloom_and::getFloatVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return (float)bloom_and::getDoubleVal(row, parm, isNull, op_ct);
}

int64_t bloom_and::getIntVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return (int64_t)getDoubleVal(row, parm, isNull, op_ct);
}

string bloom_and::getStrVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  string strBloomFilter1 = parm[0]->data()->getStrVal(row, isNull).toString();
  if (isNull)
  {
    return "";
  }

  string strBloomFilter2 = parm[1]->data()->getStrVal(row, isNull).toString();
  if (isNull)
  {
    return "";
  }

  if (strBloomFilter1.size() != strBloomFilter2.size())
  {
    isNull = true;
    return "";
  }

  std::string result(strBloomFilter1.size(), 0);

  for (size_t i = 0; i < strBloomFilter1.size(); ++i)
  {
    result[i] = static_cast<char>(static_cast<uint8_t>(strBloomFilter1[i]) & static_cast<uint8_t>(strBloomFilter2[i]));
  }

  //lg(result);

  return result;
}

IDB_Decimal bloom_and::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal dec;
  dec.value = bloom_and::getIntVal(row, parm, isNull, op_ct);
  dec.scale = 0;
  return dec;
}

int32_t bloom_and::getDateIntVal(Row& row, FunctionParm& parm, bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
  throw logic_error("Invalid API called for BLOOM_CONTAINS");
}

int64_t bloom_and::getDatetimeIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  return (int64_t)bloom_and::getDoubleVal(row, parm, isNull, op_ct);
}

bool bloom_and::getBoolVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  isNull = true;
  return false;
}

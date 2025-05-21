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

CalpontSystemCatalog::ColType MCS_bloom_contains::operationType(FunctionParm& fp,
                                                     CalpontSystemCatalog::ColType& resultType)
{
  //assert(fp.size() == 3);
  CalpontSystemCatalog::ColType rt;
  //idbassert(fp[0]->data()->resultType().colDataType == execplan::CalpontSystemCatalog::VARBINARY);

  rt.colDataType = execplan::CalpontSystemCatalog::INT;
  rt.colWidth = 8;

  return rt;
}

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

int64_t MCS_bloom_contains::getIntVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
    // For some reason the isNull parameter is True
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

int32_t MCS_bloom_contains::getDateIntVal(Row& row, FunctionParm& parm, bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
  throw logic_error("Invalid API called for BLOOM_CONTAINS");
}

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

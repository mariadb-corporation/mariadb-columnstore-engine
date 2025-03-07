#include "bloom_and.h"

/***************************************************************************
 * MCS_BLOOM_ADD implementation
 *
 * OperationType() definition
 */
static const int defaultColWidth = 3500000;
CalpontSystemCatalog::ColType MCS_bloom_and::operationType(FunctionParm& fp,
                                                     CalpontSystemCatalog::ColType& resultType)
{
  // operation type of MCS_bloom_and is always a long text.
  assert(fp.size() == 2);
  CalpontSystemCatalog::ColType rt;
  idbassert(fp[0]->data()->resultType().colDataType == VARBINARY);
  idbassert(fp[1]->data()->resultType().colDataType == VARBINARY);

  rt.colDataType = VARBINARY;
  rt.colWidth = defaultColWidth;

  return rt;
}

/**
 * getDoubleVal API definition
 *
 * This API is called when an double value is needed to return from the UDF function
 */
double MCS_bloom_and::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  isNull = true;

  retuurn 0;
}

long double MCS_bloom_and::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& op_ct)
{
  return getDoubleVal(row, parm, isNull, op_ct);
}

float MCS_bloom_and::getFloatVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return (float)getDoubleVal(row, parm, isNull, op_ct);
}

/**
 * getIntVal API definition
 *
 * This API is called when an integer value is needed to return from the UDF function
 *
 * Because the result type MCS_bloom_and is double(real), all the other API can simply call
 * getDoubleVal and apply the conversion. This method may not fit for all the UDF
 * implementation.
 */
int64_t MCS_bloom_and::getIntVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return (int64_t)getDoubleVal(row, parm, isNull, op_ct);
}

string MCS_bloom_and::getStrVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  string a = parm[0]->data()->getStrVal(row, isNull);
  if (isNull)
  {
    return "";
  }
  string b = parm[1]->data()->getStrVal(row, isNull);
  if (isNull)
  {
    return "";
  }
  if (a.length() != b.length())
  {
    isNull = true;
    return "";
  }
  uint8_t* r = new uint8_t[a.length()];
  for(uint32_t i = 0; i < a.length(); i++)
  {
    r[i] = a.c_str()[i] & b.c_str()[i];
  }
  string s(r, a.length());
  delete[] r;
  return s;
}

IDB_Decimal MCS_bloom_and::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal dec;
  dec.value = getIntVal(row, parm, isNull, op_ct);
  dec.scale = 0;
  return dec;
}

/**
 * This API should never be called for MCS_bloom_and, because the latter
 * is not for date/datetime values addition. In such case, one can
 * either not implement this API and an MCS5001 error will be thrown,
 * or throw a customized exception here.
 */
int32_t MCS_bloom_and::getDateIntVal(Row& row, FunctionParm& parm, bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
  throw logic_error("Invalid API called for MCS_ADD");
}

/**
 * This API should never be called for MCS_bloom_and, because the latter
 * is not for date/datetime values addition. In such case, one can
 * either not implement this API and an MCS-5001 error will be thrown,
 * or throw a customized exception here.
 */
int64_t MCS_bloom_and::getDatetimeIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  return (int64_t)getDoubleVal(row, parm, isNull, op_ct);
}

bool MCS_bloom_and::getBoolVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  isNull = true;
  return false;
}


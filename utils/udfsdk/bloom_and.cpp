#include "bloom_and.h"

/***************************************************************************
 * MCS_BLOOM_ADD implementation
 *
 * OperationType() definition
 */
CalpontSystemCatalog::ColType MCS_ibloom_and::operationType(FunctionParm& fp,
                                                     CalpontSystemCatalog::ColType& resultType)
{
  // operation type of MCS_add is always a long text.
  assert(fp.size() == 2);
  CalpontSystemCatalog::ColType rt;

  if (fp[0]->data()->resultType() == fp[1]->data()->resultType())
  {
    rt = fp[0]->data()->resultType();
  }
  else if (fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::CHAR ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::CHAR ||
           fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::VARCHAR ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::VARCHAR ||
           fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::DOUBLE ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::DOUBLE)
  {
    rt.colDataType = CalpontSystemCatalog::DOUBLE;
    rt.colWidth = 8;
  }
  else if (fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::DATE ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::DATE ||
           fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::DATETIME ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::DATETIME ||
           fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::TIME ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::TIME)
  {
    rt.colDataType = CalpontSystemCatalog::BIGINT;
    rt.colWidth = 8;
  }
  else if (fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::DECIMAL ||
           fp[0]->data()->resultType().colDataType == CalpontSystemCatalog::UDECIMAL ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::DECIMAL ||
           fp[1]->data()->resultType().colDataType == CalpontSystemCatalog::UDECIMAL)
  {
    rt.colDataType = CalpontSystemCatalog::DECIMAL;
    rt.colWidth = 8;
  }
  else
  {
    if (isUnsigned(fp[0]->data()->resultType().colDataType) ||
        isUnsigned(fp[1]->data()->resultType().colDataType))
    {
      rt.colDataType = CalpontSystemCatalog::UBIGINT;
      rt.colWidth = 8;
    }
    else
    {
      rt.colDataType = CalpontSystemCatalog::BIGINT;
      rt.colWidth = 8;
    }
  }

  return rt;
}

/**
 * getDoubleVal API definition
 *
 * This API is called when an double value is needed to return from the UDF function
 */
double MCS_add::getDoubleVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  switch (op_ct.colDataType)
  {
    // The APIs for the evaluation of the function parameters are the same getXXXval()
    // functions. However, only two arguments are passed in, the current
    // row reference and the NULL indicator isNull.
    case CalpontSystemCatalog::BIGINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::TINYINT:
      return (parm[0]->data()->getIntVal(row, isNull) + parm[1]->data()->getIntVal(row, isNull));

    case CalpontSystemCatalog::UBIGINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UTINYINT:
      return (parm[0]->data()->getUintVal(row, isNull) + parm[1]->data()->getUintVal(row, isNull));

    case CalpontSystemCatalog::DOUBLE:
      return (parm[0]->data()->getDoubleVal(row, isNull) + parm[1]->data()->getDoubleVal(row, isNull));

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      IDB_Decimal dec;
      IDB_Decimal op1 = parm[0]->data()->getDecimalVal(row, isNull);
      IDB_Decimal op2 = parm[1]->data()->getDecimalVal(row, isNull);

      if (op1.scale == op2.scale)
      {
        dec.scale = op1.scale;
      }
      else if (op1.scale >= op2.scale)
      {
        dec.scale = op2.scale;
        op1.value *= datatypes::scaleDivisor<int64_t>((uint32_t)(op1.scale - op2.scale));
      }
      else
      {
        dec.scale = op1.scale;
        op2.value *= datatypes::scaleDivisor<int64_t>((uint32_t)(op2.scale - op1.scale));
      }

      dec.value = op1.value + op2.value;
      return dec.decimal64ToXFloat<double>();
    }

    default: return (parm[0]->data()->getDoubleVal(row, isNull) + parm[1]->data()->getDoubleVal(row, isNull));
  }

  return 0;
}

long double MCS_add::getLongDoubleVal(Row& row, FunctionParm& parm, bool& isNull,
                                      CalpontSystemCatalog::ColType& op_ct)
{
  return getDoubleVal(row, parm, isNull, op_ct);
}

float MCS_add::getFloatVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return (float)getDoubleVal(row, parm, isNull, op_ct);
}

/**
 * getIntVal API definition
 *
 * This API is called when an integer value is needed to return from the UDF function
 *
 * Because the result type MCS_add is double(real), all the other API can simply call
 * getDoubleVal and apply the conversion. This method may not fit for all the UDF
 * implementation.
 */
int64_t MCS_add::getIntVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return (int64_t)getDoubleVal(row, parm, isNull, op_ct);
}

string MCS_add::getStrVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  // One will need a more efficient implementation if this API is frequently
  // called for this UDF function. This code is for demonstration purpose.
  ostringstream oss;
  oss << getDoubleVal(row, parm, isNull, op_ct);
  return oss.str();
}

IDB_Decimal MCS_add::getDecimalVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  IDB_Decimal dec;
  dec.value = getIntVal(row, parm, isNull, op_ct);
  dec.scale = 0;
  return dec;
}

/**
 * This API should never be called for MCS_add, because the latter
 * is not for date/datetime values addition. In such case, one can
 * either not implement this API and an MCS5001 error will be thrown,
 * or throw a customized exception here.
 */
int32_t MCS_add::getDateIntVal(Row& row, FunctionParm& parm, bool& isNull,
                               CalpontSystemCatalog::ColType& op_ct)
{
  throw logic_error("Invalid API called for MCS_ADD");
}

/**
 * This API should never be called for MCS_add, because the latter
 * is not for date/datetime values addition. In such case, one can
 * either not implement this API and an MCS-5001 error will be thrown,
 * or throw a customized exception here.
 */
int64_t MCS_add::getDatetimeIntVal(Row& row, FunctionParm& parm, bool& isNull,
                                   CalpontSystemCatalog::ColType& op_ct)
{
  return (int64_t)getDoubleVal(row, parm, isNull, op_ct);
}

bool MCS_add::getBoolVal(Row& row, FunctionParm& parm, bool& isNull, CalpontSystemCatalog::ColType& op_ct)
{
  return false;
}


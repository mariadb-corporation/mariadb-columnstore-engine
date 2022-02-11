/* Copyright (C) 2019 MariaDB Corporation

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
#include "moda.h"
#include "bytestream.h"
#include "objectreader.h"
#include "columnwidth.h"

using namespace mcsv1sdk;

// This is the standard way to get a UDAF function into the system's
// map of UDAF for lookup
class Add_moda_ToUDAFMap
{
 public:
  Add_moda_ToUDAFMap()
  {
    UDAFMap::getMap()["moda"] = new moda();
  }
};
static Add_moda_ToUDAFMap addToMap;

// There are a few design options when creating a generic moda function:
// 1) Always use DOUBLE for internal storage
//    Pros: can handle data from any native SQL type.
//    Cons: If MODA(SUM()) is called, then the LONG DOUBLE returned by SUM will
//          be truncated.
//          It requires 8 bytes in the hash table and requires streaming 8 bytes
//          per entry regardles of how small it could have been.
// 2) Always use LONG DOUBLE for internal storage
//    Pros: Solves the problem of MODA(SUM())
//    Cons: It requires 16 bytes in the hash table and requires streaming 16 bytes
//          per entry regardles of how small it could have been.
// 3) Use the data type of the column for internal storage
//    Pros: Can handle MODA(SUM()) because LONG DOUBLE all types are handeled
//          Only the data size needed is stored in the hash table and streamed
//
// This class implements option 3 by creating templated classes.
// There are two moda classes, the main one called moda, which is basically
// an adapter (Pattern) to the templated class called Moda_impl_T.
//
// The way the API works, each function class is instantiated exactly once per
// executable and then accessed via a map. This means that the function classes
// could be used by any active query, or more than once by a single query. These
// classes have no data fields for this reason. All data for a specific query is
// maintained by the context object.
//
// Each possible templated instantation is created ate moda creation during startup.
// They are the Moda_impl_T members at the bottom of the moda class definition.
// At runtime getImpl() gets the right one for the datatype involved based on context.
//
// More template magic is done in the ModaData class to create and maintained
// a hash of the correct type.

// getImpl returns the current modaImpl or gets the correct one based on context.
mcsv1_UDAF* moda::getImpl(mcsv1Context* context)
{
  ModaData* data = static_cast<ModaData*>(context->getUserData());
  if (data->modaImpl)
    return data->modaImpl;

  switch (context->getResultType())
  {
    case execplan::CalpontSystemCatalog::TINYINT: data->modaImpl = &moda_impl_int8; break;
    case execplan::CalpontSystemCatalog::SMALLINT: data->modaImpl = &moda_impl_int16; break;
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::INT: data->modaImpl = &moda_impl_int32; break;
    case execplan::CalpontSystemCatalog::BIGINT: data->modaImpl = &moda_impl_int64; break;
    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
      switch (context->getColWidth())
      {
        case 1: data->modaImpl = &moda_impl_int8; break;
        case 2: data->modaImpl = &moda_impl_int16; break;
        case 4: data->modaImpl = &moda_impl_int32; break;
        case 8: data->modaImpl = &moda_impl_int64; break;
        case 16: data->modaImpl = &moda_impl_int128; break;
      }
      break;
    case execplan::CalpontSystemCatalog::UTINYINT: data->modaImpl = &moda_impl_uint8; break;
    case execplan::CalpontSystemCatalog::USMALLINT: data->modaImpl = &moda_impl_uint16; break;
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UINT: data->modaImpl = &moda_impl_uint32; break;
    case execplan::CalpontSystemCatalog::UBIGINT: data->modaImpl = &moda_impl_uint64; break;
    case execplan::CalpontSystemCatalog::FLOAT: data->modaImpl = &moda_impl_float; break;
    case execplan::CalpontSystemCatalog::DOUBLE: data->modaImpl = &moda_impl_double; break;
    case execplan::CalpontSystemCatalog::LONGDOUBLE: data->modaImpl = &moda_impl_longdouble; break;
    default: data->modaImpl = NULL;
  }
  return data->modaImpl;
}

mcsv1_UDAF::ReturnCode moda::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() < 1)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("moda() with 0 arguments");
    return mcsv1_UDAF::ERROR;
  }

  if (context->getParameterCount() > 1)
  {
    context->setErrorMessage("moda() with more than 1 argument");
    return mcsv1_UDAF::ERROR;
  }

  if (!(datatypes::isNumeric(colTypes[0].dataType)))
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("moda() with non-numeric argument");
    return mcsv1_UDAF::ERROR;
  }

  context->setResultType(colTypes[0].dataType);

  if (colTypes[0].dataType == execplan::CalpontSystemCatalog::DECIMAL ||
      colTypes[0].dataType == execplan::CalpontSystemCatalog::UDECIMAL)
  {
    if (colTypes[0].precision < 3)
    {
      context->setColWidth(1);
    }
    else if (colTypes[0].precision < 4)
    {
      context->setColWidth(2);
    }
    else if (colTypes[0].precision < 9)
    {
      context->setColWidth(4);
    }
    else if (colTypes[0].precision < 19)
    {
      context->setColWidth(8);
    }
    else if (utils::widthByPrecision(colTypes[0].precision))
    {
      context->setColWidth(16);
    }

    context->setScale(colTypes[0].scale);
  }
  context->setPrecision(colTypes[0].precision);

  mcsv1_UDAF* impl = getImpl(context);

  if (!impl)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("moda() with non-numeric argument");
    return mcsv1_UDAF::ERROR;
  }

  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return impl->init(context, colTypes);
}

template <class T>
mcsv1_UDAF::ReturnCode Moda_impl_T<T>::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  return mcsv1_UDAF::SUCCESS;
}

template <class T>
mcsv1_UDAF::ReturnCode Moda_impl_T<T>::reset(mcsv1Context* context)
{
  ModaData* data = static_cast<ModaData*>(context->getUserData());
  data->fReturnType = context->getResultType();
  data->fColWidth = context->getColWidth();
  data->clear<T>();
  return mcsv1_UDAF::SUCCESS;
}

template <class T>
mcsv1_UDAF::ReturnCode Moda_impl_T<T>::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  static_any::any& valIn = valsIn[0].columnData;
  ModaData* data = static_cast<ModaData*>(context->getUserData());
  std::unordered_map<T, uint32_t, hasher<T> >* map = data->getMap<T>();

  if (valIn.empty())
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }

  T val = convertAnyTo<T>(valIn);

  if (context->getResultType() == execplan::CalpontSystemCatalog::DOUBLE)
  {
    // For decimal types, we need to move the decimal point.
    uint32_t scale = valsIn[0].scale;

    if (val != 0 && scale > 0)
    {
      val /= datatypes::scaleDivisor<double>(scale);
    }
  }

  data->fSum += val;
  ++data->fCount;
  (*map)[val]++;

  return mcsv1_UDAF::SUCCESS;
}

template <class T>
mcsv1_UDAF::ReturnCode Moda_impl_T<T>::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (!userDataIn)
  {
    return mcsv1_UDAF::SUCCESS;
  }

  ModaData* outData = static_cast<ModaData*>(context->getUserData());
  const ModaData* inData = static_cast<const ModaData*>(userDataIn);
  std::unordered_map<T, uint32_t, hasher<T> >* outMap = outData->getMap<T>();
  std::unordered_map<T, uint32_t, hasher<T> >* inMap = inData->getMap<T>();
  typename std::unordered_map<T, uint32_t, hasher<T> >::const_iterator iter;

  for (iter = inMap->begin(); iter != inMap->end(); ++iter)
  {
    (*outMap)[iter->first] += iter->second;
  }
  // AVG
  outData->fSum += inData->fSum;
  outData->fCount += inData->fCount;

  return mcsv1_UDAF::SUCCESS;
}

template <class T>
mcsv1_UDAF::ReturnCode Moda_impl_T<T>::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  uint64_t maxCnt = 0;
  long double avg = 0;
  T val = 0;
  ModaData* data = static_cast<ModaData*>(context->getUserData());
  std::unordered_map<T, uint32_t, hasher<T> >* map = data->getMap<T>();

  if (map->size() == 0)
  {
    valOut = (T)0;
    return mcsv1_UDAF::SUCCESS;
  }

  avg = data->fCount ? data->fSum / data->fCount : 0;
  typename std::unordered_map<T, uint32_t, hasher<T> >::iterator iter;

  for (iter = map->begin(); iter != map->end(); ++iter)
  {
    if (iter->second > maxCnt)
    {
      val = iter->first;
      maxCnt = iter->second;
    }
    else if (iter->second == maxCnt)
    {
      T absval = val >= 0 ? val : -val;
      T absfirst = iter->first >= 0 ? iter->first : -iter->first;
      // Tie breaker: choose the closest to avg. If still tie, choose smallest
      long double dist1 = val > avg ? (long double)val - avg : avg - (long double)val;
      long double dist2 = iter->first > avg ? (long double)iter->first - avg : avg - (long double)iter->first;
      if ((dist1 > dist2) || ((dist1 == dist2) && (absval > absfirst)))
      {
        val = iter->first;
      }
    }
  }

  // If scale is > 0, then the original type was DECIMAL. Set the
  // ResultType to DECIMAL so the delivery logic moves the decimal point.
  if (context->getScale() > 0)
    context->setResultType(execplan::CalpontSystemCatalog::DECIMAL);

  valOut = val;
  return mcsv1_UDAF::SUCCESS;
}

template <class T>
mcsv1_UDAF::ReturnCode Moda_impl_T<T>::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  static_any::any& valDropped = valsDropped[0].columnData;
  ModaData* data = static_cast<ModaData*>(context->getUserData());
  std::unordered_map<T, uint32_t, hasher<T> >* map = data->getMap<T>();

  if (valDropped.empty())
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }

  T val = convertAnyTo<T>(valDropped);

  data->fSum -= val;
  --data->fCount;
  (*map)[val]--;

  return mcsv1_UDAF::SUCCESS;
}

void ModaData::serialize(messageqcpp::ByteStream& bs) const
{
  bs << fReturnType;
  bs << fSum;
  bs << fCount;
  bs << fColWidth;

  switch ((execplan::CalpontSystemCatalog::ColDataType)fReturnType)
  {
    case execplan::CalpontSystemCatalog::TINYINT: serializeMap<int8_t>(bs); break;
    case execplan::CalpontSystemCatalog::SMALLINT: serializeMap<int16_t>(bs); break;
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::INT: serializeMap<int32_t>(bs); break;
    case execplan::CalpontSystemCatalog::BIGINT: serializeMap<int64_t>(bs); break;
    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
      switch (fColWidth)
      {
        case 1: serializeMap<int8_t>(bs); break;
        case 2: serializeMap<int16_t>(bs); break;
        case 4: serializeMap<int32_t>(bs); break;
        case 8: serializeMap<int64_t>(bs); break;
        case 16: serializeMap<int128_t>(bs); break;
      }
      break;
    case execplan::CalpontSystemCatalog::UTINYINT: serializeMap<uint8_t>(bs); break;
    case execplan::CalpontSystemCatalog::USMALLINT: serializeMap<uint16_t>(bs); break;
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UINT: serializeMap<uint32_t>(bs); break;
    case execplan::CalpontSystemCatalog::UBIGINT: serializeMap<uint64_t>(bs); break;
    case execplan::CalpontSystemCatalog::FLOAT: serializeMap<float>(bs); break;
    case execplan::CalpontSystemCatalog::DOUBLE: serializeMap<double>(bs); break;
    case execplan::CalpontSystemCatalog::LONGDOUBLE: serializeMap<long double>(bs); break;
    default: throw std::runtime_error("ModaData::serialize with bad data type"); break;
  }
}

void ModaData::unserialize(messageqcpp::ByteStream& bs)
{
  bs >> fReturnType;
  bs >> fSum;
  bs >> fCount;
  bs >> fColWidth;

  switch ((execplan::CalpontSystemCatalog::ColDataType)fReturnType)
  {
    case execplan::CalpontSystemCatalog::TINYINT: unserializeMap<int8_t>(bs); break;
    case execplan::CalpontSystemCatalog::SMALLINT: unserializeMap<int16_t>(bs); break;
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::INT: unserializeMap<int32_t>(bs); break;
    case execplan::CalpontSystemCatalog::BIGINT: unserializeMap<int64_t>(bs); break;
    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
      switch (fColWidth)
      {
        case 1: unserializeMap<int8_t>(bs); break;
        case 2: unserializeMap<int16_t>(bs); break;
        case 4: unserializeMap<int32_t>(bs); break;
        case 8: unserializeMap<int64_t>(bs); break;
        case 16: unserializeMap<int128_t>(bs); break;
      }
      break;
    case execplan::CalpontSystemCatalog::UTINYINT: unserializeMap<uint8_t>(bs); break;
    case execplan::CalpontSystemCatalog::USMALLINT: unserializeMap<uint16_t>(bs); break;
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UINT: unserializeMap<uint32_t>(bs); break;
    case execplan::CalpontSystemCatalog::UBIGINT: unserializeMap<uint64_t>(bs); break;
    case execplan::CalpontSystemCatalog::FLOAT: unserializeMap<float>(bs); break;
    case execplan::CalpontSystemCatalog::DOUBLE: unserializeMap<double>(bs); break;
    case execplan::CalpontSystemCatalog::LONGDOUBLE: unserializeMap<long double>(bs); break;
    default: throw std::runtime_error("ModaData::unserialize with bad data type"); break;
  }
}

void ModaData::cleanup()
{
  if (!fMap)
    return;
  switch ((execplan::CalpontSystemCatalog::ColDataType)fReturnType)
  {
    case execplan::CalpontSystemCatalog::TINYINT:
      clear<int8_t>();
      deleteMap<int8_t>();
      break;
    case execplan::CalpontSystemCatalog::SMALLINT:
      clear<int16_t>();
      deleteMap<int16_t>();
      break;
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::INT:
      clear<int32_t>();
      deleteMap<int32_t>();
      break;
    case execplan::CalpontSystemCatalog::BIGINT:
      clear<int64_t>();
      deleteMap<int64_t>();
      break;
    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
      switch (fColWidth)
      {
        case 1:
          clear<int8_t>();
          deleteMap<int8_t>();
          break;
        case 2:
          clear<int16_t>();
          deleteMap<int16_t>();
          break;
        case 4:
          clear<int32_t>();
          deleteMap<int32_t>();
          break;
        case 8:
          clear<int64_t>();
          deleteMap<int64_t>();
          break;
        case 16:
          clear<int128_t>();
          deleteMap<int128_t>();
          break;
      }
      break;
    case execplan::CalpontSystemCatalog::UTINYINT:
      clear<uint8_t>();
      deleteMap<uint8_t>();
      break;
    case execplan::CalpontSystemCatalog::USMALLINT:
      clear<uint16_t>();
      deleteMap<uint16_t>();
      break;
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UINT:
      clear<uint32_t>();
      deleteMap<uint32_t>();
      break;
    case execplan::CalpontSystemCatalog::UBIGINT:
      clear<uint64_t>();
      deleteMap<uint64_t>();
      break;
    case execplan::CalpontSystemCatalog::FLOAT:
      clear<float>();
      deleteMap<float>();
      break;
    case execplan::CalpontSystemCatalog::DOUBLE:
      clear<double>();
      deleteMap<double>();
      break;
    case execplan::CalpontSystemCatalog::LONGDOUBLE:
      clear<long double>();
      deleteMap<long double>();
      break;
    default: throw std::runtime_error("ModaData::unserialize with bad data type"); break;
  }
}

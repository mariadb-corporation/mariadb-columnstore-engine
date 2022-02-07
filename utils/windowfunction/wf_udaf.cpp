/************************************************************************************
  Copyright (c) 2019 MariaDB Corporation

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Library General Public
  License as published by the Free Software Foundation; either
  version 2 of the License, or (at your option) any later version.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Library General Public License for more details.

  You should have received a copy of the GNU Library General Public
  License along with this library; if not see <http://www.gnu.org/licenses>
  or write to the Free Software Foundation, Inc.,
  51 Franklin St., Fifth Floor, Boston, MA 02110, USA
 *************************************************************************************/

//#define NDEBUG
#include <iostream>
#include <cassert>
#include <cmath>
#include <sstream>
#include <iomanip>
using namespace std;

#include <boost/shared_ptr.hpp>
using namespace boost;

#include "loggingid.h"
#include "errorcodes.h"
#include "idberrorinfo.h"
using namespace logging;

#include "rowgroup.h"
using namespace rowgroup;

#include "idborderby.h"
using namespace ordering;

#include "joblisttypes.h"
#include "calpontsystemcatalog.h"
#include "constantcolumn.h"
using namespace execplan;

#include "windowfunctionstep.h"
using namespace joblist;

#include "wf_udaf.h"

#include "vlarray.h"

namespace windowfunction
{
boost::shared_ptr<WindowFunctionType> WF_udaf::makeFunction(int id, const string& name, int ct,
                                                            mcsv1sdk::mcsv1Context& context,
                                                            WindowFunctionColumn* wc)
{
  boost::shared_ptr<WindowFunctionType> func;

  func.reset(new WF_udaf(id, name, context));

  // Get the UDAnF function object
  WF_udaf* wfUDAF = (WF_udaf*)func.get();
  mcsv1sdk::mcsv1Context& udafContext = wfUDAF->getContext();
  udafContext.setInterrupted(wfUDAF->getInterruptedPtr());
  wfUDAF->resetData();
  return func;
}

WF_udaf::WF_udaf(WF_udaf& rhs)
 : WindowFunctionType(rhs.functionId(), rhs.functionName())
 , fUDAFContext(rhs.getContext())
 , bInterrupted(rhs.getInterrupted())
 , fDistinct(rhs.getDistinct())
{
  getContext().setInterrupted(getInterruptedPtr());
}

WindowFunctionType* WF_udaf::clone() const
{
  return new WF_udaf(*const_cast<WF_udaf*>(this));
}

void WF_udaf::resetData()
{
  getContext().getFunction()->reset(&getContext());
  fDistinctMap.clear();
  WindowFunctionType::resetData();
}

void WF_udaf::parseParms(const std::vector<execplan::SRCP>& parms)
{
  bRespectNulls = true;
  // The last parms: respect null | ignore null
  ConstantColumn* cc = dynamic_cast<ConstantColumn*>(parms[parms.size() - 1].get());
  idbassert(cc != NULL);
  bool isNull = false;  // dummy, harded coded
  bRespectNulls = (cc->getIntVal(fRow, isNull) > 0);
  if (getContext().getRunFlag(mcsv1sdk::UDAF_DISTINCT))
  {
    setDistinct();
  }
}

bool WF_udaf::dropValues(int64_t b, int64_t e)
{
  if (!bHasDropValue)
  {
    // Save work if we discovered dropValue is not implemented in the UDAnF
    return false;
  }

  mcsv1sdk::mcsv1_UDAF::ReturnCode rc;
  bool isNull = false;

  // Turn on the Analytic flag so the function is aware it is being called
  // as a Window Function.
  getContext().setContextFlag(mcsv1sdk::CONTEXT_IS_ANALYTIC);

  // Put the parameter metadata (type, scale, precision) into valsIn
  utils::VLArray<mcsv1sdk::ColumnDatum> valsIn(getContext().getParameterCount());
  ConstantColumn* cc = NULL;

  for (uint32_t i = 0; i < getContext().getParameterCount(); ++i)
  {
    mcsv1sdk::ColumnDatum& datum = valsIn[i];
    cc = static_cast<ConstantColumn*>(fConstantParms[i].get());

    if (cc)
    {
      datum.dataType = cc->resultType().colDataType;
      datum.scale = cc->resultType().scale;
      datum.precision = cc->resultType().precision;
    }
    else
    {
      uint64_t colIn = fFieldIndex[i + 1];
      datum.dataType = fRow.getColType(colIn);
      datum.scale = fRow.getScale(colIn);
      datum.precision = fRow.getPrecision(colIn);
    }
  }

  utils::VLArray<uint32_t> flags(getContext().getParameterCount());
  for (int64_t i = b; i < e; i++)
  {
    if (i % 1000 == 0 && fStep->cancelled())
      break;

    fRow.setData(getPointer(fRowData->at(i)));

    // NULL flags
    bool bSkipIt = false;

    for (uint32_t k = 0; k < getContext().getParameterCount(); ++k)
    {
      cc = static_cast<ConstantColumn*>(fConstantParms[k].get());
      uint64_t colIn = fFieldIndex[k + 1];
      mcsv1sdk::ColumnDatum& datum = valsIn[k];

      // Turn on Null flags or skip based on respect nulls
      flags[k] = 0;
      if ((!cc && fRow.isNullValue(colIn) == true) || (cc && cc->type() == ConstantColumn::NULLDATA))
      {
        if (!bRespectNulls)
        {
          bSkipIt = true;
          break;
        }

        flags[k] |= mcsv1sdk::PARAM_IS_NULL;
      }

      if (!bSkipIt && !(flags[k] & mcsv1sdk::PARAM_IS_NULL))
      {
        switch (datum.dataType)
        {
          case CalpontSystemCatalog::TINYINT:
          case CalpontSystemCatalog::SMALLINT:
          case CalpontSystemCatalog::MEDINT:
          case CalpontSystemCatalog::INT:
          case CalpontSystemCatalog::BIGINT:
          {
            int64_t valIn;

            if (cc)
            {
              valIn = cc->getIntVal(fRow, isNull);
            }
            else
            {
              getValue(colIn, valIn);
            }

            // Check for distinct, if turned on.
            // Currently, distinct only works on the first parameter.
            if (k == 0)
            {
              if (fDistinct)
              {
                DistinctMap::iterator distinct;
                distinct = fDistinctMap.find(valIn);
                if (distinct != fDistinctMap.end())
                {
                  // This is a duplicate: decrement the count
                  --(*distinct).second;
                  if ((*distinct).second > 0)  // still more of these
                  {
                    bSkipIt = true;
                    continue;
                  }
                  else
                  {
                    fDistinctMap.erase(distinct);
                  }
                }
              }
            }

            datum.columnData = valIn;
            break;
          }

          case CalpontSystemCatalog::DECIMAL:
          case CalpontSystemCatalog::UDECIMAL:
          {
            int64_t valIn;

            if (cc)
            {
              valIn = cc->getDecimalVal(fRow, isNull).value;
            }
            else
            {
              getValue(colIn, valIn);
            }

            // Check for distinct, if turned on.
            // Currently, distinct only works on the first parameter.
            if (k == 0)
            {
              if (fDistinct)
              {
                DistinctMap::iterator distinct;
                distinct = fDistinctMap.find(valIn);
                if (distinct != fDistinctMap.end())
                {
                  // This is a duplicate: decrement the count
                  --(*distinct).second;
                  if ((*distinct).second > 0)  // still more of these
                  {
                    bSkipIt = true;
                    continue;
                  }
                  else
                  {
                    fDistinctMap.erase(distinct);
                  }
                }
              }
            }

            datum.columnData = valIn;
            break;
          }

          case CalpontSystemCatalog::UTINYINT:
          case CalpontSystemCatalog::USMALLINT:
          case CalpontSystemCatalog::UMEDINT:
          case CalpontSystemCatalog::UINT:
          case CalpontSystemCatalog::UBIGINT:
          case CalpontSystemCatalog::TIME:
          case CalpontSystemCatalog::DATE:
          case CalpontSystemCatalog::DATETIME:
          case CalpontSystemCatalog::TIMESTAMP:
          {
            uint64_t valIn;

            if (cc)
            {
              valIn = cc->getUintVal(fRow, isNull);
            }
            else
            {
              getValue(colIn, valIn);
            }

            // Check for distinct, if turned on.
            // Currently, distinct only works on the first parameter.
            if (k == 0)
            {
              if (fDistinct)
              {
                DistinctMap::iterator distinct;
                distinct = fDistinctMap.find(valIn);
                if (distinct != fDistinctMap.end())
                {
                  // This is a duplicate: decrement the count
                  --(*distinct).second;
                  if ((*distinct).second > 0)  // still more of these
                  {
                    bSkipIt = true;
                    continue;
                  }
                  else
                  {
                    fDistinctMap.erase(distinct);
                  }
                }
              }
            }

            datum.columnData = valIn;
            break;
          }

          case CalpontSystemCatalog::DOUBLE:
          case CalpontSystemCatalog::UDOUBLE:
          {
            double valIn;

            if (cc)
            {
              valIn = cc->getDoubleVal(fRow, isNull);
            }
            else
            {
              getValue(colIn, valIn);
            }

            // Check for distinct, if turned on.
            // Currently, distinct only works on the first parameter.
            if (k == 0)
            {
              if (fDistinct)
              {
                DistinctMap::iterator distinct;
                distinct = fDistinctMap.find(valIn);
                if (distinct != fDistinctMap.end())
                {
                  // This is a duplicate: decrement the count
                  --(*distinct).second;
                  if ((*distinct).second > 0)  // still more of these
                  {
                    bSkipIt = true;
                    continue;
                  }
                  else
                  {
                    fDistinctMap.erase(distinct);
                  }
                }
              }
            }

            datum.columnData = valIn;
            break;
          }

          case CalpontSystemCatalog::FLOAT:
          case CalpontSystemCatalog::UFLOAT:
          {
            float valIn;

            if (cc)
            {
              valIn = cc->getFloatVal(fRow, isNull);
            }
            else
            {
              getValue(colIn, valIn);
            }

            // Check for distinct, if turned on.
            // Currently, distinct only works on the first parameter.
            if (k == 0)
            {
              if (fDistinct)
              {
                DistinctMap::iterator distinct;
                distinct = fDistinctMap.find(valIn);
                if (distinct != fDistinctMap.end())
                {
                  // This is a duplicate: decrement the count
                  --(*distinct).second;
                  if ((*distinct).second > 0)  // still more of these
                  {
                    bSkipIt = true;
                    continue;
                  }
                  else
                  {
                    fDistinctMap.erase(distinct);
                  }
                }
              }
            }

            datum.columnData = valIn;
            break;
          }

          case CalpontSystemCatalog::LONGDOUBLE:
          {
            double valIn;

            if (cc)
            {
              valIn = cc->getLongDoubleVal(fRow, isNull);
            }
            else
            {
              getValue(colIn, valIn);
            }

            // Check for distinct, if turned on.
            // Currently, distinct only works on the first parameter.
            if (k == 0)
            {
              if (fDistinct)
              {
                DistinctMap::iterator distinct;
                distinct = fDistinctMap.find(valIn);
                if (distinct != fDistinctMap.end())
                {
                  // This is a duplicate: decrement the count
                  --(*distinct).second;
                  if ((*distinct).second > 0)  // still more of these
                  {
                    bSkipIt = true;
                    continue;
                  }
                  else
                  {
                    fDistinctMap.erase(distinct);
                  }
                }
              }
            }

            datum.columnData = valIn;
            break;
          }

          case CalpontSystemCatalog::CHAR:
          case CalpontSystemCatalog::VARCHAR:
          case CalpontSystemCatalog::VARBINARY:
          case CalpontSystemCatalog::TEXT:
          case CalpontSystemCatalog::BLOB:
          {
            string valIn;

            if (cc)
            {
              valIn = cc->getStrVal(fRow, isNull);
            }
            else
            {
              getValue(colIn, valIn);
            }

            // Check for distinct, if turned on.
            // Currently, distinct only works on the first parameter.
            if (k == 0)
            {
              if (fDistinct)
              {
                DistinctMap::iterator distinct;
                distinct = fDistinctMap.find(valIn);
                if (distinct != fDistinctMap.end())
                {
                  // This is a duplicate: decrement the count
                  --(*distinct).second;
                  if ((*distinct).second > 0)  // still more of these
                  {
                    bSkipIt = true;
                    continue;
                  }
                  else
                  {
                    fDistinctMap.erase(distinct);
                  }
                }
              }
            }

            datum.columnData = valIn;
            break;
          }

          default:
          {
            string errStr = "(" + colType2String[(int)datum.dataType] + ")";
            errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_INVALID_PARM_TYPE, errStr);
            cerr << errStr << endl;
            throw IDBExcept(errStr, ERR_WF_INVALID_PARM_TYPE);

            break;
          }
        }
      }
    }

    if (bSkipIt)
    {
      continue;
    }

    getContext().setDataFlags(flags);

    rc = getContext().getFunction()->dropValue(&getContext(), valsIn);

    if (rc == mcsv1sdk::mcsv1_UDAF::NOT_IMPLEMENTED)
    {
      bHasDropValue = false;
      return false;
    }

    if (rc == mcsv1sdk::mcsv1_UDAF::ERROR)
    {
      bInterrupted = true;
      string errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_UDANF_ERROR, getContext().getErrorMessage());
      cerr << errStr << endl;
      throw IDBExcept(errStr, ERR_WF_UDANF_ERROR);
    }
  }

  WindowFunctionType::resetData();
  return true;
}

// Sets the value from valOut into column colOut, performing any conversions.
void WF_udaf::SetUDAFValue(static_any::any& valOut, int64_t colOut, int64_t b, int64_t e, int64_t c)
{
  static const static_any::any& charTypeId = (char)1;
  static const static_any::any& scharTypeId = (signed char)1;
  static const static_any::any& shortTypeId = (short)1;
  static const static_any::any& intTypeId = (int)1;
  static const static_any::any& longTypeId = (long)1;
  static const static_any::any& llTypeId = (long long)1;
  static const static_any::any& int128TypeId = (int128_t)1;
  static const static_any::any& ucharTypeId = (unsigned char)1;
  static const static_any::any& ushortTypeId = (unsigned short)1;
  static const static_any::any& uintTypeId = (unsigned int)1;
  static const static_any::any& ulongTypeId = (unsigned long)1;
  static const static_any::any& ullTypeId = (unsigned long long)1;
  static const static_any::any& floatTypeId = (float)1;
  static const static_any::any& doubleTypeId = (double)1;
  static const std::string typeStr("");
  static const static_any::any& strTypeId = typeStr;

  CDT colDataType = fRow.getColType(colOut);

  // This may seem a bit convoluted. Users shouldn't return a type
  // that they didn't set in mcsv1_UDAF::init(), but this
  // handles whatever return type is given and casts
  // it to whatever they said to return.
  int64_t intOut = 0;
  uint64_t uintOut = 0;
  int128_t int128Out = 0;
  float floatOut = 0.0;
  double doubleOut = 0.0;
  long double longdoubleOut = 0.0;
  ostringstream oss;
  std::string strOut;

  if (valOut.compatible(charTypeId))
  {
    uintOut = intOut = valOut.cast<char>();
    floatOut = intOut;
    oss << intOut;
  }
  else if (valOut.compatible(scharTypeId))
  {
    uintOut = intOut = valOut.cast<signed char>();
    floatOut = intOut;
    oss << intOut;
  }
  else if (valOut.compatible(shortTypeId))
  {
    uintOut = intOut = valOut.cast<short>();
    floatOut = intOut;
    oss << intOut;
  }
  else if (valOut.compatible(intTypeId))
  {
    uintOut = intOut = valOut.cast<int>();
    floatOut = intOut;
    oss << intOut;
  }
  else if (valOut.compatible(longTypeId))
  {
    uintOut = intOut = valOut.cast<long>();
    floatOut = intOut;
    oss << intOut;
  }
  else if (valOut.compatible(llTypeId))
  {
    uintOut = intOut = valOut.cast<long long>();
    floatOut = intOut;
    oss << intOut;
  }
  else if (valOut.compatible(ucharTypeId))
  {
    intOut = uintOut = valOut.cast<unsigned char>();
    floatOut = uintOut;
    oss << uintOut;
  }
  else if (valOut.compatible(ushortTypeId))
  {
    intOut = uintOut = valOut.cast<unsigned short>();
    floatOut = uintOut;
    oss << uintOut;
  }
  else if (valOut.compatible(uintTypeId))
  {
    intOut = uintOut = valOut.cast<unsigned int>();
    floatOut = uintOut;
    oss << uintOut;
  }
  else if (valOut.compatible(ulongTypeId))
  {
    intOut = uintOut = valOut.cast<unsigned long>();
    floatOut = uintOut;
    oss << uintOut;
  }
  else if (valOut.compatible(ullTypeId))
  {
    intOut = uintOut = valOut.cast<unsigned long long>();
    floatOut = uintOut;
    oss << uintOut;
  }
  else if (valOut.compatible(floatTypeId))
  {
    floatOut = valOut.cast<float>();
    doubleOut = floatOut;
    longdoubleOut = floatOut;
    intOut = uintOut = floatOut;
    oss << floatOut;
  }
  else if (valOut.compatible(doubleTypeId))
  {
    doubleOut = valOut.cast<double>();
    longdoubleOut = doubleOut;
    floatOut = (float)doubleOut;
    uintOut = (uint64_t)doubleOut;
    intOut = (int64_t)doubleOut;
    oss << doubleOut;
  }
  else if (valOut.compatible(int128TypeId))
  {
    int128Out = valOut.cast<int128_t>();
    uintOut = intOut = int128Out;  // may truncate
    floatOut = int128Out;
    doubleOut = int128Out;
    longdoubleOut = int128Out;
    oss << longdoubleOut;
  }

  if (valOut.compatible(strTypeId))
  {
    std::string strOut = valOut.cast<std::string>();
    // Convert the string to numeric type, just in case.
    intOut = atol(strOut.c_str());
    uintOut = strtoul(strOut.c_str(), NULL, 10);
    doubleOut = strtod(strOut.c_str(), NULL);
    longdoubleOut = doubleOut;
    floatOut = (float)doubleOut;
  }
  else
  {
    strOut = oss.str();
  }

  switch (colDataType)
  {
    case execplan::CalpontSystemCatalog::BIT:
    case execplan::CalpontSystemCatalog::TINYINT:
    case execplan::CalpontSystemCatalog::SMALLINT:
    case execplan::CalpontSystemCatalog::MEDINT:
    case execplan::CalpontSystemCatalog::INT:
    case execplan::CalpontSystemCatalog::BIGINT:
    case execplan::CalpontSystemCatalog::DECIMAL:
    case execplan::CalpontSystemCatalog::UDECIMAL:
      if (valOut.empty())
      {
        setValue(colDataType, b, e, c, (int64_t*)NULL);
      }
      else
      {
        setValue(colDataType, b, e, c, &intOut);
      }
      break;

    case execplan::CalpontSystemCatalog::UTINYINT:
    case execplan::CalpontSystemCatalog::USMALLINT:
    case execplan::CalpontSystemCatalog::UMEDINT:
    case execplan::CalpontSystemCatalog::UINT:
    case execplan::CalpontSystemCatalog::UBIGINT:
    case execplan::CalpontSystemCatalog::DATE:
    case execplan::CalpontSystemCatalog::DATETIME:
    case execplan::CalpontSystemCatalog::TIMESTAMP:
    case execplan::CalpontSystemCatalog::TIME:
      if (valOut.empty())
      {
        setValue(colDataType, b, e, c, (uint64_t*)NULL);
      }
      else
      {
        setValue(colDataType, b, e, c, &uintOut);
      }
      break;

    case execplan::CalpontSystemCatalog::FLOAT:
    case execplan::CalpontSystemCatalog::UFLOAT:
      if (valOut.empty())
      {
        setValue(colDataType, b, e, c, (float*)NULL);
      }
      else
      {
        setValue(colDataType, b, e, c, &floatOut);
      }
      break;

    case execplan::CalpontSystemCatalog::DOUBLE:
    case execplan::CalpontSystemCatalog::UDOUBLE:
      if (valOut.empty())
      {
        setValue(colDataType, b, e, c, (double*)NULL);
      }
      else
      {
        setValue(colDataType, b, e, c, &doubleOut);
      }
      break;

    case execplan::CalpontSystemCatalog::LONGDOUBLE:
      if (valOut.empty())
      {
        setValue(colDataType, b, e, c, (long double*)NULL);
      }
      else
      {
        setValue(colDataType, b, e, c, &longdoubleOut);
      }
      break;

    case execplan::CalpontSystemCatalog::CHAR:
    case execplan::CalpontSystemCatalog::VARCHAR:
    case execplan::CalpontSystemCatalog::TEXT:
    case execplan::CalpontSystemCatalog::VARBINARY:
    case execplan::CalpontSystemCatalog::CLOB:
    case execplan::CalpontSystemCatalog::BLOB:
      if (valOut.empty())
      {
        setValue(colDataType, b, e, c, (string*)NULL);
      }
      else
      {
        setValue(colDataType, b, e, c, &strOut);
      }
      break;

    default:
    {
      std::ostringstream errmsg;
      errmsg << "WF_udaf: No logic for data type: " << colDataType;
      cerr << errmsg.str() << endl;
      throw runtime_error(errmsg.str().c_str());
      break;
    }
  }
}

void WF_udaf::operator()(int64_t b, int64_t e, int64_t c)
{
  mcsv1sdk::mcsv1_UDAF::ReturnCode rc;
  uint64_t colOut = fFieldIndex[0];
  bool isNull = false;

  if ((fFrameUnit == WF__FRAME_ROWS) || (fPrev == -1) ||
      (!fPeer->operator()(getPointer(fRowData->at(c)), getPointer(fRowData->at(fPrev)))))
  {
    fValOut.reset();
    // for unbounded - current row special handling
    if (fPrev >= b && fPrev < c)
      b = c;
    else if (fPrev <= e && fPrev > c)
      e = c;

    // Turn on the Analytic flag so the function is aware it is being called
    // as a Window Function.
    getContext().setContextFlag(mcsv1sdk::CONTEXT_IS_ANALYTIC);

    // Put the parameter metadata (type, scale, precision) into valsIn
    utils::VLArray<mcsv1sdk::ColumnDatum> valsIn(getContext().getParameterCount());
    ConstantColumn* cc = NULL;

    for (uint32_t i = 0; i < getContext().getParameterCount(); ++i)
    {
      mcsv1sdk::ColumnDatum& datum = valsIn[i];
      cc = static_cast<ConstantColumn*>(fConstantParms[i].get());

      if (cc)
      {
        datum.dataType = cc->resultType().colDataType;
        datum.scale = cc->resultType().scale;
        datum.precision = cc->resultType().precision;
      }
      else
      {
        uint64_t colIn = fFieldIndex[i + 1];
        datum.dataType = fRow.getColType(colIn);
        datum.scale = fRow.getScale(colIn);
        datum.precision = fRow.getPrecision(colIn);
      }
    }

    if (b <= c && c <= e)
      getContext().setContextFlag(mcsv1sdk::CONTEXT_HAS_CURRENT_ROW);
    else
      getContext().clearContextFlag(mcsv1sdk::CONTEXT_HAS_CURRENT_ROW);

    bool bSkipIt = false;
    utils::VLArray<uint32_t> flags(getContext().getParameterCount());
    for (int64_t i = b; i <= e; i++)
    {
      if (i % 1000 == 0 && fStep->cancelled())
        break;

      fRow.setData(getPointer(fRowData->at(i)));

      // NULL flags
      bSkipIt = false;

      for (uint32_t k = 0; k < getContext().getParameterCount(); ++k)
      {
        cc = static_cast<ConstantColumn*>(fConstantParms[k].get());
        uint64_t colIn = fFieldIndex[k + 1];
        mcsv1sdk::ColumnDatum& datum = valsIn[k];

        // Turn on Null flags or skip based on respect nulls
        flags[k] = 0;

        if ((!cc && fRow.isNullValue(colIn) == true) || (cc && cc->type() == ConstantColumn::NULLDATA))
        {
          if (!bRespectNulls)
          {
            bSkipIt = true;
            break;
          }

          flags[k] |= mcsv1sdk::PARAM_IS_NULL;
        }

        if (!bSkipIt && !(flags[k] & mcsv1sdk::PARAM_IS_NULL))
        {
          switch (datum.dataType)
          {
            case CalpontSystemCatalog::TINYINT:
            case CalpontSystemCatalog::SMALLINT:
            case CalpontSystemCatalog::MEDINT:
            case CalpontSystemCatalog::INT:
            case CalpontSystemCatalog::BIGINT:
            {
              int64_t valIn;

              if (cc)
              {
                valIn = cc->getIntVal(fRow, isNull);
              }
              else
              {
                getValue(colIn, valIn);
              }

              // Check for distinct, if turned on.
              // Currently, distinct only works on the first parameter.
              if (k == 0 && fDistinct)
              {
                // MCOL-1698
                std::pair<static_any::any, uint64_t> val = make_pair(valIn, 1);
                // Unordered_map will not insert a duplicate key (valIn).
                // If it doesn't insert, the original pair will be returned
                // in distinct.first and distinct.second will be a bool --
                // true if newly inserted, false if a duplicate.
                std::pair<DistinctMap::iterator, bool> distinct;
                distinct = fDistinctMap.insert(val);
                if (distinct.second == false)
                {
                  // This is a duplicate: increment the count
                  ++(*distinct.first).second;
                  bSkipIt = true;
                  continue;
                }
              }

              datum.columnData = valIn;
              break;
            }

            case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
            {
              if (fRow.getColumnWidth(colIn) < 16)
              {
                int64_t valIn;

                if (cc)
                {
                  valIn = cc->getDecimalVal(fRow, isNull).value;
                }
                else
                {
                  getValue(colIn, valIn);
                }

                // Check for distinct, if turned on.
                // Currently, distinct only works on the first parameter.
                if (k == 0 && fDistinct)
                {
                  std::pair<static_any::any, uint64_t> val = make_pair(valIn, 1);
                  std::pair<DistinctMap::iterator, bool> distinct;
                  distinct = fDistinctMap.insert(val);
                  if (distinct.second == false)
                  {
                    ++(*distinct.first).second;
                    bSkipIt = true;
                    continue;
                  }
                }

                datum.columnData = valIn;
              }
              else
              {
                int128_t valIn;

                if (cc)
                {
                  valIn = cc->getDecimalVal(fRow, isNull).s128Value;
                }
                else
                {
                  getValue(colIn, valIn);
                }

                // Check for distinct, if turned on.
                // Currently, distinct only works on the first parameter.
                if (k == 0 && fDistinct)
                {
                  std::pair<static_any::any, uint64_t> val = make_pair(valIn, 1);
                  std::pair<DistinctMap::iterator, bool> distinct;
                  distinct = fDistinctMap.insert(val);
                  if (distinct.second == false)
                  {
                    ++(*distinct.first).second;
                    bSkipIt = true;
                    continue;
                  }
                }

                datum.columnData = valIn;
              }
              break;
            }

            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT:
            case CalpontSystemCatalog::TIME:
            case CalpontSystemCatalog::DATE:
            case CalpontSystemCatalog::DATETIME:
            case CalpontSystemCatalog::TIMESTAMP:
            {
              uint64_t valIn;

              if (cc)
              {
                valIn = cc->getUintVal(fRow, isNull);
              }
              else
              {
                getValue(colIn, valIn);
              }

              // Check for distinct, if turned on.
              // Currently, distinct only works on the first parameter.
              if (k == 0 && fDistinct)
              {
                std::pair<static_any::any, uint64_t> val = make_pair(valIn, 1);
                std::pair<DistinctMap::iterator, bool> distinct;
                distinct = fDistinctMap.insert(val);
                if (distinct.second == false)
                {
                  ++(*distinct.first).second;
                  bSkipIt = true;
                  continue;
                }
              }

              datum.columnData = valIn;
              break;
            }

            case CalpontSystemCatalog::DOUBLE:
            case CalpontSystemCatalog::UDOUBLE:
            {
              double valIn;

              if (cc)
              {
                valIn = cc->getDoubleVal(fRow, isNull);
              }
              else
              {
                getValue(colIn, valIn);
              }

              // Check for distinct, if turned on.
              // Currently, distinct only works on the first parameter.
              if (k == 0 && fDistinct)
              {
                std::pair<static_any::any, uint64_t> val = make_pair(valIn, 1);
                std::pair<DistinctMap::iterator, bool> distinct;
                distinct = fDistinctMap.insert(val);
                if (distinct.second == false)
                {
                  ++(*distinct.first).second;
                  bSkipIt = true;
                  continue;
                }
              }

              datum.columnData = valIn;
              break;
            }

            case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT:
            {
              float valIn;

              if (cc)
              {
                valIn = cc->getFloatVal(fRow, isNull);
              }
              else
              {
                getValue(colIn, valIn);
              }

              // Check for distinct, if turned on.
              // Currently, distinct only works on the first parameter.
              if (k == 0 && fDistinct)
              {
                std::pair<static_any::any, uint64_t> val = make_pair(valIn, 1);
                std::pair<DistinctMap::iterator, bool> distinct;
                distinct = fDistinctMap.insert(val);
                if (distinct.second == false)
                {
                  ++(*distinct.first).second;
                  bSkipIt = true;
                  continue;
                }
              }

              datum.columnData = valIn;
              break;
            }

            case CalpontSystemCatalog::LONGDOUBLE:
            {
              long double valIn;

              if (cc)
              {
                valIn = cc->getLongDoubleVal(fRow, isNull);
              }
              else
              {
                getValue(colIn, valIn);
              }

              // Check for distinct, if turned on.
              // Currently, distinct only works on the first parameter.
              if (k == 0 && fDistinct)
              {
                std::pair<static_any::any, uint64_t> val = make_pair(valIn, 1);
                std::pair<DistinctMap::iterator, bool> distinct;
                distinct = fDistinctMap.insert(val);
                if (distinct.second == false)
                {
                  ++(*distinct.first).second;
                  bSkipIt = true;
                  continue;
                }
              }

              datum.columnData = valIn;
              break;
            }

            case CalpontSystemCatalog::CHAR:
            case CalpontSystemCatalog::VARCHAR:
            case CalpontSystemCatalog::VARBINARY:
            case CalpontSystemCatalog::TEXT:
            case CalpontSystemCatalog::BLOB:
            {
              string valIn;

              if (cc)
              {
                valIn = cc->getStrVal(fRow, isNull);
              }
              else
              {
                getValue(colIn, valIn);
              }

              // Check for distinct, if turned on.
              // Currently, distinct only works on the first parameter.
              if (k == 0 && fDistinct)
              {
                std::pair<static_any::any, uint64_t> val = make_pair(valIn, 1);
                std::pair<DistinctMap::iterator, bool> distinct;
                distinct = fDistinctMap.insert(val);
                if (distinct.second == false)
                {
                  ++(*distinct.first).second;
                  bSkipIt = true;
                  continue;
                }
              }

              datum.columnData = valIn;
              break;
            }

            default:
            {
              string errStr = "(" + colType2String[(int)datum.dataType] + ")";
              errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_INVALID_PARM_TYPE, errStr);
              cerr << errStr << endl;
              throw IDBExcept(errStr, ERR_WF_INVALID_PARM_TYPE);

              break;
            }
          }
        }
      }

      // Skip if any value is NULL and respect nulls is off.
      if (bSkipIt)
      {
        continue;
      }

      getContext().setDataFlags(flags);

      rc = getContext().getFunction()->nextValue(&getContext(), valsIn);

      if (rc == mcsv1sdk::mcsv1_UDAF::ERROR)
      {
        bInterrupted = true;
        string errStr =
            IDBErrorInfo::instance()->errorMsg(ERR_WF_UDANF_ERROR, getContext().getErrorMessage());
        cerr << errStr << endl;
        throw IDBExcept(errStr, ERR_WF_UDANF_ERROR);
      }
    }

    rc = getContext().getFunction()->evaluate(&getContext(), fValOut);

    if (rc == mcsv1sdk::mcsv1_UDAF::ERROR)
    {
      bInterrupted = true;
      string errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_UDANF_ERROR, getContext().getErrorMessage());
      cerr << errStr << endl;
      throw IDBExcept(errStr, ERR_WF_UDANF_ERROR);
    }
  }

  SetUDAFValue(fValOut, colOut, b, e, c);

  fPrev = c;
}
}  // namespace windowfunction
// vim:ts=4 sw=4:

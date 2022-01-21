/* CopyrighT (C) 2014 InfiniDB, Inc.
   Copyright (c) 2019 MariaDB Corporation

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

//  $Id: wf_sum_avg.cpp 3932 2013-06-25 16:08:10Z xlou $

//#define NDEBUG
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
using namespace execplan;

#include "windowfunctionstep.h"
using namespace joblist;

#include "wf_sum_avg.h"

namespace windowfunction
{
template <typename T_IN, typename T_OUT>
inline void WF_sum_avg<T_IN, T_OUT>::checkSumLimit(const T_IN& val, const T_OUT& sum)
{
}

template <>
inline void WF_sum_avg<int128_t, int128_t>::checkSumLimit(const int128_t& val, const int128_t& sum)
{
  datatypes::AdditionOverflowCheck ofCheckOp;
  ofCheckOp(sum, val);
}

template <>
inline void WF_sum_avg<long double, long double>::checkSumLimit(const long double& val,
                                                                const long double& sum)
{
}

template <>
inline void WF_sum_avg<float, long double>::checkSumLimit(const float&, const long double&)
{
}

template <>
inline void WF_sum_avg<long, long double>::checkSumLimit(const long&, const long double&)
{
}

template <>
inline void WF_sum_avg<unsigned long, long double>::checkSumLimit(const unsigned long&, const long double&)
{
}
template <>
inline void WF_sum_avg<double, long double>::checkSumLimit(const double&, const long double&)
{
}

template <>
void WF_sum_avg<int128_t, int128_t>::checkSumLimit(const int128_t&, const int128_t&);
template <>
void WF_sum_avg<long double, long double>::checkSumLimit(const long double& val, const long double&);
template <>
void WF_sum_avg<float, long double>::checkSumLimit(const float&, const long double&);
template <>
void WF_sum_avg<long, long double>::checkSumLimit(const long&, const long double&);
template <>
void WF_sum_avg<unsigned long, long double>::checkSumLimit(const unsigned long&, const long double&);
template <>
void WF_sum_avg<double, long double>::checkSumLimit(const double&, const long double&);

template <typename T_IN, typename T_OUT>
int128_t WF_sum_avg<T_IN, T_OUT>::calculateAvg(const int128_t& sum, const uint64_t count, const int scale)
{
  int128_t avg = 0;
  int128_t factor;
  datatypes::getScaleDivisor(factor, scale);
  if (scale > 0)
  {
    if ((sum * factor) / factor == sum)
    {
      avg = sum * factor;
      avg /= count;
    }
    else
    {
      // scale won't fit before divide, we're gonna lose precision.
      avg = sum / count;
      if ((avg * factor) / factor != avg)  // Still won't fit
      {
        string errStr = string("AVG(int)");
        errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_OVERFLOW, errStr);
        cerr << errStr << endl;
        throw IDBExcept(errStr, ERR_WF_OVERFLOW);
      }
      avg *= factor;
    }
  }
  else
  {
    avg = sum / count;
  }

  avg += (avg < 0) ? (-0.5) : (0.5);
  return avg;
}

template <typename T_IN, typename T_OUT>
inline long double WF_sum_avg<T_IN, T_OUT>::calculateAvg(const long double& sum, const uint64_t count,
                                                         const int scale)
{
  return sum / count;
}

// For the static function makeFunction, the template parameters are ignored
template <typename T_IN, typename T_OUT>
boost::shared_ptr<WindowFunctionType> WF_sum_avg<T_IN, T_OUT>::makeFunction(int id, const string& name,
                                                                            int ct, WindowFunctionColumn* wc)
{
  boost::shared_ptr<WindowFunctionType> func;
  switch (ct)
  {
    case CalpontSystemCatalog::TINYINT:
    case CalpontSystemCatalog::SMALLINT:
    case CalpontSystemCatalog::MEDINT:
    case CalpontSystemCatalog::INT:
    case CalpontSystemCatalog::BIGINT:
    {
      // Look into using int128_t instead of long double
      func.reset(new WF_sum_avg<int64_t, long double>(id, name));
      break;
    }

    case CalpontSystemCatalog::UTINYINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT:
    case CalpontSystemCatalog::UBIGINT:
    {
      func.reset(new WF_sum_avg<uint64_t, long double>(id, name));
      break;
    }

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      decltype(datatypes::MAXDECIMALWIDTH) width = wc->functionParms()[0]->resultType().colWidth;

      if (width < datatypes::MAXDECIMALWIDTH)
      {
        if (ct == CalpontSystemCatalog::UDECIMAL)
          func.reset(new WF_sum_avg<uint64_t, int128_t>(id, name));
        else
          func.reset(new WF_sum_avg<int64_t, int128_t>(id, name));
      }
      else if (width == datatypes::MAXDECIMALWIDTH)
      {
        func.reset(new WF_sum_avg<int128_t, int128_t>(id, name));
      }
      break;
    }

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE:
    {
      func.reset(new WF_sum_avg<double, long double>(id, name));
      break;
    }

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT:
    {
      func.reset(new WF_sum_avg<float, long double>(id, name));
      break;
    }

    case CalpontSystemCatalog::LONGDOUBLE:
    {
      func.reset(new WF_sum_avg<long double, long double>(id, name));
      break;
    }
    default:
    {
      string errStr = name + "(" + colType2String[ct] + ")";
      errStr = IDBErrorInfo::instance()->errorMsg(ERR_WF_INVALID_PARM_TYPE, errStr);
      cerr << errStr << endl;
      throw IDBExcept(errStr, ERR_WF_INVALID_PARM_TYPE);

      break;
    }
  }

  return func;
}

template <typename T_IN, typename T_OUT>
WindowFunctionType* WF_sum_avg<T_IN, T_OUT>::clone() const
{
  return new WF_sum_avg<T_IN, T_OUT>(*this);
}

template <typename T_IN, typename T_OUT>
void WF_sum_avg<T_IN, T_OUT>::resetData()
{
  fAvg = 0;
  fSum = 0;
  fCount = 0;
  fSet.clear();

  WindowFunctionType::resetData();
}

template <typename T_IN, typename T_OUT>
void WF_sum_avg<T_IN, T_OUT>::operator()(int64_t b, int64_t e, int64_t c)
{
  uint64_t colOut = fFieldIndex[0];

  if ((fFrameUnit == WF__FRAME_ROWS) || (fPrev == -1) ||
      (!fPeer->operator()(getPointer(fRowData->at(c)), getPointer(fRowData->at(fPrev)))))
  {
    // for unbounded - current row special handling
    if (fPrev >= b && fPrev < c)
      b = c;
    else if (fPrev <= e && fPrev > c)
      e = c;

    uint64_t colIn = fFieldIndex[1];
    int scale = fRow.getScale(colOut) - fRow.getScale(colIn);
    for (int64_t i = b; i <= e; i++)
    {
      if (i % 1000 == 0 && fStep->cancelled())
        break;

      fRow.setData(getPointer(fRowData->at(i)));

      if (fRow.isNullValue(colIn) == true)
        continue;

      CDT cdt;
      getValue(colIn, fVal, &cdt);

      if ((!fDistinct) || (fSet.find(fVal) == fSet.end()))
      {
        checkSumLimit(fVal, fSum);
        fSum += (T_OUT)fVal;
        fCount++;

        if (fDistinct)
          fSet.insert(fVal);
      }
    }

    if ((fCount > 0) && (fFunctionId == WF__AVG || fFunctionId == WF__AVG_DISTINCT))
    {
      fAvg = calculateAvg(fSum, fCount, scale);
    }
  }

  T_OUT* v = NULL;

  if (fCount > 0)
  {
    if (fFunctionId == WF__AVG || fFunctionId == WF__AVG_DISTINCT)
      v = &fAvg;
    else
      v = &fSum;
  }

  setValue(fRow.getColType(colOut), b, e, c, v);

  fPrev = c;
}

template boost::shared_ptr<WindowFunctionType> WF_sum_avg<int64_t, long double>::makeFunction(
    int, const string&, int, WindowFunctionColumn*);

}  // namespace windowfunction
// vim:ts=4 sw=4:

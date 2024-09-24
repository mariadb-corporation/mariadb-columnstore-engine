/* Copyright (C) 2014 InfiniDB, Inc.
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

//  $Id: wf_stats.cpp 3932 2013-06-25 16:08:10Z xlou $

// #define NDEBUG
#include <cmath>
#include <sstream>
using namespace std;

#include <boost/shared_ptr.hpp>
using namespace boost;

#include "loggingid.h"
#include "errorcodes.h"
#include "idberrorinfo.h"
using namespace logging;

#include "idborderby.h"
using namespace ordering;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "windowfunctionstep.h"
using namespace joblist;

#include "wf_stats.h"

namespace windowfunction
{
template <typename T>
boost::shared_ptr<WindowFunctionType> WF_stats<T>::makeFunction(int id, const string& name, int ct,
                                                                WindowFunctionColumn* wc)
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
      func.reset(new WF_stats<int64_t>(id, name));
      break;
    }

    case CalpontSystemCatalog::UTINYINT:
    case CalpontSystemCatalog::USMALLINT:
    case CalpontSystemCatalog::UMEDINT:
    case CalpontSystemCatalog::UINT:
    case CalpontSystemCatalog::UBIGINT:
    {
      func.reset(new WF_stats<uint64_t>(id, name));
      break;
    }

    case CalpontSystemCatalog::DECIMAL:
    case CalpontSystemCatalog::UDECIMAL:
    {
      decltype(datatypes::MAXDECIMALWIDTH) width = wc->functionParms()[0]->resultType().colWidth;
      if (width < datatypes::MAXDECIMALWIDTH)
      {
        if (ct == CalpontSystemCatalog::UDECIMAL)
          func.reset(new WF_stats<uint64_t>(id, name));
        else
          func.reset(new WF_stats<int64_t>(id, name));
      }
      else if (width == datatypes::MAXDECIMALWIDTH)
      {
        func.reset(new WF_stats<int128_t>(id, name));
      }
      break;
    }

    case CalpontSystemCatalog::DOUBLE:
    case CalpontSystemCatalog::UDOUBLE:
    {
      func.reset(new WF_stats<double>(id, name));
      break;
    }

    case CalpontSystemCatalog::FLOAT:
    case CalpontSystemCatalog::UFLOAT:
    {
      func.reset(new WF_stats<float>(id, name));
      break;
    }

    case CalpontSystemCatalog::LONGDOUBLE:
    {
      func.reset(new WF_stats<long double>(id, name));
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

template <typename T>
WindowFunctionType* WF_stats<T>::clone() const
{
  return new WF_stats<T>(*this);
}

template <typename T>
void WF_stats<T>::resetData()
{
  mean_ = 0;
  scaledMomentum2_ = 0;
  count_ = 0;
  stats_ = 0.0;

  WindowFunctionType::resetData();
}

template <typename T>
void WF_stats<T>::operator()(int64_t b, int64_t e, int64_t c)
{
  CDT cdt;
  if ((fFrameUnit == WF__FRAME_ROWS) || (fPrev == -1) ||
      (!fPeer->operator()(getPointer(fRowData->at(c)), getPointer(fRowData->at(fPrev)))))
  {
    // for unbounded - current row special handling
    if (fPrev >= b && fPrev < c)
      b = c;
    else if (fPrev <= e && fPrev > c)
      e = c;

    uint64_t colIn = fFieldIndex[1];

    for (int64_t i = b; i <= e; i++)
    {
      if (i % 1000 == 0 && fStep->cancelled())
        break;

      fRow.setData(getPointer(fRowData->at(i)));

      if (fRow.isNullValue(colIn) == true)
        continue;
      // Welford's single-pass algorithm
      T valIn;
      getValue(colIn, valIn, &cdt);
      long double val = (long double)valIn;
      count_++;
      long double delta = val - mean_;
      mean_ += delta / count_;
      scaledMomentum2_ += delta * (val - mean_);
    }

    if (count_ > 1)
    {
      uint32_t scale = fRow.getScale(colIn);
      auto factor = datatypes::scaleDivisor<long double>(scale);
      long double stat = scaledMomentum2_;

      // adjust the scale if necessary
      if (scale != 0 && cdt != CalpontSystemCatalog::LONGDOUBLE)
      {
        stat /= factor * factor;
      }

      if (fFunctionId == WF__STDDEV_POP)
        stat = sqrt(stat / count_);
      else if (fFunctionId == WF__STDDEV_SAMP)
        stat = sqrt(stat / (count_ - 1));
      else if (fFunctionId == WF__VAR_POP)
        stat = stat / count_;
      else if (fFunctionId == WF__VAR_SAMP)
        stat = stat / (count_ - 1);

      stats_ = (double)stat;
    }
  }

  if (count_ == 0)
  {
    setValue(CalpontSystemCatalog::DOUBLE, b, e, c, (double*)nullptr);
  }
  else if (count_ == 1)
  {
    if (fFunctionId == WF__STDDEV_SAMP || fFunctionId == WF__VAR_SAMP)
    {
      setValue(CalpontSystemCatalog::DOUBLE, b, e, c, (double*)nullptr);
    }
    else  // fFunctionId == WF__STDDEV_POP || fFunctionId == WF__VAR_POP
    {
      double temp = 0.0;
      setValue(CalpontSystemCatalog::DOUBLE, b, e, c, (double*)&temp);
    }
  }
  else
  {
    setValue(CalpontSystemCatalog::DOUBLE, b, e, c, &stats_);
  }

  fPrev = c;
}

template boost::shared_ptr<WindowFunctionType> WF_stats<int64_t>::makeFunction(int, const string&, int,
                                                                               WindowFunctionColumn*);

}  // namespace windowfunction

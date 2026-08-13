/* Copyright (C) 2014 InfiniDB, Inc.

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

/****************************************************************************
 * $Id: Func_sysdate.cpp 2477 2011-04-01 16:07:35Z rdempsey $
 *
 *
 ****************************************************************************/

#include <cstdlib>
#include <string>
#include <sstream>
using namespace std;

#include "errorcodes.h"
#include "idberrorinfo.h"
#include "errorids.h"
using namespace logging;

#include "functor_dtm.h"
#include "funchelpers.h"
#include "functioncolumn.h"
#include "rowgroup.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

namespace funcexp
{
CalpontSystemCatalog::ColType Func_sysdate::operationType(FunctionParm& /*fp*/,
                                                          CalpontSystemCatalog::ColType& resultType)
{
  return resultType;
}

int64_t Func_sysdate::getIntVal(rowgroup::Row& /*row*/, FunctionParm& /*parm*/, bool& /*isNull*/,
                                CalpontSystemCatalog::ColType& /*operationColType*/)
{
  return nowDatetime();
}

string Func_sysdate::getStrVal(rowgroup::Row& /*row*/, FunctionParm& /*parm*/, bool& /*isNull*/,
                               CalpontSystemCatalog::ColType& /*operationColType*/)
{
  time_t now;
  now = time(NULL);
  struct tm tm;
  localtime_r(&now, &tm);

  char timestamp[80];
  strftime(timestamp, 80, "%Y-%m-%d %H:%M:%S", &tm);
  return timestamp;
}

int32_t Func_sysdate::getDateIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& operationColType)
{
  return (((getIntVal(row, parm, isNull, operationColType) >> 32) & 0xFFFFFFC0) | 0x3E);
}

int64_t Func_sysdate::getDatetimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                        CalpontSystemCatalog::ColType& operationColType)
{
  return getIntVal(row, parm, isNull, operationColType);
}

int64_t Func_sysdate::getTimestampIntVal(rowgroup::Row& /*row*/, FunctionParm& /*parm*/, bool& /*isNull*/,
                                         CalpontSystemCatalog::ColType& /*operationColType*/)
{
  timeval tv;
  gettimeofday(&tv, nullptr);

  dataconvert::TimeStamp ts;
  ts.second = tv.tv_sec;
  ts.msecond = tv.tv_usec;
  return *reinterpret_cast<int64_t*>(&ts);
}

int64_t Func_sysdate::getTimeIntVal(rowgroup::Row& row, FunctionParm& parm, bool& isNull,
                                    CalpontSystemCatalog::ColType& operationColType)
{
  return getIntVal(row, parm, isNull, operationColType);
}

}  // namespace funcexp

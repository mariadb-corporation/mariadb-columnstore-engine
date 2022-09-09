/* Copyright (C) 2017 MariaDB Corporation

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
#include "regr_intercept.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

class Add_regr_intercept_ToUDAFMap
{
 public:
  Add_regr_intercept_ToUDAFMap()
  {
    UDAFMap::getMap()["regr_intercept"] = new regr_intercept();
  }
};

static Add_regr_intercept_ToUDAFMap addToMap;

// Use the simple data model
struct regr_intercept_data
{
  uint64_t cnt;
  long double avgx;
  long double cx;
  long double avgy;
  long double cxy;
};

mcsv1_UDAF::ReturnCode regr_intercept::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() != 2)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_intercept() with other than 2 arguments");
    return mcsv1_UDAF::ERROR;
  }
  if (!(isNumeric(colTypes[0].dataType) && isNumeric(colTypes[1].dataType)))
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_intercept() with non-numeric arguments");
    return mcsv1_UDAF::ERROR;
  }

  context->setUserDataSize(sizeof(regr_intercept_data));
  context->setResultType(execplan::CalpontSystemCatalog::DOUBLE);
  context->setColWidth(8);
  context->setScale(DECIMAL_NOT_SPECIFIED);
  context->setPrecision(0);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_intercept::reset(mcsv1Context* context)
{
  struct regr_intercept_data* data = (struct regr_intercept_data*)context->getUserData()->data;
  data->cnt = 0;
  data->avgx = 0.0;
  data->cx = 0.0;
  data->avgy = 0.0;
  data->cxy = 0.0;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_intercept::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  double valy = toDouble(valsIn[0]);
  double valx = toDouble(valsIn[1]);
  struct regr_intercept_data* data = (struct regr_intercept_data*)context->getUserData()->data;

  long double avgyPrev = data->avgy;
  long double avgxPrev = data->avgx;
  long double cxPrev = data->cx;
  long double cxyPrev = data->cxy;
  ++data->cnt;
  uint64_t cnt = data->cnt;

  long double dx = valx - avgxPrev;
  long double dy = valy - avgyPrev;

  avgyPrev += dy / cnt;
  avgxPrev += dx / cnt;

  cxPrev += dx * (valx - avgxPrev);

  cxyPrev += dx * (valy - avgyPrev);

  data->avgx = avgxPrev;
  data->avgy = avgyPrev;
  data->cx = cxPrev;
  data->cxy = cxyPrev;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_intercept::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (!userDataIn)
  {
    return mcsv1_UDAF::SUCCESS;
  }

  struct regr_intercept_data* outData = (struct regr_intercept_data*)context->getUserData()->data;
  struct regr_intercept_data* inData = (struct regr_intercept_data*)userDataIn->data;

  uint64_t outCnt = outData->cnt;
  long double outAvgx = outData->avgx;
  long double outAvgy = outData->avgy;
  long double outCx = outData->cx;
  long double outCxy = outData->cxy;

  uint64_t inCnt = inData->cnt;
  long double inAvgx = inData->avgx;
  long double inAvgy = inData->avgy;
  long double inCx = inData->cx;
  long double inCxy = inData->cxy;

  uint64_t resCnt = inCnt + outCnt;
  if (resCnt == 0)
  {
    outData->avgx = 0;
    outData->avgy = 0;
    outData->cx = 0;
    outData->cxy = 0;
    outData->cnt = 0;
  }
  else
  {
    long double deltax = outAvgx - inAvgx;
    long double deltay = outAvgy - inAvgy;

    long double resAvgx = inAvgx + deltax * outCnt / resCnt;
    long double resAvgy = inAvgy + deltay * outCnt / resCnt;

    long double resCx = outCx + inCx + deltax * deltax * inCnt * outCnt / resCnt;

    long double resCxy = outCxy + inCxy + deltax * deltay * inCnt * outCnt / resCnt;

    outData->avgx = resAvgx;
    outData->avgy = resAvgy;
    outData->cx = resCx;
    outData->cxy = resCxy;
    outData->cnt = resCnt;
  }
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_intercept::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  struct regr_intercept_data* data = (struct regr_intercept_data*)context->getUserData()->data;
  long double N = data->cnt;
  if (N > 1)
  {
    long double avgx = data->avgx;
    long double avgy = data->avgy;
    long double cx = data->cx;
    long double cxy = data->cxy;
    // regr_intercept is AVG(y) - slope(y,x)*avg(x)
    if (cx > 0)
    {
      valOut = static_cast<double>(avgy - cxy / cx * avgx);
    }
  }
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_intercept::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  double valy = toDouble(valsDropped[0]);
  double valx = toDouble(valsDropped[1]);
  struct regr_intercept_data* data = (struct regr_intercept_data*)context->getUserData()->data;

  long double avgyPrev = data->avgy;
  long double avgxPrev = data->avgx;
  long double cxPrev = data->cx;
  long double cxyPrev = data->cxy;
  --data->cnt;
  uint64_t cnt = data->cnt;
  if (cnt == 0)
  {
    data->avgx = 0;
    data->avgy = 0;
    data->cx = 0;
    data->cxy = 0;
  }
  else
  {
    long double dx = valx - avgxPrev;
    long double dy = valy - avgyPrev;

    avgyPrev -= dy / cnt;
    avgxPrev -= dx / cnt;

    cxPrev -= dx * (valx - avgxPrev);

    cxyPrev -= dx * (valy - avgyPrev);

    data->avgx = avgxPrev;
    data->avgy = avgyPrev;
    data->cx = cxPrev;
    data->cxy = cxyPrev;
  }
  return mcsv1_UDAF::SUCCESS;
}

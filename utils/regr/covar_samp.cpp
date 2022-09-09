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
#include "covar_samp.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

class Add_covar_samp_ToUDAFMap
{
 public:
  Add_covar_samp_ToUDAFMap()
  {
    UDAFMap::getMap()["covar_samp"] = new covar_samp();
  }
};

static Add_covar_samp_ToUDAFMap addToMap;

// Use the simple data model
struct covar_samp_data
{
  uint64_t cnt;
  long double avgx;
  long double avgy;
  long double cxy;
};

mcsv1_UDAF::ReturnCode covar_samp::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() != 2)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("covar_samp() with other than 2 arguments");
    return mcsv1_UDAF::ERROR;
  }
  if (!(isNumeric(colTypes[0].dataType) && isNumeric(colTypes[1].dataType)))
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("covar_samp() with non-numeric arguments");
    return mcsv1_UDAF::ERROR;
  }

  context->setUserDataSize(sizeof(covar_samp_data));
  context->setResultType(execplan::CalpontSystemCatalog::DOUBLE);
  context->setColWidth(8);
  context->setScale(DECIMAL_NOT_SPECIFIED);
  context->setPrecision(0);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode covar_samp::reset(mcsv1Context* context)
{
  struct covar_samp_data* data = (struct covar_samp_data*)context->getUserData()->data;
  data->cnt = 0;
  data->avgx = 0.0;
  data->avgy = 0.0;
  data->cxy = 0.0;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode covar_samp::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  double valy = toDouble(valsIn[0]);
  double valx = toDouble(valsIn[1]);
  struct covar_samp_data* data = (struct covar_samp_data*)context->getUserData()->data;

  long double avgyPrev = data->avgy;
  long double avgxPrev = data->avgx;
  long double cxyPrev = data->cxy;
  ++data->cnt;
  uint64_t cnt = data->cnt;
  long double dx = valx - avgxPrev;
  avgyPrev += (valy - avgyPrev)/cnt;
  avgxPrev += dx / cnt;
  cxyPrev += dx * (valy - avgyPrev);
  data->avgx = avgxPrev;
  data->avgy = avgyPrev;
  data->cxy = cxyPrev;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode covar_samp::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (!userDataIn)
  {
    return mcsv1_UDAF::SUCCESS;
  }

  struct covar_samp_data* outData = (struct covar_samp_data*)context->getUserData()->data;
  struct covar_samp_data* inData = (struct covar_samp_data*)userDataIn->data;

  uint64_t outCnt = outData->cnt;
  long double outAvgx = outData->avgx;
  long double outAvgy = outData->avgy;
  long double outCxy = outData->cxy;

  uint64_t inCnt = inData->cnt;
  long double inAvgx = inData->avgx;
  long double inAvgy = inData->avgy;
  long double inCxy = inData->cxy;

  uint64_t resCnt = inCnt + outCnt;
  if (resCnt == 0)
  {
    outData->avgx = 0;
    outData->avgy = 0;
    outData->cxy = 0;
    outData->cnt = 0;
  }
  else
  {
    long double deltax = outAvgx - inAvgx;
    long double deltay = outAvgy - inAvgy;
    long double resAvgx = inAvgx + deltax * outCnt / resCnt;
    long double resAvgy = inAvgy + deltay * outCnt / resCnt;
    long double resCxy = outCxy + inCxy + deltax * deltay * inCnt * outCnt / resCnt;

    outData->avgx = resAvgx;
    outData->avgy = resAvgy;
    outData->cxy = resCxy;
    outData->cnt = resCnt;
  }
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode covar_samp::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  struct covar_samp_data* data = (struct covar_samp_data*)context->getUserData()->data;
  double N = data->cnt;
  if (N > 1)
  {
    long double covar_samp = data->cxy / (N - 1);
    valOut = static_cast<double>(covar_samp);
  }
  else if (N == 1)
  {
    valOut = 0;
  }
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode covar_samp::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  double valy = toDouble(valsDropped[0]);
  double valx = toDouble(valsDropped[1]);
  struct covar_samp_data* data = (struct covar_samp_data*)context->getUserData()->data;

  long double avgyPrev = data->avgy;
  long double avgxPrev = data->avgx;
  long double cxyPrev = data->cxy;
  --data->cnt;
  uint64_t cnt = data->cnt;
  long double dx = valx - avgxPrev;

  avgyPrev -= (valy - avgyPrev) / cnt;
  avgxPrev -= dx / cnt;
  cxyPrev -= dx * (valy - avgyPrev);

  data->avgx = avgxPrev;
  data->avgy = avgyPrev;
  data->cxy = cxyPrev;
  return mcsv1_UDAF::SUCCESS;
}

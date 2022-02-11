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
  long double sumx;
  long double sumy;
  long double sumxy;  // sum of x * y
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
  data->sumx = 0.0;
  data->sumy = 0.0;
  data->sumxy = 0.0;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode covar_samp::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  double valy = toDouble(valsIn[0]);
  double valx = toDouble(valsIn[1]);
  struct covar_samp_data* data = (struct covar_samp_data*)context->getUserData()->data;

  data->sumy += valy;

  data->sumx += valx;

  data->sumxy += valx * valy;

  ++data->cnt;

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

  outData->sumx += inData->sumx;
  outData->sumy += inData->sumy;
  outData->sumxy += inData->sumxy;
  outData->cnt += inData->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode covar_samp::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  struct covar_samp_data* data = (struct covar_samp_data*)context->getUserData()->data;
  double N = data->cnt;
  if (N > 1)
  {
    long double sumx = data->sumx;
    long double sumy = data->sumy;
    long double sumxy = data->sumxy;

    long double covar_samp = (sumxy - ((sumx * sumy) / N)) / (N - 1);
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

  data->sumy -= valy;

  data->sumx -= valx;

  data->sumxy -= valx * valy;
  --data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

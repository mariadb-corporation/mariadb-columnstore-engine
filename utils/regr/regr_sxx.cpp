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
#include "regr_sxx.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

class Add_regr_sxx_ToUDAFMap
{
 public:
  Add_regr_sxx_ToUDAFMap()
  {
    UDAFMap::getMap()["regr_sxx"] = new regr_sxx();
  }
};

static Add_regr_sxx_ToUDAFMap addToMap;

// Use the simple data model
struct regr_sxx_data
{
  uint64_t cnt;
  long double sumx;
  long double sumx2;  // sum of (x squared)
};

mcsv1_UDAF::ReturnCode regr_sxx::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() != 2)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_sxx() with other than 2 arguments");
    return mcsv1_UDAF::ERROR;
  }
  if (!(isNumeric(colTypes[1].dataType)))
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_sxx() with a non-numeric independant (second) argument");
    return mcsv1_UDAF::ERROR;
  }

  context->setUserDataSize(sizeof(regr_sxx_data));
  context->setResultType(execplan::CalpontSystemCatalog::DOUBLE);
  context->setColWidth(8);
  context->setScale(DECIMAL_NOT_SPECIFIED);
  context->setPrecision(0);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_sxx::reset(mcsv1Context* context)
{
  struct regr_sxx_data* data = (struct regr_sxx_data*)context->getUserData()->data;
  data->cnt = 0;
  data->sumx = 0.0;
  data->sumx2 = 0.0;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_sxx::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  double valx = toDouble(valsIn[1]);
  struct regr_sxx_data* data = (struct regr_sxx_data*)context->getUserData()->data;

  data->sumx += valx;
  data->sumx2 += valx * valx;

  ++data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_sxx::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (!userDataIn)
  {
    return mcsv1_UDAF::SUCCESS;
  }

  struct regr_sxx_data* outData = (struct regr_sxx_data*)context->getUserData()->data;
  struct regr_sxx_data* inData = (struct regr_sxx_data*)userDataIn->data;

  outData->sumx += inData->sumx;
  outData->sumx2 += inData->sumx2;
  outData->cnt += inData->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_sxx::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  struct regr_sxx_data* data = (struct regr_sxx_data*)context->getUserData()->data;
  long double N = data->cnt;
  if (N > 0)
  {
    long double regr_sxx = (data->sumx2 - (data->sumx * data->sumx / N));
    if (regr_sxx < 0)  // catch -0
      regr_sxx = 0;
    valOut = static_cast<double>(regr_sxx);
  }
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_sxx::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  double valx = toDouble(valsDropped[1]);
  struct regr_sxx_data* data = (struct regr_sxx_data*)context->getUserData()->data;

  data->sumx -= valx;
  data->sumx2 -= valx * valx;

  --data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

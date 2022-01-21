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
#include "regr_r2.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

class Add_regr_r2_ToUDAFMap
{
 public:
  Add_regr_r2_ToUDAFMap()
  {
    UDAFMap::getMap()["regr_r2"] = new regr_r2();
  }
};

static Add_regr_r2_ToUDAFMap addToMap;

// Use the simple data model
struct regr_r2_data
{
  uint64_t cnt;
  long double sumx;
  long double sumx2;  // sum of (x squared)
  long double sumy;
  long double sumy2;  // sum of (y squared)
  long double sumxy;  // sum of x * y
};

mcsv1_UDAF::ReturnCode regr_r2::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() != 2)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_r2() with other than 2 arguments");
    return mcsv1_UDAF::ERROR;
  }
  if (!(isNumeric(colTypes[0].dataType) && isNumeric(colTypes[1].dataType)))
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_r2() with non-numeric arguments");
    return mcsv1_UDAF::ERROR;
  }

  context->setUserDataSize(sizeof(regr_r2_data));
  context->setResultType(execplan::CalpontSystemCatalog::DOUBLE);
  context->setColWidth(8);
  context->setScale(DECIMAL_NOT_SPECIFIED);
  context->setPrecision(0);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_r2::reset(mcsv1Context* context)
{
  struct regr_r2_data* data = (struct regr_r2_data*)context->getUserData()->data;
  data->cnt = 0;
  data->sumx = 0.0;
  data->sumx2 = 0.0;
  data->sumy = 0.0;
  data->sumy2 = 0.0;
  data->sumxy = 0.0;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_r2::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  double valy = toDouble(valsIn[0]);
  double valx = toDouble(valsIn[1]);
  struct regr_r2_data* data = (struct regr_r2_data*)context->getUserData()->data;

  data->sumy += valy;
  data->sumy2 += valy * valy;

  data->sumx += valx;
  data->sumx2 += valx * valx;

  data->sumxy += valx * valy;

  ++data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_r2::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (!userDataIn)
  {
    return mcsv1_UDAF::SUCCESS;
  }

  struct regr_r2_data* outData = (struct regr_r2_data*)context->getUserData()->data;
  struct regr_r2_data* inData = (struct regr_r2_data*)userDataIn->data;

  outData->sumx += inData->sumx;
  outData->sumx2 += inData->sumx2;
  outData->sumy += inData->sumy;
  outData->sumy2 += inData->sumy2;
  outData->sumxy += inData->sumxy;
  outData->cnt += inData->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_r2::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  struct regr_r2_data* data = (struct regr_r2_data*)context->getUserData()->data;
  double N = data->cnt;
  if (N > 1)
  {
    long double sumx = data->sumx;
    long double sumy = data->sumy;
    long double sumx2 = data->sumx2;
    long double sumy2 = data->sumy2;
    long double sumxy = data->sumxy;

    long double var_popx = (sumx2 - (sumx * sumx / N)) / N;
    if (var_popx <= 0)  // Catch -0
    {
      // When var_popx is 0, NULL is the result.
      return mcsv1_UDAF::SUCCESS;
    }
    double var_popy = (sumy2 - (sumy * sumy / N)) / N;
    if (var_popy <= 0)  // Catch -0
    {
      // When var_popy is 0, 1 is the result
      valOut = 1.0;
      return mcsv1_UDAF::SUCCESS;
    }
    long double std_popx = sqrt(var_popx);
    long double std_popy = sqrt(var_popy);
    long double covar_pop = (sumxy - ((sumx * sumy) / N)) / N;
    long double corr = covar_pop / (std_popy * std_popx);
    valOut = static_cast<double>(corr * corr);
  }
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_r2::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  double valy = toDouble(valsDropped[0]);
  double valx = toDouble(valsDropped[1]);
  struct regr_r2_data* data = (struct regr_r2_data*)context->getUserData()->data;

  data->sumy -= valy;
  data->sumy2 -= valy * valy;

  data->sumx -= valx;
  data->sumx2 -= valx * valx;

  data->sumxy -= valx * valy;
  --data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

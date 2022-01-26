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
#include "regr_syy.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

class Add_regr_syy_ToUDAFMap
{
 public:
  Add_regr_syy_ToUDAFMap()
  {
    UDAFMap::getMap()["regr_syy"] = new regr_syy();
  }
};

static Add_regr_syy_ToUDAFMap addToMap;

// Use the simple data model
struct regr_syy_data
{
  uint64_t cnt;
  long double sumy;
  long double sumy2;  // sum of (y squared)
};

mcsv1_UDAF::ReturnCode regr_syy::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() != 2)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_syy() with other than 2 arguments");
    return mcsv1_UDAF::ERROR;
  }
  if (!(isNumeric(colTypes[0].dataType)))
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_syy() with a non-numeric dependant (first) argument");
    return mcsv1_UDAF::ERROR;
  }

  context->setUserDataSize(sizeof(regr_syy_data));
  context->setResultType(execplan::CalpontSystemCatalog::DOUBLE);
  context->setColWidth(8);
  context->setScale(DECIMAL_NOT_SPECIFIED);
  context->setPrecision(0);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_syy::reset(mcsv1Context* context)
{
  struct regr_syy_data* data = (struct regr_syy_data*)context->getUserData()->data;
  data->cnt = 0;
  data->sumy = 0.0;
  data->sumy2 = 0.0;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_syy::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  double valy = toDouble(valsIn[0]);
  struct regr_syy_data* data = (struct regr_syy_data*)context->getUserData()->data;

  data->sumy += valy;
  data->sumy2 += valy * valy;

  ++data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_syy::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (!userDataIn)
  {
    return mcsv1_UDAF::SUCCESS;
  }

  struct regr_syy_data* outData = (struct regr_syy_data*)context->getUserData()->data;
  struct regr_syy_data* inData = (struct regr_syy_data*)userDataIn->data;

  outData->sumy += inData->sumy;
  outData->sumy2 += inData->sumy2;
  outData->cnt += inData->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_syy::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  struct regr_syy_data* data = (struct regr_syy_data*)context->getUserData()->data;
  long double N = data->cnt;
  if (N > 0)
  {
    long double var_popy = (data->sumy2 - (data->sumy * data->sumy / N));
    if (var_popy < 0)  // might be -0
      var_popy = 0;
    valOut = static_cast<double>(var_popy);
  }
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_syy::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  double valy = toDouble(valsDropped[0]);
  struct regr_syy_data* data = (struct regr_syy_data*)context->getUserData()->data;

  data->sumy -= valy;
  data->sumy2 -= valy * valy;

  --data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

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
#include "regr_avgx.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

class Add_regr_avgx_ToUDAFMap
{
 public:
  Add_regr_avgx_ToUDAFMap()
  {
    UDAFMap::getMap()["regr_avgx"] = new regr_avgx();
  }
};

static Add_regr_avgx_ToUDAFMap addToMap;

#define DATATYPE double

// Use the simple data model
struct regr_avgx_data
{
  long double sum;
  uint64_t cnt;
};

mcsv1_UDAF::ReturnCode regr_avgx::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() != 2)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_avgx() with other than 2 arguments");
    return mcsv1_UDAF::ERROR;
  }

  if (!(isNumeric(colTypes[1].dataType)))
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_avgx() with a non-numeric x argument");
    return mcsv1_UDAF::ERROR;
  }

  context->setUserDataSize(sizeof(regr_avgx_data));
  context->setResultType(execplan::CalpontSystemCatalog::DOUBLE);
  context->setColWidth(8);
  context->setScale(colTypes[1].scale + 4);
  context->setPrecision(19);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgx::reset(mcsv1Context* context)
{
  struct regr_avgx_data* data = (struct regr_avgx_data*)context->getUserData()->data;
  data->sum = 0;
  data->cnt = 0;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgx::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  DATATYPE val = toDouble(valsIn[1]);
  struct regr_avgx_data* data = (struct regr_avgx_data*)context->getUserData()->data;

  data->sum += val;
  ++data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgx::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (!userDataIn)
  {
    return mcsv1_UDAF::SUCCESS;
  }

  struct regr_avgx_data* outData = (struct regr_avgx_data*)context->getUserData()->data;

  struct regr_avgx_data* inData = (struct regr_avgx_data*)userDataIn->data;

  outData->sum += inData->sum;

  outData->cnt += inData->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgx::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  struct regr_avgx_data* data = (struct regr_avgx_data*)context->getUserData()->data;

  if (data->cnt > 0)
  {
    valOut = static_cast<double>(data->sum / (long double)data->cnt);
  }

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgx::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  double val = toDouble(valsDropped[1]);
  struct regr_avgx_data* data = (struct regr_avgx_data*)context->getUserData()->data;

  data->sum -= val;
  --data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

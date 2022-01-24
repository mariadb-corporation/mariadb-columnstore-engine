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
#include "regr_avgy.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

class Add_regr_avgy_ToUDAFMap
{
 public:
  Add_regr_avgy_ToUDAFMap()
  {
    UDAFMap::getMap()["regr_avgy"] = new regr_avgy();
  }
};

static Add_regr_avgy_ToUDAFMap addToMap;

#define DATATYPE double

// Use the simple data model
struct regr_avgy_data
{
  long double sum;
  uint64_t cnt;
};

mcsv1_UDAF::ReturnCode regr_avgy::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() != 2)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_avgy() with other than 2 arguments");
    return mcsv1_UDAF::ERROR;
  }

  if (!(isNumeric(colTypes[0].dataType)))
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_avgy() with a non-numeric y argument");
    return mcsv1_UDAF::ERROR;
  }

  context->setUserDataSize(sizeof(regr_avgy_data));
  context->setResultType(execplan::CalpontSystemCatalog::DOUBLE);
  context->setColWidth(8);
  context->setScale(colTypes[0].scale + 4);
  context->setPrecision(19);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgy::reset(mcsv1Context* context)
{
  struct regr_avgy_data* data = (struct regr_avgy_data*)context->getUserData()->data;
  data->sum = 0;
  data->cnt = 0;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgy::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  double val = toDouble(valsIn[0]);
  struct regr_avgy_data* data = (struct regr_avgy_data*)context->getUserData()->data;

  data->sum += val;
  ++data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgy::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (!userDataIn)
  {
    return mcsv1_UDAF::SUCCESS;
  }

  struct regr_avgy_data* outData = (struct regr_avgy_data*)context->getUserData()->data;
  struct regr_avgy_data* inData = (struct regr_avgy_data*)userDataIn->data;

  outData->sum += inData->sum;
  outData->cnt += inData->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgy::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  struct regr_avgy_data* data = (struct regr_avgy_data*)context->getUserData()->data;

  if (data->cnt > 0)
  {
    valOut = static_cast<double>(data->sum / (long double)data->cnt);
  }
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgy::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  double val = toDouble(valsDropped[0]);
  struct regr_avgy_data* data = (struct regr_avgy_data*)context->getUserData()->data;

  data->sum -= val;
  --data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

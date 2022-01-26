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
#include "avgx.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

#define DATATYPE double

// Use the simple data model
struct avgx_data
{
  double sum;
  uint64_t cnt;
};

mcsv1_UDAF::ReturnCode avgx::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() != 1)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("avgx() with other than 1 arguments");
    return mcsv1_UDAF::ERROR;
  }

  if (!(isNumeric(colTypes[0].dataType)))
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("avgx() with a non-numeric x argument");
    return mcsv1_UDAF::ERROR;
  }

  context->setUserDataSize(sizeof(avgx_data));
  context->setResultType(execplan::CalpontSystemCatalog::DOUBLE);
  context->setColWidth(8);
  context->setScale(colTypes[0].scale + 4);
  context->setPrecision(19);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode avgx::reset(mcsv1Context* context)
{
  struct avgx_data* data = (struct avgx_data*)context->getUserData()->data;
  data->sum = 0;
  data->cnt = 0;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode avgx::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  struct avgx_data* data = (struct avgx_data*)context->getUserData()->data;

  if (valsIn[0].columnData.empty())
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }

  DATATYPE val = toDouble(valsIn[0]);

  data->sum += val;
  ++data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode avgx::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (!userDataIn)
  {
    return mcsv1_UDAF::SUCCESS;
  }

  struct avgx_data* outData = (struct avgx_data*)context->getUserData()->data;

  struct avgx_data* inData = (struct avgx_data*)userDataIn->data;

  outData->sum += inData->sum;

  outData->cnt += inData->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode avgx::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  struct avgx_data* data = (struct avgx_data*)context->getUserData()->data;

  valOut = data->sum / (double)data->cnt;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode avgx::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  struct avgx_data* data = (struct avgx_data*)context->getUserData()->data;

  if (valsDropped[0].columnData.empty())
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }

  DATATYPE val = toDouble(valsDropped[0]);

  data->sum -= val;
  --data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

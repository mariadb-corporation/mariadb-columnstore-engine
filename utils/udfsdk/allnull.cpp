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

#include "allnull.h"

using namespace mcsv1sdk;

struct allnull_data
{
  uint64_t totalQuantity;
  uint64_t totalNulls;
};

#define OUT_TYPE int64_t
mcsv1_UDAF::ReturnCode allnull::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  context->setUserDataSize(sizeof(allnull_data));

  if (context->getParameterCount() < 1)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("allnull() with 0 arguments");
    return mcsv1_UDAF::ERROR;
  }

  context->setResultType(execplan::CalpontSystemCatalog::TINYINT);

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode allnull::reset(mcsv1Context* context)
{
  struct allnull_data* data = (struct allnull_data*)context->getUserData()->data;
  data->totalQuantity = 0;
  data->totalNulls = 0;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode allnull::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  struct allnull_data* data = (struct allnull_data*)context->getUserData()->data;

  for (size_t i = 0; i < context->getParameterCount(); i++)
  {
    data->totalQuantity++;

    if (context->isParamNull(0))
    {
      data->totalNulls++;
    }
  }

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode allnull::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  struct allnull_data* outData = (struct allnull_data*)context->getUserData()->data;
  struct allnull_data* inData = (struct allnull_data*)userDataIn->data;
  outData->totalQuantity += inData->totalQuantity;
  outData->totalNulls += inData->totalNulls;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode allnull::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  OUT_TYPE allNull;
  struct allnull_data* data = (struct allnull_data*)context->getUserData()->data;
  allNull = data->totalQuantity > 0 && data->totalNulls == data->totalQuantity;
  valOut = allNull;
  return mcsv1_UDAF::SUCCESS;
}

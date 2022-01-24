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
#include "regr_count.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

class Add_regr_count_ToUDAFMap
{
 public:
  Add_regr_count_ToUDAFMap()
  {
    UDAFMap::getMap()["regr_count"] = new regr_count();
  }
};

static Add_regr_count_ToUDAFMap addToMap;

// Use the simple data model
struct regr_count_data
{
  long long cnt;
};

mcsv1_UDAF::ReturnCode regr_count::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() != 2)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("regr_count() with other than 2 arguments");
    return mcsv1_UDAF::ERROR;
  }

  context->setUserDataSize(sizeof(regr_count_data));
  context->setResultType(execplan::CalpontSystemCatalog::BIGINT);
  context->setColWidth(8);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_count::reset(mcsv1Context* context)
{
  struct regr_count_data* data = (struct regr_count_data*)context->getUserData()->data;
  data->cnt = 0;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_count::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  static_any::any& valIn_y = valsIn[0].columnData;
  static_any::any& valIn_x = valsIn[1].columnData;
  struct regr_count_data* data = (struct regr_count_data*)context->getUserData()->data;

  if (context->isParamNull(0) || context->isParamNull(1))
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }
  if (valIn_x.empty() || valIn_y.empty())  // Usually empty if NULL. Probably redundant
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }
  ++data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_count::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (!userDataIn)
  {
    return mcsv1_UDAF::SUCCESS;
  }

  struct regr_count_data* outData = (struct regr_count_data*)context->getUserData()->data;
  struct regr_count_data* inData = (struct regr_count_data*)userDataIn->data;

  outData->cnt += inData->cnt;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_count::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  struct regr_count_data* data = (struct regr_count_data*)context->getUserData()->data;

  valOut = data->cnt;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_count::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  static_any::any& valIn_y = valsDropped[0].columnData;
  static_any::any& valIn_x = valsDropped[1].columnData;
  struct regr_count_data* data = (struct regr_count_data*)context->getUserData()->data;

  if (context->isParamNull(0) || context->isParamNull(1))
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }
  if (valIn_x.empty() || valIn_y.empty())
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }
  --data->cnt;

  return mcsv1_UDAF::SUCCESS;
}

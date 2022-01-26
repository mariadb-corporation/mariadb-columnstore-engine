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

#include "distinct_count.h"
#include "calpontsystemcatalog.h"

using namespace mcsv1sdk;

struct distinct_count_data
{
  long long cnt;
};

#define OUT_TYPE int64_t
mcsv1_UDAF::ReturnCode distinct_count::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  context->setUserDataSize(sizeof(distinct_count_data));
  if (context->getParameterCount() != 1)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("distinct_count() with other than 1 arguments");
    return mcsv1_UDAF::ERROR;
  }
  context->setResultType(execplan::CalpontSystemCatalog::BIGINT);
  context->setColWidth(8);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  context->setRunFlag(mcsv1sdk::UDAF_DISTINCT);
  context->setRunFlag(mcsv1sdk::UDAF_OVER_REQUIRED);

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode distinct_count::reset(mcsv1Context* context)
{
  struct distinct_count_data* data = (struct distinct_count_data*)context->getUserData()->data;
  data->cnt = 0;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode distinct_count::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  static_any::any& valIn = valsIn[0].columnData;
  struct distinct_count_data* data = (struct distinct_count_data*)context->getUserData()->data;

  if (valIn.empty())
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }
  data->cnt++;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode distinct_count::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  struct distinct_count_data* outData = (struct distinct_count_data*)context->getUserData()->data;
  struct distinct_count_data* inData = (struct distinct_count_data*)userDataIn->data;
  outData->cnt += inData->cnt;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode distinct_count::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  struct distinct_count_data* data = (struct distinct_count_data*)context->getUserData()->data;
  valOut = data->cnt;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode distinct_count::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  static_any::any& valIn = valsDropped[0].columnData;
  struct distinct_count_data* data = (struct distinct_count_data*)context->getUserData()->data;

  if (valIn.empty())
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }

  data->cnt--;

  return mcsv1_UDAF::SUCCESS;
}

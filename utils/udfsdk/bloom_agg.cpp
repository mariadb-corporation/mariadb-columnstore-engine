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
#include "bloom_agg.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

struct BloomAggData;

mcsv1_UDAF::ReturnCode bloom_agg::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  // if (context->getParameterCount() < 2)
  // {
  //   // The error message will be prepended with
  //   // "The storage engine for the table doesn't support "
  //   context->setErrorMessage("bloom_agg() with 0 arguments");
  //   return mcsv1_UDAF::ERROR;
  // }

  // if (context->getParameterCount() > 2)
  // {
  //   context->setErrorMessage("bloom_agg() with more than 1 argument");
  //   return mcsv1_UDAF::ERROR;
  // }

  // if (!(isNumeric(colTypes[0].dataType)))
  // {
  //   // The error message will be prepended with
  //   // "The storage engine for the table doesn't support "
  //   context->setErrorMessage("bloom_agg() with non-numeric argument");
  //   return mcsv1_UDAF::ERROR;
  // }

  // context->setUserDataSize(sizeof(BloomAggData));
  // context->setResultType(execplan::CalpontSystemCatalog::VARBINARY);
  // context->setColWidth(8);
  // context->setScale(context->getScale() * 2);
  // context->setPrecision(19);
  // context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

// TODO - implement reset
mcsv1_UDAF::ReturnCode bloom_agg::reset(mcsv1Context* context)
{
  // struct bloom_agg_data* data = (struct bloom_agg_data*)context->getUserData()->data;

  // if (data)
  // {
  //   data->scale = 0;
  //   data->sumsq = 0;
  // }

  return mcsv1_UDAF::SUCCESS;
}

// TODO - implement nextValue
mcsv1_UDAF::ReturnCode bloom_agg::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  // struct bloom_agg_data* data = (struct bloom_agg_data*)context->getUserData()->data;

  // if (context->isParamNull(0) || valsIn[0].columnData.empty())
  // {
  //   return mcsv1_UDAF::SUCCESS;
  // }

  // DATATYPE val = toDouble(valsIn[0]);

  // data->sumsq += val * val;
  return mcsv1_UDAF::SUCCESS;
}


// TODO - implement subEvaluate
mcsv1_UDAF::ReturnCode bloom_agg::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  // If we turn off UDAF_IGNORE_NULLS in init(), then NULLS may be sent here in cases of Joins.
  // When a NULL value is sent here, userDataIn will be NULL, so check for NULLS.
  // if (context->isParamNull(0))
  // {
  //   return mcsv1_UDAF::SUCCESS;
  // }

  // struct bloom_agg_data* outData = (struct bloom_agg_data*)context->getUserData()->data;

  // struct bloom_agg_data* inData = (struct bloom_agg_data*)userDataIn->data;

  // outData->sumsq += inData->sumsq;

  return mcsv1_UDAF::SUCCESS;
}

// TODO - implement evaluate
mcsv1_UDAF::ReturnCode bloom_agg::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  // struct bloom_agg_data* data = (struct bloom_agg_data*)context->getUserData()->data;
  // valOut = data->sumsq;
  return mcsv1_UDAF::SUCCESS;
}


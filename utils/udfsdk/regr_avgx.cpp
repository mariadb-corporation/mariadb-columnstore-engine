/* Copyright (C) 2017 MariaDB Corporaton

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

#define DATATYPE double

// Use the simple data model
struct regr_avgx_data
{
    double	    sum;
    uint64_t	cnt;
};


mcsv1_UDAF::ReturnCode regr_avgx::init(mcsv1Context* context,
                                       ColumnDatum* colTypes)
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
    context->setResultType(CalpontSystemCatalog::DOUBLE);
    context->setColWidth(8);
    context->setScale(colTypes[1].scale + 4);
    context->setPrecision(19);
    context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
    return mcsv1_UDAF::SUCCESS;

}

mcsv1_UDAF::ReturnCode regr_avgx::reset(mcsv1Context* context)
{
    struct  regr_avgx_data* data = (struct regr_avgx_data*)context->getUserData()->data;
    data->sum = 0;
    data->cnt = 0;
    return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgx::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
    static_any::any& valIn_y = valsIn[0].columnData;
    static_any::any& valIn_x = valsIn[1].columnData;
    struct  regr_avgx_data* data = (struct regr_avgx_data*)context->getUserData()->data;
    DATATYPE val = 0.0;

    if (context->isParamNull(0) || context->isParamNull(1))
    {
        return mcsv1_UDAF::SUCCESS; // Ought not happen when UDAF_IGNORE_NULLS is on.
    }

    if (valIn_x.empty() || valIn_y.empty()) // Usually empty if NULL. Probably redundant
    {
        return mcsv1_UDAF::SUCCESS; // Ought not happen when UDAF_IGNORE_NULLS is on.
    }

    if (valIn_x.compatible(longTypeId))
    {
        val = valIn_x.cast<long>();
    }
    else if (valIn_x.compatible(charTypeId))
    {
        val = valIn_x.cast<char>();
    }
    else if (valIn_x.compatible(scharTypeId))
    {
        val = valIn_x.cast<signed char>();
    }
    else if (valIn_x.compatible(shortTypeId))
    {
        val = valIn_x.cast<short>();
    }
    else if (valIn_x.compatible(intTypeId))
    {
        val = valIn_x.cast<int>();
    }
    else if (valIn_x.compatible(llTypeId))
    {
        val = valIn_x.cast<long long>();
    }
    else if (valIn_x.compatible(ucharTypeId))
    {
        val = valIn_x.cast<unsigned char>();
    }
    else if (valIn_x.compatible(ushortTypeId))
    {
        val = valIn_x.cast<unsigned short>();
    }
    else if (valIn_x.compatible(uintTypeId))
    {
        val = valIn_x.cast<unsigned int>();
    }
    else if (valIn_x.compatible(ulongTypeId))
    {
        val = valIn_x.cast<unsigned long>();
    }
    else if (valIn_x.compatible(ullTypeId))
    {
        val = valIn_x.cast<unsigned long long>();
    }
    else if (valIn_x.compatible(floatTypeId))
    {
        val = valIn_x.cast<float>();
    }
    else if (valIn_x.compatible(doubleTypeId))
    {
        val = valIn_x.cast<double>();
    }

    // For decimal types, we need to move the decimal point.
    uint32_t scale = valsIn[1].scale;

    if (val != 0 && scale > 0)
    {
        val /= pow(10.0, (double)scale);
    }

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

    if (data->cnt == 0)
    {
        valOut = 0;
    }
    else
    {
        valOut = data->sum / (double)data->cnt;
    }

    return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgx::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
    static_any::any& valIn_y = valsDropped[0].columnData;
    static_any::any& valIn_x = valsDropped[1].columnData;
    struct regr_avgx_data* data = (struct regr_avgx_data*)context->getUserData()->data;
    DATATYPE val = 0.0;

    if (valIn_x.empty() || valIn_y.empty())
    {
        return mcsv1_UDAF::SUCCESS; // Ought not happen when UDAF_IGNORE_NULLS is on.
    }

    if (valIn_x.compatible(charTypeId))
    {
        val = valIn_x.cast<char>();
    }
    else if (valIn_x.compatible(scharTypeId))
    {
        val = valIn_x.cast<signed char>();
    }
    else if (valIn_x.compatible(shortTypeId))
    {
        val = valIn_x.cast<short>();
    }
    else if (valIn_x.compatible(intTypeId))
    {
        val = valIn_x.cast<int>();
    }
    else if (valIn_x.compatible(longTypeId))
    {
        val = valIn_x.cast<long>();
    }
    else if (valIn_x.compatible(llTypeId))
    {
        val = valIn_x.cast<long long>();
    }
    else if (valIn_x.compatible(ucharTypeId))
    {
        val = valIn_x.cast<unsigned char>();
    }
    else if (valIn_x.compatible(ushortTypeId))
    {
        val = valIn_x.cast<unsigned short>();
    }
    else if (valIn_x.compatible(uintTypeId))
    {
        val = valIn_x.cast<unsigned int>();
    }
    else if (valIn_x.compatible(ulongTypeId))
    {
        val = valIn_x.cast<unsigned long>();
    }
    else if (valIn_x.compatible(ullTypeId))
    {
        val = valIn_x.cast<unsigned long long>();
    }
    else if (valIn_x.compatible(floatTypeId))
    {
        val = valIn_x.cast<float>();
    }
    else if (valIn_x.compatible(doubleTypeId))
    {
        val = valIn_x.cast<double>();
    }

    // For decimal types, we need to move the decimal point.
    uint32_t scale = valsDropped[1].scale;

    if (val != 0 && scale > 0)
    {
        val /= pow(10.0, (double)scale);
    }

    data->sum -= val;
    --data->cnt;

    return mcsv1_UDAF::SUCCESS;
}


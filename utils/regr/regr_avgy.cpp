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
    double	    sum;
    uint64_t	cnt;
};


mcsv1_UDAF::ReturnCode regr_avgy::init(mcsv1Context* context,
                                       ColumnDatum* colTypes)
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
        context->setErrorMessage("regr_avgy() with a non-numeric x argument");
        return mcsv1_UDAF::ERROR;
    }

    context->setUserDataSize(sizeof(regr_avgy_data));
    context->setResultType(CalpontSystemCatalog::DOUBLE);
    context->setColWidth(8);
    context->setScale(colTypes[0].scale + 4);
    context->setPrecision(19);
    context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
    return mcsv1_UDAF::SUCCESS;

}

mcsv1_UDAF::ReturnCode regr_avgy::reset(mcsv1Context* context)
{
    struct  regr_avgy_data* data = (struct regr_avgy_data*)context->getUserData()->data;
    data->sum = 0;
    data->cnt = 0;
    return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_avgy::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
    static_any::any& valIn_y = valsIn[0].columnData;
    static_any::any& valIn_x = valsIn[1].columnData;
    struct  regr_avgy_data* data = (struct regr_avgy_data*)context->getUserData()->data;
    DATATYPE val = 0.0;

    if (context->isParamNull(0) || context->isParamNull(1))
    {
        return mcsv1_UDAF::SUCCESS; // Ought not happen when UDAF_IGNORE_NULLS is on.
    }
    if (valIn_x.empty() || valIn_y.empty()) // Usually empty if NULL. Probably redundant
    {
        return mcsv1_UDAF::SUCCESS; // Ought not happen when UDAF_IGNORE_NULLS is on.
    }

    if (valIn_y.compatible(longTypeId))
    {
        val = valIn_y.cast<long>();
    }
    else if (valIn_y.compatible(charTypeId))
    {
        val = valIn_y.cast<char>();
    }
    else if (valIn_y.compatible(scharTypeId))
    {
        val = valIn_y.cast<signed char>();
    }
    else if (valIn_y.compatible(shortTypeId))
    {
        val = valIn_y.cast<short>();
    }
    else if (valIn_y.compatible(intTypeId))
    {
        val = valIn_y.cast<int>();
    }
    else if (valIn_y.compatible(llTypeId))
    {
        val = valIn_y.cast<long long>();
    }
    else if (valIn_y.compatible(ucharTypeId))
    {
        val = valIn_y.cast<unsigned char>();
    }
    else if (valIn_y.compatible(ushortTypeId))
    {
        val = valIn_y.cast<unsigned short>();
    }
    else if (valIn_y.compatible(uintTypeId))
    {
        val = valIn_y.cast<unsigned int>();
    }
    else if (valIn_y.compatible(ulongTypeId))
    {
        val = valIn_y.cast<unsigned long>();
    }
    else if (valIn_y.compatible(ullTypeId))
    {
        val = valIn_y.cast<unsigned long long>();
    }
    else if (valIn_y.compatible(floatTypeId))
    {
        val = valIn_y.cast<float>();
    }
    else if (valIn_y.compatible(doubleTypeId))
    {
        val = valIn_y.cast<double>();
    }

    // For decimal types, we need to move the decimal point.
    uint32_t scale = valsIn[0].scale;

    if (val != 0 && scale > 0)
    {
        val /= pow(10.0, (double)scale);
    }

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

mcsv1_UDAF::ReturnCode regr_avgy::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
    static_any::any& valIn_y = valsDropped[0].columnData;
    static_any::any& valIn_x = valsDropped[1].columnData;
    struct regr_avgy_data* data = (struct regr_avgy_data*)context->getUserData()->data;
    DATATYPE val = 0.0;

    if (context->isParamNull(0) || context->isParamNull(1))
    {
        return mcsv1_UDAF::SUCCESS; // Ought not happen when UDAF_IGNORE_NULLS is on.
    }
    if (valIn_x.empty() || valIn_y.empty())
    {
        return mcsv1_UDAF::SUCCESS; // Ought not happen when UDAF_IGNORE_NULLS is on.
    }

    if (valIn_y.compatible(charTypeId))
    {
        val = valIn_y.cast<char>();
    }
    else if (valIn_y.compatible(scharTypeId))
    {
        val = valIn_y.cast<signed char>();
    }
    else if (valIn_y.compatible(shortTypeId))
    {
        val = valIn_y.cast<short>();
    }
    else if (valIn_y.compatible(intTypeId))
    {
        val = valIn_y.cast<int>();
    }
    else if (valIn_y.compatible(longTypeId))
    {
        val = valIn_y.cast<long>();
    }
    else if (valIn_y.compatible(llTypeId))
    {
        val = valIn_y.cast<long long>();
    }
    else if (valIn_y.compatible(ucharTypeId))
    {
        val = valIn_y.cast<unsigned char>();
    }
    else if (valIn_y.compatible(ushortTypeId))
    {
        val = valIn_y.cast<unsigned short>();
    }
    else if (valIn_y.compatible(uintTypeId))
    {
        val = valIn_y.cast<unsigned int>();
    }
    else if (valIn_y.compatible(ulongTypeId))
    {
        val = valIn_y.cast<unsigned long>();
    }
    else if (valIn_y.compatible(ullTypeId))
    {
        val = valIn_y.cast<unsigned long long>();
    }
    else if (valIn_y.compatible(floatTypeId))
    {
        val = valIn_y.cast<float>();
    }
    else if (valIn_y.compatible(doubleTypeId))
    {
        val = valIn_y.cast<double>();
    }

    // For decimal types, we need to move the decimal point.
    uint32_t scale = valsDropped[0].scale;

    if (val != 0 && scale > 0)
    {
        val /= pow(10.0, (double)scale);
    }

    data->sum -= val;
    --data->cnt;

    return mcsv1_UDAF::SUCCESS;
}


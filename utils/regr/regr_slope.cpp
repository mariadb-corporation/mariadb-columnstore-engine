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
#include "regr_slope.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

class Add_regr_slope_ToUDAFMap
{
public:
    Add_regr_slope_ToUDAFMap()
    {
        UDAFMap::getMap()["regr_slope"] = new regr_slope();
    }
};

static Add_regr_slope_ToUDAFMap addToMap;

// Use the simple data model
struct regr_slope_data
{
    uint64_t	cnt;
    long double sumx;
    long double sumx2;  // sum of (x squared)
    long double sumy;
    long double sumxy;  // sum of x * y
};


mcsv1_UDAF::ReturnCode regr_slope::init(mcsv1Context* context,
                                       ColumnDatum* colTypes)
{
    if (context->getParameterCount() != 2)
    {
        // The error message will be prepended with
        // "The storage engine for the table doesn't support "
        context->setErrorMessage("regr_slope() with other than 2 arguments");
        return mcsv1_UDAF::ERROR;
    }
    if (!(isNumeric(colTypes[0].dataType) && isNumeric(colTypes[1].dataType)))
    {
        // The error message will be prepended with
        // "The storage engine for the table doesn't support "
        context->setErrorMessage("regr_slope() with non-numeric arguments");
        return mcsv1_UDAF::ERROR;
    }
    context->setUserDataSize(sizeof(regr_slope_data));
    context->setResultType(execplan::CalpontSystemCatalog::DOUBLE);
    context->setColWidth(8);
    context->setScale(DECIMAL_NOT_SPECIFIED);
    context->setPrecision(0);
    context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
    return mcsv1_UDAF::SUCCESS;

}

mcsv1_UDAF::ReturnCode regr_slope::reset(mcsv1Context* context)
{
    struct  regr_slope_data* data = (struct regr_slope_data*)context->getUserData()->data;
    data->cnt = 0;
    data->sumx = 0.0;
    data->sumx2 = 0.0;
    data->sumy = 0.0;
    data->sumxy = 0.0;
    return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_slope::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
    static_any::any& valIn_y = valsIn[0].columnData;
    static_any::any& valIn_x = valsIn[1].columnData;
    struct  regr_slope_data* data = (struct regr_slope_data*)context->getUserData()->data;
    double valx = 0.0;
    double valy = 0.0;

    valx = convertAnyTo<double>(valIn_x);
    valy = convertAnyTo<double>(valIn_y);

    // For decimal types, we need to move the decimal point.
    uint32_t scaley = valsIn[0].scale;

    if (valy != 0 && scaley > 0)
    {
        valy /= pow(10.0, (double)scaley);
    }

    data->sumy += valy;

    // For decimal types, we need to move the decimal point.
    uint32_t scalex = valsIn[1].scale;

    if (valx != 0 && scalex > 0)
    {
        valx /= pow(10.0, (double)scalex);
    }

    data->sumx += valx;
    data->sumx2 += valx*valx;

    data->sumxy += valx*valy;
    ++data->cnt;
    
    return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_slope::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
    if (!userDataIn)
    {
        return mcsv1_UDAF::SUCCESS;
    }

    struct regr_slope_data* outData = (struct regr_slope_data*)context->getUserData()->data;
    struct regr_slope_data* inData = (struct regr_slope_data*)userDataIn->data;

    outData->sumx += inData->sumx;
    outData->sumx2 += inData->sumx2;
    outData->sumy += inData->sumy;
    outData->sumxy += inData->sumxy;
    outData->cnt += inData->cnt;

    return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_slope::evaluate(mcsv1Context* context, static_any::any& valOut)
{
    struct regr_slope_data* data = (struct regr_slope_data*)context->getUserData()->data;
    double N = data->cnt;
    if (N > 1)
    {
        // COVAR_POP(y, x) / VAR_POP(x)
        long double sumx = data->sumx;
        long double sumy = data->sumy;
        long double sumx2 = data->sumx2;
        long double sumxy = data->sumxy;
        long double covar_pop = N * sumxy - sumx * sumy;
        long double var_pop = N * sumx2 - sumx * sumx;
        if (var_pop != 0)
        {
            long double slope = covar_pop / var_pop;
            valOut = static_cast<double>(slope);
        }
    }
    return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode regr_slope::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
    static_any::any& valIn_y = valsDropped[0].columnData;
    static_any::any& valIn_x = valsDropped[1].columnData;
    struct regr_slope_data* data = (struct regr_slope_data*)context->getUserData()->data;

    double valx = 0.0;
    double valy = 0.0;

    valx = convertAnyTo<double>(valIn_x);
    valy = convertAnyTo<double>(valIn_y);

    // For decimal types, we need to move the decimal point.
    uint32_t scaley = valsDropped[0].scale;

    if (valy != 0 && scaley > 0)
    {
        valy /= pow(10.0, (double)scaley);
    }

    data->sumy -= valy;

    // For decimal types, we need to move the decimal point.
    uint32_t scalex = valsDropped[1].scale;

    if (valx != 0 && scalex > 0)
    {
        valx /= pow(10.0, (double)scalex);
    }

    data->sumx -= valx;
    data->sumx2 -= valx*valx;

    data->sumxy -= valx*valy;
    --data->cnt;

    return mcsv1_UDAF::SUCCESS;
}


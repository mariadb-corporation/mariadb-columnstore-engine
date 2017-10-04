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
#include "ssq.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;
#define DATATYPE double

struct ssq_data
{
	uint64_t scale;
    DATATYPE	sumsq;
	ssq_data() : scale(0){}
};

#define OUT_TYPE int64_t
mcsv1_UDAF::ReturnCode ssq::init(mcsv1Context* context,
									 COL_TYPES& colTypes)
{
	if (colTypes.size() < 1)
	{
		// The error message will be prepended with
		// "The storage engine for the table doesn't support "
		context->setErrorMessage("ssq() with 0 arguments");
		return mcsv1_UDAF::ERROR;
	}
	if (colTypes.size() > 1)
	{
		context->setErrorMessage("ssq() with more than 1 argument");
		return mcsv1_UDAF::ERROR;
	}

	if (!(isNumeric(colTypes[0].second)))
	{
		// The error message will be prepended with
		// "The storage engine for the table doesn't support "
		context->setErrorMessage("ssq() with non-numeric argument");
		return mcsv1_UDAF::ERROR;
	}

	context->setUserDataSize(sizeof(ssq_data));
	context->setResultType(CalpontSystemCatalog::DOUBLE);
	context->setColWidth(8);
	context->setScale(context->getScale()*2);
	context->setPrecision(19);
	context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
	return mcsv1_UDAF::SUCCESS;

}

mcsv1_UDAF::ReturnCode ssq::reset(mcsv1Context* context)
{
	struct ssq_data* data = (struct ssq_data*)context->getUserData()->data;
	if (data)
	{
		data->scale = 0;
		data->sumsq = 0;
	}
	return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode ssq::nextValue(mcsv1Context* context, 
										  std::vector<ColumnDatum>& valsIn)
{
	static_any::any& valIn = valsIn[0].columnData;
	struct ssq_data* data = (struct ssq_data*)context->getUserData()->data;
	DATATYPE val = 0.0;

	if (context->isParamNull(0) || valIn.empty())
	{
		return mcsv1_UDAF::SUCCESS;
	}

	if (valIn.compatible(charTypeId))
	{
		val = valIn.cast<char>();
	}
	else if (valIn.compatible(scharTypeId))
	{
		val = valIn.cast<signed char>();
	}
	else if (valIn.compatible(shortTypeId))
	{
		val = valIn.cast<short>();
	}
	else if (valIn.compatible(intTypeId))
	{
		val = valIn.cast<int>();
	}
	else if (valIn.compatible(longTypeId))
	{
		val = valIn.cast<long>();
	}
	else if (valIn.compatible(llTypeId))
	{
		val = valIn.cast<long long>();
	}
	else if (valIn.compatible(ucharTypeId))
	{
		val = valIn.cast<unsigned char>();
	}
	else if (valIn.compatible(ushortTypeId))
	{
		val = valIn.cast<unsigned short>();
	}
	else if (valIn.compatible(uintTypeId))
	{
		val = valIn.cast<unsigned int>();
	}
	else if (valIn.compatible(ulongTypeId))
	{
		val = valIn.cast<unsigned long>();
	}
	else if (valIn.compatible(ullTypeId))
	{
		val = valIn.cast<unsigned long long>();
	}
	else if (valIn.compatible(floatTypeId))
	{
		val = valIn.cast<float>();
	}
	else if (valIn.compatible(doubleTypeId))
	{
		val = valIn.cast<double>();
	}

	// For decimal types, we need to move the decimal point.
	uint32_t scale = valsIn[0].scale;
	if (val != 0 && scale > 0)
	{
		val /= pow(10.0, (double)scale);
	}
	data->sumsq += val*val;
	return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode ssq::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
	// If we turn off UDAF_IGNORE_NULLS in init(), then NULLS may be sent here in cases of Joins. 
	// When a NULL value is sent here, userDataIn will be NULL, so check for NULLS.
	if (context->isParamNull(0))
	{
		return mcsv1_UDAF::SUCCESS;
	}
	struct ssq_data* outData = (struct ssq_data*)context->getUserData()->data;
	struct ssq_data* inData = (struct ssq_data*)userDataIn->data;
	outData->sumsq += inData->sumsq;
	return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode ssq::evaluate(mcsv1Context* context, static_any::any& valOut)
{
	struct ssq_data* data = (struct ssq_data*)context->getUserData()->data;
	valOut = data->sumsq;
	return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode ssq::dropValue(mcsv1Context* context, 
													std::vector<ColumnDatum>& valsDropped)
{
	static_any::any& valIn = valsDropped[0].columnData;
	struct ssq_data* data = (struct ssq_data*)context->getUserData()->data;
	DATATYPE val = 0.0;

	if (valIn.empty())
	{
		return mcsv1_UDAF::SUCCESS; // Ought not happen when UDAF_IGNORE_NULLS is on.
	}

	if (valIn.compatible(charTypeId))
	{
		val = valIn.cast<char>();
	}
	else if (valIn.compatible(scharTypeId))
	{
		val = valIn.cast<signed char>();
	}
	else if (valIn.compatible(shortTypeId))
	{
		val = valIn.cast<short>();
	}
	else if (valIn.compatible(intTypeId))
	{
		val = valIn.cast<int>();
	}
	else if (valIn.compatible(longTypeId))
	{
		val = valIn.cast<long>();
	}
	else if (valIn.compatible(llTypeId))
	{
		val = valIn.cast<long long>();
	}
	else if (valIn.compatible(ucharTypeId))
	{
		val = valIn.cast<unsigned char>();
	}
	else if (valIn.compatible(ushortTypeId))
	{
		val = valIn.cast<unsigned short>();
	}
	else if (valIn.compatible(uintTypeId))
	{
		val = valIn.cast<unsigned int>();
	}
	else if (valIn.compatible(ulongTypeId))
	{
		val = valIn.cast<unsigned long>();
	}
	else if (valIn.compatible(ullTypeId))
	{
		val = valIn.cast<unsigned long long>();
	}
	else if (valIn.compatible(floatTypeId))
	{
		val = valIn.cast<float>();
	}
	else if (valIn.compatible(doubleTypeId))
	{
		val = valIn.cast<double>();
	}

	// For decimal types, we need to move the decimal point.
	uint32_t scale = valsDropped[0].scale;
	if (val != 0 && scale > 0)
	{
		val /= pow(10.0, (double)scale);
	}
	data->sumsq -= val*val;
	return mcsv1_UDAF::SUCCESS;
}


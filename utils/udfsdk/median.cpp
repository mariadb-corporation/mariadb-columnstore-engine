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
#include "median.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

mcsv1_UDAF::ReturnCode median::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() < 1)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("median() with 0 arguments");
    return mcsv1_UDAF::ERROR;
  }

  if (context->getParameterCount() > 1)
  {
    context->setErrorMessage("median() with more than 1 argument");
    return mcsv1_UDAF::ERROR;
  }

  if (!(isNumeric(colTypes[0].dataType)))
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("median() with non-numeric argument");
    return mcsv1_UDAF::ERROR;
  }

  context->setResultType(CalpontSystemCatalog::DOUBLE);
  context->setColWidth(8);
  context->setScale(context->getScale() * 2);
  context->setPrecision(19);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode median::reset(mcsv1Context* context)
{
  MedianData* data = static_cast<MedianData*>(context->getUserData());
  data->mData.clear();
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode median::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  static_any::any& valIn = valsIn[0].columnData;
  MEDIAN_DATA& data = static_cast<MedianData*>(context->getUserData())->mData;

  if (valIn.empty())
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }

  DATATYPE val = convertAnyTo<double>(valIn);

  // For decimal types, we need to move the decimal point.
  uint32_t scale = valsIn[0].scale;

  if (val != 0 && scale > 0)
  {
    val /= pow(10.0, (double)scale);
  }

  data[val]++;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode median::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (!userDataIn)
  {
    return mcsv1_UDAF::SUCCESS;
  }

  MEDIAN_DATA& outData = static_cast<MedianData*>(context->getUserData())->mData;
  const MEDIAN_DATA& inData = static_cast<const MedianData*>(userDataIn)->mData;
  MEDIAN_DATA::const_iterator iter = inData.begin();

  for (; iter != inData.end(); ++iter)
  {
    outData[iter->first] += iter->second;
  }

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode median::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  uint64_t cnt1 = 0, cnt2 = 0;
  MEDIAN_DATA& data = static_cast<MedianData*>(context->getUserData())->mData;

  if (data.size() == 0)
  {
    valOut = (DATATYPE)0;
    return mcsv1_UDAF::SUCCESS;
  }

  MEDIAN_DATA::iterator iter(data.begin());
  MEDIAN_DATA::iterator revfrom(data.end());
  MEDIAN_DATA::reverse_iterator riter(revfrom);
  cnt1 += iter->second;
  cnt2 += riter->second;

  while (iter->first < riter->first)
  {
    while (cnt1 < cnt2 && iter->first < riter->first)
    {
      ++iter;
      cnt1 += iter->second;
    }

    while (cnt2 < cnt1 && iter->first < riter->first)
    {
      ++riter;
      cnt2 += riter->second;
    }

    while (cnt1 == cnt2 && iter->first < riter->first)
    {
      ++iter;
      cnt1 += iter->second;

      if (iter->first > riter->first)
      {
        break;
      }

      ++riter;
      cnt2 += riter->second;
    }
  }

  valOut = (iter->first + riter->first) / 2;
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode median::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  static_any::any& valIn = valsDropped[0].columnData;
  MEDIAN_DATA& data = static_cast<MedianData*>(context->getUserData())->mData;

  if (valIn.empty())
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }

  DATATYPE val = convertAnyTo<double>(valIn);

  // For decimal types, we need to move the decimal point.
  uint32_t scale = valsDropped[0].scale;

  if (val != 0 && scale > 0)
  {
    val /= pow(10.0, (double)scale);
  }

  data[val]--;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode median::createUserData(UserData*& userData, int32_t& length)
{
  userData = new MedianData;
  length = sizeof(MedianData);
  return mcsv1_UDAF::SUCCESS;
}

void MedianData::serialize(messageqcpp::ByteStream& bs) const
{
  MEDIAN_DATA::const_iterator iter = mData.begin();
  DATATYPE num;
  uint32_t cnt;
  bs << (int32_t)mData.size();

  for (; iter != mData.end(); ++iter)
  {
    num = iter->first;
    bs << num;
    cnt = iter->second;
    bs << cnt;
  }
}

void MedianData::unserialize(messageqcpp::ByteStream& bs)
{
  mData.clear();
  int32_t sz;
  DATATYPE num;
  uint32_t cnt;
  bs >> sz;

  for (int i = 0; i < sz; ++i)
  {
    bs >> num;
    bs >> cnt;
    mData[num] = cnt;
  }
}

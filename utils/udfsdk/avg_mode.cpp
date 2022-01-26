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
#include "avg_mode.h"
#include "bytestream.h"
#include "objectreader.h"

using namespace mcsv1sdk;

mcsv1_UDAF::ReturnCode avg_mode::init(mcsv1Context* context, ColumnDatum* colTypes)
{
  if (context->getParameterCount() < 1)
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("avg_mode() with 0 arguments");
    return mcsv1_UDAF::ERROR;
  }

  if (context->getParameterCount() > 1)
  {
    context->setErrorMessage("avg_mode() with more than 1 argument");
    return mcsv1_UDAF::ERROR;
  }

  if (!(isNumeric(colTypes[0].dataType)))
  {
    // The error message will be prepended with
    // "The storage engine for the table doesn't support "
    context->setErrorMessage("avg_mode() with non-numeric argument");
    return mcsv1_UDAF::ERROR;
  }

  context->setResultType(execplan::CalpontSystemCatalog::DOUBLE);
  context->setColWidth(8);
  context->setScale(context->getScale() * 2);
  context->setPrecision(19);
  context->setRunFlag(mcsv1sdk::UDAF_IGNORE_NULLS);
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode avg_mode::reset(mcsv1Context* context)
{
  ModeData* data = static_cast<ModeData*>(context->getUserData());
  data->mData.clear();
  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode avg_mode::nextValue(mcsv1Context* context, ColumnDatum* valsIn)
{
  MODE_DATA& data = static_cast<ModeData*>(context->getUserData())->mData;

  if (valsIn[0].columnData.empty())
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }

  DATATYPE val = toDouble(valsIn[0]);

  data[val]++;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode avg_mode::subEvaluate(mcsv1Context* context, const UserData* userDataIn)
{
  if (!userDataIn)
  {
    return mcsv1_UDAF::SUCCESS;
  }

  MODE_DATA& outData = static_cast<ModeData*>(context->getUserData())->mData;
  const MODE_DATA& inData = static_cast<const ModeData*>(userDataIn)->mData;
  MODE_DATA::const_iterator iter = inData.begin();

  for (; iter != inData.end(); ++iter)
  {
    outData[iter->first] += iter->second;
  }

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode avg_mode::evaluate(mcsv1Context* context, static_any::any& valOut)
{
  uint64_t maxCnt = 0;
  MODE_DATA& data = static_cast<ModeData*>(context->getUserData())->mData;

  if (data.size() == 0)
  {
    valOut = (DATATYPE)0;
    return mcsv1_UDAF::SUCCESS;
  }

  MODE_DATA::iterator iter(data.begin());

  for (; iter != data.end(); ++iter)
  {
    if (iter->second > maxCnt)
    {
      valOut = iter->first;
      maxCnt = iter->second;
    }
  }

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode avg_mode::dropValue(mcsv1Context* context, ColumnDatum* valsDropped)
{
  MODE_DATA& data = static_cast<ModeData*>(context->getUserData())->mData;

  if (valsDropped[0].columnData.empty())
  {
    return mcsv1_UDAF::SUCCESS;  // Ought not happen when UDAF_IGNORE_NULLS is on.
  }

  DATATYPE val = toDouble(valsDropped[0]);

  data[val]--;

  return mcsv1_UDAF::SUCCESS;
}

mcsv1_UDAF::ReturnCode avg_mode::createUserData(UserData*& userData, int32_t& length)
{
  userData = new ModeData;
  length = sizeof(ModeData);
  return mcsv1_UDAF::SUCCESS;
}

void ModeData::serialize(messageqcpp::ByteStream& bs) const
{
  MODE_DATA::const_iterator iter = mData.begin();
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

void ModeData::unserialize(messageqcpp::ByteStream& bs)
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

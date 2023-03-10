/* Copyright (C) 2014 InfiniDB, Inc.

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

/***********************************************************************
 *   $Id: dmlcolumn.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/

#define DMLPKGCOLUMN_DLLEXPORT
#include "dmlcolumn.h"
#undef DMLPKGCOLUMN_DLLEXPORT


namespace dmlpackage
{
DMLColumn::DMLColumn()
{
}

DMLColumn::DMLColumn(std::string name, utils::NullString& value, bool isFromCol,
                     uint32_t funcScale, bool isNULL)
{
  fName = name;
  fColValuesList.push_back(value);
  fisNULL = isNULL || value.isNull() || (strcasecmp(value.str(), "NULL") == 0);
  fIsFromCol = isFromCol;
  fFuncScale = funcScale;
}

DMLColumn::DMLColumn(std::string name, const std::vector<utils::NullString>& valueList, bool isFromCol,
                     uint32_t funcScale, bool isNULL)
{
  fName = name;
  fColValuesList = valueList;
  fisNULL = isNULL;
  fIsFromCol = isFromCol;
  fFuncScale = funcScale;
}

DMLColumn::~DMLColumn()
{
}

int DMLColumn::read(messageqcpp::ByteStream& bytestream)
{
  int retval = 1;
  bytestream >> fName;
  bytestream >> reinterpret_cast<messageqcpp::ByteStream::byte&>(fisNULL);
  uint32_t vectorSize;
  bytestream >> vectorSize;

  for (uint32_t i = 0; i < vectorSize; i++)
  {
    utils::NullString dataStr;
    bytestream >> dataStr;
    //	if ( !fisNULL  && (dataStr.length() == 0 ))
    //		dataStr = (char) 0;

    fColValuesList.push_back(dataStr);
  }


  bytestream >> reinterpret_cast<messageqcpp::ByteStream::byte&>(fIsFromCol);
  bytestream >> (uint32_t&)fFuncScale;
  return retval;
}

int DMLColumn::write(messageqcpp::ByteStream& bytestream)
{
  int retval = 1;
  bytestream << fName;
  bytestream << static_cast<uint8_t>(fisNULL);
  uint32_t vectorSize = fColValuesList.size();
  bytestream << vectorSize;

  for (uint32_t i = 0; i < vectorSize; i++)
  {
    bytestream << fColValuesList[i];
  }

  bytestream << static_cast<uint8_t>(fIsFromCol);
  bytestream << (uint32_t)fFuncScale;
  return retval;
}

}  // namespace dmlpackage

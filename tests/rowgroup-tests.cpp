/* Copyright (C) 2020 MariaDB Corporation

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

#include <gtest/gtest.h>  // googletest header file
#include <iostream>

#include "rowgroup.h"
#include "columnwidth.h"
#include "joblisttypes.h"
#include "dataconvert.h"

#define WIDE_DEC_PRECISION 38U
#define INITIAL_ROW_OFFSET 2

using CSCDataType = execplan::CalpontSystemCatalog::ColDataType;

class RowDecimalTest : public ::testing::Test
{
 protected:
  void SetUp() override
  {
    uint32_t precision = WIDE_DEC_PRECISION;
    uint32_t oid = 3001;

    std::vector<CSCDataType> types;
    std::vector<decltype(precision)> precisionVec;
    std::vector<uint32_t> roids, tkeys, cscale;
    std::vector<uint32_t> widthVec;
    std::vector<uint32_t> charSetNumVec;
    types.push_back(execplan::CalpontSystemCatalog::DECIMAL);
    types.push_back(execplan::CalpontSystemCatalog::UDECIMAL);

    for (size_t i = 0; i <= 3; i++)
      types.push_back(execplan::CalpontSystemCatalog::DECIMAL);

    precisionVec.push_back(precision);
    precisionVec.push_back(precision);
    precisionVec.push_back(18);
    precisionVec.push_back(9);
    precisionVec.push_back(4);
    precisionVec.push_back(2);

    uint32_t offset = INITIAL_ROW_OFFSET;
    offsets.push_back(offset);

    for (size_t i = 0; i < types.size(); i++)
    {
      uint8_t width = utils::widthByPrecision(precisionVec[i]);
      widthVec.push_back(width);
      offset += width;
      offsets.push_back(offset);
      roids.push_back(oid + i);
      tkeys.push_back(i + 1);
      cscale.push_back(0);
      charSetNumVec.push_back(8);
    }

    rowgroup::RowGroup inRG(roids.size(),   // column count
                            offsets,        // oldOffset
                            roids,          // column oids
                            tkeys,          // keys
                            types,          // types
                            charSetNumVec,  // charset numbers
                            cscale,         // scale
                            precisionVec,   // precision
                            20,             // sTableThreshold
                            false           // useStringTable
    );

    rg = rgOut = inRG;
    rgD.reinit(rg);
    rgDOut.reinit(rgOut);
    rg.setData(&rgD);
    rgOut.setData(&rgDOut);

    rg.initRow(&r);
    rg.initRow(&rOutMappingCheck);
    rowSize = r.getSize();
    rg.getRow(0, &r);

    int128_t nullValue = 0;
    int128_t bigValue = 0;

    datatypes::Decimal::setWideDecimalNullValue(nullValue);
    bigValue = -static_cast<int128_t>(0xFFFFFFFF) * 0xFFFFFFFFFFFFFFFF;

    sValueVector.push_back(nullValue);
    sValueVector.push_back(-42);
    sValueVector.push_back(bigValue);
    sValueVector.push_back(0);
    sValueVector.push_back(nullValue - 1);

    anotherValueVector.push_back(nullValue);
    anotherValueVector.push_back(42);
    anotherValueVector.push_back(bigValue);
    anotherValueVector.push_back(0);
    anotherValueVector.push_back(nullValue - 1);
    s8ValueVector.push_back(joblist::TINYINTNULL);
    s8ValueVector.push_back(-0x79);
    s8ValueVector.push_back(0);
    s8ValueVector.push_back(0x81);
    s8ValueVector.push_back(joblist::TINYINTNULL - 1);

    s16ValueVector.push_back(joblist::SMALLINTNULL);
    s16ValueVector.push_back(-0x79);
    s16ValueVector.push_back(0);
    s16ValueVector.push_back(0x81);
    s16ValueVector.push_back(joblist::SMALLINTNULL - 1);

    s32ValueVector.push_back(joblist::INTNULL);
    s32ValueVector.push_back(-0x79);
    s32ValueVector.push_back(0);
    s32ValueVector.push_back(0x81);
    s32ValueVector.push_back(joblist::INTNULL - 1);

    s64ValueVector.push_back(joblist::BIGINTNULL);
    s64ValueVector.push_back(-0x79);
    s64ValueVector.push_back(0);
    s64ValueVector.push_back(0x81);
    s64ValueVector.push_back(joblist::BIGINTNULL - 1);

    for (size_t i = 0; i < sValueVector.size(); i++)
    {
      r.setBinaryField_offset(&sValueVector[i], sizeof(sValueVector[0]), offsets[0]);
      r.setBinaryField_offset(&anotherValueVector[i], sizeof(anotherValueVector[0]), offsets[1]);
      r.setIntField(s64ValueVector[i], 2);
      r.setIntField(s32ValueVector[i], 3);
      r.setIntField(s16ValueVector[i], 4);
      r.setIntField(s8ValueVector[i], 5);
      r.nextRow(rowSize);
    }

    rowCount = sValueVector.size();
  }

  // void TearDown() override {}

  rowgroup::Row r, rOut, sameRow, nonequiRow;
  rowgroup::Row rOutMappingCheck;
  rowgroup::RowGroup rg, rgOut;
  rowgroup::RGData rgD, rgDOut;
  uint32_t rowSize;
  size_t rowCount;
  std::vector<int128_t> sValueVector;
  std::vector<int128_t> anotherValueVector;
  std::vector<int64_t> s8ValueVector;
  std::vector<int64_t> s16ValueVector;
  std::vector<int64_t> s32ValueVector;
  std::vector<int64_t> s64ValueVector;
  std::vector<uint32_t> offsets;
};

TEST_F(RowDecimalTest, NonNullValueCheck)
{
  rg.getRow(1, &r);

  for (size_t i = 1; i <= sValueVector.size(); i++)
  {
    EXPECT_FALSE(r.isNullValue(0));
    EXPECT_FALSE(r.isNullValue(1));
    EXPECT_FALSE(r.isNullValue(2));
    EXPECT_FALSE(r.isNullValue(3));
    EXPECT_FALSE(r.isNullValue(4));
    EXPECT_FALSE(r.isNullValue(5));
    r.nextRow(rowSize);
  }
}

TEST_F(RowDecimalTest, InitToNullAndIsNullValueCheck)
{
  rg.getRow(0, &r);
  r.initToNull();

  EXPECT_TRUE(r.isNullValue(0));
  EXPECT_TRUE(r.isNullValue(1));
  EXPECT_TRUE(r.isNullValue(2));
  EXPECT_TRUE(r.isNullValue(3));
  EXPECT_TRUE(r.isNullValue(4));
  EXPECT_TRUE(r.isNullValue(5));
}

TEST_F(RowDecimalTest, GetBinaryFieldCheck)
{
  rg.getRow(0, &r);
  int128_t* a128Value;
  int128_t* s128Value;

  for (size_t i = 0; i < sValueVector.size(); i++)
  {
    s128Value = r.getBinaryField<int128_t>(0);
    EXPECT_EQ(sValueVector[i], *s128Value);
    a128Value = r.getBinaryField<int128_t>(1);
    EXPECT_EQ(anotherValueVector[i], *a128Value);
    // EXPECT_EQ(s64ValueVector[i], r.getIntField(2));
    // EXPECT_EQ(s32ValueVector[i],r.getIntField(3));
    // EXPECT_EQ(r.getIntField(4),s16ValueVector[i]);
    // EXPECT_EQ(r.getIntField(5),s8ValueVector[i]);
    r.nextRow(rowSize);
  }
}

TEST_F(RowDecimalTest, ToStringCheck)
{
  std::vector<std::string> exemplarVector;

  exemplarVector.push_back(std::string("[    0]: 0: NULL NULL NULL NULL NULL NULL "));
  exemplarVector.push_back(std::string("[    0]: 0: -42 42 -121 -121 -121 -121 "));
  exemplarVector.push_back(
      std::string("[    0]: 0: -79228162495817593515539431425 -79228162495817593515539431425 0 0 0 0 "));
  exemplarVector.push_back(std::string("[    0]: 0: 0 0 129 129 129 -127 "));
  exemplarVector.push_back(
      std::string("[    0]: 0: 170141183460469231731687303715884105727 "
                  "170141183460469231731687303715884105727 9223372036854775807 2147483647 32767 127 "));

  rg.getRow(0, &r);
  r.initToNull();

  for (auto& el : exemplarVector)
  {
    EXPECT_EQ(el, r.toString());
    r.nextRow(rowSize);
  }
}

TEST_F(RowDecimalTest, ToCSVCheck)
{
}

TEST_F(RowDecimalTest, ApplyMappingCheck)
{
  int mapping[] = {0, 1, -1, -1, -1, -1};
  rg.getRow(1, &r);
  rg.getRow(2, &rOutMappingCheck);
  int128_t* s128Value = rOutMappingCheck.getBinaryField<int128_t>(0);
  int128_t* a128Value = rOutMappingCheck.getBinaryField<int128_t>(1);
  EXPECT_NE(sValueVector[1], *s128Value);
  EXPECT_NE(anotherValueVector[1], *a128Value);
  applyMapping(mapping, r, &rOutMappingCheck);
  s128Value = rOutMappingCheck.getBinaryField<int128_t>(0);
  a128Value = rOutMappingCheck.getBinaryField<int128_t>(1);
  EXPECT_EQ(sValueVector[1], *s128Value);
  EXPECT_EQ(anotherValueVector[1], *a128Value);
}

TEST_F(RowDecimalTest, CopyBinaryFieldCheck)
{
  int128_t constVal = 1;
  int128_t *col1Out, *col1In;
  int128_t *col2Out, *col2In;
  rgOut.getRow(0, &rOut);

  for (size_t i = 0; i < sValueVector.size(); i++)
  {
    rOut.setBinaryField_offset(&constVal, 16, offsets[0]);
    rOut.setBinaryField_offset(&constVal, 16, offsets[1]);
    rOut.nextRow(rowSize);
  }

  rgOut.initRow(&rOut);
  rgOut.getRow(0, &rOut);
  rg.getRow(0, &r);

  for (size_t i = 0; i < sValueVector.size(); i++)
  {
    col1In = r.getBinaryField<int128_t>(0);
    col1Out = rOut.getBinaryField<int128_t>(0);
    col2In = r.getBinaryField<int128_t>(1);
    col2Out = rOut.getBinaryField<int128_t>(1);
    EXPECT_NE(*col1In, *col1Out);
    EXPECT_NE(*col2In, *col2Out);
    r.copyBinaryField<int128_t>(rOut, 0, 0);
    r.copyBinaryField<int128_t>(rOut, 1, 1);
    col1Out = rOut.getBinaryField<int128_t>(0);
    col2Out = rOut.getBinaryField<int128_t>(1);
    EXPECT_EQ(*col1In, *col1Out);
    EXPECT_EQ(*col2In, *col2Out);
    r.nextRow(rowSize);
    rOut.nextRow(rowSize);
  }
}

TEST_F(RowDecimalTest, RowEqualsCheck)
{
  rg.getRow(0, &r);
  rg.getRow(0, &sameRow);
  rg.getRow(1, &nonequiRow);

  for (size_t i = 0; i < sValueVector.size(); i++)
  {
    EXPECT_TRUE(r.equals(sameRow));
    if (i < sValueVector.size() - 1)
    {
      EXPECT_FALSE(r.equals(nonequiRow));
    }
    r.nextRow(rowSize);
    sameRow.nextRow(rowSize);
    if (i < sValueVector.size() - 1)
    {
      nonequiRow.nextRow(rowSize);
    }
  }
}

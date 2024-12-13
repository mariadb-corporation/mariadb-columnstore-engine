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
#include <cstring>
#include <iostream>
#include <numeric>

#include "countingallocator.h"
#include "rowgroup.h"
#include "columnwidth.h"
#include "joblisttypes.h"
#include "dataconvert.h"

#define WIDE_DEC_PRECISION 38U
#define INITIAL_ROW_OFFSET 2

using CSCDataType = execplan::CalpontSystemCatalog::ColDataType;
using datatypes::TSInt128;
using RGFieldsType = std::vector<uint32_t>;

rowgroup::RowGroup setupRG(const std::vector<execplan::CalpontSystemCatalog::ColDataType>& cts,
                           const RGFieldsType& widths, const RGFieldsType& charsets)
{
  std::vector<execplan::CalpontSystemCatalog::ColDataType> types = cts;
  RGFieldsType offsets{2};
  for (auto w : widths)
  {
    offsets.push_back(offsets.back() + w);
  }
  RGFieldsType roids(widths.size());
  std::iota(roids.begin(), roids.end(), 3000);
  RGFieldsType tkeys(widths.size());
  std::fill(tkeys.begin(), tkeys.end(), 1);
  RGFieldsType cscale(widths.size());
  std::fill(cscale.begin(), cscale.end(), 0);
  RGFieldsType precision(widths.size());
  std::fill(precision.begin(), precision.end(), 20);
  return rowgroup::RowGroup(roids.size(),  // column count
                            offsets,       // oldOffset
                            roids,         // column oids
                            tkeys,         // keys
                            types,         // types
                            charsets,      // charset numbers
                            cscale,        // scale
                            precision,     // precision
                            20,            // sTableThreshold
                            true          // useStringTable
  );
}

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
    rg.initRow(&rOut);
    rg.initRow(&sameRow);
    rg.initRow(&nonequiRow);
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
      r.nextRow();
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
    r.nextRow();
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
  TSInt128 a128Value;
  TSInt128 s128Value;

  for (size_t i = 0; i < sValueVector.size(); i++)
  {
    s128Value = r.getTSInt128Field(0);
    EXPECT_EQ(sValueVector[i], s128Value.getValue());
    a128Value = r.getTSInt128Field(1);
    EXPECT_EQ(anotherValueVector[i], a128Value.getValue());
    // EXPECT_EQ(s64ValueVector[i], r.getIntField(2));
    // EXPECT_EQ(s32ValueVector[i],r.getIntField(3));
    // EXPECT_EQ(r.getIntField(4),s16ValueVector[i]);
    // EXPECT_EQ(r.getIntField(5),s8ValueVector[i]);
    r.nextRow();
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
    r.nextRow();
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

  auto s128Value = rOutMappingCheck.getTSInt128Field(0);
  auto a128Value = rOutMappingCheck.getTSInt128Field(1);
  EXPECT_NE(s128Value.getValue(), sValueVector[1]);
  EXPECT_NE(a128Value.getValue(), anotherValueVector[1]);

  applyMapping(mapping, r, &rOutMappingCheck);
  s128Value = rOutMappingCheck.getTSInt128Field(0);
  a128Value = rOutMappingCheck.getTSInt128Field(1);
  EXPECT_EQ(sValueVector[1], s128Value.getValue());
  EXPECT_EQ(anotherValueVector[1], a128Value.getValue());
}

TEST_F(RowDecimalTest, CopyBinaryFieldCheck)
{
  int128_t constVal = 1;
  TSInt128 col1Out, col1In;
  TSInt128 col2Out, col2In;
  rgOut.getRow(0, &rOut);

  for (size_t i = 0; i < sValueVector.size(); i++)
  {
    rOut.setBinaryField_offset(&constVal, 16, offsets[0]);
    rOut.setBinaryField_offset(&constVal, 16, offsets[1]);
    rOut.nextRow();
  }

  rgOut.initRow(&rOut);
  rgOut.getRow(0, &rOut);
  rg.getRow(0, &r);

  for (size_t i = 0; i < sValueVector.size(); i++)
  {
    col1In = r.getTSInt128Field(0);
    col1Out = rOut.getTSInt128Field(0);
    col2In = r.getTSInt128Field(1);
    col2Out = rOut.getTSInt128Field(1);
    EXPECT_NE(col1In, col1Out);
    EXPECT_NE(col2In, col2Out);
    r.copyBinaryField(rOut, 0, 0);
    r.copyBinaryField(rOut, 1, 1);
    col1Out = rOut.getTSInt128Field(0);
    col2Out = rOut.getTSInt128Field(1);
    EXPECT_EQ(col1In.getValue(), col1Out.getValue());
    EXPECT_EQ(col2In.getValue(), col2Out.getValue());
    r.nextRow();
    rOut.nextRow();
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
    r.nextRow();
    sameRow.nextRow();
    if (i < sValueVector.size() - 1)
    {
      nonequiRow.nextRow();
    }
  }
}

static const constexpr int64_t MemoryAllowance = 10 * 1024 * 1024;

class RGDataTest : public ::testing::Test
{
 protected:
  RGDataTest()
    : allocatedMemory(MemoryAllowance), alloc(&allocatedMemory, MemoryAllowance / 100) {}
  void SetUp() override
  {
    rg = setupRG({execplan::CalpontSystemCatalog::VARCHAR, execplan::CalpontSystemCatalog::UDECIMAL,
                         execplan::CalpontSystemCatalog::DECIMAL, execplan::CalpontSystemCatalog::DECIMAL,
                         execplan::CalpontSystemCatalog::DECIMAL, execplan::CalpontSystemCatalog::DECIMAL},
                        {65536, 16, 8, 4, 2, 1}, {8, 8, 8, 8, 8, 8});
  }

  // void TearDown() override {}

  rowgroup::Row r;
  rowgroup::RowGroup rg;
  rowgroup::RGData rgD;
  std::atomic<int64_t> allocatedMemory{MemoryAllowance};
  allocators::CountingAllocator<rowgroup::RGDataBufType> alloc;
};
  // bool useStringTable = true;
TEST_F(RGDataTest, AllocData)
{
    std::cout << " test  allocatedMemery " << allocatedMemory.load() << " rowsize " << rg.getRowSize() << " " << rg.getMaxDataSize() << std::endl;
    rgD = rowgroup::RGData(rg, alloc);
    rg.setData(&rgD);
    rg.initRow(&r);
    rg.getRow(0, &r);
    std::cout << " test inStringTable(colIndex) " << r.inStringTable(0) << std::endl;

    std::cout << " test  allocatedMemery " << allocatedMemory.load() << std::endl;
    auto currentAllocation = allocatedMemory.load();
    EXPECT_LE(currentAllocation, MemoryAllowance - rg.getMaxDataSize());

    r.setStringField(utils::ConstString{"testaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, 0);
    std::cout << " test  allocatedMemery " << allocatedMemory.load() << std::endl;
    std::cout << " test  inStringTable " << r.getColumnWidth(0) << std::endl;
    EXPECT_LE(allocatedMemory.load(), currentAllocation);

    currentAllocation = allocatedMemory.load();
    r.nextRow();
    r.setStringField(utils::ConstString{"testaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, 0);
    std::cout << " test  allocatedMemery " << allocatedMemory.load() << std::endl;
    std::cout << " test  inStringTable " << r.getColumnWidth(0) << std::endl;
    EXPECT_EQ(allocatedMemory.load(), currentAllocation);

    currentAllocation = allocatedMemory.load();
    r.nextRow();
    std::string longString(64 * 1024 + 1000, 'a');
    auto cs = utils::ConstString(longString);
    std::cout << "test longString " << longString.size() << " cs len " << cs.length()<< std::endl;

    r.setStringField(cs, 0);
    std::cout << " test  allocatedMemery " << allocatedMemory.load() << std::endl;
    std::cout << " test  inStringTable " << r.getColumnWidth(0) << std::endl;
    EXPECT_LE(allocatedMemory.load(), currentAllocation);

    rgD = rowgroup::RGData(rg);
    std::cout << " test  allocatedMemery " << allocatedMemory.load() << std::endl;

    EXPECT_EQ(allocatedMemory.load(), MemoryAllowance);

    // reinit
}

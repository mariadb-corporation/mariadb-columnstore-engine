/* Copyright (C) 2021 MariaDB Corporation

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

#include <iostream>
#include <gtest/gtest.h>
#include "datatypes/mcs_datatype.h"
#include "stats.h"
#include "primitives/linux-port/primitiveprocessor.h"
#include "col1block.h"
#include "col2block.h"
#include "col4block.h"
#include "col8block.h"
#include "col_float_block.h"
#include "col_double_block.h"
#include "col_neg_float.h"
#include "col_neg_double.h"

using namespace primitives;
using namespace datatypes;
using namespace std;

// If a test crashes check if there is a corresponding literal binary array in 
// readBlockFromLiteralArray.

class ColumnScanFilterTest : public ::testing::Test
{
  protected:
  PrimitiveProcessor pp;
  uint8_t input[BLOCK_SIZE];
  uint8_t output[4 * BLOCK_SIZE];
  uint8_t block[BLOCK_SIZE];
  uint16_t* rids;
  uint32_t i;
  uint32_t written;
  NewColRequestHeader* in;
  NewColResultHeader* out;
  ColArgs* args;
  
  void SetUp() override
  {
    memset(input, 0, BLOCK_SIZE);
    memset(output, 0, 4 * BLOCK_SIZE);
    in = reinterpret_cast<NewColRequestHeader*>(input);
    out = reinterpret_cast<NewColResultHeader*>(output);
    rids = reinterpret_cast<uint16_t*>(&in[1]);
    args = reinterpret_cast<ColArgs*>(&in[1]);
  }

  uint8_t* readBlockFromFile(const std::string& fileName, uint8_t* block)
  {
    int fd;
    uint32_t i;

    fd = open(fileName.c_str(), O_RDONLY);

    if (fd < 0)
    {
        cerr << "getBlock(): skipping this test; needs the index list file "
             << fileName << endl;
        return nullptr;
    }

    i = read(fd, block, BLOCK_SIZE);

    if (i <= 0)
    {
        cerr << "getBlock(): Couldn't read the file " << fileName << endl;
        close(fd);
        return nullptr;
    }

    if (i != BLOCK_SIZE)
    {
        cerr << "getBlock(): could not read a whole block" << endl;
        close(fd);
        return nullptr;
    }

    close(fd);
    return block;
  }
 uint8_t* readBlockFromLiteralArray(const std::string& fileName, uint8_t* block)
 {
    if (fileName == std::string("col1block.cdf"))
      return &__col1block_cdf[0];
    else if (fileName == std::string("col2block.cdf"))
      return &__col2block_cdf[0];
    else if (fileName == std::string("col4block.cdf"))
      return &__col4block_cdf[0];
    else if (fileName == std::string("col8block.cdf"))
      return &___bin_col8block_cdf[0];
    else if (fileName == std::string("col_float_block.cdf"))
      return &___bin_col_float_block_cdf[0];
    else if (fileName == std::string("col_double_block.cdf"))
      return &___bin_col_double_block_cdf[0];
    else if (fileName == std::string("col_neg_float.cdf"))
      return &___bin_col_neg_float_cdf[0];
    else if (fileName == std::string("col_neg_double.cdf"))
      return &___bin_col_neg_double_cdf[0];

    return nullptr;
 }
};


TEST_F(ColumnScanFilterTest, ColumnScan1Byte)
{
  constexpr const uint8_t W = 1;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  datatypes::make_unsigned<IntegralType>::type* results;
  in->colType = ColRequestHeaderDataType();
  in->colType.DataSize = 1;
  in->colType.DataType = SystemCatalog::CHAR;
  in->OutputType = OT_DATAVALUE;
  in->NOPS = 0;
  in->NVALS = 0;

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col1block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = &output[sizeof(NewColResultHeader)];
  EXPECT_EQ(out->NVALS, 8160);

  for (i = 0; i < 300; i++)
      EXPECT_EQ(results[i],i % 255);

}

TEST_F(ColumnScanFilterTest, ColumnScan2Bytes)
{
  constexpr const uint8_t W = 2;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  datatypes::make_unsigned<IntegralType>::type* results;
  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::INT;
  in->OutputType = OT_DATAVALUE;
  in->NOPS = 0;
  in->NVALS = 0;

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col2block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = reinterpret_cast<uint16_t*>(&output[sizeof(NewColResultHeader)]);
  EXPECT_EQ(out->NVALS, 4096);

  for (i = 0; i < out->NVALS; i++)
      EXPECT_EQ(results[i], i);
}

TEST_F(ColumnScanFilterTest, ColumnScan4Bytes)
{
  constexpr const uint8_t W = 4;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  datatypes::make_unsigned<IntegralType>::type* results;
  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::INT;
  in->OutputType = OT_DATAVALUE;
  in->NOPS = 0;
  in->NVALS = 0;

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col4block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = reinterpret_cast<uint32_t*>(&output[sizeof(NewColResultHeader)]);
  EXPECT_EQ(out->NVALS, 2048);

  for (i = 0; i < out->NVALS; i++)
      EXPECT_EQ(results[i], (uint32_t) i);
}

TEST_F(ColumnScanFilterTest, ColumnScan8Bytes)
{
  constexpr const uint8_t W = 8;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  datatypes::make_unsigned<IntegralType>::type* results;
  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::INT;
  in->OutputType = OT_DATAVALUE;
  in->NOPS = 0;
  in->NVALS = 0;

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col8block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = reinterpret_cast<u_int64_t*>(&output[sizeof(NewColResultHeader)]);
  ASSERT_EQ(out->NVALS, 1024);

  for (i = 0; i < out->NVALS; i++)
      ASSERT_EQ(results[i], (uint32_t) i);
}

TEST_F(ColumnScanFilterTest, ColumnScan1ByteUsingRID)
{
  constexpr const uint8_t W = 1;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  datatypes::make_unsigned<IntegralType>::type* results;
  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::INT;
  in->OutputType = OT_DATAVALUE;
  in->NOPS = 0;
  in->NVALS = 2;
  rids[0] = 20;
  rids[1] = 17;

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col1block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = reinterpret_cast<uint8_t*>(&output[sizeof(NewColResultHeader)]);
  ASSERT_EQ(out->NVALS, 2);

  for (i = 0; i < out->NVALS; i++)
      ASSERT_EQ(results[i], rids[i]);
}

TEST_F(ColumnScanFilterTest, ColumnScan4Bytes1Filter)
{
  constexpr const uint8_t W = 4;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  datatypes::make_unsigned<IntegralType>::type* results;
  IntegralType tmp;
  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::INT;
  in->OutputType = OT_DATAVALUE;
  in->NOPS = 2;
  in->BOP = BOP_AND;
  in->NVALS = 0;

  tmp = 20;
  args->COP = COMPARE_LT;
  memcpy(args->val, &tmp, in->colType.DataSize);
  args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) +
                                           sizeof(ColArgs) + in->colType.DataSize]);
  args->COP = COMPARE_GT;
  tmp = 10;
  memcpy(args->val, &tmp, in->colType.DataSize);

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col4block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = reinterpret_cast<uint32_t*>(&output[sizeof(NewColResultHeader)]);
  ASSERT_EQ(out->NVALS, 9);

  for (i = 0; i < out->NVALS; i++)
      ASSERT_EQ(results[i], 11 + (uint32_t)i);
}

//void p_Col_7()
TEST_F(ColumnScanFilterTest, ColumnScan8Bytes2CompFilters)
{
  constexpr const uint8_t W = 8;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  datatypes::make_unsigned<IntegralType>::type* results;
  IntegralType tmp;

  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::INT;
  in->OutputType = OT_DATAVALUE;
  in->NOPS = 2;
  in->BOP = BOP_OR;
  in->NVALS = 0;
  
  tmp = 10;
  args->COP = COMPARE_LT;
  memcpy(args->val, &tmp, in->colType.DataSize);
  args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) +
                                           sizeof(ColArgs) + in->colType.DataSize]);
  args->COP = COMPARE_GT;
  tmp = 1000;
  memcpy(args->val, &tmp, in->colType.DataSize);

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col8block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = reinterpret_cast<u_int64_t*>(&output[sizeof(NewColResultHeader)]);
  ASSERT_EQ(out->NVALS, 33);

  for (i = 0; i < out->NVALS; i++)
      ASSERT_EQ(results[i], (uint32_t) (i < 10 ? i : i - 10 + 1001));
}

TEST_F(ColumnScanFilterTest, ColumnScan8Bytes2EqFilters)
{
  constexpr const uint8_t W = 8;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  datatypes::make_unsigned<IntegralType>::type* results;
  IntegralType tmp;

  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::INT;
  in->OutputType = OT_DATAVALUE;
  in->NOPS = 2;
  in->BOP = BOP_OR;
  in->NVALS = 0;

  tmp = 10;
  args->COP = COMPARE_EQ;
  memcpy(args->val, &tmp, in->colType.DataSize);
  args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) +
                                    sizeof(ColArgs) + in->colType.DataSize]);
  args->COP = COMPARE_EQ;
  tmp = 1000;
  memcpy(args->val, &tmp, in->colType.DataSize);

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col8block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = reinterpret_cast<u_int64_t*>(&output[sizeof(NewColResultHeader)]);

  ASSERT_EQ(out->NVALS, 2);
  ASSERT_EQ(results[0], 10);
  ASSERT_EQ(results[1], 1000);
}

TEST_F(ColumnScanFilterTest, ColumnScan8Bytes2EqFiltersRID)
{
  constexpr const uint8_t W = 8;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  datatypes::make_unsigned<IntegralType>::type* results;
  IntegralType tmp;

  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::INT;
  in->OutputType = OT_DATAVALUE;
  in->NOPS = 2;
  in->BOP = BOP_OR;
  in->NVALS = 2;

  tmp = 10;
  args->COP = COMPARE_EQ;
  memcpy(args->val, &tmp, in->colType.DataSize);
  args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) +
                                           sizeof(ColArgs) + in->colType.DataSize]);
  args->COP = COMPARE_EQ;
  tmp = 1000;
  memcpy(args->val, &tmp, in->colType.DataSize);

  rids = reinterpret_cast<uint16_t*>(&input[sizeof(NewColRequestHeader) +
                                     2 * (sizeof(ColArgs) + in->colType.DataSize)]);

  rids[0] = 10;
  rids[1] = 100;

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col8block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = reinterpret_cast<u_int64_t*>(&output[sizeof(NewColResultHeader)]);
  ASSERT_EQ(out->NVALS, 1);
  ASSERT_EQ(results[0], 10);
}

TEST_F(ColumnScanFilterTest, ColumnScan8Bytes2EqFiltersRIDOutputRid)
{
  constexpr const uint8_t W = 8;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  int16_t* results;
  IntegralType tmp;

  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::INT;
  in->OutputType = OT_RID;
  in->NOPS = 2;
  in->BOP = BOP_OR;
  in->NVALS = 0;
  
  tmp = 10;
  args->COP = COMPARE_LT;
  memcpy(args->val, &tmp, in->colType.DataSize);
  args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) +
                                           sizeof(ColArgs) + in->colType.DataSize]);
  args->COP = COMPARE_GT;
  tmp = 1000;
  memcpy(args->val, &tmp, in->colType.DataSize);

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col8block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = reinterpret_cast<int16_t*>(&output[sizeof(NewColResultHeader)]);
  ASSERT_EQ(out->NVALS, 33);

  for (i = 0; i < out->NVALS; i++)
      ASSERT_EQ(results[i], (i < 10 ? i : i - 10 + 1001));
}

TEST_F(ColumnScanFilterTest, ColumnScan8Bytes2EqFiltersRIDOutputBoth)
{
  constexpr const uint8_t W = 8;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  IntegralType tmp;
  IntegralType* resultVal;
  int16_t* resultRid;

  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::INT;
  in->OutputType = OT_BOTH;
  in->NOPS = 2;
  in->BOP = BOP_OR;
  in->NVALS = 0;
  
  tmp = 10;
  args->COP = COMPARE_LT;
  memcpy(args->val, &tmp, in->colType.DataSize);
  args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) +
                                           sizeof(ColArgs) + in->colType.DataSize]);
  args->COP = COMPARE_GT;
  tmp = 1000;
  memcpy(args->val, &tmp, in->colType.DataSize);

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col8block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  ASSERT_EQ(out->NVALS, 33);

  for (i = 0; i < out->NVALS; i++)
  {
      resultRid = reinterpret_cast<int16_t*>(&output[
      sizeof(NewColResultHeader) + i * (sizeof(int16_t) + in->colType.DataSize)]);
      resultVal = reinterpret_cast<int64_t*>(&resultRid[1]);
      ASSERT_EQ(*resultRid, (i < 10 ? i : i - 10 + 1001));
      ASSERT_EQ(*resultVal, (i < 10 ? i : i - 10 + 1001));
  }
}

//void p_Col_12()
TEST_F(ColumnScanFilterTest, ColumnScan1Byte2CompFilters)
{
  constexpr const uint8_t W = 1;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  datatypes::make_unsigned<IntegralType>::type* results;

  in->colType = ColRequestHeaderDataType();
  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::CHAR;
  in->OutputType = OT_DATAVALUE;
  in->BOP = BOP_AND;
  in->NOPS = 2;
  in->NVALS = 0;

  args->COP = COMPARE_GT;
  args->val[0] = '2';
  // Filter is COP(1 byte)/rf(1 byte)/val(1 byte)
  args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) + 2 + W]);
  args->COP = COMPARE_LT;
  args->val[0] = '4';

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col1block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = &output[sizeof(NewColResultHeader)];
  ASSERT_EQ(out->NVALS, 32);

  for (i = 0; i < out->NVALS; i++)
      ASSERT_EQ( (int)'3', results[i]);
}
/*
The code doesn't support filters with RID and literal
//void p_Col_13()
TEST_F(ColumnScanFilterTest, ColumnScan4Bytes2CompFiltersOutputRID)
{
  constexpr const uint8_t W = 4;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  IntegralType tmp;
  int16_t ridTmp;
  int16_t* results;
  in->colType.DataSize = 4;
  in->colType.DataType = SystemCatalog::INT;
  in->OutputType = OT_RID;
  in->NOPS = 3;
  in->BOP = BOP_OR;
  in->NVALS = 3;

  // first argument "is RID 8 < 10?"  Answer is yes
  tmp = 10;	// value to check
  ridTmp = 8;
  args->COP = COMPARE_LT;
  memcpy(args->val, &tmp, in->colType.DataSize);
  memcpy(&args->val[in->colType.DataSize], &ridTmp, 2);

  // second argument "is RID 5 > 10?"  Answer is no
  args = reinterpret_cast<ColArgs*>(&args->val[in->colType.DataSize + 2]);
  args->COP = COMPARE_GT;
  tmp = 10;
  ridTmp = 5;
  memcpy(args->val, &tmp, in->colType.DataSize);
  memcpy(&args->val[in->colType.DataSize], &ridTmp, 2);

  // third argument "is RID 11 < 1000?"  Answer is yes
  args = reinterpret_cast<ColArgs*>(&args->val[in->colType.DataSize + 2]);
  args->COP = COMPARE_LT;
  tmp = 1000;
  ridTmp = 11;
  memcpy(args->val, &tmp, in->colType.DataSize);
  memcpy(&args->val[in->colType.DataSize], &ridTmp, 2);

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col4block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = reinterpret_cast<int16_t*>(&output[sizeof(NewColResultHeader)]);
  ASSERT_EQ(out->NVALS, 2);
  ASSERT_EQ(results[0], 8);
  ASSERT_EQ(results[1], 11);
}
*/
//void p_Col_double_1()
TEST_F(ColumnScanFilterTest, ColumnScan8BytesDouble2CompFilters)
{
  constexpr const uint8_t W = 8;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  double* results;
  double tmp;

  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::DOUBLE;
  in->OutputType = OT_DATAVALUE;
  in->BOP = BOP_AND;
  in->NOPS = 2;
  in->NVALS = 0;

  tmp = 10.5;
  args->COP = COMPARE_GT;
  memcpy(args->val, &tmp, sizeof(tmp));
  tmp = 15;
  args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) + 10]);
  args->COP = COMPARE_LT;
  memcpy(args->val, &tmp, sizeof(tmp));

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col_double_block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = reinterpret_cast<double*>(&output[sizeof(NewColResultHeader)]);
  ASSERT_EQ(out->NVALS, 8);

  for (i = 0; i < out->NVALS; i++)
      ASSERT_EQ(results[i], 11 + (i * 0.5));
}

//void p_Col_float_1()
TEST_F(ColumnScanFilterTest, ColumnScan4BytesFloat2CompFiltersOutputBoth)
{
  constexpr const uint8_t W = 4;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  float* resultVal;
  float tmp;
  int16_t* resultRid;

  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::FLOAT;
  in->OutputType = OT_BOTH;
  in->BOP = BOP_AND;
  in->NOPS = 2;
  in->NVALS = 0;
  
  tmp = 10.5;
  args->COP = COMPARE_GT;
  memcpy(args->val, &tmp, sizeof(tmp));
  tmp = 15;
  args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) + 6]);
  args->COP = COMPARE_LT;
  memcpy(args->val, &tmp, sizeof(tmp));

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col_float_block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  ASSERT_EQ(out->NVALS, 8);

  for (i = 0; i < out->NVALS; i++)
  {
      resultRid = reinterpret_cast<int16_t*>(&output[
      sizeof(NewColResultHeader) + i * (sizeof(int16_t) + in->colType.DataSize)]);
      resultVal = reinterpret_cast<float*>(&resultRid[1]);
      ASSERT_EQ(*resultVal, 11 + (i * 0.5));
  }
}

//void p_Col_neg_float_1()
TEST_F(ColumnScanFilterTest, ColumnScan4BytesNegFloat2CompFiltersOutputBoth)
{
  constexpr const uint8_t W = 4;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  float* resultVal;
  float tmp;
  int16_t* resultRid;

  in->colType.DataSize = 4;
  in->colType.DataType = SystemCatalog::FLOAT;
  in->OutputType = OT_BOTH;
  in->BOP = BOP_AND;
  in->NOPS = 2;
  in->NVALS = 0;
  
  tmp = -5.0;
  args->COP = COMPARE_GT;
  memcpy(args->val, &tmp, sizeof(tmp));
  tmp = 5.0;
  args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) + 6]);
  args->COP = COMPARE_LT;
  memcpy(args->val, &tmp, sizeof(tmp));

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col_neg_float.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);
  ASSERT_EQ(out->NVALS, 19);

  for (i = 0; i < out->NVALS; i++)
  {
      resultRid = reinterpret_cast<int16_t*>(&output[
      sizeof(NewColResultHeader) + i * (sizeof(int16_t) + in->colType.DataSize)]);
      resultVal = reinterpret_cast<float*>(&resultRid[1]);
      ASSERT_EQ(*resultVal, -4.5 + (i * 0.5));
  }
}

//void p_Col_neg_double_1()
TEST_F(ColumnScanFilterTest, ColumnScan4BytesNegDouble2CompFilters)
{
  constexpr const uint8_t W = 8;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  double* results;
  double tmp;

  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::DOUBLE;
  in->OutputType = OT_DATAVALUE;
  in->BOP = BOP_AND;
  in->NOPS = 2;
  in->NVALS = 0;

  tmp = -5.0;
  args->COP = COMPARE_GT;
  memcpy(args->val, &tmp, sizeof(tmp));
  tmp = 5.0;
  args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) + 10]);
  args->COP = COMPARE_LT;
  memcpy(args->val, &tmp, sizeof(tmp));

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col_neg_double.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);

  results = reinterpret_cast<double*>(&output[sizeof(NewColResultHeader)]);
  ASSERT_EQ(out->NVALS, 19);

  for (i = 0; i < out->NVALS; i++)
      ASSERT_EQ(results[i], -4.5 + (i * 0.5));
}

TEST_F(ColumnScanFilterTest, ColumnScan16Bytes2CompFilters)
{
//TBD
}
// vim:ts=2 sw=2:

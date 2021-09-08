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
#include <benchmark/benchmark.h>

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

// TODO Use FastOperation() to speed up run loop

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

class FilterBenchFixture : public benchmark::Fixture
{
 public:
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

  void SetUp(::benchmark::State& state)
  {
    memset(input, 0, BLOCK_SIZE);
    memset(output, 0, 4 * BLOCK_SIZE);
    in = reinterpret_cast<NewColRequestHeader*>(input);
    out = reinterpret_cast<NewColResultHeader*>(output);
    rids = reinterpret_cast<uint16_t*>(&in[1]);
    args = reinterpret_cast<ColArgs*>(&in[1]);
  }

  void inTestRunSetUp(const std::string& dataName, const size_t dataSize, const uint8_t dataType, const uint32_t outputType, ColArgs* args)
  {
    in->colType = ColRequestHeaderDataType();
    in->colType.DataSize = dataSize;
    in->colType.DataType = dataType;
    in->OutputType = outputType;
    in->NOPS = 0;
    in->NVALS = 0;
    pp.setBlockPtr((int*) readBlockFromLiteralArray(dataName, block));
  }
  void runFilterBenchLegacy()
  {
    pp.p_Col(in, out, 4 * BLOCK_SIZE, &written);
  }

  template<int W>
  void runFilterBenchTemplated()
  {
    using IntegralType = typename datatypes::WidthToSIntegralType<W>::type;
    pp.columnScanAndFilter<IntegralType>(in, out, 4 * BLOCK_SIZE, &written);
  }

};

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan1ByteLegacyCode)(benchmark::State& state)
{
  for (auto _ : state)
  {
    constexpr const uint8_t W = 1;
    state.PauseTiming();
    inTestRunSetUp("col1block.cdf", W, SystemCatalog::CHAR, OT_DATAVALUE, args);
    state.ResumeTiming();
    runFilterBenchLegacy();
  }
}

BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan1ByteLegacyCode);

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan1ByteTemplatedCode)(benchmark::State& state)
{
  for (auto _ : state)
  {
    constexpr const uint8_t W = 1;
    state.PauseTiming();
    inTestRunSetUp("col1block.cdf", W, SystemCatalog::CHAR, OT_DATAVALUE, args);
    state.ResumeTiming();
    runFilterBenchTemplated<1>();
  }
}

BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan1ByteTemplatedCode);

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan2ByteLegacyCode)(benchmark::State& state)
{
  for (auto _ : state)
  {
    constexpr const uint8_t W = 2;
    state.PauseTiming();
    inTestRunSetUp("col2block.cdf", W, SystemCatalog::INT, OT_DATAVALUE, args);
    state.ResumeTiming();
    runFilterBenchLegacy();
  }
}
BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan2ByteLegacyCode);

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan2ByteTemplatedCode)(benchmark::State& state)
{
  for (auto _ : state)
  {
    constexpr const uint8_t W = 2;
    state.PauseTiming();
    inTestRunSetUp("col2block.cdf", W, SystemCatalog::INT, OT_DATAVALUE, args);
    state.ResumeTiming();
    runFilterBenchTemplated<2>();
  }
}

BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan2ByteTemplatedCode);

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan4ByteLegacyCode)(benchmark::State& state)
{
  for (auto _ : state)
  {
    constexpr const uint8_t W = 4;
    state.PauseTiming();
    inTestRunSetUp("col4block.cdf", W, SystemCatalog::INT, OT_DATAVALUE, args);
    state.ResumeTiming();
    runFilterBenchLegacy();
  }
}
BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan4ByteLegacyCode);

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan4ByteTemplatedCode)(benchmark::State& state)
{
  for (auto _ : state)
  {
    constexpr const uint8_t W = 4;
    state.PauseTiming();
    inTestRunSetUp("col4block.cdf", W, SystemCatalog::INT, OT_DATAVALUE, args);
    state.ResumeTiming();
    runFilterBenchTemplated<4>();
  }
}

BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan4ByteTemplatedCode);

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan8ByteLegacyCode)(benchmark::State& state)
{
  for (auto _ : state)
  {
    constexpr const uint8_t W = 8;
    state.PauseTiming();
    inTestRunSetUp("col8block.cdf", W, SystemCatalog::INT, OT_DATAVALUE, args);
    state.ResumeTiming();
    runFilterBenchLegacy();
  }
}
BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan8ByteLegacyCode);

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan8ByteTemplatedCode)(benchmark::State& state)
{
  for (auto _ : state)
  {
    constexpr const uint8_t W = 8;
    state.PauseTiming();
    inTestRunSetUp("col8block.cdf", W, SystemCatalog::INT, OT_DATAVALUE, args);
    state.ResumeTiming();
    runFilterBenchTemplated<8>();
  }
}

BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan8ByteTemplatedCode);
BENCHMARK_MAIN();
// vim:ts=2 sw=2:
